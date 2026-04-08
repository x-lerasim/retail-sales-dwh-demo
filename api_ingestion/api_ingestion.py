import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Dict, List

import boto3
import requests
from botocore.exceptions import ClientError
from requests.exceptions import RequestException

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("Core")

base_url = os.getenv("TMS_API_BASE_URL", "http://tms-api:8085")
headers = {}

PAGE_LIMIT = int(os.getenv("TRACKING_PAGE_LIMIT", "500"))
WATERMARK_KEY = os.getenv("TRACKING_WATERMARK_KEY", "api/state/watermark.json")


def _s3():
    client = boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT"),
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY"),
    )
    bucket = os.getenv("MINIO_BUCKET", "raw")
    return client, bucket


def load_watermark():
    s3, bucket = _s3()
    try:
        r = s3.get_object(Bucket=bucket, Key=WATERMARK_KEY)
        doc = json.loads(r["Body"].read())
        return doc.get("last_timestamp")
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            return None
        raise


def save_watermark(last_timestamp: str):
    s3, bucket = _s3()
    body = json.dumps(
        {
            "last_timestamp": last_timestamp,
            "saved_at": datetime.now(timezone.utc).isoformat(),
        },
        ensure_ascii=False,
    ).encode("utf-8")
    s3.put_object(Bucket=bucket, Key=WATERMARK_KEY, Body=body)
    logger.info("Watermark: %s", last_timestamp)


def _fetch_page(since, page: int, limit: int, max_retries: int, timeout: int) -> dict:
    url = f"{base_url}/api/v1/tracking"
    params = {"page": page, "limit": limit}
    if since is not None:
        params["since"] = since

    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers, params=params, timeout=timeout)
            response.raise_for_status()
            data = response.json()
            if "data" not in data or "pagination" not in data:
                raise ValueError("Invalid response: need data and pagination")
            return data
        except RequestException as e:
            logger.error("Request failed page=%s: %s", page, e)
            if attempt < max_retries - 1:
                wait = 2**attempt
                logger.info("Retry in %s s", wait)
                time.sleep(wait)
            else:
                raise ValueError(f"Failed page {page} after {max_retries} attempts") from e


def ingest_all_pages(since=None, max_retries=5, timeout=30):
    limit = max(1, min(PAGE_LIMIT, 1000))
    all_events = []
    page = 1

    while True:
        data = _fetch_page(since, page, limit, max_retries, timeout)
        chunk = data["data"]
        pages = data["pagination"]["pages"]

        all_events.extend(chunk)
        logger.info("Page %s/%s, rows in chunk=%s", page, pages, len(chunk))

        if page >= pages or not chunk:
            break
        page += 1

    return all_events


def max_timestamp(events: List[Dict]):
    if not events:
        return None
    return max(e["timestamp"] for e in events)


def upload_to_minio(events: List[Dict]):
    s3, bucket = _s3()
    run_dt = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    batch = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    key = f"api/tms_tracking/dt={run_dt}/batch_{batch}.jsonl"
    body = "\n".join(json.dumps(e, ensure_ascii=False) for e in events)
    s3.put_object(Bucket=bucket, Key=key, Body=body.encode("utf-8"))
    logger.info("Uploaded %s rows -> s3://%s/%s", len(events), bucket, key)


if __name__ == "__main__":
    wm = load_watermark()
    if wm is None and os.getenv("TRACKING_INITIAL_SINCE"):
        wm = os.getenv("TRACKING_INITIAL_SINCE")

    if wm is None:
        logger.info("No watermark — full load (no since)")
        since = None
    else:
        since = wm
        logger.info("since=%s", since)

    events = ingest_all_pages(since=since)
    if not events:
        logger.info("Nothing to upload")
    else:
        upload_to_minio(events)
        m = max_timestamp(events)
        if m:
            save_watermark(m)
