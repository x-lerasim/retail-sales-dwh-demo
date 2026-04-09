from pyspark.sql import SparkSession
import logging
import os
from datetime import datetime, timedelta
import argparse

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('ExtractPG')


SOURCE_SCHEMA = "source_kmk"

ALL_TABLES = ["orders", "order_items", "shipments", "order_status_history", "payments", "returns", "customers", "products"]
INCREMENTAL_TABLES = {
    "orders": "event_timestamp",
    "order_items": "event_timestamp",
    "shipments": "event_timestamp",
    "order_status_history": "event_timestamp",
    "payments": "event_timestamp",
    "returns": "event_timestamp",
}

SNAPSHOT_TABLES = ["customers", "products"]

def parse_args():
    parser = argparse.ArgumentParser(description="Extract data from PostgreSQL and save to S3")
    parser.add_argument(
        "--data-date",
        required=True,
        help="Data date in YYYY-MM-DD format (partition data_date=… and default UTC day window)",
    )
    args = parser.parse_args()
    try:
        datetime.strptime(args.data_date, "%Y-%m-%d")
    except ValueError:
        parser.error("--data-date must be YYYY-MM-DD")


    return args


def main():
    spark = None
    try:
        spark = (
            SparkSession.builder
            .appName("Extract PostgreSQL data")
            .getOrCreate()
        )
   

        args = parse_args()

        logging.info("Starting Spark session")

        url = f"jdbc:postgresql://{os.getenv('SOURCES_POSTGRES_HOST', 'postgres')}:{os.getenv('SOURCES_POSTGRES_PORT', '5432')}/{os.getenv('SOURCES_POSTGRES_DB', 'kmk')}"
        props = {
            "user": os.getenv("SOURCES_POSTGRES_USER", "admin"),
            "password": os.getenv("SOURCES_POSTGRES_PASSWORD", "admin"),
            "driver": "org.postgresql.Driver",
        }
         
        day = datetime.strptime(args.data_date, "%Y-%m-%d")
        end = day.replace(hour=0, minute=0, second=0, microsecond=0)
        start = end - timedelta(days=1)

        start_s = start.strftime("%Y-%m-%d %H:%M:%S")
        end_s = end.strftime("%Y-%m-%d %H:%M:%S")
        load_date = start.strftime("%Y-%m-%d")
       
        for table in ALL_TABLES:
            out_path = f"s3a://raw/postgres/{table}/data_date={load_date}/"
            if table in INCREMENTAL_TABLES:
                column = INCREMENTAL_TABLES[table]
                subquery = (
                    f"(SELECT * FROM {SOURCE_SCHEMA}.{table} "
                    f"WHERE {column} >= TIMESTAMP '{start_s}' "
                    f"AND {column} < TIMESTAMP '{end_s}') AS t"
                )
                df = spark.read.jdbc(url=url, table=subquery, properties=props)
                df.write.mode("overwrite").parquet(out_path)
                logger.info("Saved %s to %s", table, out_path)
            elif table in SNAPSHOT_TABLES:
                df = spark.read.jdbc(url=url, table=f"{SOURCE_SCHEMA}.{table}", properties=props)
                df.write.mode("overwrite").parquet(out_path)
                logger.info("Saved %s to %s", table, out_path)

    except Exception:
        logger.exception("Extract job failed")
        raise
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()
