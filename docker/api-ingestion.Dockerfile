FROM python:3.11-slim

WORKDIR /app

COPY api_ingestion/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY api_ingestion /app/api_ingestion
CMD ["python", "-m", "api_ingestion.api_ingestion"]
