FROM python:3.10-slim

WORKDIR /app
COPY api/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY api/tms_api.py .

CMD ["uvicorn", "tms_api:app", "--host", "0.0.0.0", "--port", "8085"]
