FROM python:3.11-slim

WORKDIR /app

COPY app/archiver/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app/archiver/archiver.py .

ENV KAFKA_BOOTSTRAP=kafka:9092
ENV INPUT_TOPIC=raw-logs
ENV GROUP_ID=archiver-group
ENV INTERVAL_MINUTES=60
ENV MINIO_ENDPOINT=minio:9000
ENV MINIO_ACCESS_KEY=minioadmin
ENV MINIO_SECRET_KEY=minioadmin
ENV MINIO_BUCKET=logs-archive

CMD ["python", "archiver.py"]
