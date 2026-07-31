FROM python:3.14-slim

WORKDIR /app

# Установка зависимостей
COPY app/collector/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копирование кода
COPY app/collector/ ./

# Переменные окружения по умолчанию
ENV KAFKA_BOOTSTRAP=kafka:9092
ENV KAFKA_TOPIC=raw-logs
ENV COLLECT_INTERVAL=5
ENV USE_EMULATOR=true

CMD ["python", "collector.py"]

