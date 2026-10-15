FROM python:3.14-slim

WORKDIR /app

# Установка зависимостей
COPY app/parser/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копирование файлов
COPY app/parser/ ./

# Переменные окружения по умолчанию
ENV KAFKA_BOOTSTRAP=kafka:9092
ENV INPUT_TOPIC=raw-logs
ENV OUTPUT_TOPIC=processed-logs
ENV ERROR_TOPIC=error-logs
ENV GROUP_ID=log-parser-group
ENV ALERT_TYPE=log

CMD ["python", "parser.py"]
