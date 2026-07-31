import os
import json
import logging
import time
from datetime import datetime
from kafka import KafkaConsumer
import boto3
from botocore.exceptions import ClientError
import gzip
from io import BytesIO

# Настройка логгирования
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Конфигурация
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
INPUT_TOPIC = os.getenv("INPUT_TOPIC", "raw-logs")
GROUP_ID = os.getenv("GROUP_ID", "archiver-group")
INTERVAL_MINUTES = int(os.getenv("INTERVAL_MINUTES", "60"))

# Конфигурация MinIO
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "logs-archive")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"

# Архиватор логов из Kafka в MinIO
class LogArchiver:

    # Сама фунция bucket        
    def _ensure_bucket(self):
        try:
            self.s3_client.head_bucket(Bucket=self.bucket)
            logger.info(f"✅ Bucket {self.bucket} уже существует")
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                self.s3_client.create_bucket(Bucket=self.bucket)
                logger.info(f"✅ Создан bucket {self.bucket}")
            else:
                logger.error(f"❌ Ошибка проверки bucket: {e}")
                raise

    def __init__(self):
        self.consumer = None
        self.s3_client= None
        self.bucket = MINIO_BUCKET
        
        # Инициализация Consumer
        try:
            self.consumer = KafkaConsumer(
                INPUT_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                group_id=GROUP_ID,
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                consumer_timeout_ms=5000,
                value_deserializer=lambda v: json.loads(v.decode('utf-8'))
            )
            logger.info(f"✅ Consumer подключен к топику {INPUT_TOPIC}")
        except Exception as e:
            logger.error(f"❌ Ошибка создания Consumer: {e}")
            raise
            
        # Инициализация MinIO клиента (S3)
        try:
            self.s3_client = boto3.client(
                's3',
                endpoint_url=f"http{'s' if MINIO_SECURE else ''}://{MINIO_ENDPOINT}",
                aws_access_key_id=MINIO_ACCESS_KEY,
                aws_secret_access_key=MINIO_SECRET_KEY,
                verify=False
            )
            logger.info(f"✅ Подключен к MinIO: {MINIO_ENDPOINT}")
            
            # Создание bucket, если его нет
            self._ensure_bucket()
        except Exception as e:
            logger.error(f"❌ Ошибка подключения к MinIO: {e}")
            raise
                
    # Архиватор логов в MinIO
    def archive_logs(self, logs: list, date_str: str):
        if not logs:
            return
            
        # Формирование имени файла logs/YYYY-MM-DD/HH-MM-ss.json.gz
        filename = f"logs/{date_str[:10]}/{date_str[11:13]}-{date_str[14:16]}-{date_str[17:19]}.json.gz"
        
        try:
            # Сжимание логов
            json_data = json.dumps(logs, ensure_ascii=False).encode('utf-8')
            compressed = BytesIO()
            with gzip.GzipFile(fileobj=compressed, mode='wb') as gz:
                gz.write(json_data)
            compressed.seek(0)
            
            # Загрузка в MinIO
            self.s3_client.upload_fileobj(
                compressed,
                self.bucket,
                filename,
                ExtraArgs={'ContentType': 'application/json', 'ContentEncoding': 'gzip'}
            )
            logger.info(f"✅ Архив сохранён: {filename} ({len(logs)} логов, {compressed.getbuffer().nbytes} байт)")
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения архива: {e}")
            
    # Основной цикл архивации
    def run(self):
        logger.info("🚀 Запуск архиватора логов")
        logger.info(f"📥 Входной топик: {INPUT_TOPIC}")
        logger.info(f"📦 Bucket: {MINIO_BUCKET}")
        logger.info(f"⏱️  Интервал: {INTERVAL_MINUTES} минут")
        
        last_run = datetime.now()
        batch = []
        
        while True:
            try:
                # Чтение сообщений из Kafka
                messages = self.consumer.poll(timeout_ms=5000)
                
                if messages:
                    for topic_partition, msgs in messages.items():
                        for msg in msgs:
                            try:
                                batch.append(msg.value)
                            except Exception as e:
                                logger.error(f"❌ Ошибка обработки сообщения: {e}")
                                
                # Проверка, пора ли архивировать
                now = datetime.now()
                if (now - last_run).total_seconds() >= INTERVAL_MINUTES *60:
                    if batch:
                        date_str = now.isoformat()
                        self.archive_logs(batch, date_str)
                        batch = []
                    last_run = now
                
                time.sleep(1)
                
            except KeyboardInterrupt:
                logger.info("🛑 Получен сигнал остановки")
                # Сохранение оставшихся логов
                if batch:
                    self.archive_logs(batch, datetime.now().isoformat())
                break
            except Exception as e:
                logger.error(f"💥 Критическая ошибка: {e}")
                time.sleep(10)
                
        logger.info("👋 Завершение работы архиватора")
        
if __name__ == "__main__":
    archiver = LogArchiver()
    archiver.run()
    
