import os
import json
import logging
import time
from typing import Dict, Optional
from datetime import datetime
from kafka import KafkaConsumer, KafkaProducer

# Настройка логирования
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format='%(asctime)s - %(name)s - %(levelname)s -%(message)s'
)
logger = logging.getLogger(__name__)

# Конфигурация из переменных окружения
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
INPUT_TOPIC = os.getenv("INPUT_TOPIC", "raw-logs")
OUTPUT_TOPIC = os.getenv("OUTPUT_TOPIC", "processed-logs")
ERROR_TOPIC = os.getenv("ERROR_TOPIC", "error-logs")
GROUP_ID = os.getenv("GROUP_ID", "log-parser-group")
CONSUMER_TIMEOUT = int(os.getenv("CONSUMER_TIMEOUT", "1000"))

# Правила для обнаружения ошибок
ERROR_KEYWORDS = ["error", "failed", "crash", "exception", "fatal", "panic", "timeout", "unavailable"]
WARNING_KEYWORDS = ["warning", "warn", "full", "low", "slow"]

# Парсер логов из Кафка
class LogParser:
    def __init__(self):
        self.consumer = None
        self.producer = None
        self.alert_counter = 0
        
        # Инициализация Consumer
        try:
            self.consumer = KafkaConsumer(
                INPUT_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                group_id=GROUP_ID,
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                consumer_timeout_ms=CONSUMER_TIMEOUT,
                value_deserializer=lambda v: json.loads(v.decode('utf-8'))
            )
            logger.info(f"✅ Consumer подключен к топику {INPUT_TOPIC}")
        except Exception as e:
            logger.error(f"❌ Ошибка создания Consumer: {e}")
            raise
            
        # Инициализация Produser
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                compression_type='gzip'
            )
            logger.info(f"✅ Producer подключен к Kafka: {KAFKA_BOOTSTRAP}")
        except Exception as e:
            logger.error(f"❌ Ошибка создания Producer: {e}")
            raise
            
    # Анализ лог и определение его важности
    def analyze_log(self, log: Dict) -> Dict:
        result = {
            "original": log,
            "level": log.get("level", "info"),
            "message": log.get("message", ""),
            "timestamp": log.get("timestamp", ""),
            "severity": "info",
            "matched_keywords": [],
            "alert_needed": False
        }
        
        # Проверка уровня лога
        if result["level"] in ["error", "critical"]:
            result["severity"] = "critical"
            result["alert_needed"] = True
            result["matched_keywords"].append("level: " + result["level"])
            
        # Проверка сообщения на ключевые слова
        message_lower = result["message"].lower()
        
        for keyword in ERROR_KEYWORDS:
            if keyword in message_lower:
                result["severity"] = "error"
                result["alert_needed"] = True
                result["matched_keywords"].append(keyword)
                break
                
        if not result["alert_needed"]:
            for keyword in WARNING_KEYWORDS:
                if keyword in message_lower:
                    result["severity"] = "warning"
                    result["matched_keywords"].append(keyword)
                    break
                
        return result
        
    # Отправление алертов в ТГ, Слак или просто логгинг
    def send_alert(self, log: Dict, analysis: Dict):
        alert_type = os.getenv("ALERT_TYPE", "log")
        
        message = (
            f"🚨 {analysis['severity'].upper()} Alert!\n"
            f"📅 Time: {log.get('timestamp', 'unknown')}\n"
            f"💻 Host: {log.get('host', 'unknown')}\n"
            f"📝 Message: {log.get('message', '')[:200]}\n"
            f"🔍 Keywords: {', '.join(analysis['matched_keywords'])}"
        )
        
        if alert_type == "telegram":
            self.send_telegram_alert(message)
        else:
            # Просто логирование
            logger.warning(f"🔔 ALERT: {message}")
            
        self.alert_counter += 1
        
    def send_telegram_alert(self, message:str):
        bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
        chat_id = os.getenv("TELEGRAM_CHAT_ID")
        
        if not bot_token or not chat_id:
            logger.warning("⚠️ Telegram не настроен, используется логирование")
            logger.warning(f"🔔 ALERT: {message}")
            return
            
        try:
            import requests
            url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
            payload = {
                "chat_id": chat_id,
                "text": message,
                "parse_mode": "HTML",
            }
            response = requests.post(url, json=payload, timeout=5)
            if response.status_code == 200:
                logger.info("✅ Алерт отправлен в Telegram")
            else:
                logger.error(f"❌ Ошибка отправки в Telegram: {response.text}")
        except Exception as e:
            logger.error(f"❌ Исключение при отправке в Telegram: {e}")
            
    # Обработка одного лога
    def process_log(self, log: Dict) -> Optional[Dict]:
        try:
            analysis = self.analyze_log(log)
            
            # Отправление алерта
            if analysis["alert_needed"]:
                self.send_alert(log, analysis)
                
            # Подготовка обогащенного лога для отправки
            enriched_log = {
                **log,
                "parsed": {
                    "severity": analysis["severity"],
                    "matched_keywords": analysis["matched_keywords"],
                    "alert_sent": analysis["alert_needed"],
                    "processed_at": datetime.now().isoformat()
                }
            }
            
            return enriched_log
        except Exception as e:
            logger.error(f"❌ Ошибка обработки лога: {e}")
            return None
            
    # Основной цикл обработки
    def run(self):
        logger.info("🚀 Запуск парсера логов")
        logger.info(f"📥 Входной топик: {INPUT_TOPIC}")
        logger.info(f"📤 Выходной топик: {OUTPUT_TOPIC}")
        logger.info(f"🔴 Топик ошибок: {ERROR_TOPIC}")
        
        processed_count = 0
        error_count = 0
        
        while True:
            try:
                # Считывание сообщений из Kafka
                messages = self.consumer.poll(timeout_ms=CONSUMER_TIMEOUT)
                
                if not messages:
                    continue
                    
                for topic_partition, msgs in messages.items():
                    for msg in msgs:
                        try:
                            log = msg.value
                            processed_log = self.process_log(log)
                            
                            if processed_log:
                                # Отправление обработанного лога в выходной топик
                                self.producer.send(OUTPUT_TOPIC, value=processed_log)
                                processed_count +=1
                                
                                if processed_count % 10 == 0:
                                    logger.info(f"📊 Обработано {processed_count} логов, "
                                                      f"алертов: {self.alert_counter}")
                            else:
                                # Отправление в топик ошибок
                                error_log = {
                                    "original": log,
                                    "error": "Ошибка обработки",
                                    "timestamp": datetime.now().isoformat()
                                }
                                self.producer.send(ERROR_TOPIC, value=error_log)
                                error_count +=1
                                
                        except Exception as e:
                            logger.error(f"💥 Ошибка обработки сообщения: {e}")
                            error_count +=1
                        
                # Сброс буфера
                self.producer.flush()
                
            except KeyboardInterrupt:
                logger.info("🛑 Получен сигнал остановки")
                break
            except Exception as e:
                logger.error(f"💥 Критическая ошибка: {e}")
                time.sleep(5)
                
        logger.info(f"📊 Итог: обработано {processed_count} логов, ошибок: {error_count}")
        logger.info("👋 Завершение работы парсера")
        
    # Закрытие соединений
    def __del__(self):
        if self.consumer:
            self.consumer.close()
        if self.producer:
            self.producer.flush()
            self.producer.close()
            
           
if __name__ == "__main__":
    parser = LogParser()
    parser.run()
    
                        
            
