#!/usr/bin/env python3
import os
import json
import logging
import time
from datetime import datetime
from typing import List, Dict
from emulator import WindowsLogEmulator

# Настройка логгирования
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Импорт WinRM клиента
# from winrm_client import WinRmCollector

# Конфигурация из переменных окружения (задаются через k8s ConfigMap)
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "raw-logs")
COLLECT_INTERVAL = int(os.getenv("COLLECT_INTERVAL", "5"))
USE_EMULATOR = os.getenv("USE_EMULATOR", "true").lower() == "true"
WINRM_HOSTS = os.getenv("WINRM_HOSTS", "").split(",") if os.getenv("WINRM_HOSTS") else []


class LogCollector:
    # Универсальный сборщик логов (эмулятор + WinRM)
    def __init__(self):
        self.use_emulator = USE_EMULATOR
        self.winrm_hosts = WINRM_HOSTS
        self.kafka_producer = None
        
        # Инициализация Kafka
        try:
            from kafka import KafkaProducer
            self.kafka_producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                compression_type='gzip'
            )
            logger.info(f"✅ Подключен к Kafka: {KAFKA_BOOTSTRAP}")
        except ImportError:
            logger.warning("⚠️  Библиотека kafka-python не установлена. Работа в режиме STDOUT")
            self.kafka_producer = None
        except Exception as e:
            logger.error(f"❌ Ошибка подключения к Kafka: {str(e)}")
            self.kafka_producer = None
            
    # Сбор из эмулятора (для разработки и демонстрации)
    def collect_from_emulator(self) -> List[Dict]:
        logger.info("📊 Используется эмулятор Windows-логов")
        batch = WindowsLogEmulator.generate_batch(10)
        logger.info(f"✅ Сгенерировано {len(batch)} логов")
        return batch
        
    # Сбор с реальных Windows серверов через WinRM
    def collect_from_winrm(self) -> List[Dict]:
        logger.info("🔐 Режим WinRM: сбор с реальных серверов")
        all_logs = []
    
        # Раскомментировать для продакшна
        """
        for host in self.winrm_hosts:
            if not host.strip():
                continue
            
            # Получение логинов паролей из k8s Secrets
            username = os.getenv(f"WINRM_USER_{host.upper().replace('-','_')}", "")
            password = os.getenv(f"WINRM_PASS_{host.upper().replace('-','_')}", "")
        
            if not username or not password:
                logger.warning(f"⚠️  Пропускаем {host}: нет учетных данных")
                continue
            
            collector = WinRMCollector(host, username, password)
            if collector.connect():
                logs = collector.get_security_logs(hours_back=1)
                if logs:
                    # Добавляем имя хоста в каждый лог
                    for log in logs:
                        log['source_host'] = host
                    all_logs.extend(logs)
                    logger.info(f"✅ Получено {len(logs)} логов с {host}")
                collector.disconnect()
            time.sleep(1) # Защита от флуда
            
        """
        
        #Заглушка для демонстрации кода
        logger.info("ℹ️  WinRM код закомментирован. Для production раскомментируйте в collector.py")
        return all_logs
        
    # Отправка логов в Kafka
    def send_to_kafka(self, logs: List[Dict]):
        if not logs:
            return
            
        if self.kafka_producer:
            try:
                for log in logs:
                    future = self.kafka_producer.send(KAFKA_TOPIC, value=log)
                    # Блокировка до подтверждения для демо, в проде асинхронно
                    future.get(timeout=5)
                self.kafka_producer.flush()
                logger.info(f"📤 Отправлено {len(logs)} логов в топик {KAFKA_TOPIC}")
            except Exception as e:
                logger.error(f"❌ Ошибка отправки в Kafka: {str(e)}")
        else:
            # Режим отладки, печать в консоль
            logger.info("=" * 60)
            logger.info(f"📋 Логи (режим STDOUT): {datetime.now().isoformat()}")
            logger.info("=" * 60)
            for log in logs:
                print(json.dumps(log, ensure_ascii=False, indent=2))
                print("-" * 40)
                
    # Основной цикл сбора логов
    def run(self):
        logger.info("🚀 Запуск сборщика логов")
        logger.info(f"⚙️  Режим: {'ЭМУЛЯТОР' if self.use_emulator else 'WINRM'}")
        logger.info(f"⏱️  Интервал: {COLLECT_INTERVAL} сек")
        
        while True:
            try:
                if self.use_emulator:
                    logs = self.collect_from_emulator()
                else:
                    logs = self.collect_from_winrm()
                    
                if logs:
                    self.send_to_kafka(logs)
                else:
                    logger.warning("⚠️  Логи не получены")
                    
                time.sleep(COLLECT_INTERVAL)
                
            except KeyboardInterrupt:
                logger.info("🛑 Получен сигнал остановки")
                break
            except Exception as e:
                logger.error(f"💥 Критическая ошибка: {str(e)}")
                time.sleep(COLLECT_INTERVAL * 2)
                
if __name__ == "__main__":
    collector = LogCollector()
    collector.run()
            
            
