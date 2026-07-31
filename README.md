# 🚀 College Log Pipeline

Система эмуляции, сбора, обработки и архивации логов для колледжа с использованием **Kafka**, **Kubernetes**, **MinIO** и **Grafana**.

## 📋 Оглавление

- [Архитектура](#-архитектура)
- [Технологии](#-технологии)
- [Требования](#-требования)
- [Быстрый старт](#-быстрый-старт)
- [Локальная разработка](#-локальная-разработка)
- [Деплой в Kubernetes](#-деплой-в-kubernetes)
- [Компоненты](#-компоненты)
  - [Коллектор](#-коллектор)
  - [Парсер](#-парсер)
  - [Архиватор](#-архиватор)
- [Мониторинг](#-мониторинг)
- [Структура проекта](#-структура-проекта)
- [Команды Makefile](#-команды-makefile)

## 🏗 Архитектура

[Эмулятор Windows Logs] → [Collector] → (raw-logs) → Kafka
↓
[Parser]
↓
(processed-logs) → Kafka
↓
[Archiver]
↓
MinIO (S3)
↓
[Grafana] ← Prometheus

ℹ️ **Важно:** В проекте используется **эмулятор** Windows-логов для демонстрации. В коде есть закомментированный модуль для реального сбора с Windows-серверов через WinRM, готовый к использованию в production.

## 🛠 Технологии

| Компонент | Технология |
|-----------|------------|
| Оркестрация | Kubernetes (Kind) |
| Потоковая обработка | Apache Kafka |
| Хранилище | MinIO (S3) |
| Мониторинг | Prometheus + Grafana |
| Автоматизация | Ansible |
| Контейнеризация | Docker |
| CI/CD | GitHub Actions |

## 📦 Требования

- Docker 20.10+
- Kubernetes (Kind / Minikube)
- Helm 3.0+
- Python 3.11+
- Ansible 2.9+

## 🚀 Быстрый старт

### 1. Клонирование репозитория

```bash
git clone https://github.com/yourusername/college-log-pipeline.git
cd college-log-pipeline

### 2. Сборка Docker образов

docker build -t log-collector:latest -f app/collector/Dockerfile .
docker build -t log-parser:latest -f app/parser/Dockerfile .
docker build -t log-archiver:latest -f app/archiver/Dockerfile .


### 3. Создание Kind кластера и загрузка образов

#Создание кластера
make create-cluster

#Проверка
kubectl cluster-info
kubectl get nodes

# Загрузка образов в Kind
kind load docker-image log-collector:latest --name log-pipeline
kind load docker-image log-parser:latest --name log-pipeline
kind load docker-image log-archiver:latest --name log-pipeline

#Деплой всех компонентов
make deploy-all
make status

#Вывод должен быть таким
NAME                         READY   STATUS    RESTARTS   AGE
archiver-xxx                 1/1     Running   0          1m
collector-xxx                1/1     Running   0          1m
kafka-xxx                    1/1     Running   0          2m
minio-xxx                    1/1     Running   0          1m
parser-xxx                   1/1     Running   0          1m
zookeeper-0                  1/1     Running   0          2m

# Деплой мониторинга
make deploy-monitoring

Примечание: kube-state-metrics в составе стека может не запускаться в Kind из-за ограничений ресурсов. Он не является критичным для работы системы. Если под не запускается — просто проигнорируйте или отключите его через --set kube-state-metrics.enabled=false.


#После деплоя приложений некоторые сервисы работают внутри кластера. Чтобы получить к ним доступ из браузера или приложений, нужно пробросить порты:


# Kafka UI (веб-интерфейс для просмотра сообщений)
kubectl port-forward -n log-pipeline service/kafka-ui 8080:8080

#MinIO Console (управление хранилищем)
kubectl port-forward -n log-pipeline service/minio 9001:9001

#Grafana (дашборды мониторинга)
kubectl port-forward -n monitoring service/monitoring-grafana 3000:80

#Сервисы будут доступны по следующим адресам
Сервис	        URL	                Логин/Пароль
Kafka UI	http://localhost:8080	-
MinIO Console	http://localhost:9001	minioadmin / minioadmin
Grafana	        http://localhost:3000	admin / admin (пароль меняется после ввода этого)

# Принцип работы компонентов
Коллектор собирает логи из эмулятора и отправляет их в Kafka. 
Парсер читает логи из Kafka, анализирует, ищет ошибки и отпавляет алерты. Протестировано локально
Архиватор сохраняет раз в час логи из Kafka в MiniIO в сжатом формате .json.gz

