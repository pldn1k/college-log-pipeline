.PHONY: help install-kind create-cluster delete-cluster deploy-kafka deploy-apps deploy-monitoring deploy-all status logs-collector logs-parser logs-archiver port-forward-grafana port-forward-minio port-forward-kafka-ui clean

help:
	@echo "Доступные команды:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

install-kind:
	curl -Lo ./kind https://kind.sigs.k8s.io/dl/v0.20.0/kind-linux-amd64
	chmod +x ./kind
	sudo mv ./kind /usr/local/bin/kind

create-cluster:
	kind create cluster --config kind-config.yaml --name log-pipeline

delete-cluster:
	kind delete cluster --name log-pipeline

deploy-kafka:
	kubectl apply -f k8s/namespace.yaml
	kubectl apply -f k8s/kafka/

deploy-apps:
	kubectl apply -f k8s/apps/collector-configmap.yaml
	kubectl apply -f k8s/apps/parser-configmap.yaml
	kubectl apply -f k8s/apps/collector-deployment.yaml
	kubectl apply -f k8s/apps/parser-deployment.yaml
	kubectl apply -f k8s/apps/archiver/deployment.yaml
	kubectl apply -f k8s/storage/

deploy-monitoring:
	helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
	helm repo update
	helm upgrade --install monitoring prometheus-community/kube-prometheus-stack \
          --namespace monitoring \
          --set kube-state-metrics.enabled=false \
          --set prometheus-node-exporter.enabled=true \
          -f monitoring-values.yaml

deploy-all: deploy-kafka deploy-apps

status:
	kubectl get pods -n log-pipeline
	kubectl get pods -n monitoring

logs-collector:
	kubectl logs -n log-pipeline deployment/collector --tail=50

logs-parser:
	kubectl logs -n log-pipeline deployment/parser --tail=50

logs-archiver:
	kubectl logs -n log-pipeline deployment/archiver --tail=50

port-forward-grafana:
	kubectl port-forward -n monitoring service/monitoring-grafana 3000:80

port-forward-minio:
	kubectl port-forward -n log-pipeline service/minio 9001:9001

port-forward-kafka-ui:
	kubectl port-forward -n log-pipeline service/kafka-ui 8080:8080

clean:
	kind delete cluster --name log-pipeline
	docker system prune -f
	
ci-lint:
	flake8 app/ --count --max-complexity=10 --statistics
	@echo "✅ Python код проверен (YAML пропущен)"
        
ci-build:        
	docker build -t log-collector:test -f app/collector/Dockerfile .
	docker build -t log-parser:test -f app/parser/Dockerfile .
	docker build -t log-archiver:test -f app/archiver/Dockerfile .
	
ci-all: ci-lint ci-build
	@echo "✅ Все проверки пройдены!"
	
