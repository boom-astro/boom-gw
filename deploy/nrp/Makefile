# boom-gw NRP deployment shortcuts. Modeled on boom-deploy-nrp.
#
# Manifests are split into three groups:
#   k8s/core/           — minimum to run boom-gw (mongo + gw-api + ingress).
#   k8s/ingestion/      — kafka + valkey + gw-clusterer + bayestar-service.
#   k8s/observability/  — otel-collector + prometheus + exporters.

NS ?= umn-babamul
SECRETS := k8s/secrets.yaml

define apply_dir
	@find $(1) -maxdepth 1 -name '*.yaml' ! -name '*.example.yaml' -print0 \
		| xargs -0 -I {} kubectl apply -n $(NS) -f {}
endef

define delete_dir
	@find $(1) -maxdepth 1 -name '*.yaml' ! -name '*.example.yaml' -print0 \
		| xargs -0 -I {} kubectl delete -n $(NS) --ignore-not-found -f {}
endef

.PHONY: help deploy deploy-secrets \
        deploy-core deploy-ingestion deploy-observability deploy-all \
        delete-core delete-ingestion delete-observability status

help:
	@echo "Targets:"
	@echo "  deploy               Alias of deploy-core."
	@echo "  deploy-secrets       Apply k8s/secrets.yaml (explicit; not run by deploy-core)."
	@echo "  deploy-core          Apply k8s/core/          (mongo, gw-api, ingress)."
	@echo "  deploy-ingestion     Apply k8s/ingestion/     (kafka, valkey, clusterer, bayestar)."
	@echo "  deploy-observability Apply k8s/observability/ (otel, prometheus, exporters)."
	@echo "  deploy-all           core + ingestion + observability."
	@echo "  delete-core          Tear down the core stack (data will be lost)."
	@echo "  delete-ingestion     Tear down the ingestion stack."
	@echo "  delete-observability Tear down the observability stack."
	@echo "  status               kubectl get pods,svc in the namespace."
	@echo ""
	@echo "Namespace: $(NS)  (override with: make deploy NS=other-ns)"

deploy-secrets:
	@test -f $(SECRETS) || { \
		echo "ERROR: $(SECRETS) is missing."; \
		echo "       cp k8s/secrets.example.yaml $(SECRETS) and fill it in."; \
		exit 1; }
	kubectl apply -n $(NS) -f $(SECRETS)

deploy: deploy-core

deploy-core:
	$(call apply_dir,k8s/core)

deploy-ingestion:
	$(call apply_dir,k8s/ingestion)

deploy-observability:
	$(call apply_dir,k8s/observability)

deploy-all: deploy-core deploy-ingestion deploy-observability

delete-core:
	$(call delete_dir,k8s/core)

delete-ingestion:
	$(call delete_dir,k8s/ingestion)

delete-observability:
	$(call delete_dir,k8s/observability)

status:
	kubectl get pods,svc -n $(NS)
