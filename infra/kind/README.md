# Local Kubernetes (kind) + Airflow

The **default** orchestrator is `local_sequential` (in-process Python). This folder
is for the opt-in `airflow_kind` mode — useful when you want to see the real
Airflow UI, test KubernetesExecutor behavior, or demo end-to-end with the same
orchestrator class as AKS prod.

## Prerequisites

All three must be on your PATH in WSL2:

```bash
# kind (Kubernetes-in-Docker)
curl -Lo ./kind https://kind.sigs.k8s.io/dl/v0.23.0/kind-linux-amd64
chmod +x ./kind && sudo mv ./kind /usr/local/bin/

# kubectl
curl -LO "https://dl.k8s.io/release/v1.30.0/bin/linux/amd64/kubectl"
chmod +x ./kubectl && sudo mv ./kubectl /usr/local/bin/

# helm
curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash
```

## Bring it up

```bash
make up-airflow       # creates kind cluster + installs Airflow via Helm
# Airflow UI → http://localhost:8080  (admin / admin_local_only)
```

## Tear it down

```bash
make down-airflow     # deletes kind cluster (and everything in it)
```

## What's in this folder

- `cluster.yaml` — kind cluster spec (1 control-plane node, port mappings for Airflow + webhook-stub).
- `../helm/airflow-values.yaml` — Helm values override for the official Airflow chart.
