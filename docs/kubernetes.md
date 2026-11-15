# Kubernetes operations

The base is a **single-node development environment**, with persistent volumes for PostgreSQL and Kafka. It demonstrates Deployments, StatefulSets, Jobs, health probes, rolling updates, resource limits, and rollback without requiring cloud infrastructure. One broker with replication factor one is not highly available. Do not increase the Kafka StatefulSet replica count: that would require distinct node IDs, a controller quorum, new advertised listeners, and replication settings.

## Local deployment

Use Docker Desktop Kubernetes, kind, or minikube with a default StorageClass. Allow roughly 6 GB RAM for the complete stack. `kubectl` must point at the intended development cluster.

```sh
python scripts/setup_env.py
docker build -t feedback-ai:local -f infrastructure/docker/Dockerfile .
# kind only: kind load docker-image feedback-ai:local
# minikube only: minikube image load feedback-ai:local
python infrastructure/kubernetes/create_secret.py --env-file .env
kubectl apply -k infrastructure/kubernetes/base
kubectl -n feedback-ai wait --for=condition=complete job/feedback-migrate job/feedback-kafka-init --timeout=300s
kubectl -n feedback-ai rollout status deployment/ingestion --timeout=300s
kubectl -n feedback-ai rollout status deployment/worker --timeout=300s
kubectl -n feedback-ai port-forward service/ingestion 8000:8000
# In a second terminal:
kubectl -n feedback-ai port-forward service/rag 8002:8000
```

Secrets are generated locally, not committed. The bootstrap Job alone receives the database admin connection. Application deployments select only their runtime connection and service token from the Secret. PostgreSQL application access uses a non-superuser role with row-level security. A Kubernetes Secret is base64 encoded, not encrypted by default; configure encryption at rest and scoped RBAC before using a shared cluster.

Startup order is handled by retries and probes; a few initial restarts while the bootstrap Job completes are expected. Readiness removes unavailable pods from Service routing. Liveness checks process health and should not make a dependency outage trigger a restart loop.

## Scale and inspect

```sh
kubectl -n feedback-ai scale deployment/worker --replicas=3
kubectl -n feedback-ai get pods -o wide
kubectl -n feedback-ai top pods
kubectl -n feedback-ai describe deployment/worker
kubectl -n feedback-ai logs deployment/worker --tail=100
kubectl -n feedback-ai exec kafka-0 -- /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --group feedback-workers --describe
```

`kubectl top` and the optional CPU HPAs require metrics-server. Apply `infrastructure/kubernetes/optional/autoscaling.yaml` to autoscale workers from one to six replicas and inference from one to four. Six event partitions cap useful concurrent consumers at six. CPU-based worker scaling is educational: inference-bound consumers can lag while using little CPU. A production installation should use Kafka consumer lag with KEDA and tune partition count against database and inference capacity.

## Rolling updates and rollback

Use immutable `sha-<full commit SHA>` image tags from GHCR. The same application image is run with different commands, so each service can be rolled independently.

```sh
kubectl -n feedback-ai set image deployment/inference inference=ghcr.io/etonealbert/customer-feedback-ai-platform:sha-<tested-commit>
kubectl -n feedback-ai rollout status deployment/inference --timeout=180s
kubectl -n feedback-ai rollout history deployment/inference
kubectl -n feedback-ai rollout undo deployment/inference
kubectl -n feedback-ai rollout status deployment/inference
```

Deployments allow one surge replica and zero unavailable replicas. An unhealthy replacement fails readiness while the previous ready replica continues serving. Rollback restores a deployment pod template; it does not reverse database schema changes. Schema initialization is additive and idempotent in this version. Future migrations need expand-and-contract compatibility and backups.

Model selection is explicit on feedback events, so replay can reproduce the chosen version. Keep both supported versions available while old events remain in Kafka. See the model evaluation report before changing the ingestion default. Change an environment value with `kubectl set env deployment/ingestion MODEL_VERSION=v2`; roll it back with `kubectl rollout undo deployment/ingestion`. Selecting a version is separate from deploying a new image that implements it.

For subsequent releases, recreate the completed bootstrap Jobs before applying manifests because Kubernetes Job pod templates are immutable:

```sh
kubectl -n feedback-ai delete job feedback-migrate feedback-kafka-init
kubectl apply -k infrastructure/kubernetes/base
```

## Monitoring

The Compose deployment provisions Prometheus and Grafana automatically. Kubernetes pods carry Prometheus scrape annotations; configure your cluster Prometheus to discover them, or use the optional namespace-local monitoring manifests described in `infrastructure/kubernetes/monitoring/README.md`. Metrics definitions and dashboard queries are under `monitoring/`.

The dashboard includes throughput, p95 HTTP latency, error rate, consumer offset lag, event processing age, outbox backlog, process CPU/memory, and inference latency. Offset lag and processing age answer different questions; an idle partition can have zero lag while its last completed event was old. An unpopulated chart during startup is absence of samples, not a measurement of zero.

## CI and an optional remote cluster

GitHub Actions runs lint, unit tests, real Compose integration tests, Compose validation, Kubernetes schema validation, and image builds. Successful main-branch runs publish immutable and `latest` tags to GHCR. Pull requests build without publishing.

The optional **Deploy tested image to configured Kubernetes cluster** workflow accepts a full tested commit SHA. Before using it, create a `staging` GitHub Environment, set `KUBE_CONFIG_B64` to a narrowly scoped kubeconfig, provision the `feedback-secrets` Secret, and make the GHCR package accessible to cluster nodes (public package or an image pull secret). Configure environment protection rules according to your deployment process. The workflow creates no cloud resources and is not evidence that a remote cluster has been tested.

For cloud use, prefer managed PostgreSQL with pgvector and a managed Kafka service. Replace the included stateful data services and connection settings. Add TLS, Kafka SASL/ACLs, NetworkPolicies, backups with restore drills, external secrets, database connection limits, and registry digest pinning. The bundled plaintext broker is intended only for an isolated development network; application tenant isolation is not a substitute for infrastructure access control.

## Optional KServe

The current inference server is deployed as a standard Kubernetes Deployment to keep the baseline small and CPU-only. It exposes a domain API (`/predict`, `/embed`), not the KServe V2 tensor protocol. Installing a KServe InferenceService pointing at that image without an adapter would be incorrect. A KServe extension needs a V2 protocol adapter, an artifact registry, and the KServe CRDs/controller; it is deliberately not presented as a tested capability here.

References: [Kafka Docker guide](https://kafka.apache.org/39/getting-started/docker/), [Kafka listener configuration](https://kafka.apache.org/39/security/listener-configuration/), [Kubernetes Deployments](https://kubernetes.io/docs/concepts/workloads/controllers/deployment/), [HPA](https://kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale/), and [kubeconform](https://github.com/yannh/kubeconform).
