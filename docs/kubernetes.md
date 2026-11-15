# Kubernetes operations

The base is a **single-node development environment**, with persistent volumes for PostgreSQL and Kafka. It demonstrates Deployments, StatefulSets, Jobs, health probes, rolling updates, resource limits, and rollback without requiring cloud infrastructure. One broker with replication factor one is not highly available. Do not increase the Kafka StatefulSet replica count: that would require distinct node IDs, a controller quorum, new advertised listeners, and replication settings.

## Executed local verification

On November 15, 2026, the base was deployed to a dedicated kind v0.33.0 cluster running Kubernetes v1.37.0. The [raw smoke report](evidence/kubernetes-local.json) records successful schema/topic initialization, healthy services, three available worker replicas, and real in-cluster ingestion, asynchronous prediction, idempotency, search, cited answers, and cross-tenant denial.

The test deliberately deployed `MODEL_VERSION=definitely-invalid`. The replacement failed startup and rollout did not complete; the old inference replica continued answering readiness with HTTP 200 and v1. Rollback restored the healthy deployment. The script restored one worker after testing. Initial deployment exposed a Kafka DNS readiness deadlock, fixed by `publishNotReadyAddresses: true` on its headless Service so the KRaft controller can resolve itself during bootstrap.

The separate [autoscaling report](evidence/autoscaling-local.json) records a real CPU HPA test: 8,236 successful synthetic inference calls and zero failed calls over a 60-second load window. The inference HPA changed desired replicas from one to four; all four became available. Metrics Server v0.8.1 supplied CPU/memory readings. Its `--kubelet-insecure-tls` setting was used only for kind's local test certificates. The test removed its optional HPAs and restored one inference/worker replica afterward. This verifies a scale-up event, not production sizing or sustained capacity; the load targets inference directly and is separate from feedback pipeline benchmarks.

Reproduce against an explicitly selected development cluster after deploying the base:

```sh
uv run python scripts/kubernetes_smoke.py --kubeconfig /path/to/development-kubeconfig
# Install metrics-server with the certificate configuration appropriate to your cluster first.
uv run python scripts/autoscaling_smoke.py --kubeconfig /path/to/development-kubeconfig
```

Run these experiments sequentially; an active HPA would override the smoke test's manual replica changes. Both scripts retain observed failures and attempt cleanup. The temporary verification cluster was used only for synthetic demo data; cloud and KServe controller deployment are separate, untested paths.

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

With Docker Desktop's containerd image store, importing a multi-platform manifest can fail with a missing content digest. A platform-specific archive worked in the verified local run:

```sh
docker image save --platform linux/amd64 --output feedback-app.tar feedback-ai:local
kind load image-archive feedback-app.tar --name YOUR_DEVELOPMENT_CLUSTER
```

Store this intermediate archive outside the repository; it is not a source artifact.

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

The optional **Deploy tested image to configured Kubernetes cluster** workflow accepts a full tested commit SHA. It checks the GitHub Actions API for a successful main-branch push run of `ci.yml` for that exact SHA (including image publication), then checks out that SHA before rendering the release manifests. Before using it, create a `staging` GitHub Environment, set `KUBE_CONFIG_B64` to a narrowly scoped kubeconfig, provision the `feedback-secrets` Secret, and make the GHCR package accessible to cluster nodes (public package or an image pull secret). Configure environment protection rules according to your deployment process. The workflow creates no cloud resources and is not evidence that a remote cluster has been tested.

For cloud use, prefer managed PostgreSQL with pgvector and a managed Kafka service. Replace the included stateful data services and connection settings. Add TLS, Kafka SASL/ACLs, NetworkPolicies, backups with restore drills, external secrets, database connection limits, and registry digest pinning. The bundled plaintext broker is intended only for an isolated development network; application tenant isolation is not a substitute for infrastructure access control.

## Optional KServe

The default inference server runs as a standard Kubernetes Deployment to keep the baseline small and CPU-only. It also implements a bounded subset of the KServe V2 HTTP protocol: `/v2/health/live`, `/v2/health/ready`, `/v2/models/{model_name}/ready`, and `/v2/models/{model_name}/infer`. Model names are `v1` and `v2`. Requests accept one `BYTES` tensor named `text`, shape `[batch_size]`, with at most 32 strings. Responses contain sentiment, confidence, and embedding tensors. Binary tensor payloads, gRPC, model metadata endpoints, and arbitrary tensor types are not implemented.

`infrastructure/kubernetes/optional/kserve.yaml` supplies an optional custom-container `InferenceService` in Standard mode. It requires an externally installed KServe controller and CRDs supporting that mode; it does not install them, and no KServe cluster deployment has been exercised for this repository. The manifest uses the same image and CPU model, listens on port 8080, and obtains the existing internal service token from the Secret. Replace the local image with a tested GHCR SHA tag on remote clusters.

```sh
# Only after installing KServe independently and provisioning the base Secret:
kubectl apply -f infrastructure/kubernetes/optional/kserve.yaml
kubectl -n feedback-ai get inferenceservice feedback-inference
kubectl -n feedback-ai get services -l serving.kserve.io/inferenceservice=feedback-inference
```

Use the generated predictor Service or the URL reported in the InferenceService status. Send `X-Service-Token` with the internal token; health endpoints are public to probes. The manifest adds a separate predictor and does not silently redirect the existing worker. To use it for normal processing, point the worker and RAG `INFERENCE_URL` at the generated predictor's HTTP address after validating `/predict` and `/embed`. Those domain endpoints remain available alongside V2. Undo that environment change to restore the baseline inference Deployment.

Example V2 JSON payload for `POST /v2/models/v1/infer`:

```json
{"inputs":[{"name":"text","shape":[1],"datatype":"BYTES","data":["The mobile app crashes during checkout"]}]}
```

See the [KServe custom predictor guide](https://kserve.github.io/website/docs/model-serving/predictive-inference/frameworks/custom-predictor) and [deployment mode configuration](https://kserve.github.io/website/docs/admin-guide/configurations). Standard mode uses ordinary Kubernetes workloads; metrics-server is needed for its CPU HPA.

References: [Kafka Docker guide](https://kafka.apache.org/39/getting-started/docker/), [Kafka listener configuration](https://kafka.apache.org/39/security/listener-configuration/), [Kubernetes Deployments](https://kubernetes.io/docs/concepts/workloads/controllers/deployment/), [HPA](https://kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale/), and [kubeconform](https://github.com/yannh/kubeconform).
