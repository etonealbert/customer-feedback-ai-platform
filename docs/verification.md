# Verification record

The repository contains executable checks and raw evidence. This is the scope actually verified locally on November 14–15, 2026; unexecuted paths are listed explicitly.

| Capability | Executed verification |
| --- | --- |
| Unit and integration checks | **34 passing tests**: 25 unit tests and 9 tests against real Docker services. Ruff passes. |
| Complete ingestion pipeline | HTTP 202 acceptance → durable outbox → Kafka → inference → PostgreSQL prediction and vector → status retrieval. |
| Idempotency | Concurrent HTTP retries reuse one feedback ID. Explicit Kafka redelivery is consumed and preserves one immutable prediction. |
| Multi-tenancy | Foreign IDs, lists, prediction history, reprocessing, search, and answers are isolated. Restricted-role SQL without tenant context sees zero rows; foreign writes are rejected. |
| Poison events | Invalid Kafka envelope is durably recorded and its DLQ outbox entry is published; arbitrary raw content is not copied into the DLQ. |
| Reliability | Five [Compose experiments](evidence/reliability-local.json), plus a [25-second Kafka outage](evidence/reliability-long-kafka-outage.json), all pass. |
| Model deployment | v1 → v2 → v1 prediction selection retains immutable history; invalid inference deployment fails startup and can be rolled back. Held-out synthetic evaluation is recorded separately. |
| Performance | 4,000 measured feedback items complete without failures. Actual throughput, p50/p95/p99, inference timing, and CPU/memory samples are in the [benchmark report](benchmarks.md). |
| Kubernetes | Actual kind deployment passes [in-cluster workflow, three-worker scaling, unhealthy rollout protection and rollback](evidence/kubernetes-local.json). |
| Autoscaling | CPU HPA scales inference from one to four available replicas under [8,236 successful synthetic inference calls](evidence/autoscaling-local.json), then test cleanup restores one. |
| Monitoring | Prometheus scrapes each application service and scaled worker; promtool validates configuration and five alert rules. Grafana provisions a 12-panel dashboard. |
| CI/CD | GitHub Actions runs lint, unit/integration tests, Kubernetes schemas, and image publication. Main commit `396ddb1` passed [CI and GHCR publication](https://github.com/etonealbert/customer-feedback-ai-platform/actions/runs/43660575548); subsequent commits trigger the same workflow. |

The final tests emit one upstream Starlette warning about its HTTPX TestClient integration; tests still pass. No test result or measurement was invented. The benchmark source revision and one-run sampling limits are retained, so later recovery improvements are not misrepresented as measured performance gains.

## Implemented but not deployed externally

- **Paid LLM generation:** Responses API request formation and citation validation are tested with a mock transport. Default extractive answers and real retrieval are exercised. No paid API call or external model-quality claim is made.
- **KServe controller:** the V2 protocol subset is unit-tested and an optional Standard-mode `InferenceService` manifest is provided. No KServe controller installation was performed.
- **Cloud deployment:** a manually triggered workflow verifies successful CI for the selected immutable image before deploying to a configured cluster. No cloud account, paid resources, or production cluster was provisioned.
- **Production resilience and ML quality:** single-node storage/broker topology is not highly available. The small synthetic corpus, short benchmark batches, and local fault drills do not establish production quality, capacity, or service-level objectives.
