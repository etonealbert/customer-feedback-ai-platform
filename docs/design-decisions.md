# Architecture decisions

## ADR 001 — Kafka and a transactional outbox

Kafka provides durable ordered partitions, replay, consumer groups, and independent scaling. It is useful when feedback bursts should not wait for inference and when operators need to replay a backlog. The cost is operational overhead and eventual consistency. A simpler queue would suffice for a smaller production requirement; this project uses Kafka to make those tradeoffs inspectable.

Directly writing PostgreSQL and then publishing to Kafka loses events if the process dies between writes. Publishing first can create work for feedback that never committed. The outbox fixes that dual-write gap. Its publisher can duplicate messages, so the consumer's unique database key supplies idempotent effects. See [Kafka delivery semantics](https://kafka.apache.org/20/design/design/).

## ADR 002 — When synchronous inference is appropriate

Use synchronous inference for an interactive action whose response must contain a prediction, when model latency is bounded and backpressure is acceptable. Internal `/predict` demonstrates this interface. Public feedback ingestion is asynchronous because durable acceptance and burst absorption matter more than an immediate label. A single request does not hold a database transaction open across inference.

## ADR 003 — Isolation in authentication, queries, and storage

Untrusted clients cannot select tenant identity. API keys map to tenants; all feedback reads include a tenant predicate; forced row-level security makes an accidentally omitted predicate fail closed. `SET LOCAL` scopes the setting to one transaction. RLS is not a replacement for internal credential separation, because a trusted service can choose a tenant setting. API keys are high-entropy random values, so indexed SHA-256 digests are appropriate; they are not human passwords.

## ADR 004 — Versioned CPU models and stable embeddings

Logistic regression and TF-IDF are fast, deterministic, explainable baselines. Training uses an intentionally small synthetic corpus and separately held-out examples. v2 changes classifier features/regularization, not the embedding coordinate system. Every event pins a model version and every prediction records it. A new encoder would require a new `embedding_version`, backfill, and dual retrieval/migration plan; vectors from incompatible spaces must never be compared.

An unhealthy model configuration fails inference startup. Kubernetes readiness prevents routing traffic to it, and `maxUnavailable: 0` preserves the previous replica during rollout. Model deployment rollback and selecting a previous feedback prediction are distinct operations. Compare held-out quality before changing the default; do not assume v2 is better just because it is newer.

## ADR 005 — pgvector, exact retrieval, and optional LLM

Keeping vectors with feedback gives one transactional store and one tenant policy. Exact cosine search is sufficient for this small dataset; approximate HNSW would trade recall for latency and require tenant-filter selectivity testing. [pgvector documents exact search and filtering behavior](https://github.com/pgvector/pgvector).

The default LSA encoder and extractive answer keep the entire demo offline after dependencies are installed. LLM generation is explicit opt-in because it sends customer evidence to an external service. The integration uses the [Responses API](https://developers.openai.com/api/reference/resources/responses/methods/create) with storage disabled, bounded output, no tools, and checked citation references. Prompt instructions and citation validation reduce risk but do not prove factual accuracy.

## ADR 006 — One image, independent services

One locked Python dependency graph and image minimize deployment drift. Each service has a separate command, health probe, scaling policy, and deployment. This is independent deployment, not independent dependency release cycles. Splitting images would reduce footprint and blast radius at larger scale; it adds little value for the demonstration.

## ADR 007 — Evidence before claims

Benchmarks retain raw samples, workload parameters and environment details. Poll-based completion latency includes observation delay and is labeled accordingly. CPU/memory are sampled container observations, not exact process attribution. Reliability scripts store observed results and restore stopped services. Cluster/cloud and paid-LLM paths are marked unverified until actually exercised; no synthetic timing is represented as a real benchmark.
