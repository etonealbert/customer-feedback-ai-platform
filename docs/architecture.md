# Architecture

## Durable acceptance and asynchronous processing

The API validates a maximum 10,000-character feedback body, authenticates a bearer key against a stored SHA-256 digest, and starts a transaction with `app.tenant_id` set using transaction-local PostgreSQL configuration. Feedback and a pointer-only outbox event are inserted together. HTTP 202 means that transaction committed, not that Kafka or inference has finished.

Outbox publishers lock batches with `FOR UPDATE SKIP LOCKED`. They send each event with a stable feedback-ID partition key, Kafka `acks=all`, and producer idempotence, then mark it published in the same database transaction. PostgreSQL cannot atomically commit with Kafka: a publisher crash after broker acknowledgement can replay an event. This is an intentional at-least-once boundary.

Workers disable Kafka auto-commit. They authenticate to inference, validate the prediction shape and version, and transactionally insert the prediction and mark feedback completed before acknowledging the exact partition offset. `(feedback_id, model_version)` is unique. A crash after database commit but before Kafka commit therefore yields a harmless replay. Different model versions retain distinct prediction history.

Inference failures retry with exponential backoff and a durable per-feedback/version attempt count. After four attempts, the failure status, dead-letter record, and DLQ outbox row are written transactionally before the source offset is committed. A database failure cannot be acknowledged; the supervisor reconnects and consumes from the last committed offset. Invalid event envelopes are dead-lettered with location/size metadata, without copying arbitrary unvalidated content.

The design promises idempotent **database effects**, not global exactly-once execution. Inference may run more than once. Six topic partitions bound active worker parallelism. A slow or failing event temporarily blocks its partition. Workers fetch one record per poll, bound each HTTP inference attempt to ten seconds total, and size the poll interval above the configured retry budget. Database statement/lock timeouts bound query waits. Kafka client shutdown has a five-second deadline; if flushing cannot finish during broker loss, the service exits so its orchestrator can restart it. Durable rows and offsets remain the recovery boundary.

## Tenant boundary

Public endpoints never accept a tenant ID. Bearer authentication establishes one. Queries explicitly filter that tenant, and `feedback`, `predictions`, and `processing_attempts` also have forced PostgreSQL row-level security. The application role is neither owner nor superuser and has no `BYPASSRLS`. Transaction-local context prevents pooled connections from retaining another tenant's identity. Feedback/prediction composite foreign keys prevent mismatched tenant references.

The app role can read hashed keys and internal queue metadata. Internal processes share that role for this miniature platform; a compromised internal service can deliberately choose another tenant context. RLS here defends query mistakes and untrusted API consumers, not fully compromised service credentials. Production should split service roles, enforce Kafka ACLs and network policies, and separate migration/admin privileges. Admin credentials only enter the migration job and explicit local test tooling.

## Retrieval and grounded answers

The worker stores the encoder version beside each vector. RAG uses the same encoder, checks its version and vector shape, and performs exact cosine search within the authenticated tenant and selected prediction version. Small datasets benefit from exact search's recall and simplicity. No approximate vector index is claimed. Empty/unknown-vocabulary queries abstain; low-scoring matches are filtered.

Topic and sentiment population counts come from `/v1/insights`. Retrieval-based answer counts describe only the selected sample. The default answer includes source excerpts; the optional Responses API receives only these tenant-filtered sources, no tools, and explicit untrusted-evidence instructions. Citation range checking detects nonexistent sources but cannot prove every claim. Provider errors fall back visibly to extractive mode.

## Operational boundary

Each service has its own process, deployment, health checks, metrics and structured logs. HTTP metric route labels use route templates to avoid feedback-ID cardinality. Logs avoid feedback content and API keys. Prometheus exposes request rate, error rate, histogram p95, event age, inference latency, outbox backlog, and consumer lag. Docker stats are sampled separately during benchmarks.

The local Kafka broker and PostgreSQL instance use persistent volumes but are single replicas; they survive container restarts, not disk loss. Kafka retention is seven days for feedback and thirty days for DLQ. The PostgreSQL outbox is retained for audit and must eventually be pruned/archived in production. Kubernetes manifests preserve this deliberate development topology. CI tests Docker services and validates manifests; schema validation alone is not a deployed-cluster test.
