# Incident runbook and reliability experiments

These commands affect only this repository's Docker Compose project. Run them from the
repository root. They retain volumes and accepted feedback. The development cluster is
single-node; host/disk loss and a Kafka volume loss are outside its availability guarantee.
Do not use `docker compose down -v` as a recovery action.

## First observations

```sh
docker compose ps
docker compose logs --tail 100 ingestion outbox worker inference rag
docker compose exec -T postgres psql -U postgres -d feedback -c "SELECT status,count(*) FROM feedback GROUP BY status;"
docker compose exec -T postgres psql -U postgres -d feedback -c "SELECT count(*) AS unpublished FROM outbox WHERE published_at IS NULL;"
docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --group feedback-workers
```

Use Grafana to distinguish HTTP failures, outbox buildup, Kafka lag, and slow inference.
Liveness answers whether the process is alive; readiness answers whether it can serve its
role. Kafka unavailability need not make ingestion unready because accepted feedback and
its event are committed together in PostgreSQL. Database failure must fail ingestion
closed: clients should receive HTTP 503 and retry with the same `Idempotency-Key`.

## Automated experiments

Run the whole suite, or one scenario while investigating a regression:

```sh
python scripts/reliability.py --count 60 --output docs/evidence/reliability-local.json
python scripts/reliability.py --scenario worker --output docs/evidence/worker-crash.json
python scripts/reliability.py --scenario kafka --output docs/evidence/kafka-outage.json
python scripts/reliability.py --scenario database --output docs/evidence/database-outage.json
python scripts/reliability.py --scenario unhealthy-model --output docs/evidence/unhealthy-model.json
python scripts/reliability.py --scenario model-rollout --output docs/evidence/model-rollout.json
```

The runner restores stopped services in `finally` blocks and stops the suite on the first
failed experiment. If interrupted by a hard process termination or host shutdown, run:

```sh
docker compose up -d postgres kafka inference
docker compose start worker
docker compose ps
```

Keep both failed and successful evidence. A failure is diagnostic data, not a result to
replace silently. The JSON includes the actual observations and no generated success
claims. No cloud account, LLM provider key, or GPU is needed for these exercises.

## Worker crashes and duplicate delivery

The worker experiment stops consumers, accepts a known backlog, starts workers, sends
SIGKILL while processing, then restarts them. Every accepted ID must reach `completed`.
The database audit requires one feedback row and exactly one prediction per model version;
redeliveries must not create duplicate results. This timing-based test cannot guarantee
that the kill landed between a PostgreSQL commit and a Kafka offset commit; deterministic
idempotency tests complement it.

```sh
docker compose kill -s SIGKILL worker
docker compose start worker
docker compose logs --tail 100 worker
```

If Kafka lag grows after restart, check inference readiness, database connectivity and
consumer-group partition assignments. More worker replicas than partitions cannot improve
partition-level concurrency. Do not reset consumer offsets just to clear lag: that can
skip accepted work or replay history unexpectedly.

## Kafka unavailable

The Kafka experiment stops the broker and submits feedback. HTTP 202 confirms durable
acceptance into PostgreSQL; those items should remain `pending` while the broker is down.
Starting Kafka must let the outbox drain and all accepted IDs finish. The outbox publishes
with acknowledgement before marking an event published. A crash in that small gap may
publish twice; the worker's unique prediction key absorbs the duplicate.

```sh
docker compose start kafka
docker compose logs --tail 100 kafka outbox worker
```

Before scaling a production deployment, configure replicated Kafka topics, suitable
retention, alerts for oldest outbox item, and backups for PostgreSQL. The local single
broker is intentionally not described as highly available.

## Database unavailable

The database experiment stops PostgreSQL and requires ingestion and readiness to return
503, then restores PostgreSQL and confirms new work completes. A client with an uncertain
response should retry the same payload with the same idempotency key; a changed payload
with that key must receive 409. The idempotency key is scoped to the authenticated tenant.

```sh
docker compose start postgres
docker compose logs --tail 100 postgres ingestion worker outbox
```

Do not run schema bootstrap with a new application password against an existing deployment
unless the application secret is updated consistently. Keep the application DB role
restricted; running the APIs as PostgreSQL superuser bypasses the intended isolation.

## Unhealthy model deployment and rollback

The automated Compose experiment pauses workers, replaces inference with an invalid
`MODEL_VERSION`, observes its failed readiness/startup, accepts pending work, restores the
previous service configuration, and resumes workers. It checks completion and uniqueness.
The pause prevents a deliberately bad model from exhausting retries while demonstrating
that accepted feedback can wait safely. This tests replacement/recovery, not Kubernetes
rolling-update availability.

The `model-rollout` scenario processes one item with v1, reprocesses it with v2, and rolls
it back to v1. `/v1/feedback/{id}/predictions` must retain both immutable predictions.
Compare the actual outputs and held-out model evaluation; a version increment alone does
not establish an improvement.

For a running Kubernetes development namespace, test readiness protection explicitly:

```sh
kubectl -n feedback-ai set env deployment/inference MODEL_VERSION=definitely-invalid
kubectl -n feedback-ai rollout status deployment/inference --timeout=90s
kubectl -n feedback-ai describe deployment/inference
kubectl -n feedback-ai rollout undo deployment/inference
kubectl -n feedback-ai rollout status deployment/inference --timeout=120s
```

Verify the namespace in [Kubernetes operations](kubernetes.md) before running. A failed
rollout should keep old ready replicas available when resources and rolling-update policy
allow. Keep this scenario marked untested until a real cluster run has produced evidence.

## Poison events, failed predictions, and tenant isolation

Inspect failed feedback and dead letters before replaying anything. Never change tenant
IDs to get around authentication or row-level security. Events carry identifiers; the
worker loads the feedback under the event's tenant context. Invalid messages must be
retained in the dead-letter path, and repeated transient failures must not silently
advance offsets before a durable outcome exists.

```sh
docker compose exec -T postgres psql -U postgres -d feedback -c "SELECT event_id,error_type,created_at FROM dead_letters ORDER BY created_at DESC LIMIT 20;"
docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic feedback.dlq.v1 --from-beginning --max-messages 10 --timeout-ms 10000
```

After correcting a failing dependency or model, the tenant-authenticated
`POST /v1/feedback/{id}/reprocess` endpoint with `{"model_version":"v1"}` can enqueue a new
attempt for a failed item. Existing successful predictions remain immutable; rolling
back to a previously computed version can reuse that result. Do not blindly republish
untrusted DLQ payloads or erase evidence to force retries.

Tenant isolation is checked at the API and PostgreSQL layers. Integration tests try
foreign feedback IDs, lists, search, answers, prediction history and reprocessing. They
also connect as the restricted `feedback_app` role and verify that missing tenant context
returns no rows and cross-tenant writes are denied. Credentials should never be pasted
into logs, screenshots, benchmark reports, or committed files.
