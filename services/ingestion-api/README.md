# Ingestion API

Entrypoint: `uvicorn feedback_platform.ingestion:app --host 0.0.0.0 --port 8000`.

This service authenticates tenant API keys, validates feedback, and atomically writes feedback with its outbox event. Kafka downtime does not prevent acceptance while PostgreSQL remains available. The outbox publisher is a separate process so API response latency does not include broker acknowledgement.

The image is built from `infrastructure/docker/Dockerfile`. It runs as UID 10001. Liveness is `/health/live`, readiness is `/health/ready`, and Prometheus scrapes `/metrics`. See `/docs` on port 8000 for request schemas.
