# ML worker and outbox publisher

Entrypoints: `python -m feedback_platform.worker` and `python -m feedback_platform.outbox`.

They use independent containers and the same versioned image. Six Kafka partitions allow up to six workers in one consumer group to process concurrently. Additional replicas provide standby capacity, not extra active partitions. Do not change the group ID during ordinary scaling or rollback; a new group intentionally replays the retained log.

Each process serves `/health/live`, `/health/ready`, and `/metrics` on internal port 8000. The worker receives a 45-second graceful shutdown window in Compose and Kubernetes. A durable database result precedes offset acknowledgement. Replayed events are safe because result insertion is idempotent.

Run `docker compose up -d --scale worker=3`; inspect `docker compose logs worker`. Kafka CLI tools are available inside the Kafka container under `/opt/kafka/bin/`.
