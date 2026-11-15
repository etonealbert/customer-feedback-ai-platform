import asyncio
import logging
import time
from datetime import datetime, timezone
from uuid import NAMESPACE_URL, uuid5

import httpx
from aiokafka import AIOKafkaConsumer
from aiokafka.structs import TopicPartition
from psycopg.types.json import Jsonb
from pydantic import ValidationError

from feedback_platform.daemon import daemon_app, serve
from feedback_platform.db import fetchone, tenant_connection
from feedback_platform.metrics import CONSUMER_LAG, PROCESSING, PROCESSING_LAG, WORKER_RESULTS
from feedback_platform.schemas import Event, Prediction
from feedback_platform.settings import get_settings

logger = logging.getLogger(__name__)


def vector_literal(values):
    return "[" + ",".join(str(float(value)) for value in values) + "]"


async def dead_letter(pool, event_id, payload, error_type, event=None):
    settings = get_settings()
    dlq_id = uuid5(NAMESPACE_URL, f"feedback-dlq:{event_id}")
    async with pool.connection() as conn, conn.transaction():
        if event is not None:
            await conn.execute(
                "SELECT set_config('app.tenant_id',%s,true)", (str(event.tenant_id),)
            )
            await conn.execute(
                """UPDATE feedback SET status='failed',updated_at=now()
                WHERE id=%s AND tenant_id=%s AND model_version=%s AND status='pending'""",
                (event.feedback_id, event.tenant_id, event.model_version),
            )
        envelope = {"event_id": str(event_id), "event": payload, "error_type": error_type}
        await conn.execute(
            "INSERT INTO dead_letters(event_id,payload,error_type) VALUES(%s,%s,%s) ON CONFLICT DO NOTHING",
            (dlq_id, Jsonb(envelope), error_type),
        )
        await conn.execute(
            "INSERT INTO outbox(event_id,topic,partition_key,payload) VALUES(%s,%s,%s,%s) ON CONFLICT DO NOTHING",
            (dlq_id, settings.kafka_dlq_topic, str(event_id), Jsonb(envelope)),
        )
    WORKER_RESULTS.labels("dead_letter").inc()


async def process_event(pool, client, event):
    settings = get_settings()
    while True:
        async with tenant_connection(pool, event.tenant_id) as conn:
            row = await fetchone(
                conn,
                "SELECT * FROM feedback WHERE id=%s AND tenant_id=%s",
                (event.feedback_id, event.tenant_id),
            )
            if row is None:
                # A fabricated or stale pointer must never let another tenant's row be fetched.
                return "missing"
            exists = await fetchone(
                conn,
                "SELECT 1 FROM predictions WHERE feedback_id=%s AND tenant_id=%s AND model_version=%s",
                (event.feedback_id, event.tenant_id, event.model_version),
            )
            if exists:
                return "duplicate"
            attempt = await fetchone(
                conn,
                """INSERT INTO processing_attempts(feedback_id,tenant_id,model_version,attempts)
                VALUES(%s,%s,%s,1) ON CONFLICT(feedback_id,model_version)
                DO UPDATE SET attempts=processing_attempts.attempts+1 RETURNING attempts""",
                (event.feedback_id, event.tenant_id, event.model_version),
            )
        if attempt["attempts"] > settings.max_attempts:
            await dead_letter(
                pool, event.event_id, event.model_dump(mode="json"), "RetriesExhausted", event
            )
            return "failed"
        try:
            response = await client.post(
                settings.inference_url + "/predict",
                headers={"X-Service-Token": settings.internal_api_key},
                json={"text": row["text"], "model_version": event.model_version},
            )
            response.raise_for_status()
            result = Prediction.model_validate(response.json())
            if (
                result.model_version != event.model_version
                or result.embedding_version != settings.embedding_version
            ):
                raise ValueError("Inference version mismatch")
        except (httpx.HTTPError, ValidationError, ValueError) as exc:
            # Record only the type: provider errors could contain customer text.
            async with tenant_connection(pool, event.tenant_id) as conn:
                await conn.execute(
                    "UPDATE processing_attempts SET last_error=%s WHERE feedback_id=%s AND model_version=%s",
                    (type(exc).__name__, event.feedback_id, event.model_version),
                )
            WORKER_RESULTS.labels("retry").inc()
            if attempt["attempts"] >= settings.max_attempts:
                await dead_letter(
                    pool, event.event_id, event.model_dump(mode="json"), type(exc).__name__, event
                )
                return "failed"
            await asyncio.sleep(
                min(10, settings.retry_base_seconds * 2 ** (attempt["attempts"] - 1))
            )
            continue
        async with tenant_connection(pool, event.tenant_id) as conn:
            await conn.execute(
                """INSERT INTO predictions
                (feedback_id,tenant_id,model_version,sentiment,confidence,topics,embedding,embedding_version,inference_ms)
                VALUES(%s,%s,%s,%s,%s,%s,%s::vector,%s,%s) ON CONFLICT(feedback_id,model_version) DO NOTHING""",
                (
                    event.feedback_id,
                    event.tenant_id,
                    result.model_version,
                    result.sentiment,
                    result.confidence,
                    result.topics,
                    vector_literal(result.embedding),
                    result.embedding_version,
                    result.inference_ms,
                ),
            )
            await conn.execute(
                """UPDATE feedback SET status='completed',updated_at=now()
                WHERE id=%s AND tenant_id=%s AND model_version=%s""",
                (event.feedback_id, event.tenant_id, event.model_version),
            )
        PROCESSING_LAG.observe((datetime.now(timezone.utc) - row["created_at"]).total_seconds())
        return "completed"


async def process_record(pool, client, record):
    try:
        event = Event.model_validate_json(record.value)
    except (ValidationError, ValueError):
        stable_id = uuid5(NAMESPACE_URL, f"{record.topic}:{record.partition}:{record.offset}")
        # Store location and size, not arbitrary unvalidated content or secrets.
        await dead_letter(
            pool,
            stable_id,
            {
                "topic": record.topic,
                "partition": record.partition,
                "offset": record.offset,
                "bytes": len(record.value or b""),
            },
            "InvalidEvent",
        )
        return "invalid"
    return await process_event(pool, client, event)


async def run(app):
    settings = get_settings()
    consumer = AIOKafkaConsumer(
        settings.kafka_topic,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=settings.kafka_group_id,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        max_poll_records=10,
        max_poll_interval_ms=300000,
    )
    try:
        await consumer.start()
        async with httpx.AsyncClient(timeout=10) as client:
            while True:
                batches = await consumer.getmany(timeout_ms=1000, max_records=10)
                app.state.last_tick = time.monotonic()
                for partition, records in batches.items():
                    for record in records:
                        with PROCESSING.time():
                            outcome = await process_record(app.state.pool, client, record)
                        # This is the only acknowledgement path. DB/DLQ persistence
                        # must succeed first. On commit failure reconnect and replay.
                        await consumer.commit(
                            {TopicPartition(record.topic, record.partition): record.offset + 1}
                        )
                        WORKER_RESULTS.labels(outcome).inc()
                        app.state.last_tick = time.monotonic()
                    highwater = consumer.highwater(partition)
                    committed = await consumer.committed(partition)
                    if highwater is not None and committed is not None:
                        CONSUMER_LAG.labels(str(partition.partition)).set(
                            max(0, highwater - committed)
                        )
    finally:
        await consumer.stop()


app = daemon_app("worker", run)

if __name__ == "__main__":
    serve(app)
