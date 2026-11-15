import asyncio
import json
import time

from aiokafka import AIOKafkaProducer

from feedback_platform.daemon import daemon_app, serve
from feedback_platform.db import fetchall, fetchone
from feedback_platform.kafka import close_kafka
from feedback_platform.metrics import OUTBOX_PENDING
from feedback_platform.settings import get_settings


async def publish_batch(pool, producer):
    # Keep the lock until delivery is acknowledged and published_at is durable.
    # A crash between those two actions may redeliver; consumers are idempotent.
    async with pool.connection() as conn, conn.transaction():
        pending = await fetchone(
            conn, "SELECT count(*) AS count FROM outbox WHERE published_at IS NULL"
        )
        OUTBOX_PENDING.set(pending["count"])
        rows = await fetchall(
            conn,
            """SELECT * FROM outbox WHERE published_at IS NULL
            ORDER BY created_at LIMIT 20 FOR UPDATE SKIP LOCKED""",
        )
        for row in rows:
            await asyncio.wait_for(
                producer.send_and_wait(
                    row["topic"],
                    key=row["partition_key"].encode(),
                    value=json.dumps(row["payload"]).encode(),
                ),
                timeout=15,
            )
            await conn.execute(
                "UPDATE outbox SET published_at=now() WHERE event_id=%s", (row["event_id"],)
            )
        return len(rows)


async def run(app):
    settings = get_settings()
    producer = AIOKafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        enable_idempotence=True,
        acks="all",
        request_timeout_ms=10000,
    )
    try:
        await producer.start()
        while True:
            count = await publish_batch(app.state.pool, producer)
            app.state.last_tick = time.monotonic()
            if not count:
                await asyncio.sleep(0.25)
    finally:
        await close_kafka(producer)


app = daemon_app("outbox", run)

if __name__ == "__main__":
    serve(app)
