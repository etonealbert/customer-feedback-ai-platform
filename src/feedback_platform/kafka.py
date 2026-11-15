import asyncio
import logging

logger = logging.getLogger(__name__)


async def close_kafka(client, timeout=5):
    """Never leave an idempotent producer flushing forever during broker loss.

    If the public client shutdown API cannot complete, terminate this service
    process so the orchestrator releases its transports and restarts it. The
    database outbox / uncommitted consumer offset remain the recovery boundary.
    """
    try:
        await asyncio.wait_for(client.stop(), timeout=timeout)
    except TimeoutError as exc:
        logger.error("Kafka shutdown exceeded deadline; restarting service process")
        raise SystemExit(1) from exc
