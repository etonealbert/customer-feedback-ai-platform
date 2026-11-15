"""Run supervised background work alongside HTTP probes and metrics."""

import asyncio
import logging
import time
from contextlib import asynccontextmanager, suppress

import uvicorn
from fastapi import FastAPI, HTTPException

from feedback_platform.db import database_ready, install_database_errors, make_pool
from feedback_platform.logging import configure_logging
from feedback_platform.metrics import install_metrics
from feedback_platform.settings import get_settings

logger = logging.getLogger(__name__)


def daemon_app(name, run):
    @asynccontextmanager
    async def lifespan(app):
        configure_logging(get_settings().log_level)
        app.state.pool = make_pool()
        app.state.last_tick = 0.0
        await app.state.pool.open(wait=True)

        async def supervise():
            while True:
                try:
                    await run(app)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    app.state.last_tick = 0.0
                    logger.exception("Background loop failed; reconnecting")
                    await asyncio.sleep(2)

        task = asyncio.create_task(supervise())
        try:
            yield
        finally:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
            await app.state.pool.close()

    app = FastAPI(title=f"Feedback {name}", lifespan=lifespan)
    install_metrics(app, name)
    install_database_errors(app)

    @app.get("/health/live")
    async def live():
        return {"status": "ok"}

    @app.get("/health/ready")
    async def ready():
        if time.monotonic() - app.state.last_tick > 60:
            raise HTTPException(503, "Background loop is not ready")
        await database_ready(app.state.pool)
        return {"status": "ready"}

    return app


def serve(app):
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level=get_settings().log_level.lower())
