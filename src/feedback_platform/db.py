from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from psycopg import OperationalError
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool, PoolTimeout

from feedback_platform.settings import get_settings


def make_pool():
    settings = get_settings()
    return AsyncConnectionPool(
        settings.database_url, min_size=1, max_size=settings.db_pool_size,
        timeout=5, open=False, kwargs={"row_factory": dict_row, "connect_timeout": 5},
        check=AsyncConnectionPool.check_connection,
    )


@asynccontextmanager
async def tenant_connection(pool, tenant_id):
    async with pool.connection() as conn, conn.transaction():
        await conn.execute("SELECT set_config('app.tenant_id', %s, true)", (str(tenant_id),))
        yield conn


async def fetchone(conn, query, params=()):
    return await (await conn.execute(query, params)).fetchone()


async def fetchall(conn, query, params=()):
    return await (await conn.execute(query, params)).fetchall()


async def database_ready(pool):
    async with pool.connection() as conn:
        await conn.execute("SELECT 1")


def install_database_errors(app: FastAPI):
    async def unavailable(request: Request, exc):
        return JSONResponse(status_code=503, content={"detail": "Database temporarily unavailable"})

    app.add_exception_handler(OperationalError, unavailable)
    app.add_exception_handler(PoolTimeout, unavailable)
