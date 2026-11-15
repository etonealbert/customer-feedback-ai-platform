import hashlib
import json
from contextlib import asynccontextmanager
from uuid import UUID, uuid4

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from psycopg.types.json import Jsonb

from feedback_platform.auth import tenant_auth
from feedback_platform.db import (
    database_ready,
    fetchall,
    fetchone,
    install_database_errors,
    make_pool,
    tenant_connection,
)
from feedback_platform.logging import configure_logging
from feedback_platform.metrics import install_metrics
from feedback_platform.schemas import FeedbackIn, ReprocessIn
from feedback_platform.settings import get_settings


@asynccontextmanager
async def lifespan(app):
    configure_logging(get_settings().log_level)
    app.state.pool = make_pool()
    await app.state.pool.open(wait=True)
    yield
    await app.state.pool.close()


app = FastAPI(title="Feedback ingestion API", version="0.1.0", lifespan=lifespan)
install_metrics(app, "ingestion")
install_database_errors(app)


async def feedback_view(conn, feedback_id, tenant_id):
    row = await fetchone(
        conn, "SELECT * FROM feedback WHERE id=%s AND tenant_id=%s", (feedback_id, tenant_id)
    )
    if row is None:
        raise HTTPException(404, "Feedback not found")
    row.pop("request_hash")
    row.pop("idempotency_key")
    row["prediction"] = await fetchone(
        conn,
        """
        SELECT sentiment,confidence,topics,model_version,embedding_version,inference_ms,created_at
        FROM predictions WHERE feedback_id=%s AND tenant_id=%s AND model_version=%s
    """,
        (feedback_id, tenant_id, row["model_version"]),
    )
    return row


async def enqueue(conn, feedback_id, tenant_id, version):
    event_id = uuid4()
    payload = {
        "event_id": str(event_id),
        "feedback_id": str(feedback_id),
        "tenant_id": str(tenant_id),
        "model_version": version,
    }
    await conn.execute(
        """
        INSERT INTO outbox(event_id,topic,partition_key,payload) VALUES(%s,%s,%s,%s)
    """,
        (event_id, get_settings().kafka_topic, str(feedback_id), Jsonb(payload)),
    )


@app.post("/v1/feedback", status_code=202)
async def create_feedback(
    body: FeedbackIn,
    request: Request,
    tenant_id: UUID = Depends(tenant_auth),
    idempotency_key: str | None = Header(default=None, min_length=1, max_length=128),
):
    request_hash = hashlib.sha256(
        json.dumps(body.model_dump(), sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    key = idempotency_key or str(uuid4())
    version = get_settings().model_version
    if version not in ("v1", "v2"):
        raise HTTPException(503, "Configured model version is unavailable")
    async with tenant_connection(request.app.state.pool, tenant_id) as conn:
        row = await fetchone(
            conn,
            """
            INSERT INTO feedback(id,tenant_id,idempotency_key,request_hash,text,source,metadata,status,model_version)
            VALUES(%s,%s,%s,%s,%s,%s,%s,'pending',%s)
            ON CONFLICT (tenant_id,idempotency_key) DO NOTHING RETURNING id
        """,
            (
                uuid4(),
                tenant_id,
                key,
                request_hash,
                body.text,
                body.source,
                Jsonb(body.metadata),
                version,
            ),
        )
        if row:
            await enqueue(conn, row["id"], tenant_id, version)
        else:
            row = await fetchone(
                conn,
                "SELECT id,request_hash FROM feedback WHERE tenant_id=%s AND idempotency_key=%s",
                (tenant_id, key),
            )
            if row["request_hash"] != request_hash:
                raise HTTPException(409, "Idempotency-Key was already used for a different request")
        return await feedback_view(conn, row["id"], tenant_id)


@app.get("/v1/feedback/{feedback_id}")
async def get_feedback(feedback_id: UUID, request: Request, tenant_id: UUID = Depends(tenant_auth)):
    async with tenant_connection(request.app.state.pool, tenant_id) as conn:
        return await feedback_view(conn, feedback_id, tenant_id)


@app.get("/v1/feedback")
async def list_feedback(
    request: Request,
    tenant_id: UUID = Depends(tenant_auth),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0, le=100000),
):
    async with tenant_connection(request.app.state.pool, tenant_id) as conn:
        rows = await fetchall(
            conn,
            "SELECT id FROM feedback WHERE tenant_id=%s ORDER BY created_at DESC,id LIMIT %s OFFSET %s",
            (tenant_id, limit, offset),
        )
        total = await fetchone(
            conn, "SELECT count(*) AS total FROM feedback WHERE tenant_id=%s", (tenant_id,)
        )
        items = [await feedback_view(conn, r["id"], tenant_id) for r in rows]
        return {"items": items, "total": total["total"]}


@app.get("/v1/feedback/{feedback_id}/predictions")
async def list_predictions(
    feedback_id: UUID, request: Request, tenant_id: UUID = Depends(tenant_auth)
):
    async with tenant_connection(request.app.state.pool, tenant_id) as conn:
        await feedback_view(conn, feedback_id, tenant_id)
        rows = await fetchall(
            conn,
            """SELECT sentiment,confidence,topics,model_version,embedding_version,inference_ms,created_at
            FROM predictions WHERE feedback_id=%s AND tenant_id=%s ORDER BY created_at""",
            (feedback_id, tenant_id),
        )
        return {"items": rows}


@app.post("/v1/feedback/{feedback_id}/reprocess", status_code=202)
async def reprocess(
    feedback_id: UUID, body: ReprocessIn, request: Request, tenant_id: UUID = Depends(tenant_auth)
):
    async with tenant_connection(request.app.state.pool, tenant_id) as conn:
        row = await fetchone(
            conn,
            "SELECT id FROM feedback WHERE id=%s AND tenant_id=%s FOR UPDATE",
            (feedback_id, tenant_id),
        )
        if row is None:
            raise HTTPException(404, "Feedback not found")
        existing = await fetchone(
            conn,
            "SELECT 1 FROM predictions WHERE feedback_id=%s AND model_version=%s",
            (feedback_id, body.model_version),
        )
        await conn.execute(
            "UPDATE feedback SET model_version=%s,status=%s,updated_at=now() WHERE id=%s AND tenant_id=%s",
            (body.model_version, "completed" if existing else "pending", feedback_id, tenant_id),
        )
        if not existing:
            await conn.execute(
                "UPDATE processing_attempts SET attempts=0,last_error=NULL WHERE feedback_id=%s AND model_version=%s",
                (feedback_id, body.model_version),
            )
            await enqueue(conn, feedback_id, tenant_id, body.model_version)
        return await feedback_view(conn, feedback_id, tenant_id)


@app.get("/v1/insights")
async def insights(request: Request, tenant_id: UUID = Depends(tenant_auth)):
    async with tenant_connection(request.app.state.pool, tenant_id) as conn:
        statuses = await fetchall(
            conn,
            "SELECT status,count(*) AS count FROM feedback WHERE tenant_id=%s GROUP BY status",
            (tenant_id,),
        )
        sentiments = await fetchall(
            conn,
            """SELECT p.sentiment,count(*) AS count FROM predictions p JOIN feedback f
            ON f.id=p.feedback_id AND f.model_version=p.model_version WHERE f.tenant_id=%s GROUP BY p.sentiment""",
            (tenant_id,),
        )
        topics = await fetchall(
            conn,
            """SELECT topic,count(*) AS count FROM feedback f JOIN predictions p
            ON f.id=p.feedback_id AND f.model_version=p.model_version CROSS JOIN unnest(p.topics) AS topic
            WHERE f.tenant_id=%s GROUP BY topic ORDER BY count DESC""",
            (tenant_id,),
        )
    return {
        "total": sum(r["count"] for r in statuses),
        "by_status": {r["status"]: r["count"] for r in statuses},
        "by_sentiment": {r["sentiment"]: r["count"] for r in sentiments},
        "by_topic": {r["topic"]: r["count"] for r in topics},
    }


@app.get("/health/live")
async def live():
    return {"status": "ok"}


@app.get("/health/ready")
async def ready(request: Request):
    await database_ready(request.app.state.pool)
    return {"status": "ready"}
