"""Acceptance tests against real FastAPI, Kafka, PostgreSQL and inference services."""

from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

import psycopg
import pytest

pytestmark = pytest.mark.integration


def submit(platform, text, tenant="a", key=None):
    headers = dict(platform[tenant])
    if key:
        headers["Idempotency-Key"] = key
    response = platform["client"].post(
        platform["api"] + "/v1/feedback",
        headers=headers,
        json={"text": text, "source": "mobile", "metadata": {"test": True}},
    )
    assert response.status_code == 202, response.text
    return response.json()


def test_end_to_end_prediction_and_insights(platform, wait_for_prediction):
    feedback = submit(platform, f"The mobile app crashes every time I open it. {uuid4()}")
    completed = wait_for_prediction(feedback["id"])
    prediction = completed["prediction"]
    assert prediction["sentiment"] in {"positive", "negative", "neutral"}
    assert 0 <= prediction["confidence"] <= 1
    assert prediction["topics"]
    assert prediction["model_version"] == completed["model_version"]
    assert prediction["embedding_version"]
    assert prediction["inference_ms"] >= 0
    response = platform["client"].get(platform["api"] + "/v1/insights", headers=platform["a"])
    assert response.status_code == 200, response.text
    assert response.json()["total"] >= 1


def test_idempotency_and_payload_conflict(platform, wait_for_prediction):
    key = str(uuid4())
    text = "Checkout charged me twice and I need a refund. " + key
    first = submit(platform, text, key=key)
    duplicate = submit(platform, text, key=key)
    assert first["id"] == duplicate["id"]
    response = platform["client"].post(
        platform["api"] + "/v1/feedback",
        headers={**platform["a"], "Idempotency-Key": key},
        json={"text": text + " changed", "source": "mobile", "metadata": {"test": True}},
    )
    assert response.status_code == 409, response.text
    completed = wait_for_prediction(first["id"])
    replay = submit(platform, text, key=key)
    assert replay["id"] == completed["id"]
    assert replay["prediction"] == completed["prediction"]


def test_concurrent_idempotent_requests_create_one_feedback(platform, wait_for_prediction):
    key = str(uuid4())
    with ThreadPoolExecutor(max_workers=8) as executor:
        results = list(
            executor.map(
                lambda _: submit(platform, "Great mobile app and helpful support " + key, key=key),
                range(12),
            )
        )
    assert len({item["id"] for item in results}) == 1
    wait_for_prediction(results[0]["id"])


def test_tenant_isolation_feedback_list_search_and_answers(platform, wait_for_prediction):
    marker = str(uuid4())
    a = submit(platform, "Mobile app crashes and freezes on launch. " + marker, "a", marker)
    b = submit(platform, "Mobile app crashes and loses login sessions. " + marker, "b", marker)
    assert a["id"] != b["id"], "Idempotency keys must be scoped to the authenticated tenant"
    wait_for_prediction(a["id"], "a")
    wait_for_prediction(b["id"], "b")
    for tenant, own, foreign in (("a", a, b), ("b", b, a)):
        response = platform["client"].get(
            f"{platform['api']}/v1/feedback/{foreign['id']}", headers=platform[tenant]
        )
        assert response.status_code == 404, response.text
        response = platform["client"].get(
            platform["api"] + "/v1/feedback", headers=platform[tenant], params={"limit": 100}
        )
        assert response.status_code == 200, response.text
        items = response.json()["items"]
        assert all(item["tenant_id"] == own["tenant_id"] for item in items)
        assert foreign["id"] not in {item["id"] for item in items}
        response = platform["client"].post(
            platform["rag"] + "/v1/search",
            headers=platform[tenant],
            json={"query": "mobile app crashes " + marker, "limit": 20},
        )
        assert response.status_code == 200, response.text
        results = response.json()["results"]
        assert results, "Completed feedback should be searchable"
        assert foreign["id"] not in {item["id"] for item in results}
        response = platform["client"].post(
            platform["rag"] + "/v1/ask",
            headers=platform[tenant],
            json={"question": "What problems happen in the mobile app? " + marker, "limit": 20},
        )
        assert response.status_code == 200, response.text
        answer = response.json()
        assert answer["mode"] in {"extractive", "llm"}
        assert answer["grounded"] is True
        assert answer["sources"]
        assert foreign["id"] not in {item["id"] for item in answer["sources"]}


def test_authentication_and_request_validation(platform):
    client = platform["client"]
    for headers in ({}, {"Authorization": "Bearer invalid-key"}):
        response = client.post(
            platform["api"] + "/v1/feedback", headers=headers, json={"text": "hello"}
        )
        assert response.status_code in {401, 403}, response.text
        response = client.post(
            platform["rag"] + "/v1/search", headers=headers, json={"query": "mobile app"}
        )
        assert response.status_code in {401, 403}, response.text
    for body in ({"text": ""}, {"text": " "}, {"text": "x" * 10001}):
        response = client.post(platform["api"] + "/v1/feedback", headers=platform["a"], json=body)
        assert response.status_code == 422, response.text


def test_model_reprocessing_history_and_rollback(platform, wait_for_prediction):
    feedback = submit(
        platform, "The mobile app is fast but billing support is awful. " + str(uuid4())
    )
    original = wait_for_prediction(feedback["id"])
    assert original["prediction"]["model_version"] == "v1"
    url = f"{platform['api']}/v1/feedback/{feedback['id']}"
    response = platform["client"].post(
        url + "/reprocess", headers=platform["b"], json={"model_version": "v2"}
    )
    assert response.status_code == 404, response.text
    response = platform["client"].get(url + "/predictions", headers=platform["b"])
    assert response.status_code == 404, response.text
    for version in ("v2", "v1"):
        response = platform["client"].post(
            url + "/reprocess", headers=platform["a"], json={"model_version": version}
        )
        assert response.status_code == 202, response.text
        result = wait_for_prediction(feedback["id"])
        assert result["prediction"]["model_version"] == version
    response = platform["client"].get(url + "/predictions", headers=platform["a"])
    assert response.status_code == 200, response.text
    history = response.json()["items"]
    assert {item["model_version"] for item in history} == {"v1", "v2"}
    assert len(history) == 2, "Rollback should reuse the immutable v1 prediction"


def test_postgresql_row_security_denies_missing_and_foreign_tenant(platform, wait_for_prediction):
    a = submit(platform, "The mobile app is easy to use. " + str(uuid4()), "a")
    b = submit(platform, "The mobile app keeps crashing. " + str(uuid4()), "b")
    wait_for_prediction(a["id"], "a")
    wait_for_prediction(b["id"], "b")
    connection = os.getenv("TEST_DATABASE_URL")
    if connection is None:
        password = os.getenv("APP_DB_PASSWORD")
        assert password, "APP_DB_PASSWORD or TEST_DATABASE_URL is required for the RLS test"
        connection = psycopg.conninfo.make_conninfo(
            host="127.0.0.1",
            port=int(os.getenv("POSTGRES_PORT", "55432")),
            dbname="feedback",
            user="feedback_app",
            password=password,
        )
    with psycopg.connect(connection, connect_timeout=10) as conn:
        role = conn.execute(
            "SELECT rolsuper,rolbypassrls FROM pg_roles WHERE rolname=current_user"
        ).fetchone()
        assert role == (False, False), "Tests must use the restricted application role"
        for table in ("feedback", "predictions"):
            assert conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0] == 0
        conn.execute("SELECT set_config('app.tenant_id', %s, true)", (a["tenant_id"],))
        assert (
            conn.execute("SELECT count(*) FROM feedback WHERE id=%s", (a["id"],)).fetchone()[0] == 1
        )
        assert (
            conn.execute("SELECT count(*) FROM feedback WHERE id=%s", (b["id"],)).fetchone()[0] == 0
        )
        assert (
            conn.execute(
                "SELECT count(*) FROM predictions WHERE feedback_id=%s", (b["id"],)
            ).fetchone()[0]
            == 0
        )
        with pytest.raises(psycopg.errors.InsufficientPrivilege), conn.transaction():
            conn.execute("UPDATE feedback SET tenant_id=%s WHERE id=%s", (b["tenant_id"], a["id"]))
