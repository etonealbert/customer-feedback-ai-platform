"""Acceptance tests against real FastAPI, Kafka, PostgreSQL and inference services."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

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
        response = client.post(
            platform["api"] + "/v1/feedback", headers=platform["a"], json=body
        )
        assert response.status_code == 422, response.text
