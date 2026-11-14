"""Live acceptance fixtures. Select explicitly with ``pytest -m integration``."""

from __future__ import annotations

import os
import time
from pathlib import Path

import httpx
import pytest
from dotenv import load_dotenv


@pytest.fixture(scope="session")
def platform(request):
    selected = "integration" in (request.config.option.markexpr or "")
    if not selected and os.getenv("RUN_INTEGRATION") != "1":
        pytest.skip("Live stack tests require pytest -m integration or RUN_INTEGRATION=1")
    load_dotenv(Path(__file__).resolve().parents[2] / ".env")
    keys = [os.getenv("DEMO_TENANT_A_KEY"), os.getenv("DEMO_TENANT_B_KEY")]
    if not all(keys):
        pytest.fail("Set DEMO_TENANT_A_KEY and DEMO_TENANT_B_KEY; run scripts/setup_env.py")
    api_url = os.getenv("TEST_API_URL", "http://localhost:8000")
    rag_url = os.getenv("TEST_RAG_URL", "http://localhost:8002")
    with httpx.Client(timeout=30) as client:
        deadline = time.monotonic() + 90
        for endpoint in (api_url, rag_url):
            while True:
                try:
                    response = client.get(endpoint + "/health/ready")
                    if response.status_code == 200:
                        break
                except httpx.HTTPError:
                    pass
                if time.monotonic() > deadline:
                    pytest.fail(f"Service not ready: {endpoint}")
                time.sleep(1)
        yield {
            "client": client,
            "api": api_url,
            "rag": rag_url,
            "a": {"Authorization": f"Bearer {keys[0]}"},
            "b": {"Authorization": f"Bearer {keys[1]}"},
        }


@pytest.fixture
def wait_for_prediction(platform):
    def wait(feedback_id, tenant="a", timeout=90):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            response = platform["client"].get(
                f"{platform['api']}/v1/feedback/{feedback_id}",
                headers=platform[tenant],
            )
            assert response.status_code == 200, response.text
            item = response.json()
            if item["status"] == "completed":
                assert item["prediction"] is not None
                return item
            assert item["status"] != "failed", item
            time.sleep(0.25)
        pytest.fail(f"Feedback {feedback_id} was not processed within {timeout}s")

    return wait
