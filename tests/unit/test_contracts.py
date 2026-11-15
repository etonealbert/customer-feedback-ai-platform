import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import httpx
import pytest
from pydantic import ValidationError

from feedback_platform import worker
from feedback_platform.auth import hash_key
from feedback_platform.kafka import close_kafka
from feedback_platform.rag import extractive_answer, llm_answer, valid_citations
from feedback_platform.schemas import Event, FeedbackIn, Prediction
from feedback_platform.settings import get_settings


@pytest.mark.parametrize(
    "body",
    [
        {"text": "   "},
        {"text": "x" * 10001},
        {"text": "ok", "tenant_id": str(uuid4())},
        {"text": "ok", "metadata": {"oversized": "x" * 16385}},
    ],
)
def test_public_input_rejects_invalid_or_tenant_override(body):
    with pytest.raises(ValidationError):
        FeedbackIn.model_validate(body)


def test_key_storage_is_deterministic_digest():
    assert len(hash_key("example")) == 64
    assert hash_key("example") == hash_key("example")
    assert hash_key("example") != hash_key("different")


def test_prediction_rejects_nonfinite_vector():
    value = {
        "sentiment": "negative",
        "confidence": 0.8,
        "topics": ["mobile"],
        "model_version": "v1",
        "embedding_version": "lsa-v1",
        "inference_ms": 2,
        "embedding": [0.0] * 127 + [float("nan")],
    }
    with pytest.raises(ValidationError):
        Prediction.model_validate(value)


def test_rag_abstains_and_citation_validation_is_bounded():
    assert "could not find" in extractive_answer([])
    assert valid_citations("Crash reports [1]. Billing [2].", 2)
    assert not valid_citations("Invented source [3]", 2)
    assert not valid_citations("No citation", 2)
    assert not valid_citations("Invalid source [0]", 2)


async def test_llm_request_sends_only_evidence_and_disables_storage(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "test-only-no-network")
    monkeypatch.setenv("OPENAI_MODEL", "configured-model")
    get_settings.cache_clear()
    captured = {}

    def handler(request):
        captured.update(json.loads(request.content))
        return httpx.Response(
            200,
            json={
                "output": [
                    {
                        "type": "message",
                        "content": [
                            {"type": "output_text", "text": "Customers report crashes [1]."}
                        ],
                    }
                ]
            },
        )

    try:
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            answer = await llm_answer(
                client, "What is wrong?", [{"text": "The app crashes", "topics": ["mobile"]}]
            )
        assert answer.endswith("[1].")
        assert captured["store"] is False
        assert "tools" not in captured
        assert "untrusted" in captured["instructions"]
        assert json.loads(captured["input"])["evidence"] == [
            {"source": 1, "text": "The app crashes", "topics": ["mobile"]}
        ]
    finally:
        get_settings.cache_clear()


async def test_invalid_event_dead_letter_is_stable_and_does_not_copy_content(monkeypatch):
    dead_letter = AsyncMock()
    monkeypatch.setattr(worker, "dead_letter", dead_letter)
    record = SimpleNamespace(
        topic="feedback.v1", partition=2, offset=10, value=b"secret bad payload"
    )
    assert await worker.process_record(None, None, record) == "invalid"
    first = dead_letter.call_args
    assert await worker.process_record(None, None, record) == "invalid"
    assert first == dead_letter.call_args
    assert "secret" not in str(first)
    assert first.args[2]["offset"] == 10


@pytest.mark.parametrize("fails", [False, True])
async def test_worker_acknowledges_only_after_durable_processing(monkeypatch, fails):
    actions = []
    partition = worker.TopicPartition("feedback.v1", 0)
    record = SimpleNamespace(topic="feedback.v1", partition=0, offset=8)

    class Consumer:
        def __init__(self, *args, **kwargs):
            assert kwargs["enable_auto_commit"] is False
            self.polled = False

        async def start(self):
            pass

        async def getmany(self, **kwargs):
            if self.polled:
                raise asyncio.CancelledError
            self.polled = True
            return {partition: [record]}

        async def commit(self, offsets):
            actions.append(("commit", offsets[partition]))

        async def committed(self, partition):
            return 9

        def highwater(self, partition):
            return 9

        def assignment(self):
            return {partition}

        async def stop(self):
            actions.append(("stop", None))

    async def process(*args):
        actions.append(("persist", None))
        if fails:
            raise RuntimeError("Database unavailable")
        return "completed"

    monkeypatch.setattr(worker, "AIOKafkaConsumer", Consumer)
    monkeypatch.setattr(worker, "process_record", process)
    with pytest.raises(RuntimeError if fails else asyncio.CancelledError):
        await worker.run(SimpleNamespace(state=SimpleNamespace(pool=None)))
    assert actions[0] == ("persist", None)
    assert (("commit", 9) in actions) is (not fails)
    assert actions[-1] == ("stop", None)


def test_event_pins_supported_model_version():
    payload = {
        "event_id": uuid4(),
        "feedback_id": uuid4(),
        "tenant_id": uuid4(),
        "model_version": "v3",
    }
    with pytest.raises(ValidationError):
        Event.model_validate(payload)


async def test_stalled_kafka_shutdown_terminates_service_instead_of_hanging():
    async def stalled():
        await asyncio.Event().wait()

    with pytest.raises(SystemExit):
        await close_kafka(SimpleNamespace(stop=stalled), timeout=0.01)


def test_poll_budget_exceeds_configured_worst_case_inference_and_backoff():
    for attempts in (1, 4, 20):
        settings = SimpleNamespace(max_attempts=attempts, retry_base_seconds=30)
        budget = worker.poll_budget_ms(settings) / 1000
        assert budget > attempts * 10 + sum(min(10, 30 * 2**i) for i in range(attempts - 1)) + 60


async def test_lag_refresh_removes_revoked_partitions_and_updates_idle_assignment(monkeypatch):
    old, current = worker.TopicPartition("feedback.v1", 0), worker.TopicPartition("feedback.v1", 1)
    values = {"0": 99}

    class Gauge:
        def remove(self, label):
            values.pop(label, None)

        def labels(self, label):
            return SimpleNamespace(set=lambda value: values.update({label: value}))

    consumer = SimpleNamespace(
        assignment=lambda: {current}, highwater=lambda _: 9, committed=AsyncMock(return_value=7)
    )
    monkeypatch.setattr(worker, "CONSUMER_LAG", Gauge())
    offsets = {old: 3}
    await worker.refresh_lag(consumer, offsets)
    assert values == {"1": 2}
    assert offsets == {current: 7}
