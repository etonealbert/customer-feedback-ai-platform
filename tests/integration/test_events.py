"""Verify actual Kafka redelivery and poison-event handling."""

import json
import os
import subprocess
import time
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest

pytestmark = pytest.mark.integration
ROOT = Path(__file__).resolve().parents[2]


def database():
    return psycopg.connect(
        host="localhost",
        port=int(os.getenv("POSTGRES_PORT", "55432")),
        dbname="feedback",
        user="postgres",
        password=os.environ["POSTGRES_PASSWORD"],
        autocommit=True,
    )


def in_worker(code, value=""):
    result = subprocess.run(
        ["docker", "compose", "exec", "-T", "worker", "python", "-c", code],
        input=value,
        text=True,
        capture_output=True,
        cwd=ROOT,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr
    return result.stdout.strip()


PUBLISH = """
import asyncio,sys
from aiokafka import AIOKafkaProducer
from feedback_platform.settings import get_settings
value=sys.stdin.buffer.read()
async def main():
    p=AIOKafkaProducer(bootstrap_servers=get_settings().kafka_bootstrap_servers,enable_idempotence=True)
    await p.start()
    try:
        m=await p.send_and_wait('feedback.v1',value=value)
        print(str(m.partition)+':'+str(m.offset))
    finally:
        await p.stop()
asyncio.run(main())
"""


def duplicate_count():
    return float(
        in_worker("""
import urllib.request
text=urllib.request.urlopen('http://localhost:8000/metrics').read().decode()
print(sum(float(line.split()[-1]) for line in text.splitlines() if line.startswith('feedback_worker_results_total{outcome="duplicate"}')))
""")
    )


def test_kafka_redelivery_keeps_one_immutable_prediction(platform, wait_for_prediction):
    response = platform["client"].post(
        platform["api"] + "/v1/feedback",
        headers=platform["a"],
        json={"text": "The mobile application crashes on checkout " + str(uuid4())},
    )
    assert response.status_code == 202
    item = wait_for_prediction(response.json()["id"])
    with database() as conn:
        payload = conn.execute(
            "SELECT payload FROM outbox WHERE payload->>'feedback_id'=%s AND topic='feedback.v1'",
            (item["id"],),
        ).fetchone()[0]
        before = duplicate_count()
        in_worker(PUBLISH, json.dumps(payload))
        deadline = time.monotonic() + 30
        while duplicate_count() <= before:
            assert time.monotonic() < deadline, (
                "Replayed Kafka record was not observed by the worker"
            )
            time.sleep(0.25)
        assert (
            conn.execute(
                "SELECT count(*) FROM predictions WHERE feedback_id=%s", (item["id"],)
            ).fetchone()[0]
            == 1
        )
    current = (
        platform["client"]
        .get(platform["api"] + "/v1/feedback/" + item["id"], headers=platform["a"])
        .json()
    )
    assert current["prediction"] == item["prediction"]


def test_poison_event_is_durably_dead_lettered_and_published(platform):
    partition, offset = map(
        int, in_worker(PUBLISH, "invalid-secret-payload-" + str(uuid4())).split(":")
    )
    deadline = time.monotonic() + 30
    with database() as conn:
        while True:
            row = conn.execute(
                """SELECT d.payload,o.published_at FROM dead_letters d JOIN outbox o USING(event_id)
                WHERE d.error_type='InvalidEvent' AND (d.payload->'event'->>'partition')::int=%s
                    AND (d.payload->'event'->>'offset')::bigint=%s""",
                (partition, offset),
            ).fetchone()
            if row and row[1]:
                assert "invalid-secret" not in json.dumps(row[0])
                break
            assert time.monotonic() < deadline, "DLQ outbox was not durably published"
            time.sleep(0.25)
