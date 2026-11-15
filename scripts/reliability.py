"""Run recoverable fault experiments against this repository's Compose project."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID, uuid4

import httpx
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]


class Experiments:
    def __init__(self, api, key, timeout, count, outage_seconds=25):
        self.api, self.timeout, self.count = api, timeout, count
        self.client = httpx.Client(headers={"Authorization": f"Bearer {key}"}, timeout=15)
        self.commands = []
        self.outage_seconds = outage_seconds

    def compose(self, *args, override=None, check=True, timeout=120):
        command = [
            "docker",
            "compose",
            "--project-directory",
            str(ROOT),
            "-f",
            str(ROOT / "compose.yaml"),
        ]
        if override:
            command.extend(["-f", str(override)])
        command.extend(args)
        start = time.monotonic()
        result = subprocess.run(command, cwd=ROOT, text=True, capture_output=True, timeout=timeout)
        self.commands.append(
            {
                "args": list(args),
                "override": bool(override),
                "returncode": result.returncode,
                "elapsed_seconds": time.monotonic() - start,
                "stdout": result.stdout[-4000:],
                "stderr": result.stderr[-4000:],
            }
        )
        if check and result.returncode:
            raise RuntimeError(f"Compose {' '.join(args)} failed: {result.stderr[-1000:]}")
        return result

    def submit(self, count=None):
        ids = []
        for index in range(count if count is not None else self.count):
            response = self.client.post(
                self.api + "/v1/feedback",
                headers={"Idempotency-Key": "reliability-" + str(uuid4())},
                json={
                    "text": "The mobile app crashes and the login is broken.",
                    "source": "mobile",
                    "metadata": {"reliability": True, "sample": index},
                },
            )
            if response.status_code != 202:
                raise AssertionError(
                    f"Expected acceptance, got {response.status_code}: {response.text}"
                )
            ids.append(response.json()["id"])
        return ids

    def ready(self):
        deadline = time.monotonic() + self.timeout
        while time.monotonic() < deadline:
            try:
                if self.client.get(self.api + "/health/ready").status_code == 200:
                    return
            except httpx.HTTPError:
                pass
            time.sleep(0.5)
        raise TimeoutError("Ingestion API did not become ready")

    def statuses(self, ids):
        results = {}
        for feedback_id in ids:
            response = self.client.get(self.api + "/v1/feedback/" + feedback_id)
            response.raise_for_status()
            results[feedback_id] = response.json()["status"]
        return results

    def completed(self, ids):
        deadline = time.monotonic() + self.timeout
        pending = set(ids)
        completed = {}
        while pending and time.monotonic() < deadline:
            for feedback_id in list(pending):
                try:
                    response = self.client.get(self.api + "/v1/feedback/" + feedback_id)
                    response.raise_for_status()
                except httpx.HTTPError:
                    continue
                value = response.json()
                if value["status"] == "failed":
                    raise AssertionError(f"Feedback moved to failed state: {feedback_id}")
                if value["status"] == "completed":
                    completed[feedback_id] = value["prediction"]
                    pending.remove(feedback_id)
            if pending:
                time.sleep(0.25)
        if pending:
            raise TimeoutError(
                f"{len(pending)} of {len(ids)} items did not complete: {sorted(pending)[:5]}"
            )
        return completed

    def audit(self, ids):
        literals = ",".join("'" + str(UUID(value)) + "'::uuid" for value in ids)
        query = f"""
            SELECT json_build_object(
              'feedback_rows', (SELECT count(*) FROM feedback WHERE id IN ({literals})),
              'completed_rows', (SELECT count(*) FROM feedback WHERE id IN ({literals}) AND status='completed'),
              'prediction_rows', (SELECT count(*) FROM predictions WHERE feedback_id IN ({literals})),
              'duplicate_predictions', (SELECT count(*) FROM (
                SELECT feedback_id, model_version FROM predictions WHERE feedback_id IN ({literals})
                GROUP BY feedback_id, model_version HAVING count(*) > 1
              ) duplicates)
            )
        """
        result = self.compose(
            "exec",
            "-T",
            "postgres",
            "psql",
            "-X",
            "-A",
            "-t",
            "-U",
            "postgres",
            "-d",
            "feedback",
            "-c",
            query,
        )
        audit = json.loads(result.stdout.strip())
        if audit["feedback_rows"] != len(ids) or audit["completed_rows"] != len(ids):
            raise AssertionError(f"Missing or unfinished database rows: {audit}")
        if audit["duplicate_predictions"] != 0:
            raise AssertionError(f"Duplicate predictions found: {audit}")
        return audit

    def worker_crash(self):
        """Queue work, SIGKILL the consumer, then check completion and uniqueness."""
        ids = []
        try:
            self.compose("stop", "-t", "5", "worker")
            ids = self.submit()
            before = self.statuses(ids)
            assert set(before.values()) == {"pending"}, before
            self.compose("start", "worker")
            time.sleep(0.5)
            self.compose("kill", "-s", "SIGKILL", "worker")
            after_kill = self.statuses(ids)
            self.compose("start", "worker")
            predictions = self.completed(ids)
            audit = self.audit(ids)
            assert audit["prediction_rows"] == len(ids), audit
            return {
                "feedback_ids": ids,
                "before": before,
                "after_kill": after_kill,
                "completed": len(predictions),
                "audit": audit,
                "limitation": "Timing-based SIGKILL does not prove a specific crash occurred between DB commit and Kafka offset commit; idempotency is also covered by deterministic tests.",
            }
        finally:
            self.compose("start", "worker", check=False)

    def kafka_outage(self):
        try:
            self.compose("stop", "-t", "5", "kafka")
            ids = self.submit()
            before = self.statuses(ids)
            assert set(before.values()) == {"pending"}, before
            time.sleep(self.outage_seconds)
            self.compose("start", "kafka")
            predictions = self.completed(ids)
            audit = self.audit(ids)
            assert audit["prediction_rows"] == len(ids), audit
            return {
                "feedback_ids": ids,
                "accepted_during_outage": len(ids),
                "outage_hold_seconds": self.outage_seconds,
                "before_recovery": before,
                "completed": len(predictions),
                "audit": audit,
            }
        finally:
            self.compose("start", "kafka", check=False)

    def database_outage(self):
        try:
            self.compose("stop", "-t", "5", "postgres")
            response = self.client.post(
                self.api + "/v1/feedback", json={"text": "Feedback during database outage"}
            )
            status, body = response.status_code, response.text
            assert status == 503, f"Expected fail-closed HTTP 503, got {status}: {body}"
            readiness = self.client.get(self.api + "/health/ready").status_code
            assert readiness == 503, readiness
            self.compose("start", "postgres")
            self.ready()
            ids = self.submit(3)
            predictions = self.completed(ids)
            return {
                "ingestion_during_outage_http_status": status,
                "readiness_during_outage_http_status": readiness,
                "completed_after_recovery": len(predictions),
                "feedback_ids": ids,
                "audit": self.audit(ids),
            }
        finally:
            self.compose("start", "postgres", check=False)

    def unhealthy_model(self):
        """Reject an unhealthy inference deployment, restore it, then drain accepted work."""
        probe = [
            "exec",
            "-T",
            "inference",
            "python",
            "-c",
            "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/health/ready', timeout=3)",
        ]
        try:
            self.compose("stop", "-t", "5", "worker")
            with tempfile.TemporaryDirectory(prefix="feedback-model-experiment-") as temporary:
                override = Path(temporary) / "unhealthy.json"
                override.write_text(
                    json.dumps(
                        {
                            "services": {
                                "inference": {
                                    "environment": {"MODEL_VERSION": "definitely-invalid"}
                                }
                            }
                        }
                    ),
                    encoding="utf-8",
                )
                self.compose("up", "-d", "--no-deps", "inference", override=override)
                time.sleep(4)
                result = self.compose(*probe, check=False)
                assert result.returncode != 0, "Invalid model unexpectedly passed readiness"
                unhealthy_logs = self.compose(
                    "logs", "--tail", "40", "inference", check=False
                ).stdout
                ids = self.submit(3)
                before = self.statuses(ids)
                assert set(before.values()) == {"pending"}, before
                self.compose("up", "-d", "--no-deps", "inference")
                deadline = time.monotonic() + self.timeout
                while time.monotonic() < deadline:
                    if self.compose(*probe, check=False).returncode == 0:
                        break
                    time.sleep(1)
                else:
                    raise TimeoutError("Restored inference deployment did not become ready")
                self.compose("start", "worker")
                predictions = self.completed(ids)
                return {
                    "invalid_model": "definitely-invalid",
                    "readiness_probe_exit_code": result.returncode,
                    "unhealthy_logs": unhealthy_logs[-6000:],
                    "feedback_ids": ids,
                    "pending_while_unhealthy": before,
                    "completed_after_rollback": len(predictions),
                    "audit": self.audit(ids),
                    "limitation": "Compose replacement experiment with workers paused; Kubernetes rolling-update availability is a separate runbook exercise.",
                }
        finally:
            self.compose("up", "-d", "--no-deps", "inference", check=False)
            self.compose("start", "worker", check=False)

    def model_rollout(self):
        ids = self.submit(1)
        initial = self.completed(ids)[ids[0]]
        results = {initial["model_version"]: initial}
        for version in ("v2", "v1"):
            response = self.client.post(
                self.api + f"/v1/feedback/{ids[0]}/reprocess", json={"model_version": version}
            )
            assert response.status_code == 202, response.text
            prediction = self.completed(ids)[ids[0]]
            assert prediction["model_version"] == version, prediction
            results[version] = prediction
        response = self.client.get(self.api + f"/v1/feedback/{ids[0]}/predictions")
        response.raise_for_status()
        history = response.json()["items"]
        assert {item["model_version"] for item in history} >= {"v1", "v2"}, history
        return {
            "feedback_ids": ids,
            "predictions_by_version": results,
            "history": history,
            "active_model_after_rollback": "v1",
            "audit": self.audit(ids),
        }


def main(args):
    load_dotenv(ROOT / ".env")
    key = os.getenv("DEMO_TENANT_A_KEY")
    if not key:
        raise SystemExit("DEMO_TENANT_A_KEY required; run scripts/setup_env.py first")
    experiment = Experiments(args.api, key, args.timeout, args.count, args.outage_seconds)
    report = {
        "schema_version": 1,
        "started_at_utc": datetime.now(UTC).isoformat(),
        "configuration": vars(args).copy(),
        "experiments": [],
        "scope": "Only the Compose project defined by this repository; existing data is retained",
    }
    selected = args.scenario or ["worker", "kafka", "database", "unhealthy-model", "model-rollout"]
    mapping = {
        "worker": experiment.worker_crash,
        "kafka": experiment.kafka_outage,
        "database": experiment.database_outage,
        "unhealthy-model": experiment.unhealthy_model,
        "model-rollout": experiment.model_rollout,
    }
    failed = False
    try:
        experiment.ready()
        for name in selected:
            start = time.monotonic()
            print(f"Running {name}...", flush=True)
            result = {"scenario": name, "started_at_utc": datetime.now(UTC).isoformat()}
            try:
                result["observations"] = mapping[name]()
                result["passed"] = True
            except Exception as exc:
                result["passed"] = False
                result["error"] = f"{type(exc).__name__}: {exc}"
                failed = True
            result["elapsed_seconds"] = time.monotonic() - start
            report["experiments"].append(result)
            print(
                json.dumps({key: value for key, value in result.items() if key != "observations"}),
                flush=True,
            )
            if failed:
                break
    finally:
        experiment.client.close()
        report["commands"] = experiment.commands
        report["finished_at_utc"] = datetime.now(UTC).isoformat()
        output = Path(args.output)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(report, indent=2), encoding="utf-8")
        print(f"Evidence: {output}", flush=True)
    return int(failed)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--api", default=os.getenv("TEST_API_URL", "http://localhost:8000"))
    parser.add_argument("--count", type=int, default=60)
    parser.add_argument("--timeout", type=float, default=180)
    parser.add_argument("--outage-seconds", type=float, default=25)
    parser.add_argument(
        "--scenario",
        action="append",
        choices=["worker", "kafka", "database", "unhealthy-model", "model-rollout"],
    )
    parser.add_argument("--output", default="docs/evidence/reliability-local.json")
    arguments = parser.parse_args()
    if arguments.count < 1 or arguments.timeout <= 0:
        parser.error("count and timeout must be positive")
    raise SystemExit(main(arguments))
