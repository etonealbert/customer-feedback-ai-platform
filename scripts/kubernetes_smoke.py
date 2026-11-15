"""Exercise an isolated Kubernetes deployment and save real, credential-free evidence.

This deliberately changes the selected cluster: it scales workers to three, attempts
an unhealthy inference rollout, rolls that deployment back, and restores one worker.
Every kubectl invocation includes the explicit --kubeconfig supplied by the caller.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

from dotenv import dotenv_values

ROOT = Path(__file__).resolve().parents[1]
NAMESPACE = "feedback-ai"
SERVICES = ("ingestion", "inference", "rag", "outbox", "worker")

# Credentials arrive over stdin, not process arguments or the evidence log.
REMOTE_SMOKE = r'''
import json
import sys
import time
from uuid import uuid4

import httpx

config = json.load(sys.stdin)
checks = {}
evidence = {"checks": checks}
try:
    text = "The mobile application crashes during checkout and payment fails."
    a = {"Authorization": "Bearer " + config["tenant_a_key"]}
    b = {"Authorization": "Bearer " + config["tenant_b_key"]}
    ingest = "http://ingestion:8000"
    rag = "http://rag:8000"
    with httpx.Client(timeout=10) as client:
        key = str(uuid4())
        payload = {"text": text, "source": "kubernetes-smoke"}
        created = client.post(ingest + "/v1/feedback", headers={**a, "Idempotency-Key": key}, json=payload)
        checks["accepted_202"] = created.status_code == 202
        created.raise_for_status()
        feedback_id = created.json()["id"]
        evidence["feedback_id"] = feedback_id
        replay = client.post(ingest + "/v1/feedback", headers={**a, "Idempotency-Key": key}, json=payload)
        replay.raise_for_status()
        checks["idempotent_submission"] = replay.json()["id"] == feedback_id
        started = time.monotonic()
        deadline = started + 120
        row = {"status": "pending"}
        while time.monotonic() < deadline:
            response = client.get(ingest + "/v1/feedback/" + feedback_id, headers=a)
            response.raise_for_status()
            row = response.json()
            if row["status"] != "pending":
                break
            time.sleep(0.5)
        evidence["processing_wait_seconds"] = round(time.monotonic() - started, 4)
        evidence["feedback_status"] = row["status"]
        evidence["prediction"] = row.get("prediction")
        checks["asynchronous_prediction_completed"] = row["status"] == "completed" and bool(row.get("prediction"))
        other = client.get(ingest + "/v1/feedback/" + feedback_id, headers=b)
        evidence["other_tenant_status"] = other.status_code
        checks["other_tenant_receives_404"] = other.status_code == 404
        search = client.post(rag + "/v1/search", headers=a, json={"query": text, "limit": 5})
        search.raise_for_status()
        evidence["search"] = search.json()
        checks["semantic_search_retrieves_feedback"] = any(item["id"] == feedback_id for item in search.json()["results"])
        other_search = client.post(rag + "/v1/search", headers=b, json={"query": text, "limit": 5})
        other_search.raise_for_status()
        checks["other_tenant_search_excludes_feedback"] = all(item["id"] != feedback_id for item in other_search.json()["results"])
        answer = client.post(rag + "/v1/ask", headers=a, json={"question": "What complaints concern the mobile application crashing during checkout?", "limit": 5})
        answer.raise_for_status()
        evidence["ask"] = answer.json()
        checks["grounded_answer_has_sources"] = bool(answer.json()["sources"]) and bool(answer.json()["answer"]) and answer.json()["grounded"] is True
        checks["answer_cites_submitted_feedback"] = any(item["id"] == feedback_id for item in answer.json()["sources"])
    evidence["success"] = all(checks.values())
except Exception as exc:
    evidence["success"] = False
    evidence["error"] = {"type": type(exc).__name__, "message": str(exc)}
print(json.dumps(evidence))
sys.exit(0 if evidence["success"] else 1)
'''

REMOTE_READY = r'''
import json
import sys
import httpx

try:
    response = httpx.get("http://inference:8000/health/ready", timeout=10)
    result = {"status_code": response.status_code, "body": response.json()}
    result["success"] = response.status_code == 200 and result["body"].get("model_version") != "definitely-invalid"
except Exception as exc:
    result = {"success": False, "error": {"type": type(exc).__name__, "message": str(exc)}}
print(json.dumps(result))
sys.exit(0 if result["success"] else 1)
'''


def utc_now() -> str:
    return datetime.now(UTC).isoformat()


def text_output(value: str | bytes | None) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value or ""


class SmokeFailure(RuntimeError):
    """An expected verification condition did not hold."""


class Recorder:
    def __init__(self, kubeconfig: Path, context: str | None, output: Path, secrets: list[str]):
        self.output = output
        self.secrets = [value for value in secrets if value]
        self.command = ["kubectl", "--kubeconfig", str(kubeconfig.resolve())]
        if context:
            self.command += ["--context", context]
        # The request timeout bounds API calls independently of rollout's watch deadline.
        self.command += ["--request-timeout=30s"]
        self.report = {
            "started_at": utc_now(),
            "scope": "isolated local Kubernetes smoke and rollout experiment",
            "namespace": NAMESPACE,
            "kubeconfig": str(kubeconfig.resolve()),
            "requested_context": context,
            "commands": [],
            "checks": {},
            "cleanup": {},
            "success": False,
        }

    def redact(self, text: str) -> str:
        for secret in self.secrets:
            text = text.replace(secret, "[REDACTED]")
        return text

    def save(self) -> None:
        self.output.parent.mkdir(parents=True, exist_ok=True)
        serialized = self.redact(json.dumps(self.report, indent=2, ensure_ascii=False)) + "\n"
        temporary = self.output.with_name(self.output.name + ".tmp")
        temporary.write_text(serialized, encoding="utf-8")
        temporary.replace(self.output)

    def run(
        self,
        label: str,
        args: list[str],
        *,
        timeout: int = 60,
        required: bool = True,
        stdin: str | None = None,
        display_args: list[str] | None = None,
    ) -> dict:
        command = self.command + args
        entry = {
            "label": label,
            "started_at": utc_now(),
            "command": self.command + (display_args if display_args is not None else args),
            "timeout_seconds": timeout,
            "stdin_recorded": False,
        }
        started = time.monotonic()
        try:
            result = subprocess.run(
                command, input=stdin, capture_output=True, text=True,
                encoding="utf-8", errors="replace", timeout=timeout, check=False,
            )
            entry.update(
                returncode=result.returncode,
                stdout=self.redact(result.stdout.strip()),
                stderr=self.redact(result.stderr.strip()),
                timed_out=False,
            )
        except subprocess.TimeoutExpired as exc:
            entry.update(
                returncode=None,
                stdout=self.redact(text_output(exc.stdout).strip()),
                stderr=self.redact(text_output(exc.stderr).strip()),
                timed_out=True,
            )
        except OSError as exc:
            entry.update(returncode=None, stdout="", stderr=self.redact(str(exc)), timed_out=False)
        entry["elapsed_seconds"] = round(time.monotonic() - started, 4)
        entry["success"] = entry["returncode"] == 0 and not entry["timed_out"]
        self.report["commands"].append(entry)
        self.save()
        print(f"{label}: {'ok' if entry['success'] else 'failed'}", flush=True)
        if required and not entry["success"]:
            raise SmokeFailure(f"Command failed: {label}; see saved evidence")
        return entry

    def rollout(self, service: str, *, seconds: int = 180, required: bool = True) -> dict:
        return self.run(
            f"rollout/{service}",
            ["-n", NAMESPACE, "rollout", "status", f"deployment/{service}", f"--timeout={seconds}s"],
            timeout=seconds + 20,
            required=required,
        )

    def snapshot(self, label: str) -> None:
        self.run(
            f"{label}/deployments", ["-n", NAMESPACE, "get", "deployments", "-o", "wide"],
            required=False,
        )
        self.run(
            f"{label}/pods", ["-n", NAMESPACE, "get", "pods", "-o", "wide"], required=False,
        )

    def remote(self, label: str, script: str, payload: dict | None = None, timeout: int = 60) -> dict:
        prefix = ["-n", NAMESPACE, "exec", "-i", "deployment/ingestion", "--", "python", "-c"]
        entry = self.run(
            label,
            prefix + [script],
            display_args=prefix + [f"<inline {label} program; source is committed in this script>"],
            stdin=json.dumps(payload) if payload is not None else "",
            timeout=timeout,
            required=False,
        )
        try:
            # kubectl warnings go to stderr; the remote program emits one JSON document.
            result = json.loads(entry["stdout"])
        except (TypeError, json.JSONDecodeError) as exc:
            raise SmokeFailure(f"{label} did not return JSON evidence") from exc
        self.report[label] = result
        self.save()
        if not entry["success"] or result.get("success") is not True:
            raise SmokeFailure(f"{label} failed; see saved evidence")
        return result


def exercise(recorder: Recorder, values: dict) -> None:
    inference_changed = False
    workers_changed = False
    try:
        recorder.run("context", ["config", "current-context"])
        recorder.run("version", ["version", "-o", "json"])
        recorder.run("nodes", ["get", "nodes", "-o", "wide"])
        for service in SERVICES:
            recorder.rollout(service)
        recorder.report["checks"]["initial_deployments_ready"] = True
        recorder.snapshot("initial")

        workers_changed = True
        recorder.run(
            "scale-worker-three", ["-n", NAMESPACE, "scale", "deployment/worker", "--replicas=3"]
        )
        recorder.rollout("worker")
        state = recorder.run(
            "worker-replica-state", ["-n", NAMESPACE, "get", "deployment/worker", "-o", "json"]
        )
        deployment = json.loads(state["stdout"])
        recorder.report["checks"]["three_workers_available"] = (
            deployment["spec"]["replicas"] == 3
            and deployment.get("status", {}).get("availableReplicas", 0) == 3
        )
        if not recorder.report["checks"]["three_workers_available"]:
            raise SmokeFailure("Worker deployment did not reach three available replicas")

        recorder.remote(
            "application_smoke",
            REMOTE_SMOKE,
            payload={
                "tenant_a_key": values["DEMO_TENANT_A_KEY"],
                "tenant_b_key": values["DEMO_TENANT_B_KEY"],
            },
            timeout=190,
        )
        recorder.report["checks"]["cluster_application_flow"] = True
        top = recorder.run("resource-usage", ["-n", NAMESPACE, "top", "pods"], required=False)
        recorder.report["resource_metrics"] = {
            "available": top["success"],
            "reason_if_unavailable": None if top["success"] else top["stderr"] or "kubectl top pods failed",
        }
        hpa = recorder.run("autoscalers", ["-n", NAMESPACE, "get", "hpa", "-o", "json"], required=False)
        recorder.report["hpa"] = {
            "query_succeeded": hpa["success"],
            "resources": json.loads(hpa["stdout"]).get("items", []) if hpa["success"] else [],
        }

        # Mark before mutation: a command timeout might occur after the API accepts it.
        inference_changed = True
        recorder.run(
            "introduce-unhealthy-model",
            ["-n", NAMESPACE, "set", "env", "deployment/inference", "MODEL_VERSION=definitely-invalid"],
        )
        unhealthy = recorder.rollout("inference", seconds=30, required=False)
        recorder.report["checks"]["unhealthy_rollout_blocked"] = not unhealthy["success"]
        recorder.snapshot("unhealthy-model")
        if unhealthy["success"]:
            raise SmokeFailure("Unhealthy model unexpectedly completed its rollout")
        recorder.remote("old_inference_still_ready", REMOTE_READY)
        recorder.report["checks"]["previous_replica_preserved_availability"] = True
    finally:
        # Cleanup is attempted even when verification, parsing, or kubectl fails.
        if inference_changed:
            undo = recorder.run(
                "rollback-inference",
                ["-n", NAMESPACE, "rollout", "undo", "deployment/inference"],
                required=False,
            )
            ready = recorder.rollout("inference", required=False)
            recorder.report["cleanup"]["inference_rolled_back"] = undo["success"] and ready["success"]
        if workers_changed:
            scale = recorder.run(
                "restore-one-worker", ["-n", NAMESPACE, "scale", "deployment/worker", "--replicas=1"],
                required=False,
            )
            ready = recorder.rollout("worker", required=False)
            recorder.report["cleanup"]["one_worker_restored"] = scale["success"] and ready["success"]
        recorder.snapshot("final")
        recorder.save()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--kubeconfig", type=Path, required=True)
    parser.add_argument("--context")
    parser.add_argument("--output", type=Path, default=ROOT / "docs/evidence/kubernetes-local.json")
    args = parser.parse_args()
    values = dotenv_values(ROOT / ".env")
    secrets = [value for key, value in values.items() if value and ("KEY" in key or "PASSWORD" in key)]
    recorder = Recorder(args.kubeconfig, args.context, args.output, secrets)
    try:
        if not args.kubeconfig.is_file():
            raise SmokeFailure("The explicit kubeconfig path does not exist")
        missing = [key for key in ("DEMO_TENANT_A_KEY", "DEMO_TENANT_B_KEY") if not values.get(key)]
        if missing:
            raise SmokeFailure("Missing .env keys: " + ", ".join(missing))
        exercise(recorder, values)
        recorder.report["success"] = (
            bool(recorder.report["checks"])
            and all(recorder.report["checks"].values())
            and all(recorder.report["cleanup"].values())
        )
    except (Exception, KeyboardInterrupt) as exc:
        recorder.report["error"] = {"type": type(exc).__name__, "message": str(exc)}
        recorder.report["success"] = False
    finally:
        recorder.report["finished_at"] = utc_now()
        recorder.save()
        print(f"Evidence saved to {args.output}")
    return 0 if recorder.report["success"] else 1


if __name__ == "__main__":
    sys.exit(main())
