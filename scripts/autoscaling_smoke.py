"""Exercise CPU autoscaling in an explicitly selected development cluster.

Requires deployed base resources and metrics-server. Generates synthetic inference
traffic for 60 seconds, records actual HPA decisions, then restores one replica.
Do not run concurrently with kubernetes_smoke.py or performance benchmarks.
"""

import argparse
import json
import subprocess
import time
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LOAD = r"""
import concurrent.futures,json,os,time,httpx
deadline=time.monotonic()+60
def load(_):
    good,bad=0,0
    with httpx.Client(timeout=10) as client:
        while time.monotonic()<deadline:
            try:
                r=client.post('http://inference:8000/predict',headers={'X-Service-Token':os.environ['INTERNAL_API_KEY']},json={'text':'The mobile application crashes at checkout','model_version':'v1'})
                good+=r.status_code==200
                bad+=r.status_code!=200
            except httpx.HTTPError:
                bad+=1
    return good,bad
with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
    results=list(executor.map(load,range(8)))
print(json.dumps({'successful_inference_requests':sum(x[0] for x in results),'failed_inference_requests':sum(x[1] for x in results)}))
"""


def main(args):
    command = ["kubectl", "--kubeconfig", str(args.kubeconfig.resolve()), "--request-timeout=30s"]
    report = {
        "started_at_utc": datetime.now(UTC).isoformat(),
        "commands": [],
        "snapshots": [],
        "success": False,
        "scope": "60-second synthetic inference CPU autoscaling in local development cluster",
    }
    output = args.output
    output.parent.mkdir(parents=True, exist_ok=True)

    def save():
        output.write_text(json.dumps(report, indent=2), encoding="utf-8")

    def run(*arguments, required=True, timeout=60):
        result = subprocess.run(
            command + list(arguments), capture_output=True, text=True, timeout=timeout
        )
        report["commands"].append(
            {
                "args": list(arguments),
                "returncode": result.returncode,
                "stdout": result.stdout,
                "stderr": result.stderr,
            }
        )
        save()
        if required and result.returncode:
            raise RuntimeError(result.stderr)
        return result

    process = None
    try:
        run("apply", "-f", str(ROOT / "infrastructure/kubernetes/optional/autoscaling.yaml"))
        process = subprocess.Popen(
            command
            + ["-n", "feedback-ai", "exec", "deployment/ingestion", "--", "python", "-c", LOAD],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        deadline = time.monotonic() + 105
        scaled = False
        while time.monotonic() < deadline:
            state = json.loads(
                run("-n", "feedback-ai", "get", "hpa/inference", "-o", "json").stdout
            )
            deployment = json.loads(
                run("-n", "feedback-ai", "get", "deployment/inference", "-o", "json").stdout
            )
            snapshot = {
                "time_utc": datetime.now(UTC).isoformat(),
                "hpa": state["status"],
                "deployment": deployment.get("status", {}),
            }
            report["snapshots"].append(snapshot)
            scaled |= (
                state["status"].get("desiredReplicas", 0) > 1
                and deployment.get("status", {}).get("availableReplicas", 0) > 1
            )
            save()
            if scaled and process.poll() is not None:
                break
            time.sleep(5)
        stdout, stderr = process.communicate(timeout=20)
        report["load"] = json.loads(stdout) if stdout.strip() else {"error": stderr}
        report["metrics"] = run("-n", "feedback-ai", "top", "pods").stdout
        report["success"] = (
            scaled
            and process.returncode == 0
            and report["load"].get("failed_inference_requests", 1) == 0
        )
    except Exception as exc:
        report["error"] = f"{type(exc).__name__}: {exc}"
    finally:
        if process is not None and process.poll() is None:
            process.terminate()
            process.wait(timeout=10)
        run(
            "delete",
            "-f",
            str(ROOT / "infrastructure/kubernetes/optional/autoscaling.yaml"),
            required=False,
        )
        for name in ("inference", "worker"):
            run("-n", "feedback-ai", "scale", "deployment/" + name, "--replicas=1", required=False)
            restored = run(
                "-n",
                "feedback-ai",
                "rollout",
                "status",
                "deployment/" + name,
                "--timeout=90s",
                required=False,
                timeout=100,
            )
            if restored.returncode:
                report["success"] = False
        report["finished_at_utc"] = datetime.now(UTC).isoformat()
        save()
    print(
        json.dumps(
            {"success": report["success"], "load": report.get("load"), "evidence": str(output)}
        )
    )
    return 0 if report["success"] else 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--kubeconfig", type=Path, required=True)
    parser.add_argument(
        "--output", type=Path, default=ROOT / "docs/evidence/autoscaling-local.json"
    )
    raise SystemExit(main(parser.parse_args()))
