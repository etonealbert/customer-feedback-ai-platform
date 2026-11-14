"""Measure a live stack without inventing throughput or hiding failed requests."""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import os
import platform
import statistics
import subprocess
import time
from datetime import UTC, datetime
from html import escape
from pathlib import Path
from uuid import uuid4

import httpx
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
TEXTS = [
    "The mobile app crashes every time I open the checkout page.",
    "Customer support answered quickly and solved my billing problem.",
    "The new mobile application is fast and easy to use. I love it.",
    "I was charged twice and the refund has not arrived.",
    "Notifications are delayed and I keep losing my login session.",
    "The dashboard displays my account details and order history.",
    "The app freezes on my phone and drains the battery.",
    "The delivery was on time and the product quality is excellent.",
]


def command(*args, timeout=20):
    result = subprocess.run(args, cwd=ROOT, capture_output=True, text=True, timeout=timeout)
    if result.returncode:
        raise RuntimeError(result.stderr.strip() or result.stdout.strip())
    return result.stdout.strip()


def percentiles(values):
    """Linear interpolation over observed values; empty samples remain null."""
    if not values:
        return {"count": 0, "p50": None, "p95": None, "p99": None, "mean": None, "max": None}
    ordered = sorted(values)

    def percentile(q):
        index = (len(ordered) - 1) * q
        low, high = math.floor(index), math.ceil(index)
        return round(ordered[low] + (ordered[high] - ordered[low]) * (index - low), 4)

    return {
        "count": len(values),
        "p50": percentile(0.50),
        "p95": percentile(0.95),
        "p99": percentile(0.99),
        "mean": round(statistics.mean(values), 4),
        "max": round(max(values), 4),
    }


def docker_sample():
    ids = command("docker", "compose", "ps", "-q").splitlines()
    if not ids:
        return {"error": "No containers in this Compose project"}
    output = command("docker", "stats", "--no-stream", "--format", "{{json .}}", *ids)
    return {"containers": [json.loads(line) for line in output.splitlines() if line.strip()]}


async def monitor(stop, samples):
    start = time.monotonic()
    while not stop.is_set():
        try:
            sample = await asyncio.to_thread(docker_sample)
        except (OSError, RuntimeError, subprocess.TimeoutExpired) as exc:
            samples.append({"elapsed_seconds": time.monotonic() - start, "error": str(exc)})
            return
        samples.append({"elapsed_seconds": time.monotonic() - start, **sample})
        try:
            await asyncio.wait_for(stop.wait(), timeout=2)
        except TimeoutError:
            pass


async def run_workload(client, api, count, concurrency, timeout, label):
    gate = asyncio.Semaphore(concurrency)
    observations = []
    start = time.monotonic()

    async def submit(index):
        async with gate:
            sent = time.monotonic()
            row = {"index": index, "submitted_at_seconds": sent - start}
            try:
                response = await client.post(
                    api + "/v1/feedback",
                    headers={"Idempotency-Key": f"benchmark-{label}-{index}"},
                    json={
                        "text": TEXTS[index % len(TEXTS)],
                        "source": "mobile",
                        "metadata": {"benchmark": label, "sample": index},
                    },
                )
                row["ingestion_ms"] = (time.monotonic() - sent) * 1000
                row["http_status"] = response.status_code
                if response.status_code == 202:
                    row["id"] = response.json()["id"]
                else:
                    row["error"] = response.text[:500]
            except (httpx.HTTPError, ValueError, KeyError) as exc:
                row["ingestion_ms"] = (time.monotonic() - sent) * 1000
                row["error"] = f"{type(exc).__name__}: {exc}"
            observations.append(row)

    await asyncio.gather(*(submit(index) for index in range(count)))
    ingestion_seconds = time.monotonic() - start
    pending = {row["id"]: row for row in observations if "id" in row}
    deadline = time.monotonic() + timeout

    async def poll(feedback_id, row):
        async with gate:
            try:
                response = await client.get(api + "/v1/feedback/" + feedback_id)
                response.raise_for_status()
                item = response.json()
                if item["status"] in {"completed", "failed"}:
                    row["status"] = item["status"]
                    row["end_to_end_ms"] = (
                        time.monotonic() - start - row["submitted_at_seconds"]
                    ) * 1000
                    if item.get("prediction"):
                        row["inference_ms"] = item["prediction"]["inference_ms"]
                        row["model_version"] = item["prediction"]["model_version"]
                    pending.pop(feedback_id, None)
            except (httpx.HTTPError, ValueError, KeyError) as exc:
                row["last_poll_error"] = f"{type(exc).__name__}: {exc}"

    while pending and time.monotonic() < deadline:
        await asyncio.gather(*(poll(key, row) for key, row in list(pending.items())))
        if pending:
            await asyncio.sleep(0.25)
    for row in pending.values():
        row["status"] = "timed_out"
    total_seconds = time.monotonic() - start
    accepted = [row for row in observations if "id" in row]
    completed = [row for row in observations if row.get("status") == "completed"]
    return {
        "requested": count,
        "accepted": len(accepted),
        "completed": len(completed),
        "http_failed": count - len(accepted),
        "processing_failed": sum(row.get("status") == "failed" for row in observations),
        "timed_out": len(pending),
        "ingestion_window_seconds": ingestion_seconds,
        "total_window_seconds": total_seconds,
        "accepted_per_second": len(accepted) / ingestion_seconds,
        "completed_per_second": len(completed) / total_seconds,
        "ingestion_ms": percentiles([row["ingestion_ms"] for row in accepted]),
        "end_to_end_ms": percentiles([row["end_to_end_ms"] for row in completed]),
        "inference_ms": percentiles([row["inference_ms"] for row in completed]),
        "observations": sorted(observations, key=lambda row: row["index"]),
    }


def render_svg(report, path):
    """Create a portable graph from the actual successful request observations."""
    summary = report["results"]
    rows = summary["observations"]
    svg = [
        '<svg xmlns="http://www.w3.org/2000/svg" width="1100" height="680" viewBox="0 0 1100 680">',
        '<rect width="1100" height="680" fill="#f7fafc"/>',
        '<style>text{font-family:Arial,sans-serif;fill:#172b4d}.label{font-size:13px}.title{font-size:25px;font-weight:bold}</style>',
        '<text x="50" y="45" class="title">Customer feedback platform: observed load test</text>',
        f'<text x="50" y="72" class="label">{escape(report["started_at_utc"])} · '
        f'{summary["requested"]} requests · concurrency {report["configuration"]["concurrency"]}</text>',
        f'<text x="50" y="105" class="label">Accepted: {summary["accepted"]} · '
        f'Completed: {summary["completed"]} · HTTP failures: {summary["http_failed"]} · '
        f'Processing failures: {summary["processing_failed"]} · Timeouts: {summary["timed_out"]}</text>',
        f'<text x="50" y="132" class="label">Accepted throughput: {summary["accepted_per_second"]:.2f}/s · '
        f'Completed throughput: {summary["completed_per_second"]:.2f}/s</text>',
    ]
    for origin, title, key, color in (
        (50, "Ingestion latency (ms)", "ingestion_ms", "#006d77"),
        (585, "End-to-end completion (ms)", "end_to_end_ms", "#9b5de5"),
    ):
        values = sorted(float(row[key]) for row in rows if key in row and "id" in row)
        svg.append(f'<text x="{origin}" y="185" font-size="17">{title}</text>')
        x0, y0, width, height = origin + 55, 465, 400, 240
        maximum = max(values, default=1) or 1
        for tick in range(5):
            y = y0 - height * tick / 4
            svg.append(f'<path d="M{x0} {y}h{width}" stroke="#d5dee8"/>')
            svg.append(
                f'<text x="{x0-7}" y="{y+4}" text-anchor="end" class="label">'
                f'{maximum*tick/4:.0f}</text>'
            )
        if values:
            points = " ".join(
                f"{x0 + width * index / max(1, len(values)-1):.1f},{y0-height*value/maximum:.1f}"
                for index, value in enumerate(values)
            )
            svg.append(f'<polyline points="{points}" fill="none" stroke="{color}" stroke-width="3"/>')
        else:
            svg.append(f'<text x="{x0+60}" y="350" class="label">No successful observations</text>')
        for tick in range(5):
            svg.append(
                f'<text x="{x0+width*tick/4}" y="490" text-anchor="middle" class="label">'
                f'{tick*25}%</text>'
            )
        svg.append(f'<text x="{x0+120}" y="515" class="label">Observed percentile</text>')
        p = summary[key]
        values_text = " · ".join(
            f"{name}: {p[name]:.2f}" if p[name] is not None else f"{name}: n/a"
            for name in ("p50", "p95", "p99")
        )
        svg.append(f'<text x="{origin}" y="550" class="label">{values_text}</text>')
    svg.extend(
        [
            '<text x="50" y="600" class="label">Completion latency includes polling and is an upper bound. Synthetic workload; no production capacity claim.</text>',
            '<text x="50" y="625" class="label">CPU / memory time series, raw samples, configuration and provenance are in the adjacent JSON file.</text>',
            '</svg>',
        ]
    )
    path.write_text("\n".join(svg), encoding="utf-8")


async def main(args):
    load_dotenv(ROOT / ".env")
    key = os.getenv("DEMO_TENANT_A_KEY")
    if not key:
        raise SystemExit("DEMO_TENANT_A_KEY is required; run scripts/setup_env.py first")
    label = str(uuid4())
    report = {
        "schema_version": 1,
        "run_id": label,
        "started_at_utc": datetime.now(UTC).isoformat(),
        "configuration": vars(args).copy(),
        "host": {"platform": platform.platform(), "python": platform.python_version(), "cpu_count": os.cpu_count()},
        "methodology": {
            "latency_units": "milliseconds",
            "ingestion": "time from acquiring concurrency slot until HTTP response; accepted requests only",
            "completion": "time from send until first observed completed response; polling upper bound",
            "timeout": "processing deadline begins after ingestion finishes",
            "cpu_memory": "docker stats sampled approximately every 2 seconds; raw Docker units",
        },
    }
    try:
        report["git"] = {"commit": command("git", "rev-parse", "HEAD"), "dirty": bool(command("git", "status", "--porcelain"))}
    except (OSError, RuntimeError, subprocess.TimeoutExpired) as exc:
        report["git"] = {"unavailable": str(exc)}
    try:
        report["compose_ps"] = command("docker", "compose", "ps", "--format", "json")
    except (OSError, RuntimeError, subprocess.TimeoutExpired) as exc:
        report["compose_ps"] = {"unavailable": str(exc)}
    stop, samples = asyncio.Event(), []
    monitor_task = asyncio.create_task(monitor(stop, samples)) if not args.no_stats else None
    try:
        limits = httpx.Limits(max_connections=args.concurrency, max_keepalive_connections=args.concurrency)
        async with httpx.AsyncClient(headers={"Authorization": f"Bearer {key}"}, timeout=30, limits=limits) as client:
            if args.warmup:
                report["warmup"] = await run_workload(client, args.api, args.warmup, args.concurrency, args.timeout, label + "-warmup")
            report["results"] = await run_workload(client, args.api, args.count, args.concurrency, args.timeout, label)
    finally:
        stop.set()
        if monitor_task:
            await monitor_task
    report["docker_stats"] = samples
    report["finished_at_utc"] = datetime.now(UTC).isoformat()
    prefix = Path(args.output)
    prefix.parent.mkdir(parents=True, exist_ok=True)
    prefix.with_suffix(".json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    render_svg(report, prefix.with_suffix(".svg"))
    summary = {key: value for key, value in report["results"].items() if key != "observations"}
    print(json.dumps(summary, indent=2))
    print(f"Evidence: {prefix.with_suffix('.json')} and {prefix.with_suffix('.svg')}")
    return int(report["results"]["completed"] != args.count)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--api", default=os.getenv("TEST_API_URL", "http://localhost:8000"))
    parser.add_argument("--count", type=int, default=300)
    parser.add_argument("--concurrency", type=int, default=20)
    parser.add_argument("--timeout", type=float, default=180)
    parser.add_argument("--warmup", type=int, default=5)
    parser.add_argument("--no-stats", action="store_true")
    parser.add_argument("--output", default="docs/evidence/load-local")
    arguments = parser.parse_args()
    if arguments.count < 1 or arguments.concurrency < 1 or arguments.timeout <= 0 or arguments.warmup < 0:
        parser.error("count and concurrency must be positive; timeout > 0; warmup >= 0")
    raise SystemExit(asyncio.run(main(arguments)))
