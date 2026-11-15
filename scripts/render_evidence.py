"""Render comparative Matplotlib figures from retained live benchmark observations."""

from __future__ import annotations

import argparse
import json
import os
import re
import statistics
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
os.environ.setdefault("MPLCONFIGDIR", str(ROOT / "work" / "matplotlib"))

import matplotlib  # noqa: E402

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402

RUNS = (
    ("load-1worker.json", "baseline"),
    ("load-3workers.json", "scaled"),
    ("load-burst-3workers.json", "burst"),
)
COLORS = ("#087e8b", "#e19c24", "#7851a9")
SERVICE = re.compile(
    r"-(ingestion|inference|worker|outbox|postgres|kafka|rag|prometheus|grafana)-\d+$"
)


def memory_mib(value):
    match = re.fullmatch(r"\s*([\d.]+)\s*(B|KiB|MiB|GiB|TiB|kB|MB|GB)\s*", value)
    if not match:
        raise ValueError(f"Unsupported Docker memory unit: {value!r}")
    number, unit = match.groups()
    units = {
        "B": 1,
        "KiB": 1024,
        "MiB": 1024**2,
        "GiB": 1024**3,
        "TiB": 1024**4,
        "kB": 1000,
        "MB": 1000**2,
        "GB": 1000**3,
    }
    return float(number) * units[unit] / 1024**2


def resource_summary(report):
    """Sum replicas within a snapshot, then calculate sample means and observed maxima."""
    values = defaultdict(lambda: {"cpu_percent": [], "memory_mib": []})
    times, errors = [], []
    for snapshot in report["docker_stats"]:
        if "containers" not in snapshot:
            errors.append(snapshot.get("error", "Missing containers"))
            continue
        times.append(snapshot["elapsed_seconds"])
        totals = defaultdict(lambda: {"cpu_percent": 0.0, "memory_mib": 0.0})
        for container in snapshot["containers"]:
            match = SERVICE.search(container["Name"])
            service = match.group(1) if match else container["Name"]
            cpu = float(container["CPUPerc"].removesuffix("%"))
            memory = memory_mib(container["MemUsage"].split("/")[0])
            for name in (service, "all_containers"):
                totals[name]["cpu_percent"] += cpu
                totals[name]["memory_mib"] += memory
        for service, sample in totals.items():
            for metric, value in sample.items():
                values[service][metric].append(value)
    services = {
        service: {
            metric: {"mean": statistics.mean(samples), "max": max(samples), "samples": len(samples)}
            for metric, samples in metrics.items()
        }
        for service, metrics in values.items()
    }
    return {
        "snapshot_count": len(times),
        "sample_elapsed_seconds": times,
        "interval_seconds": [b - a for a, b in zip(times, times[1:])],
        "errors": errors,
        "services": services,
    }


def grouped_bars(axis, labels, series, ylabel, title):
    positions = np.arange(len(labels))
    width = 0.72 / len(series)
    for index, (name, data) in enumerate(series):
        offset = (index - (len(series) - 1) / 2) * width
        bars = axis.bar(positions + offset, data, width, label=name, color=COLORS[index])
        axis.bar_label(bars, fmt="%.1f", padding=3, fontsize=8)
    axis.set_xticks(positions, labels, fontsize=9)
    axis.set_ylabel(ylabel)
    axis.set_title(title, loc="left", fontsize=12, weight="bold", pad=13)
    axis.grid(axis="y", alpha=0.2)
    axis.set_axisbelow(True)
    axis.margins(y=0.23)
    axis.legend(loc="upper left", frameon=False, ncols=len(series), fontsize=8)
    axis.spines[["top", "right"]].set_visible(False)


def main(args):
    directory = Path(args.evidence_dir)
    reports = [json.loads((directory / name).read_text(encoding="utf-8")) for name, _ in RUNS]
    summaries = [resource_summary(report) for report in reports]
    if any("all_containers" not in summary["services"] for summary in summaries):
        raise SystemExit("Every run needs Docker resource samples for this comparison")
    results = [report["results"] for report in reports]
    labels = []
    inference_counts = []
    for report, (_, name) in zip(reports, RUNS):
        replicas = {}
        for service in ("worker", "inference"):
            replicas[service] = max(
                sum(f"-{service}-" in item["Name"] for item in sample.get("containers", []))
                for sample in report["docker_stats"]
            )
        inference_counts.append(str(replicas["inference"]))
        count = report["results"]["requested"]
        concurrency = report["configuration"]["concurrency"]
        noun = "worker" if replicas["worker"] == 1 else "workers"
        suffix = ", burst" if name == "burst" else ""
        labels.append(f"{replicas['worker']} {noun}{suffix}\n{count:,} requests / C{concurrency}")
    plt.rcParams.update(
        {
            "font.family": "DejaVu Sans",
            "font.size": 10,
            "axes.labelsize": 10,
            "svg.fonttype": "none",
            "figure.facecolor": "white",
            "savefig.facecolor": "white",
        }
    )
    figure, axes = plt.subplots(3, 2, figsize=(14.5, 13.5))
    figure.subplots_adjust(left=0.075, right=0.97, top=0.89, bottom=0.13, hspace=0.62, wspace=0.26)
    figure.suptitle(
        "Customer feedback platform · measured Compose batches",
        x=0.075,
        y=0.975,
        ha="left",
        fontsize=20,
        weight="bold",
    )
    commits = ", ".join(sorted({report["git"]["commit"][:7] for report in reports}))
    warmups = "/".join(str(report["configuration"]["warmup"]) for report in reports)
    inference_replicas = "/".join(inference_counts)
    figure.text(
        0.075,
        0.946,
        f"Source {commits} · warmup requests: {warmups} · inference replicas: {inference_replicas} · host details in raw JSON",
        ha="left",
        fontsize=11,
        color="#394b5a",
    )
    errors = [r["http_failed"] + r["processing_failed"] + r["timed_out"] for r in results]
    figure.text(
        0.075,
        0.924,
        "Completed: "
        + " / ".join(f"{r['completed']:,} of {r['requested']:,}" for r in results)
        + " · failed or unfinished: "
        + " / ".join(map(str, errors)),
        ha="left",
        fontsize=10,
    )
    grouped_bars(
        axes[0, 0],
        labels,
        [
            ("Accepted", [r["accepted_per_second"] for r in results]),
            ("Completed", [r["completed_per_second"] for r in results]),
        ],
        "Items / second",
        "Observed throughput",
    )
    for axis, metric, scale, ylabel, title in (
        (axes[0, 1], "ingestion_ms", 1, "Milliseconds", "HTTP ingestion latency"),
        (axes[1, 0], "end_to_end_ms", 1000, "Seconds", "Observed completion (polling upper bound)"),
        (axes[1, 1], "inference_ms", 1, "Milliseconds", "Model inference duration"),
    ):
        grouped_bars(
            axis,
            labels,
            [(q, [r[metric][q] / scale for r in results]) for q in ("p50", "p95", "p99")],
            ylabel,
            title,
        )
    for axis, metric, ylabel, title in (
        (
            axes[2, 0],
            "cpu_percent",
            "Docker CPU % (100% = one logical CPU)",
            "All containers: sampled CPU",
        ),
        (axes[2, 1], "memory_mib", "MiB", "All containers: sampled memory"),
    ):
        grouped_bars(
            axis,
            labels,
            [
                (stat, [s["services"]["all_containers"][metric][stat] for s in summaries])
                for stat in ("mean", "max")
            ],
            ylabel,
            title,
        )
    counts = ", ".join(str(summary["snapshot_count"]) for summary in summaries)
    gaps = [gap for summary in summaries for gap in summary["interval_seconds"]]
    interval = f"{min(gaps):.2f}–{max(gaps):.2f}s apart" if gaps else "interval not available"
    figure.text(
        0.075,
        0.075,
        f"Resource snapshots/run: {counts}; include warmup/cooldown, {interval}, not time-weighted.\n"
        "One short batch per setting, reused database, no confidence intervals. No saturation ceiling established.\n"
        "Only the recorded source revisions are measured. Version changes and full methodology: docs/benchmarks.md.",
        ha="left",
        va="top",
        fontsize=10,
        linespacing=1.6,
        color="#394b5a",
    )
    output = Path(args.output_prefix)
    output.parent.mkdir(parents=True, exist_ok=True)
    figure.savefig(output.with_suffix(".svg"))
    figure.savefig(output.with_suffix(".png"), dpi=160)
    plt.close(figure)
    print(json.dumps({name: summary for (name, _), summary in zip(RUNS, summaries)}, indent=2))
    print(f"Rendered {output.with_suffix('.svg')} and {output.with_suffix('.png')}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence-dir", default=str(ROOT / "docs" / "evidence"))
    parser.add_argument(
        "--output-prefix", default=str(ROOT / "docs" / "evidence" / "benchmark-comparison")
    )
    main(parser.parse_args())
