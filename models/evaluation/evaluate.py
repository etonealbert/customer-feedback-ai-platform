"""Evaluate both versions on the committed, excluded holdout split.

Run after installing the project: python models/evaluation/evaluate.py
The report contains observed results, never preset benchmark numbers.
"""

import argparse
import json
import platform
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter

import numpy as np
import sklearn
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix, f1_score

from feedback_platform.ml import MODEL_VERSIONS, get_registry, load_corpus


def evaluate() -> dict:
    started = perf_counter()
    registry = get_registry()
    training_seconds = perf_counter() - started
    test = [row for row in load_corpus() if row["split"] == "test"]
    reports = {}
    all_predictions = {}
    for version in MODEL_VERSIONS:
        predictions = [registry.predict(row["text"], version) for row in test]
        all_predictions[version] = predictions
        targets = {}
        for target in ("sentiment", "topic"):
            truth = [row[target] for row in test]
            predicted = [
                result["sentiment"] if target == "sentiment" else result["topics"][0]
                for result in predictions
            ]
            labels = sorted(set(truth))
            targets[target] = {
                "accuracy": accuracy_score(truth, predicted),
                "macro_f1": f1_score(truth, predicted, average="macro"),
                "labels": labels,
                "confusion_matrix": confusion_matrix(truth, predicted, labels=labels).tolist(),
                "classification_report": classification_report(
                    truth, predicted, output_dict=True, zero_division=0
                ),
            }
        duration = [row["inference_ms"] for row in predictions]
        reports[version] = {
            **targets,
            "heldout_inference_ms": {
                "p50": float(np.percentile(duration, 50)),
                "p95": float(np.percentile(duration, 95)),
                "p99": float(np.percentile(duration, 99)),
            },
            "predictions": [
                {
                    "id": row["id"],
                    "text": row["text"],
                    "expected_sentiment": row["sentiment"],
                    "actual_sentiment": result["sentiment"],
                    "expected_topic": row["topic"],
                    "actual_topic": result["topics"][0],
                    "confidence": result["confidence"],
                }
                for row, result in zip(test, predictions, strict=True)
            ],
        }
    differences = [
        {
            "id": row["id"],
            "text": row["text"],
            "v1_sentiment": first["sentiment"],
            "v2_sentiment": second["sentiment"],
            "v1_topic": first["topics"][0],
            "v2_topic": second["topics"][0],
        }
        for row, first, second in zip(test, all_predictions["v1"], all_predictions["v2"], strict=True)
        if first["sentiment"] != second["sentiment"] or first["topics"] != second["topics"]
    ]
    return {
        "measured_at_utc": datetime.now(timezone.utc).isoformat(),
        "python": platform.python_version(),
        "platform": platform.platform(),
        "scikit_learn": sklearn.__version__,
        "training_seconds": training_seconds,
        "metadata": registry.metadata(),
        "versions": reports,
        "changed_predictions": differences,
        "interpretation": (
            "Observed results on 18 hand-authored synthetic holdout examples. "
            "This is a functional smoke test, not a production quality estimate. "
            "Latency here is a tiny in-process sample, not a throughput benchmark."
        ),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=Path(__file__).with_name("results.json"))
    args = parser.parse_args()
    report = evaluate()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    for version, metrics in report["versions"].items():
        print(
            f"{version}: sentiment accuracy={metrics['sentiment']['accuracy']:.3f}; "
            f"topic accuracy={metrics['topic']['accuracy']:.3f}; "
            f"inference p95={metrics['heldout_inference_ms']['p95']:.3f} ms"
        )
    print(f"Wrote measured report to {args.output}")


if __name__ == "__main__":
    main()
