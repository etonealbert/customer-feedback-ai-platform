"""Apply namespace-local monitoring using the canonical Compose configurations."""

import json
import subprocess
from pathlib import Path


def main() -> None:
    root = Path(__file__).resolve().parents[3]
    config_maps = {
        "feedback-prometheus": {
            "prometheus.yml": root / "monitoring/prometheus/prometheus-kubernetes.yml",
            "alerts.yml": root / "monitoring/prometheus/alerts.yml",
        },
        "feedback-grafana-datasources": {
            "prometheus.yml": root / "monitoring/grafana/provisioning/datasources/prometheus.yml",
        },
        "feedback-grafana-providers": {
            "feedback.yml": root / "monitoring/grafana/provisioning/dashboards/feedback.yml",
        },
        "feedback-grafana-dashboard": {
            "feedback.json": root / "monitoring/grafana/dashboards/feedback.json",
        },
    }
    for name, files in config_maps.items():
        resource = {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"name": name, "namespace": "feedback-ai"},
            "data": {key: path.read_text(encoding="utf-8") for key, path in files.items()},
        }
        subprocess.run(
            ["kubectl", "apply", "-f", "-"],
            input=json.dumps(resource),
            text=True,
            check=True,
        )
    subprocess.run(
        ["kubectl", "apply", "-f", str(Path(__file__).with_name("monitoring.yaml"))], check=True
    )


if __name__ == "__main__":
    main()
