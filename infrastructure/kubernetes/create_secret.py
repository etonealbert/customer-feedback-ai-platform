"""Create the development namespace and Secret without printing its values.

Usage: python infrastructure/kubernetes/create_secret.py --env-file .env
Uses local kubectl credentials and modifies only namespace feedback-ai.
"""

import argparse
import json
import subprocess
from pathlib import Path
from urllib.parse import quote


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    args = parser.parse_args()
    values = {}
    for line in args.env_file.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            values[key] = value.strip().strip("\"'")
    required = (
        "POSTGRES_PASSWORD",
        "APP_DB_PASSWORD",
        "INTERNAL_API_KEY",
        "DEMO_TENANT_A_KEY",
        "DEMO_TENANT_B_KEY",
        "GRAFANA_PASSWORD",
    )
    missing = [name for name in required if not values.get(name)]
    if missing:
        raise SystemExit("Missing .env keys: " + ", ".join(missing))
    values["DATABASE_URL"] = (
        "postgresql://feedback_app:"
        + quote(values["APP_DB_PASSWORD"], safe="")
        + "@postgres:5432/feedback"
    )
    values["ADMIN_DATABASE_URL"] = (
        "postgresql://postgres:"
        + quote(values["POSTGRES_PASSWORD"], safe="")
        + "@postgres:5432/feedback"
    )
    namespace = {"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "feedback-ai"}}
    # Only this Job receives ADMIN_DATABASE_URL. Application deployments select individual keys.
    secret = {
        "apiVersion": "v1",
        "kind": "Secret",
        "type": "Opaque",
        "metadata": {"name": "feedback-secrets", "namespace": "feedback-ai"},
        "stringData": values,
    }
    for resource in (namespace, secret):
        subprocess.run(
            ["kubectl", "apply", "-f", "-"],
            input=json.dumps(resource),
            text=True,
            check=True,
        )


if __name__ == "__main__":
    main()
