"""Create local secrets without displaying them or replacing an existing .env."""

import secrets
from pathlib import Path

root = Path(__file__).resolve().parents[1]
destination = root / ".env"
keys = [
    "POSTGRES_PASSWORD",
    "APP_DB_PASSWORD",
    "INTERNAL_API_KEY",
    "DEMO_TENANT_A_KEY",
    "DEMO_TENANT_B_KEY",
    "GRAFANA_PASSWORD",
]
if destination.exists():
    print(".env already exists; preserved.")
else:
    with destination.open("x", encoding="utf-8") as stream:
        stream.write("# Generated local development secrets; never commit this file.\n")
        stream.writelines(f"{key}={secrets.token_hex(24)}\n" for key in keys)
        stream.write("MODEL_VERSION=v1\nLLM_PROVIDER=extractive\nOPENAI_API_KEY=\nOPENAI_MODEL=\n")
    print("Created .env with random local credentials.")
