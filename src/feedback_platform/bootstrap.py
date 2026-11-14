"""Idempotent administrator-only schema setup and local demo tenant provisioning."""
from pathlib import Path
from uuid import UUID

import psycopg
from psycopg import sql

from feedback_platform.auth import hash_key
from feedback_platform.settings import get_settings

TENANT_A = UUID("11111111-1111-4111-8111-111111111111")
TENANT_B = UUID("22222222-2222-4222-8222-222222222222")


def main():
    settings = get_settings()
    if not settings.admin_database_url or len(settings.app_db_password) < 16:
        raise RuntimeError("ADMIN_DATABASE_URL and a strong APP_DB_PASSWORD are required")
    with psycopg.connect(settings.admin_database_url) as conn:
        conn.execute("SELECT pg_advisory_xact_lock(7328541)")
        if not conn.execute("SELECT 1 FROM pg_roles WHERE rolname='feedback_app'").fetchone():
            conn.execute(sql.SQL("CREATE ROLE feedback_app LOGIN PASSWORD {}").format(
                sql.Literal(settings.app_db_password)
            ))
        conn.execute("ALTER ROLE feedback_app NOSUPERUSER NOBYPASSRLS NOCREATEDB NOCREATEROLE")
        conn.execute(Path(__file__).with_name("schema.sql").read_text(encoding="utf-8"))
        for tenant_id, name, key in [
            (TENANT_A, "Acme Mobile", settings.demo_tenant_a_key),
            (TENANT_B, "Other Company", settings.demo_tenant_b_key),
        ]:
            if not key:
                continue
            if len(key) < 24:
                raise RuntimeError("Demo API keys must contain at least 24 characters")
            conn.execute("INSERT INTO tenants(id,name) VALUES(%s,%s) ON CONFLICT DO NOTHING", (tenant_id, name))
            conn.execute("INSERT INTO api_keys(key_hash,tenant_id) VALUES(%s,%s) ON CONFLICT DO NOTHING", (hash_key(key), tenant_id))
    print("Schema version 1 applied; configured demo tenants are ready.")


if __name__ == "__main__":
    main()
