from typing import Dict

from .env import env


def pg_url() -> str:
    host = env("PG_HOST", "postgres")
    port = env("PG_PORT", "5432")
    db = env("PG_DB", "bigdata")
    return f"jdbc:postgresql://{host}:{port}/{db}"


def pg_props() -> Dict[str, str]:
    return {
        "user": env("PG_USER", "bigdata") or "bigdata",
        "password": env("PG_PASSWORD", "bigdata") or "bigdata",
        "driver": "org.postgresql.Driver",
    }


def ch_url() -> str:
    host = env("CH_HOST", "clickhouse")
    port = env("CH_PORT", "8123")
    db = env("CH_DB", "reports")
    return f"jdbc:clickhouse://{host}:{port}/{db}"


def ch_props() -> Dict[str, str]:
    return {
        "user": env("CH_USER", "default") or "default",
        "password": env("CH_PASSWORD", "") or "",
        "driver": "com.clickhouse.jdbc.ClickHouseDriver",
    }
