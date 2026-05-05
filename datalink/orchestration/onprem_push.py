"""OnPrem push — Phase 16.8 (real fan-out to downstream products).

Replaces the legacy stub in onprem_push_task. For each routing target on the
pipeline instance:
  * postgres        → real psycopg connection, CREATE TABLE, INSERT
  * sqlserver       → real pyodbc / pymssql connection, CREATE TABLE, INSERT
  * snowflake_share → in-Snowflake CREATE SCHEMA + INSERT (demo equivalent —
                      a real Snowflake share would be a SHARE object)

Each push records a row in CONTROL.egress_batch_log with status (PUSHED /
FAILED / NOOP) and row_count. Failures are caught + logged but don't
abort the task — the platform's design is "best-effort fan-out, alert on
errors". Per-target failures are visible in egress_batch_log.

Connection details for downstream targets come from environment vars:
  POSTGRES_ONPREM_DSN       e.g. "postgresql://datalink:datalink@datalink-postgres:5432/postgres"
  SQLSERVER_ONPREM_DSN      e.g. "Server=datalink-sqlserver,1433;UID=sa;PWD=Strong!Password1"
  SNOWFLAKE_SHARE_SCHEMA    e.g. "ONPREM_SHARES" (created in DATALINK_DEV)

If a DSN env var is unset, the target is recorded as NOOP (skipped) so the
DAG still succeeds end-to-end.
"""

from __future__ import annotations

import os
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from datalink.adapters.protocols import Warehouse
from datalink.logging import get_logger
from datalink.quality.control import CONTROL_SCHEMA

_log = get_logger(__name__)


@dataclass
class PushResult:
    target_name: str
    target_system: str
    status: str  # 'PUSHED' | 'FAILED' | 'NOOP'
    row_count: int
    error: str | None = None
    duration_ms: int = 0


def _now() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


def _read_gold_rows(
    wh: Warehouse,
    *,
    fq_gold_table: str,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    """Pull all Gold rows for the push. For demo purposes we read EVERYTHING;
    a real impl would use a watermark + delta query."""
    sql = f"SELECT * FROM {fq_gold_table}"
    if limit:
        sql += f" LIMIT {int(limit)}"
    return list(wh.query(sql))


def _column_types(wh: Warehouse, fq_gold_table: str) -> list[tuple[str, str]]:
    """Return [(column_name, snowflake_type), ...] in declaration order."""
    sch, _, tbl = fq_gold_table.partition(".")
    rows = list(
        wh.query(
            f"SELECT column_name, data_type FROM INFORMATION_SCHEMA.COLUMNS "
            f"WHERE TABLE_SCHEMA = '{sch}' AND TABLE_NAME = '{tbl.upper()}' "
            f"ORDER BY ordinal_position"
        )
    )
    return [(str(r["column_name"]), str(r["data_type"])) for r in rows]


def _sf_type_to_postgres(t: str) -> str:
    t = t.upper()
    if "VARCHAR" in t or "TEXT" in t or "STRING" in t:
        return "TEXT"
    if "INTEGER" in t or ("NUMBER" in t and "(" not in t):
        return "BIGINT"
    if "FLOAT" in t or "DOUBLE" in t or "REAL" in t:
        return "DOUBLE PRECISION"
    if "DECIMAL" in t or "NUMERIC" in t or "NUMBER(" in t:
        return "NUMERIC"
    if t == "DATE":
        return "DATE"
    if "TIMESTAMP" in t:
        return "TIMESTAMP"
    if "BOOLEAN" in t:
        return "BOOLEAN"
    if "VARIANT" in t or "OBJECT" in t or "ARRAY" in t:
        return "JSONB"
    return "TEXT"


def _sf_type_to_sqlserver(t: str) -> str:
    t = t.upper()
    if "VARCHAR" in t or "TEXT" in t or "STRING" in t:
        return "NVARCHAR(MAX)"
    if "INTEGER" in t or ("NUMBER" in t and "(" not in t):
        return "BIGINT"
    if "FLOAT" in t or "DOUBLE" in t or "REAL" in t:
        return "FLOAT"
    if "DECIMAL" in t or "NUMERIC" in t or "NUMBER(" in t:
        return "DECIMAL(38, 6)"
    if t == "DATE":
        return "DATE"
    if "TIMESTAMP" in t:
        return "DATETIME2"
    if "BOOLEAN" in t:
        return "BIT"
    if "VARIANT" in t or "OBJECT" in t or "ARRAY" in t:
        return "NVARCHAR(MAX)"
    return "NVARCHAR(MAX)"


# ---------------------------------------------------------------------------
# Postgres pusher
# ---------------------------------------------------------------------------


def _push_postgres(
    *,
    target_name: str,
    target_uri: str,
    client_id: str,
    dataset_code: str,
    fq_gold_table: str,
    columns: list[tuple[str, str]],
    rows: list[dict[str, Any]],
) -> PushResult:
    """Push rows to a downstream Postgres target. Connection from env, table
    auto-created with column-name match."""
    started = datetime.now(UTC)
    dsn = os.environ.get("POSTGRES_ONPREM_DSN") or os.environ.get("DL_POSTGRES_ONPREM_DSN")
    if not dsn:
        # Fall back to the docker-compose default — our Postgres container.
        dsn = "postgresql://datalink:datalink@datalink-postgres:5432/datalink_um"

    try:
        import psycopg
        from psycopg import sql as psql
    except ImportError:
        return PushResult(
            target_name=target_name,
            target_system="postgres",
            status="FAILED",
            row_count=0,
            error="psycopg not installed in this environment",
        )

    schema_name = f"onprem_{target_name.lower()}"
    table_name = f"{client_id}_{dataset_code}".lower()

    # Column DDL
    col_ddl = ", ".join(f'"{name.lower()}" {_sf_type_to_postgres(t)}' for name, t in columns)

    try:
        with (
            psycopg.connect(dsn, autocommit=True, connect_timeout=10) as conn,
            conn.cursor() as cur,
        ):
            cur.execute(
                psql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(psql.Identifier(schema_name))
            )
            cur.execute(f'DROP TABLE IF EXISTS "{schema_name}"."{table_name}"')
            cur.execute(f'CREATE TABLE "{schema_name}"."{table_name}" ({col_ddl})')
            if rows:
                cols = [name.lower() for name, _ in columns]
                placeholders = ", ".join(["%s"] * len(cols))
                quoted_cols = ", ".join('"' + c + '"' for c in cols)
                insert_sql = (
                    f'INSERT INTO "{schema_name}"."{table_name}" '
                    f"({quoted_cols}) "
                    f"VALUES ({placeholders})"
                )
                batch = [tuple(r.get(c) or r.get(c.upper()) for c in cols) for r in rows]
                cur.executemany(insert_sql, batch)
        duration_ms = int((datetime.now(UTC) - started).total_seconds() * 1000)
        return PushResult(
            target_name=target_name,
            target_system="postgres",
            status="PUSHED",
            row_count=len(rows),
            duration_ms=duration_ms,
        )
    except Exception as exc:
        return PushResult(
            target_name=target_name,
            target_system="postgres",
            status="FAILED",
            row_count=0,
            error=f"{type(exc).__name__}: {str(exc)[:200]}",
            duration_ms=int((datetime.now(UTC) - started).total_seconds() * 1000),
        )


# ---------------------------------------------------------------------------
# SQL Server pusher
# ---------------------------------------------------------------------------


def _push_sqlserver(
    *,
    target_name: str,
    target_uri: str,
    client_id: str,
    dataset_code: str,
    fq_gold_table: str,
    columns: list[tuple[str, str]],
    rows: list[dict[str, Any]],
) -> PushResult:
    started = datetime.now(UTC)
    server = os.environ.get("SQLSERVER_ONPREM_HOST", "datalink-sqlserver")
    user = os.environ.get("SQLSERVER_ONPREM_USER", "sa")
    password = os.environ.get(
        "SQLSERVER_ONPREM_PASSWORD",
        os.environ.get("MSSQL_SA_PASSWORD", "Datalink!2026"),
    )
    database = os.environ.get("SQLSERVER_ONPREM_DATABASE", "DataLinkUM")

    try:
        import pymssql
    except ImportError:
        return PushResult(
            target_name=target_name,
            target_system="sqlserver",
            status="FAILED",
            row_count=0,
            error="pymssql not installed in this environment",
        )

    schema_name = f"onprem_{target_name.lower()}"
    table_name = f"{client_id}_{dataset_code}".lower()

    col_ddl = ", ".join(f"[{name.lower()}] {_sf_type_to_sqlserver(t)}" for name, t in columns)

    try:
        with pymssql.connect(
            server=server,
            user=user,
            password=password,
            database=database,
            autocommit=True,
            login_timeout=10,
        ) as conn:
            cur = conn.cursor()
            # Schema + table
            cur.execute(
                f"IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = '{schema_name}') "
                f"EXEC('CREATE SCHEMA [{schema_name}]')"
            )
            cur.execute(
                f"IF OBJECT_ID('[{schema_name}].[{table_name}]', 'U') IS NOT NULL "
                f"DROP TABLE [{schema_name}].[{table_name}]"
            )
            cur.execute(f"CREATE TABLE [{schema_name}].[{table_name}] ({col_ddl})")
            if rows:
                cols = [name.lower() for name, _ in columns]
                placeholders = ", ".join(["%s"] * len(cols))
                insert_sql = (
                    f"INSERT INTO [{schema_name}].[{table_name}] "
                    f"({', '.join(f'[{c}]' for c in cols)}) "
                    f"VALUES ({placeholders})"
                )
                batch = [tuple(r.get(c) or r.get(c.upper()) for c in cols) for r in rows]
                cur.executemany(insert_sql, batch)
            cur.close()
        duration_ms = int((datetime.now(UTC) - started).total_seconds() * 1000)
        return PushResult(
            target_name=target_name,
            target_system="sqlserver",
            status="PUSHED",
            row_count=len(rows),
            duration_ms=duration_ms,
        )
    except Exception as exc:
        return PushResult(
            target_name=target_name,
            target_system="sqlserver",
            status="FAILED",
            row_count=0,
            error=f"{type(exc).__name__}: {str(exc)[:200]}",
            duration_ms=int((datetime.now(UTC) - started).total_seconds() * 1000),
        )


# ---------------------------------------------------------------------------
# Snowflake share pusher (demo equivalent — same Snowflake, dedicated schema)
# ---------------------------------------------------------------------------


def _push_snowflake_share(
    *,
    wh: Warehouse,
    target_name: str,
    client_id: str,
    dataset_code: str,
    fq_gold_table: str,
    columns: list[tuple[str, str]],
) -> PushResult:
    started = datetime.now(UTC)
    share_db = os.environ.get("SNOWFLAKE_DATABASE", "DATALINK_DEV")
    share_schema = f"SHARE_{target_name.upper()}"

    table_name = f"{client_id.upper()}_{dataset_code.upper()}"
    fq_target = f"{share_db}.{share_schema}.{table_name}"

    try:
        wh.execute(f"CREATE SCHEMA IF NOT EXISTS {share_db}.{share_schema}")
        wh.execute(f"DROP TABLE IF EXISTS {fq_target}")
        wh.execute(f"CREATE TABLE {fq_target} AS SELECT * FROM {fq_gold_table}")
        n = next(iter(wh.query(f"SELECT COUNT(*) c FROM {fq_target}")))["c"]
        duration_ms = int((datetime.now(UTC) - started).total_seconds() * 1000)
        return PushResult(
            target_name=target_name,
            target_system="snowflake_share",
            status="PUSHED",
            row_count=int(n),
            duration_ms=duration_ms,
        )
    except Exception as exc:
        return PushResult(
            target_name=target_name,
            target_system="snowflake_share",
            status="FAILED",
            row_count=0,
            error=f"{type(exc).__name__}: {str(exc)[:200]}",
            duration_ms=int((datetime.now(UTC) - started).total_seconds() * 1000),
        )


# ---------------------------------------------------------------------------
# Public entrypoint — called by onprem_push_task
# ---------------------------------------------------------------------------


def push_to_targets(
    *,
    warehouse: Warehouse,
    client_id: str,
    dataset_code: str,
    targets: list[dict[str, Any]],
    pipeline_run_id: str | None = None,
) -> dict[str, Any]:
    """Push the LIVE Gold table for (client_id, dataset_code) to every
    routing target. Returns aggregate summary + per-target results.

    Each target dict follows the proposal's ``routing_plan`` shape:
        {"downstream_product": "CC", "target_system": "postgres",
         "target_uri": "postgres://care_compass.onprem", "action": "ENABLE"}
    """
    fq_gold = f"GOLD_{client_id.upper()}.{dataset_code.lower()}"

    # Fetch source data once
    columns = _column_types(warehouse, fq_gold)
    rows = _read_gold_rows(warehouse, fq_gold_table=fq_gold)
    run_id = pipeline_run_id or str(uuid.uuid4())

    results: list[PushResult] = []
    for tgt in targets:
        if str(tgt.get("action", "")).startswith("DISABLED"):
            continue  # client opted out of this downstream
        target_name = str(tgt.get("downstream_product") or "")
        target_system = str(tgt.get("target_system") or "").lower()

        if target_system == "postgres":
            r = _push_postgres(
                target_name=target_name,
                target_uri=str(tgt.get("target_uri") or ""),
                client_id=client_id,
                dataset_code=dataset_code,
                fq_gold_table=fq_gold,
                columns=columns,
                rows=rows,
            )
        elif target_system == "sqlserver":
            r = _push_sqlserver(
                target_name=target_name,
                target_uri=str(tgt.get("target_uri") or ""),
                client_id=client_id,
                dataset_code=dataset_code,
                fq_gold_table=fq_gold,
                columns=columns,
                rows=rows,
            )
        elif target_system in ("snowflake_share", "snowflake"):
            r = _push_snowflake_share(
                wh=warehouse,
                target_name=target_name,
                client_id=client_id,
                dataset_code=dataset_code,
                fq_gold_table=fq_gold,
                columns=columns,
            )
        else:
            r = PushResult(
                target_name=target_name,
                target_system=target_system,
                status="NOOP",
                row_count=0,
                error=f"Unknown target_system: {target_system!r}",
            )

        # Audit log entry per target
        try:
            warehouse.execute(
                f"INSERT INTO {CONTROL_SCHEMA}.egress_batch_log "
                f"(egress_batch_id, pipeline_run_id, client_id, entity, "
                f" target, row_count, started_at, finished_at, status, error) "
                f"SELECT $bid, $rid, $cid, $ent, $tgt, $n, $st, $ft, $stt, $err",
                {
                    "bid": str(uuid.uuid4()),
                    "rid": run_id,
                    "cid": client_id,
                    "ent": dataset_code,
                    "tgt": f"{r.target_system}:{r.target_name}",
                    "n": r.row_count,
                    "st": _now(),
                    "ft": _now(),
                    "stt": r.status,
                    "err": r.error,
                },
            )
        except Exception as exc:
            _log.warning("onprem.audit_failed", err=str(exc)[:200])

        results.append(r)

    pushed = sum(1 for r in results if r.status == "PUSHED")
    failed = sum(1 for r in results if r.status == "FAILED")
    noop = sum(1 for r in results if r.status == "NOOP")
    total_rows = sum(r.row_count for r in results)

    _log.info(
        "onprem.fanout_complete",
        client_id=client_id,
        dataset_code=dataset_code,
        pushed=pushed,
        failed=failed,
        noop=noop,
        total_rows=total_rows,
    )
    return {
        "pushed": pushed,
        "failed": failed,
        "noop": noop,
        "total_rows_pushed": total_rows,
        "n_targets": len(results),
        "per_target": [
            {
                "target": f"{r.target_system}:{r.target_name}",
                "status": r.status,
                "rows": r.row_count,
                "duration_ms": r.duration_ms,
                "error": r.error,
            }
            for r in results
        ],
    }
