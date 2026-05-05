"""Phase 13.8 — file deployer.

When an APPROVED mapping session is ready to ship, :func:`deploy_session`:

  1. Reads the session's generated artifacts (silver_sql, gold_sql,
     push scripts).
  2. Writes each one to its conventional path under the project tree:
       - ``dbt/models/silver/<silver_target_table>.sql``
       - ``dbt/models/gold/<gold_target_table>.sql`` (if present)
       - ``scripts/onprem_push/<gold_target_table>_postgres.sql`` (if present)
       - ``scripts/onprem_push/<gold_target_table>_mssql.sql`` (if present)
  3. Registers a row in ``CONTROL.mapping_artifacts`` with the file paths,
     content hash, deployer, and timestamp — provenance + lineage.
  4. Calls ``MappingSessionRegistry.mark_deployed`` to walk the session
     state from APPROVED → DEPLOYED.

Deployment is idempotent: re-deploying the same session writes the same
files (overwrite is intentional — the session row is the source of
truth) and inserts a fresh artifact log row each time. Operators can
re-run if a partial failure occurred.

Returns a :class:`DeploymentArtifact` carrying the list of files written.
"""

from __future__ import annotations

import hashlib
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from datalink.adapters.protocols import Warehouse
from datalink.agents.mapper.session import (
    MappingSessionRegistry,
    SessionStatus,
)
from datalink.logging import get_logger
from datalink.quality.control import CONTROL_SCHEMA

_log = get_logger(__name__)


# ============================================================================
# OUTPUT TYPES
# ============================================================================


@dataclass
class DeploymentArtifact:
    """Record of one deployment — what got written, where, by whom."""

    artifact_id: str
    session_id: str
    files_written: list[str] = field(default_factory=list)
    content_hash: str = ""
    deployed_by: str = ""
    deployed_at: datetime = field(default_factory=lambda: datetime.now(UTC))


# ============================================================================
# CORE
# ============================================================================


# Project-relative target paths. Resolved against the repo root.
_DBT_SILVER_DIR = Path("dbt/models/silver")
_DBT_GOLD_DIR = Path("dbt/models/gold")
_PUSH_DIR = Path("scripts/onprem_push")


def deploy_session(
    *,
    warehouse: Warehouse,
    session_id: str,
    actor: str,
    repo_root: Path | None = None,
) -> DeploymentArtifact:
    """Write all approved artifacts to disk + record the deployment.

    ``repo_root`` defaults to the parent of ``datalink/``, walking up
    from the current file path. Override for tests.

    Raises ``ValueError`` if the session isn't APPROVED, or if it has
    no artifacts to deploy.
    """
    reg = MappingSessionRegistry(warehouse)
    session = reg.get_session(session_id)
    if session is None:
        raise KeyError(f"Unknown session_id: {session_id!r}")
    if session.status is not SessionStatus.APPROVED:
        raise ValueError(f"deploy_session requires APPROVED, session is {session.status.value}")

    if repo_root is None:
        repo_root = _resolve_repo_root()

    files_to_write: list[tuple[Path, str]] = []

    # Silver — required
    if not session.silver_sql or not session.silver_target_table:
        raise ValueError(f"session {session_id!r} has no Silver artifact to deploy")
    files_to_write.append(
        (
            repo_root / _DBT_SILVER_DIR / f"{session.silver_target_table}.sql",
            session.silver_sql,
        )
    )

    # Gold — optional
    if session.gold_sql and session.gold_target_table:
        files_to_write.append(
            (
                repo_root / _DBT_GOLD_DIR / f"{session.gold_target_table}.sql",
                session.gold_sql,
            )
        )

    # On-Prem push — optional, per backend
    push_base = session.gold_target_table or session.silver_target_table or "untitled"
    if session.onprem_postgres_sql:
        files_to_write.append(
            (
                repo_root / _PUSH_DIR / f"{push_base}_postgres.sql",
                session.onprem_postgres_sql,
            )
        )
    if session.onprem_mssql_sql:
        files_to_write.append(
            (
                repo_root / _PUSH_DIR / f"{push_base}_mssql.sql",
                session.onprem_mssql_sql,
            )
        )

    # Write all files.
    written: list[str] = []
    rolling_hash = hashlib.sha256()
    for path, content in files_to_write:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
        written.append(str(path.relative_to(repo_root)))
        rolling_hash.update(content.encode("utf-8"))

    artifact_id = str(uuid.uuid4())
    content_hash = rolling_hash.hexdigest()[:16]
    artifact = DeploymentArtifact(
        artifact_id=artifact_id,
        session_id=session_id,
        files_written=written,
        content_hash=content_hash,
        deployed_by=actor,
        deployed_at=datetime.now(UTC),
    )

    # Persist artifact row.
    warehouse.execute(
        f"INSERT INTO {CONTROL_SCHEMA}.mapping_artifacts "
        "(artifact_id, session_id, files_written, content_hash, "
        " deployed_by, deployed_at) "
        "VALUES ($id, $sid, $f, $h, $a, $ts)",
        {
            "id": artifact_id,
            "sid": session_id,
            "f": "\n".join(written),
            "h": content_hash,
            "a": actor,
            "ts": artifact.deployed_at,
        },
    )

    # Walk session APPROVED → DEPLOYED.
    reg.mark_deployed(
        session_id,
        actor=actor,
        deploy_notes=f"{len(written)} files written; hash={content_hash}",
    )

    _log.info(
        "mapper.deploy.done",
        session_id=session_id,
        artifact_id=artifact_id,
        files=len(written),
        content_hash=content_hash,
    )
    return artifact


def list_deployments(
    warehouse: Warehouse,
    *,
    session_id: str | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    """Read deployment history. For the Smart Mapper UI's footer."""
    if session_id:
        return warehouse.query(
            f"SELECT * FROM {CONTROL_SCHEMA}.mapping_artifacts "
            "WHERE session_id = $s ORDER BY deployed_at DESC LIMIT $lim",
            {"s": session_id, "lim": limit},
        )
    return warehouse.query(
        f"SELECT * FROM {CONTROL_SCHEMA}.mapping_artifacts ORDER BY deployed_at DESC LIMIT $lim",
        {"lim": limit},
    )


def _resolve_repo_root() -> Path:
    """Walk up from this file until we find ``pyproject.toml``."""
    here = Path(__file__).resolve()
    for parent in here.parents:
        if (parent / "pyproject.toml").exists():
            return parent
    # Fallback: 3 levels up from this file
    # (datalink/agents/mapper/deployer.py → repo root).
    return here.parents[3]
