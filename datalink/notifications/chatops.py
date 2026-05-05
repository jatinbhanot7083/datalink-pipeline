"""Slack / Microsoft Teams webhook senders for DataLink events.

Configuration (env vars):
  * ``SLACK_WEBHOOK_URL``      — incoming webhook URL for Slack
  * ``TEAMS_WEBHOOK_URL``      — Office 365 connector URL for Teams
  * ``CHATOPS_DEFAULT``        — 'slack' | 'teams' | 'both' (default 'both' if both set)

Each notify_* function returns a list of dicts ``[{"target": "slack",
"status": "ok"|"error", "detail": ...}]`` so callers (and the UI test page)
can surface partial failures.
"""

from __future__ import annotations

import os
from typing import Any

import httpx

from datalink.logging import get_logger

_log = get_logger(__name__)


def _enabled_targets() -> list[str]:
    targets = []
    if os.environ.get("SLACK_WEBHOOK_URL"):
        targets.append("slack")
    if os.environ.get("TEAMS_WEBHOOK_URL"):
        targets.append("teams")
    return targets


def _post_slack(text: str, blocks: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    url = os.environ.get("SLACK_WEBHOOK_URL")
    if not url:
        return {"target": "slack", "status": "skip", "detail": "SLACK_WEBHOOK_URL not set"}
    payload: dict[str, Any] = {"text": text}
    if blocks:
        payload["blocks"] = blocks
    try:
        r = httpx.post(url, json=payload, timeout=10.0)
        if r.status_code == 200:
            return {"target": "slack", "status": "ok", "detail": "sent"}
        return {"target": "slack", "status": "error", "detail": f"{r.status_code}: {r.text[:120]}"}
    except Exception as exc:
        return {"target": "slack", "status": "error", "detail": str(exc)[:200]}


def _post_teams(text: str, title: str | None = None) -> dict[str, Any]:
    url = os.environ.get("TEAMS_WEBHOOK_URL")
    if not url:
        return {"target": "teams", "status": "skip", "detail": "TEAMS_WEBHOOK_URL not set"}
    card = {
        "@type": "MessageCard",
        "@context": "https://schema.org/extensions",
        "summary": title or "DataLink notification",
        "themeColor": "0a1a3e",
        "title": title or "DataLink",
        "text": text,
    }
    try:
        r = httpx.post(url, json=card, timeout=10.0)
        if r.status_code in (200, 202):
            return {"target": "teams", "status": "ok", "detail": "sent"}
        return {"target": "teams", "status": "error", "detail": f"{r.status_code}: {r.text[:120]}"}
    except Exception as exc:
        return {"target": "teams", "status": "error", "detail": str(exc)[:200]}


def post_message(
    text: str,
    *,
    blocks: list[dict[str, Any]] | None = None,
    title: str | None = None,
    targets: list[str] | None = None,
) -> list[dict[str, Any]]:
    """Send a message to all enabled targets (or a custom subset)."""
    targets = targets or _enabled_targets()
    out: list[dict[str, Any]] = []
    if "slack" in targets:
        out.append(_post_slack(text, blocks))
    if "teams" in targets:
        out.append(_post_teams(text, title))
    if not out:
        out.append(
            {
                "target": "all",
                "status": "skip",
                "detail": "No webhook targets configured. "
                "Set SLACK_WEBHOOK_URL or TEAMS_WEBHOOK_URL in .env.",
            }
        )
    _log.info("chatops.posted", text=text[:80], outcomes=out)
    return out


def notify_dag_failed(
    dag_id: str, run_id: str, task_id: str, *, log_url: str | None = None
) -> list[dict[str, Any]]:
    text = f"🚨 *DAG failure* — `{dag_id}` task `{task_id}` in run `{run_id}` failed."
    if log_url:
        text += f"\n→ {log_url}"
    return post_message(text, title="DataLink — DAG failure")


def notify_anomaly(
    *, client_id: str, dataset_code: str, metric: str, sigma: float, action: str
) -> list[dict[str, Any]]:
    text = (
        f"⚠️ *Anomaly detected* — {client_id} / {dataset_code} / "
        f"`{metric}` deviated {sigma:.2f}σ. Action: `{action}`."  # noqa: RUF001
    )
    return post_message(text, title="DataLink — Anomaly detected")


def notify_proposal_pending(
    proposal_id: str, agent_type: str, scope_key: str
) -> list[dict[str, Any]]:
    text = f"📝 *Proposal pending review* — `{agent_type}` for `{scope_key}` (`{proposal_id}`)."
    return post_message(text, title="DataLink — Proposal pending")


def test_connection() -> list[dict[str, Any]]:
    """Send a one-line ping to all enabled targets. Used by the UI test
    page to validate webhook config before relying on it for real alerts."""
    return post_message(
        "🟢 *DataLink ChatOps test* — webhooks are reachable.",
        title="DataLink — Connection test",
    )
