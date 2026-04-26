"""Phase 13 — AI Smart Mapper.

Multi-stage agent that converts a Bronze source + a plain-English Gold
contract into:

  * Silver dbt model SQL (hub/sat/link, SCD2-aware)
  * Gold dbt view SQL (operational shape)
  * Optimized On-Prem MERGE script (Postgres + SQL Server)

All artifacts go through Phase-10/12-style HITL review before being
written to disk. The mapper agent is conversational — operator can say
"no, denial_reason should come from the latest non-null" and the agent
revises in place, tracking turns in a persisted session.

This package is intentionally lazy-loaded: importing
``datalink.agents.mapper`` does NOT pull in the LLM SDKs, dbt, or
Streamlit. Concrete implementations live in submodules.
"""

from __future__ import annotations
