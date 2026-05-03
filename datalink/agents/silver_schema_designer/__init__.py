"""Phase 15.8 — Silver Schema Designer.

Independent Silver-layer authoring. Three modes converge on the same
Silver registry tables (`global_silver_schema_*`):

  * AI_CONSTRUCT  — agent designs Hub/Sat/Link or NORMALIZED from Bronze + anchor
  * MANUAL_AUTHOR — operator hand-authors via column grid
  * IMPORT        — parse external schema (DDL_SQL, DBT_YAML, DBT_PROJECT, JSON_SCHEMA)

Public API:

  * ``SilverSchemaDesignerAgent`` — Anthropic-backed agent
  * ``SilverPattern``, ``SilverDesignerMode`` — enums
  * ``propose_silver_ai`` / ``propose_silver_manual`` / ``propose_silver_import``
  * ``persist_silver_proposal`` — DRAFT / PENDING_REVIEW
  * ``approve_silver_schema``   — flip to LIVE; archive prior LIVE
  * ``revert_silver_schema``    — clone an ARCHIVED row back to LIVE as new version
  * ``list_silver_datasets``, ``get_silver_schema``, ``fetch_live_silver``
"""

from .agent import (
    SilverDesignerMode,
    SilverPattern,
    SilverSchemaDesignerAgent,
)
from .bridge import (
    approve_silver_schema,
    fetch_live_silver,
    get_silver_schema,
    list_silver_datasets,
    persist_silver_proposal,
    propose_silver_ai,
    propose_silver_import,
    propose_silver_manual,
    revert_silver_schema,
)
from .importers import parse as parse_imported_silver

__all__ = [
    "SilverDesignerMode",
    "SilverPattern",
    "SilverSchemaDesignerAgent",
    "approve_silver_schema",
    "fetch_live_silver",
    "get_silver_schema",
    "list_silver_datasets",
    "parse_imported_silver",
    "persist_silver_proposal",
    "propose_silver_ai",
    "propose_silver_import",
    "propose_silver_manual",
    "revert_silver_schema",
]
