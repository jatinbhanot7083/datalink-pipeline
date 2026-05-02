"""Phase 15.6 — Gold Schema Designer.

The Gold registry's authoring engine. Three modes converge on the same
Gold registry tables (global_gold_schema_*):

  * AI_CONSTRUCT  — agent designs from Bronze + chosen anchor + RAG corpus
  * MANUAL_AUTHOR — operator hand-authors via column grid
  * IMPORT        — parse external schema (DDL/YAML/FHIR profile/etc.)

Public API:

  * ``GoldSchemaDesignerAgent`` — Anthropic-backed agent for AI_CONSTRUCT
  * ``DesignerMode``, ``GoldAnchor`` — enums
  * ``propose_gold_ai`` / ``propose_gold_manual`` / ``propose_gold_import``
  * ``persist_proposal``       — DRAFT / PENDING_REVIEW
  * ``approve_gold_schema``    — flip to LIVE
  * ``list_gold_datasets``, ``get_gold_schema``  — UI read helpers
"""

from .agent import (
    DesignerMode,
    GoldAnchor,
    GoldSchemaDesignerAgent,
    GoldSchemaProposal,
)
from .bridge import (
    approve_gold_schema,
    get_gold_schema,
    list_gold_datasets,
    persist_proposal,
    propose_gold_ai,
    propose_gold_import,
    propose_gold_manual,
)
from .importers import parse as parse_imported_schema

__all__ = [
    "DesignerMode",
    "GoldAnchor",
    "GoldSchemaDesignerAgent",
    "GoldSchemaProposal",
    "approve_gold_schema",
    "get_gold_schema",
    "list_gold_datasets",
    "parse_imported_schema",
    "persist_proposal",
    "propose_gold_ai",
    "propose_gold_import",
    "propose_gold_manual",
]
