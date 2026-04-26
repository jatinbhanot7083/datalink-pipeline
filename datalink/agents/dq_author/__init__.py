"""dq_author crew — human-in-the-loop authoring of custom DQ checks.

Workflow:

    1. User types a natural-language ask in the DQ AI Architect UI:
       "Flag any provider with > 100 claims/day — fraud signal."
    2. ``DqProposerAgent`` reads the ask + the table's column metadata (no
       row data — PHI boundary preserved) and emits a structured proposal:
          { expectation_type, kwargs, meta, sql_preview, rationale }
    3. The orchestration layer (``datalink.quality.agent_authored``) runs
       the ``sql_preview`` against the warehouse for sample output, packs
       the result into a ``Proposal`` record, and hands it to the UI.
    4. Operator reviews → approves → record gets written to ``CONTROL.dq_suites``
       with ``source = SuiteSource.AGENT`` and routed through the standard
       DRAFT → PENDING_REVIEW → APPROVED → LIVE state machine.
    5. From that moment, every checkpoint run for that client picks the
       expectation up alongside the baseline suites — no profiler change
       required.

The crew is a deliberately small surface area: one agent that does NL → spec.
SQL execution + PHI assertion + persistence belong outside the agent so the
LLM never sees raw rows.
"""

from datalink.agents.dq_author.proposer import DqProposerAgent

__all__ = ["DqProposerAgent"]
