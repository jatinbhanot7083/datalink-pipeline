"""Contract Architect agent — Phase 14 — Data Contract Architect.

Authoring layer for Bronze schemas + their data contracts BEFORE files
arrive. Two modes:

  * Mode A (FILE_DRIVEN)    — operator drops a sample file, the agent
    profiles the header + first ~200 rows and proposes a contract.

  * Mode B (CONTRACT_FIRST) — operator describes the source in natural
    language (or pastes a vendor mapping spec), the agent proposes a
    contract from scratch.

Both modes share the same RAG-grounded core: top-k chunks are
retrieved from agent_memory.standard_references for the operator-
selected anchored standards (FHIR R4, X12, NCPDP, CMS, DV2, HEDIS,
or operator-uploaded custom standards), then injected into Claude's
context as the source of truth for proposed naming + types.
"""

from datalink.agents.contract_architect.agent import (
    ContractArchitectAgent,
    ContractMode,
    ContractProposal,
    ProposedColumn,
)
from datalink.agents.contract_architect.bridge import (
    approve_design,
    persist_design,
    propose_contract,
)

__all__ = [
    "ContractArchitectAgent",
    "ContractMode",
    "ContractProposal",
    "ProposedColumn",
    "approve_design",
    "persist_design",
    "propose_contract",
]
