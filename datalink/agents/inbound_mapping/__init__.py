"""Bronze Inbound Mapping Agent (BIMA) — Phase 23.

Public API:

  from datalink.agents.inbound_mapping import (
      InboundMappingAgent,        # the agent class (subclass of AgentBase)
      detect_and_read,            # routes a file to the right reader
      propose_mapping,            # UI bridge: read file + run agent + persist DRAFT
      submit_proposal,            # UI bridge: DRAFT → PENDING_REVIEW
      approve_proposal,           # UI bridge: PENDING_REVIEW → APPROVED → LIVE
      reject_proposal,            # UI bridge: PENDING_REVIEW → REJECTED with notes
      list_proposals,             # UI bridge: filter by client / dataset / status
      list_live_mappings,         # UI bridge: read current LIVE mappings
      get_proposal,               # UI bridge: full detail incl. field rows
      SourceView, FieldDescriptor, MappingProposal,
  )
"""

from datalink.agents.inbound_mapping.agent import (  # noqa: F401
    InboundMappingAgent,
    MappingProposal,
    ProposedFieldMapping,
)
from datalink.agents.inbound_mapping.bridge import (  # noqa: F401
    approve_proposal,
    get_proposal,
    list_live_mappings,
    list_proposals,
    propose_mapping,
    recommend_target_for_file,
    reject_proposal,
    submit_proposal,
)
from datalink.agents.inbound_mapping.extensions import (  # noqa: F401
    approve_canonical_extension,
    get_extension_proposal,
    list_extension_proposals,
    propose_canonical_extension,
    reject_canonical_extension,
)
from datalink.agents.inbound_mapping.readers import (  # noqa: F401
    FieldDescriptor,
    SourceView,
    detect_and_read,
    read_csv,
    read_docx,
    read_json,
    read_pdf,
    read_text,
    read_xlsx,
)
