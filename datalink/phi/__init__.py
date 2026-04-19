"""PHI boundary enforcement.

The plug-in agent layer (CrewAI + Anthropic Claude) is architecturally required
to send only metadata / aggregate statistics to the LLM — never row-level
healthcare data. That guarantee is enforced here, in code, not just in docs.

Any agent → LLM call goes through `PhiRedactionLayer.inspect_payload(...)`
first. If the payload contains anything that looks like a row, a PHI field, or
a list of dicts with PHI-shaped keys, `PhiBoundaryViolationError` is raised and the
LLM call is aborted before a byte leaves the process.
"""

from datalink.phi.guard import SAFE_FIELDS, PhiBoundaryViolationError, PhiRedactionLayer

__all__ = ["SAFE_FIELDS", "PhiBoundaryViolationError", "PhiRedactionLayer"]
