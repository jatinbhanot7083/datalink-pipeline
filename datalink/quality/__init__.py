"""Great Expectations layer — the plug-in quality gate.

Runs at three checkpoints per DataLink_Medallion_Pipeline.docx §3.4, 4.3, 5.2:
  - CP1 Bronze (structural)   — nulls, row counts, date formats, duplicates
  - CP2 Silver (clinical)     — CPT/ICD validity, member eligibility, NPI
  - CP3 Gold   (business)     — KPI ranges, TAT compliance, amount bounds

When features.gx.enabled = false, all checkpoints are skipped with a
`gx: disabled (config)` log line and the pipeline runs unchanged.
"""

from datalink.quality.checkpoint import (
    CheckpointResult,
    CheckpointStatus,
    ExpectationResult,
    run_checkpoint,
)
from datalink.quality.control import (
    PipelineControl,
    PipelineState,
    create_control_tables,
)

__all__ = [
    "CheckpointResult",
    "CheckpointStatus",
    "ExpectationResult",
    "PipelineControl",
    "PipelineState",
    "create_control_tables",
    "run_checkpoint",
]
