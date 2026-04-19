"""Bronze zone — raw ingestion from SFTP → object store → warehouse MERGE.

Contract (from DataLink_Medallion_Pipeline.docx §3.1):
  - INSERT-or-MERGE only, never DELETE.
  - Idempotent MERGE on natural key. Re-running the same file is a no-op.
  - No type coercion — everything VARCHAR at Bronze.
  - No clinical validation. That's Silver.
  - Exactly 4 metadata columns added: _load_dt, _source_file, _batch_id, _record_source.

Tables (per §3.2): RAW_CLAIMS, RAW_MEMBERSHIP, RAW_PROVIDER.
"""

from datalink.pipeline.bronze.ingest import BronzeIngestResult, ingest_file

__all__ = ["BronzeIngestResult", "ingest_file"]
