"""Pluggable source-file parsers — Phase 6.

The Bronze layer historically assumed every source is a CSV with `,` as
delimiter. Production payer feeds break that assumption:
  - Some clients ship pipe-delimited (`|`) for quoting safety.
  - Some ship tab-delimited from legacy HL7 exports.
  - Some ship EDI X12 (834 enrollment, 837 claim) — fixed-field segments.
  - Future: Parquet / JSONL.

This module defines the `SourceFileParser` Protocol so Bronze ingest can
defer "how to turn a file into a staging-table-compatible stream" to a
strategy object. Phase 6 ships with `CsvParser` (delimiter-configurable)
and `EdiParserStub` (raises NotImplementedError; Phase 9 will fill it in).

Factory: `build_parser(source_cfg)` picks the right impl from `source_cfg.format`.

Design note on Protocol vs ABC:
We use `Protocol` because the warehouse adapter code already drives CSV
ingestion directly (read_csv_auto), and the parser's only real job is
to compute the `copy_from_stage` options dict. Keeping the parser
Protocol-typed means existing callers don't have to instantiate an ABC;
they just consume the options and hand them to the warehouse.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal, Protocol, runtime_checkable

# ----------------------------------------------------------------------------
# Config — per-source parser behaviour (delimiter, quoting, encoding, ...)
# ----------------------------------------------------------------------------


@dataclass(frozen=True)
class SourceFormatConfig:
    """Parser-level config for a single source_type.

    Ignored by the warehouse at MERGE time — these fields drive ONLY the
    file-to-staging load step. Default matches Phase-5.x behaviour
    (comma-delimited CSV with header).
    """

    format: Literal["csv", "parquet", "edi_834", "edi_837"] = "csv"
    delimiter: str = ","
    has_header: bool = True
    encoding: str = "utf-8"
    # Arbitrary adapter-specific knobs (e.g. DuckDB `quote` / `escape`).
    # Keys flow into warehouse.copy_from_stage(options=...).
    options: dict[str, Any] = field(default_factory=dict)


# ----------------------------------------------------------------------------
# Protocol + concrete impls
# ----------------------------------------------------------------------------


@runtime_checkable
class SourceFileParser(Protocol):
    """Strategy for turning a source file into warehouse-ingestible form.

    Contract:
      * `local_path` exists and is readable.
      * `file_format()` returns the string literal the warehouse adapter
        understands (currently 'csv' or 'parquet').
      * `copy_options()` returns a dict safe to pass to
        `warehouse.copy_from_stage(options=...)`.
    """

    def file_format(self) -> str: ...

    def copy_options(self) -> dict[str, Any]: ...


@dataclass(frozen=True)
class CsvParser:
    """Delimiter-configurable CSV parser.

    Honours `SourceFormatConfig.delimiter` (pipe / comma / tab / anything
    DuckDB's `read_csv_auto` accepts). The extra `options` dict flows
    through to the adapter — useful for `quote="'"`, `escape="\\\\"`, etc.
    """

    cfg: SourceFormatConfig

    def file_format(self) -> str:
        return "csv"

    def copy_options(self) -> dict[str, Any]:
        return {
            "header": self.cfg.has_header,
            "delimiter": self.cfg.delimiter,
            **self.cfg.options,
        }


@dataclass(frozen=True)
class ParquetParser:
    """Parquet parser. No format knobs needed; warehouse reads natively."""

    cfg: SourceFormatConfig

    def file_format(self) -> str:
        return "parquet"

    def copy_options(self) -> dict[str, Any]:
        return dict(self.cfg.options)


class EdiParserStub:
    """Stub for EDI 834 (enrollment) / 837 (claim) parsing.

    Phase 9 will replace this with a real parser backed by pyx12 or a
    hand-rolled segment walker. For now, callers hit NotImplementedError
    with a pointer to the phase roadmap so misconfiguration surfaces loud
    and early rather than silently-truncated data.
    """

    def __init__(self, cfg: SourceFormatConfig) -> None:
        self.cfg = cfg

    def file_format(self) -> str:  # pragma: no cover — stub
        raise NotImplementedError(
            f"EDI format {self.cfg.format!r} is planned for Phase 9. "
            "Convert your feed to CSV (pipe-/comma-/tab-delimited) and "
            "configure the delimiter in config/sources.yaml for now."
        )

    def copy_options(self) -> dict[str, Any]:  # pragma: no cover — stub
        raise NotImplementedError(f"EDI format {self.cfg.format!r} is planned for Phase 9.")


# ----------------------------------------------------------------------------
# Factory — pick the right parser for a given SourceFormatConfig
# ----------------------------------------------------------------------------


def build_parser(cfg: SourceFormatConfig) -> SourceFileParser:
    """Return the parser impl matching `cfg.format`. Raises on unknown format."""
    if cfg.format == "csv":
        return CsvParser(cfg=cfg)
    if cfg.format == "parquet":
        return ParquetParser(cfg=cfg)
    if cfg.format in ("edi_834", "edi_837"):
        return EdiParserStub(cfg=cfg)
    raise ValueError(
        f"Unknown parser format {cfg.format!r}; expected one of "
        "'csv' / 'parquet' / 'edi_834' / 'edi_837'."
    )


def infer_format_from_path(local: Path) -> str:
    """Fallback guess based on file extension. Used when config is absent.

    Recognised: .csv / .tsv / .psv / .txt → csv;  .parquet / .pq → parquet.
    Unknown extensions default to csv (the historical Phase-5.x default).
    """
    suffix = local.suffix.lower()
    if suffix in (".parquet", ".pq"):
        return "parquet"
    if suffix in (".csv", ".tsv", ".psv", ".txt", ""):
        return "csv"
    return "csv"
