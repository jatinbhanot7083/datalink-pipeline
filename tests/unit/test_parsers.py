"""Tests for datalink.pipeline.bronze.parsers — Phase 6."""

from __future__ import annotations

from pathlib import Path

import pytest

from datalink.pipeline.bronze.parsers import (
    CsvParser,
    EdiParserStub,
    ParquetParser,
    SourceFileParser,
    SourceFormatConfig,
    build_parser,
    infer_format_from_path,
)

# ---------- SourceFormatConfig defaults ----------


def test_source_format_defaults_match_phase_5_behaviour() -> None:
    """Un-configured source → comma-delim CSV with header. Zero-config demo works."""
    cfg = SourceFormatConfig()
    assert cfg.format == "csv"
    assert cfg.delimiter == ","
    assert cfg.has_header is True
    assert cfg.encoding == "utf-8"
    assert cfg.options == {}


# ---------- CsvParser ----------


def test_csv_parser_exposes_delimiter_and_header_in_copy_options() -> None:
    cfg = SourceFormatConfig(format="csv", delimiter="|", has_header=False)
    parser = CsvParser(cfg=cfg)
    assert parser.file_format() == "csv"
    opts = parser.copy_options()
    assert opts["delimiter"] == "|"
    assert opts["header"] is False


def test_csv_parser_merges_options_dict_last() -> None:
    """User-supplied options flow through to the warehouse opaquely."""
    cfg = SourceFormatConfig(delimiter="\t", options={"quote": "'", "escape": "\\"})
    parser = CsvParser(cfg=cfg)
    opts = parser.copy_options()
    assert opts["delimiter"] == "\t"
    assert opts["quote"] == "'"
    assert opts["escape"] == "\\"


# ---------- ParquetParser ----------


def test_parquet_parser_returns_parquet_format() -> None:
    cfg = SourceFormatConfig(format="parquet")
    parser = ParquetParser(cfg=cfg)
    assert parser.file_format() == "parquet"
    # Parquet has no delimiter / header concept — options pass through raw.
    assert "delimiter" not in parser.copy_options()
    assert "header" not in parser.copy_options()


# ---------- EdiParserStub ----------


@pytest.mark.parametrize("edi_fmt", ["edi_834", "edi_837"])
def test_edi_parser_raises_notimplemented_with_phase_9_hint(edi_fmt: str) -> None:
    """EDI is Phase 9 — stub must fail LOUDLY with a pointer to the roadmap."""
    cfg = SourceFormatConfig(format=edi_fmt)  # type: ignore[arg-type]
    parser = EdiParserStub(cfg=cfg)
    with pytest.raises(NotImplementedError, match="Phase 9"):
        parser.file_format()
    with pytest.raises(NotImplementedError, match="Phase 9"):
        parser.copy_options()


# ---------- build_parser factory ----------


@pytest.mark.parametrize(
    ("fmt", "expected_cls"),
    [
        ("csv", CsvParser),
        ("parquet", ParquetParser),
        ("edi_834", EdiParserStub),
        ("edi_837", EdiParserStub),
    ],
)
def test_build_parser_returns_correct_impl(fmt: str, expected_cls: type) -> None:
    cfg = SourceFormatConfig(format=fmt)  # type: ignore[arg-type]
    parser = build_parser(cfg)
    assert isinstance(parser, expected_cls)


def test_build_parser_rejects_unknown_format() -> None:
    # Bypass the Literal type-check to test runtime validation.
    cfg = SourceFormatConfig()
    object.__setattr__(cfg, "format", "xml")  # frozen dataclass; force an invalid value
    with pytest.raises(ValueError, match="Unknown parser format"):
        build_parser(cfg)


def test_all_parsers_conform_to_protocol() -> None:
    """Protocol runtime check — ensures new parsers can't drift from the contract."""
    for fmt in ("csv", "parquet"):
        cfg = SourceFormatConfig(format=fmt)  # type: ignore[arg-type]
        parser = build_parser(cfg)
        assert isinstance(parser, SourceFileParser)


# ---------- infer_format_from_path ----------


@pytest.mark.parametrize(
    ("filename", "expected"),
    [
        ("claims.csv", "csv"),
        ("claims.tsv", "csv"),
        ("claims.psv", "csv"),
        ("claims.txt", "csv"),
        ("claims_no_ext", "csv"),  # default
        ("claims.parquet", "parquet"),
        ("claims.pq", "parquet"),
        ("claims.unknown", "csv"),  # unknown falls back to csv
    ],
)
def test_infer_format_from_path(filename: str, expected: str) -> None:
    assert infer_format_from_path(Path(filename)) == expected
