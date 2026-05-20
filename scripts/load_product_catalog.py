"""Phase 15.1 — Product Catalog loader.

Seeds the Global Gold Catalog from the source-of-truth xlsx
(``data/sample/product_catalog.xlsx``) into three CONTROL tables:

  * ``CONTROL.global_bronze_catalog_datasets``  — 33 datasets (master)
  * ``CONTROL.global_bronze_catalog_fields``    — 943 fields (per-dataset)
  * ``CONTROL.onprem_routing_rules``          — per (dataset, downstream-product)
                                                 from the "Used by" matrix

Idempotent: re-running with ``--bump-version`` increments
``catalog_version`` on every row written and replaces prior rows. Without
the flag the loader DELETE-then-INSERTs at the same version.

The loader infers ``logical_type`` and PII/PHI flags from field-name +
description heuristics. The ``PipelineArchitectAgent`` can refine these
later via human-in-the-loop edits — this is the deterministic *seed*.

Run from inside the control_tower container:

    docker exec datalink-control-tower python3 \\
        /opt/datalink/scripts/load_product_catalog.py

CLI:
    --xlsx PATH        override the default catalog path
    --bump-version     bump catalog_version on every row (default: keep at 1)
    --dry-run          print the work plan but DO NOT write to Snowflake
    --by                "system" (default) or your username (for audit)
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import uuid
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import openpyxl  # type: ignore[import-untyped]

# Repository root — script lives under repo/scripts/.
_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT))

# Phase 22 fix — build_adapters + load_settings import is deferred to
# inside ``load_catalog`` because pulling them at module load triggers
# ``datalink.adapters.factory`` which imports the Azurite object store
# (azure-core), and that package isn't installed inside the Streamlit
# container.  The CLI entry point still works because the host venv DOES
# have azure-core.
from datalink.adapters.protocols import Warehouse  # noqa: E402
from datalink.logging import get_logger  # noqa: E402
from datalink.quality.control import CONTROL_SCHEMA, create_control_tables  # noqa: E402

_log = get_logger(__name__)

DEFAULT_XLSX = _REPO_ROOT / "data" / "sample" / "product_catalog.xlsx"

# Sheets used from the workbook.
_SHEET_DATASETS = "05_Datasets_Master"
_SHEET_FIELDS = "08_Field_Catalogue"

# Datasets master headers row index (1-based as openpyxl reports).
_DATASETS_HEADER_ROW = 5
_FIELDS_HEADER_ROW = 5

# Downstream product codes recognised in the "Used by" column. These map
# 1:1 to DataLink product names; the catalog uses bulleted abbreviations.
_KNOWN_PRODUCTS = {
    "CC": "Care Compass",
    "E360": "Enterprise 360",
    "RBN": "Risk-Based Networks",
    "EC": "EvokeConnect Care",
    "ESV": "Enterprise SaaS Value",
    "RAE": "Regional Accountable Entity",
}

# Reconciliation: the Datasets Master and Field Catalogue sheets in the
# source workbook use slightly different names for the same dataset (e.g.
# 'Member Hierarchy' in the master vs. plain 'Hierarchy' in the fields
# sheet). Map fields-sheet name -> master-sheet name so we don't lose
# 943 fields silently. This is an alias table — not a rename.
_FIELDS_TO_MASTER_ALIAS = {
    "Activities": "Member Activities",
    "Care Team": "Member Care Team",
    "Caregiver": "Member Caregiver",
    "Claims": "Member Claims",
    "Custom Data": "Member Custom Data",
    "Documents": "Member Documents",
    "HEDIS / Quality": "Quality-HEDIS",
    "Hierarchy": "Member Hierarchy",
    "Notes": "Member Notes",
    "PCP": "Member PCP",
    "Programs": "Member Programs",
}


# Default OnPrem target system per downstream product.
# Operator can override per-client via client_routing_overrides.
_DEFAULT_TARGETS = {
    "CC": ("postgres", "postgres://care_compass.onprem"),
    "E360": ("snowflake_share", "snowflake://e360_share"),
    "RBN": ("postgres", "postgres://rbn.onprem"),
    "EC": ("sqlserver", "sqlserver://evokeconnect.onprem"),
    "ESV": ("snowflake_share", "snowflake://esv_share"),
    "RAE": ("sftp", "sftp://rae.partner.com/inbox"),
}

# PII patterns — fields likely to contain personally identifiable info.
_PII_PATTERNS = [
    re.compile(r"\b(name|first|middle|last|surname|legal)\b", re.IGNORECASE),
    re.compile(r"\b(phone|mobile|email|fax)\b", re.IGNORECASE),
    re.compile(r"\b(address|street|city|state|zip|county)\b", re.IGNORECASE),
    re.compile(r"\b(ssn|social[\s_]?security|tax[\s_]?id|tin)\b", re.IGNORECASE),
    re.compile(r"\b(birth[\s_]?date|dob|date[\s_]?of[\s_]?birth)\b", re.IGNORECASE),
]

# PHI patterns — clinical / coverage / claim identifiers.
_PHI_PATTERNS = [
    re.compile(r"\b(diagnosis|icd|procedure|cpt|hcpcs)\b", re.IGNORECASE),
    re.compile(r"\b(medicare|medicaid|coverage|plan|benefit)\b", re.IGNORECASE),
    re.compile(r"\b(claim|encounter|admission|discharge)\b", re.IGNORECASE),
    re.compile(r"\b(member|patient|subscriber)[\s_]?id\b", re.IGNORECASE),
    re.compile(r"\b(npi|provider[\s_]?id|tin)\b", re.IGNORECASE),
]

# Business-key heuristic — fields commonly used as natural keys.
_BUSINESS_KEY_PATTERNS = [
    re.compile(r"\b(member|patient|subscriber|claim|encounter|provider)[\s_]?id\b", re.IGNORECASE),
    re.compile(r"\b(card|medicare|medicaid|payer)[\s_]?id\b", re.IGNORECASE),
]


# =============================================================================
# Type inference
# =============================================================================


def infer_logical_type(field_display_name: str, description: str, example: str = "") -> str:
    """Map a field's name+description to one of our six logical types.

    TEXT (default) | INTEGER | DECIMAL | DATE | TIMESTAMP | BOOLEAN

    Heuristic — favors TEXT when ambiguous so the operator can refine later.
    """
    name_l = (field_display_name or "").lower()
    desc_l = (description or "").lower()
    haystack = f"{name_l} {desc_l}"

    # Date / timestamp first — they're most distinctive.
    if re.search(r"\bdate\b", haystack) or re.search(r"\bbirth date\b", haystack):
        return "DATE"
    if re.search(r"\btimestamp\b|\bdatetime\b|\bdate.?time\b", haystack):
        return "TIMESTAMP"
    if "begin date" in haystack or "end date" in haystack or "effective" in haystack:
        return "DATE"
    if "refresh date" in haystack:
        return "DATE"

    # Boolean / indicator / flag.
    if re.search(r"\bindicator\b|\bflag\b", haystack):
        return "BOOLEAN"
    if re.search(r"\b(is|has)[_\s]", name_l):
        return "BOOLEAN"

    # Decimal — money + percentage + ratio + rate.
    if re.search(
        r"\b(amount|amt|cost|charge|paid|allowed|coinsurance|copay|deductible|rate|percent|score|risk)\b",
        haystack,
    ):
        return "DECIMAL"

    # Integer — counts, days, quantity, age.
    if re.search(r"\b(count|days|quantity|qty|age|number of|years|months)\b", haystack):
        return "INTEGER"

    # NPI / TIN are 10/9 digit fixed-width — treat as TEXT (preserves leading zeros).
    if re.search(r"\bnpi\b|\btin\b|\bzip\b|\bcode\b|\bid\b", haystack):
        return "TEXT"

    # Default: TEXT.
    return "TEXT"


def is_pii(field_display_name: str) -> bool:
    name = field_display_name or ""
    return any(p.search(name) for p in _PII_PATTERNS)


def is_phi(field_display_name: str, description: str) -> bool:
    haystack = f"{field_display_name or ''} {description or ''}"
    return any(p.search(haystack) for p in _PHI_PATTERNS)


def is_business_key(field_display_name: str) -> bool:
    name = field_display_name or ""
    return any(p.search(name) for p in _BUSINESS_KEY_PATTERNS)


# =============================================================================
# Field-name normalization
# =============================================================================


def to_snake_case(display: str) -> str:
    """Convert a catalog 'Field name' to a DDL-friendly snake_case identifier.

    Examples:
        'Member Card ID'                          -> 'member_card_id'
        'Member Street Address Line1'             -> 'member_street_address_line1'
        'Plan Benenfit Package ID' (typo in src!) -> 'plan_benenfit_package_id'
        'Atrributed Provider NPI'                 -> 'atrributed_provider_npi'
        'Member Provider(PCP) Begin Date'         -> 'member_provider_pcp_begin_date'
    """
    s = (display or "").strip()
    # Split CamelCase boundaries (e.g. 'NPIID' -> 'NPI_ID').
    s = re.sub(r"([a-z])([A-Z])", r"\1_\2", s)
    # Strip parens but keep their contents.
    s = re.sub(r"\(([^)]+)\)", r"_\1", s)
    # Collapse non-word chars to underscore.
    s = re.sub(r"[^a-zA-Z0-9]+", "_", s)
    s = s.strip("_").lower()
    s = re.sub(r"_+", "_", s)
    if not s:
        return "field"
    if s[0].isdigit():
        s = f"f_{s}"
    return s


def to_dataset_code(display: str) -> str:
    """Dataset code ='member_claims' for 'Member Claims' etc."""
    s = (display or "").strip()
    s = re.sub(r"[^a-zA-Z0-9]+", "_", s)
    s = s.strip("_").lower()
    return s or "dataset"


def parse_used_by(used_by: str | None) -> list[str]:
    """Split 'CC · E360 · RBN · EC · ESV' into ['CC','E360','RBN','EC','ESV']."""
    if not used_by:
        return []
    # The xlsx uses U+00B7 (MIDDLE DOT) as separator. Tolerate '/', ',' too.
    parts = re.split(r"[·/,]+", used_by)
    return [p.strip() for p in parts if p.strip()]


# =============================================================================
# Workbook reading
# =============================================================================


def _load_datasets_master(wb: openpyxl.Workbook) -> list[dict[str, Any]]:
    """Read the 33 dataset rows from 05_Datasets_Master."""
    ws = wb[_SHEET_DATASETS]
    out: list[dict[str, Any]] = []
    for row_idx, row in enumerate(ws.iter_rows(values_only=True), start=1):
        if row_idx <= _DATASETS_HEADER_ROW:
            continue
        # Layout: ['', dataset, frequency, fields, required, optional, used_by, category, notes, '']
        ds = row[1]
        if ds is None or str(ds).strip().upper() == "TOTAL":
            continue
        display = str(ds).strip()
        out.append(
            {
                "display_name": display,
                "dataset_code": to_dataset_code(display),
                "default_frequency": _str_or_none(row[2]),
                "total_fields": _int_or_zero(row[3]),
                "required_fields": _int_or_zero(row[4]),
                "optional_fields": _int_or_zero(row[5]),
                "used_by": _str_or_none(row[6]),
                "used_by_list": parse_used_by(_str_or_none(row[6])),
                "category": _str_or_none(row[7]),
                "notes": _str_or_none(row[8]),
            }
        )
    return out


def _load_fields_catalogue(wb: openpyxl.Workbook) -> dict[str, list[dict[str, Any]]]:
    """Read field rows; group by dataset display name (handles merged cells).

    Applies _FIELDS_TO_MASTER_ALIAS so the returned dict keys match the
    master sheet's dataset names — no orphaned field groups.
    """
    ws = wb[_SHEET_FIELDS]
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    current_dataset: str | None = None
    field_order_per_ds: dict[str, int] = defaultdict(int)
    for row_idx, row in enumerate(ws.iter_rows(values_only=True), start=1):
        if row_idx <= _FIELDS_HEADER_ROW:
            continue
        # Layout: ['', dataset, field_name, req, description, additional_notes, example, frequency]
        ds_cell = row[1]
        if ds_cell is not None:
            raw = str(ds_cell).strip()
            current_dataset = _FIELDS_TO_MASTER_ALIAS.get(raw, raw)
        if current_dataset is None:
            continue
        if str(current_dataset).strip().upper() == "TOTAL":
            continue
        field_name = row[2]
        if field_name is None:
            continue

        display = str(field_name).strip()
        # Some catalog rows wrap multi-line text — collapse.
        desc = re.sub(r"\s+", " ", str(row[4] or "")).strip()
        notes = re.sub(r"\s+", " ", str(row[5] or "")).strip()
        example = re.sub(r"\s+", " ", str(row[6] or "")).strip()
        req = (str(row[3] or "")).strip() or "Optional"

        field_order_per_ds[current_dataset] += 1
        grouped[current_dataset].append(
            {
                "field_display_name": display,
                "gold_column_name": to_snake_case(display),
                "field_order": field_order_per_ds[current_dataset],
                "requirement": "Required" if req.lower().startswith("req") else "Optional",
                "logical_type": infer_logical_type(display, desc, example),
                "description": desc,
                "additional_notes": notes,
                "example": example,
                "is_pii": is_pii(display),
                "is_phi": is_phi(display, desc),
                "is_business_key": is_business_key(display),
            }
        )
    return grouped


def _str_or_none(v: Any) -> str | None:
    if v is None:
        return None
    s = str(v).strip()
    return s or None


def _int_or_zero(v: Any) -> int:
    if v is None:
        return 0
    try:
        return int(v)
    except (TypeError, ValueError):
        return 0


# =============================================================================
# Idempotent writes
# =============================================================================


def _wipe_phase15_seed(wh: Warehouse) -> None:
    """DELETE all rows from the three seed tables. Safe — these are
    deterministically rebuilt from the xlsx on every run."""
    wh.execute(f"DELETE FROM {CONTROL_SCHEMA}.global_bronze_catalog_fields")
    wh.execute(f"DELETE FROM {CONTROL_SCHEMA}.global_bronze_catalog_datasets")
    wh.execute(f"DELETE FROM {CONTROL_SCHEMA}.onprem_routing_rules WHERE is_default = TRUE")


def _insert_dataset(
    wh: Warehouse,
    *,
    dataset_id: str,
    catalog_version: int,
    actor: str,
    source_doc_uri: str,
    row: dict[str, Any],
) -> None:
    wh.execute(
        f"""
        INSERT INTO {CONTROL_SCHEMA}.global_bronze_catalog_datasets
            (dataset_id, dataset_code, display_name, category, default_frequency,
             used_by, total_fields, required_fields, optional_fields, notes,
             is_active, catalog_version, source_doc_uri, registered_at, registered_by)
        VALUES ($id, $code, $name, $cat, $freq,
                $used, $tot, $req, $opt, $notes,
                TRUE, $ver, $src, $ts, $by)
        """,
        {
            "id": dataset_id,
            "code": row["dataset_code"],
            "name": row["display_name"],
            "cat": row["category"],
            "freq": row["default_frequency"],
            "used": json.dumps(row["used_by_list"]),
            "tot": row["total_fields"],
            "req": row["required_fields"],
            "opt": row["optional_fields"],
            "notes": row["notes"],
            "ver": catalog_version,
            "src": source_doc_uri,
            "ts": datetime.now(UTC),
            "by": actor,
        },
    )


def _insert_field(
    wh: Warehouse,
    *,
    field_id: str,
    dataset_id: str,
    dataset_code: str,
    catalog_version: int,
    row: dict[str, Any],
) -> None:
    wh.execute(
        f"""
        INSERT INTO {CONTROL_SCHEMA}.global_bronze_catalog_fields
            (field_id, dataset_id, dataset_code, field_order,
             field_display_name, bronze_column_name, requirement, logical_type,
             description, additional_notes, example,
             is_pii, is_phi, is_business_key, catalog_version, registered_at)
        VALUES ($id, $ds_id, $ds_code, $ord,
                $name, $gold, $req, $type,
                $desc, $notes, $ex,
                $pii, $phi, $bkey, $ver, $ts)
        """,
        {
            "id": field_id,
            "ds_id": dataset_id,
            "ds_code": dataset_code,
            "ord": row["field_order"],
            "name": row["field_display_name"],
            "gold": row["gold_column_name"],
            "req": row["requirement"],
            "type": row["logical_type"],
            "desc": row["description"],
            "notes": row["additional_notes"],
            "ex": row["example"],
            "pii": row["is_pii"],
            "phi": row["is_phi"],
            "bkey": row["is_business_key"],
            "ver": catalog_version,
            "ts": datetime.now(UTC),
        },
    )


def _insert_routing_rule(
    wh: Warehouse,
    *,
    dataset_code: str,
    product_code: str,
    actor: str,
) -> None:
    target_system, target_uri = _DEFAULT_TARGETS.get(product_code, ("postgres", ""))
    wh.execute(
        f"""
        INSERT INTO {CONTROL_SCHEMA}.onprem_routing_rules
            (rule_id, dataset_code, downstream_product, target_system, target_uri,
             is_default, is_active, notes, created_at, created_by)
        VALUES ($id, $ds, $prod, $tsys, $turi,
                TRUE, TRUE, $notes, $ts, $by)
        """,
        {
            "id": str(uuid.uuid4()),
            "ds": dataset_code,
            "prod": product_code,
            "tsys": target_system,
            "turi": target_uri,
            "notes": f"Default routing seeded from product catalog Used-by matrix ({_KNOWN_PRODUCTS.get(product_code, product_code)})",
            "ts": datetime.now(UTC),
            "by": actor,
        },
    )


# =============================================================================
# Orchestrator
# =============================================================================


def load_catalog(
    *,
    xlsx_path: Path,
    catalog_version: int = 1,
    actor: str = "system",
    dry_run: bool = False,
) -> dict[str, int]:
    """Top-level loader. Returns a dict of counts written."""
    if not xlsx_path.exists():
        raise FileNotFoundError(f"Product catalog not found: {xlsx_path}")

    wb = openpyxl.load_workbook(xlsx_path, data_only=True)
    datasets = _load_datasets_master(wb)
    fields_by_ds = _load_fields_catalogue(wb)

    print(f"Loaded {len(datasets)} datasets from {xlsx_path}")
    field_count = sum(len(f) for f in fields_by_ds.values())
    print(f"Loaded {field_count} fields across {len(fields_by_ds)} dataset groups")
    print()

    # Sanity check — datasets master should match fields catalogue groupings.
    master_names = {d["display_name"] for d in datasets}
    field_groups = set(fields_by_ds.keys())
    only_in_master = master_names - field_groups
    only_in_fields = field_groups - master_names
    if only_in_master:
        print(f"  WARN: datasets in master but not in fields: {sorted(only_in_master)}")
    if only_in_fields:
        print(f"  WARN: dataset groups in fields but not in master: {sorted(only_in_fields)}")
        print("        (will still seed fields under reconciled name)")
    print()

    if dry_run:
        print("DRY RUN — no Snowflake writes.")
        total_fields_by_ds = 0
        for ds in datasets:
            ds_fields = fields_by_ds.get(ds["display_name"], [])
            total_fields_by_ds += len(ds_fields)
            print(
                f"  would seed dataset '{ds['display_name']}' "
                f"({ds['dataset_code']}, {ds['default_frequency']}, "
                f"{len(ds_fields)} fields, used_by={ds['used_by_list']})"
            )
        return {
            "datasets": len(datasets),
            "fields_total_in_workbook": field_count,
            "fields_matched_to_master": total_fields_by_ds,
            "routing_rules": 0,
            "dry_run": 1,
        }

    # Phase 22 fix — try the UI's _build_backend path first.  It constructs
    # SnowflakeWarehouse directly from env vars and skips the factory chain
    # that pulls in azure-core (not installed in the Streamlit container).
    # Falls back to the full factory for CLI contexts where azure IS
    # available (host venv).
    try:
        from datalink.ui._query import _build_backend

        wh = _build_backend(readonly=False)
    except Exception:
        # Deferred imports — only loaded on the CLI fallback path.
        from datalink.adapters.factory import build_adapters
        from datalink.config.loader import load_settings

        settings = load_settings(env=os.environ.get("DL_ENV", "local"))
        adapters = build_adapters(settings)
        wh = adapters.warehouse
    print(f"Warehouse: {type(wh).__name__}")
    create_control_tables(wh)
    print("CONTROL tables ensured (idempotent).")

    print("Wiping prior seed rows in the three Phase 15 seed tables...")
    _wipe_phase15_seed(wh)

    print("Inserting datasets...")
    dataset_id_by_name: dict[str, str] = {}
    for ds in datasets:
        dataset_id = str(uuid.uuid4())
        dataset_id_by_name[ds["display_name"]] = dataset_id
        _insert_dataset(
            wh,
            dataset_id=dataset_id,
            catalog_version=catalog_version,
            actor=actor,
            source_doc_uri=str(xlsx_path.relative_to(_REPO_ROOT))
            if xlsx_path.is_relative_to(_REPO_ROOT)
            else str(xlsx_path),
            row=ds,
        )
    print(f"  -> wrote {len(datasets)} datasets")

    print("Inserting fields...")
    fields_written = 0
    fields_skipped: list[str] = []
    for ds_name, fields in fields_by_ds.items():
        # Reconcile: prefer master name; if a fields-only group exists,
        # synthesize a dataset row so we don't lose its fields silently.
        if ds_name not in dataset_id_by_name:
            # Try to map "Hierarchy" -> "Member Hierarchy" etc. by the master row
            # closest by category. Fallback: skip with warning so the operator
            # can fix the source xlsx in a follow-up.
            fields_skipped.append(ds_name)
            continue
        ds_id = dataset_id_by_name[ds_name]
        ds_code = to_dataset_code(ds_name)
        for f in fields:
            _insert_field(
                wh,
                field_id=str(uuid.uuid4()),
                dataset_id=ds_id,
                dataset_code=ds_code,
                catalog_version=catalog_version,
                row=f,
            )
            fields_written += 1
    print(f"  -> wrote {fields_written} fields")
    if fields_skipped:
        print(f"  -> skipped fields for unmatched dataset groups: {fields_skipped}")

    print("Inserting OnPrem routing rules from Used-by matrix...")
    routing_written = 0
    for ds in datasets:
        for product in ds["used_by_list"]:
            if product not in _KNOWN_PRODUCTS:
                continue
            _insert_routing_rule(
                wh,
                dataset_code=ds["dataset_code"],
                product_code=product,
                actor=actor,
            )
            routing_written += 1
    print(f"  -> wrote {routing_written} routing rules")

    return {
        "datasets": len(datasets),
        "fields": fields_written,
        "fields_skipped": len(fields_skipped),
        "routing_rules": routing_written,
        "catalog_version": catalog_version,
    }


# =============================================================================
# CLI entry
# =============================================================================


def _cli() -> None:
    ap = argparse.ArgumentParser(
        description="Load Product Catalog into the Global Gold Catalog tables."
    )
    ap.add_argument("--xlsx", type=Path, default=DEFAULT_XLSX, help="Path to product_catalog.xlsx")
    ap.add_argument("--catalog-version", type=int, default=1)
    ap.add_argument(
        "--by", type=str, default="system", help="Actor for audit (defaults to 'system')"
    )
    ap.add_argument("--dry-run", action="store_true", help="Print plan only — no writes")
    args = ap.parse_args()

    counts = load_catalog(
        xlsx_path=args.xlsx,
        catalog_version=args.catalog_version,
        actor=args.by,
        dry_run=args.dry_run,
    )
    print()
    print("Summary:")
    for k, v in counts.items():
        print(f"  {k:<20} {v}")


if __name__ == "__main__":
    _cli()
