"""Phase 15.8 — Silver schema importers.

Parses external sources into the canonical Silver proposal shape:

  * DDL_SQL              — paste a CREATE TABLE statement (treats it as NORMALIZED)
  * DBT_YAML             — paste a dbt schema.yml model spec (treats it as NORMALIZED)
  * DBT_PROJECT          — paste a multi-model dbt project YAML with HUBs/SATs/LINKs
  * JSON_SCHEMA          — paste a JSON Schema (treats it as NORMALIZED)

Each parser returns the same shape:

    {
      "silver_pattern": "NORMALIZED" | "HUB_SAT_LINK",
      "silver_tables": [<SilverTable dicts>],
      "bronze_to_silver_mappings": [],   # IMPORT defaults to no mappings —
                                         # operator wires later or AI fills in
      "rationale": "Imported from <format>; review tables + add mappings."
    }
"""

from __future__ import annotations

import json
import re
from typing import Any

# Reuse the type-coercion logic from gold_schema_designer.importers — same
# 6 logical types apply to Silver columns.
from datalink.agents.gold_schema_designer.importers import (
    _coerce_type as _gold_coerce_type,
)
from datalink.agents.gold_schema_designer.importers import (
    _looks_phi,
    _looks_pii,
)

# ----------------------------------------------------------------------------
# DDL_SQL — parse CREATE TABLE → single NORMALIZED Silver table
# ----------------------------------------------------------------------------


def parse_ddl_sql(text: str, *, dataset_code: str = "imported") -> dict[str, Any]:
    s = text.strip().rstrip(";").strip()
    m = re.search(
        r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:[\w]+\.)?([\w]+)",
        s,
        re.IGNORECASE,
    )
    if not m:
        raise ValueError("Could not find CREATE TABLE clause.")
    table = m.group(1).lower()

    first = s.find("(")
    last = s.rfind(")")
    if first < 0 or last < 0 or first >= last:
        raise ValueError("Could not find column list.")
    body = s[first + 1 : last]

    columns: list[dict[str, Any]] = []
    depth = 0
    current: list[str] = []
    parts: list[str] = []
    for ch in body:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        if ch == "," and depth == 0:
            parts.append("".join(current))
            current = []
            continue
        current.append(ch)
    if current:
        parts.append("".join(current))

    for raw_line in parts:
        line = re.sub(r"--.*$", "", raw_line.strip()).strip()
        if not line:
            continue
        if re.match(
            r"^(PRIMARY\s+KEY|UNIQUE|CHECK|FOREIGN\s+KEY|CONSTRAINT)\b", line, re.IGNORECASE
        ):
            continue
        cm = re.match(r"^[\"`]?([\w]+)[\"`]?\s+([\w]+(?:\([\w,\s]+\))?)", line)
        if not cm:
            continue
        name = cm.group(1).lower()
        if name.startswith("_"):
            continue
        sql_type = cm.group(2).strip()
        nullable = "NOT NULL" not in line.upper()
        is_business_key = bool(re.search(r"(^|_)id$", name) or name in {"npi"})
        columns.append(
            {
                "column_name": name,
                "logical_type": _gold_coerce_type(sql_type),
                "nullable": nullable,
                "is_business_key": is_business_key,
                "is_pii": _looks_pii(name),
                "is_phi": _looks_phi(name),
                "description": f"Imported from DDL_SQL — original type: {sql_type}",
            }
        )

    silver_table = {
        "table_name": table,
        "table_kind": "NORMALIZED",
        "business_keys": [c["column_name"] for c in columns if c.get("is_business_key")],
        "parent_hub_name": None,
        "linked_hub_names": [],
        "description": "Imported NORMALIZED Silver table from CREATE TABLE DDL.",
        "columns": columns,
    }
    return _wrap("NORMALIZED", [silver_table], "Imported from CREATE TABLE DDL (NORMALIZED).")


# ----------------------------------------------------------------------------
# DBT_YAML — parse a dbt schema.yml model entry → NORMALIZED Silver
# ----------------------------------------------------------------------------


def parse_dbt_yaml(text: str) -> dict[str, Any]:
    try:
        import yaml  # type: ignore[import-untyped,unused-ignore]
    except ImportError as e:
        raise RuntimeError("PyYAML is required for DBT_YAML import.") from e
    doc = yaml.safe_load(text)
    if not isinstance(doc, dict):
        raise ValueError("DBT_YAML root must be a YAML document.")
    models = doc.get("models")
    if not isinstance(models, list) or not models:
        raise ValueError("DBT_YAML must contain a top-level 'models' list.")
    model = models[0]
    table_name = str(model.get("name", "")).lower()
    if not table_name:
        raise ValueError("DBT_YAML first model is missing 'name'.")
    cols_raw = model.get("columns") or []

    columns: list[dict[str, Any]] = []
    for c in cols_raw:
        if not isinstance(c, dict):
            continue
        name = str(c.get("name", "")).lower()
        if not name or name.startswith("_"):
            continue
        data_type = str(c.get("data_type") or c.get("type") or "TEXT")
        tests = c.get("tests") or []
        nullable = True
        is_business_key = False
        for t in tests if isinstance(tests, list) else []:
            t_str = str(t).lower() if isinstance(t, str) else json.dumps(t).lower()
            if "not_null" in t_str:
                nullable = False
            if "unique" in t_str or "primary_key" in t_str:
                is_business_key = True
        columns.append(
            {
                "column_name": name,
                "logical_type": _gold_coerce_type(data_type),
                "nullable": nullable,
                "is_business_key": is_business_key,
                "is_pii": _looks_pii(name),
                "is_phi": _looks_phi(name),
                "description": str(c.get("description") or ""),
            }
        )

    silver_table = {
        "table_name": table_name,
        "table_kind": "NORMALIZED",
        "business_keys": [c["column_name"] for c in columns if c.get("is_business_key")],
        "parent_hub_name": None,
        "linked_hub_names": [],
        "description": "Imported NORMALIZED Silver table from dbt schema.yml.",
        "columns": columns,
    }
    return _wrap("NORMALIZED", [silver_table], "Imported from dbt schema.yml (NORMALIZED).")


# ----------------------------------------------------------------------------
# DBT_PROJECT — multi-model dbt YAML → HUB_SAT_LINK Silver
# ----------------------------------------------------------------------------


def parse_dbt_project_yaml(text: str) -> dict[str, Any]:
    """Parse a dbt schema.yml with multiple models, classifying each by name
    convention: HUB_*, SAT_*, LINK_*.
    """
    try:
        import yaml  # type: ignore[import-untyped,unused-ignore]
    except ImportError as e:
        raise RuntimeError("PyYAML is required for DBT_PROJECT import.") from e
    doc = yaml.safe_load(text)
    if not isinstance(doc, dict):
        raise ValueError("DBT_PROJECT YAML must be a document.")
    models = doc.get("models")
    if not isinstance(models, list) or not models:
        raise ValueError("DBT_PROJECT must contain a 'models' list.")

    silver_tables: list[dict[str, Any]] = []
    for m in models:
        if not isinstance(m, dict):
            continue
        name = str(m.get("name", ""))
        upper = name.upper()
        if upper.startswith("HUB_"):
            kind = "HUB"
        elif upper.startswith("SAT_"):
            kind = "SAT"
        elif upper.startswith("LINK_"):
            kind = "LINK"
        else:
            continue  # ignore non-DV2 models
        cols_raw = m.get("columns") or []
        columns: list[dict[str, Any]] = []
        bks: list[str] = []
        for c in cols_raw:
            if not isinstance(c, dict):
                continue
            cname = str(c.get("name", "")).lower()
            if not cname:
                continue
            ctype = str(c.get("data_type") or c.get("type") or "TEXT")
            is_hash_key = cname == "hash_key"
            is_hash_diff = cname == "hash_diff"
            is_business_key = False
            for t in c.get("tests") or []:
                t_str = str(t).lower() if isinstance(t, str) else json.dumps(t).lower()
                if "primary_key" in t_str or "unique" in t_str:
                    is_business_key = True
                    bks.append(cname)
            columns.append(
                {
                    "column_name": cname,
                    "logical_type": _gold_coerce_type(ctype),
                    "nullable": not is_hash_key,
                    "is_business_key": is_business_key,
                    "is_hash_key": is_hash_key,
                    "is_hash_diff": is_hash_diff,
                    "is_pii": _looks_pii(cname),
                    "is_phi": _looks_phi(cname),
                    "description": str(c.get("description") or ""),
                }
            )
        silver_tables.append(
            {
                "table_name": name,
                "table_kind": kind,
                "business_keys": bks if kind == "HUB" else [],
                "parent_hub_name": str(m.get("parent_hub")) if kind == "SAT" else None,
                "linked_hub_names": list(m.get("linked_hubs") or []) if kind == "LINK" else [],
                "description": str(m.get("description") or f"Imported {kind} from dbt project."),
                "columns": columns,
            }
        )

    if not silver_tables:
        raise ValueError(
            "DBT_PROJECT YAML had no HUB_*/SAT_*/LINK_* models — refusing empty import."
        )
    return _wrap("HUB_SAT_LINK", silver_tables, "Imported from dbt project YAML (HUB_SAT_LINK).")


# ----------------------------------------------------------------------------
# JSON_SCHEMA — flat JSON Schema → NORMALIZED Silver
# ----------------------------------------------------------------------------


def parse_json_schema(text: str) -> dict[str, Any]:
    try:
        doc = json.loads(text)
    except json.JSONDecodeError as e:
        raise ValueError(f"JSON_SCHEMA: invalid JSON — {e}") from e
    title = str(doc.get("title", "")) or "imported"
    table_name = re.sub(r"[^\w]+", "_", title).strip("_").lower() or "imported"
    properties = doc.get("properties") or {}
    if not isinstance(properties, dict) or not properties:
        raise ValueError("JSON_SCHEMA must have a non-empty `properties` object.")
    required = set(doc.get("required") or [])
    columns: list[dict[str, Any]] = []
    for name, spec in properties.items():
        col_name = str(name).lower()
        if col_name.startswith("_"):
            continue
        if not isinstance(spec, dict):
            continue
        js_type = str(spec.get("type") or "string")
        fmt = str(spec.get("format") or "")
        from datalink.agents.gold_schema_designer.importers import (
            _json_schema_type_to_logical,
        )

        logical = _json_schema_type_to_logical(js_type, fmt)
        columns.append(
            {
                "column_name": col_name,
                "logical_type": logical,
                "nullable": col_name not in required,
                "is_business_key": col_name == "id" or col_name.endswith("_id"),
                "is_pii": _looks_pii(col_name),
                "is_phi": _looks_phi(col_name),
                "description": str(spec.get("description") or "")[:300],
            }
        )
    silver_table = {
        "table_name": table_name,
        "table_kind": "NORMALIZED",
        "business_keys": [c["column_name"] for c in columns if c.get("is_business_key")],
        "parent_hub_name": None,
        "linked_hub_names": [],
        "description": "Imported NORMALIZED Silver table from JSON Schema.",
        "columns": columns,
    }
    return _wrap("NORMALIZED", [silver_table], "Imported from JSON Schema (NORMALIZED).")


# ----------------------------------------------------------------------------
# Wrapper
# ----------------------------------------------------------------------------


def _wrap(
    silver_pattern: str, silver_tables: list[dict[str, Any]], rationale: str
) -> dict[str, Any]:
    if not silver_tables:
        raise ValueError("Importer produced no Silver tables — refusing empty import.")
    return {
        "silver_pattern": silver_pattern,
        "silver_tables": silver_tables,
        "bronze_to_silver_mappings": [],
        "rationale": rationale,
    }


def parse(import_format: str, text: str, *, dataset_code: str = "imported") -> dict[str, Any]:
    fmt = import_format.upper()
    if fmt == "DDL_SQL":
        return parse_ddl_sql(text, dataset_code=dataset_code)
    if fmt == "DBT_YAML":
        return parse_dbt_yaml(text)
    if fmt == "DBT_PROJECT":
        return parse_dbt_project_yaml(text)
    if fmt == "JSON_SCHEMA":
        return parse_json_schema(text)
    raise ValueError(
        f"Unknown import_format {import_format!r}. "
        f"Supported: DDL_SQL, DBT_YAML, DBT_PROJECT, JSON_SCHEMA."
    )
