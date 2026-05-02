"""Phase 15.6 — Gold schema importers.

Parses external schema sources into the canonical GoldSchemaProposal shape
so the IMPORT mode can populate global_gold_schema_* without an LLM call.

Supported formats:

  * DDL_SQL              — operator pastes a CREATE TABLE statement
  * DBT_YAML             — operator pastes a dbt schema.yml model spec
  * FHIR_PROFILE_JSON    — operator pastes a FHIR R4 StructureDefinition
  * JSON_SCHEMA          — operator pastes a JSON Schema document
  * SNOWFLAKE_DESCRIBE   — operator pastes the output of `DESCRIBE TABLE`

Each parser returns the same shape:

    {
      "proposed_gold_table_name": str,
      "proposed_columns": [<GoldColumn dicts>],
      "bronze_to_gold_mappings": [],   # IMPORT defaults to no mappings yet —
                                       # operator wires them later or the
                                       # AI follow-up agent fills them in
      "silver_pattern_recommendation": {
        "recommended_pattern": "HUB_SAT_LINK",  # default; operator can change
        "is_overkill_flag": False,
        "reasoning": "Default DV2 — review after import.",
        "proposed_silver_shape": {}
      },
      "rationale": "Imported from <format>; review columns + add mappings."
    }

These parsers are conservative — when in doubt they emit logical_type=TEXT
and let the operator refine via the UI grid.
"""

from __future__ import annotations

import json
import re
from typing import Any

# ----------------------------------------------------------------------------
# Type-mapping helper — coerces vendor-specific SQL types to our 6 logical types.
# ----------------------------------------------------------------------------

_TYPE_MAP: dict[str, str] = {
    # Strings
    "varchar": "TEXT",
    "char": "TEXT",
    "text": "TEXT",
    "string": "TEXT",
    "nvarchar": "TEXT",
    "nchar": "TEXT",
    # Integers
    "int": "INTEGER",
    "integer": "INTEGER",
    "bigint": "INTEGER",
    "smallint": "INTEGER",
    "tinyint": "INTEGER",
    "number": "INTEGER",  # Snowflake NUMBER without scale -> INTEGER
    # Decimals
    "decimal": "DECIMAL",
    "numeric": "DECIMAL",
    "float": "DECIMAL",
    "double": "DECIMAL",
    "real": "DECIMAL",
    # Dates
    "date": "DATE",
    # Timestamps
    "timestamp": "TIMESTAMP",
    "datetime": "TIMESTAMP",
    "datetime2": "TIMESTAMP",
    "timestamp_ntz": "TIMESTAMP",
    "timestamp_ltz": "TIMESTAMP",
    "timestamp_tz": "TIMESTAMP",
    # Booleans
    "boolean": "BOOLEAN",
    "bool": "BOOLEAN",
    "bit": "BOOLEAN",
}


def _coerce_type(raw_type: str) -> str:
    """Map an arbitrary vendor type string to one of our 6 logical types."""
    if not raw_type:
        return "TEXT"
    base = re.split(r"\s|\(", raw_type.strip().lower(), maxsplit=1)[0]
    return _TYPE_MAP.get(base, "TEXT")


# ----------------------------------------------------------------------------
# DDL_SQL — parse CREATE TABLE
# ----------------------------------------------------------------------------


def parse_ddl_sql(text: str) -> dict[str, Any]:
    """Parse a CREATE TABLE statement into a Gold proposal.

    Tolerant — handles:
      - CREATE TABLE foo.bar (...);
      - CREATE TABLE IF NOT EXISTS schema.table (...)
      - Column lines with NOT NULL, DEFAULT, COMMENT, inline comments

    Strips out trailing audit columns starting with `_`.
    """
    s = text.strip().rstrip(";").strip()

    # Pull the table name out
    m = re.search(
        r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:[\w]+\.)?([\w]+)",
        s,
        re.IGNORECASE,
    )
    if not m:
        raise ValueError("Could not find CREATE TABLE clause.")
    table = m.group(1).lower()

    # Extract the column list between first '(' and last ')'
    first = s.find("(")
    last = s.rfind(")")
    if first < 0 or last < 0 or first >= last:
        raise ValueError("Could not find column list.")
    body = s[first + 1 : last]

    columns: list[dict[str, Any]] = []
    # Split on commas at top level (depth-aware to skip commas inside DECIMAL(10,2))
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
        line = raw_line.strip()
        # strip inline `-- ...` comments
        line = re.sub(r"--.*$", "", line).strip()
        if not line:
            continue
        # Skip table-level constraints
        if re.match(
            r"^(PRIMARY\s+KEY|UNIQUE|CHECK|FOREIGN\s+KEY|CONSTRAINT)\b", line, re.IGNORECASE
        ):
            continue
        # Match column name + type
        cm = re.match(r"^[\"`]?([\w]+)[\"`]?\s+([\w]+(?:\([\w,\s]+\))?)", line)
        if not cm:
            continue
        name = cm.group(1).lower()
        if name.startswith("_"):
            # Skip audit columns
            continue
        sql_type = cm.group(2).strip()
        nullable = "NOT NULL" not in line.upper()
        # Heuristic: if column name contains '_id' it's likely a business key
        is_business_key = bool(re.search(r"(^|_)id$", name) or name in {"npi"})
        columns.append(
            {
                "gold_column_name": name,
                "logical_type": _coerce_type(sql_type),
                "nullable": nullable,
                "is_business_key": is_business_key,
                "is_pii": _looks_pii(name),
                "is_phi": _looks_phi(name),
                "description": "",
                "anchor_reference": None,
                "rationale": f"Imported from DDL_SQL — original SQL type: {sql_type}",
            }
        )

    return _wrap_proposal(table, columns, "Imported from CREATE TABLE DDL.")


# ----------------------------------------------------------------------------
# DBT_YAML — parse a dbt schema.yml models entry
# ----------------------------------------------------------------------------


def parse_dbt_yaml(text: str) -> dict[str, Any]:
    """Parse a dbt schema.yml model block (PyYAML-based)."""
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
    if not isinstance(cols_raw, list):
        raise ValueError("DBT_YAML model.columns must be a list.")

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
                "gold_column_name": name,
                "logical_type": _coerce_type(data_type),
                "nullable": nullable,
                "is_business_key": is_business_key,
                "is_pii": _looks_pii(name),
                "is_phi": _looks_phi(name),
                "description": str(c.get("description") or ""),
                "anchor_reference": None,
                "rationale": f"Imported from DBT_YAML — original data_type: {data_type}",
            }
        )
    return _wrap_proposal(table_name, columns, "Imported from dbt schema.yml.")


# ----------------------------------------------------------------------------
# FHIR_PROFILE_JSON — parse a FHIR StructureDefinition resource
# ----------------------------------------------------------------------------


def parse_fhir_profile(text: str) -> dict[str, Any]:
    """Parse a FHIR R4 StructureDefinition JSON.

    Walks the `differential.element[]` (or `snapshot.element[]`) list and
    extracts each leaf element as a Gold column.
    """
    try:
        doc = json.loads(text)
    except json.JSONDecodeError as e:
        raise ValueError(f"FHIR_PROFILE_JSON: invalid JSON — {e}") from e

    if doc.get("resourceType") != "StructureDefinition":
        raise ValueError("FHIR_PROFILE_JSON: resourceType must be 'StructureDefinition'.")

    resource_type = str(doc.get("type", "")) or doc.get("name", "Resource")
    table_name = re.sub(r"[^\w]+", "_", str(resource_type)).strip("_").lower() or "fhir_resource"

    diff = doc.get("differential") or {}
    snapshot = doc.get("snapshot") or {}
    elements = (
        (diff.get("element") if isinstance(diff, dict) else None)
        or (snapshot.get("element") if isinstance(snapshot, dict) else None)
        or []
    )
    if not isinstance(elements, list) or not elements:
        raise ValueError("FHIR profile has no differential/snapshot.element[] entries.")

    columns: list[dict[str, Any]] = []
    seen: set[str] = set()
    for el in elements:
        if not isinstance(el, dict):
            continue
        path = str(el.get("path") or el.get("id") or "")
        if not path or "." not in path:
            continue  # root or malformed
        # path = 'Patient.identifier' → gold_column_name='identifier'
        # path = 'Patient.address.line' → 'address_line' (collapse nested)
        parts_path = path.split(".")[1:]
        if not parts_path:
            continue
        name = "_".join(re.sub(r"[^\w]+", "_", p).strip("_") for p in parts_path).lower()
        if not name or name in seen:
            continue
        seen.add(name)
        # Type
        types = el.get("type") or []
        if isinstance(types, list) and types:
            type_code = str(types[0].get("code", "string"))
        else:
            type_code = "string"
        logical = _fhir_type_to_logical(type_code)
        # Cardinality — min=0 → nullable
        min_card = el.get("min", 0)
        try:
            nullable = int(min_card) == 0
        except (TypeError, ValueError):
            nullable = True
        # Description
        description = str(el.get("short") or el.get("definition") or "")[:300]
        # Business key heuristic
        is_business_key = name in {"id", "identifier"}
        columns.append(
            {
                "gold_column_name": name,
                "logical_type": logical,
                "nullable": nullable,
                "is_business_key": is_business_key,
                "is_pii": _looks_pii(name),
                "is_phi": _looks_phi(name),
                "description": description,
                "anchor_reference": f"FHIR {resource_type}.{'.'.join(parts_path)}",
                "rationale": f"Imported from FHIR_PROFILE_JSON — type={type_code}, min={min_card}.",
            }
        )

    return _wrap_proposal(
        table_name,
        columns,
        f"Imported from FHIR R4 StructureDefinition for {resource_type}.",
    )


def _fhir_type_to_logical(type_code: str) -> str:
    code = type_code.lower()
    if code in {"string", "code", "id", "uri", "url", "uuid", "oid", "markdown", "canonical"}:
        return "TEXT"
    if code in {"integer", "positiveint", "unsignedint"}:
        return "INTEGER"
    if code in {"decimal"}:
        return "DECIMAL"
    if code in {"date"}:
        return "DATE"
    if code in {"datetime", "instant", "time"}:
        return "TIMESTAMP"
    if code in {"boolean"}:
        return "BOOLEAN"
    return "TEXT"


# ----------------------------------------------------------------------------
# JSON_SCHEMA — parse a JSON Schema document
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
        logical = _json_schema_type_to_logical(js_type, fmt)
        columns.append(
            {
                "gold_column_name": col_name,
                "logical_type": logical,
                "nullable": col_name not in required,
                "is_business_key": col_name in {"id"} or col_name.endswith("_id"),
                "is_pii": _looks_pii(col_name),
                "is_phi": _looks_phi(col_name),
                "description": str(spec.get("description") or "")[:300],
                "anchor_reference": None,
                "rationale": f"Imported from JSON Schema — type={js_type}, format={fmt or 'n/a'}.",
            }
        )
    return _wrap_proposal(table_name, columns, "Imported from JSON Schema.")


def _json_schema_type_to_logical(js_type: str, fmt: str) -> str:
    t = js_type.lower()
    f = fmt.lower()
    if t == "integer":
        return "INTEGER"
    if t == "number":
        return "DECIMAL"
    if t == "boolean":
        return "BOOLEAN"
    if f == "date":
        return "DATE"
    if f in {"date-time", "datetime"}:
        return "TIMESTAMP"
    return "TEXT"


# ----------------------------------------------------------------------------
# SNOWFLAKE_DESCRIBE — parse the output of `DESCRIBE TABLE` (text format)
# ----------------------------------------------------------------------------


def parse_snowflake_describe(text: str, *, table_hint: str = "imported") -> dict[str, Any]:
    """Parse the row-grid output of Snowflake's `DESCRIBE TABLE`.

    Expected columns: name | type | kind | null? | default | primary key | unique key | check | expression | comment
    Rows separated by newlines, columns separated by `|` or by 2+ spaces.
    """
    lines = [ln.rstrip() for ln in text.splitlines() if ln.strip()]
    if not lines:
        raise ValueError("SNOWFLAKE_DESCRIBE: empty input.")

    # Detect separator
    sep_re = re.compile(r"\s*\|\s*") if "|" in lines[0] else re.compile(r"\s{2,}")

    # Header
    header = [h.strip().lower() for h in sep_re.split(lines[0])]

    def _idx(*names: str) -> int:
        for n in names:
            if n in header:
                return header.index(n)
        return -1

    col_name_idx = _idx("name", "column_name")
    col_type_idx = _idx("type", "data_type")
    col_null_idx = _idx("null?", "is_nullable", "nullable")
    col_pk_idx = _idx("primary key", "primary_key", "pk")
    col_comment_idx = _idx("comment")

    if col_name_idx < 0 or col_type_idx < 0:
        raise ValueError(
            "SNOWFLAKE_DESCRIBE: header must contain at least 'name' and 'type' columns."
        )

    columns: list[dict[str, Any]] = []
    for ln in lines[1:]:
        # Skip dashed dividers
        if set(ln.strip()) <= {"-", "+", "|", " "}:
            continue
        cells = sep_re.split(ln)
        if len(cells) <= max(col_name_idx, col_type_idx):
            continue
        name = cells[col_name_idx].strip().lower()
        if not name or name.startswith("_"):
            continue
        sql_type = cells[col_type_idx].strip()
        nullable = True
        if 0 <= col_null_idx < len(cells):
            nullable = cells[col_null_idx].strip().upper() in {"Y", "YES", "TRUE", "1"}
        is_business_key = False
        if 0 <= col_pk_idx < len(cells):
            is_business_key = cells[col_pk_idx].strip().upper() in {"Y", "YES", "TRUE"}
        description = ""
        if 0 <= col_comment_idx < len(cells):
            description = cells[col_comment_idx].strip()
        columns.append(
            {
                "gold_column_name": name,
                "logical_type": _coerce_type(sql_type),
                "nullable": nullable,
                "is_business_key": is_business_key or name.endswith("_id"),
                "is_pii": _looks_pii(name),
                "is_phi": _looks_phi(name),
                "description": description,
                "anchor_reference": None,
                "rationale": f"Imported from SNOWFLAKE_DESCRIBE — original type: {sql_type}",
            }
        )

    return _wrap_proposal(
        re.sub(r"[^\w]+", "_", table_hint).strip("_").lower() or "imported",
        columns,
        "Imported from Snowflake DESCRIBE TABLE output.",
    )


# ----------------------------------------------------------------------------
# Wrappers + heuristics
# ----------------------------------------------------------------------------


def _wrap_proposal(table: str, columns: list[dict[str, Any]], rationale: str) -> dict[str, Any]:
    """Wrap an imported column list into the canonical proposal shape."""
    if not columns:
        raise ValueError("Importer produced zero columns — refusing to import an empty schema.")
    return {
        "proposed_gold_table_name": table,
        "proposed_columns": columns,
        # IMPORT mode does NOT auto-generate Bronze→Gold mappings.
        # Operator wires them on the next page; or invokes the AI agent
        # via a follow-up to generate mappings against this imported shape.
        "bronze_to_gold_mappings": [],
        "silver_pattern_recommendation": {
            "recommended_pattern": "HUB_SAT_LINK",
            "is_overkill_flag": False,
            "reasoning": "Default DV2 — review after import; the agent can re-recommend later.",
            "proposed_silver_shape": {},
        },
        "rationale": rationale,
    }


_PII_HINTS = re.compile(
    r"\b(name|first|middle|last|surname|phone|mobile|email|fax|"
    r"address|street|city|state|zip|county|"
    r"ssn|social[\s_]?security|tin|"
    r"birth[\s_]?date|dob)\b",
    re.IGNORECASE,
)
_PHI_HINTS = re.compile(
    r"\b(diagnosis|icd|procedure|cpt|hcpcs|medicare|medicaid|"
    r"coverage|plan|benefit|claim|encounter|admission|discharge|"
    r"member|patient|subscriber|npi)\b",
    re.IGNORECASE,
)


def _looks_pii(name: str) -> bool:
    return bool(_PII_HINTS.search(name))


def _looks_phi(name: str) -> bool:
    return bool(_PHI_HINTS.search(name))


# ----------------------------------------------------------------------------
# Public dispatcher
# ----------------------------------------------------------------------------


def parse(import_format: str, text: str, *, table_hint: str = "imported") -> dict[str, Any]:
    """Top-level parser dispatcher used by the bridge."""
    fmt = import_format.upper()
    if fmt == "DDL_SQL":
        return parse_ddl_sql(text)
    if fmt == "DBT_YAML":
        return parse_dbt_yaml(text)
    if fmt == "FHIR_PROFILE_JSON":
        return parse_fhir_profile(text)
    if fmt == "JSON_SCHEMA":
        return parse_json_schema(text)
    if fmt == "SNOWFLAKE_DESCRIBE":
        return parse_snowflake_describe(text, table_hint=table_hint)
    raise ValueError(
        f"Unknown import_format {import_format!r}. "
        f"Supported: DDL_SQL, DBT_YAML, FHIR_PROFILE_JSON, JSON_SCHEMA, SNOWFLAKE_DESCRIBE."
    )
