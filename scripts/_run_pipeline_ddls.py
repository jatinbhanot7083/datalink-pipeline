"""One-shot: fix the Pipeline Architect DDL comma-in-comment bug at runtime
and execute Bronze + Gold DDLs against Snowflake. Also creates SILVER_AETNA
schema (Silver tables come from dbt later)."""

from __future__ import annotations

import os
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

# Auto-load .env so script works without `set -a && source .env` (Gotcha #1).
_ENV = ROOT / ".env"
if _ENV.exists():
    for raw in _ENV.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        v = v.strip().strip("'").strip('"')
        os.environ.setdefault(k.strip(), v)
os.environ.setdefault("DL_ENV", "dev")

from datalink.adapters.factory import build_adapters  # noqa: E402
from datalink.config.loader import load_settings  # noqa: E402

CLIENT = "aetna"
DATASET = "membership"

DDL_PATHS = {
    "BRONZE": ROOT / "datalink" / "pipeline" / "bronze" / "ddl" / f"{CLIENT}_{DATASET}.sql",
    "GOLD": ROOT / "datalink" / "pipeline" / "gold" / "ddl" / f"{CLIENT}_{DATASET}.sql",
}


def fix_inline_comma_bug(sql: str) -> str:
    """Move stray `,` from inside `-- comment,` to BEFORE the `--`.

    The Pipeline Architect emitter writes:
        col_name VARCHAR NOT NULL  -- [PII],
    Snowflake parses everything after `--` as comment, so the comma never
    becomes a column separator. Fix: rewrite to:
        col_name VARCHAR NOT NULL,  -- [PII]
    """
    out = []
    for ln in sql.splitlines():
        stripped = ln.lstrip()
        # Skip pure-comment lines (whole line is a comment)
        if stripped.startswith("--"):
            out.append(ln)
            continue
        # Match: <code>  -- <comment>,<optional ws>
        m = re.match(r"^(\s+\S.*?\S)\s+--\s*(.*?),\s*$", ln)
        if m:
            code = m.group(1)
            comment = m.group(2)
            out.append(f"{code},  -- {comment}")
        else:
            out.append(ln)
    return "\n".join(out)


def main() -> int:
    settings = load_settings(env=os.environ.get("DL_ENV", "dev"))
    wh = build_adapters(settings).warehouse
    print(f"Warehouse: {type(wh).__name__}")
    print()

    # 1. Create the three schemas
    for sch in (f"BRONZE_{CLIENT.upper()}", f"SILVER_{CLIENT.upper()}", f"GOLD_{CLIENT.upper()}"):
        wh.execute(f"CREATE SCHEMA IF NOT EXISTS {sch}")
        print(f"  [OK] CREATE SCHEMA IF NOT EXISTS {sch}")
    print()

    # 2. Execute Bronze + Gold DDLs (with comma-bug fix)
    for layer, path in DDL_PATHS.items():
        if not path.exists():
            print(f"  [skip] {layer}: {path} not found")
            continue
        sql_raw = path.read_text(encoding="utf-8")
        sql_fixed = fix_inline_comma_bug(sql_raw)
        if sql_raw != sql_fixed:
            print(f"  [patch] {layer}: applied comma-in-comment fix at runtime")
        # Strip leading comment-only block, run a single CREATE TABLE statement
        # (DDL files contain exactly one statement followed by SQL comments)
        try:
            wh.execute(sql_fixed)
            print(f"  [OK]   {layer}: executed {path.relative_to(ROOT)}")
        except Exception as e:
            print(f"  [FAIL] {layer}: {str(e)[:200]}")
            print()
            print("  --- corrected SQL preview (first 30 lines) ---")
            for line in sql_fixed.splitlines()[:30]:
                print("    " + line)
            return 2

    # 3. Workaround for Pipeline Architect bug: ALTER all business columns to NULL.
    # Bronze landing should be permissive — vendor data may have blanks, and Gold
    # COPY-from-Silver also breaks if Gold inherits NOT NULL on conditional fields.
    print()
    print("=" * 72)
    print(" Dropping NOT NULL on business columns (workaround for emitter bug)")
    print("=" * 72)
    for _layer, sch_table in [
        ("BRONZE", f"BRONZE_{CLIENT.upper()}.RAW_{DATASET.upper()}"),
        ("GOLD", f"GOLD_{CLIENT.upper()}.{DATASET.upper()}"),
    ]:
        sch, _, tbl = sch_table.partition(".")
        # Discover columns
        col_rows = list(
            wh.query(
                f"SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS "
                f"WHERE TABLE_SCHEMA = '{sch}' AND TABLE_NAME = '{tbl}' "
                f"ORDER BY ordinal_position"
            )
        )
        biz_cols = [r["column_name"] for r in col_rows if not r["column_name"].startswith("_")]
        n_altered = 0
        for col in biz_cols:
            try:
                wh.execute(f"ALTER TABLE {sch_table} ALTER COLUMN {col} DROP NOT NULL")
                n_altered += 1
            except Exception:
                pass  # already nullable
        print(f"  [OK]  {sch_table}: {n_altered}/{len(biz_cols)} business cols set NULL-permissive")
    print()

    # 3. Verify
    print("=== Verification ===")
    schemas = [s["name"] for s in wh.query("SHOW SCHEMAS IN DATABASE DATALINK_DEV")]
    for s in (f"BRONZE_{CLIENT.upper()}", f"SILVER_{CLIENT.upper()}", f"GOLD_{CLIENT.upper()}"):
        flag = "PRESENT" if s in schemas else "MISSING"
        print(f"  Schema {s:25s} {flag}")
    for sch in (f"BRONZE_{CLIENT.upper()}", f"GOLD_{CLIENT.upper()}"):
        if sch not in schemas:
            continue
        tabs = list(wh.query(f"SHOW TABLES IN SCHEMA DATALINK_DEV.{sch}"))
        print(f"  Tables in {sch}: {len(tabs)}")
        for t in tabs:
            tname = t["name"]
            # Get column count
            ccount = next(
                iter(
                    wh.query(
                        f"SELECT COUNT(*) AS c FROM INFORMATION_SCHEMA.COLUMNS "
                        f"WHERE TABLE_SCHEMA = '{sch}' AND TABLE_NAME = '{tname}'"
                    )
                )
            )["c"]
            print(f"     - {tname}  ({ccount} cols)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
