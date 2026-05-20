"""Phase 22 — product-line scoping for datasets.

Adds two artifacts:

  1. CONTROL.product_lines — master list (E360, CC, RBN, EC, ESV, plus
     EvokeConnectCare sub-modules CM/UM/AG/Provider/Member).
  2. CONTROL.dataset_product_lines — junction (dataset_code, product_line,
     scope_level) where scope_level is NULL for top-level and one of
     {'CM','UM','AG','Provider','Member'} for CC sub-modules.

Seeds product_lines table with the canonical list.  Does NOT seed the
junction — that's the catalogue loader's job (parses the Excel "Used by"
column).

Idempotent: safe to re-run.
"""

from __future__ import annotations

import sys

from datalink.adapters.factory import build_adapters
from datalink.config.loader import load_settings
from datalink.quality.control import CONTROL_SCHEMA

# Canonical product lines.  Source: DataLink_Product_Catalogue_Datasets.xlsx
# + user's Word document directive.  "Global Term" is a synthetic line meaning
# "every product line" — applied via a special row dataset_code='*'.
PRODUCT_LINES = [
    # code,       display name,                category,         description
    ("E360", "E360", "Platform", "E360 product platform"),
    (
        "CC",
        "EvokeConnectCare",
        "Platform",
        "EvokeConnectCare (case mgmt / utilization mgmt / appeals / provider / member)",
    ),
    ("CC_CM", "EvokeConnectCare · CM", "ConnectCare", "Case Management module"),
    ("CC_UM", "EvokeConnectCare · UM", "ConnectCare", "Utilization Management module"),
    ("CC_AG", "EvokeConnectCare · AG", "ConnectCare", "Appeals & Grievances module"),
    ("CC_PROV", "EvokeConnectCare · Provider", "ConnectCare", "Provider module"),
    ("CC_MBR", "EvokeConnectCare · Member", "ConnectCare", "Member module"),
    ("RBN", "RBN", "Platform", "RBN product"),
    ("EC", "EC", "Platform", "EC product"),
    ("ESV", "ESV", "Platform", "ESV product"),
]


def main() -> int:
    wh = build_adapters(load_settings(env="dev")).warehouse

    print("Step 1/3 — CREATE product_lines + dataset_product_lines tables")
    wh.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {CONTROL_SCHEMA}.product_lines (
            product_line_code   VARCHAR(64)  NOT NULL,
            display_name        VARCHAR(128) NOT NULL,
            category            VARCHAR(64),
            description         VARCHAR(512),
            is_global_term      BOOLEAN DEFAULT FALSE,
            created_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            PRIMARY KEY (product_line_code)
        )
        """
    )
    wh.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {CONTROL_SCHEMA}.dataset_product_lines (
            dataset_code        VARCHAR(64)  NOT NULL,
            product_line_code   VARCHAR(64)  NOT NULL,
            scope_level         VARCHAR(32),
            is_required         BOOLEAN DEFAULT TRUE,
            created_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            PRIMARY KEY (dataset_code, product_line_code)
        )
        """
    )

    print("Step 2/3 — seed product_lines master list (idempotent)")
    n_seeded = 0
    for code, name, category, desc in PRODUCT_LINES:
        wh.execute(
            f"MERGE INTO {CONTROL_SCHEMA}.product_lines tgt "
            f"USING (SELECT %(c)s code, %(n)s name, %(cat)s cat, %(d)s desc) src "
            f"ON tgt.product_line_code = src.code "
            f"WHEN NOT MATCHED THEN INSERT (product_line_code, display_name, category, description) "
            f"VALUES (src.code, src.name, src.cat, src.desc)",
            {"c": code, "n": name, "cat": category, "d": desc},
        )
        n_seeded += 1

    print(f"  -> seeded {n_seeded} product lines")

    print("Step 3/3 — verification")
    rows = list(
        wh.query(
            f"SELECT product_line_code, display_name, category FROM {CONTROL_SCHEMA}.product_lines "
            f"ORDER BY category, product_line_code"
        )
    )
    for r in rows:
        print(f"  {r['product_line_code']:10s}  {r['display_name']:32s}  {r.get('category','')}")

    print()
    print(
        "Migration 22 complete.  Next: run scripts/load_catalogue_xlsx.py to "
        "seed the 33 datasets + 943 fields + junction rows from the Excel."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
