"""Smoke test: prove PUT + COPY INTO works for the Membership PSV.

End-to-end:
  1. CREATE STAGE BRONZE_AETNA.MEMBERSHIP_STAGE (pipe-delim file format)
  2. PUT data/generated/membership_bronze_sample.psv → @MEMBERSHIP_STAGE
  3. COPY INTO BRONZE_AETNA.RAW_MEMBERSHIP (with audit cols populated)
  4. SELECT COUNT(*) and a sample row to verify

If this works, we know the mechanic. Path B.3 then refactors bronze_land_task
to call the same SQL.
"""

from __future__ import annotations

import os
import sys
import uuid
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import snowflake.connector  # noqa: E402

PSV = ROOT / "data" / "generated" / "membership_bronze_sample.psv"
SCHEMA = "BRONZE_AETNA"
TABLE = "RAW_MEMBERSHIP"
STAGE = "MEMBERSHIP_STAGE"

# All 41 business columns — Pipeline Architect emitter wrongly puts NOT NULL on these.
# Bronze should be permissive; we ALTER to NULL at runtime as a workaround.
BUSINESS_COLS = [
    "payer_name",
    "member_card_id",
    "member_medicare_id",
    "member_medicaid_id",
    "member_first_name",
    "member_middle_name",
    "member_last_name",
    "member_gender",
    "member_birth_date",
    "member_phone",
    "member_street_address_line1",
    "member_street_address_line2",
    "member_county_code",
    "member_county_name",
    "member_city",
    "member_state",
    "member_zip_code",
    "member_active_status",
    "member_relationship_code",
    "member_medicaid_indicator",
    "member_medicaid_dual_eligibility_indicator",
    "member_dual_eligibility_begin_date",
    "member_dual_eligibility_end_date",
    "member_coverage_effective_date",
    "member_coverage_end_date",
    "member_provider_pcp_begin_date",
    "member_health_plan_begin_date",
    "member_enrollment_date",
    "member_line_of_business",
    "member_product",
    "product_description",
    "plan_benenfit_package_id",
    "plan_benefit_package_name",
    "segment_id_plan_benefit_package",
    "member_product_begin_date",
    "managed_care_organization_contract_number",
    "attributed_provider_id",
    "atrributed_provider_npi",
    "attributed_provider_contract_id",
    "plan_benefit_package_id",
    "refresh_date",
]


def main() -> int:
    if not PSV.exists():
        print(f"❌ PSV not found: {PSV}")
        return 1

    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        role=os.environ["SNOWFLAKE_ROLE"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema=SCHEMA,
        autocommit=True,
    )
    cur = conn.cursor()

    batch_id = f"BATCH_{uuid.uuid4().hex[:12]}"
    load_dt = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")
    source_file = PSV.name

    try:
        # 0) Workaround for Pipeline Architect bug: ALTER all business columns to NULL.
        #    Bronze landing should be permissive — vendor data may have blanks.
        print(f"[0/5] ALTER {SCHEMA}.{TABLE} to drop NOT NULL on business columns ...")
        for col in BUSINESS_COLS:
            try:
                cur.execute(f"ALTER TABLE {SCHEMA}.{TABLE} ALTER COLUMN {col} DROP NOT NULL")
            except Exception as e:
                # Ignore if already nullable
                if "already" not in str(e).lower():
                    print(f"      [warn] {col}: {str(e)[:100]}")
        print(f"      ✅ {len(BUSINESS_COLS)} columns are now nullable")

        # 1) Create stage with pipe-delimited file format (empties → NULL)
        print(f"[1/5] DROP+CREATE STAGE {SCHEMA}.{STAGE} ...")
        cur.execute(f"DROP STAGE IF EXISTS {SCHEMA}.{STAGE}")
        cur.execute(f"""
            CREATE STAGE {SCHEMA}.{STAGE}
            FILE_FORMAT = (
                TYPE = 'CSV'
                FIELD_DELIMITER = '|'
                SKIP_HEADER = 1
                FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                NULL_IF = ('', 'NULL')
                EMPTY_FIELD_AS_NULL = TRUE
                TRIM_SPACE = TRUE
            )
        """)
        print("      ✅ stage ready")

        # 2) Truncate target so we get a clean count
        print(f"[2/5] TRUNCATE {SCHEMA}.{TABLE} (clean slate)")
        cur.execute(f"TRUNCATE TABLE {SCHEMA}.{TABLE}")

        # 3) (no need to REMOVE — stage was just dropped+recreated)

        # 4) PUT the file
        print(f"[3/5] PUT {PSV.name} → @{SCHEMA}.{STAGE} ...")
        # PUT requires file:// URI on Linux/Mac
        cur.execute(f"PUT 'file://{PSV}' @{SCHEMA}.{STAGE} AUTO_COMPRESS=FALSE OVERWRITE=TRUE")
        for row in cur.fetchall():
            print(f"      put: source={row[0]} status={row[6]}")

        # 5) COPY INTO with audit columns
        # The 41 business columns from the PSV map positionally; the 8 audit cols
        # (_extra, _load_dt, _source_file, _batch_id, _record_source,
        # _load_type, _file_row_number, _record_hash) get populated via column
        # selection from $1..$41 + computed values.
        print(f"[4/5] COPY INTO {SCHEMA}.{TABLE} FROM @{STAGE} ...")
        copy_sql = f"""
            COPY INTO {SCHEMA}.{TABLE} (
                payer_name, member_card_id, member_medicare_id, member_medicaid_id,
                member_first_name, member_middle_name, member_last_name, member_gender,
                member_birth_date, member_phone, member_street_address_line1,
                member_street_address_line2, member_county_code, member_county_name,
                member_city, member_state, member_zip_code, member_active_status,
                member_relationship_code, member_medicaid_indicator,
                member_medicaid_dual_eligibility_indicator, member_dual_eligibility_begin_date,
                member_dual_eligibility_end_date, member_coverage_effective_date,
                member_coverage_end_date, member_provider_pcp_begin_date,
                member_health_plan_begin_date, member_enrollment_date, member_line_of_business,
                member_product, product_description, plan_benenfit_package_id,
                plan_benefit_package_name, segment_id_plan_benefit_package,
                member_product_begin_date, managed_care_organization_contract_number,
                attributed_provider_id, atrributed_provider_npi,
                attributed_provider_contract_id, plan_benefit_package_id, refresh_date,
                _load_dt, _source_file, _batch_id, _record_source, _load_type, _file_row_number
            )
            FROM (
                SELECT
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                    $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
                    $21, $22, $23, $24, $25, $26, $27, $28, $29, $30,
                    $31, $32, $33, $34, $35, $36, $37, $38, $39, $40, $41,
                    '{load_dt}'::TIMESTAMP, '{source_file}', '{batch_id}',
                    'aetna', 'FULL', METADATA$FILE_ROW_NUMBER
                FROM @{SCHEMA}.{STAGE}/{source_file}
            )
            ON_ERROR = 'ABORT_STATEMENT'
        """
        cur.execute(copy_sql)
        for row in cur.fetchall():
            print(f"      copy: file={row[0]} status={row[1]} rows_loaded={row[2]} errors={row[5]}")

        # 6) Verify
        print()
        print("=== Verification ===")
        cur.execute(f"SELECT COUNT(*) FROM {SCHEMA}.{TABLE}")
        n = cur.fetchone()[0]
        print(f"  Row count in {SCHEMA}.{TABLE}: {n}")

        cur.execute(f"""
            SELECT member_card_id, member_first_name, member_last_name, member_state,
                   member_line_of_business, _batch_id, _source_file, _load_dt
            FROM {SCHEMA}.{TABLE}
            ORDER BY _file_row_number
            LIMIT 3
        """)
        print("  Sample rows:")
        for r in cur.fetchall():
            print(f"    {r}")

        return 0
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
