"""End-to-end smoke test of the rewired DAG task callables.

Runs Bronze → Silver → Gold against the new Snowflake account, prints row
counts at each layer + a sample row from the final Gold table.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from datalink.orchestration.tasks import (  # noqa: E402
    bronze_land_task,
    gold_dbt_task,
    silver_dbt_task,
)


def main() -> int:
    print("=" * 60)
    print(" Bronze → Silver → Gold end-to-end smoke")
    print("=" * 60)

    print()
    print("[1/3] Bronze land")
    r1 = bronze_land_task(client_id="aetna", dataset_code="membership", bronze_anchor="FLAT_FILE")
    rows_loaded = r1.get("rows_loaded")
    total_rows = r1.get("total_rows")
    print(f"      rows_loaded = {rows_loaded}")
    print(f"      total_rows  = {total_rows}")
    print(f"      batch_id    = {r1.get('batch_id')}")

    print()
    print("[2/3] Silver dbt (raw SQL)")
    r2 = silver_dbt_task(client_id="aetna", dataset_code="membership")
    silver_loc = f"{r2.get('silver_schema')}.{r2.get('silver_table')}"
    print(f"      silver table = {silver_loc}")
    print(f"      rows         = {r2.get('rows')}")

    print()
    print("[3/3] Gold dbt (raw SQL)")
    r3 = gold_dbt_task(client_id="aetna", dataset_code="membership")
    gold_loc = f"{r3.get('gold_schema')}.{r3.get('gold_table')}"
    print(f"      gold table   = {gold_loc}")
    print(f"      rows         = {r3.get('rows')}")

    # Sample row from Gold
    print()
    print("=== Sample rows from Gold ===")
    import snowflake.connector

    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        role=os.environ["SNOWFLAKE_ROLE"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
    )
    cur = conn.cursor()
    cur.execute(
        f"SELECT member_card_id, member_first_name, member_last_name, member_state, "
        f"member_line_of_business FROM {gold_loc} ORDER BY member_card_id LIMIT 3"
    )
    for r in cur.fetchall():
        print(f"  {r}")
    cur.close()
    conn.close()

    print()
    print("🟢 END-TO-END SUCCESS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
