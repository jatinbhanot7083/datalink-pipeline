"""Reset for a clean end-to-end re-run for aetna/membership + aetna/provider.

What gets cleared:
  * Bronze tables (DELETE rows; keep schema)
  * Silver tables (DROP — they're CTAS-rebuilt by silver_dbt_task)
  * Gold tables  (DELETE rows; keep schema)
  * CONTROL.dataset_pipeline_runs rows for aetna
  * Airflow DAG run history for aetna_membership_pipeline + aetna_provider_pipeline

What is preserved:
  * Pipeline instance rows (status=LIVE)
  * Schemas + table DDLs (so next run doesn't need bootstrap)
  * GLOBAL_CORP templates + DMD silver/gold metadata
  * Airflow DAG registrations + schedule
"""

import os
import subprocess

import snowflake.connector

CLIENT = "AETNA"

# --- 1. Snowflake reset --------------------------------------------------
conn = snowflake.connector.connect(
    account=os.environ["SNOWFLAKE_ACCOUNT"],
    user=os.environ["SNOWFLAKE_USER"],
    password=os.environ["SNOWFLAKE_PASSWORD"],
    role=os.environ["SNOWFLAKE_ROLE"],
    warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
    database=os.environ["SNOWFLAKE_DATABASE"],
)
cur = conn.cursor()

print("=" * 70)
print(f"Snowflake reset for {CLIENT}")
print("=" * 70)

# Bronze + Gold: DELETE rows (preserve table)
for tbl in (
    f"BRONZE_{CLIENT}.RAW_MEMBERSHIP",
    f"BRONZE_{CLIENT}.RAW_PROVIDER",
    f"GOLD_{CLIENT}.MEMBERSHIP",
    f"GOLD_{CLIENT}.PROVIDER",
):
    try:
        cur.execute(f"DELETE FROM {tbl}")
        print(f"  ✓ DELETE FROM {tbl}  ({cur.rowcount or 0} rows)")
    except Exception as exc:
        print(f"  ⚠ {tbl} skip: {str(exc)[:80]}")

# Silver: DROP (silver_dbt_task does DROP+CREATE on every run)
for tbl in (
    f"SILVER_{CLIENT}.MEMBERSHIP_CLEAN",
    f"SILVER_{CLIENT}.PROVIDER_CLEAN",
):
    try:
        cur.execute(f"DROP TABLE IF EXISTS {tbl}")
        print(f"  ✓ DROP TABLE {tbl}")
    except Exception as exc:
        print(f"  ⚠ {tbl} skip: {str(exc)[:80]}")

# CONTROL.dataset_pipeline_runs
cur.execute(
    "DELETE FROM CONTROL.dataset_pipeline_runs WHERE LOWER(client_id) = %s",
    (CLIENT.lower(),),
)
print(f"  ✓ DELETE FROM CONTROL.dataset_pipeline_runs  ({cur.rowcount or 0} rows)")

conn.commit()
conn.close()

# --- 2. Airflow DAG run history -----------------------------------------
print()
print("=" * 70)
print("Airflow DAG run history reset")
print("=" * 70)
for dag in ("aetna_membership_pipeline", "aetna_provider_pipeline"):
    # `airflow tasks clear` clears task instances for the DAG; pair with
    # explicit dag_run delete to wipe the history rows too.
    r = subprocess.run(
        [
            "docker",
            "exec",
            "datalink-airflow-scheduler",
            "airflow",
            "db",
            "clean",
            "--clean-before-timestamp",
            "2099-01-01",
            "--tables",
            "dag_run,task_instance,task_fail,xcom",
            "--dry-run",  # safety: dry-run first; flip below for real
        ],
        capture_output=True,
        text=True,
        timeout=20,
    )
    # The cleaner approach: directly delete via the Airflow CLI's
    # `dags delete` would also drop the registration.  Instead we use
    # the airflow metadata DB via `airflow connections` style commands —
    # but the simplest is to delete dag_run rows with a postgres exec.
    pass

# Use Airflow's postgres directly to nuke just dag_run + task_instance for the 2 DAGs
PG_CMD = (
    "PGPASSWORD=airflow psql -h localhost -U airflow -d airflow -c "
    "\"DELETE FROM task_instance WHERE dag_id IN ('aetna_membership_pipeline','aetna_provider_pipeline'); "
    "DELETE FROM dag_run WHERE dag_id IN ('aetna_membership_pipeline','aetna_provider_pipeline');\""
)
r = subprocess.run(
    ["docker", "exec", "datalink-airflow-db", "bash", "-c", PG_CMD],
    capture_output=True,
    text=True,
    timeout=15,
)
print(r.stdout.strip() or r.stderr.strip()[:500])

print()
print("✅ Reset complete. Click ▶ Run now on a LIVE instance for a fresh run.")
