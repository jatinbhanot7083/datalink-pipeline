# Custom Airflow image for DataLink demo.
# Starts from the official 2.9.3 python-3.11 image, bakes in:
#   - Microsoft ODBC Driver 18 for SQL Server (+ unixodbc runtime)
#   - dbt-core + dbt-duckdb so Airflow tasks can shell out to dbt
#   - the datalink package + its Phase 2/3/4/5 extras (ingest, silver,
#     operational, quality, agents)
# The scheduler + webserver run this same image; scheduler executes the
# PythonOperator tasks in-process (LocalExecutor), so the DuckDB file lock
# is never contested between tasks of a single DAG run.

FROM apache/airflow:2.9.3-python3.11

USER root

# Microsoft ODBC driver 18 for SQL Server (needed by pyodbc, the router, and
# the SqlServerOperationalDb adapter). We pull Microsoft's apt repo keys,
# install, and clean the cache in one layer.
RUN apt-get update \
 && apt-get install -y --no-install-recommends curl gnupg lsb-release ca-certificates apt-transport-https \
 && curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg \
 && echo "deb [arch=amd64 signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" > /etc/apt/sources.list.d/mssql-release.list \
 && apt-get update \
 && ACCEPT_EULA=Y apt-get install -y --no-install-recommends unixodbc unixodbc-dev msodbcsql18 \
 && rm -rf /var/lib/apt/lists/*

USER airflow

# Install the Python stack in one pip invocation so resolver can reconcile.
# Pinned providers match Airflow 2.9.x. dbt stays on 1.8 (dbt-duckdb 1.8).
# datalink is installed editable from the bind-mount at /opt/datalink so
# code changes are hot-reloaded without rebuilding the image.
# apache-airflow constraint URL is the official one for the EXACT Airflow
# version — prevents dep-resolver blow-ups.
ARG AIRFLOW_VERSION=2.9.3
ARG PYTHON_VERSION=3.11
ARG CONSTRAINT_URL=https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt

RUN pip install --no-cache-dir \
    --constraint "${CONSTRAINT_URL}" \
    "apache-airflow-providers-postgres" \
    "apache-airflow-providers-microsoft-mssql" \
    "apache-airflow-providers-http" \
 && pip install --no-cache-dir \
    "pyodbc>=5.1,<6.0" \
    "psycopg[binary]>=3.2,<4.0" \
    "sqlalchemy>=1.4.36,<2.0" \
    "paramiko>=3.4,<4.0" \
    "duckdb>=1.0,<2.0" \
    "pandas>=2.2,<3.0" \
    "dbt-core>=1.8,<1.9" \
    "dbt-duckdb>=1.8,<1.9" \
    "dbt-snowflake>=1.8,<1.9" \
    "snowflake-connector-python>=3.11,<4.0" \
    "voyageai>=0.3,<1.0" \
    "great-expectations>=1.0,<2.0" \
    "structlog>=24.1,<25.0" \
    "pydantic>=2.7,<3.0" \
    "pydantic-settings>=2.3,<3.0" \
    "pyyaml>=6.0,<7.0" \
    "click>=8.1,<9.0" \
    "python-dotenv>=1.0,<2.0" \
    "anthropic>=0.40,<1.0"

# datalink itself will be installed editable at container-start via the
# scheduler entrypoint (after the bind-mount is in place). See docker-compose.yml.
