# Streamlit Control Tower — the single exec URL.
# Separate image from Airflow (which is 2 GB) because Streamlit doesn't
# need dbt/airflow/celery. We only bake in what the UI actually queries:
#   - streamlit              the UI runtime
#   - duckdb + pandas        Bronze/Silver/Gold row counts
#   - psycopg[binary]        Postgres row counts
#   - pyodbc + ODBC 18       SQL Server row counts
#   - paramiko               drop uploaded files onto SFTP
#   - requests + httpx       Airflow REST API calls (trigger DAGs)
#   - datalink-pipeline      bind-mounted editable for settings + adapters

FROM python:3.11-slim

ENV DEBIAN_FRONTEND=noninteractive \
    PIP_NO_CACHE_DIR=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# Microsoft ODBC Driver 18 for SQL Server + unixodbc runtime.
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      curl gnupg ca-certificates apt-transport-https \
 && curl -fsSL https://packages.microsoft.com/keys/microsoft.asc \
      | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg \
 && echo "deb [arch=amd64 signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" \
      > /etc/apt/sources.list.d/mssql-release.list \
 && apt-get update \
 && ACCEPT_EULA=Y apt-get install -y --no-install-recommends \
      unixodbc unixodbc-dev msodbcsql18 \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

# Python deps for the Control Tower. datalink is bind-mounted + installed
# editable at container start via the entrypoint.
# Phase 6: plotly added for Executive + CrewAI dashboard charts.
RUN pip install \
    "streamlit>=1.40,<2.0" \
    "streamlit-extras>=0.5,<1.0" \
    "plotly>=5.22,<7.0" \
    "duckdb>=1.0,<2.0" \
    "pandas>=2.2,<3.0" \
    "psycopg[binary]>=3.2,<4.0" \
    "pyodbc>=5.1,<6.0" \
    "paramiko>=3.4,<4.0" \
    "httpx>=0.27,<1.0" \
    "pydantic>=2.7,<3.0" \
    "pydantic-settings>=2.3,<3.0" \
    "pyyaml>=6.0,<7.0" \
    "structlog>=24.1,<25.0" \
    "click>=8.1,<9.0" \
    "python-dotenv>=1.0,<2.0"

WORKDIR /opt/datalink

# Entrypoint installs datalink editable from the bind-mount, then runs
# streamlit. Keeps the image source-less so code edits are hot-reloaded.
COPY entrypoint.sh /opt/entrypoint.sh
RUN chmod +x /opt/entrypoint.sh

EXPOSE 8000

ENTRYPOINT ["/opt/entrypoint.sh"]
