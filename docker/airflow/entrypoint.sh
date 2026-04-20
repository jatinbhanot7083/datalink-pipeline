#!/bin/bash
# Airflow container entrypoint wrapper.
# Installs the bind-mounted datalink package editable, then hands off
# to the standard apache/airflow image entrypoint with whatever command
# docker-compose passed (init / webserver / scheduler).

set -e

# /opt/datalink is bind-mounted from the repo root at runtime.
if [ -f /opt/datalink/pyproject.toml ]; then
    pip install --quiet --no-deps -e /opt/datalink 2>/dev/null || true
fi

# Ensure dbt finds its project files and the warehouse at the same
# absolute paths the datalink tasks expect.
export DBT_PROFILES_DIR=/opt/datalink/dbt
export PYTHONPATH="/opt/datalink:${PYTHONPATH}"

# Hand off to the standard apache/airflow entrypoint.
exec /entrypoint "$@"
