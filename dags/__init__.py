"""Airflow DAGs for DataLink pipelines.

Every DAG in this folder is a thin wrapper around task callables defined in
`datalink.orchestration.tasks` + pipeline graphs in
`datalink.orchestration.pipelines`. The same callables run under the
local_sequential orchestrator — no task logic is duplicated.

Airflow's scheduler discovers DAGs by walking this directory at parse time
(`dags_folder` in `airflow.cfg` → `/opt/airflow/dags` in the Helm chart).
"""
