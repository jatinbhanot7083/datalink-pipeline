"""Membership pipeline DAG factory — Phase 17.3.

Generates ONE Airflow DAG per LIVE row in
``CONTROL.client_pipeline_instances WHERE dataset_code='membership'``.
Per-client schedules + tier sizing flow through metadata, not code.

Adding a new client to Membership = INSERT a row in CONTROL — no edits
to this file. Removing a client = mark their instance ARCHIVED in
CONTROL — the corresponding DAG disappears on next scheduler reload.

This file replaces the hand-written ``dags/<client>_membership_pipeline.py``
files that existed pre-17.3. The old one (global_corp_membership_pipeline.py)
was removed in the same commit that introduced this factory.
"""

from __future__ import annotations

from _factory_common import register_dags_for_dataset  # noqa: E402  (Airflow adds dags_folder to sys.path)

DATASET = "membership"

# Calling register_dags_for_dataset injects each generated DAG into this
# module's globals() — Airflow's DagBag picks them up automatically.
register_dags_for_dataset(DATASET, globals())
