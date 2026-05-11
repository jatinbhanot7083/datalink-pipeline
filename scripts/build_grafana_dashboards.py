"""Phase 20.2 — generate the 3 DataLink Grafana dashboards.

Idempotent: re-running overwrites the JSON files in
``docker/grafana/dashboards/``.  Grafana's file-provisioning provider
(see ``docker/grafana/provisioning/dashboards/dashboards.yaml``) re-loads
them automatically every 10 seconds.

Three dashboards built:
  * datalink_pipeline_runs.json — fleet-level pipeline activity
  * datalink_dq_quality.json    — DQ pass-rate + drift
  * datalink_ai_economics.json  — token spend, proposal volume, latency
"""

from __future__ import annotations

import json
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
OUT = REPO / "docker" / "grafana" / "dashboards"
PROM_DS = {"type": "prometheus", "uid": "datalink-prometheus"}


def panel(id, title, type, gridPos, targets, **extra):
    p = {
        "id": id,
        "title": title,
        "type": type,
        "gridPos": gridPos,
        "datasource": PROM_DS,
        "targets": targets,
        "options": extra.pop("options", {}),
        "fieldConfig": extra.pop("fieldConfig", {"defaults": {}, "overrides": []}),
    }
    p.update(extra)
    return p


def query(expr, legend=None, range_=False):
    t = {"expr": expr, "refId": "A", "datasource": PROM_DS}
    if legend:
        t["legendFormat"] = legend
    if range_:
        t["range"] = True
        t["instant"] = False
    return t


def dashboard(uid, title, description, panels, refresh="30s"):
    return {
        "uid": uid,
        "title": title,
        "description": description,
        "editable": True,
        "graphTooltip": 1,
        "refresh": refresh,
        "schemaVersion": 39,
        "tags": ["datalink", "phase20"],
        "time": {"from": "now-6h", "to": "now"},
        "timepicker": {},
        "timezone": "",
        "version": 1,
        "panels": panels,
        "templating": {"list": []},
        "annotations": {"list": []},
    }


# =========================================================================
# Dashboard 1 — Pipeline Runs
# =========================================================================
def build_pipeline_runs():
    panels = []
    panels.append(
        panel(
            1,
            "Total Runs (24h)",
            "stat",
            {"h": 4, "w": 4, "x": 0, "y": 0},
            [query("sum(increase(datalink_pipeline_runs_total[24h]))", "runs")],
            options={"reduceOptions": {"calcs": ["lastNotNull"]}, "colorMode": "value"},
            fieldConfig={"defaults": {"unit": "short", "color": {"mode": "palette-classic"}}},
        )
    )
    panels.append(
        panel(
            2,
            "Success Rate (24h)",
            "stat",
            {"h": 4, "w": 4, "x": 4, "y": 0},
            [
                query(
                    '100 * sum(increase(datalink_pipeline_runs_total{status="SUCCESS"}[24h])) / clamp_min(sum(increase(datalink_pipeline_runs_total[24h])),1)',
                    "pct",
                )
            ],
            fieldConfig={
                "defaults": {
                    "unit": "percent",
                    "min": 0,
                    "max": 100,
                    "thresholds": {
                        "mode": "absolute",
                        "steps": [
                            {"color": "red"},
                            {"color": "orange", "value": 80},
                            {"color": "green", "value": 95},
                        ],
                    },
                }
            },
        )
    )
    panels.append(
        panel(
            3,
            "Active Datasets",
            "stat",
            {"h": 4, "w": 4, "x": 8, "y": 0},
            [query("count(count by (dataset) (datalink_pipeline_runs_total))", "datasets")],
            fieldConfig={"defaults": {"unit": "short"}},
        )
    )
    panels.append(
        panel(
            4,
            "Active Clients",
            "stat",
            {"h": 4, "w": 4, "x": 12, "y": 0},
            [query("count(count by (client) (datalink_pipeline_runs_total))", "clients")],
            fieldConfig={"defaults": {"unit": "short"}},
        )
    )
    panels.append(
        panel(
            5,
            "Bronze Rows / hr",
            "stat",
            {"h": 4, "w": 4, "x": 16, "y": 0},
            [query("sum(rate(datalink_bronze_rows_loaded_total[1h])) * 3600", "rows/hr")],
            fieldConfig={"defaults": {"unit": "short"}},
        )
    )
    panels.append(
        panel(
            6,
            "Gold Rows / hr",
            "stat",
            {"h": 4, "w": 4, "x": 20, "y": 0},
            [query("sum(rate(datalink_gold_rows_total[1h])) * 3600", "rows/hr")],
            fieldConfig={"defaults": {"unit": "short"}},
        )
    )

    panels.append(
        panel(
            10,
            "Pipeline Runs by Status",
            "timeseries",
            {"h": 8, "w": 12, "x": 0, "y": 4},
            [
                query(
                    "sum by (status) (rate(datalink_pipeline_runs_total[5m])) * 60",
                    "{{status}}",
                    range_=True,
                )
            ],
            fieldConfig={
                "defaults": {
                    "unit": "short",
                    "custom": {"drawStyle": "line", "lineWidth": 2, "fillOpacity": 30},
                }
            },
        )
    )
    panels.append(
        panel(
            11,
            "Rows Processed by Layer",
            "timeseries",
            {"h": 8, "w": 12, "x": 12, "y": 4},
            [
                query(
                    "sum(rate(datalink_bronze_rows_loaded_total[5m])) * 60", "Bronze", range_=True
                ),
                query("sum(rate(datalink_silver_rows_total[5m])) * 60", "Silver", range_=True),
                query("sum(rate(datalink_gold_rows_total[5m])) * 60", "Gold", range_=True),
            ],
            fieldConfig={
                "defaults": {
                    "unit": "short",
                    "custom": {"drawStyle": "line", "lineWidth": 2, "fillOpacity": 30},
                }
            },
        )
    )

    panels.append(
        panel(
            12,
            "Runs by Dataset (24h)",
            "bargauge",
            {"h": 8, "w": 12, "x": 0, "y": 12},
            [
                query(
                    "sum by (dataset) (increase(datalink_pipeline_runs_total[24h]))", "{{dataset}}"
                )
            ],
            options={"orientation": "horizontal", "displayMode": "gradient"},
            fieldConfig={"defaults": {"unit": "short", "color": {"mode": "continuous-BlYlRd"}}},
        )
    )
    panels.append(
        panel(
            13,
            "Runs by Client (24h)",
            "bargauge",
            {"h": 8, "w": 12, "x": 12, "y": 12},
            [query("sum by (client) (increase(datalink_pipeline_runs_total[24h]))", "{{client}}")],
            options={"orientation": "horizontal", "displayMode": "gradient"},
            fieldConfig={"defaults": {"unit": "short", "color": {"mode": "continuous-GrYlRd"}}},
        )
    )

    panels.append(
        panel(
            14,
            "Task Duration (p50 / p95)",
            "timeseries",
            {"h": 8, "w": 24, "x": 0, "y": 20},
            [
                query(
                    "histogram_quantile(0.50, sum by (task, le) (rate(datalink_task_duration_seconds_bucket[5m])))",
                    "p50 {{task}}",
                    range_=True,
                ),
                query(
                    "histogram_quantile(0.95, sum by (task, le) (rate(datalink_task_duration_seconds_bucket[5m])))",
                    "p95 {{task}}",
                    range_=True,
                ),
            ],
            fieldConfig={
                "defaults": {"unit": "s", "custom": {"drawStyle": "line", "lineWidth": 2}}
            },
        )
    )

    return dashboard(
        "dl-pipeline-runs",
        "DataLink — Pipeline Runs",
        "Phase 20.2 — pipeline-run telemetry across the factory pattern.  Counts, durations, rows-per-layer, by-dataset and by-client breakdowns.",
        panels,
    )


# =========================================================================
# Dashboard 2 — Data Quality
# =========================================================================
def build_dq_quality():
    panels = []
    panels.append(
        panel(
            1,
            "Total DQ Checks (24h)",
            "stat",
            {"h": 4, "w": 6, "x": 0, "y": 0},
            [query("sum(increase(datalink_dq_checks_total[24h]))", "checks")],
            fieldConfig={"defaults": {"unit": "short"}},
        )
    )
    panels.append(
        panel(
            2,
            "Pass Rate (24h)",
            "stat",
            {"h": 4, "w": 6, "x": 6, "y": 0},
            [
                query(
                    '100 * sum(increase(datalink_dq_checks_total{result="pass"}[24h])) / clamp_min(sum(increase(datalink_dq_checks_total[24h])),1)',
                    "pct",
                )
            ],
            fieldConfig={
                "defaults": {
                    "unit": "percent",
                    "min": 0,
                    "max": 100,
                    "thresholds": {
                        "mode": "absolute",
                        "steps": [
                            {"color": "red"},
                            {"color": "orange", "value": 90},
                            {"color": "green", "value": 98},
                        ],
                    },
                }
            },
        )
    )
    panels.append(
        panel(
            3,
            "Failures (24h)",
            "stat",
            {"h": 4, "w": 6, "x": 12, "y": 0},
            [query('sum(increase(datalink_dq_checks_total{result="fail"}[24h]))', "failures")],
            fieldConfig={
                "defaults": {"unit": "short", "color": {"mode": "fixed", "fixedColor": "red"}}
            },
        )
    )
    panels.append(
        panel(
            4,
            "Schema Drift (7d)",
            "stat",
            {"h": 4, "w": 6, "x": 18, "y": 0},
            [query("sum(increase(datalink_schema_drift_events_total[7d]))", "events")],
            fieldConfig={
                "defaults": {"unit": "short", "color": {"mode": "fixed", "fixedColor": "orange"}}
            },
        )
    )

    panels.append(
        panel(
            10,
            "DQ Pass-Rate Over Time by Layer",
            "timeseries",
            {"h": 9, "w": 12, "x": 0, "y": 4},
            [
                query(
                    '100 * sum by (layer) (rate(datalink_dq_checks_total{result="pass"}[5m])) / clamp_min(sum by (layer) (rate(datalink_dq_checks_total[5m])), 0.001)',
                    "{{layer}}",
                    range_=True,
                )
            ],
            fieldConfig={
                "defaults": {
                    "unit": "percent",
                    "min": 0,
                    "max": 100,
                    "custom": {"drawStyle": "line", "lineWidth": 2},
                }
            },
        )
    )
    panels.append(
        panel(
            11,
            "DQ Checks by Dataset (24h)",
            "bargauge",
            {"h": 9, "w": 12, "x": 12, "y": 4},
            [
                query(
                    "sum by (dataset, result) (increase(datalink_dq_checks_total[24h]))",
                    "{{dataset}} ({{result}})",
                )
            ],
            options={"orientation": "horizontal", "displayMode": "gradient"},
            fieldConfig={"defaults": {"unit": "short"}},
        )
    )

    panels.append(
        panel(
            12,
            "DQ Failures by Dataset+Layer",
            "timeseries",
            {"h": 8, "w": 12, "x": 0, "y": 13},
            [
                query(
                    'sum by (dataset, layer) (rate(datalink_dq_checks_total{result="fail"}[5m]))',
                    "{{dataset}} / {{layer}}",
                    range_=True,
                )
            ],
            fieldConfig={
                "defaults": {
                    "unit": "short",
                    "custom": {"drawStyle": "bars", "lineWidth": 1, "fillOpacity": 80},
                }
            },
        )
    )
    panels.append(
        panel(
            13,
            "Schema Drift Events Timeline",
            "timeseries",
            {"h": 8, "w": 12, "x": 12, "y": 13},
            [
                query(
                    "sum by (dataset, severity) (rate(datalink_schema_drift_events_total[5m]))",
                    "{{dataset}} [{{severity}}]",
                    range_=True,
                )
            ],
            fieldConfig={
                "defaults": {
                    "unit": "short",
                    "custom": {"drawStyle": "bars", "lineWidth": 1, "fillOpacity": 70},
                }
            },
        )
    )

    return dashboard(
        "dl-dq-quality",
        "DataLink — Data Quality",
        "Phase 20.2 — DQ check pass/fail rates, per-layer breakdowns, schema-drift event timeline.",
        panels,
    )


# =========================================================================
# Dashboard 3 — AI Economics
# =========================================================================
def build_ai_economics():
    panels = []
    panels.append(
        panel(
            1,
            "Total Tokens (7d)",
            "stat",
            {"h": 4, "w": 6, "x": 0, "y": 0},
            [query("sum(increase(datalink_ai_tokens_total[7d]))", "tokens")],
            fieldConfig={"defaults": {"unit": "short"}},
        )
    )
    # Cost: Haiku 4.5 blended ~$0.000003 / token average between input/output
    panels.append(
        panel(
            2,
            "Est. Spend (7d, Haiku 4.5)",
            "stat",
            {"h": 4, "w": 6, "x": 6, "y": 0},
            [query("sum(increase(datalink_ai_tokens_total[7d])) * 0.000003", "usd")],
            fieldConfig={
                "defaults": {
                    "unit": "currencyUSD",
                    "decimals": 3,
                    "color": {"mode": "fixed", "fixedColor": "purple"},
                }
            },
        )
    )
    panels.append(
        panel(
            3,
            "Proposals (7d)",
            "stat",
            {"h": 4, "w": 6, "x": 12, "y": 0},
            [query("sum(increase(datalink_ai_proposals_total[7d]))", "proposals")],
            fieldConfig={"defaults": {"unit": "short"}},
        )
    )
    panels.append(
        panel(
            4,
            "Latency p95 (1h)",
            "stat",
            {"h": 4, "w": 6, "x": 18, "y": 0},
            [
                query(
                    "histogram_quantile(0.95, sum by (le) (rate(datalink_ai_latency_seconds_bucket[1h])))",
                    "p95",
                )
            ],
            fieldConfig={"defaults": {"unit": "s", "decimals": 1}},
        )
    )

    panels.append(
        panel(
            10,
            "Tokens / hour by Agent",
            "timeseries",
            {"h": 9, "w": 12, "x": 0, "y": 4},
            [
                query(
                    "sum by (agent) (rate(datalink_ai_tokens_total[5m])) * 3600",
                    "{{agent}}",
                    range_=True,
                )
            ],
            fieldConfig={
                "defaults": {
                    "unit": "short",
                    "custom": {
                        "drawStyle": "line",
                        "lineWidth": 2,
                        "fillOpacity": 30,
                        "stacking": {"mode": "normal"},
                    },
                }
            },
        )
    )
    panels.append(
        panel(
            11,
            "Proposals by Agent (7d)",
            "piechart",
            {"h": 9, "w": 6, "x": 12, "y": 4},
            [query("sum by (agent) (increase(datalink_ai_proposals_total[7d]))", "{{agent}}")],
            options={"pieType": "donut", "legend": {"displayMode": "list", "placement": "right"}},
        )
    )
    panels.append(
        panel(
            12,
            "Latency p50 / p95 by Agent",
            "timeseries",
            {"h": 9, "w": 6, "x": 18, "y": 4},
            [
                query(
                    "histogram_quantile(0.50, sum by (agent, le) (rate(datalink_ai_latency_seconds_bucket[5m])))",
                    "p50 {{agent}}",
                    range_=True,
                ),
                query(
                    "histogram_quantile(0.95, sum by (agent, le) (rate(datalink_ai_latency_seconds_bucket[5m])))",
                    "p95 {{agent}}",
                    range_=True,
                ),
            ],
            fieldConfig={
                "defaults": {"unit": "s", "custom": {"drawStyle": "line", "lineWidth": 2}}
            },
        )
    )

    panels.append(
        panel(
            13,
            "Proposal Outcomes (success vs failed) (7d)",
            "timeseries",
            {"h": 8, "w": 24, "x": 0, "y": 13},
            [
                query(
                    "sum by (agent, outcome) (increase(datalink_ai_proposals_total[1h]))",
                    "{{agent}} ({{outcome}})",
                    range_=True,
                )
            ],
            fieldConfig={
                "defaults": {
                    "unit": "short",
                    "custom": {"drawStyle": "bars", "lineWidth": 1, "fillOpacity": 80},
                }
            },
        )
    )

    return dashboard(
        "dl-ai-economics",
        "DataLink — AI Economics",
        "Phase 20.2 — token spend, proposal volume, and latency for AI agents (DQ Architect, Pipeline Architect).",
        panels,
    )


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    for name, builder in [
        ("datalink_pipeline_runs.json", build_pipeline_runs),
        ("datalink_dq_quality.json", build_dq_quality),
        ("datalink_ai_economics.json", build_ai_economics),
    ]:
        path = OUT / name
        path.write_text(json.dumps(builder(), indent=2))
        print(f"  wrote {path.relative_to(REPO)}  ({path.stat().st_size:,} bytes)")
    print("Done.  Grafana picks up changes within 10 seconds.")


if __name__ == "__main__":
    main()
