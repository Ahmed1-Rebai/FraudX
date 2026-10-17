"""Generates the provisioned Grafana dashboard (dashboards as code).

    python infra/grafana/build_dashboard.py

writes infra/grafana/provisioning/dashboards/fraudx-pipeline.json. Grafana
loads it at start (`docker compose restart grafana` after a change). Risk
levels use the status colors of the React dashboard.
"""

import json
from pathlib import Path

OUT = Path(__file__).parent / "provisioning" / "dashboards" / "fraudx-pipeline.json"
DS = {"type": "prometheus", "uid": "prometheus"}
LEVEL_COLORS = {"LOW": "#0ca30c", "MEDIUM": "#fab219", "HIGH": "#d03b3b"}

panels: list[dict] = []
y = 0


def target(expr: str, legend: str = "", ref: str = "A") -> dict:
    return {"datasource": DS, "expr": expr, "legendFormat": legend, "refId": ref}


def row(title: str) -> None:
    global y
    panels.append(
        {
            "type": "row",
            "title": title,
            "collapsed": False,
            "panels": [],
            "gridPos": {"h": 1, "w": 24, "x": 0, "y": y},
        }
    )
    y += 1


def stats(items: list[tuple]) -> None:
    """(title, expr, unit, description, thresholds [(value, color)], decimals)."""
    global y
    w = 24 // len(items)
    for i, (title, expr, unit, desc, thresholds, decimals) in enumerate(items):
        steps = [{"color": "green", "value": None}] + [
            {"color": c, "value": v} for v, c in thresholds
        ]
        panels.append(
            {
                "type": "stat",
                "title": title,
                "description": desc,
                "datasource": DS,
                "gridPos": {"h": 4, "w": w, "x": i * w, "y": y},
                "targets": [target(expr)],
                "options": {
                    "reduceOptions": {"calcs": ["lastNotNull"]},
                    "colorMode": "value",
                    "graphMode": "area",
                    "textMode": "value",
                },
                "fieldConfig": {
                    "defaults": {
                        "unit": unit,
                        "noValue": "0",
                        "decimals": decimals,
                        "thresholds": {"mode": "absolute", "steps": steps},
                        "color": {
                            "mode": "thresholds" if thresholds else "fixed",
                            "fixedColor": "text",
                        },
                    }
                },
            }
        )
    y += 4


def series(title, exprs, unit, desc, x, w, overrides=None, stack=False, no_value=None):
    defaults = {
        "unit": unit,
        "custom": {
            "lineWidth": 2,
            "fillOpacity": 10,
            "showPoints": "never",
            "stacking": {"mode": "normal" if stack else "none"},
        },
    }
    if no_value:
        defaults["noValue"] = no_value
    panels.append(
        {
            "type": "timeseries",
            "title": title,
            "description": desc,
            "datasource": DS,
            "gridPos": {"h": 8, "w": w, "x": x, "y": y},
            "targets": [target(e, legend, chr(65 + i)) for i, (e, legend) in enumerate(exprs)],
            "options": {
                "legend": {"displayMode": "list", "placement": "bottom"},
                "tooltip": {"mode": "multi", "sort": "desc"},
            },
            "fieldConfig": {"defaults": defaults, "overrides": overrides or []},
        }
    )


def level_overrides() -> list[dict]:
    return [
        {
            "matcher": {"id": "byName", "options": level},
            "properties": [{"id": "color", "value": {"mode": "fixed", "fixedColor": color}}],
        }
        for level, color in LEVEL_COLORS.items()
    ]


def per_message_p95(worker: str) -> str:
    return (
        f"histogram_quantile(0.95, sum by (le) "
        f'(rate(fraudx_consumer_processing_seconds_bucket{{worker="{worker}"}}[5m])))'
    )


row("Pipeline health")
stats(
    [
        (
            "Scored",
            "sum(rate(fraudx_scored_total[1m]))",
            "suffix: tx/s",
            "Transactions scored per second (1-minute rate).",
            [],
            1,
        ),
        (
            "Alert rate (1 h)",
            "sum(increase(fraudx_alerts_total[1h])) / sum(increase(fraudx_scored_total[1h]))",
            "percentunit",
            "Fraud detection rate: share of scored transactions that raised an alert.",
            [],
            2,
        ),
        (
            "Average risk score (5 m)",
            "sum(rate(fraudx_risk_score_sum[5m])) / sum(rate(fraudx_risk_score_count[5m]))",
            "none",
            "Mean 0-100 score.",
            [],
            1,
        ),
        (
            "Consumer lag",
            "sum(fraudx_consumer_lag)",
            "short",
            "Messages behind the log end, all consumer groups.",
            [(1000, "orange"), (10000, "red")],
            0,
        ),
        (
            "Dead-lettered (1 h)",
            'sum(increase(fraudx_consumer_messages_total{outcome="dlq"}[1h]))',
            "short",
            "Invalid messages sent to transactions.dlq.",
            [(1, "orange")],
            0,
        ),
        (
            "Scored without a signal (1 h)",
            "sum(increase(fraudx_signal_missing_total[1h]))",
            "short",
            "Degraded scoring, e.g. Neo4j unavailable (ADR-031).",
            [(1, "orange")],
            0,
        ),
    ]
)
series(
    "Throughput by stage",
    [('sum by (worker) (rate(fraudx_consumer_messages_total{outcome="ok"}[1m]))', "{{worker}}")],
    "ops",
    "Messages processed per second by each stream worker.",
    0,
    12,
)
series(
    "Consumer lag by group",
    [("sum by (group) (fraudx_consumer_lag)", "{{group}}")],
    "short",
    "Backlog per consumer group; a growing line means a stage can't keep up or is retrying.",
    12,
    12,
)
y += 8

row("Latency")
series(
    "Stage latency p95 (produce → processed)",
    [
        (
            "histogram_quantile(0.95, sum by (le, worker) "
            "(rate(fraudx_consumer_message_age_seconds_bucket[5m])))",
            "{{worker}}",
        )
    ],
    "s",
    "Time from a message being produced to its topic until the stage processed it.",
    0,
    12,
)
series(
    "Scorer time per transaction by stage",
    [
        (
            "sum by (stage) (rate(fraudx_scorer_stage_seconds_sum[5m])) / "
            "scalar(sum(rate(fraudx_scored_total[5m])))",
            "{{stage}}",
        )
    ],
    "s",
    "Model inference latency: graph features and known fraud (Neo4j), the three "
    "models + rules + risk engine, publishing.",
    12,
    12,
    stack=True,
)
y += 8
series(
    "Store write time p95 per message",
    [
        (per_message_p95("feature-processor"), "Redis (feature-processor)"),
        (per_message_p95("graph-writer"), "Neo4j (graph-writer)"),
        (per_message_p95("history-writer"), "Cassandra (history-writer)"),
        (per_message_p95("profile-writer"), "MongoDB (profile-writer)"),
    ],
    "s",
    "Each writer's time per message is dominated by its store's batched write.",
    0,
    12,
)
series(
    "Handler time p95 per message",
    [
        (
            "histogram_quantile(0.95, sum by (le, worker) "
            "(rate(fraudx_consumer_processing_seconds_bucket[5m])))",
            "{{worker}}",
        )
    ],
    "s",
    "Processing cost per message for every worker (batch time / batch size).",
    12,
    12,
)
y += 8

row("Risk")
series(
    "Medium and high-risk transactions per minute",
    [('sum by (level) (increase(fraudx_scored_total{level!="LOW"}[1m]))', "{{level}}")],
    "short",
    "LOW is omitted: it is over 99 % of traffic.",
    0,
    8,
    level_overrides(),
    stack=True,
)
series(
    "Rule hits per minute",
    [("sum by (rule) (increase(fraudx_rule_hits_total[1m]))", "{{rule}}")],
    "short",
    "Deterministic rules, including the analyst-feedback rule payee_confirmed_fraud.",
    8,
    8,
    no_value="No rule hits",
)
series(
    "Investigations opened per minute",
    [("sum by (generator) (increase(fraudx_investigations_total[1m]))", "{{generator}}")],
    "short",
    "One per alert; generator = template or llm.",
    16,
    8,
)
y += 8

row("API and Kafka")
series(
    "API requests by route",
    [("sum by (route) (rate(fraudx_http_requests_total[1m]))", "{{route}}")],
    "reqps",
    "",
    0,
    8,
)
series(
    "API latency p95 by route",
    [
        (
            "histogram_quantile(0.95, sum by (le, route) "
            "(rate(fraudx_http_request_duration_seconds_bucket[5m])))",
            "{{route}}",
        )
    ],
    "s",
    "",
    8,
    8,
)
series(
    "Kafka delivery failures",
    [('sum by (topic) (increase(fraudx_kafka_produced_total{outcome="failed"}[5m]))', "{{topic}}")],
    "short",
    "Messages the producer could not deliver.",
    16,
    8,
    no_value="No delivery failures",
)
y += 8

for i, panel in enumerate(panels, 1):
    panel["id"] = i
dashboard = {
    "uid": "fraudx-pipeline",
    "title": "FraudX pipeline",
    "tags": ["fraudx"],
    "timezone": "utc",
    "refresh": "10s",
    "time": {"from": "now-1h", "to": "now"},
    "schemaVersion": 39,
    "version": 2,
    "editable": True,
    "panels": panels,
}
OUT.write_text(json.dumps(dashboard, indent=2) + "\n", encoding="utf-8")
print(f"wrote {OUT} ({len(panels)} panels)")
