from fraudx.bench_stream import render


def test_streaming_report_lists_both_runs():
    run = {"events": 100, "scored": 100, "send_rate": 200, "throughput": 190,
           "latency_ms": {"p50": 80.0, "p95": 150.0, "p99": 200.0, "max": 250.0}}  # fmt: skip
    meta = {"run_at": "2026-10-17T00:00:00+00:00", "dataset": "data/generated", "cpus": 12,
            "platform": "Linux"}  # fmt: skip
    text = render({"steady": {**run, "target_rate": 200.0}, "burst": {**run, "target_rate": None}},
                  meta)  # fmt: skip
    assert "| steady (200 tx/s) | 100 | 100 | 200 | 190 | 80.0 | 150.0 | 200.0 | 250.0 |" in text
    assert "| burst (unpaced) |" in text
