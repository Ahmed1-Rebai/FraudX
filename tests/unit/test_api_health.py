from fastapi.testclient import TestClient

from fraudx.api.app import create_app
from fraudx.storage.health import CheckResult


class FakeProbe:
    def __init__(self, results):
        self.results = results
        self.closed = False

    async def check_all(self):
        return self.results

    def close(self):
        self.closed = True


def _client(results):
    probe = FakeProbe(results)
    return TestClient(create_app(probe=probe)), probe


def test_liveness_needs_no_dependencies():
    client, _ = _client([])
    with client:
        resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"


def test_ready_when_all_dependencies_ok():
    client, probe = _client([CheckResult("redis", True, 1.0, "ping ok")])
    with client:
        resp = client.get("/health/ready")
    assert resp.status_code == 200
    assert resp.json()["dependencies"]["redis"]["ok"] is True
    assert probe.closed  # lifespan shutdown releases clients


def test_degraded_returns_503():
    client, _ = _client(
        [CheckResult("redis", True, 1.0), CheckResult("neo4j", False, 3000.0, "timed out")]
    )
    with client:
        resp = client.get("/health/ready")
    assert resp.status_code == 503
    body = resp.json()
    assert body["status"] == "degraded"
    assert body["dependencies"]["neo4j"]["detail"] == "timed out"


def test_metrics_exposed_with_route_templates():
    client, _ = _client([])
    with client:
        client.get("/health")
        resp = client.get("/metrics/")
    assert resp.status_code == 200
    assert 'route="/health"' in resp.text
