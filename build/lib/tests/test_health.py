from tests.fixtures import HEALTH_URL


def test_get_health_returns_dependency_status(client):
    """
    Verifies startup-created resources are reachable because a healthy API needs
    working shared infrastructure, not just a running HTTP server.
    """

    response = client.get(HEALTH_URL)

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ok"
    assert body["database"] == "up"
    assert body["cache"] == "up"
