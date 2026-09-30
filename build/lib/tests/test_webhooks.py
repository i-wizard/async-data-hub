from tests.fixtures import TRIGGER_EVENT_URL

SUCCESS_URL = "http://testserver/api/v1/mock-webhooks/success"
FAILURE_URL = "http://testserver/api/v1/mock-webhooks/failure"


def test_trigger_event_returns_success_and_failure_attempts(client):
    """
    Verifies webhook fan-out records target-level outcomes so partial delivery is
    surfaced instead of being hidden behind one overall status code.
    """

    payload = {
        "event_name": "aggregate.completed",
        "payload": {"query": "tesla"},
        "targets": [SUCCESS_URL, FAILURE_URL],
    }

    response = client.post(TRIGGER_EVENT_URL, json=payload)

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "PARTIAL"
    assert len(body["attempts"]) >= 3
    statuses = {attempt["status"] for attempt in body["attempts"]}
    assert "success" in statuses
    assert "failed" in statuses
