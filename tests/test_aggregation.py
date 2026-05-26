from tests.fixtures import AGGREGATE_STREAM_URL, AGGREGATE_URL

QUERY = "tesla"


def test_aggregate_returns_all_sources_and_persists_cache(client):
    """
    Verifies the aggregate endpoint returns every expected source and that the
    second identical request uses cache instead of repeating the full fan-out.
    """

    first_response = client.get(AGGREGATE_URL, params={"q": QUERY, "use_cache": "true"})
    second_response = client.get(AGGREGATE_URL, params={"q": QUERY, "use_cache": "true"})

    assert first_response.status_code == 200
    first_body = first_response.json()
    assert first_body["query"] == QUERY
    assert first_body["status"] == "COMPLETED"
    assert first_body["cached"] is False
    assert len(first_body["results"]) == 3
    assert {result["source"] for result in first_body["results"]} == {"weather", "news", "finance"}

    assert second_response.status_code == 200
    second_body = second_response.json()
    assert second_body["query"] == QUERY
    assert second_body["cached"] is True
    assert len(second_body["results"]) == 3


def test_aggregate_stream_emits_started_results_and_completed_events(client):
    """
    Verifies the SSE endpoint streams progress frames so partial async results
    become visible before the entire aggregation workflow is summarized.
    """

    with client.stream("GET", AGGREGATE_STREAM_URL, params={"q": QUERY}) as response:
        payload = response.text

    assert response.status_code == 200
    assert "event: started" in payload
    assert '"source": "weather"' in payload
    assert '"source": "news"' in payload
    assert '"source": "finance"' in payload
    assert "event: completed" in payload
