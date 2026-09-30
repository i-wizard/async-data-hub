from tests.fixtures import HEAVY_ANALYSIS_URL


def test_heavy_analysis_supports_multiple_execution_strategies(client):
    """
    Verifies the CPU endpoint can run the same workload through different
    strategies so the learning comparison stays available through the API.
    """

    for strategy in ["blocking", "threadpool", "processpool"]:
        response = client.post(
            HEAVY_ANALYSIS_URL,
            json={"number": 20, "strategy": strategy},
        )

        assert response.status_code == 200
        body = response.json()
        assert body["strategy"] == strategy
        assert body["number"] == 20
        assert body["result"] == 6765
