from __future__ import annotations

from polymarket_insight.data.clients.base import PublicHttpClient
from polymarket_insight.data.clients.data_api import DataApiClient


def test_data_api_leaderboard_uses_v1_endpoint(httpx_mock):
    httpx_mock.add_response(
        json=[{"rank": "1", "proxyWallet": "0x1111111111111111111111111111111111111111"}]
    )
    client = DataApiClient(PublicHttpClient(source="data_api", base_url="https://data.test"))

    rows = client.get_leaderboard(category="OVERALL", time_period="MONTH", order_by="PNL")

    request = httpx_mock.get_request()
    assert request.url.path == "/v1/leaderboard"
    assert request.url.params["timePeriod"] == "MONTH"
    assert rows[0]["rank"] == "1"
