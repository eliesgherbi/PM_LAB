from __future__ import annotations

from polymarket_insight.data.clients.base import PublicHttpClient
from polymarket_insight.data.clients.gamma import GammaClient


def test_gamma_list_markets(httpx_mock):
    httpx_mock.add_response(json=[{"id": "1"}])
    client = GammaClient(PublicHttpClient(source="gamma", base_url="https://gamma.test"))

    rows = client.list_markets(limit=1)

    assert rows == [{"id": "1"}]
