from __future__ import annotations

from polymarket_insight.data.clients.base import PublicHttpClient
from polymarket_insight.data.clients.clob import ClobClient


def test_clob_get_book_public(httpx_mock):
    httpx_mock.add_response(json={"asset_id": "123", "bids": [], "asks": []})
    client = ClobClient(PublicHttpClient(source="clob", base_url="https://clob.test"))

    book = client.get_book("123")

    assert book["asset_id"] == "123"
    assert httpx_mock.get_request().url.path == "/book"
