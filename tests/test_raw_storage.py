from __future__ import annotations

from datetime import UTC, datetime

from polymarket_insight.data.clients.base import ApiRequest, ApiResponse
from polymarket_insight.data.storage.raw import RawStore, read_raw_file


def test_raw_storage_append_only(tmp_path):
    store = RawStore(tmp_path)
    request = ApiRequest("data_api", "/trades", "abc", {}, datetime.now(UTC))
    response = ApiResponse(request, 200, {"ok": True}, datetime.now(UTC))

    first = store.write(response)
    second = store.write(response)

    assert first != second
    assert read_raw_file(first)["payload"] == {"ok": True}
