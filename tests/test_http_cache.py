from __future__ import annotations

from polymarket_insight.data.storage.cache import HttpCache


def test_http_cache_roundtrip(tmp_path):
    cache = HttpCache(tmp_path, default_ttl_s=60)
    cache.set("gamma", "/markets", "abc", {"rows": 1})

    assert cache.get("gamma", "/markets", "abc") == {"rows": 1}
