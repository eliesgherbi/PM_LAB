from __future__ import annotations

import pandas as pd

from polymarket_insight.data.storage.normalized import NormalizedStore


def test_normalized_store_roundtrip(tmp_path):
    store = NormalizedStore(tmp_path)
    rows = pd.DataFrame({"timestamp": ["2026-01-01"], "tx_hash": ["x"]})
    store.write_partition("trades", rows, ts_column="timestamp")

    assert store.read_table("trades").iloc[0]["tx_hash"] == "x"
