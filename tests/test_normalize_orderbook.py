from __future__ import annotations

import json

from polymarket_insight.data.normalize import normalize_envelope


def test_normalize_orderbook(fixture_dir):
    envelope = json.loads((fixture_dir / "orderbook_raw.json").read_text())
    df = normalize_envelope(envelope, "orderbook_raw.json")["orderbook_snapshots"]

    row = df.iloc[0]
    assert str(row["best_bid"]) == "0.39"
    assert str(row["best_ask"]) == "0.41"
    assert str(row["spread"]) == "0.02"
