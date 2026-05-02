from __future__ import annotations

import json

from polymarket_insight.data.normalize import normalize_envelope


def test_normalize_trade(fixture_dir):
    envelope = json.loads((fixture_dir / "trade_raw.json").read_text())
    df = normalize_envelope(envelope, "trade_raw.json")["trades"]

    assert df.iloc[0]["tx_hash"] == "0xtx"
    assert str(df.iloc[0]["notional_usd"]) == "4.0"
