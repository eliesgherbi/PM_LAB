from __future__ import annotations

import json

from polymarket_insight.data.normalize import normalize_envelope


def test_normalize_market(fixture_dir):
    envelope = json.loads((fixture_dir / "market_raw.json").read_text())
    df = normalize_envelope(envelope, "market_raw.json")["markets"]

    assert df.iloc[0]["condition_id"].startswith("0x")
    assert df.iloc[0]["outcome_token_map"]["Yes"] == "123"
