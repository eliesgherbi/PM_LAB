from __future__ import annotations

import pandas as pd

from polymarket_insight.analysis.copy_delay import simulate_copy_delay


def test_copy_delay_selects_delayed_price():
    trades = pd.DataFrame(
        {
            "tx_hash": ["0xtx"],
            "trader_wallet": ["0x1"],
            "condition_id": ["c1"],
            "token_id": ["t1"],
            "side": ["BUY"],
            "timestamp": [pd.Timestamp("2026-01-01T00:00:00Z")],
            "price": [0.4],
        }
    )
    prices = pd.DataFrame(
        {
            "token_id": ["t1", "t1"],
            "timestamp": [
                pd.Timestamp("2026-01-01T00:00:30Z"),
                pd.Timestamp("2026-01-01T00:01:00Z"),
            ],
            "price": [0.41, 0.42],
            "price_kind": ["last_trade", "last_trade"],
            "source": ["clob_history", "clob_history"],
        }
    )

    result = simulate_copy_delay(trades, prices, [pd.Timedelta("1m")])

    assert result.iloc[0]["copy_price"] == 0.42
    assert result.iloc[0]["matched_price_ts"] == pd.Timestamp("2026-01-01T00:01:00Z")
    assert result.iloc[0]["match_lag_seconds"] == 0
    assert bool(result.iloc[0]["worse_than_guru"]) is True


def test_copy_delay_matches_nearest_later_price_within_tolerance():
    trades = pd.DataFrame(
        {
            "tx_hash": ["0xtx"],
            "trader_wallet": ["0x1"],
            "condition_id": ["c1"],
            "token_id": ["t1"],
            "side": ["BUY"],
            "timestamp": [pd.Timestamp("2026-01-01T10:03:17Z")],
            "price": [0.4],
        }
    )
    prices = pd.DataFrame(
        {
            "token_id": ["t1"],
            "timestamp": [pd.Timestamp("2026-01-01T10:10:00Z")],
            "price": [0.44],
            "price_kind": ["clob_history"],
            "source": ["clob_history"],
        }
    )

    result = simulate_copy_delay(trades, prices, [pd.Timedelta("5m")])

    assert result.iloc[0]["copy_price"] == 0.44
    assert result.iloc[0]["missing_reason"] is None
    assert result.iloc[0]["match_lag_seconds"] == 103


def test_copy_delay_rejects_price_outside_tolerance():
    trades = pd.DataFrame(
        {
            "tx_hash": ["0xtx"],
            "trader_wallet": ["0x1"],
            "condition_id": ["c1"],
            "token_id": ["t1"],
            "side": ["BUY"],
            "timestamp": [pd.Timestamp("2026-01-01T10:00:00Z")],
            "price": [0.4],
        }
    )
    prices = pd.DataFrame(
        {
            "token_id": ["t1"],
            "timestamp": [pd.Timestamp("2026-01-01T10:30:01Z")],
            "price": [0.44],
            "price_kind": ["clob_history"],
            "source": ["clob_history"],
        }
    )

    result = simulate_copy_delay(
        trades,
        prices,
        [pd.Timedelta("5m")],
        tolerance=pd.Timedelta("10m"),
    )

    assert bool(result.iloc[0]["missing_price"]) is True
    assert result.iloc[0]["missing_reason"] == "price_after_target_outside_tolerance"
