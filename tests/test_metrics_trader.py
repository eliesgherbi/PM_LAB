from __future__ import annotations

from decimal import Decimal

import pandas as pd

from polymarket_insight.metrics.trader import market_count, trade_count, volume


def test_trader_metrics():
    trades = pd.DataFrame(
        {
            "tx_hash": ["a", "b"],
            "condition_id": ["m1", "m2"],
            "notional_usd": [Decimal("1.5"), Decimal("2.5")],
        }
    )

    assert trade_count(trades) == 2
    assert market_count(trades) == 2
    assert volume(trades) == Decimal("4.0")
