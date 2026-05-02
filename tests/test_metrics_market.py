from __future__ import annotations

import pandas as pd

from polymarket_insight.metrics.market import path_volatility, trader_concentration


def test_market_metrics():
    prices = pd.DataFrame({"timestamp": [1, 2, 3], "price": [0.4, 0.5, 0.45]})
    trades = pd.DataFrame({"trader_wallet": ["a", "b"], "notional_usd": [3, 1]})

    assert path_volatility(prices) is not None
    assert trader_concentration(trades).iloc[0]["trader_wallet"] == "a"
