"""Sports and event line movement analysis."""

from __future__ import annotations

from datetime import datetime

import pandas as pd

from polymarket_insight.data.datasets import markets as market_data
from polymarket_insight.metrics.price_paths import pre_game_price_movement


def line_movement(
    token_id: str,
    *,
    anchor_ts: datetime,
    windows: list[str] | None = None,
) -> pd.DataFrame:
    """Measure price movement before an anchor time."""

    prices = market_data.price_history(token_id)
    return pre_game_price_movement(
        prices,
        anchor_ts=pd.Timestamp(anchor_ts),
        windows=windows or ["24h", "6h", "1h"],
    )
