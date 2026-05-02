"""Reusable research universes."""

from __future__ import annotations

from decimal import Decimal

import pandas as pd


def liquid_markets(markets: pd.DataFrame, *, min_volume: Decimal = Decimal("5000")) -> pd.DataFrame:
    """Select markets above a simple liquidity threshold."""

    if markets.empty or "volume" not in markets:
        return pd.DataFrame()
    mask = markets["volume"].apply(
        lambda value: value is not None and Decimal(str(value)) >= min_volume
    )
    return markets[mask]
