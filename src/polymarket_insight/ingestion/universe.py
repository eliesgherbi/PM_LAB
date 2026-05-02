"""Liquid market universe selection."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pandas as pd


def select_liquid_markets(
    markets: pd.DataFrame,
    *,
    min_24h_volume_usd: Decimal,
    resolving_within_days: int,
) -> pd.DataFrame:
    """Select markets by volume or near resolution."""

    if markets.empty:
        return markets
    df = markets.copy()
    volume_mask = df.get("volume", pd.Series(dtype=object)).apply(
        lambda value: value is not None and Decimal(str(value)) >= min_24h_volume_usd
    )
    end = pd.to_datetime(df.get("end_time"), utc=True, errors="coerce")
    deadline = datetime.now(UTC) + timedelta(days=resolving_within_days)
    resolving_mask = end.notna() & (end <= deadline)
    return df[volume_mask | resolving_mask].reset_index(drop=True)
