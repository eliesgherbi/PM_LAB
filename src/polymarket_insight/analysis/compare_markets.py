"""Market comparison analysis."""

from __future__ import annotations

import pandas as pd

from polymarket_insight.data.datasets import markets as market_data


def compare_markets(market_ids: list[str], *, dimension: str) -> pd.DataFrame:
    """Compare selected markets by a metadata dimension."""

    rows = []
    for market_id in market_ids:
        df = market_data.metadata(market_id)
        if not df.empty:
            row = df.iloc[-1].to_dict()
            rows.append(
                {"condition_id": market_id, "dimension": dimension, "value": row.get(dimension)}
            )
    return pd.DataFrame(rows)
