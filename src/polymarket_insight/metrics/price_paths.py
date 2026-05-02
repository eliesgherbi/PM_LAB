"""Price path and volume profile metrics."""

from __future__ import annotations

import pandas as pd


def volume_profile(trades: pd.DataFrame, *, freq: str = "1h") -> pd.DataFrame:
    """Aggregate normalized trade notional over time."""

    if trades.empty:
        return pd.DataFrame(columns=["timestamp", "notional_usd"])
    df = trades.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    return df.set_index("timestamp").resample(freq)["notional_usd"].sum().reset_index()


def pre_game_price_movement(
    price_points: pd.DataFrame,
    *,
    anchor_ts: pd.Timestamp,
    windows: list[str],
) -> pd.DataFrame:
    """Measure token price changes before an anchor timestamp."""

    if price_points.empty:
        return pd.DataFrame(columns=["window", "price", "delta_to_anchor"])
    df = price_points.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    anchor_value = pd.Timestamp(anchor_ts)
    anchor_ts = (
        anchor_value.tz_convert("UTC")
        if anchor_value.tzinfo
        else pd.Timestamp(anchor_value, tz="UTC")
    )
    anchor_price = pd.to_numeric(
        df[df["timestamp"] <= anchor_ts].sort_values("timestamp")["price"]
    ).iloc[-1]
    rows = []
    for window in windows:
        target = anchor_ts - pd.Timedelta(window)
        before = df[df["timestamp"] <= target].sort_values("timestamp")
        if before.empty:
            rows.append({"window": window, "price": None, "delta_to_anchor": None})
        else:
            price = float(before.iloc[-1]["price"])
            rows.append(
                {"window": window, "price": price, "delta_to_anchor": float(anchor_price) - price}
            )
    return pd.DataFrame(rows)
