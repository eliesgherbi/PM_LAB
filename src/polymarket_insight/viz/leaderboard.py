"""Leaderboard visualization."""

from __future__ import annotations

import pandas as pd
import plotly.express as px


def leaderboard(df: pd.DataFrame):
    """Return a leaderboard bar chart."""

    y_axis = "official_leaderboard_pnl" if "official_leaderboard_pnl" in df else "pnl"
    return px.bar(df, x="wallet", y=y_axis)
