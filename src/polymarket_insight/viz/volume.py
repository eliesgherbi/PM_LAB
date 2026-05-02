"""Volume visualization."""

from __future__ import annotations

import pandas as pd
import plotly.express as px


def volume_profile(df: pd.DataFrame):
    """Return a volume profile chart."""

    return px.bar(df, x="timestamp", y="notional_usd")
