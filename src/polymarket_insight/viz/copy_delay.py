"""Copy-delay visualization."""

from __future__ import annotations

import pandas as pd
import plotly.express as px


def copy_delay_curve(df: pd.DataFrame):
    """Return copy-delay slippage by delay."""

    return px.box(df, x="delay", y="price_delta")
