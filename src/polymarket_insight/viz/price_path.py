"""Price path visualization."""

from __future__ import annotations

import pandas as pd
import plotly.express as px


def price_path(df: pd.DataFrame):
    """Return a token price path chart."""

    return px.line(df, x="timestamp", y="price", color="token_id" if "token_id" in df else None)
