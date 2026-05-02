"""Calibration visualization."""

from __future__ import annotations

import pandas as pd
import plotly.express as px


def calibration_plot(df: pd.DataFrame):
    """Return a calibration scatter plot."""

    return px.scatter(df, x="close_price", y="realized")
