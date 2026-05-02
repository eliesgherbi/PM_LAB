from __future__ import annotations

from decimal import Decimal

import pandas as pd

from polymarket_insight.data.storage.normalized import NormalizedStore
from polymarket_insight.research.sports import (
    build_sports_universe,
    extract_prices_around_anchor,
    sample_size_guard,
    sports_line_movement_metrics,
)


def test_sports_universe_anchor_prices_and_metrics(tmp_path):
    store = NormalizedStore(tmp_path)
    store.write_partition(
        "markets",
        pd.DataFrame(
            {
                "market_id": ["m1"],
                "condition_id": ["c1"],
                "event_slug": ["nba-game"],
                "market_slug": ["nba-game-yes"],
                "question": ["Will Team A win?"],
                "start_time": ["2026-01-02T00:00:00Z"],
                "end_time": ["2026-01-02T03:00:00Z"],
                "resolved": [True],
                "winning_outcome": ["Yes"],
                "volume": [Decimal("10000")],
                "liquidity": [Decimal("2000")],
                "token_ids": [["t1"]],
                "tags": [["Sports", "NBA"]],
                "category": ["SPORTS"],
            }
        ),
        ts_column="start_time",
    )
    store.write_partition(
        "price_points",
        pd.DataFrame(
            {
                "token_id": ["t1", "t1"],
                "timestamp": ["2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z"],
                "price": [0.4, 0.55],
            }
        ),
        ts_column="timestamp",
    )

    universe = build_sports_universe(league="NBA", store=store)
    anchors = extract_prices_around_anchor(universe, offsets=["24h", "close"], store=store)
    metrics = sports_line_movement_metrics(anchors, universe)
    guard = sample_size_guard(universe, anchors)

    assert len(universe) == 1
    assert anchors.iloc[0]["price_t_minus_24h"] == 0.4
    assert metrics.iloc[0]["movement_24h_to_close"] > 0
    assert guard["number_of_markets"] == 1
