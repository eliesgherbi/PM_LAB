from __future__ import annotations

from decimal import Decimal

import pandas as pd

from polymarket_insight.data.storage.normalized import NormalizedStore
from polymarket_insight.research.trader import (
    build_trader_research_mart,
    build_trader_universe,
    classify_copyability,
)


def test_trader_universe_and_mart(tmp_path):
    store = NormalizedStore(tmp_path)
    store.write_partition(
        "leaderboard_snapshots",
        pd.DataFrame(
            {
                "snapshot_ts": ["2026-01-01", "2026-01-02"],
                "wallet": ["0x1", "0x1"],
                "rank": [2, 1],
                "category": ["OVERALL", "OVERALL"],
                "period": ["MONTH", "MONTH"],
                "order_by": ["PNL", "PNL"],
                "pnl": [Decimal("5"), Decimal("10")],
                "volume": [Decimal("50"), Decimal("100")],
            }
        ),
        ts_column="snapshot_ts",
    )
    store.write_partition(
        "trades",
        pd.DataFrame(
            {
                "timestamp": ["2026-01-01"],
                "trader_wallet": ["0x1"],
                "condition_id": ["c1"],
                "token_id": ["t1"],
                "tx_hash": ["tx"],
                "side": ["BUY"],
                "price": [Decimal("0.50")],
                "size": [Decimal("2")],
                "notional_usd": [Decimal("1")],
            }
        ),
        ts_column="timestamp",
    )
    store.write_partition(
        "price_points",
        pd.DataFrame(
            {
                "token_id": ["t1"],
                "timestamp": ["2026-01-01T00:05:00Z"],
                "price": [Decimal("0.55")],
                "price_kind": ["clob_history"],
                "source": ["clob_history"],
            }
        ),
        ts_column="timestamp",
    )

    universe = build_trader_universe(store=store)
    mart = build_trader_research_mart(store=store, marts_dir=tmp_path / "marts")

    assert len(universe) == 1
    assert universe.iloc[0]["official_rank"] == 1
    assert len(mart) == 1
    assert mart.iloc[0]["wallet"] == "0x1"
    assert mart.iloc[0]["copy_delay_5m_median_slippage"] is not None
    assert mart.iloc[0]["trade_price_coverage_5m"] == 1.0


def test_copyability_classification():
    assert (
        classify_copyability(
            trade_count=100,
            missing_price_ratio=0.1,
            trade_price_coverage_5m=0.9,
            copy_delay_5m_median_slippage=-0.01,
            profit_concentration_proxy=0.2,
            coverage_status="good",
        )
        == "potentially_copyable"
    )
