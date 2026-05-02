from __future__ import annotations

import pandas as pd

from polymarket_insight.data.storage.normalized import NormalizedStore
from polymarket_insight.research.coverage import build_coverage_report


def test_coverage_report_labels_missing_data(tmp_path):
    store = NormalizedStore(tmp_path)
    store.write_partition(
        "trades",
        pd.DataFrame(
            {
                "timestamp": ["2026-01-01"],
                "trader_wallet": ["0x1"],
                "condition_id": ["c1"],
                "token_id": ["t1"],
            }
        ),
        ts_column="timestamp",
    )

    report = build_coverage_report(store)

    assert report["number_of_trades"] == 1
    assert report["number_of_trades_with_missing_market_metadata"] == 1
    assert report["coverage_status"] == "unusable"
