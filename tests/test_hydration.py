from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

import pandas as pd

from polymarket_insight.analysis.copy_delay import simulate_copy_delay
from polymarket_insight.data.clients.base import ApiError, ApiRequest, ApiResponse
from polymarket_insight.data.clients.clob import ClobClient
from polymarket_insight.data.storage.manifest import Manifest
from polymarket_insight.data.storage.normalized import NormalizedStore
from polymarket_insight.data.storage.raw import RawStore
from polymarket_insight.research.coverage import build_coverage_report
from polymarket_insight.research.hydration import (
    build_trade_price_history_windows,
    hydrate_guru_price_history,
    hydrate_market_metadata_from_trades,
    hydrate_price_history_from_trades,
)
from polymarket_insight.research.price_coverage import build_price_coverage_report


@dataclass
class FakeGamma:
    def list_markets(self, **kwargs):
        return []


@dataclass
class FakeClob:
    raw_store: RawStore
    split_reject_seconds: int | None = None
    calls: list[dict] | None = None

    def get_price_history(self, token_id: str, **kwargs):
        if self.calls is None:
            self.calls = []
        self.calls.append({"token_id": token_id, **kwargs})
        duration = int(kwargs["end_ts"]) - int(kwargs["start_ts"])
        if self.split_reject_seconds is not None and duration > self.split_reject_seconds:
            raise ApiError(
                "clob",
                "/prices-history",
                400,
                "invalid filters: 'startTs' and 'endTs' interval is too long",
            )

        payload = {
            "history": [
                {"t": kwargs["start_ts"], "p": 0.42},
                {"t": kwargs["start_ts"], "p": 0.42},
            ]
        }
        request = ApiRequest(
            source="clob",
            endpoint="/prices-history",
            params_hash=f"price-{token_id}",
            params={
                "market": token_id,
                "startTs": kwargs["start_ts"],
                "endTs": kwargs["end_ts"],
                **({"interval": kwargs["interval"]} if kwargs.get("interval") is not None else {}),
                "fidelity": kwargs.get("fidelity"),
            },
            requested_at=datetime.now(UTC),
        )
        self.raw_store.write(ApiResponse(request, 200, payload, datetime.now(UTC)))
        return payload


@dataclass
class FakeRuntime:
    normalized_store: NormalizedStore
    raw_store: RawStore
    manifest: Manifest
    gamma: FakeGamma
    clob: FakeClob


def _runtime(tmp_path) -> FakeRuntime:
    raw_store = RawStore(tmp_path / "raw")
    return FakeRuntime(
        normalized_store=NormalizedStore(tmp_path / "normalized"),
        raw_store=raw_store,
        manifest=Manifest(tmp_path / "manifest.duckdb"),
        gamma=FakeGamma(),
        clob=FakeClob(raw_store),
    )


def test_market_hydration_writes_crosswalk_stub(tmp_path):
    runtime = _runtime(tmp_path)
    trades = pd.DataFrame(
        {
            "timestamp": ["2026-04-30T10:00:00Z"],
            "condition_id": ["c1"],
            "token_id": ["t1"],
            "outcome": ["Yes"],
            "market_slug": ["market-one"],
            "event_slug": ["event-one"],
            "title": ["Will this hydrate?"],
        }
    )

    result = hydrate_market_metadata_from_trades(runtime, trades=trades, max_markets=0)
    crosswalk = runtime.normalized_store.read_table("market_crosswalk")

    assert result["stubbed"] == 1
    assert crosswalk.iloc[0]["condition_id"] == "c1"
    assert crosswalk.iloc[0]["metadata_source"] == "trade_stub"


def test_coverage_uses_market_crosswalk(tmp_path):
    store = NormalizedStore(tmp_path)
    store.write_partition(
        "trades",
        pd.DataFrame(
            {
                "timestamp": ["2026-04-30T10:00:00Z"],
                "trader_wallet": ["0x1"],
                "condition_id": ["c1"],
                "token_id": ["t1"],
            }
        ),
        ts_column="timestamp",
    )
    store.write_partition(
        "market_crosswalk",
        pd.DataFrame(
            {
                "condition_id": ["c1"],
                "token_id": ["t1"],
                "resolved": [False],
                "ingested_at": ["2026-04-30T10:00:00Z"],
            }
        ),
        ts_column="ingested_at",
    )

    report = build_coverage_report(store, explain=True)

    assert report["number_of_trades_with_known_market_metadata"] == 1
    assert report["explain"]["metadata_ratio"] == 1.0


def test_price_history_hydration_normalizes_points(tmp_path):
    runtime = _runtime(tmp_path)
    trades = pd.DataFrame(
        {
            "timestamp": ["2026-04-30T10:00:00Z"],
            "condition_id": ["c1"],
            "token_id": ["t1"],
        }
    )

    result = hydrate_price_history_from_trades(runtime, trades=trades)
    prices = runtime.normalized_store.read_table("price_points")

    assert result["fetched"] == 1
    assert prices.iloc[0]["token_id"] == "t1"


def test_price_history_windows_are_trade_relative_and_capped():
    trades = pd.DataFrame(
        {
            "timestamp": [
                "2026-04-30T10:00:00Z",
                "2026-04-30T10:10:00Z",
                "2026-05-02T10:00:00Z",
            ],
            "token_id": ["t1", "t1", "t1"],
        }
    )

    windows = build_trade_price_history_windows(
        trades,
        lookback_minutes=10,
        lookahead_minutes=180,
        max_window_hours=24,
    )

    assert len(windows) == 2
    assert windows[0].end_ts - windows[0].start_ts <= 24 * 3600
    assert windows[0].start_ts == int(pd.Timestamp("2026-04-30T09:50:00Z").timestamp())


def test_clob_price_history_omits_interval_with_explicit_window():
    class FakeHttp:
        def __init__(self):
            self.params = None

        def request(self, method, endpoint, *, params=None, save_raw=False, json_body=None):
            self.params = params
            request = ApiRequest("clob", endpoint, "h", params or {}, datetime.now(UTC))
            return ApiResponse(request, 200, {"history": []}, datetime.now(UTC))

    http = FakeHttp()
    client = ClobClient(http)

    client.get_price_history("t1", start_ts=1, end_ts=2, fidelity=5)

    assert http.params == {"market": "t1", "startTs": 1, "endTs": 2, "fidelity": 5}


def test_price_history_adaptive_split_on_long_interval_error(tmp_path):
    runtime = _runtime(tmp_path)
    runtime.clob.split_reject_seconds = 3600
    trades = pd.DataFrame(
        {
            "timestamp": ["2026-04-30T10:00:00Z"],
            "condition_id": ["c1"],
            "token_id": ["t1"],
        }
    )

    result = hydrate_price_history_from_trades(
        runtime,
        trades=trades,
        lookback_minutes=10,
        lookahead_minutes=180,
        adaptive_split=True,
    )

    assert result["fetched"] > 1
    assert len(runtime.clob.calls) > 1
    assert all(call.get("interval") is None for call in runtime.clob.calls)


def test_successful_price_history_chunks_dedupe_points(tmp_path):
    runtime = _runtime(tmp_path)
    trades = pd.DataFrame(
        {
            "timestamp": ["2026-04-30T10:00:00Z"],
            "condition_id": ["c1"],
            "token_id": ["t1"],
        }
    )

    hydrate_price_history_from_trades(runtime, trades=trades)
    prices = runtime.normalized_store.read_table("price_points")

    assert len(prices) == 1


def test_copy_delay_populates_when_price_points_exist():
    trades = pd.DataFrame(
        {
            "tx_hash": ["0xtx"],
            "trader_wallet": ["0x1"],
            "condition_id": ["c1"],
            "token_id": ["t1"],
            "side": ["BUY"],
            "timestamp": [pd.Timestamp("2026-04-30T10:00:00Z")],
            "price": [0.40],
        }
    )
    price_points = pd.DataFrame(
        {
            "token_id": ["t1"],
            "timestamp": [pd.Timestamp("2026-04-30T10:05:00Z")],
            "price": [0.44],
            "price_kind": ["clob_history"],
            "source": ["clob_history"],
        }
    )

    result = simulate_copy_delay(trades, price_points, [pd.Timedelta("5m")])

    assert result.iloc[0]["copy_price"] == 0.44
    assert bool(result.iloc[0]["missing_price"]) is False


def test_price_coverage_detects_missing_and_covered_token_ids(tmp_path):
    store = NormalizedStore(tmp_path)
    store.write_partition(
        "leaderboard_snapshots",
        pd.DataFrame(
            {
                "snapshot_ts": ["2026-04-30T00:00:00Z", "2026-04-30T00:00:00Z"],
                "wallet": ["0x1", "0x2"],
                "rank": [1, 2],
                "category": ["OVERALL", "OVERALL"],
                "period": ["MONTH", "MONTH"],
                "order_by": ["PNL", "PNL"],
                "pnl": [10, 5],
                "volume": [100, 50],
            }
        ),
        ts_column="snapshot_ts",
    )
    store.write_partition(
        "trades",
        pd.DataFrame(
            {
                "timestamp": ["2026-04-30T10:00:00Z", "2026-04-30T10:00:00Z"],
                "trader_wallet": ["0x1", "0x2"],
                "condition_id": ["c1", "c2"],
                "token_id": ["t1", "t2"],
                "tx_hash": ["tx1", "tx2"],
                "side": ["BUY", "BUY"],
                "price": [0.40, 0.50],
            }
        ),
        ts_column="timestamp",
    )
    store.write_partition(
        "price_points",
        pd.DataFrame(
            {
                "token_id": ["t1"],
                "timestamp": ["2026-04-30T10:05:00Z"],
                "price": [0.44],
                "price_kind": ["clob_history"],
                "source": ["clob_history"],
            }
        ),
        ts_column="timestamp",
    )

    report = build_price_coverage_report(store=store, top_n=2)

    assert report["total_trades"] == 2
    assert report["unique_token_ids"] == 2
    assert report["token_ids_with_price_points"] == 1
    assert report["trades_with_5m_price"] == 1
    assert report["trade_price_coverage_5m"] == 0.5
    assert report["top_missing_token_ids"][0]["token_id"] == "t2"


def test_hydrate_guru_price_history_selects_leaderboard_universe(tmp_path):
    runtime = _runtime(tmp_path)
    runtime.normalized_store.write_partition(
        "leaderboard_snapshots",
        pd.DataFrame(
            {
                "snapshot_ts": ["2026-04-30T00:00:00Z", "2026-04-30T00:00:00Z"],
                "wallet": ["0x1", "0x2"],
                "rank": [1, 2],
                "category": ["OVERALL", "OVERALL"],
                "period": ["MONTH", "MONTH"],
                "order_by": ["PNL", "PNL"],
                "pnl": [10, 5],
                "volume": [100, 50],
            }
        ),
        ts_column="snapshot_ts",
    )
    runtime.normalized_store.write_partition(
        "trades",
        pd.DataFrame(
            {
                "timestamp": ["2026-04-30T10:00:00Z", "2026-04-30T10:00:00Z"],
                "trader_wallet": ["0x1", "0x2"],
                "condition_id": ["c1", "c2"],
                "token_id": ["top-token", "other-token"],
            }
        ),
        ts_column="timestamp",
    )

    result = hydrate_guru_price_history(
        runtime,
        top_n=1,
        max_trades_per_wallet=500,
        rebuild_marts=False,
    )

    assert result["selected_wallets"] == 1
    assert runtime.clob.calls[0]["token_id"] == "top-token"
