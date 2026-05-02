"""Research dataset coverage reporting."""

from __future__ import annotations

from typing import Any

import pandas as pd

from polymarket_insight.analysis.copy_delay import simulate_copy_delay
from polymarket_insight.data.storage.normalized import NormalizedStore
from polymarket_insight.research.confidence import coverage_status


def build_coverage_report(
    store: NormalizedStore | None = None,
    *,
    explain: bool = False,
) -> dict[str, Any]:
    """Summarize local data coverage before trusting research metrics."""

    normalized = store or NormalizedStore()
    markets = normalized.read_table("markets")
    trades = normalized.read_table("trades")
    price_points = normalized.read_table("price_points")
    orderbooks = normalized.read_table("orderbook_snapshots")
    leaderboard = normalized.read_table("leaderboard_snapshots")
    crosswalk = normalized.read_table("market_crosswalk")

    report: dict[str, Any] = {
        "number_of_markets": int(len(markets)),
        "number_of_resolved_markets": 0,
        "number_of_unresolved_markets": 0,
        "number_of_traders": 0,
        "number_of_trades": int(len(trades)),
        "number_of_trades_with_known_market_metadata": 0,
        "number_of_trades_with_missing_market_metadata": int(len(trades)),
        "number_of_trades_with_known_resolution": 0,
        "number_of_trades_with_missing_resolution": int(len(trades)),
        "number_of_trades_with_price_history_available": 0,
        "number_of_trades_with_orderbook_snapshot_available": 0,
        "price_points": int(len(price_points)),
        "traded_token_ids": 0,
        "token_ids_with_price_points": 0,
        "token_price_coverage": 0.0,
        "eligible_trades": int(len(trades)),
        "trades_with_5m_copy_price": 0,
        "trade_price_coverage_5m": 0.0,
        "trades_with_30m_copy_price": 0,
        "trade_price_coverage_30m": 0.0,
        "date_range_covered": None,
        "categories_covered": [],
        "sports_leagues_covered": [],
        "leaderboard_rows": int(len(leaderboard)),
        "market_crosswalk_rows": int(len(crosswalk)),
    }
    if not markets.empty:
        resolved = markets.get("resolved", pd.Series(False, index=markets.index)).fillna(False)
        report["number_of_resolved_markets"] = int(resolved.sum())
        report["number_of_unresolved_markets"] = int((~resolved.astype(bool)).sum())
        report["categories_covered"] = sorted(
            str(value) for value in markets.get("category", pd.Series(dtype=str)).dropna().unique()
        )
        report["sports_leagues_covered"] = _sports_leagues(markets)
    if not trades.empty:
        report["number_of_traders"] = int(trades["trader_wallet"].nunique())
        report["date_range_covered"] = _date_range(trades["timestamp"])
        market_ids = set(markets.get("condition_id", pd.Series(dtype=str)).dropna())
        if not crosswalk.empty:
            market_ids |= set(crosswalk.get("condition_id", pd.Series(dtype=str)).dropna())
        resolved_ids = set()
        if not markets.empty:
            resolved_ids |= set(
                markets.loc[markets["resolved"].fillna(False), "condition_id"].dropna()
            )
        if not crosswalk.empty and "resolved" in crosswalk:
            resolved_ids |= set(
                crosswalk.loc[crosswalk["resolved"].fillna(False), "condition_id"].dropna()
            )
        known_market = trades["condition_id"].isin(market_ids)
        known_resolution = trades["condition_id"].isin(resolved_ids)
        traded_token_ids = set(trades["token_id"].dropna().astype(str))
        report["traded_token_ids"] = len(traded_token_ids)
        report["number_of_trades_with_known_market_metadata"] = int(known_market.sum())
        report["number_of_trades_with_missing_market_metadata"] = int((~known_market).sum())
        report["number_of_trades_with_known_resolution"] = int(known_resolution.sum())
        report["number_of_trades_with_missing_resolution"] = int((~known_resolution).sum())
        if not price_points.empty:
            price_token_ids = set(price_points["token_id"].dropna().astype(str))
            price_copy = simulate_copy_delay(
                trades,
                price_points,
                [pd.Timedelta("5m"), pd.Timedelta("30m")],
            )
            delay_counts = _delay_counts(price_copy)
            report["number_of_trades_with_price_history_available"] = int(
                trades["token_id"].isin(set(price_points["token_id"].dropna())).sum()
            )
            report["traded_token_ids"] = len(traded_token_ids)
            report["token_ids_with_price_points"] = len(traded_token_ids & price_token_ids)
            report["token_price_coverage"] = (
                report["token_ids_with_price_points"] / report["traded_token_ids"]
                if report["traded_token_ids"]
                else 0.0
            )
            report["trades_with_5m_copy_price"] = delay_counts.get("5m", 0)
            report["trade_price_coverage_5m"] = (
                report["trades_with_5m_copy_price"] / report["eligible_trades"]
                if report["eligible_trades"]
                else 0.0
            )
            report["trades_with_30m_copy_price"] = delay_counts.get("30m", 0)
            report["trade_price_coverage_30m"] = (
                report["trades_with_30m_copy_price"] / report["eligible_trades"]
                if report["eligible_trades"]
                else 0.0
            )
        if not orderbooks.empty:
            report["number_of_trades_with_orderbook_snapshot_available"] = int(
                trades["token_id"].isin(set(orderbooks["token_id"].dropna())).sum()
            )
    report["coverage_status"] = coverage_status(_coverage_score(report))
    if explain:
        report["explain"] = _coverage_explain(report)
    return report


def _coverage_score(report: dict[str, Any]) -> float:
    trades = report["number_of_trades"]
    if trades == 0:
        return 0.0
    metadata = report["number_of_trades_with_known_market_metadata"] / trades
    resolution = report["number_of_trades_with_known_resolution"] / trades
    prices = max(
        float(report.get("token_price_coverage", 0.0)),
        float(report.get("trade_price_coverage_5m", 0.0)),
        float(report.get("trade_price_coverage_30m", 0.0)),
    )
    return (metadata * 0.35) + (resolution * 0.35) + (prices * 0.30)


def _coverage_explain(report: dict[str, Any]) -> dict[str, Any]:
    trades = report["number_of_trades"]
    if trades == 0:
        return {
            "reason": "No normalized trades are available.",
            "next_steps": ["Run pmi research seed or fetch trader trades."],
        }
    metadata_ratio = report["number_of_trades_with_known_market_metadata"] / trades
    resolution_ratio = report["number_of_trades_with_known_resolution"] / trades
    price_ratio = report["number_of_trades_with_price_history_available"] / trades
    next_steps = []
    if metadata_ratio < 0.60:
        next_steps.append("Run pmi research hydrate-trades --include-market-metadata.")
    if resolution_ratio < 0.30:
        next_steps.append("Hydrate closed/resolved Gamma markets where available.")
    if price_ratio < 0.50:
        next_steps.append("Run pmi research hydrate-price-history --from-trades.")
    return {
        "metadata_ratio": metadata_ratio,
        "resolution_ratio": resolution_ratio,
        "price_history_ratio": price_ratio,
        "token_price_coverage": report.get("token_price_coverage", 0.0),
        "trade_price_coverage_5m": report.get("trade_price_coverage_5m", 0.0),
        "trade_price_coverage_30m": report.get("trade_price_coverage_30m", 0.0),
        "coverage_formula": "0.35*metadata + 0.35*resolution + 0.30*price_history",
        "next_steps": next_steps,
    }


def _delay_counts(copy_results: pd.DataFrame) -> dict[str, int]:
    counts: dict[str, int] = {}
    if copy_results.empty:
        return counts
    for delay, group in copy_results.groupby("delay"):
        minutes = int(pd.Timedelta(delay).total_seconds() // 60)
        key = f"{minutes}m" if minutes < 60 else f"{minutes // 60}h"
        counts[key] = int((~group["missing_price"]).sum())
    return counts


def _date_range(values: pd.Series) -> dict[str, str] | None:
    timestamps = pd.to_datetime(values, utc=True, errors="coerce").dropna()
    if timestamps.empty:
        return None
    return {"start": timestamps.min().isoformat(), "end": timestamps.max().isoformat()}


def _sports_leagues(markets: pd.DataFrame) -> list[str]:
    tags = []
    for value in markets.get("tags", pd.Series(dtype=object)).dropna():
        if isinstance(value, list):
            tags.extend(str(item) for item in value)
    sports_hint = markets[
        markets.get("category", pd.Series(dtype=str)).fillna("").str.upper() == "SPORTS"
    ]
    if sports_hint.empty:
        return []
    return sorted(set(tags))
