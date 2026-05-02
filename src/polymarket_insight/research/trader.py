"""Trader research workflows for guru copyability."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from polymarket_insight.analysis.copy_delay import simulate_copy_delay
from polymarket_insight.config import resolve_project_path
from polymarket_insight.data.normalize import normalize_raw
from polymarket_insight.data.storage.normalized import NormalizedStore
from polymarket_insight.ingestion.runner import Runtime
from polymarket_insight.metrics.trader import (
    avg_trade_size,
    category_exposure,
    market_count,
    median_trade_size,
    resolved_market_count,
    trade_count,
)
from polymarket_insight.research.artifacts import (
    new_run_id,
    write_research_card,
    write_research_run,
)
from polymarket_insight.research.confidence import confidence_label
from polymarket_insight.research.coverage import build_coverage_report


def build_trader_universe(
    *,
    category: str = "OVERALL",
    period: str = "MONTH",
    order_by: str = "PNL",
    top_n: int = 50,
    store: NormalizedStore | None = None,
) -> pd.DataFrame:
    """Build a leaderboard-derived trader universe from local snapshots."""

    leaderboard = (store or NormalizedStore()).read_table("leaderboard_snapshots")
    columns = [
        "wallet",
        "official_rank",
        "official_pnl",
        "official_volume",
        "category",
        "period",
        "snapshot_ts",
    ]
    if leaderboard.empty:
        return pd.DataFrame(columns=columns)
    subset = leaderboard[
        (leaderboard["category"].str.upper() == category.upper())
        & (leaderboard["period"].str.upper() == period.upper())
        & (leaderboard["order_by"].str.upper() == order_by.upper())
    ].copy()
    if subset.empty:
        return pd.DataFrame(columns=columns)
    subset = (
        subset.assign(_wallet_key=subset["wallet"].astype(str).str.lower())
        .sort_values(["snapshot_ts", "rank"], ascending=[False, True])
        .drop_duplicates("_wallet_key", keep="first")
        .head(top_n)
        .drop(columns=["_wallet_key"])
    )
    return subset.rename(
        columns={"rank": "official_rank", "pnl": "official_pnl", "volume": "official_volume"}
    )[columns].reset_index(drop=True)


def hydrate_trader_research_dataset(
    wallets: list[str],
    *,
    runtime: Runtime | None = None,
    trade_limit: int = 500,
    save_raw: bool = True,
) -> dict[str, Any]:
    """Fetch public trades for wallets and normalize the resulting raw evidence."""

    rt = runtime or Runtime()
    failures: dict[str, str] = {}
    for wallet in wallets:
        try:
            rt.data_api.get_trades(user=wallet, limit=trade_limit, save_raw=save_raw)
        except Exception as exc:  # noqa: BLE001 - record gap and continue wallet batch
            failures[wallet] = str(exc)
            rt.manifest.record_gap("trades", entity_id=wallet, reason=str(exc))
    counts = normalize_raw("data_api", rt.raw_store, rt.normalized_store, rt.manifest)
    return {"wallets": len(wallets), "failures": failures, "normalized": counts}


def build_trader_research_mart(
    *,
    store: NormalizedStore | None = None,
    marts_dir: str | Path = "data/marts",
) -> pd.DataFrame:
    """Build the trader research mart used by notebooks and research cards."""

    normalized = store or NormalizedStore()
    trades = normalized.read_table("trades")
    markets = normalized.read_table("markets")
    crosswalk = normalized.read_table("market_crosswalk")
    market_lookup = _market_lookup(markets, crosswalk)
    price_points = normalized.read_table("price_points")
    orderbooks = normalized.read_table("orderbook_snapshots")
    universe = build_trader_universe(store=normalized, top_n=10_000)
    rows = []
    for _, trader in universe.iterrows():
        wallet = trader["wallet"]
        trader_trades = (
            trades[trades["trader_wallet"].str.lower() == str(wallet).lower()]
            if not trades.empty
            else pd.DataFrame()
        )
        copy_delay = _copy_delay_summary(trader_trades, price_points)
        coverage = _trader_coverage_status(trader_trades, market_lookup, price_points, orderbooks)
        rows.append(
            {
                "wallet": wallet,
                "official_pnl": trader["official_pnl"],
                "official_volume": trader["official_volume"],
                "official_rank": trader["official_rank"],
                "trade_count": trade_count(trader_trades),
                "market_count": market_count(trader_trades),
                "resolved_market_count": resolved_market_count(trader_trades, market_lookup),
                "category_exposure": _top_category(trader_trades, market_lookup),
                "median_trade_size": median_trade_size(trader_trades),
                "avg_trade_size": avg_trade_size(trader_trades),
                "profit_concentration_proxy": _profit_concentration_proxy(trader_trades),
                "entry_edge_vs_resolution_mean": None,
                "entry_edge_vs_close_mean": None,
                "copy_delay_5m_median_slippage": copy_delay.get("5m"),
                "copy_delay_30m_median_slippage": copy_delay.get("30m"),
                "trade_price_coverage_5m": copy_delay.get("coverage_5m", 0.0),
                "trade_price_coverage_30m": copy_delay.get("coverage_30m", 0.0),
                "missing_price_ratio": copy_delay.get("missing_price_ratio", 1.0),
                "coverage_status": coverage["coverage_status"],
                "sample_size": len(trader_trades),
                "missing_data_ratio": coverage["missing_data_ratio"],
                "confidence_label": confidence_label(
                    sample_size=len(trader_trades),
                    missing_data_ratio=coverage["missing_data_ratio"],
                    coverage=coverage["coverage_status"],
                ),
                "copyability_label": classify_copyability(
                    trade_count=trade_count(trader_trades),
                    missing_price_ratio=copy_delay.get("missing_price_ratio", 1.0),
                    trade_price_coverage_5m=copy_delay.get("coverage_5m", 0.0),
                    copy_delay_5m_median_slippage=copy_delay.get("5m"),
                    profit_concentration_proxy=_profit_concentration_proxy(trader_trades),
                    coverage_status=coverage["coverage_status"],
                ),
            }
        )
    mart = pd.DataFrame(rows)
    out = resolve_project_path(marts_dir)
    out.mkdir(parents=True, exist_ok=True)
    if not mart.empty:
        mart.to_parquet(out / "trader_research.parquet", index=False)
    return mart


def classify_copyability(
    *,
    trade_count: int,
    missing_price_ratio: float,
    trade_price_coverage_5m: float,
    copy_delay_5m_median_slippage: float | None,
    profit_concentration_proxy: float | None,
    coverage_status: str,
) -> str:
    """Classify copyability without hiding component metrics."""

    if trade_count < 20 or coverage_status == "unusable":
        return "inconclusive"
    if trade_price_coverage_5m < 0.20 or missing_price_ratio > 0.80:
        return "inconclusive"
    if profit_concentration_proxy is not None and profit_concentration_proxy > 0.60:
        return "not_copyable_candidate"
    if copy_delay_5m_median_slippage is not None and copy_delay_5m_median_slippage <= 0:
        return "potentially_copyable"
    return "not_copyable_candidate"


def run_guru_copyability_research(
    wallet: str,
    *,
    store: NormalizedStore | None = None,
    run_id: str | None = None,
) -> Path:
    """Write a reproducible guru-copyability research run and card."""

    normalized = store or NormalizedStore()
    mart = build_trader_research_mart(store=normalized)
    row = mart[mart["wallet"].str.lower() == wallet.lower()] if not mart.empty else pd.DataFrame()
    metrics = (
        row
        if not row.empty
        else pd.DataFrame([{"wallet": wallet, "copyability_label": "inconclusive"}])
    )
    coverage = build_coverage_report(normalized)
    label = metrics.iloc[0].get("copyability_label", "inconclusive")
    card = _guru_card(wallet, str(label), metrics.iloc[0].to_dict(), coverage)
    safe_wallet = wallet[:10].lower().replace("0x", "")
    write_research_card(f"research/0001_guru_{safe_wallet}_copyability.md", card)
    return write_research_run(
        run_id=run_id or new_run_id("guru_copyability"),
        input_config={"workflow": "guru-copyability", "wallet": wallet},
        dataset_coverage=coverage,
        metrics=metrics,
        research_card=card,
    )


def _copy_delay_summary(trades: pd.DataFrame, price_points: pd.DataFrame) -> dict[str, float]:
    if trades.empty or price_points.empty:
        return {"missing_price_ratio": 1.0}
    result = simulate_copy_delay(
        trades,
        price_points,
        [pd.Timedelta("5m"), pd.Timedelta("30m")],
    )
    if result.empty:
        return {"missing_price_ratio": 1.0}
    summary: dict[str, float] = {
        "missing_price_ratio": float(result["missing_price"].mean()),
    }
    for delay, group in result.groupby("delay"):
        non_missing = group[~group["missing_price"]]
        minutes = int(pd.Timedelta(delay).total_seconds() // 60)
        key = f"{minutes}m" if minutes < 60 else f"{minutes // 60}h"
        summary[f"coverage_{key}"] = float((~group["missing_price"]).mean())
        if not non_missing.empty:
            summary[key] = float(non_missing["price_delta"].median())
    return summary


def _trader_coverage_status(
    trades: pd.DataFrame,
    market_lookup: pd.DataFrame,
    price_points: pd.DataFrame,
    orderbooks: pd.DataFrame,
) -> dict[str, Any]:
    if trades.empty:
        return {"coverage_status": "unusable", "missing_data_ratio": 1.0}
    known_market = (
        trades["condition_id"].isin(set(market_lookup["condition_id"].dropna()))
        if not market_lookup.empty
        else pd.Series(False, index=trades.index)
    )
    known_price = (
        trades["token_id"].isin(set(price_points["token_id"].dropna()))
        if not price_points.empty
        else pd.Series(False, index=trades.index)
    )
    known_book = (
        trades["token_id"].isin(set(orderbooks["token_id"].dropna()))
        if not orderbooks.empty
        else pd.Series(False, index=trades.index)
    )
    coverage_ratio = (
        (known_market.astype(int) + known_price.astype(int) + known_book.astype(int)) / 3
    ).mean()
    missing = float(1 - coverage_ratio)
    if coverage_ratio >= 0.85:
        status = "good"
    elif coverage_ratio >= 0.60:
        status = "partial"
    elif coverage_ratio >= 0.30:
        status = "weak"
    else:
        status = "unusable"
    return {"coverage_status": status, "missing_data_ratio": missing}


def _top_category(trades: pd.DataFrame, markets: pd.DataFrame) -> str | None:
    exposure = category_exposure(trades, markets)
    if exposure.empty:
        return None
    return str(exposure.iloc[0]["category"])


def _market_lookup(markets: pd.DataFrame, crosswalk: pd.DataFrame) -> pd.DataFrame:
    """Combine full Gamma markets and trade/Gamma crosswalk rows by condition id."""

    parts = []
    if not markets.empty:
        parts.append(
            markets[
                [
                    "condition_id",
                    "category",
                    "resolved",
                    "winning_outcome",
                    "market_slug",
                    "event_slug",
                ]
            ].copy()
        )
    if not crosswalk.empty:
        rows = crosswalk[
            ["condition_id", "resolved", "winning_outcome", "market_slug", "event_slug"]
        ].copy()
        rows["category"] = None
        parts.append(rows)
    if not parts:
        return pd.DataFrame(columns=["condition_id", "category", "resolved", "winning_outcome"])
    lookup = pd.concat(parts, ignore_index=True)
    lookup["_resolved_sort"] = lookup["resolved"].fillna(False).astype(bool).astype(int)
    return (
        lookup.sort_values(["condition_id", "_resolved_sort"])
        .drop_duplicates("condition_id", keep="last")
        .drop(columns=["_resolved_sort"])
        .reset_index(drop=True)
    )


def _profit_concentration_proxy(trades: pd.DataFrame) -> float | None:
    if trades.empty or "notional_usd" not in trades:
        return None
    by_market = trades.groupby("condition_id")["notional_usd"].sum()
    total = by_market.sum()
    return None if total == 0 else float(by_market.max() / total)


def _guru_card(wallet: str, label: str, metrics: dict[str, Any], coverage: dict[str, Any]) -> str:
    return f"""# Finding: Guru Copyability for {wallet}

## Question

Can this leaderboard trader be copied after realistic delay?

## Dataset

- Source: local normalized leaderboard, trades, markets, prices, and orderbooks
- Known gaps: {coverage.get("coverage_status")}
- Sample size: {metrics.get("sample_size", 0)}

## Method

Compare official leaderboard evidence, trade diversity, coverage, concentration,
and copy-delay slippage.

## Result

Conclusion: **{label}**

Key metrics:

- official_pnl: {metrics.get("official_pnl")}
- trade_count: {metrics.get("trade_count")}
- market_count: {metrics.get("market_count")}
- resolved_market_count: {metrics.get("resolved_market_count")}
- missing_price_ratio: {metrics.get("missing_price_ratio")}
- confidence_label: {metrics.get("confidence_label")}

## Trading implication

Research classification only. PMI does not execute trades.

## Failure modes

Incomplete resolution metadata, missing price history, sparse orderbook snapshots,
and leaderboard opacity.

## Confidence

{metrics.get("confidence_label", "insufficient_data")}

## Next step

Collect more price/orderbook evidence or reject the trader if copy-delay metrics remain weak.
"""
