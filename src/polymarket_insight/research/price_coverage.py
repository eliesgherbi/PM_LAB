"""Price coverage diagnostics for copyability research."""

from __future__ import annotations

from typing import Any

import pandas as pd

from polymarket_insight.analysis.copy_delay import simulate_copy_delay
from polymarket_insight.data.storage.normalized import NormalizedStore
from polymarket_insight.research.trader import build_trader_universe


def parse_delays(value: str | list[str] | None = None) -> list[pd.Timedelta]:
    """Parse CLI delay values such as '5m,30m'."""

    raw = value or "5m,30m"
    parts = raw if isinstance(raw, list) else raw.split(",")
    return [pd.Timedelta(part.strip()) for part in parts if part.strip()]


def select_guru_universe_trades(
    *,
    store: NormalizedStore | None = None,
    category: str = "OVERALL",
    period: str = "MONTH",
    top_n: int = 50,
    max_trades_per_wallet: int | None = None,
) -> pd.DataFrame:
    """Read trades for the same leaderboard universe used by guru ranking."""

    normalized = store or NormalizedStore()
    universe = build_trader_universe(
        category=category,
        period=period,
        order_by="PNL",
        top_n=top_n,
        store=normalized,
    )
    trades = normalized.read_table("trades")
    if universe.empty or trades.empty:
        return pd.DataFrame()
    wallets = set(universe["wallet"].dropna().astype(str).str.lower())
    selected = trades[trades["trader_wallet"].astype(str).str.lower().isin(wallets)].copy()
    if selected.empty or max_trades_per_wallet is None:
        return selected
    selected["timestamp"] = pd.to_datetime(selected["timestamp"], utc=True, errors="coerce")
    return (
        selected.sort_values("timestamp", ascending=False)
        .groupby(selected["trader_wallet"].astype(str).str.lower(), group_keys=False)
        .head(max_trades_per_wallet)
        .sort_values("timestamp")
        .reset_index(drop=True)
    )


def build_price_coverage_report(
    *,
    store: NormalizedStore | None = None,
    category: str = "OVERALL",
    period: str = "MONTH",
    top_n: int = 50,
    delays: str | list[str] | None = None,
    tolerance_minutes: int = 10,
) -> dict[str, Any]:
    """Summarize price coverage for a guru leaderboard universe."""

    normalized = store or NormalizedStore()
    trades = select_guru_universe_trades(
        store=normalized,
        category=category,
        period=period,
        top_n=top_n,
    )
    price_points = normalized.read_table("price_points")
    parsed_delays = parse_delays(delays)
    summary = _price_coverage_summary(
        trades,
        price_points,
        delays=parsed_delays,
        tolerance_minutes=tolerance_minutes,
    )
    per_wallet = [
        _wallet_summary(wallet, group, price_points, parsed_delays, tolerance_minutes)
        for wallet, group in trades.groupby("trader_wallet", dropna=False)
    ] if not trades.empty else []
    zero_coverage_wallets = [
        row["wallet"] for row in per_wallet if row.get("trades_with_5m_price", 0) == 0
    ]
    return {
        "category": category,
        "period": period,
        "top_n": top_n,
        **summary,
        "zero_coverage_wallets": zero_coverage_wallets,
        "per_wallet": per_wallet,
    }


def _price_coverage_summary(
    trades: pd.DataFrame,
    price_points: pd.DataFrame,
    *,
    delays: list[pd.Timedelta],
    tolerance_minutes: int,
) -> dict[str, Any]:
    price_token_ids = (
        set(price_points.get("token_id", pd.Series(dtype=str)).dropna().astype(str))
        if not price_points.empty
        else set()
    )
    trade_token_ids = (
        set(trades.get("token_id", pd.Series(dtype=str)).dropna().astype(str))
        if not trades.empty
        else set()
    )
    copy_results = simulate_copy_delay(
        trades,
        price_points,
        delays,
        tolerance=pd.Timedelta(minutes=tolerance_minutes),
    )
    delay_counts = _delay_counts(copy_results)
    trade_count = int(len(trades))
    with_5m = delay_counts.get("5m", 0)
    with_30m = delay_counts.get("30m", 0)
    missing_ratio = (
        1.0
        if copy_results.empty
        else float(copy_results["missing_price"].mean())
    )
    token_coverage = (
        len(trade_token_ids & price_token_ids) / len(trade_token_ids)
        if trade_token_ids
        else 0.0
    )
    return {
        "total_trades": trade_count,
        "unique_token_ids": len(trade_token_ids),
        "token_ids_with_price_points": len(trade_token_ids & price_token_ids),
        "token_price_coverage": token_coverage,
        "trades_with_t0_price": _near_trade_price_count(trades, price_points, minutes=5),
        "trades_with_5m_price": with_5m,
        "trades_with_30m_price": with_30m,
        "trade_price_coverage_5m": with_5m / trade_count if trade_count else 0.0,
        "trade_price_coverage_30m": with_30m / trade_count if trade_count else 0.0,
        "missing_price_ratio": missing_ratio,
        "top_missing_token_ids": _top_missing_token_ids(copy_results),
    }


def _wallet_summary(
    wallet: str,
    trades: pd.DataFrame,
    price_points: pd.DataFrame,
    delays: list[pd.Timedelta],
    tolerance_minutes: int,
) -> dict[str, Any]:
    summary = _price_coverage_summary(
        trades,
        price_points,
        delays=delays,
        tolerance_minutes=tolerance_minutes,
    )
    return {"wallet": str(wallet), **summary}


def _delay_counts(copy_results: pd.DataFrame) -> dict[str, int]:
    counts: dict[str, int] = {}
    if copy_results.empty:
        return counts
    for delay, group in copy_results.groupby("delay"):
        minutes = int(pd.Timedelta(delay).total_seconds() // 60)
        key = f"{minutes}m" if minutes < 60 else f"{minutes // 60}h"
        counts[key] = int((~group["missing_price"]).sum())
    return counts


def _near_trade_price_count(
    trades: pd.DataFrame,
    price_points: pd.DataFrame,
    *,
    minutes: int,
) -> int:
    if trades.empty or price_points.empty:
        return 0
    prices = price_points.copy()
    prices["timestamp"] = pd.to_datetime(prices["timestamp"], utc=True, errors="coerce")
    tolerance = pd.Timedelta(minutes=minutes)
    count = 0
    for _, trade in trades.iterrows():
        token_id = str(trade.get("token_id") or "")
        trade_ts = pd.Timestamp(trade["timestamp"])
        trade_ts = (
            trade_ts.tz_localize("UTC")
            if trade_ts.tzinfo is None
            else trade_ts.tz_convert("UTC")
        )
        token_prices = prices[prices["token_id"].astype(str) == token_id]
        if token_prices.empty:
            continue
        near = token_prices[
            (token_prices["timestamp"] >= trade_ts - tolerance)
            & (token_prices["timestamp"] <= trade_ts + tolerance)
        ]
        if not near.empty:
            count += 1
    return count


def _top_missing_token_ids(copy_results: pd.DataFrame, limit: int = 10) -> list[dict[str, Any]]:
    if copy_results.empty:
        return []
    missing = copy_results[copy_results["missing_price"]]
    if missing.empty:
        return []
    counts = missing["token_id"].astype(str).value_counts().head(limit)
    return [
        {"token_id": token_id, "missing_count": int(count)}
        for token_id, count in counts.items()
    ]
