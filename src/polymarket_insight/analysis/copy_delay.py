"""Copy-delay research simulation."""

from __future__ import annotations

from typing import Literal

import pandas as pd

from polymarket_insight.data import datasets


def simulate_copy_delay(
    trader_trades: pd.DataFrame,
    price_points: pd.DataFrame,
    delays: list[pd.Timedelta],
    price_kind: Literal["last_trade", "midpoint", "ask", "bid"] = "last_trade",
    tolerance: pd.Timedelta | None = None,
) -> pd.DataFrame:
    """Simulate delayed copy prices from token price points.

    Results are descriptive research evidence, not executable PnL.
    """

    rows = []
    tolerance = tolerance or pd.Timedelta("10m")
    if trader_trades.empty:
        return pd.DataFrame(columns=_OUTPUT_COLUMNS)
    prices = price_points.copy()
    if not prices.empty:
        prices["timestamp"] = pd.to_datetime(prices["timestamp"], utc=True)
    for _, trade in trader_trades.iterrows():
        trade_ts = pd.Timestamp(trade["timestamp"])
        guru_ts = (
            trade_ts.tz_localize("UTC")
            if trade_ts.tzinfo is None
            else trade_ts.tz_convert("UTC")
        )
        token_id = trade.get("token_id")
        if token_id is None or pd.isna(token_id) or "token_id" not in prices:
            token_prices = pd.DataFrame()
            token_missing_reason = "token_id_mismatch"
        else:
            token_prices = (
                prices[prices["token_id"].astype(str) == str(token_id)]
                if not prices.empty
                else pd.DataFrame()
            )
            token_missing_reason = "no_price_points_for_token"
        if not token_prices.empty and "price_kind" in token_prices:
            requested = token_prices[token_prices["price_kind"].isin([price_kind, "clob_history"])]
            token_prices = requested if not requested.empty else token_prices
        for delay in delays:
            copy_target_ts = guru_ts + delay
            price_row, missing_reason = _nearest_after(
                token_prices,
                copy_target_ts,
                tolerance=tolerance,
                empty_reason=token_missing_reason,
            )
            missing = price_row is None
            copy_price = None if missing else price_row["price"]
            evidence = "missing" if missing else price_row.get("source", "clob_history")
            guru_price = trade["price"]
            price_delta = None if missing else float(copy_price) - float(guru_price)
            matched_price_ts = None if missing else pd.Timestamp(price_row["timestamp"])
            match_lag_seconds = (
                None
                if matched_price_ts is None
                else float((matched_price_ts - copy_target_ts).total_seconds())
            )
            worse = None
            if price_delta is not None:
                worse = price_delta > 0 if str(trade["side"]).upper() == "BUY" else price_delta < 0
            rows.append(
                {
                    "tx_hash": trade.get("tx_hash"),
                    "trader_wallet": trade.get("trader_wallet"),
                    "condition_id": trade.get("condition_id"),
                    "token_id": trade.get("token_id"),
                    "side": trade.get("side"),
                    "guru_trade_ts": guru_ts,
                    "guru_price": guru_price,
                    "delay": delay,
                    "copy_ts": copy_target_ts,
                    "copy_target_ts": copy_target_ts,
                    "matched_price_ts": matched_price_ts,
                    "match_lag_seconds": match_lag_seconds,
                    "copy_price": copy_price,
                    "price_delta": price_delta,
                    "worse_than_guru": bool(worse) if worse is not None else False,
                    "missing_price": missing,
                    "missing_reason": None if not missing else missing_reason,
                    "price_evidence": evidence,
                }
            )
    return pd.DataFrame(rows, columns=_OUTPUT_COLUMNS)


def simulate_copy_delay_v1(
    trader_trades: pd.DataFrame,
    price_points: pd.DataFrame,
    orderbooks: pd.DataFrame,
    delays: list[pd.Timedelta],
) -> pd.DataFrame:
    """Prefer bid/ask snapshots for delayed copy prices, falling back to price history."""

    ob_prices = []
    if not orderbooks.empty:
        for _, row in orderbooks.iterrows():
            ob_prices.append(
                {
                    "token_id": row["token_id"],
                    "timestamp": row["timestamp"],
                    "price": row.get("best_ask"),
                    "price_kind": "ask",
                    "source": "orderbook_snapshot",
                }
            )
            ob_prices.append(
                {
                    "token_id": row["token_id"],
                    "timestamp": row["timestamp"],
                    "price": row.get("best_bid"),
                    "price_kind": "bid",
                    "source": "orderbook_snapshot",
                }
            )
    combined = (
        pd.concat([price_points, pd.DataFrame(ob_prices)], ignore_index=True)
        if ob_prices
        else price_points
    )
    outputs = []
    for side, kind in (("BUY", "ask"), ("SELL", "bid")):
        subset = (
            trader_trades[trader_trades["side"].str.upper() == side]
            if "side" in trader_trades
            else trader_trades
        )
        outputs.append(simulate_copy_delay(subset, combined, delays, price_kind=kind))
    if outputs:
        return pd.concat(outputs, ignore_index=True)
    return pd.DataFrame(columns=_OUTPUT_COLUMNS)


def trader_copy_delay_report(wallet: str, *, delays: list[str] | None = None) -> pd.DataFrame:
    """Build a copy-delay report from local normalized datasets."""

    parsed = [pd.Timedelta(value) for value in (delays or ["1m", "5m", "30m", "2h"])]
    trades = datasets.traders.trades(wallet)
    if trades.empty:
        return pd.DataFrame(columns=_OUTPUT_COLUMNS)
    prices = pd.concat(
        [
            datasets.markets.price_history(str(token_id))
            for token_id in trades["token_id"].dropna().unique()
        ],
        ignore_index=True,
    )
    return simulate_copy_delay(trades, prices, parsed)


def _nearest_after(
    prices: pd.DataFrame,
    ts: pd.Timestamp,
    *,
    tolerance: pd.Timedelta,
    empty_reason: str,
) -> tuple[pd.Series | None, str | None]:
    if prices.empty:
        return None, empty_reason
    candidates = prices[prices["timestamp"] >= ts].sort_values("timestamp")
    if candidates.empty:
        return None, "no_price_after_target"
    row = candidates.iloc[0]
    if pd.Timestamp(row["timestamp"]) > ts + tolerance:
        return None, "price_after_target_outside_tolerance"
    return row, None


_OUTPUT_COLUMNS = [
    "tx_hash",
    "trader_wallet",
    "condition_id",
    "token_id",
    "side",
    "guru_trade_ts",
    "guru_price",
    "delay",
    "copy_ts",
    "copy_target_ts",
    "matched_price_ts",
    "match_lag_seconds",
    "copy_price",
    "price_delta",
    "worse_than_guru",
    "missing_price",
    "missing_reason",
    "price_evidence",
]
