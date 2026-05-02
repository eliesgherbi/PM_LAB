"""Hydrate trader research data from public read-only sources."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC
from typing import Any

import pandas as pd

from polymarket_insight.data.clients.base import ApiError
from polymarket_insight.data.normalize import normalize_raw
from polymarket_insight.data.storage.marts import build_basic_marts
from polymarket_insight.ingestion.runner import Runtime
from polymarket_insight.research.price_coverage import parse_delays, select_guru_universe_trades


@dataclass(frozen=True)
class PriceHistoryWindow:
    """A bounded token price-history request window."""

    token_id: str
    start_ts: int
    end_ts: int


def hydrate_traded_markets(
    runtime: Runtime | None = None,
    *,
    include_market_metadata: bool = True,
    include_resolution: bool = True,
    include_price_history: bool = False,
    save_raw: bool = True,
    max_markets: int | None = None,
    max_tokens: int | None = None,
) -> dict[str, Any]:
    """Hydrate market metadata, resolution, and optional price history from trades."""

    rt = runtime or Runtime()
    trades = rt.normalized_store.read_table("trades")
    if trades.empty:
        return {"trades": 0, "markets_attempted": 0, "tokens_attempted": 0}
    result: dict[str, Any] = {"trades": len(trades)}
    if include_market_metadata or include_resolution:
        result["market_metadata"] = hydrate_market_metadata_from_trades(
            rt,
            trades=trades,
            save_raw=save_raw,
            max_markets=max_markets,
        )
    if include_price_history:
        result["price_history"] = hydrate_price_history_from_trades(
            rt,
            trades=trades,
            save_raw=save_raw,
            max_tokens=max_tokens,
        )
    result["normalized"] = normalize_raw("all", rt.raw_store, rt.normalized_store, rt.manifest)
    return result


def hydrate_market_metadata_from_trades(
    runtime: Runtime,
    *,
    trades: pd.DataFrame | None = None,
    save_raw: bool = True,
    max_markets: int | None = None,
) -> dict[str, Any]:
    """Fetch missing Gamma market metadata and write crosswalk stubs from trades."""

    rt = runtime
    trades = trades if trades is not None else rt.normalized_store.read_table("trades")
    if trades.empty:
        return {"attempted": 0, "fetched": 0, "stubbed": 0, "gaps": 0}
    markets = rt.normalized_store.read_table("markets")
    known_condition_ids = set(markets.get("condition_id", pd.Series(dtype=str)).dropna())
    needed = (
        trades[~trades["condition_id"].isin(known_condition_ids)]
        .drop_duplicates("condition_id")
        .sort_values("timestamp")
    )
    if max_markets is not None:
        needed = needed.head(max_markets)
    fetched = 0
    gaps = 0
    for _, trade in needed.iterrows():
        slug = trade.get("market_slug")
        condition_id = str(trade.get("condition_id") or "")
        try:
            rows = []
            if slug:
                rows = rt.gamma.list_markets(limit=10, slug=slug, save_raw=save_raw)
            if not rows and trade.get("event_slug"):
                rows = rt.gamma.list_markets(
                    limit=10,
                    event_slug=trade.get("event_slug"),
                    save_raw=save_raw,
                )
            if rows:
                fetched += 1
            else:
                gaps += 1
                rt.manifest.record_gap(
                    "markets",
                    entity_id=condition_id,
                    reason="gamma_market_metadata_not_found_using_trade_slug",
                )
        except Exception as exc:  # noqa: BLE001 - record gap and continue hydration
            gaps += 1
            rt.manifest.record_gap("markets", entity_id=condition_id, reason=str(exc))
    stub = _crosswalk_stubs_from_trades(trades)
    partition = rt.normalized_store.write_partition(
        "market_crosswalk",
        stub,
        ts_column="ingested_at",
    )
    if partition is not None:
        rt.manifest.register_partition("market_crosswalk", partition, stub, ts_column="ingested_at")
    return {"attempted": len(needed), "fetched": fetched, "stubbed": len(stub), "gaps": gaps}


def hydrate_price_history_from_trades(
    runtime: Runtime | None = None,
    *,
    trades: pd.DataFrame | None = None,
    save_raw: bool = True,
    max_tokens: int | None = None,
    lookback_minutes: int = 10,
    lookahead_minutes: int = 40,
    fidelity_minutes: int = 5,
    max_window_hours: int = 24,
    adaptive_split: bool = True,
) -> dict[str, Any]:
    """Fetch CLOB price history over trade-relative windows."""

    rt = runtime or Runtime()
    trades = trades if trades is not None else rt.normalized_store.read_table("trades")
    if trades.empty:
        return {"attempted": 0, "fetched": 0, "empty": 0, "gaps": 0}
    windows = build_trade_price_history_windows(
        trades,
        lookback_minutes=lookback_minutes,
        lookahead_minutes=lookahead_minutes,
        max_window_hours=max_window_hours,
        max_tokens=max_tokens,
    )
    if not windows:
        return {"attempted": 0, "fetched": 0, "empty": 0, "gaps": 0}
    fetched = empty = gaps = chunks_succeeded = 0
    attempted_tokens = len({window.token_id for window in windows})
    for window in windows:
        result = _fetch_price_history_window(
            rt,
            window,
            fidelity_minutes=fidelity_minutes,
            save_raw=save_raw,
            adaptive_split=adaptive_split,
        )
        fetched += result["fetched"]
        empty += result["empty"]
        gaps += result["gaps"]
        chunks_succeeded += result["chunks_succeeded"]
    counts = normalize_raw("clob", rt.raw_store, rt.normalized_store, rt.manifest)
    return {
        "attempted": attempted_tokens,
        "windows": len(windows),
        "chunks_succeeded": chunks_succeeded,
        "fetched": fetched,
        "empty": empty,
        "gaps": gaps,
        "normalized": counts,
    }


def hydrate_guru_price_history(
    runtime: Runtime | None = None,
    *,
    category: str = "OVERALL",
    period: str = "MONTH",
    top_n: int = 50,
    max_trades_per_wallet: int = 500,
    delays: str = "5m,30m",
    lookback_minutes: int = 10,
    lookahead_minutes: int = 45,
    fidelity_minutes: int = 5,
    max_window_hours: int = 24,
    adaptive_split: bool = True,
    save_raw: bool = True,
    rebuild_marts: bool = True,
) -> dict[str, Any]:
    """Hydrate price history for the exact guru leaderboard universe."""

    rt = runtime or Runtime()
    parsed_delays = parse_delays(delays)
    effective_lookahead = max(
        lookahead_minutes,
        int(max((delay.total_seconds() for delay in parsed_delays), default=0) // 60) + 10,
    )
    trades = select_guru_universe_trades(
        store=rt.normalized_store,
        category=category,
        period=period,
        top_n=top_n,
        max_trades_per_wallet=max_trades_per_wallet,
    )
    hydrate = hydrate_price_history_from_trades(
        rt,
        trades=trades,
        save_raw=save_raw,
        lookback_minutes=lookback_minutes,
        lookahead_minutes=effective_lookahead,
        fidelity_minutes=fidelity_minutes,
        max_window_hours=max_window_hours,
        adaptive_split=adaptive_split,
    )
    marts = (
        build_basic_marts(rt.normalized_store, rt.settings.data.marts_dir)
        if rebuild_marts
        else {}
    )
    return {
        "category": category,
        "period": period,
        "top_n": top_n,
        "selected_trades": len(trades),
        "selected_wallets": int(trades["trader_wallet"].nunique()) if not trades.empty else 0,
        "selected_token_ids": int(trades["token_id"].nunique()) if not trades.empty else 0,
        "lookahead_minutes": effective_lookahead,
        "price_history": hydrate,
        "marts": marts,
    }


def build_trade_price_history_windows(
    trades: pd.DataFrame,
    *,
    lookback_minutes: int = 10,
    lookahead_minutes: int = 40,
    max_window_hours: int = 24,
    max_tokens: int | None = None,
) -> list[PriceHistoryWindow]:
    """Build merged trade-relative windows per token without exceeding a cap."""

    if trades.empty:
        return []
    df = trades.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    df = df.dropna(subset=["timestamp", "token_id"])
    token_ids = sorted(str(token_id) for token_id in df["token_id"].dropna().unique())
    if max_tokens is not None:
        token_ids = token_ids[:max_tokens]
    windows: list[PriceHistoryWindow] = []
    max_seconds = max_window_hours * 3600
    for token_id in token_ids:
        token_trades = df[df["token_id"].astype(str) == token_id].sort_values("timestamp")
        raw_windows = [
            PriceHistoryWindow(
                token_id=token_id,
                start_ts=int((ts - pd.Timedelta(minutes=lookback_minutes)).timestamp()),
                end_ts=int((ts + pd.Timedelta(minutes=lookahead_minutes)).timestamp()),
            )
            for ts in token_trades["timestamp"]
        ]
        windows.extend(_merge_windows(raw_windows, max_seconds=max_seconds))
    return windows


def _merge_windows(
    windows: list[PriceHistoryWindow],
    *,
    max_seconds: int,
) -> list[PriceHistoryWindow]:
    if not windows:
        return []
    merged: list[PriceHistoryWindow] = []
    current = windows[0]
    for window in windows[1:]:
        proposed_end = max(current.end_ts, window.end_ts)
        overlaps = window.start_ts <= current.end_ts
        within_cap = proposed_end - current.start_ts <= max_seconds
        if overlaps and within_cap:
            current = PriceHistoryWindow(current.token_id, current.start_ts, proposed_end)
        else:
            merged.append(current)
            current = window
    merged.append(current)
    return merged


def _fetch_price_history_window(
    runtime: Runtime,
    window: PriceHistoryWindow,
    *,
    fidelity_minutes: int,
    save_raw: bool,
    adaptive_split: bool,
) -> dict[str, int]:
    try:
        payload = runtime.clob.get_price_history(
            window.token_id,
            start_ts=window.start_ts,
            end_ts=window.end_ts,
            interval=None,
            fidelity=fidelity_minutes,
            save_raw=save_raw,
        )
        history = payload.get("history", []) if isinstance(payload, dict) else []
        if history:
            return {"fetched": 1, "empty": 0, "gaps": 0, "chunks_succeeded": 1}
        runtime.manifest.record_gap(
            "price_points",
            entity_id=window.token_id,
            reason="price_history_empty",
            start_ts=pd.to_datetime(window.start_ts, unit="s", utc=True).to_pydatetime(),
            end_ts=pd.to_datetime(window.end_ts, unit="s", utc=True).to_pydatetime(),
        )
        return {"fetched": 0, "empty": 1, "gaps": 0, "chunks_succeeded": 1}
    except ApiError as exc:
        reason = _price_history_gap_reason(exc)
        if (
            adaptive_split
            and reason == "price_history_window_too_long"
            and window.end_ts - window.start_ts > 3600
        ):
            midpoint = window.start_ts + ((window.end_ts - window.start_ts) // 2)
            first = PriceHistoryWindow(window.token_id, window.start_ts, midpoint)
            second = PriceHistoryWindow(window.token_id, midpoint, window.end_ts)
            left = _fetch_price_history_window(
                runtime,
                first,
                fidelity_minutes=fidelity_minutes,
                save_raw=save_raw,
                adaptive_split=True,
            )
            right = _fetch_price_history_window(
                runtime,
                second,
                fidelity_minutes=fidelity_minutes,
                save_raw=save_raw,
                adaptive_split=True,
            )
            return {key: left[key] + right[key] for key in left}
        if reason == "price_history_window_too_long":
            reason = "price_history_rejected_after_adaptive_split"
        runtime.manifest.record_gap(
            "price_points",
            entity_id=window.token_id,
            reason=reason,
            start_ts=pd.to_datetime(window.start_ts, unit="s", utc=True).to_pydatetime(),
            end_ts=pd.to_datetime(window.end_ts, unit="s", utc=True).to_pydatetime(),
        )
        return {"fetched": 0, "empty": 0, "gaps": 1, "chunks_succeeded": 0}


def _price_history_gap_reason(exc: ApiError) -> str:
    message = str(exc).lower()
    if "interval is too long" in message:
        return "price_history_window_too_long"
    if exc.status_code == 400 and "token" in message:
        return "price_history_invalid_token"
    if exc.status_code is None and "timeout" in message:
        return "price_history_timeout"
    return "price_history_endpoint_error"


def _crosswalk_stubs_from_trades(trades: pd.DataFrame) -> pd.DataFrame:
    rows = []
    now = pd.Timestamp.now(tz=UTC)
    grouped = trades.groupby(["condition_id", "token_id", "outcome"], dropna=False)
    for (condition_id, token_id, outcome), group in grouped:
        rows.append(
            {
                "condition_id": str(condition_id),
                "token_id": str(token_id),
                "market_id": None,
                "market_slug": _first(group, "market_slug"),
                "event_slug": _first(group, "event_slug"),
                "title": _first(group, "title"),
                "outcome": None if pd.isna(outcome) else str(outcome),
                "outcome_index": _first(group, "outcome_index"),
                "resolved": None,
                "winning_outcome": None,
                "resolution_source": "unknown",
                "metadata_source": "trade_stub",
                "source": "data_api",
                "raw_path": "derived:trades",
                "ingested_at": now,
                "schema_version": 1,
            }
        )
    return pd.DataFrame(rows)


def _first(df: pd.DataFrame, column: str) -> Any:
    if column not in df:
        return None
    values = df[column].dropna()
    if values.empty:
        return None
    return values.iloc[0]
