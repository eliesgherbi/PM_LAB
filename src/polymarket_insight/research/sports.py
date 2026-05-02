"""Sports market pattern research workflows."""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path
from typing import Any

import pandas as pd

from polymarket_insight.data.storage.normalized import NormalizedStore
from polymarket_insight.research.artifacts import (
    new_run_id,
    write_research_card,
    write_research_run,
)
from polymarket_insight.research.confidence import confidence_label
from polymarket_insight.research.coverage import build_coverage_report


def build_sports_universe(
    *,
    sport: str | None = None,
    league: str | None = None,
    resolved_only: bool = True,
    min_volume: Decimal | None = None,
    store: NormalizedStore | None = None,
) -> pd.DataFrame:
    """Build a normalized sports market universe."""

    markets = (store or NormalizedStore()).read_table("markets")
    columns = [
        "market_id",
        "condition_id",
        "event_slug",
        "market_slug",
        "sport",
        "league",
        "question",
        "game_start_time",
        "resolved",
        "winning_outcome",
        "volume",
        "liquidity",
        "token_ids",
    ]
    if markets.empty:
        return pd.DataFrame(columns=columns)
    df = markets.copy()
    df["sport"] = df["tags"].apply(
        lambda tags: _tag_match(tags, sport) or _tag_match(tags, "sports")
    )
    df["league"] = df["tags"].apply(lambda tags: _tag_match(tags, league))
    sports_mask = (
        df["category"].fillna("").str.upper().eq("SPORTS")
        | df["tags"].apply(lambda tags: any("sport" in str(tag).lower() for tag in tags))
    )
    df = df[sports_mask]
    if sport:
        df = df[df["tags"].apply(lambda tags: _contains_tag(tags, sport))]
    if league:
        df = df[df["tags"].apply(lambda tags: _contains_tag(tags, league))]
    if resolved_only:
        df = df[df["resolved"].fillna(False)]
    if min_volume is not None:
        volume_mask = df["volume"].apply(
            lambda value: value is not None and Decimal(str(value)) >= min_volume
        )
        df = df[volume_mask]
    df["game_start_time"] = df["start_time"].fillna(df["end_time"])
    return df[columns].reset_index(drop=True)


def extract_prices_around_anchor(
    markets: pd.DataFrame,
    *,
    anchor_col: str = "game_start_time",
    offsets: list[str] | None = None,
    store: NormalizedStore | None = None,
) -> pd.DataFrame:
    """Extract token prices around market anchors."""

    offsets = offsets or ["24h", "6h", "1h", "15m", "close"]
    price_points = (store or NormalizedStore()).read_table("price_points")
    rows = []
    if markets.empty:
        return pd.DataFrame()
    for _, market in markets.iterrows():
        anchor = pd.to_datetime(market.get(anchor_col), utc=True, errors="coerce")
        if pd.isna(anchor):
            rows.append(_missing_anchor_row(market, offsets))
            continue
        for token_id in market.get("token_ids") or []:
            token_prices = (
                price_points[price_points["token_id"] == str(token_id)].copy()
                if not price_points.empty
                else pd.DataFrame()
            )
            if not token_prices.empty:
                token_prices["timestamp"] = pd.to_datetime(token_prices["timestamp"], utc=True)
            row = {
                "market_id": market["market_id"],
                "condition_id": market["condition_id"],
                "token_id": str(token_id),
                "resolved_outcome": market.get("winning_outcome"),
                "missing_anchor": False,
            }
            for offset in offsets:
                column = "closing_price" if offset == "close" else f"price_t_minus_{offset}"
                target = anchor if offset == "close" else anchor - pd.Timedelta(offset)
                row[column] = _price_at_or_before(token_prices, target)
            rows.append(row)
    return pd.DataFrame(rows)


def sports_line_movement_metrics(
    anchor_prices: pd.DataFrame,
    markets: pd.DataFrame,
) -> pd.DataFrame:
    """Compute sports line movement metrics with sample-size guards."""

    if anchor_prices.empty:
        return pd.DataFrame()
    df = anchor_prices.merge(
        markets[["condition_id", "volume", "liquidity", "resolved", "winning_outcome"]],
        on="condition_id",
        how="left",
    )
    for source in ("24h", "6h", "1h"):
        col = f"price_t_minus_{source}"
        if col in df and "closing_price" in df:
            df[f"movement_{source}_to_close"] = df["closing_price"] - df[col]
    df["liquidity_bucket"] = pd.cut(
        pd.to_numeric(df["liquidity"], errors="coerce").fillna(0),
        bins=[-1, 1000, 5000, 25_000, float("inf")],
        labels=["low", "medium", "high", "very_high"],
    )
    full_price_cols = [col for col in df.columns if col.startswith("price_t_minus_")]
    df["has_full_price_history"] = df[full_price_cols + ["closing_price"]].notna().all(axis=1)
    df["sample_size"] = len(df)
    df["missing_data_ratio"] = 1 - float(df["has_full_price_history"].mean())
    df["coverage_status"] = _sports_coverage_status(df)
    df["confidence_label"] = confidence_label(
        sample_size=len(df),
        missing_data_ratio=float(df["missing_data_ratio"].iloc[0]),
        coverage=str(df["coverage_status"].iloc[0]),
    )
    return df


def sample_size_guard(markets: pd.DataFrame, anchor_prices: pd.DataFrame) -> dict[str, int]:
    """Report sports sample-size and missing-data guardrails."""

    price_cols = [col for col in anchor_prices.columns if col.startswith("price_t_minus_")]
    return {
        "number_of_markets": int(len(markets)),
        "number_of_resolved_markets": int(markets["resolved"].fillna(False).sum())
        if not markets.empty
        else 0,
        "number_with_full_price_history": int(
            anchor_prices[price_cols + ["closing_price"]].notna().all(axis=1).sum()
        )
        if not anchor_prices.empty and price_cols
        else 0,
        "number_missing_anchors": int(
            anchor_prices.get("missing_anchor", pd.Series(dtype=bool)).sum()
        )
        if not anchor_prices.empty
        else 0,
        "number_with_enough_liquidity": int(
            pd.to_numeric(markets.get("liquidity", pd.Series(dtype=float)), errors="coerce")
            .fillna(0)
            .ge(1000)
            .sum()
        )
        if not markets.empty
        else 0,
    }


def run_sports_line_movement_research(
    *,
    league: str | None = None,
    sport: str | None = None,
    store: NormalizedStore | None = None,
    run_id: str | None = None,
) -> Path:
    """Write a reproducible sports line movement research run and card."""

    normalized = store or NormalizedStore()
    universe = build_sports_universe(league=league, sport=sport, store=normalized)
    anchor_prices = extract_prices_around_anchor(universe, store=normalized)
    metrics = sports_line_movement_metrics(anchor_prices, universe)
    guard = sample_size_guard(universe, anchor_prices)
    coverage = build_coverage_report(normalized)
    conclusion = _sports_conclusion(metrics, guard)
    card = _sports_card(league or sport or "sports", conclusion, guard, metrics)
    slug = (league or sport or "sports").lower().replace(" ", "_")
    write_research_card(f"research/0002_sports_line_movement_{slug}.md", card)
    return write_research_run(
        run_id=run_id or new_run_id("sports_line_movement"),
        input_config={"workflow": "sports-line-movement", "league": league, "sport": sport},
        dataset_coverage={**coverage, "sports_guard": guard},
        metrics=metrics if not metrics.empty else pd.DataFrame([guard]),
        research_card=card,
    )


def _tag_match(tags: Any, wanted: str | None) -> str | None:
    tags = _coerce_tags(tags)
    if wanted is None:
        return str(tags[0]) if tags else None
    for tag in tags:
        if str(tag).lower() == wanted.lower():
            return str(tag)
    return None


def _contains_tag(tags: Any, wanted: str) -> bool:
    return any(wanted.lower() in str(tag).lower() for tag in _coerce_tags(tags))


def _coerce_tags(tags: Any) -> list[Any]:
    if tags is None or isinstance(tags, str):
        return []
    if isinstance(tags, list | tuple):
        return list(tags)
    if hasattr(tags, "tolist"):
        value = tags.tolist()
        return value if isinstance(value, list) else []
    return []


def _price_at_or_before(prices: pd.DataFrame, target: pd.Timestamp) -> float | None:
    if prices.empty:
        return None
    candidates = prices[prices["timestamp"] <= target].sort_values("timestamp")
    if candidates.empty:
        return None
    return float(candidates.iloc[-1]["price"])


def _missing_anchor_row(market: pd.Series, offsets: list[str]) -> dict[str, Any]:
    row = {
        "market_id": market.get("market_id"),
        "condition_id": market.get("condition_id"),
        "token_id": None,
        "resolved_outcome": market.get("winning_outcome"),
        "missing_anchor": True,
    }
    for offset in offsets:
        row["closing_price" if offset == "close" else f"price_t_minus_{offset}"] = None
    return row


def _sports_coverage_status(df: pd.DataFrame) -> str:
    full_ratio = float(df["has_full_price_history"].mean()) if "has_full_price_history" in df else 0
    if full_ratio >= 0.85:
        return "good"
    if full_ratio >= 0.60:
        return "partial"
    if full_ratio >= 0.30:
        return "weak"
    return "unusable"


def _sports_conclusion(metrics: pd.DataFrame, guard: dict[str, int]) -> str:
    if metrics.empty or guard["number_of_markets"] < 20:
        return "inconclusive"
    confidence = str(metrics.iloc[0].get("confidence_label", "insufficient_data"))
    if confidence == "insufficient_data":
        return "inconclusive"
    movement_cols = [col for col in metrics.columns if col.startswith("movement_")]
    if not movement_cols:
        return "inconclusive"
    mean_abs_movement = metrics[movement_cols].abs().mean(numeric_only=True).mean()
    return "pattern_supported" if mean_abs_movement > 0.05 else "pattern_rejected"


def _sports_card(
    universe_name: str,
    conclusion: str,
    guard: dict[str, int],
    metrics: pd.DataFrame,
) -> str:
    confidence = "insufficient_data" if metrics.empty else metrics.iloc[0].get("confidence_label")
    return f"""# Finding: Sports Line Movement for {universe_name}

## Question

Do sports prices move predictably before event start, and does closing price contain useful signal?

## Dataset

- Markets: {guard["number_of_markets"]}
- Resolved markets: {guard["number_of_resolved_markets"]}
- Full price history rows: {guard["number_with_full_price_history"]}
- Missing anchors: {guard["number_missing_anchors"]}
- Enough liquidity: {guard["number_with_enough_liquidity"]}

## Method

Extract token prices at T-24h, T-6h, T-1h, T-15m, and close, then measure drift.

## Result

Conclusion: **{conclusion}**

## Trading implication

Research classification only. PMI does not execute trades.

## Failure modes

Small sample, missing start times, missing price history, sparse liquidity, and unresolved markets.

## Confidence

{confidence}

## Next step

Collect a broader sports dataset and repeat with league-specific filters.
"""
