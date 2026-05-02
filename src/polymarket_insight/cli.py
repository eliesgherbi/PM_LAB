"""Command line interface for Polymarket Insight."""

from __future__ import annotations

import json
from typing import Annotated

import httpx
import typer

from polymarket_insight import __version__
from polymarket_insight.config import load_settings
from polymarket_insight.data.normalize import normalize_raw
from polymarket_insight.data.storage.marts import build_basic_marts
from polymarket_insight.ingestion.health import snapshot_health
from polymarket_insight.ingestion.leaderboard_job import run_leaderboard_job
from polymarket_insight.ingestion.metadata_job import run_metadata_job
from polymarket_insight.ingestion.orderbook_job import run_orderbook_loop, run_orderbook_once
from polymarket_insight.ingestion.runner import Runtime
from polymarket_insight.logging import configure_logging
from polymarket_insight.research.coverage import build_coverage_report
from polymarket_insight.research.hydration import (
    hydrate_guru_price_history,
    hydrate_price_history_from_trades,
    hydrate_traded_markets,
)
from polymarket_insight.research.price_coverage import build_price_coverage_report
from polymarket_insight.research.sports import run_sports_line_movement_research
from polymarket_insight.research.trader import (
    build_trader_research_mart,
    build_trader_universe,
    hydrate_trader_research_dataset,
    run_guru_copyability_research,
)

app = typer.Typer(help="Read-only Polymarket research toolkit.")
debug_app = typer.Typer(help="Debug public API requests.")
fetch_app = typer.Typer(help="Fetch public API data.")
normalize_app = typer.Typer(help="Normalize raw data.")
storage_app = typer.Typer(help="Storage commands.")
build_app = typer.Typer(help="Build derived artifacts.")
ingest_app = typer.Typer(help="Run ingestion jobs.")
snapshots_app = typer.Typer(help="Snapshot health commands.")
research_app = typer.Typer(help="Research readiness workflows.")
research_run_app = typer.Typer(help="Run reproducible research workflows.")
app.add_typer(fetch_app, name="fetch")
app.add_typer(debug_app, name="debug")
app.add_typer(normalize_app, name="normalize")
app.add_typer(storage_app, name="storage")
app.add_typer(build_app, name="build")
app.add_typer(ingest_app, name="ingest")
app.add_typer(snapshots_app, name="snapshots")
research_app.add_typer(research_run_app, name="run")
app.add_typer(research_app, name="research")


def _runtime() -> Runtime:
    settings = load_settings()
    configure_logging(settings.logging.level, json_logs=settings.logging.json_logs)
    return Runtime(settings)


@app.command()
def version() -> None:
    """Print package version."""

    typer.echo(f"polymarket-insight {__version__}")


@fetch_app.command()
def markets(limit: int = 100, save_raw: bool = False) -> None:
    """Fetch public Gamma markets."""

    rt = _runtime()
    rows = rt.gamma.list_markets(limit=limit, save_raw=save_raw)
    typer.echo(f"Fetched {len(rows)} markets")


@fetch_app.command()
def trades(user: str, limit: int = 500, save_raw: bool = False) -> None:
    """Fetch public user trades."""

    rt = _runtime()
    rows = rt.data_api.get_trades(user=user, limit=limit, save_raw=save_raw)
    typer.echo(f"Fetched {len(rows)} trades")


@fetch_app.command()
def leaderboard(
    category: str = "OVERALL",
    period: str = "MONTH",
    order_by: str = "PNL",
    save_raw: bool = False,
) -> None:
    """Fetch public trader leaderboard."""

    rt = _runtime()
    rows = rt.data_api.get_leaderboard(
        category=category,
        time_period=period,
        order_by=order_by,
        save_raw=save_raw,
    )
    typer.echo(f"Fetched {len(rows)} leaderboard rows")


@fetch_app.command()
def book(token_id: Annotated[str, typer.Option("--token-id")], save_raw: bool = False) -> None:
    """Fetch a public CLOB orderbook."""

    rt = _runtime()
    payload = rt.clob.get_book(token_id, save_raw=save_raw)
    typer.echo(f"Fetched book for {payload.get('asset_id') or token_id}")


@fetch_app.command("trader-universe")
def fetch_trader_universe(
    leaderboard: str = "OVERALL",
    period: str = "MONTH",
    top_n: int = 50,
    save_raw: bool = False,
) -> None:
    """Fetch leaderboard and trades for top wallets, then normalize."""

    rt = _runtime()
    rt.data_api.get_leaderboard(
        category=leaderboard,
        time_period=period,
        order_by="PNL",
        limit=top_n,
        save_raw=save_raw,
    )
    normalize_raw("data_api", rt.raw_store, rt.normalized_store, rt.manifest)
    universe_df = build_trader_universe(
        category=leaderboard,
        period=period,
        order_by="PNL",
        top_n=top_n,
        store=rt.normalized_store,
    )
    hydrate = hydrate_trader_research_dataset(
        universe_df["wallet"].dropna().astype(str).tolist(),
        runtime=rt,
        save_raw=save_raw,
    )
    coverage = build_coverage_report(rt.normalized_store)
    typer.echo(
        json.dumps(
            {
                "leaderboard_rows": len(universe_df),
                "hydration": hydrate,
                "coverage_status": coverage["coverage_status"],
            },
            default=str,
        )
    )


@normalize_app.command("raw")
def normalize_raw_command(source: str = "all") -> None:
    """Normalize raw API evidence."""

    rt = _runtime()
    counts = normalize_raw(source, rt.raw_store, rt.normalized_store, rt.manifest)
    typer.echo(f"Normalized rows: {json.dumps(counts, default=str)}")


@storage_app.command("report")
def storage_report() -> None:
    """Print a compact storage report."""

    rt = _runtime()
    typer.echo(json.dumps(rt.manifest.report(), default=str))


@build_app.command("marts")
def build_marts() -> None:
    """Build research marts from normalized tables."""

    rt = _runtime()
    counts = build_basic_marts(rt.normalized_store, rt.settings.data.marts_dir)
    typer.echo(f"Built marts: {json.dumps(counts)}")


@research_app.command("seed")
def research_seed(top_n: int = 50, category: str = "OVERALL", period: str = "MONTH") -> None:
    """Seed local research data for guru analysis."""

    rt = _runtime()
    rt.gamma.list_markets(limit=500, save_raw=True)
    rt.data_api.get_leaderboard(
        category=category,
        time_period=period,
        order_by="PNL",
        limit=top_n,
        save_raw=True,
    )
    rt.data_api.get_leaderboard(
        category="SPORTS",
        time_period="MONTH",
        order_by="PNL",
        save_raw=True,
    )
    rt.data_api.get_leaderboard(
        category="OVERALL",
        time_period="ALL",
        order_by="PNL",
        save_raw=True,
    )
    normalize_raw("all", rt.raw_store, rt.normalized_store, rt.manifest)
    universe_df = build_trader_universe(
        category=category,
        period=period,
        order_by="PNL",
        top_n=top_n,
        store=rt.normalized_store,
    )
    hydrate = hydrate_trader_research_dataset(
        universe_df["wallet"].dropna().astype(str).tolist(),
        runtime=rt,
    )
    mart = build_trader_research_mart(
        store=rt.normalized_store,
        marts_dir=rt.settings.data.marts_dir,
    )
    coverage = build_coverage_report(rt.normalized_store)
    typer.echo(
        json.dumps(
            {
                "leaderboard_wallets": len(universe_df),
                "trader_research_rows": len(mart),
                "hydration": hydrate,
                "coverage_status": coverage["coverage_status"],
            },
            default=str,
        )
    )


@research_app.command("coverage")
def research_coverage(explain: bool = False) -> None:
    """Report research dataset coverage and quality status."""

    rt = _runtime()
    typer.echo(json.dumps(build_coverage_report(rt.normalized_store, explain=explain), default=str))


@research_app.command("hydrate-trades")
def research_hydrate_trades(
    include_market_metadata: bool = True,
    include_resolution: bool = True,
    include_price_history: bool = False,
    max_markets: int | None = None,
    max_tokens: int | None = None,
) -> None:
    """Hydrate market metadata, resolution, and optional price history from trades."""

    rt = _runtime()
    result = hydrate_traded_markets(
        rt,
        include_market_metadata=include_market_metadata,
        include_resolution=include_resolution,
        include_price_history=include_price_history,
        max_markets=max_markets,
        max_tokens=max_tokens,
    )
    typer.echo(json.dumps(result, default=str))


@research_app.command("hydrate-price-history")
def research_hydrate_price_history(
    from_trades: Annotated[bool, typer.Option("--from-trades")] = True,
    max_tokens: int | None = None,
    lookback_minutes: int = 10,
    lookahead_minutes: int = 40,
    fidelity_minutes: int = 5,
    max_window_hours: int = 24,
    adaptive_split: bool = True,
) -> None:
    """Hydrate CLOB price history for tokens from normalized trades."""

    if not from_trades:
        raise typer.BadParameter("Only --from-trades is currently supported.")
    rt = _runtime()
    result = hydrate_price_history_from_trades(
        rt,
        max_tokens=max_tokens,
        lookback_minutes=lookback_minutes,
        lookahead_minutes=lookahead_minutes,
        fidelity_minutes=fidelity_minutes,
        max_window_hours=max_window_hours,
        adaptive_split=adaptive_split,
    )
    typer.echo(json.dumps(result, default=str))


@research_app.command("hydrate-guru-price-history")
def research_hydrate_guru_price_history(
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
) -> None:
    """Hydrate CLOB price history for the guru notebook universe."""

    rt = _runtime()
    result = hydrate_guru_price_history(
        rt,
        category=category,
        period=period,
        top_n=top_n,
        max_trades_per_wallet=max_trades_per_wallet,
        delays=delays,
        lookback_minutes=lookback_minutes,
        lookahead_minutes=lookahead_minutes,
        fidelity_minutes=fidelity_minutes,
        max_window_hours=max_window_hours,
        adaptive_split=adaptive_split,
    )
    typer.echo(json.dumps(result, default=str))


@research_app.command("price-coverage")
def research_price_coverage(
    category: str = "OVERALL",
    period: str = "MONTH",
    top_n: int = 50,
    delays: str = "5m,30m",
    tolerance_minutes: int = 10,
) -> None:
    """Diagnose trade-level price coverage for the guru notebook universe."""

    rt = _runtime()
    result = build_price_coverage_report(
        store=rt.normalized_store,
        category=category,
        period=period,
        top_n=top_n,
        delays=delays,
        tolerance_minutes=tolerance_minutes,
    )
    typer.echo(json.dumps(result, default=str))


@debug_app.command("price-history")
def debug_price_history(
    token_id: Annotated[str, typer.Option("--token-id")],
    start_ts: Annotated[int, typer.Option("--start-ts")],
    end_ts: Annotated[int, typer.Option("--end-ts")],
    fidelity: int = 5,
) -> None:
    """Inspect a single public CLOB price-history request."""

    rt = _runtime()
    url = f"{rt.clob.http.base_url}/prices-history"
    params = {
        "market": token_id,
        "startTs": start_ts,
        "endTs": end_ts,
        "fidelity": fidelity,
    }
    with httpx.Client(timeout=rt.settings.api.timeout_s) as client:
        response = client.get(url, params=params)
    try:
        payload = response.json()
    except ValueError:
        payload = {"text": response.text}
    history = payload.get("history", []) if isinstance(payload, dict) else []
    body = response.content or b""
    output = {
        "request_url": str(response.request.url),
        "status_code": response.status_code,
        "response_size": len(body),
        "history_points": len(history),
        "first_points": history[:3],
        "last_points": history[-3:] if history else [],
    }
    if response.status_code >= 400:
        output["error_body"] = payload
    typer.echo(json.dumps(output, default=str))


@research_run_app.command("guru-copyability")
def research_run_guru_copyability(wallet: Annotated[str, typer.Option("--wallet")]) -> None:
    """Run a reproducible guru-copyability workflow."""

    rt = _runtime()
    run_dir = run_guru_copyability_research(wallet, store=rt.normalized_store)
    typer.echo(f"Research run written to {run_dir}")


@research_run_app.command("sports-line-movement")
def research_run_sports_line_movement(
    league: Annotated[str | None, typer.Option("--league")] = None,
    sport: Annotated[str | None, typer.Option("--sport")] = None,
) -> None:
    """Run a reproducible sports line-movement workflow."""

    rt = _runtime()
    run_dir = run_sports_line_movement_research(
        league=league,
        sport=sport,
        store=rt.normalized_store,
    )
    typer.echo(f"Research run written to {run_dir}")


@ingest_app.command("metadata")
def ingest_metadata() -> None:
    """Run metadata ingestion once."""

    counts = run_metadata_job(_runtime())
    typer.echo(f"Metadata ingestion complete: {json.dumps(counts, default=str)}")


@ingest_app.command("leaderboard")
def ingest_leaderboard() -> None:
    """Run leaderboard ingestion once."""

    counts = run_leaderboard_job(_runtime())
    typer.echo(f"Leaderboard ingestion complete: {json.dumps(counts, default=str)}")


@ingest_app.command("orderbook")
def ingest_orderbook(once: bool = False, loop: bool = False) -> None:
    """Run orderbook ingestion once or continuously."""

    rt = _runtime()
    if loop:
        typer.echo("Starting orderbook ingestion loop")
        run_orderbook_loop(rt)
    if once or not loop:
        counts = run_orderbook_once(rt)
        typer.echo(f"Orderbook ingestion complete: {json.dumps(counts, default=str)}")


@snapshots_app.command("health")
def snapshots_health() -> None:
    """Print snapshot health and recent gaps."""

    rt = _runtime()
    typer.echo(json.dumps(snapshot_health(rt.manifest), default=str))
