# Implementation Plan

This repository implements the upgraded Polymarket Insight guide as a read-only Python package.

## Phases

1. Foundation: installable `src/` package, Typer CLI, config, logging, tests, lint/type config.
2. Public API clients and raw storage: Gamma, Data API, CLOB public reads, retries, raw evidence, cache.
3. Normalization and manifest: canonical models, Parquet storage, DuckDB manifest and reports.
4. Dataset and metrics: local dataset access, trader/market metrics, copy-delay simulation, analysis helpers.
5. Ingestion: metadata, leaderboard, liquid universe, orderbook snapshots, health/gap reporting.
6. Research artifacts: notebooks, Plotly visualizations, research card templates and examples.
7. Documentation: README, user guide, developer guide, ingestion docs.

## API Adaptations

Current Polymarket public docs expose the Data API leaderboard at `/v1/leaderboard` with `timePeriod` and `orderBy`. CLOB public market-data endpoints remain unauthenticated; authenticated trade/order endpoints are intentionally excluded.

## Known Deviations

The package avoids trading SDKs and authenticated clients entirely. Copy-delay and fillable-price outputs are research approximations and must not be treated as executable trading guarantees.
