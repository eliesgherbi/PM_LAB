# Developer Guide

## Architecture

- `clients`: public, read-only API wrappers only.
- `storage`: raw JSON evidence, temporary cache, normalized Parquet, marts, DuckDB manifest.
- `models`: canonical schemas.
- `datasets`: notebook-friendly data access.
- `metrics`: pure functions with no I/O.
- `analysis`: reusable workflows built from datasets and metrics.
- `research`: coverage/confidence reporting, research marts, classifications, and reproducible run artifacts.
- `ingestion`: scheduled public data collection.
- `viz`: Plotly figure builders.

## Boundaries

Do not add order placement, cancellation, signing, private-key handling, authenticated CLOB write calls, live risk, OMS, or trading runtime integration. Tests enforce this boundary by searching source text.

## Adding An Endpoint

Add a method to the appropriate public client, preserve read-only semantics, return JSON-like Python objects, and support `save_raw`. Add fixture-based tests; do not depend on live API calls.

## Adding A Schema

Create a model under `data/models`, add a normalizer in `data/normalize.py`, write Parquet through `NormalizedStore`, and register partitions in `Manifest`.

## Adding A Metric

Metrics must be pure functions over DataFrames or typed values. Every price-based metric must state its price assumption. Do not name reconstructed values exact `pnl`; use `estimated_*` or `edge` where appropriate.

## Adding A Research Workflow

Add reusable logic under `src/polymarket_insight/research/`, expose it from `pmi research ...`, and write artifacts through `write_research_run()`. Every workflow should include `sample_size`, `missing_data_ratio`, `coverage_status`, and `confidence_label`.

## Adding A Notebook

Keep notebooks thin. Import from datasets, metrics, analysis, and viz. Do not call raw APIs or implement reusable math in notebook cells.

## Adding An Ingestion Job

Save raw first, normalize successful responses, update the manifest, tolerate partial failures, and record data gaps for failed entities.

## Decimal Rules

Use `Decimal` for ingestion and normalization of money, prices, size, volume, liquidity, bids, asks, and notional. Convert to float only inside statistical analysis or plotting.

## Storage Distinctions

- Raw: append-only research evidence.
- Cache: temporary and safe to delete.
- Normalized: canonical Parquet tables rebuilt from raw.
- Marts: derived outputs rebuilt from normalized tables.
