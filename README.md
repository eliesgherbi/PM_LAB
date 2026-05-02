# Polymarket Insight

Polymarket Insight is a read-only research and analytics toolkit for Polymarket. It helps researchers fetch public data, preserve raw evidence, normalize reusable datasets, compute metrics, run notebooks, and write documented research cards.

It is not a trading bot, execution system, wallet manager, risk engine, OMS, or live strategy runtime. The package never requires secrets and does not implement authenticated trading endpoints.

## Quickstart

```bash
pip install -e ".[dev]"
pmi version
pytest
pmi fetch markets --limit 10 --save-raw
pmi normalize raw --source all
pmi storage report
```

Copy `config.example.toml` if you want local overrides. Runtime data is written under `data/`, which is gitignored:

- `data/raw`: append-only API evidence
- `data/cache`: temporary HTTP cache
- `data/normalized`: canonical Parquet tables
- `data/marts`: rebuildable research marts
- `data/manifest.duckdb`: raw/partition/gap manifest

## CLI

```bash
pmi version
pmi fetch markets --limit 100 --save-raw
pmi fetch trades --user 0x... --limit 500 --save-raw
pmi fetch leaderboard --category OVERALL --period MONTH --order-by PNL --save-raw
pmi fetch book --token-id <token_id> --save-raw
pmi normalize raw --source all
pmi build marts
pmi research seed --top-n 50 --category OVERALL --period MONTH
pmi research coverage
pmi research run guru-copyability --wallet 0x...
pmi research run sports-line-movement --league NBA
pmi ingest metadata
pmi ingest leaderboard
pmi ingest orderbook --once
pmi snapshots health
pmi storage report
```

## Notebook Workflow

Use notebooks as thin clients over `polymarket_insight.data.datasets`, `polymarket_insight.metrics`, `polymarket_insight.analysis`, and `polymarket_insight.viz`. Do not put raw API calls or core metric math in notebooks.

## Research Workflow

Use `pmi research seed` to collect a local leaderboard/trader seed dataset, then check `pmi research coverage` before trusting metrics. Reproducible research runs write artifacts under `research_runs/<run_id>/`, including coverage, metrics, and a research card.

## Safety

This project is for read-only research. It never places orders, cancels orders, signs messages, stores private keys, or reconciles live trading positions.
