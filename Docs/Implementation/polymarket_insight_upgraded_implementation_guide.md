# Polymarket Insight — Upgraded Implementation Guide v0.3

## 0. Purpose of this document

This document is the implementation handoff for a code agent. It converts the project idea into a clear, explicit, buildable plan.

The agent should treat this file as the source of truth for the first implementation pass.

---

## 1. Mission

Build `polymarket_insight`, a **read-only research package** for Polymarket.

The package must help the user answer questions such as:

- Which Polymarket traders are profitable, consistent, and potentially copyable?
- Which traders are profitable but not realistically copyable?
- How do sports markets move before game start or resolution?
- Are apparent market patterns still valid after accounting for spread, liquidity, and delay?
- Which research findings deserve to become strategy hypotheses later?

The package must produce:

- cached Polymarket data,
- normalized research tables,
- reusable metrics,
- thin notebooks,
- research cards,
- and, later, strategy-spec handoffs.

---

## 2. Scope boundaries

### 2.1 What this project is

Polymarket Insight is the research layer.

```text
Polymarket Insight = observe, clean, measure, discover
Trading system     = decide, risk-check, execute, reconcile
```

It should be flexible, exploratory, and notebook-friendly.

### 2.2 What this project is not

Do **not** implement:

- live trading,
- order placement,
- wallet management,
- authenticated CLOB write operations,
- risk engine,
- OMS,
- live reconciliation,
- strategy execution,
- production public dashboard,
- full trading backtester.

Any code related to order submission, wallet signing, live risk checks, or execution belongs outside this project.

### 2.3 Relationship to Tyrex_PM

Tyrex_PM is the trading/runtime project. Polymarket Insight should not depend on Tyrex_PM internals.

This project may borrow design principles from Tyrex_PM:

- clear module boundaries,
- typed models,
- raw evidence preservation,
- structured artifacts,
- fixture-based tests,
- explicit reasoned outputs.

But it must not import Tyrex_PM execution, risk, OMS, or live-state modules.

---

## 3. High-level architecture

The project has the following data flow:

```text
Polymarket APIs / optional external sources
        ↓
Raw immutable API responses
        ↓
Normalized canonical tables
        ↓
Research marts
        ↓
Metrics and analysis functions
        ↓
Thin notebooks and visualizations
        ↓
Research cards
        ↓
Optional future strategy specs
```

The architecture should be package-first, notebook-second.

A notebook is only a thin client. It should call reusable functions from the package, not contain core logic.

### 3.1 Layer rules

```text
notebooks/        → may import anything, but should mostly use analysis/, datasets/, viz/
viz/              → may import metrics/, datasets/
analysis/         → may import metrics/, datasets/, universes/
metrics/          → pure functions only, no I/O
datasets/         → high-level data access, may use storage/ and clients/
models/           → canonical schemas and validation
storage/          → raw, cache, parquet, manifest
clients/          → public API wrappers only
ingestion/        → scheduled jobs using clients/ and storage/
```

### 3.2 Hard rules

- No raw API calls in notebooks.
- No metric math in notebooks.
- No reusable plotting boilerplate in notebooks.
- No I/O inside `metrics/`.
- No trading/authenticated write operations anywhere.
- Raw data is immutable.
- Normalized data is rebuildable from raw.
- Marts are rebuildable from normalized.

---

## 4. Project structure

Create the repository with this structure:

```text
polymarket_insight/
├── pyproject.toml
├── README.md
├── IMPLEMENTATION_GUIDE.md
├── config.example.toml
│
├── src/polymarket_insight/
│   ├── __init__.py
│   ├── config.py
│   ├── logging.py
│   ├── cli.py
│   │
│   ├── data/
│   │   ├── __init__.py
│   │   ├── clients/
│   │   │   ├── __init__.py
│   │   │   ├── base.py
│   │   │   ├── gamma.py
│   │   │   ├── data_api.py
│   │   │   └── clob.py
│   │   │
│   │   ├── models/
│   │   │   ├── __init__.py
│   │   │   ├── market.py
│   │   │   ├── trade.py
│   │   │   ├── price_point.py
│   │   │   ├── orderbook.py
│   │   │   └── trader.py
│   │   │
│   │   ├── storage/
│   │   │   ├── __init__.py
│   │   │   ├── raw.py
│   │   │   ├── cache.py
│   │   │   ├── normalized.py
│   │   │   ├── marts.py
│   │   │   └── manifest.py
│   │   │
│   │   ├── datasets/
│   │   │   ├── __init__.py
│   │   │   ├── markets.py
│   │   │   ├── traders.py
│   │   │   └── snapshots.py
│   │   │
│   │   └── normalize.py
│   │
│   ├── ingestion/
│   │   ├── __init__.py
│   │   ├── universe.py
│   │   ├── metadata_job.py
│   │   ├── leaderboard_job.py
│   │   ├── orderbook_job.py
│   │   ├── runner.py
│   │   └── health.py
│   │
│   ├── metrics/
│   │   ├── __init__.py
│   │   ├── trader.py
│   │   ├── market.py
│   │   ├── liquidity.py
│   │   └── price_paths.py
│   │
│   ├── analysis/
│   │   ├── __init__.py
│   │   ├── rank_traders.py
│   │   ├── compare_markets.py
│   │   ├── line_movement.py
│   │   └── copy_delay.py
│   │
│   ├── universes/
│   │   ├── __init__.py
│   │   └── library.py
│   │
│   └── viz/
│       ├── __init__.py
│       ├── leaderboard.py
│       ├── price_path.py
│       ├── calibration.py
│       ├── volume.py
│       └── copy_delay.py
│
├── ingestion/
│   ├── crontab.txt
│   └── README.md
│
├── data/                         # gitignored
│   ├── raw/
│   ├── normalized/
│   ├── marts/
│   ├── cache/
│   ├── manifest.duckdb
│   └── logs/
│
├── notebooks/
│   ├── 01_market_discovery.ipynb
│   ├── 02_guru_ranking.ipynb
│   ├── 03_trader_profile.ipynb
│   ├── 04_copy_delay_simulation.ipynb
│   ├── 05_sports_line_movement.ipynb
│   └── scratch/
│
├── research/
│   └── 0001_<finding_slug>.md
│
└── tests/
    ├── conftest.py
    ├── fixtures/
    ├── test_clients_gamma.py
    ├── test_clients_data_api.py
    ├── test_clients_clob.py
    ├── test_raw_storage.py
    ├── test_normalize_market.py
    ├── test_normalize_trade.py
    ├── test_normalize_orderbook.py
    ├── test_manifest.py
    ├── test_metrics_trader.py
    ├── test_copy_delay.py
    └── test_cli_smoke.py
```

---

## 5. Technology choices

| Concern | Choice | Notes |
|---|---|---|
| Language | Python 3.11+ | Use modern typing. |
| Package layout | `src/` layout | Avoid import shadowing. |
| CLI | Typer or argparse | Keep simple. |
| HTTP | `httpx` | Support retries and async later. |
| DataFrames | pandas | Primary research interface. |
| Parquet | pyarrow | Columnar storage. |
| Query engine | DuckDB | Query Parquet and manifest. |
| Config | pydantic-settings or TOML parser | Typed configuration preferred. |
| Logging | structlog or JSON logging | Structured ingestion logs. |
| Tests | pytest | Use recorded raw fixtures. |
| Linting | ruff | Keep formatting simple. |
| Type checks | mypy optional but configured | Do not block early experimentation if too slow. |

---

## 6. Configuration

Create `config.example.toml`:

```toml
[data]
root_dir = "data"
raw_dir = "data/raw"
normalized_dir = "data/normalized"
marts_dir = "data/marts"
cache_dir = "data/cache"
manifest_path = "data/manifest.duckdb"
logs_dir = "data/logs"

[api]
gamma_base_url = "https://gamma-api.polymarket.com"
data_api_base_url = "https://data-api.polymarket.com"
clob_base_url = "https://clob.polymarket.com"
timeout_s = 20
max_retries = 3
backoff_base_s = 0.5
max_concurrency = 8

[cache]
default_ttl_s = 300
market_ttl_s = 3600
leaderboard_ttl_s = 3600
orderbook_ttl_s = 30
price_history_ttl_s = 3600

[ingestion]
liquid_market_min_24h_volume_usd = "5000"
include_markets_resolving_within_days = 7
orderbook_snapshot_interval_s = 300
orderbook_max_concurrency = 8
orderbook_per_market_timeout_s = 15

[logging]
level = "INFO"
json = true
```

Rules:

- Secrets are not needed for v1.
- No private keys.
- No authenticated trading credentials.
- Environment variables may override paths and API base URLs.

---

## 7. Required API clients

Implement only public/read-only methods.

### 7.1 `data/clients/base.py`

Create shared HTTP infrastructure.

Required behavior:

- use `httpx`,
- support timeout,
- support retries with exponential backoff and jitter,
- support concurrency caps for batch calls,
- map non-2xx responses to structured errors,
- never crash an entire batch because one entity failed,
- attach request metadata to every raw response.

Suggested models:

```python
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

@dataclass(frozen=True)
class ApiRequest:
    source: str
    endpoint: str
    params_hash: str
    params: dict[str, Any]
    requested_at: datetime

@dataclass(frozen=True)
class ApiResponse:
    request: ApiRequest
    status_code: int
    payload: Any
    received_at: datetime
    raw_path: Path | None = None

class ApiError(Exception):
    def __init__(self, source: str, endpoint: str, status_code: int | None, message: str): ...
```

### 7.2 `data/clients/gamma.py`

Required methods:

```python
class GammaClient:
    def list_markets(self, *, limit: int = 100, offset: int = 0, **params) -> list[dict]: ...
    def get_market(self, market_id: str | int) -> dict: ...
    def list_events(self, *, limit: int = 100, offset: int = 0, **params) -> list[dict]: ...
    def get_event(self, event_id: str | int) -> dict: ...
    def list_tags(self) -> list[dict]: ...
    def list_sports(self) -> list[dict]: ...
```

### 7.3 `data/clients/data_api.py`

Required methods:

```python
class DataApiClient:
    def get_trades(
        self,
        *,
        user: str | None = None,
        market: str | None = None,
        event_id: int | None = None,
        side: str | None = None,
        limit: int = 500,
        offset: int = 0,
    ) -> list[dict]: ...

    def get_positions(self, user: str) -> list[dict]: ...
    def get_closed_positions(self, user: str) -> list[dict]: ...

    def get_leaderboard(
        self,
        *,
        category: str = "OVERALL",
        time_period: str = "MONTH",
        order_by: str = "PNL",
        limit: int = 50,
        offset: int = 0,
    ) -> list[dict]: ...
```

Validation rules:

- `category` must be one of known Polymarket leaderboard categories.
- `time_period` must be one of `DAY`, `WEEK`, `MONTH`, `ALL`.
- `order_by` must be one of `PNL`, `VOL`.
- `limit` must be capped to the endpoint maximum if needed.

### 7.4 `data/clients/clob.py`

Required methods:

```python
class ClobClient:
    def get_book(self, token_id: str) -> dict: ...
    def get_books(self, token_ids: list[str]) -> list[dict]: ...
    def get_price(self, token_id: str, side: str | None = None) -> dict: ...
    def get_prices(self, token_ids: list[str]) -> list[dict]: ...
    def get_midpoint(self, token_id: str) -> dict: ...
    def get_spread(self, token_id: str) -> dict: ...

    def get_price_history(
        self,
        token_id: str,
        *,
        start_ts: int | None = None,
        end_ts: int | None = None,
        interval: str = "1h",
        fidelity: int | None = None,
    ) -> dict: ...

    def get_batch_price_history(
        self,
        token_ids: list[str],
        *,
        start_ts: int,
        end_ts: int,
        interval: str = "1h",
        fidelity: int | None = None,
    ) -> dict: ...
```

Do not implement order placement or authenticated trading endpoints.

---

## 8. Storage contract

### 8.1 Raw storage

Raw storage is permanent evidence.

Path format:

```text
data/raw/{source}/{endpoint}/date=YYYY-MM-DD/{timestamp}_{params_hash}.json
```

Example:

```text
data/raw/data_api/trades/date=2026-04-30/20260430T081530Z_user_0xabc_limit_500.json
```

Raw file schema:

```json
{
  "schema_version": 1,
  "source": "data_api",
  "endpoint": "/trades",
  "params": {},
  "requested_at": "2026-04-30T08:15:30Z",
  "received_at": "2026-04-30T08:15:31Z",
  "status_code": 200,
  "payload": {}
}
```

Rules:

- Append-only.
- Never overwrite existing raw files.
- Every network response used for research must have a raw file.
- Raw files are the replay source for tests and normalization.
- Raw files may contain failed API responses if useful for debugging, but failed responses must not produce normalized rows.

### 8.2 HTTP cache

The HTTP cache is only an optimization.

Path:

```text
data/cache/http/{source}/{endpoint}/{params_hash}.json
```

Rules:

- TTL-based.
- Safe to delete.
- Not research evidence.
- A cache hit should not automatically create new raw evidence unless the caller requests raw persistence.

### 8.3 Normalized storage

Use Parquet directory datasets:

```text
data/normalized/markets/
data/normalized/events/
data/normalized/trades/
data/normalized/price_points/
data/normalized/orderbook_snapshots/
data/normalized/leaderboard_snapshots/
```

Avoid single giant files where append/update behavior becomes painful.

### 8.4 Marts

Use Parquet directory datasets:

```text
data/marts/trader_daily/
data/marts/market_daily/
data/marts/event_type_daily/
data/marts/trader_summary/
data/marts/market_summary/
```

Marts must be rebuildable from normalized data.

### 8.5 DuckDB manifest

Create `data/manifest.duckdb`.

Required tables:

```sql
CREATE TABLE IF NOT EXISTS raw_files (
    raw_path TEXT PRIMARY KEY,
    source TEXT NOT NULL,
    endpoint TEXT NOT NULL,
    params_hash TEXT NOT NULL,
    requested_at TIMESTAMP NOT NULL,
    received_at TIMESTAMP NOT NULL,
    status_code INTEGER NOT NULL,
    normalized BOOLEAN DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS normalized_partitions (
    table_name TEXT NOT NULL,
    partition_path TEXT NOT NULL,
    schema_version INTEGER NOT NULL,
    min_ts TIMESTAMP,
    max_ts TIMESTAMP,
    row_count INTEGER,
    created_at TIMESTAMP NOT NULL,
    PRIMARY KEY (table_name, partition_path)
);

CREATE TABLE IF NOT EXISTS data_gaps (
    gap_id TEXT PRIMARY KEY,
    table_name TEXT NOT NULL,
    entity_id TEXT,
    start_ts TIMESTAMP,
    end_ts TIMESTAMP,
    reason TEXT NOT NULL,
    detected_at TIMESTAMP NOT NULL
);
```

---

## 9. Canonical schemas

Use Pydantic models or dataclasses for validation. DataFrame columns must remain stable.

All normalized rows must include:

```text
source
raw_path
ingested_at
schema_version
```

### 9.1 Market schema

Required columns:

```text
market_id: string
condition_id: string
question: string
market_slug: string
event_id: string | null
event_slug: string | null
category: string | null
tags: list[string]
outcomes: list[string]
token_ids: list[string]
outcome_token_map: dict[string, string]
start_time: datetime | null
end_time: datetime | null
resolution_time: datetime | null
resolved: bool
winning_outcome: string | null
volume: Decimal | null
liquidity: Decimal | null
active: bool | null
closed: bool | null
source: string
raw_path: string
ingested_at: datetime
schema_version: int
```

Important distinction:

```text
condition_id is not the same thing as token_id.
A binary market has multiple outcome tokens.
Most price/orderbook analysis must be token-level.
```

### 9.2 Trade schema

Required columns:

```text
tx_hash: string
timestamp: datetime
trader_wallet: string
condition_id: string
token_id: string
outcome: string
outcome_index: int | null
side: BUY | SELL
price: Decimal
size: Decimal
notional_usd: Decimal
market_slug: string | null
event_slug: string | null
title: string | null
source: string
raw_path: string
ingested_at: datetime
schema_version: int
```

### 9.3 PricePoint schema

Required columns:

```text
token_id: string
condition_id: string | null
timestamp: datetime
price: Decimal
price_kind: midpoint | last_trade | bid | ask | clob_history
source: clob_history | orderbook_snapshot | external
interval: string | null
raw_path: string
ingested_at: datetime
schema_version: int
```

### 9.4 OrderBookSnapshot schema

Required columns:

```text
token_id: string
condition_id: string
timestamp: datetime
best_bid: Decimal | null
best_ask: Decimal | null
midpoint: Decimal | null
spread: Decimal | null
last_trade_price: Decimal | null
bid_depth_total: Decimal
ask_depth_total: Decimal
depth_1pct_bid: Decimal
depth_1pct_ask: Decimal
depth_5pct_bid: Decimal
depth_5pct_ask: Decimal
bids_json: string
asks_json: string
min_order_size: Decimal | null
tick_size: Decimal | null
neg_risk: bool | null
source: string
raw_path: string
ingested_at: datetime
schema_version: int
```

### 9.5 Trader schema

Required columns:

```text
wallet: string
first_seen: datetime | null
last_seen: datetime | null
n_trades: int
n_markets: int
n_resolved_markets: int
total_volume: Decimal | null
categories: list[string]
source: string
raw_path: string | null
ingested_at: datetime
schema_version: int
```

### 9.6 Leaderboard snapshot schema

Required columns:

```text
snapshot_ts: datetime
wallet: string
rank: int | null
category: string
period: string
order_by: string
pnl: Decimal | null
volume: Decimal | null
raw_payload_json: string
source: string
raw_path: string
ingested_at: datetime
schema_version: int
```

---

## 10. Price semantics

Every metric must be explicit about which price it uses.

### 10.1 Price kinds

```text
observed price:
  midpoint or last trade; useful for descriptive analysis

tradable price:
  BUY uses best ask
  SELL uses best bid

estimated fill price:
  size-aware estimate using orderbook depth
```

### 10.2 Rules

- PnL-style calculations must not silently use midpoint.
- Midpoint is allowed only when explicitly requested.
- Copy-delay analysis should prefer bid/ask snapshots when available.
- If only price history is available, label the result as weaker evidence.

---

## 11. Metric semantics

Do not implement vague `pnl` or `roi` names in v1.

Use explicit names:

```python
estimated_realized_pnl(...)
estimated_mark_to_market_pnl(...)
entry_edge_vs_close(...)
entry_edge_vs_resolution(...)
official_leaderboard_pnl(...)
```

Rules:

- If using Polymarket leaderboard PnL, call it `official_leaderboard_pnl`.
- If reconstructing from trades, call it `estimated_*`.
- If comparing entry price to final result, call it `edge`, not PnL.
- Every metric docstring must state required inputs and price assumption.

### 11.1 MVP trader metrics

Implement first:

```text
official_leaderboard_pnl
trade_count
market_count
resolved_market_count
volume
avg_trade_size
median_trade_size
category_exposure
profit_concentration_proxy
entry_edge_vs_resolution
entry_edge_vs_close
copy_delay_slippage
```

### 11.2 Backlog trader metrics

Implement later:

```text
estimated_realized_pnl
estimated_mark_to_market_pnl
Sharpe-like daily series
max_drawdown
holding_time
fillable-size simulation
```

### 11.3 MVP market metrics

Implement first:

```text
closing_price_calibration
path_volatility
liquidity_profile
volume_profile
trader_concentration
```

### 11.4 Sports metrics

Implement in Phase 6:

```text
pre_game_price_movement
T-24h / T-6h / T-1h vs closing price
closing price vs realized outcome
liquidity around game start
favorite / underdog asymmetries
league-level patterns
```

---

## 12. Copy-delay simulation

Implement copy-delay v0 early, even before orderbook snapshots exist.

### 12.1 Function contract

```python
def simulate_copy_delay(
    trader_trades: pd.DataFrame,
    price_points: pd.DataFrame,
    delays: list[pd.Timedelta],
    price_kind: Literal["last_trade", "midpoint", "ask", "bid"] = "last_trade",
) -> pd.DataFrame:
    ...
```

### 12.2 Output columns

```text
tx_hash
trader_wallet
condition_id
token_id
side
guru_trade_ts
guru_price
delay
copy_ts
copy_price
price_delta
worse_than_guru: bool
missing_price: bool
price_evidence: orderbook_snapshot | clob_history | missing
```

### 12.3 Price selection rules

- If side is BUY and orderbook snapshots exist, use best ask.
- If side is SELL and orderbook snapshots exist, use best bid.
- If orderbook snapshots do not exist, use CLOB price history and set `price_evidence = clob_history`.
- If no price exists near `copy_ts`, set `missing_price = true` and do not invent a price.

---

## 13. Dataset access functions

Implement high-level dataset access in `data/datasets/`.

### 13.1 `data/datasets/markets.py`

Required functions:

```python
def list_markets(
    *,
    status: str | None = None,
    tag: str | None = None,
    category: str | None = None,
    min_volume: Decimal | None = None,
    start: datetime | None = None,
    end: datetime | None = None,
    source: str = "official",
) -> pd.DataFrame: ...


def metadata(market_id: str, *, source: str = "official") -> pd.DataFrame: ...


def trades(market_id: str, *, source: str = "official") -> pd.DataFrame: ...


def price_history(
    token_id: str,
    *,
    start: datetime | None = None,
    end: datetime | None = None,
    price_kind: str = "clob_history",
    source: str = "official",
) -> pd.DataFrame: ...


def orderbook_history(
    token_id: str,
    *,
    start: datetime | None = None,
    end: datetime | None = None,
) -> pd.DataFrame: ...
```

### 13.2 `data/datasets/traders.py`

Required functions:

```python
def leaderboard(
    *,
    category: str = "OVERALL",
    period: str = "MONTH",
    order_by: str = "PNL",
    top_n: int = 50,
    source: str = "official",
) -> pd.DataFrame: ...


def trades(
    wallet: str,
    *,
    start: datetime | None = None,
    end: datetime | None = None,
    source: str = "official",
) -> pd.DataFrame: ...


def positions(wallet: str, *, source: str = "official") -> pd.DataFrame: ...


def closed_positions(wallet: str, *, source: str = "official") -> pd.DataFrame: ...
```

### 13.3 `data/datasets/snapshots.py`

Required functions:

```python
def list_snapshots(
    *,
    kind: str,
    start: datetime | None = None,
    end: datetime | None = None,
) -> pd.DataFrame: ...


def health() -> pd.DataFrame: ...
```

---

## 14. Analysis layer contracts

### 14.1 Trader ranking

File: `analysis/rank_traders.py`

```python
def rank_traders(
    *,
    universe: str,
    category: str = "OVERALL",
    period: str = "MONTH",
    min_trades: int = 20,
    min_markets: int = 5,
    metric: str = "copyability_v0",
) -> pd.DataFrame: ...
```

Output should include:

```text
wallet
rank_source
official_leaderboard_pnl
volume
trade_count
market_count
category_exposure
profit_concentration_proxy
copy_delay_summary
copyability_label: copyable | not_copyable | inconclusive
notes
```

Do not pretend the label is final truth. It is a research classification.

### 14.2 Market comparison

File: `analysis/compare_markets.py`

```python
def compare_markets(market_ids: list[str], *, dimension: str) -> pd.DataFrame: ...
```

### 14.3 Line movement

File: `analysis/line_movement.py`

```python
def line_movement(
    token_id: str,
    *,
    anchor_ts: datetime,
    windows: list[str] = ["24h", "6h", "1h"],
) -> pd.DataFrame: ...
```

### 14.4 Copy delay

File: `analysis/copy_delay.py`

```python
def trader_copy_delay_report(
    wallet: str,
    *,
    delays: list[str] = ["1m", "5m", "30m", "2h"],
) -> pd.DataFrame: ...
```

---

## 15. Visualization layer

Create one function per reusable chart.

Required functions:

```python
viz.leaderboard(df: pd.DataFrame) -> Any
viz.price_path(df: pd.DataFrame) -> Any
viz.calibration_plot(df: pd.DataFrame) -> Any
viz.volume_profile(df: pd.DataFrame) -> Any
viz.copy_delay_curve(df: pd.DataFrame) -> Any
```

Rules:

- Use Plotly by default.
- Return figure objects; do not call `.show()` inside library functions unless explicitly requested.
- Notebooks decide display.

---

## 16. CLI commands

Implement these commands.

```bash
pmi version

pmi fetch markets --limit 100 --save-raw
pmi fetch trades --user 0x... --limit 500 --save-raw
pmi fetch leaderboard --category OVERALL --period MONTH --order-by PNL --save-raw
pmi fetch book --token-id <token_id> --save-raw

pmi normalize raw --source all
pmi build marts

pmi ingest metadata
pmi ingest leaderboard
pmi ingest orderbook --once
pmi ingest orderbook --loop

pmi snapshots health
pmi storage report
```

Every command must:

- return exit code 0 on success,
- print a compact human-readable summary,
- write structured JSON logs,
- avoid secrets,
- avoid trading operations.

---

## 17. Ingestion jobs

### 17.1 Metadata job

File: `ingestion/metadata_job.py`

Purpose:

- daily snapshot of markets and events,
- save raw API responses,
- normalize market rows,
- update manifest.

### 17.2 Leaderboard job

File: `ingestion/leaderboard_job.py`

Purpose:

- daily snapshot of leaderboard categories and periods,
- save raw API responses,
- normalize leaderboard rows,
- update manifest.

### 17.3 Liquid market universe

File: `ingestion/universe.py`

Default liquid market definition:

```text
rolling 24h volume >= $5,000
OR market resolves within next 7 days
```

This must be configurable.

### 17.4 Orderbook job

File: `ingestion/orderbook_job.py`

Purpose:

- every 5 minutes, fetch order books for liquid markets,
- compute best bid, best ask, spread, midpoint, depth,
- append normalized orderbook snapshots,
- update manifest,
- log gaps and failures.

Required behavior:

- max concurrency,
- per-market timeout,
- retries with jitter,
- partial success allowed,
- one market failure does not abort the batch,
- record data gaps for failures,
- request spreading across the 5-minute window if needed.

---

## 18. Research cards

Every meaningful finding must produce a Markdown research card in `research/`.

Template:

```markdown
# Finding: <title>

## Question

## Dataset

- Source:
- Period:
- Inclusion criteria:
- Exclusions:
- Known gaps:

## Method

## Result

## Trading implication

## Failure modes

## Confidence

## Next step
```

Research cards are the formal output of this project.

---

## 19. Strategy-spec handoff template

Do not implement trading strategies in this project. But once a research card looks promising, create a strategy-spec handoff.

Template:

```markdown
# Strategy hypothesis

## Source research card

## Market universe

## Signal definition

## Required data

## Entry rule

## Exit rule

## Risk assumptions

## Execution assumptions

## Failure modes

## Minimum backtest requirements

## Live-trading blockers
```

---

## 20. Testing requirements

### 20.1 Test files

Minimum test suite:

```text
tests/test_clients_gamma.py
tests/test_clients_data_api.py
tests/test_clients_clob.py
tests/test_raw_storage.py
tests/test_normalize_market.py
tests/test_normalize_trade.py
tests/test_normalize_orderbook.py
tests/test_manifest.py
tests/test_metrics_trader.py
tests/test_copy_delay.py
tests/test_cli_smoke.py
```

### 20.2 Fixture rules

- Use recorded raw fixtures.
- Do not rely on live API calls in unit tests.
- Place fixtures under `tests/fixtures/`.
- Include at least one fixture per API family.

### 20.3 Acceptance tests

The implementation is not complete until these pass:

1. Fetch one leaderboard page → raw file exists → normalize leaderboard rows.
2. Fetch one user trade page → raw file exists → normalize trade rows.
3. Fetch one orderbook → raw file exists → normalized snapshot has best bid, best ask, and spread.
4. Run copy-delay simulation on fixture → expected delayed price selected.
5. Run `pmi storage report` on temp data directory → returns counts.
6. Run a notebook-equivalent analysis function → no raw API calls.
7. Run `pmi snapshots health` on fixture gaps → reports expected missing intervals.

---

## 21. Implementation phases

### Phase 0 — Foundation

Goal: installable package.

Deliverables:

```text
pyproject.toml
src/ layout
config.example.toml
pmi CLI
logging setup
pytest/ruff/mypy config
README quickstart
```

Done when:

```bash
pip install -e ".[dev]"
pmi version
pytest
ruff check .
```

---

### Phase 1 — API clients and raw storage

Goal: prove public API access and raw persistence.

Deliverables:

```text
GammaClient
DataApiClient
ClobClient
RawStore
HttpCache
recorded fixtures
```

Done when:

```bash
pmi fetch markets --limit 10 --save-raw
pmi fetch leaderboard --category OVERALL --period MONTH --save-raw
pmi fetch trades --user 0x... --limit 100 --save-raw
pmi fetch book --token-id ... --save-raw
```

No trading/auth endpoints are allowed.

---

### Phase 2 — Normalization and DuckDB manifest

Goal: turn raw data into stable research tables.

Deliverables:

```text
Market normalizer
Trade normalizer
OrderBookSnapshot normalizer
PricePoint normalizer
LeaderboardSnapshot normalizer
DuckDB manifest
Parquet writers/readers
```

Done when:

```bash
pmi normalize raw --source all
pmi storage report
```

The storage report must show raw file counts, normalized row counts, partitions, and gaps.

---

### Phase 3 — Guru ranking MVP

Goal: explain one trader's behavior without depending on private snapshots yet.

Deliverables:

```text
leaderboard dataset
trader trades dataset
market metadata join
initial trader metrics
copy-delay v0 using CLOB price history
notebook 01_market_discovery
notebook 02_guru_ranking
research card 0001
```

Done when one leaderboard trader can be analyzed with:

```text
trades
markets traded
category exposure
entry edge vs resolution or close where available
copy-delay v0
copyable / not copyable / inconclusive label
research card
```

This is the first real milestone.

---

### Phase 4 — Daily snapshots

Goal: begin building private historical archive.

Deliverables:

```text
metadata daily job
leaderboard daily job
snapshot health command
cron docs
```

Done when:

```text
3 successful daily runs
pmi snapshots health reports no unexpected gaps
```

---

### Phase 5 — Orderbook snapshots

Goal: capture depth and spread history.

Deliverables:

```text
liquid market universe selector
orderbook snapshot job
concurrency cap
retry/backoff
partial failure handling
gap log
orderbook_history dataset
```

Done when:

```text
pmi ingest orderbook --once succeeds
pmi ingest orderbook --loop works
orderbook snapshots are queryable through data.markets.orderbook_history(...)
```

Production-confidence target:

```text
one week of continuous operation without manual repair
```

---

### Phase 6 — Better copyability and sports analysis

Goal: use the orderbook archive to improve research quality.

Deliverables:

```text
copy-delay v1 using bid/ask snapshots
fillable price simulation
sports line movement notebook
liquidity profile metrics
volume profile metrics
2-3 research cards
```

Done when:

```text
one guru is classified as copyable / not copyable with evidence
one sports market pattern is supported or rejected with evidence
```

---

### Phase 7 — Handoff format

Goal: convert findings into strategy-ready specs without implementing trading.

Deliverables:

```text
research-card template
strategy-spec template
export folder
README explaining handoff process
```

Done when:

```text
a research card can be converted into a strategy-spec markdown file
without adding trading code to this project
```

---

## 22. First milestone definition

The first concrete milestone is:

> Given one leaderboard trader, Polymarket Insight can fetch their trades, join market metadata, estimate basic edge and copy-delay, and produce a research card explaining whether the trader is copyable, not copyable, or inconclusive.

This milestone is more important than building many dashboards or many metrics.

---

## 23. Version 1.0 success criteria

The project reaches v1.0 when:

- the first guru copyability research card exists,
- market discovery notebook runs end-to-end,
- guru ranking notebook runs end-to-end,
- copy-delay notebook runs end-to-end,
- sports line movement notebook runs end-to-end,
- daily metadata and leaderboard snapshots are working,
- orderbook snapshots are working,
- `pmi snapshots health` and `pmi storage report` are useful,
- at least 3 research cards exist,
- a new simple research question can be answered in under 15 minutes using existing functions,
- a new metric can be added with tests in under 2 hours.

---

## 24. Agent implementation priorities

When in doubt, optimize for:

1. correctness of raw data preservation,
2. stable schemas,
3. simple CLI commands,
4. testable pure metrics,
5. thin notebooks,
6. honest uncertainty in research outputs.

Do not optimize early for:

- fancy UI,
- complex backtesting,
- live streaming,
- trading integration,
- premature composite scores,
- too many metrics before the first research card.

---

## 25. Final instruction to the code agent

Build the project in phases. Do not skip raw storage, schema tests, or fixture tests. Do not implement trading endpoints. Do not hide uncertainty in metrics. The goal is not to prove that a strategy works; the goal is to build a reliable research substrate that can distinguish real evidence from intuition.

