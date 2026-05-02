# Ingestion

Run jobs manually:

```bash
pmi ingest metadata
pmi ingest leaderboard
pmi ingest orderbook --once
pmi snapshots health
```

The jobs save raw responses first, normalize successful rows, update the DuckDB manifest, and record data gaps for partial failures.

Orderbook loop mode:

```bash
pmi ingest orderbook --loop
```
