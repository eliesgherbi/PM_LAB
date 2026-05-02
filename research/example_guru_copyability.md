# Finding: Example Guru Copyability Is Inconclusive

## Question

Can an official leaderboard trader be copied after a realistic delay?

## Dataset

- Source: local normalized leaderboard, trades, and price history fixtures
- Period: example
- Inclusion criteria: one leaderboard wallet
- Exclusions: live execution, private positions, wallet signing
- Known gaps: fixture data is small

## Method

Rank trader, inspect trades, and run copy-delay simulation over fixed delays.

## Result

The example remains inconclusive because liquidity and delayed price evidence are limited.

## Trading implication

No trading action. This is a research classification only.

## Failure modes

Sparse price history, missing orderbook depth, and stale leaderboard snapshots.

## Confidence

Low.

## Next step

Collect orderbook snapshots and rerun copy-delay v1.
