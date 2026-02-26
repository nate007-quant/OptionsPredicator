# Changelog

This project is under active development. Not all changes are versioned with formal releases yet.

## 2026-02-26

### SPX chain ingestion (Phase 1)
- Added near-real-time ingestion of SPX options-chain snapshot JSON files from a drop directory (e.g. `/mnt/SPX`) into Postgres/TimescaleDB.
- Archive + quarantine handling under `ARCHIVE_ROOT` with a read-only input-dir contingency via `state/processed.log`.

### Phase 2 (0DTE features + labels)
- Added 0DTE feature builder (`spx.chain_features_0dte`) and multi-horizon labels (`spx.chain_labels_0dte`).

### Multi-anchor debit spreads + labels
- Added deterministic GEX levels (`spx.gex_levels_0dte`) and multi-anchor adjacent-strike debit spread candidates (`spx.debit_spread_candidates_0dte`) + labels (`spx.debit_spread_labels_0dte`).

### Debit spread ML scoring
- Added ML scorer that trains on realized outcomes and scores latest tradable candidates.
- Scores table: `spx.debit_spread_scores_0dte` including:
  - `pred_change` (expected debit change)
  - `p_bigwin` (probability of hitting configured big-win multiple)

### Dashboard UI
- Added "Debit Spreads" view:
  - shows ATM / call wall / put wall / magnet levels
  - shows top tradable candidates sorted by ML scores
  - shows recent realized outcomes

### Test plans
- Added rerunnable smoke tests and a consolidated test plan document.
