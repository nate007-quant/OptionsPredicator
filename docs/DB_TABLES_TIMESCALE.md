# TimescaleDB Tables (Phase 1/2 + Debit Spreads)

This file documents the Timescale/Postgres tables created/used by the ingestion + ML-ready dataset builders.

---

## Phase 1: Raw chain hypertable

Schema: `spx`

Table:
- `spx.option_chain`

Primary key:
- `(snapshot_ts, option_symbol)`

Notes:
- `snapshot_ts` is derived from filename local time (America/Chicago) and stored as `timestamptz` (UTC internally).
- File handling supports read-only input directory via `ARCHIVE_ROOT/state/processed.log`.

---

## Phase 2: 0DTE features + labels

### Features
- `spx.chain_features_0dte`
- Grain: one row per snapshot (`snapshot_ts`) for 0DTE only.

### Labels
- `spx.chain_labels_0dte`
- Grain: one row per `(snapshot_ts, horizon_minutes)`.

Label matching rule:
- For `t + H`, pick the first `snapshot_ts >= target_ts` within `MAX_FUTURE_LOOKAHEAD_MINUTES`.

---

## Debit spreads: GEX levels + candidates + labels

### GEX levels
- `spx.gex_levels_0dte`
- Columns: `call_wall`, `put_wall`, `magnet`

Definitions (MVP, deterministic):
- `call_gex(k) = sum(gamma_call * OI_call)` per strike
- `put_gex(k)  = sum(gamma_put  * OI_put)` per strike
- `CALL_WALL = argmax(call_gex(k))`
- `PUT_WALL  = argmax(put_gex(k))`
- `MAGNET    = argmax(abs(call_gex(k)) + abs(put_gex(k)))`

### Candidates
- `spx.debit_spread_candidates_0dte`
- PK: `(snapshot_ts, anchor_type, spread_type)`
- `anchor_type ∈ {ATM, CALL_WALL, PUT_WALL, MAGNET}`
- `spread_type ∈ {CALL, PUT}`

Tradable rule:
- `0 < debit_points <= MAX_DEBIT_POINTS` (default 5.0)

### Labels (realized outcomes)
- `spx.debit_spread_labels_0dte`
- PK: `(snapshot_ts, horizon_minutes, anchor_type, spread_type)`
- `change = debit_tH - debit_t` (mid-based)

---

## Debit spread scores (ML)

- `spx.debit_spread_scores_0dte`
- PK: `(snapshot_ts, horizon_minutes, anchor_type, spread_type)`

Columns:
- `pred_change`: regression prediction for expected `change`.
- `p_bigwin`: probability of achieving configured big-win multiple by horizon.

Big-win rule:
- ATM: `debit_tH >= DEBIT_BIGWIN_MULT_ATM * debit_t` (default 2x)
- Walls/Magnet: `debit_tH >= DEBIT_BIGWIN_MULT_WALL * debit_t` (default 4x)
