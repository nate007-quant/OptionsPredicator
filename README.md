# OptionsPredicator — SPX Options AI Prediction System (v2.3.x)

File-driven, local daemon that watches for new **SPX** snapshot JSON files and produces a **15-minute directional prediction** (bullish/bearish/neutral). No live market-data APIs and no broker execution.

## Baseline (current)

- **SPX only** (fails fast if `TICKER != SPX`)
- **File-driven** snapshots: `/mnt/options_ai/incoming/SPX/` (+ optional charts)
- Deterministic signals + **GEX structure** (call wall / put wall / magnet / flip)
- **Dual-model routing**
  - Remote (OAuth): `openai-codex/gpt-5.2`
  - Local fallback (OpenAI-compatible endpoint): `deepseek-r1:8b` (default)
- **Bootstrap backtest** on first run (empty DB): `/mnt/options_ai/historical/SPX/`
- **Multi-layer caching** (derived + model caches)
- **Structured JSONL logging + full error capture** under `/mnt/options_ai/logs/*.log`
- **Idempotent reprocessing (no crash)**: if a prediction already exists for
  `(source_snapshot_hash, prompt_version, model_used)`, ingest is a no-op and returns a duplicate skip.

## Quick start

### 1) Create runtime directories

```bash
sudo mkdir -p /mnt/options_ai
sudo chown -R $USER:$USER /mnt/options_ai
```

### 2) Python env

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
# edit .env
```

### 3) Run daemon

```bash
source .venv/bin/activate
python -m options_ai.main
```

### 4) Run tests

```bash
pytest -q
```

## Spec

The repository includes the original `SPEC_OPTIONS_AI_v1_3.txt`. The implementation has advanced beyond v1.3; see `.env.example` and current source for active knobs/behavior.

## SPX Options Chain → TimescaleDB ingestion (Rev3)

This repo also includes an **early-stage ingester** that watches a drop directory (default `/mnt/SPX`) for SPX options chain snapshot JSON files and upserts the chain into **Postgres + TimescaleDB**.

### Configure

Set these in `.env` (see `.env.example`):

- `INPUT_DIR=/mnt/SPX`
- `ARCHIVE_ROOT=/mnt/options_ai` (writable)
- `SPX_CHAIN_DATABASE_URL=postgresql://spx:spxpass@localhost:5432/spxdb`
- `FILENAME_TZ=America/Chicago`

### Run

```bash
source .venv/bin/activate
python -m options_ai.spx_chain_main
```

### File handling

- On successful DB commit: moves (or copies) JSON to `ARCHIVE_ROOT/archive/YYYYMMDD/<filename>`
- On parse/validation/DB error: moves (or copies) JSON to `ARCHIVE_ROOT/bad/<filename>` and writes `ARCHIVE_ROOT/bad/<filename>.error.txt`
- If the process cannot delete/rename files in `INPUT_DIR`, it will copy and record processed filenames in `ARCHIVE_ROOT/state/processed.log`.

### One-command stack + auto-start on reboot

TimescaleDB runs in Docker with `restart: unless-stopped`, and the ingester can be run under systemd.

**Bring up TimescaleDB (one command):**

```bash
docker compose -f docker-compose.timescale.yml up -d
```

**Then start the ingester:**

```bash
source .venv/bin/activate
python -m options_ai.spx_chain_main
```

**Auto-start on reboot (recommended):**

1) Install both systemd units:

```bash
sudo cp systemd/spx_chain_ingester.service /etc/systemd/system/spx_chain_ingester.service
sudo cp systemd/optionspredicator-stack.service /etc/systemd/system/optionspredicator-stack.service
sudo systemctl daemon-reload
```

2) Enable services:

```bash
sudo systemctl enable --now spx_chain_ingester
sudo systemctl enable --now optionspredicator-stack
```

`optionspredicator-stack` is a convenience unit that runs docker compose for TimescaleDB and then starts `spx_chain_ingester`.

## Phase 2 + Debit Spreads + ML (TimescaleDB pipeline)

This repo now includes an end-to-end **TimescaleDB-backed dataset builder** for:

- Phase 1: raw chain ingestion (`spx.option_chain`)
- Phase 2: 0DTE feature vectors + labels (`spx.chain_features_0dte`, `spx.chain_labels_0dte`)
- Debit spreads: multi-anchor candidates + labels + ML scores

Key docs:
- `TEST_PLANS_PHASE1_PHASE2.md` — rerunnable test plans (smoke tests + production checks)
- `docs/STACK_AND_SERVICES.md` — service inventory + boot behavior
- `docs/DB_TABLES_TIMESCALE.md` — Timescale table reference
- `CHANGELOG.md`

### Dashboard

Dashboard API/UI is served by FastAPI on port **8088** via:
- `systemd/options_ai_dashboard_api.service`

It includes a **Debit Spreads** view that shows:
- 0DTE levels (ATM / call wall / put wall / magnet)
- top **tradable** candidates
- ML scores:
  - `pred(30m)` expected change in debit points
  - `p(bigwin)` probability of hitting your configured win multiple
- recent realized outcomes (historical)


## Regime Detection and Routing (v2.9)

The ingest pipeline now computes a deterministic **regime_v1** label before ML feature generation and model inference.

### Regime labels

- `trend_up`
- `trend_down`
- `pin_mean_revert`
- `vol_expansion_breakout`
- `event_unstable`

### Inputs used (event-time safe)

- Existing model signals (`trend`, `volume_class`, expected move, IV, PCR, unusual activity, compact GEX regime + level distances)
- Session flags derived from `observed_utc` (open / lunch / power hour)
- Optional same-timestamp `spx.chain_features_0dte` fields (if Timescale is configured):
  `skew_25d`, `bf_25d`, `atm_bidask_spread`, `valid_mid_count`, `contract_count`, `low_quality`

No future rows or labels are used.

### Hysteresis

Regime switching uses a configurable hold rule:
- `REGIME_HYSTERESIS_SNAPSHOTS` controls minimum persistence needed before switching labels.

### ML routing

When `REGIME_ENABLED=true`:
- if `regime.confidence >= REGIME_MIN_CONFIDENCE_FOR_ROUTING` and mapped bundle exists, ML routes to `REGIME_MODEL_MAP_JSON[label]`
- otherwise it falls back to `REGIME_FALLBACK_MODEL_VERSION`

Routing reason is written to prediction row (`routing_reason`) with values like:
- `ml_regime_route`
- `ml_regime_fallback_low_confidence`
- `ml_regime_fallback_unmapped`
- `ml_regime_fallback_missing_bundle`

### Example regime payload

```json
{
  "label": "pin_mean_revert",
  "confidence": 0.67,
  "version": "regime_v1",
  "reasons": ["top_score=0.700", "runner_up=0.420"],
  "inputs_used": {
    "trend": "neutral",
    "volume_class": "normal",
    "expected_move_pct": 0.0042,
    "gex_regime_label": "positive_gamma",
    "session_flags": {
      "session_open": 0.0,
      "session_lunch": 1.0,
      "session_power_hour": 0.0
    }
  }
}
```

### Known limitations / next improvements

- No macro-event calendar in v1; scheduled data releases are only inferred indirectly via volatility/flow proxies.
- No explicit term-structure regime features yet (front/back IV term shape).
- Optional Timescale quality fields are best-effort; when unavailable, confidence is reduced conservatively.
