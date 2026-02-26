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
