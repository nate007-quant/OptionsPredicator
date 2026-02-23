# OptionsPredicator — SPX Options AI Prediction System (v1.3)

File-driven, local daemon that watches for new **SPX** snapshot JSON files and produces a **15-minute directional prediction** (bullish/bearish/neutral). No live market-data APIs and no broker execution in v1.

## Key points

- **Input source of truth**: snapshot JSON files under `/mnt/options_ai/incoming/SPX/`.
- Optional chart images: `/mnt/options_ai/incoming/SPX/charts/`.
- Outputs (DB, logs, state, processed files) live under `/mnt/options_ai/` (never in the repo).
- Model: `gpt-5.2-codex` via `OPENAI_API_KEY`.

## Quick start

### 1) Create runtime directories (daemon also creates them)

```bash
sudo mkdir -p /mnt/options_ai
sudo chown -R $USER:$USER /mnt/options_ai
```

### 2) Python env

```bash
python -m venv .venv
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

See `SPEC_OPTIONS_AI_v1_3.txt` for the authoritative requirements.
