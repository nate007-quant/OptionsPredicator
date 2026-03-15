#!/usr/bin/env bash
set -euo pipefail

ROOT="/opt/OptionsPredicator"
ENV_FILE="$ROOT/.env"
PY="$ROOT/.venv/bin/python"
MIG="$ROOT/scripts/migrate_sqlite_to_postgres_state.py"
PARITY="$ROOT/scripts/pg_state_parity_check.py"
PG_URL="${1:-postgresql://spx:spxpass@localhost:5433/spxdb}"
SCHEMA="${2:-spx}"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "ERROR: $ENV_FILE not found"
  exit 1
fi
if [[ ! -x "$PY" ]]; then
  echo "ERROR: python venv not found at $PY"
  exit 1
fi

TS="$(date -u +%Y%m%dT%H%M%SZ)"
cp "$ENV_FILE" "$ENV_FILE.backup.$TS"
echo "Backed up env to $ENV_FILE.backup.$TS"

if grep -q '^TRADING_ENABLED=' "$ENV_FILE"; then
  sed -i 's/^TRADING_ENABLED=.*/TRADING_ENABLED=false/' "$ENV_FILE"
else
  printf '\nTRADING_ENABLED=false\n' >> "$ENV_FILE"
fi
echo "Set TRADING_ENABLED=false"

echo "Stopping services..."
sudo systemctl stop options_ai_execution.service || true
sudo systemctl stop options_ai_execution_monitor.service || true
sudo systemctl stop options_ai_risk_guard.service || true
sudo systemctl stop options_ai_dashboard_api.service || true

echo "Service states after stop:"
for s in options_ai_execution.service options_ai_execution_monitor.service options_ai_risk_guard.service options_ai_dashboard_api.service; do
  st="$(systemctl is-active "$s" 2>/dev/null || true)"
  echo "  $s => $st"
done

echo "Running final incremental migration sync..."
"$PY" "$MIG" --pg "$PG_URL" --schema "$SCHEMA" --apply

echo "Running strict parity check..."
"$PY" "$PARITY" --pg "$PG_URL" --schema "$SCHEMA" --strict

echo "Freeze/sync/parity completed successfully."
echo "You can now proceed with runtime code cutover + restart in controlled order."
