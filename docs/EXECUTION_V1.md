# Execution V1 Runbook (Tastytrade)

Execution v1 adds four workers:

1. Intent builder (`options_ai.execution_intent_main`)
2. Executor (`options_ai.execution_main`)
3. Monitor (`options_ai.execution_monitor_main`)
4. Risk guard (`options_ai.execution_risk_guard_main`)

> Safety default: `TRADING_ENABLED=false`

---

## 1) Sandbox setup

Edit `/opt/OptionsPredicator/.env`:

```bash
TRADING_ENABLED=false
BROKER_NAME=tastytrade
BROKER_ENV=sandbox
TASTY_BASE_URL=https://api.cert.tastyworks.com
TASTY_STREAMER_URL=
TASTY_ACCOUNT_NUMBER=

# choose one auth method
TASTY_SESSION_TOKEN=
# or
TASTY_USERNAME=
TASTY_PASSWORD=

MAX_DAILY_LOSS_USD=300
SESSION_TZ=America/Chicago
FORCE_CLOSE_MINUTES_BEFORE_END=15

REPRICE_MAX_ATTEMPTS=3
REPRICE_STEP=0.05
REPRICE_INTERVAL_SECONDS=25
REPRICE_MAX_TOTAL_CONCESSION=0.15

EXECUTION_INTENT_POLL_SECONDS=30
EXECUTION_POLL_SECONDS=5
EXECUTION_MONITOR_POLL_SECONDS=10
EXECUTION_RISK_GUARD_POLL_SECONDS=15
```

Reload systemd definitions:

```bash
sudo systemctl daemon-reload
```

---

## 2) Install/enable services

```bash
sudo cp /opt/OptionsPredicator/systemd/options_ai_execution.service /etc/systemd/system/
sudo cp /opt/OptionsPredicator/systemd/options_ai_execution_monitor.service /etc/systemd/system/
sudo cp /opt/OptionsPredicator/systemd/options_ai_risk_guard.service /etc/systemd/system/
sudo systemctl daemon-reload

sudo systemctl enable --now options_ai_execution
sudo systemctl enable --now options_ai_execution_monitor
sudo systemctl enable --now options_ai_risk_guard
```

Health checks:

```bash
systemctl status options_ai_execution --no-pager
systemctl status options_ai_execution_monitor --no-pager
systemctl status options_ai_risk_guard --no-pager
```

---

## 3) Start/stop procedures

Start all:

```bash
sudo systemctl start options_ai_execution
sudo systemctl start options_ai_execution_monitor
sudo systemctl start options_ai_risk_guard
```

Stop all:

```bash
sudo systemctl stop options_ai_execution
sudo systemctl stop options_ai_execution_monitor
sudo systemctl stop options_ai_risk_guard
```

Tail logs:

```bash
journalctl -u options_ai_execution -f
journalctl -u options_ai_execution_monitor -f
journalctl -u options_ai_risk_guard -f
```

---

## 4) Operational checks (dashboard)

Execution tab endpoints:

- `GET /api/execution/intents`
- `GET /api/execution/trades/open`
- `GET /api/execution/trades/{id}`
- `GET/PUT /api/execution/reprice-policy`
- `GET /api/execution/risk-session`
- `POST /api/execution/kill-switch`

Expected behavior:

- one candidate source -> one idempotent intent
- executor transitions intent status and writes `order_events`
- monitor writes sync events and missing-protection alerts
- risk guard updates `risk_session_state`

---

## 5) Incident steps

### A) Kill switch (block new entries)

Via API/dashboard:

```bash
curl -s -X POST http://127.0.0.1:8088/api/execution/kill-switch \
  -H 'Content-Type: application/json' \
  -d '{"block_new_entries":true,"reason":"manual_kill_switch"}'
```

### B) Manual close / emergency flatten

1. Enable kill switch.
2. If needed, stop executor and monitor:

```bash
sudo systemctl stop options_ai_execution
sudo systemctl stop options_ai_execution_monitor
```

3. Use broker platform or scripted close endpoints to flatten positions.

### C) Resume after incident

1. Verify no unintended open positions.
2. Disable kill switch:

```bash
curl -s -X POST http://127.0.0.1:8088/api/execution/kill-switch \
  -H 'Content-Type: application/json' \
  -d '{"block_new_entries":false,"reason":"manual_unblock"}'
```

3. Start workers again.

---

## 6) Live migration checklist (later)

Do **not** switch to live until sandbox KPIs are stable:

- fill behavior matches expectation
- repricing policy tuned
- protective exits armed consistently
- daily loss gate blocks correctly
- force-close behavior verified

Then:

```bash
BROKER_ENV=live
TRADING_ENABLED=true
```

Restart workers and monitor closely.


## 7) Week-1 sandbox rollout plan

Run with:

```bash
BROKER_ENV=sandbox
TRADING_ENABLED=true
```

Use **small quantity only** and keep full logs.

Daily checks:

1. Fill rate
2. Reprice concession drift
3. TP/SL protection arm rate for open trades
4. Force-close reliability at session T-15m

Dashboard API helper for KPIs:

```bash
curl -s "http://127.0.0.1:8088/api/execution/kpis?days=7"
```

Recommended acceptance targets (tune over time):

- Fill rate stable and not collapsing vs baseline
- Avg reprice concession within expected range
- Protection rate for open trades near 100%
- Force-close events produce expected close counts

Only after stable sandbox results should live be considered.


## 8) Production enablement (when ready)

Keep these protections ON in live-prep mode:

```bash
PRETRADE_REQUIRED_CHECKS=true
REQUIRE_COMPLEX_EXIT_ORDERS=true
REQUIRE_BROKER_EXTERNAL_IDENTIFIER=true
STARTUP_RECONCILE_REQUIRED=true
STRICT_QUARANTINE_REQUIRES_OPERATOR_CLEAR=true
```

Two-step arm for live:

1) Configure live env but keep interlock engaged:

```bash
BROKER_ENV=live
TRADING_ENABLED=true
LIVE_ARMED=false
```

2) Run preflight (must pass):

```bash
python -m options_ai.execution_preflight_main
```

3) Only then arm live submission:

```bash
LIVE_ARMED=true
sudo systemctl restart options_ai_execution
```

If anything drifts, set:

```bash
LIVE_ARMED=false
```

and restart executor to immediately stop new live entries.
