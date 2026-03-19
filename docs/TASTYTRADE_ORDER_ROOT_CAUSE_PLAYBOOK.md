# Tastytrade Order Opening — Root Cause Playbook

## What failed (confirmed)

During order submission, the client sent:

- `Authorization: <token>`

But tastytrade API docs require OAuth2 Bearer format:

- `Authorization: Bearer <token>`

This mismatch can produce repeated `401 Unauthorized` and block order entry.

## Code fix applied

File: `options_ai/brokers/tastytrade/client.py`

- Added `TastytradeClient._authorization_header_value()`
- `_headers()` now normalizes auth header to Bearer format
- Existing `Bearer ...` tokens are preserved as-is

## Validation suite

Run:

```bash
. .venv/bin/activate
python -m pytest -q \
  tests/test_tastytrade_client_dry_run.py \
  tests/test_tastytrade_auth_contract.py \
  tests/test_tastytrade_mapping.py
```

Coverage from this suite:

1. dry-run payload correctness for opening spread orders
2. auth header contract (bare token -> Bearer token)
3. auth header preservation when token already prefixed
4. spread leg and price mapping constraints

## Production triage checklist (when an entry fails)

1. **Inspect latest order event**
   - Query `order_events` where `event_type='executor_error'` or `event_type='submit_attempt'`
   - Capture `http_status`, `request_url`, `response_body`, `response_json`

2. **Classify quickly by HTTP code**
   - `401`: auth/session problem (expired or malformed token)
   - `403`: account permission/environment mismatch
   - `404/405`: wrong endpoint/path or environment base URL
   - `422`: payload shape/field validation issue
   - `429`: rate-limit (watch retries/backoff)
   - `5xx`: broker-side or transient infra issues

3. **Auth path checks**
   - Verify outbound header is `Authorization: Bearer <token>`
   - If using username/password mode, verify `/sessions` succeeds and `session-token` is extracted
   - Confirm sandbox/live base URL matches environment

4. **Payload checks**
   - Confirm `order-type`, `time-in-force`, `price`, `price-effect`, `legs[]`
   - Confirm leg actions map correctly (`Buy/Sell to Open/Close`)
   - Confirm symbols are tastytrade-formatted OCC symbols

5. **Execution-state checks**
   - Intent status path: `pending -> PRECHECK_PENDING -> submitting -> filled/working`
   - If status is `PRECHECK_FAILED`, inspect `precheck_payload_json`
   - If `QUARANTINED/BLOCKED`, inspect kill switch/risk session/close-only/live interlock

6. **Repro + isolate**
   - Re-run same DTO in `dry_run=True` (local mapping/path validation)
   - Then run one controlled sandbox live submit with a small size
   - Compare submit payload against previous failing payload

## Fast rollback / mitigation

If failures continue:

1. turn on kill switch (`block_new_entries=true`)
2. keep monitoring existing trades and protection
3. fix + rerun validation suite
4. resume with sandbox smoke submit before live
