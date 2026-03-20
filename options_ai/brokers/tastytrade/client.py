from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Literal
import os
import re
import time
import json
import shlex
import threading
from datetime import datetime, timezone

import httpx


PriceEffect = Literal["DEBIT", "CREDIT"]


@dataclass(frozen=True)
class OptionLeg:
    symbol: str
    quantity: int
    side: Literal["BUY", "SELL"]
    effect: Literal["OPEN", "CLOSE"] = "OPEN"


@dataclass(frozen=True)
class OrderDTO:
    account_number: str
    underlying: str
    quantity: int
    price_effect: PriceEffect
    limit_price: float
    legs: list[OptionLeg] = field(default_factory=list)
    time_in_force: str = "Day"
    order_type: str = "Limit"
    dry_run: bool = False
    client_order_id: str | None = None


class TastytradeMappingError(ValueError):
    pass


def _to_decimal(x: float | int | str | Decimal) -> Decimal:
    if isinstance(x, Decimal):
        return x
    return Decimal(str(x))


def normalize_limit_price(*, raw_price: float | int | str | Decimal, effect: PriceEffect) -> Decimal:
    """Normalize signed/unsigned limit prices into Tastytrade-compatible positive decimals.

    Internal callers may pass signed values (+ debit / - credit). We normalize to absolute
    and rely on `price_effect` to carry direction semantics.
    """
    p = _to_decimal(raw_price)
    if p == 0:
        raise TastytradeMappingError("limit price cannot be zero")

    # Convert sign conventions to absolute price + explicit effect
    # (effect is authoritative here)
    p = abs(p)

    # 1 cent precision by default for option spreads.
    p = p.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    if p <= 0:
        raise TastytradeMappingError("normalized limit price must be > 0")
    return p


def validate_spread_legs(legs: list[OptionLeg]) -> None:
    if not isinstance(legs, list) or len(legs) < 2:
        raise TastytradeMappingError("spread must include at least 2 legs")

    buy = 0
    sell = 0
    symbols: set[str] = set()

    for leg in legs:
        sym = str(leg.symbol or "").strip().upper()
        if not sym:
            raise TastytradeMappingError("leg symbol required")
        if sym in symbols:
            raise TastytradeMappingError(f"duplicate leg symbol: {sym}")
        symbols.add(sym)

        qty = int(leg.quantity)
        if qty <= 0:
            raise TastytradeMappingError("leg quantity must be > 0")

        if leg.side == "BUY":
            buy += 1
        elif leg.side == "SELL":
            sell += 1
        else:
            raise TastytradeMappingError(f"invalid leg side: {leg.side}")

    if buy == 0 or sell == 0:
        raise TastytradeMappingError("spread must include both BUY and SELL legs")


def _tasty_action(side: str, effect: str) -> str:
    side_u = str(side).upper()
    eff_u = str(effect).upper()
    if side_u == "BUY" and eff_u == "OPEN":
        return "Buy to Open"
    if side_u == "SELL" and eff_u == "OPEN":
        return "Sell to Open"
    if side_u == "BUY" and eff_u == "CLOSE":
        return "Buy to Close"
    if side_u == "SELL" and eff_u == "CLOSE":
        return "Sell to Close"
    raise TastytradeMappingError(f"invalid side/effect combo: {side}/{effect}")


def map_order_dto_to_tasty_payload(dto: OrderDTO) -> dict[str, Any]:
    validate_spread_legs(dto.legs)
    px = normalize_limit_price(raw_price=dto.limit_price, effect=dto.price_effect)

    pe = "Debit" if str(dto.price_effect).upper() == "DEBIT" else "Credit"

    legs_payload: list[dict[str, Any]] = []
    for leg in dto.legs:
        legs_payload.append(
            {
                "instrument-type": "Equity Option",
                "symbol": normalize_option_symbol_for_tasty(str(leg.symbol)),
                "quantity": int(leg.quantity),
                "action": _tasty_action(leg.side, leg.effect),
            }
        )

    payload: dict[str, Any] = {
        "order-type": dto.order_type,
        "time-in-force": dto.time_in_force,
        "price": f"{px:.2f}",
        "price-effect": pe,
        "legs": legs_payload,
    }
    if dto.client_order_id:
        payload["client-order-id"] = str(dto.client_order_id)
    return payload


def normalize_option_symbol_for_tasty(symbol: str) -> str:
    """Convert compact OCC symbols to Tastytrade padded-root format.

    Example:
      SPXW260311P06775000 -> SPXW  260311P06775000
    """
    sym = str(symbol or '').strip().upper()
    if not sym:
        return sym
    # Already padded/root-separated
    m = re.match(r'^([A-Z ]{1,6})(\d{6})([CP])(\d{8})$', sym)
    if m:
        root = m.group(1).strip().ljust(6)
        return f"{root}{m.group(2)}{m.group(3)}{m.group(4)}"
    # Compact OCC (root + date + C/P + strike)
    m = re.match(r'^([A-Z]{1,6})(\d{6})([CP])(\d{8})$', sym)
    if m:
        root = m.group(1).ljust(6)
        return f"{root}{m.group(2)}{m.group(3)}{m.group(4)}"
    return sym


def _looks_placeholder(v: str | None) -> bool:
    t = str(v or '').strip().lower()
    if not t:
        return True
    bad = {'changeme','change_me','your_username','your_password','username','password','token','your_token'}
    if t in bad:
        return True
    if '<' in t or '>' in t:
        return True
    if 'example' in t or 'placeholder' in t:
        return True
    return False


class TastytradeClient:
    """Minimal Tastytrade REST adapter with dry-run support.

    Auth notes:
      - If `session_token` is provided, it is used as OAuth2 bearer token.
      - Else if username/password are available, client can call `authenticate()`.
    """

    def __init__(
        self,
        *,
        base_url: str | None = None,
        streamer_url: str | None = None,
        environment: str = "sandbox",
        session_token: str | None = None,
        username: str | None = None,
        password: str | None = None,
        account_number: str | None = None,
        timeout_seconds: int = 20,
        dry_run: bool = True,
        target_api_version: str | None = None,
        http_max_retries: int = 3,
        http_backoff_seconds: float = 0.5,
    ) -> None:
        self.environment = str(environment or "sandbox").lower()
        env_base = None
        if self.environment == 'live':
            env_base = os.getenv('TASTY_LIVE_BASE_URL')
        else:
            env_base = os.getenv('TASTY_SANDBOX_BASE_URL')
        self.base_url = (base_url or env_base or os.getenv("TASTY_BASE_URL") or ("https://api.tastyworks.com" if self.environment == 'live' else "https://api.cert.tastyworks.com")).rstrip("/")
        self.streamer_url = (streamer_url or os.getenv("TASTY_STREAMER_URL") or "").strip()
        self.session_token = (session_token or os.getenv("TASTY_SESSION_TOKEN") or "").strip() or None
        self.auth_scheme = str(os.getenv('TASTY_AUTH_SCHEME') or ('raw' if self.session_token else 'bearer')).strip().lower()
        self.username = (username or os.getenv("TASTY_USERNAME") or "").strip() or None
        self.password = (password or os.getenv("TASTY_PASSWORD") or "").strip() or None
        self.account_number = (account_number or os.getenv("TASTY_ACCOUNT_NUMBER") or "").strip() or None
        self.timeout_seconds = int(timeout_seconds)
        self.dry_run = bool(dry_run)
        self.target_api_version = (target_api_version or os.getenv("TARGET_API_VERSION") or "").strip() or None
        self.use_accept_version = str(os.getenv('TASTY_USE_ACCEPT_VERSION','0')).strip().lower() in {'1','true','yes','on'}
        self.http_max_retries = max(0, int(http_max_retries))
        self.http_backoff_seconds = max(0.0, float(http_backoff_seconds))
        self.verbatim_log_path = (os.getenv('TASTY_VERBATIM_LOG_PATH') or '/mnt/options_ai/logs/tasty_verbatim.log').strip()
        self.verbatim_log_enabled = str(os.getenv('TASTY_VERBATIM_LOG_ENABLED', '1')).strip().lower() not in {'0','false','no','off'}
        self.intent_logs_dir = (os.getenv('TASTY_INTENT_LOGS_DIR') or '/mnt/options_ai/logs/intents').strip()
        self._ctx = threading.local()

    @staticmethod
    def _authorization_header_value(token: str | None, scheme: str = "bearer") -> str | None:
        """Normalize token to docs-compliant OAuth2 Bearer header value.

        tastytrade API overview specifies:
          Authorization: Bearer <access_token>
        """
        t = str(token or "").strip()
        if not t:
            return None
        sc = str(scheme or "bearer").strip().lower()
        if sc == "raw":
            if t.lower().startswith("bearer "):
                return t.split(" ",1)[1].strip()
            return t
        if t.lower().startswith("bearer "):
            return t
        return f"Bearer {t}"

    def _headers(self, *, include_version: bool = True) -> dict[str, str]:
        h = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        auth = self._authorization_header_value(self.session_token, self.auth_scheme)
        if auth:
            h["Authorization"] = auth
        if include_version and self.use_accept_version and self.target_api_version:
            h["Accept-Version"] = str(self.target_api_version)
        return h

    def set_debug_context(self, *, intent_id: int | None = None, trade_run_id: int | None = None) -> None:
        self._ctx.intent_id = (int(intent_id) if intent_id is not None else None)
        self._ctx.trade_run_id = (int(trade_run_id) if trade_run_id is not None else None)

    def clear_debug_context(self) -> None:
        self._ctx.intent_id = None
        self._ctx.trade_run_id = None

    @staticmethod
    def _redact_value(k: str, v: Any) -> Any:
        kk = str(k or '').lower()
        if any(x in kk for x in ['password','token','authorization','secret','api_key','apikey']):
            return '***REDACTED***'
        return v

    @classmethod
    def _redact_obj(cls, obj: Any) -> Any:
        if isinstance(obj, dict):
            return {str(k): cls._redact_obj(cls._redact_value(str(k), v)) for k, v in obj.items()}
        if isinstance(obj, list):
            return [cls._redact_obj(x) for x in obj]
        return obj

    def _emit_verbatim(self, payload: dict[str, Any]) -> None:
        if not self.verbatim_log_enabled:
            return
        try:
            rec = dict(payload or {})
            rec.setdefault('ts_utc', datetime.now(timezone.utc).replace(microsecond=0).isoformat())
            iid = getattr(self._ctx, 'intent_id', None)
            trid = getattr(self._ctx, 'trade_run_id', None)
            if iid is not None:
                rec.setdefault('intent_id', int(iid))
            if trid is not None:
                rec.setdefault('trade_run_id', int(trid))
            p = self.verbatim_log_path
            if not p:
                return
            os.makedirs(os.path.dirname(p), exist_ok=True)
            line = json.dumps(rec, separators=(',', ':'), sort_keys=True) + '\n'
            with open(p, 'a', encoding='utf-8') as f:
                f.write(line)
            if iid is not None:
                d = self.intent_logs_dir
                if d:
                    os.makedirs(d, exist_ok=True)
                    ip = os.path.join(d, f'intent_{int(iid)}.jsonl')
                    with open(ip, 'a', encoding='utf-8') as f2:
                        f2.write(line)
        except Exception:
            pass

    @staticmethod
    def _curl_cmd(method: str, url: str, headers: dict[str, str], json_body: Any | None, params: dict[str, Any] | None) -> str:
        parts = ['curl', '-i', '-sS', '-X', str(method).upper()]
        for k, v in (headers or {}).items():
            parts += ['-H', f"{k}: {v}"]
        if params:
            import urllib.parse as _up
            qs = _up.urlencode({k: v for k, v in params.items() if v is not None})
            if qs:
                sep = '&' if ('?' in url) else '?'
                url = f"{url}{sep}{qs}"
        if json_body is not None:
            try:
                jb = json.dumps(json_body, separators=(',', ':'), sort_keys=True)
            except Exception:
                jb = str(json_body)
            parts += ['--data-raw', jb]
        parts.append(url)
        return ' '.join(shlex.quote(x) for x in parts)

    def _request(self, method: str, path: str, *, json_body: Any | None = None, params: dict[str, Any] | None = None) -> dict[str, Any]:
        if not path.startswith("/"):
            path = "/" + path
        url = f"{self.base_url}{path}"
        # Lazy auth: services can start with username/password in env and no pre-seeded token.
        if (not path.startswith("/sessions")) and (not path.startswith("/oauth/token")) and (not self.session_token) and self.username and self.password:
            self.authenticate()

        last_exc: Exception | None = None
        did_reauth = False
        for attempt in range(0, self.http_max_retries + 1):
            try:
                with httpx.Client(timeout=self.timeout_seconds) as client:
                    req_headers = self._headers(include_version=(not path.startswith("/sessions") and not path.startswith("/oauth/token")))
                    req_id = f"{int(time.time()*1000)}-{attempt}"
                    red_h = self._redact_obj(dict(req_headers or {}))
                    red_b = self._redact_obj(json_body)
                    self._emit_verbatim({
                        "event": "request", "req_id": req_id, "environment": self.environment, "method": method.upper(), "url": url,
                        "path": path, "params": params, "json_body": red_b, "headers": red_h,
                        "curl": self._curl_cmd(method, url, red_h, red_b, params),
                    })
                    resp = client.request(method.upper(), url, headers=req_headers, json=json_body, params=params)
                    try:
                        _rj = resp.json()
                    except Exception:
                        _rj = None
                    self._emit_verbatim({
                        "event": "response", "req_id": req_id, "environment": self.environment, "method": method.upper(), "url": url, "path": path,
                        "status_code": int(resp.status_code), "response_headers": self._redact_obj(dict(resp.headers)),
                        "response_json": self._redact_obj(_rj) if _rj is not None else None,
                        "response_text": (resp.text if _rj is None and resp is not None else None),
                    })
                    # 401 can happen when token expired/invalid; re-auth once then retry immediately.
                    if resp.status_code == 401 and (not path.startswith("/sessions")) and self.username and self.password and (not did_reauth):
                        did_reauth = True
                        self.session_token = None
                        self.authenticate()
                        continue
                    # Retryable statuses
                    if resp.status_code == 429 or resp.status_code >= 500:
                        if attempt < self.http_max_retries:
                            time.sleep(self.http_backoff_seconds * (2 ** attempt))
                            continue
                    resp.raise_for_status()
                    if not resp.content:
                        return {"ok": True, "status_code": resp.status_code}
                    try:
                        return resp.json()
                    except Exception:
                        return {"ok": True, "status_code": resp.status_code, "raw": resp.text}
            except httpx.HTTPStatusError as e:
                self._emit_verbatim({"event":"http_status_error","environment":self.environment,"path":path,"status_code":(int(e.response.status_code) if e.response is not None else None),"error":str(e),"response_text":((e.response.text if e.response is not None else None))})
                last_exc = e
                code = int(e.response.status_code) if e.response is not None else 0
                if code == 429 or code >= 500:
                    if attempt < self.http_max_retries:
                        time.sleep(self.http_backoff_seconds * (2 ** attempt))
                        continue
                raise
            except Exception as e:
                self._emit_verbatim({"event":"auth_exception","environment":self.environment,"path":path,"error_type":type(e).__name__,"error":str(e)})
                last_exc = e
                if attempt < self.http_max_retries:
                    time.sleep(self.http_backoff_seconds * (2 ** attempt))
                    continue
                raise

        if last_exc:
            raise last_exc
        raise RuntimeError("request failed with unknown error")

    def authenticate(self) -> dict[str, Any]:
        if self.session_token and (not _looks_placeholder(self.session_token)):
            self._emit_verbatim({"event":"auth_mode","environment":self.environment,"mode":"existing_bearer","endpoint":None,"config_injected":True})
            return {"ok": True, "auth": "session_token", "environment": self.environment}

        if _looks_placeholder(self.username) or _looks_placeholder(self.password):
            raise RuntimeError("tasty credentials invalid/placeholder; refusing auth attempt")

        last_exc: Exception | None = None
        attempt_errors: list[str] = []

        # Attempt 1: OAuth token endpoint (form-encoded)
        try:
            self._emit_verbatim({"event":"auth_mode","environment":self.environment,"mode":"oauth_password","endpoint":"/oauth/token","config_injected":True})
            url = f"{self.base_url}/oauth/token"
            headers = {"Accept": "application/json", "Content-Type": "application/x-www-form-urlencoded"}
            body = {"grant_type": "password", "username": str(self.username), "password": str(self.password)}
            req_id = f"auth-oauth-{int(time.time()*1000)}"
            self._emit_verbatim({"event":"request","req_id":req_id,"environment":self.environment,"method":"POST","url":url,"path":"/oauth/token","headers":self._redact_obj(headers),"json_body":self._redact_obj(body),"curl":self._curl_cmd('POST',url,self._redact_obj(headers),self._redact_obj(body),None)})
            with httpx.Client(timeout=self.timeout_seconds) as client:
                resp = client.post(url, headers=headers, data=body)
            try:
                rj = resp.json()
            except Exception:
                rj = None
            self._emit_verbatim({"event":"response","req_id":req_id,"environment":self.environment,"method":"POST","url":url,"path":"/oauth/token","status_code":int(resp.status_code),"response_headers":self._redact_obj(dict(resp.headers)),"response_json":self._redact_obj(rj) if rj is not None else None,"response_text":(resp.text if rj is None else None)})
            if 200 <= int(resp.status_code) < 300:
                token = None
                if isinstance(rj, dict):
                    token = rj.get('access_token') or rj.get('token') or ((rj.get('data') or {}).get('access_token') if isinstance(rj.get('data'), dict) else None)
                if token:
                    self.session_token = str(token)
                    self.auth_scheme = "bearer"
                    return {"ok": True, "auth": "oauth_token", "environment": self.environment}
                raise RuntimeError("oauth token response missing access_token")
            attempt_errors.append(f"/oauth/token -> HTTP {int(resp.status_code)}")
        except Exception as e:
            last_exc = e
            attempt_errors.append(f"/oauth/token -> {type(e).__name__}: {e}")

        # Attempt 2: sessions login endpoint (no Accept-Version)
        for path in ('/sessions','/sessions/'):
            try:
                self._emit_verbatim({"event":"auth_mode","environment":self.environment,"mode":"sessions_login","endpoint":path,"config_injected":True})
                url = f"{self.base_url}{path}"
                headers = {"Accept":"application/json","Content-Type":"application/json"}
                body = {"login": str(self.username), "password": str(self.password)}
                req_id = f"auth-sessions-{int(time.time()*1000)}"
                self._emit_verbatim({"event":"request","req_id":req_id,"environment":self.environment,"method":"POST","url":url,"path":path,"headers":self._redact_obj(headers),"json_body":self._redact_obj(body),"curl":self._curl_cmd('POST',url,self._redact_obj(headers),self._redact_obj(body),None)})
                with httpx.Client(timeout=self.timeout_seconds) as client:
                    resp = client.post(url, headers=headers, json=body)
                try:
                    rj = resp.json()
                except Exception:
                    rj = None
                self._emit_verbatim({"event":"response","req_id":req_id,"environment":self.environment,"method":"POST","url":url,"path":path,"status_code":int(resp.status_code),"response_headers":self._redact_obj(dict(resp.headers)),"response_json":self._redact_obj(rj) if rj is not None else None,"response_text":(resp.text if rj is None else None)})
                if 200 <= int(resp.status_code) < 300:
                    token = None
                    data = (rj.get('data') if isinstance(rj, dict) else None)
                    if isinstance(data, dict):
                        token = data.get('session-token') or data.get('session_token')
                    if (not token) and isinstance(rj, dict):
                        token = rj.get('session-token') or rj.get('session_token')
                    if token:
                        self.session_token = str(token)
                        self.auth_scheme = "raw"
                        return {"ok": True, "auth": "sessions_token", "environment": self.environment}
                    raise RuntimeError("sessions response missing token")
                attempt_errors.append(f"{path} -> HTTP {int(resp.status_code)}")
                last_exc = RuntimeError(f"{path} http {int(resp.status_code)}")
            except Exception as e:
                last_exc = e
                attempt_errors.append(f"{path} -> {type(e).__name__}: {e}")

        if last_exc:
            raise RuntimeError(f"tasty authenticate failed after attempts: {attempt_errors}") from last_exc
        raise RuntimeError("tasty authenticate failed")


    def instrument_supported(self, symbol: str) -> dict[str, Any]:
        sym = normalize_option_symbol_for_tasty(str(symbol or ''))
        if not sym:
            return {'ok': False, 'supported': False, 'reason': 'empty_symbol'}
        from urllib.parse import quote
        path = f"/instruments/equity-options/{quote(sym, safe='')}"
        try:
            resp = self._request('GET', path)
            return {'ok': True, 'supported': True, 'symbol': sym, 'response': resp}
        except httpx.HTTPStatusError as e:
            code = int(e.response.status_code) if e.response is not None else 0
            if code in {404, 422}:
                msg = None
                try:
                    msg = e.response.text if e.response is not None else None
                except Exception:
                    msg = None
                return {'ok': True, 'supported': False, 'symbol': sym, 'status_code': code, 'message': msg}
            raise


    def place_order(self, dto: OrderDTO, *, dry_run: bool | None = None) -> dict[str, Any]:
        payload = map_order_dto_to_tasty_payload(dto)
        dr = self.dry_run if dry_run is None else bool(dry_run)
        if dr:
            return {
                "ok": True,
                "dry_run": True,
                "environment": self.environment,
                "account_number": dto.account_number,
                "payload": payload,
            }

        acct = dto.account_number or self.account_number
        if not acct:
            raise RuntimeError("account number required")
        return self._request("POST", f"/accounts/{acct}/orders", json_body=payload)

    def replace_order(self, *, account_number: str, order_id: str, dto: OrderDTO, dry_run: bool | None = None) -> dict[str, Any]:
        payload = map_order_dto_to_tasty_payload(dto)
        dr = self.dry_run if dry_run is None else bool(dry_run)
        if dr:
            return {
                "ok": True,
                "dry_run": True,
                "action": "replace_order",
                "account_number": account_number,
                "order_id": order_id,
                "payload": payload,
            }
        return self._request("PUT", f"/accounts/{account_number}/orders/{order_id}", json_body=payload)

    def cancel_order(self, *, account_number: str, order_id: str, dry_run: bool | None = None) -> dict[str, Any]:
        dr = self.dry_run if dry_run is None else bool(dry_run)
        if dr:
            return {"ok": True, "dry_run": True, "action": "cancel_order", "account_number": account_number, "order_id": order_id}
        return self._request("DELETE", f"/accounts/{account_number}/orders/{order_id}")

    def get_orders(self, *, account_number: str, status: str | None = None) -> dict[str, Any]:
        params = {"status": status} if status else None
        return self._request("GET", f"/accounts/{account_number}/orders", params=params)

    def get_order_history(self, *, account_number: str, order_id: str) -> dict[str, Any]:
        return self._request("GET", f"/accounts/{account_number}/orders/{order_id}")

    def get_positions(self, *, account_number: str) -> dict[str, Any]:
        return self._request("GET", f"/accounts/{account_number}/positions")

    def submit_complex_order(self, *, account_number: str, payload: dict[str, Any], dry_run: bool | None = None) -> dict[str, Any]:
        dr = self.dry_run if dry_run is None else bool(dry_run)
        if dr:
            return {"ok": True, "dry_run": True, "action": "submit_complex_order", "account_number": account_number, "payload": payload}
        return self._request("POST", f"/accounts/{account_number}/complex-orders", json_body=payload)

    def cancel_complex_order(self, *, account_number: str, complex_order_id: str, dry_run: bool | None = None) -> dict[str, Any]:
        dr = self.dry_run if dry_run is None else bool(dry_run)
        if dr:
            return {"ok": True, "dry_run": True, "action": "cancel_complex_order", "account_number": account_number, "complex_order_id": complex_order_id}
        return self._request("DELETE", f"/accounts/{account_number}/complex-orders/{complex_order_id}")

    def place_order_with_warning_reconfirm(self, dto: OrderDTO, *, dry_run: bool | None = None) -> dict[str, Any]:
        # First submit. If broker returns warning requiring confirm/reconfirm, submit confirm path.
        resp = self.place_order(dto, dry_run=dry_run)
        if bool(dry_run if dry_run is not None else self.dry_run):
            return resp

        # Generic warning shapes; API versions can vary.
        warning = None
        data = resp.get('data') if isinstance(resp, dict) else None
        if isinstance(resp, dict):
            warning = resp.get('warning') or resp.get('warnings')
        if warning is None and isinstance(data, dict):
            warning = data.get('warning') or data.get('warnings')

        if warning:
            confirm_payload = map_order_dto_to_tasty_payload(dto)
            confirm_payload['confirm'] = True
            acct = dto.account_number or self.account_number
            if not acct:
                raise RuntimeError('account number required for warning reconfirm')
            resp2 = self._request('POST', f"/accounts/{acct}/orders", json_body=confirm_payload)
            return {'initial': resp, 'reconfirm': resp2}
        return resp

    def place_oco_exits(
        self,
        *,
        account_number: str,
        take_profit: OrderDTO,
        stop_loss: OrderDTO,
        dry_run: bool | None = None,
    ) -> dict[str, Any]:
        tp_payload = map_order_dto_to_tasty_payload(take_profit)
        sl_payload = map_order_dto_to_tasty_payload(stop_loss)

        # Broker-side OCO wrapper payload (shape may vary by API version; kept explicit + auditable).
        payload = {
            "order-type": "OCO",
            "orders": [tp_payload, sl_payload],
        }

        dr = self.dry_run if dry_run is None else bool(dry_run)
        if dr:
            return {
                "ok": True,
                "dry_run": True,
                "action": "place_oco_exits",
                "account_number": account_number,
                "payload": payload,
            }
        return self._request("POST", f"/accounts/{account_number}/orders", json_body=payload)

    def close_position(self, *, account_number: str, symbol: str, quantity: int, dry_run: bool | None = None) -> dict[str, Any]:
        payload = {
            "symbol": str(symbol).upper(),
            "quantity": int(quantity),
            "action": "Close",
        }
        dr = self.dry_run if dry_run is None else bool(dry_run)
        if dr:
            return {
                "ok": True,
                "dry_run": True,
                "action": "close_position",
                "account_number": account_number,
                "payload": payload,
            }
        return self._request("POST", f"/accounts/{account_number}/positions/close", json_body=payload)
