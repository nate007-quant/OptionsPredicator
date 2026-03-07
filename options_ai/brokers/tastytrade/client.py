from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Literal
import os

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
                "symbol": str(leg.symbol).upper(),
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


class TastytradeClient:
    """Minimal Tastytrade REST adapter with dry-run support.

    Auth notes:
      - If `session_token` is provided, it is used directly as Bearer token.
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
    ) -> None:
        self.environment = str(environment or "sandbox").lower()
        self.base_url = (base_url or os.getenv("TASTY_BASE_URL") or "https://api.cert.tastyworks.com").rstrip("/")
        self.streamer_url = (streamer_url or os.getenv("TASTY_STREAMER_URL") or "").strip()
        self.session_token = (session_token or os.getenv("TASTY_SESSION_TOKEN") or "").strip() or None
        self.username = (username or os.getenv("TASTY_USERNAME") or "").strip() or None
        self.password = (password or os.getenv("TASTY_PASSWORD") or "").strip() or None
        self.account_number = (account_number or os.getenv("TASTY_ACCOUNT_NUMBER") or "").strip() or None
        self.timeout_seconds = int(timeout_seconds)
        self.dry_run = bool(dry_run)
        self.target_api_version = (target_api_version or os.getenv("TARGET_API_VERSION") or "").strip() or None

    def _headers(self) -> dict[str, str]:
        h = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        if self.session_token:
            h["Authorization"] = f"Bearer {self.session_token}"
        if self.target_api_version:
            h["Accept-Version"] = str(self.target_api_version)
        return h

    def _request(self, method: str, path: str, *, json_body: Any | None = None, params: dict[str, Any] | None = None) -> dict[str, Any]:
        if not path.startswith("/"):
            path = "/" + path
        url = f"{self.base_url}{path}"
        with httpx.Client(timeout=self.timeout_seconds) as client:
            resp = client.request(method.upper(), url, headers=self._headers(), json=json_body, params=params)
            resp.raise_for_status()
            if not resp.content:
                return {"ok": True, "status_code": resp.status_code}
            try:
                return resp.json()
            except Exception:
                return {"ok": True, "status_code": resp.status_code, "raw": resp.text}

    def authenticate(self) -> dict[str, Any]:
        if self.session_token:
            return {"ok": True, "auth": "session_token", "environment": self.environment}
        if not self.username or not self.password:
            raise RuntimeError("tasty credentials missing (set TASTY_SESSION_TOKEN or TASTY_USERNAME/TASTY_PASSWORD)")

        # Endpoint contract can evolve; keep payload minimal and persist token from known keys.
        resp = self._request(
            "POST",
            "/sessions",
            json_body={"login": self.username, "password": self.password},
        )
        data = resp.get("data") if isinstance(resp, dict) else None
        token = None
        if isinstance(data, dict):
            token = data.get("session-token") or data.get("session_token")
        if not token:
            token = resp.get("session-token") if isinstance(resp, dict) else None
        if token:
            self.session_token = str(token)
        return resp

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
