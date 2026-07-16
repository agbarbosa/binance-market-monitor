"""Stdlib-only Hermes tool client for the local Binance Market Monitor API."""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.parse
import urllib.request
from decimal import Decimal, InvalidOperation
from typing import Any

DEFAULT_API_URL = "http://127.0.0.1:8000"
ENV_API_URL = "BMM_HERMES_API_URL"
REQUEST_TIMEOUT_SECONDS = 2.0
MAX_RESPONSE_BYTES = 1_000_000
REMEDIATION = (
    "uv run --python 3.12 binance-market-monitor monitor --config "
    "configs/config.example.yaml --serve-api --no-futures"
)
ENDPOINTS = {
    "candidates": "/candidates/current",
    "alerts": "/alerts/recent",
    "health": "/health/binance",
    "metrics": "/metrics/summary",
}
LOOPBACK_HOSTS = {"127.0.0.1", "localhost", "::1"}


class RedirectRejected(Exception):
    """Raised when the local API tries to redirect a read-only request."""


class InvalidResponse(Exception):
    """Raised when the local API returns malformed or unsafe data."""


class NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Reject every redirect instead of allowing urllib to follow it."""

    def redirect_request(  # type: ignore[override]
        self,
        req: urllib.request.Request,
        fp: Any,
        code: int,
        msg: str,
        headers: Any,
        newurl: str,
    ) -> urllib.request.Request:
        raise RedirectRejected(f"Local API redirect rejected for {req.full_url} -> {newurl}")


def handle_binance_opportunities_list(args: dict[str, Any] | None = None, **_kwargs: Any) -> str:
    """Hermes handler for binance_opportunities_list."""
    try:
        parsed_args = _parse_args(args or {})
        base_url = _validated_base_url()
        payloads = _fetch_payloads(base_url)
        response = _build_response(base_url, payloads, parsed_args)
        return _json(response)
    except ValueError as exc:
        return _json(_error("invalid_arguments", str(exc)))
    except RedirectRejected as exc:
        return _json(_error("invalid_response", str(exc)))
    except InvalidResponse as exc:
        return _json(_error("invalid_response", str(exc)))
    except (OSError, TimeoutError, urllib.error.URLError) as exc:
        return _json(_error("api_unavailable", f"Local monitor API is unavailable: {exc}"))
    except Exception as exc:  # noqa: BLE001 - Hermes tools must never raise to the agent loop.
        return _json(_error("invalid_response", f"Unexpected local API response error: {exc}"))


def _parse_args(args: dict[str, Any]) -> dict[str, Any]:
    limit = _parse_int(args.get("limit", 10), "limit", minimum=1, maximum=50)
    min_stage1_score = _parse_decimal(args.get("min_stage1_score", 0), "min_stage1_score")
    min_watch_score = _parse_int(
        args.get("min_watch_score", 0), "min_watch_score", minimum=0, maximum=100
    )
    confirmed_raw = args.get("confirmed_only", False)
    if not isinstance(confirmed_raw, bool):
        raise ValueError("confirmed_only must be a boolean.")
    return {
        "limit": limit,
        "min_stage1_score": min_stage1_score,
        "min_watch_score": min_watch_score,
        "confirmed_only": confirmed_raw,
    }


def _parse_int(value: Any, name: str, *, minimum: int, maximum: int) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{name} must be an integer between {minimum} and {maximum}.")
    if isinstance(value, int):
        parsed = value
    elif isinstance(value, str) and value.strip().lstrip("+-").isdigit():
        parsed = int(value.strip())
    else:
        raise ValueError(f"{name} must be an integer between {minimum} and {maximum}.")
    if parsed < minimum or parsed > maximum:
        raise ValueError(f"{name} must be an integer between {minimum} and {maximum}.")
    return parsed


def _parse_decimal(value: Any, name: str) -> Decimal:
    if isinstance(value, bool):
        raise ValueError(f"{name} must be a number.")
    try:
        parsed = Decimal(str(value).strip())
    except (InvalidOperation, ValueError, AttributeError) as exc:
        raise ValueError(f"{name} must be a number.") from exc
    if not parsed.is_finite():
        raise ValueError(f"{name} must be a finite number.")
    return parsed


def _validated_base_url() -> str:
    raw = os.environ.get(ENV_API_URL, DEFAULT_API_URL).strip() or DEFAULT_API_URL
    parsed = urllib.parse.urlparse(raw)
    host = parsed.hostname
    if parsed.scheme != "http" or host not in LOOPBACK_HOSTS:
        raise ValueError(
            "BMM_HERMES_API_URL must use http and a loopback host "
            "(127.0.0.1, localhost, or ::1)."
        )
    if not parsed.netloc:
        raise ValueError(
            "BMM_HERMES_API_URL must use http and a loopback host "
            "(127.0.0.1, localhost, or ::1)."
        )
    return urllib.parse.urlunparse(("http", parsed.netloc, "", "", "", ""))


def _fetch_payloads(base_url: str) -> dict[str, Any]:
    return {name: _get_json(base_url, path) for name, path in ENDPOINTS.items()}


def _get_json(base_url: str, path: str) -> Any:
    url = f"{base_url}{path}"
    request = urllib.request.Request(
        url,
        method="GET",
        headers={"Accept": "application/json", "User-Agent": "hermes-binance-opportunities/0.1"},
    )
    opener = urllib.request.build_opener(NoRedirectHandler)
    try:
        with opener.open(request, timeout=REQUEST_TIMEOUT_SECONDS) as response:
            status = getattr(response, "status", 200)
            if status < 200 or status >= 300:
                raise InvalidResponse(f"Local API returned HTTP {status} for {path}.")
            body = response.read(MAX_RESPONSE_BYTES + 1)
    except urllib.error.HTTPError as exc:
        raise InvalidResponse(f"Local API returned HTTP {exc.code} for {path}.") from exc
    if len(body) > MAX_RESPONSE_BYTES:
        raise InvalidResponse(f"Local API response for {path} is too large.")
    try:
        return json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise InvalidResponse(f"Local API response for {path} is not valid JSON.") from exc


def _build_response(
    base_url: str, payloads: dict[str, Any], args: dict[str, Any]
) -> dict[str, Any]:
    candidates = _extract_list(payloads["candidates"], "candidates", ENDPOINTS["candidates"])
    alerts = _extract_list(payloads["alerts"], "alerts", ENDPOINTS["alerts"])
    health = _extract_dict(payloads["health"], ENDPOINTS["health"])
    metrics = _extract_dict(payloads["metrics"], ENDPOINTS["metrics"])
    latest_alerts = _latest_alert_by_symbol(alerts)
    opportunities = _merge_opportunities(candidates, latest_alerts)
    filtered = _filter_opportunities(opportunities, args)
    filtered.sort(key=_sort_key)
    limited = filtered[: args["limit"]]
    return {
        "success": True,
        "source_api": {"base_url": base_url, "endpoints": list(ENDPOINTS.values())},
        "monitor_health": health,
        "metrics": metrics,
        "count": len(limited),
        "opportunities": [_public_opportunity(item) for item in limited],
        "disclaimer": "Watch alert only. Not financial advice. No trade recommendation.",
    }


def _extract_list(payload: Any, key: str, path: str) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        raise InvalidResponse(f"Local API response for {path} must be an object.")
    values = payload.get(key)
    if not isinstance(values, list):
        raise InvalidResponse(f"Local API response for {path} must contain a {key} list.")
    return [item for item in values if isinstance(item, dict)]


def _extract_dict(payload: Any, path: str) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise InvalidResponse(f"Local API response for {path} must be an object.")
    return payload


def _latest_alert_by_symbol(alerts: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    latest: dict[str, dict[str, Any]] = {}
    for alert in alerts:
        symbol = _symbol(alert)
        if not symbol:
            continue
        previous = latest.get(symbol)
        if previous is None or _time_key(alert) >= _time_key(previous):
            latest[symbol] = alert
    return latest


def _merge_opportunities(
    candidates: list[dict[str, Any]], latest_alerts: dict[str, dict[str, Any]]
) -> list[dict[str, Any]]:
    opportunities: list[dict[str, Any]] = []
    seen_symbols: set[str] = set()
    for candidate in candidates:
        symbol = _symbol(candidate)
        if not symbol:
            continue
        alert = latest_alerts.get(symbol)
        opportunities.append(_opportunity_from(candidate, alert))
        seen_symbols.add(symbol)
    for symbol, alert in latest_alerts.items():
        if symbol not in seen_symbols:
            opportunities.append(_opportunity_from(None, alert))
    return opportunities


def _opportunity_from(
    candidate: dict[str, Any] | None, alert: dict[str, Any] | None
) -> dict[str, Any]:
    assert candidate is not None or alert is not None
    source = candidate if candidate is not None else alert
    assert source is not None
    symbol = _symbol(source)
    stage1_score = _decimal_or_none(candidate.get("score")) if candidate is not None else None
    watch_score = _int_or_none(alert.get("watch_score")) if alert is not None else None
    candidate_reasons = _list_of_strings(candidate.get("reason_codes")) if candidate else []
    alert_reasons = _list_of_strings(alert.get("reason_codes")) if alert else []
    if candidate:
        features = _dict_or_empty(candidate.get("features"))
    else:
        assert alert is not None
        features = _dict_or_empty(alert.get("metrics"))
    opportunity: dict[str, Any] = {
        "symbol": symbol,
        "status": "confirmed_watch" if alert is not None else "stage1_candidate",
        "stage1_score": _number_or_none(stage1_score),
        "detected_at": _detected_at(candidate, alert),
        "reason_codes": _dedupe(candidate_reasons + alert_reasons),
        "features": features,
        "summary": _summary(symbol, candidate, alert),
        "_stage1_score_sort": stage1_score,
        "_watch_score_sort": watch_score,
    }
    if watch_score is not None:
        opportunity["watch_score"] = watch_score
    if alert is not None:
        if isinstance(alert.get("evidence_confidence"), str):
            opportunity["evidence_confidence"] = alert["evidence_confidence"]
        if isinstance(alert.get("emitted_at"), str):
            opportunity["emitted_at"] = alert["emitted_at"]
        data_quality = alert.get("data_quality")
        if isinstance(data_quality, dict):
            opportunity["data_quality"] = data_quality
        invalidation = alert.get("invalidation")
        if isinstance(invalidation, list):
            opportunity["invalidation"] = [item for item in invalidation if isinstance(item, str)]
    return opportunity


def _filter_opportunities(
    opportunities: list[dict[str, Any]], args: dict[str, Any]
) -> list[dict[str, Any]]:
    filtered: list[dict[str, Any]] = []
    min_stage1_score = args["min_stage1_score"]
    min_watch_score = args["min_watch_score"]
    for item in opportunities:
        if args["confirmed_only"] and item["status"] != "confirmed_watch":
            continue
        stage1_score = item["_stage1_score_sort"]
        if stage1_score is None and min_stage1_score > 0:
            continue
        if stage1_score is not None and stage1_score < min_stage1_score:
            continue
        watch_score = item["_watch_score_sort"]
        if watch_score is None and min_watch_score > 0:
            continue
        if watch_score is not None and watch_score < min_watch_score:
            continue
        filtered.append(item)
    return filtered


def _sort_key(item: dict[str, Any]) -> tuple[int, int, Decimal, str]:
    confirmed_rank = 0 if item["status"] == "confirmed_watch" else 1
    watch_score = item["_watch_score_sort"] if item["_watch_score_sort"] is not None else -1
    stage1_score = (
        item["_stage1_score_sort"] if item["_stage1_score_sort"] is not None else Decimal("-1")
    )
    return (confirmed_rank, -int(watch_score), -stage1_score, str(item["symbol"]))


def _public_opportunity(item: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in item.items() if not key.startswith("_")}


def _symbol(payload: dict[str, Any]) -> str:
    value = payload.get("symbol")
    return str(value).upper() if value else ""


def _time_key(payload: dict[str, Any]) -> str:
    emitted = payload.get("emitted_at")
    detected = payload.get("detected_at")
    if isinstance(emitted, str) and emitted:
        return emitted
    if isinstance(detected, str) and detected:
        return detected
    return ""


def _detected_at(candidate: dict[str, Any] | None, alert: dict[str, Any] | None) -> str | None:
    if candidate is not None and isinstance(candidate.get("detected_at"), str):
        return candidate["detected_at"]
    if alert is not None and isinstance(alert.get("detected_at"), str):
        return alert["detected_at"]
    return None


def _summary(symbol: str, candidate: dict[str, Any] | None, alert: dict[str, Any] | None) -> str:
    if alert is not None and isinstance(alert.get("summary"), str) and alert["summary"]:
        return alert["summary"]
    reasons = _list_of_strings(candidate.get("reason_codes")) if candidate else []
    reason_text = ", ".join(_dedupe(reasons)) if reasons else "Stage1 criteria"
    return f"{symbol} Stage1 candidate: {reason_text}. Manual review required."


def _list_of_strings(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, str)]


def _dict_or_empty(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _dedupe(values: list[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        if value not in seen:
            deduped.append(value)
            seen.add(value)
    return deduped


def _decimal_or_none(value: Any) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    return parsed if parsed.is_finite() else None


def _int_or_none(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    if not parsed.is_finite():
        return None
    return max(0, min(100, int(parsed)))


def _number_or_none(value: Decimal | None) -> int | float | None:
    if value is None:
        return None
    if value == value.to_integral_value():
        return int(value)
    return float(value)


def _error(code: str, message: str) -> dict[str, Any]:
    return {
        "success": False,
        "error": {"code": code, "message": message, "remediation": REMEDIATION},
    }


def _json(payload: dict[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
