#!/usr/bin/env python3
"""Emit Telegram-ready text only when new confirmed Binance watches appear."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
import urllib.error
import urllib.request
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

API_URL = "http://127.0.0.1:8000/alerts/recent"
MAX_RESPONSE_BYTES = 1_000_000
REQUEST_TIMEOUT_SECONDS = 3.0
MAX_SEEN_ALERTS = 1_000
ERROR_NOTICE_INTERVAL = timedelta(hours=1)
DEFAULT_STATE_PATH = Path.home() / ".hermes" / "state" / "binance-alert-watchdog.json"


class NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    def redirect_request(  # type: ignore[override]
        self,
        req: urllib.request.Request,
        fp: Any,
        code: int,
        msg: str,
        headers: Any,
        newurl: str,
    ) -> None:
        return None


def fetch_confirmed_alerts() -> list[dict[str, Any]]:
    """Read confirmed alerts from the loopback-only monitor API."""
    request = urllib.request.Request(
        API_URL,
        method="GET",
        headers={"Accept": "application/json", "User-Agent": "bmm-telegram-watchdog/1.0"},
    )
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirectHandler)
    with opener.open(request, timeout=REQUEST_TIMEOUT_SECONDS) as response:
        status = getattr(response, "status", 200)
        if status < 200 or status >= 300:
            raise OSError(f"HTTP {status} from local monitor API")
        body = response.read(MAX_RESPONSE_BYTES + 1)
    if len(body) > MAX_RESPONSE_BYTES:
        raise OSError("local monitor API response is too large")
    try:
        payload = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise OSError("local monitor API returned invalid JSON") from exc
    if not isinstance(payload, dict) or not isinstance(payload.get("alerts"), list):
        raise OSError("local monitor API response has no alerts list")
    return [item for item in payload["alerts"] if isinstance(item, dict)]


def run_watchdog(
    *,
    fetcher: Callable[[], list[dict[str, Any]]] = fetch_confirmed_alerts,
    state_path: Path = DEFAULT_STATE_PATH,
    now: datetime | None = None,
) -> str:
    """Return an exact Telegram message, or an empty string when there is nothing new."""
    current_time = now or datetime.now(UTC)
    state = _load_state(state_path)
    try:
        alerts = fetcher()
    except Exception as exc:  # noqa: BLE001 - expected network boundary.
        return _handle_api_error(state, state_path, current_time, exc)

    seen = [str(value) for value in state.get("seen", []) if isinstance(value, str)]
    seen_set = set(seen)
    keyed_alerts = [(_alert_key(alert), alert) for alert in alerts]
    keyed_alerts.sort(key=lambda item: _alert_time(item[1]))

    if not state.get("initialized", False):
        state.update(
            {
                "initialized": True,
                "seen": _bounded_seen(seen + [key for key, _alert in keyed_alerts]),
                "last_error_notice_at": None,
            }
        )
        _save_state(state_path, state)
        return ""

    new_alerts = [(key, alert) for key, alert in keyed_alerts if key not in seen_set]
    state.update(
        {
            "seen": _bounded_seen(seen + [key for key, _alert in keyed_alerts]),
            "last_error_notice_at": None,
        }
    )
    _save_state(state_path, state)
    return "\n\n---\n\n".join(_render_alert(alert) for _key, alert in new_alerts)


def _handle_api_error(
    state: dict[str, Any], state_path: Path, now: datetime, exc: Exception
) -> str:
    previous = _parse_time(state.get("last_error_notice_at"))
    if previous is not None and now - previous < ERROR_NOTICE_INTERVAL:
        return ""
    state["last_error_notice_at"] = now.isoformat()
    _save_state(state_path, state)
    return (
        "⚠️ **Monitor Binance indisponível**\n\n"
        f"A verificação local de alertas falhou: `{type(exc).__name__}: {exc}`\n"
        "Uma nova advertência só será enviada se o problema persistir por mais de uma hora."
    )


def _render_alert(alert: dict[str, Any]) -> str:
    symbol = str(alert.get("symbol") or "símbolo desconhecido").upper()
    score = alert.get("watch_score")
    confidence = alert.get("evidence_confidence") or "não informada"
    emitted_at = alert.get("emitted_at") or alert.get("detected_at") or "não informado"
    summary = alert.get("summary") or "Atividade pública de mercado com múltiplas confirmações."
    reasons = _strings(alert.get("reason_codes"))
    invalidation = _strings(alert.get("invalidation"))
    quality = alert.get("data_quality") if isinstance(alert.get("data_quality"), dict) else {}

    lines = [
        "🚨 **Oportunidade Binance confirmada**",
        "",
        f"**Símbolo:** `{symbol}`",
        f"**Watch score:** {_format_score(score)}",
        f"**Confiança da evidência:** {confidence}",
        f"**Emitido em:** {emitted_at}",
        "",
        f"**Resumo:** {summary}",
    ]
    if reasons:
        lines.extend(["", "**Motivos:**", *[f"- `{reason}`" for reason in reasons]])
    if quality:
        compact_quality = ", ".join(f"{key}={value}" for key, value in sorted(quality.items()))
        lines.extend(["", f"**Qualidade dos dados:** {compact_quality}"])
    if invalidation:
        lines.extend(["", "**Invalidação:**", *[f"- {item}" for item in invalidation]])
    lines.extend(
        [
            "",
            "⚠️ Alerta observacional; não é recomendação financeira nem indicação de trade.",
        ]
    )
    return "\n".join(lines)


def _format_score(value: Any) -> str:
    if value is None or isinstance(value, bool):
        return "não informado"
    return f"{value}/100"


def _strings(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    result: list[str] = []
    seen: set[str] = set()
    for item in value:
        if isinstance(item, str) and item and item not in seen:
            result.append(item)
            seen.add(item)
    return result


def _alert_key(alert: dict[str, Any]) -> str:
    alert_id = alert.get("alert_id")
    if isinstance(alert_id, str) and alert_id:
        return alert_id
    stable = json.dumps(alert, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return "sha256:" + hashlib.sha256(stable.encode("utf-8")).hexdigest()


def _alert_time(alert: dict[str, Any]) -> str:
    value = alert.get("emitted_at") or alert.get("detected_at")
    return str(value) if value is not None else ""


def _bounded_seen(values: list[str]) -> list[str]:
    unique: list[str] = []
    seen: set[str] = set()
    for value in values:
        if value not in seen:
            unique.append(value)
            seen.add(value)
    return unique[-MAX_SEEN_ALERTS:]


def _load_state(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, OSError, json.JSONDecodeError):
        return {"initialized": False, "seen": [], "last_error_notice_at": None}
    if not isinstance(payload, dict):
        return {"initialized": False, "seen": [], "last_error_notice_at": None}
    return payload


def _save_state(path: Path, state: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(prefix=path.name + ".", dir=path.parent)
    temporary_path = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(state, handle, ensure_ascii=False, sort_keys=True)
            handle.write("\n")
        os.chmod(temporary_path, 0o600)
        temporary_path.replace(path)
    finally:
        temporary_path.unlink(missing_ok=True)


def _parse_time(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)


def main() -> int:
    state_override = os.environ.get("BMM_ALERT_WATCHDOG_STATE_PATH")
    state_path = Path(state_override) if state_override else DEFAULT_STATE_PATH
    output = run_watchdog(state_path=state_path)
    if output:
        print(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
