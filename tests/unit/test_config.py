from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import SecretStr, ValidationError

from binance_market_monitor.config import AppConfig


def test_config_loads_yaml_and_overrides_webhook_secret_from_env(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        """
universe:
  eligible_quote_assets: [USDT]
  exclusion_patterns: [".*(UP|DOWN|BULL|BEAR)USDT$"]
  denied_symbols: [BUSDUSDT]
  permanent_deep_symbols: [BTCUSDT, ETHUSDT]
  max_dynamic_candidates: 7
stage1:
  min_quote_volume_usdt: "25000000"
  min_relative_volume_5m: 2.5
  max_spread_bps: 4.0
stage2:
  min_evidence_groups: 2
  min_depth_notional_usdt: "100000"
features:
  windows: ["1m", "5m", "15m", "1h"]
alerts:
  cooldown_seconds: 900
  repeated_symbol_window_seconds: 21600
stale_thresholds:
  ticker_ms: 5000
  book_ticker_ms: 3000
  aggregate_trade_ms: 10000
  futures_mark_price_ms: 5000
storage:
  base_path: data
  retention_days: 14
webhook:
  enabled: true
  timeout_seconds: 3.5
  max_retries: 4
logging:
  level: DEBUG
api:
  bind_host: 127.0.0.1
  bind_port: 8088
""".strip(),
        encoding="utf-8",
    )

    config = AppConfig.from_yaml(
        config_path,
        env={"BMM_WEBHOOK_URL": "https://n8n.example.invalid/webhook/private-token"},
    )

    assert config.universe.eligible_quote_assets == ("USDT",)
    assert config.universe.permanent_deep_symbols == ("BTCUSDT", "ETHUSDT")
    assert config.universe.max_dynamic_candidates == 7
    assert config.stage1.min_quote_volume_usdt == "25000000"
    assert config.features.windows == ("1m", "5m", "15m", "1h")
    assert config.webhook.url == SecretStr("https://n8n.example.invalid/webhook/private-token")
    assert "private-token" not in repr(config)
    assert "private-token" not in config.model_dump_json()
    assert config.safe_dump()["webhook"]["url"] == "**********"


def test_config_rejects_public_bind_without_authentication_or_allowlist(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        """
api:
  bind_host: 0.0.0.0
  authentication_required: false
  allowlist_cidrs: []
webhook:
  enabled: false
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(ValidationError, match="public bind host"):
        AppConfig.from_yaml(config_path)


def test_config_allows_public_bind_when_protected_by_allowlist(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        """
api:
  bind_host: 0.0.0.0
  authentication_required: false
  allowlist_cidrs: ["203.0.113.0/24"]
webhook:
  enabled: false
""".strip(),
        encoding="utf-8",
    )

    config = AppConfig.from_yaml(config_path)

    assert config.api.bind_host == "0.0.0.0"
    assert config.api.allowlist_cidrs == ("203.0.113.0/24",)


def test_webhook_enabled_requires_runtime_secret() -> None:
    with pytest.raises(ValidationError, match="webhook URL"):
        AppConfig.model_validate({"webhook": {"enabled": True}})
