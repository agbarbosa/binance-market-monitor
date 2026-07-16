from __future__ import annotations

import os
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Self

import yaml
from pydantic import BaseModel, ConfigDict, Field, SecretStr, model_validator

EnvMapping = Mapping[str, str]


class StrictBaseModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class UniverseConfig(StrictBaseModel):
    eligible_quote_assets: tuple[str, ...] = ("USDT",)
    exclusion_patterns: tuple[str, ...] = (r".*(UP|DOWN|BULL|BEAR)USDT$",)
    denied_symbols: tuple[str, ...] = ()
    permanent_deep_symbols: tuple[str, ...] = ("BTCUSDT", "ETHUSDT")
    max_dynamic_candidates: int = Field(default=20, ge=0)


class Stage1Config(StrictBaseModel):
    min_quote_volume_usdt: str = "10000000"
    min_relative_volume_5m: float = Field(default=2.0, ge=0)
    max_spread_bps: float = Field(default=5.0, gt=0)


class Stage2Config(StrictBaseModel):
    min_evidence_groups: int = Field(default=2, ge=2)
    min_depth_notional_usdt: str = "50000"


class FeatureWindowConfig(StrictBaseModel):
    windows: tuple[str, ...] = ("1m", "5m", "15m", "1h")


class AlertsConfig(StrictBaseModel):
    cooldown_seconds: int = Field(default=900, ge=0)
    repeated_symbol_window_seconds: int = Field(default=21_600, ge=0)


class StaleThresholdsConfig(StrictBaseModel):
    ticker_ms: int = Field(default=5_000, gt=0)
    book_ticker_ms: int = Field(default=3_000, gt=0)
    aggregate_trade_ms: int = Field(default=10_000, gt=0)
    futures_mark_price_ms: int = Field(default=5_000, gt=0)


class StorageConfig(StrictBaseModel):
    base_path: Path = Path("data")
    retention_days: int = Field(default=14, ge=1)


class WebhookConfig(StrictBaseModel):
    enabled: bool = False
    url: SecretStr | None = None
    timeout_seconds: float = Field(default=5.0, gt=0)
    max_retries: int = Field(default=3, ge=0)

    @model_validator(mode="after")
    def require_url_when_enabled(self) -> Self:
        if self.enabled and self.url is None:
            raise ValueError("webhook URL must be supplied through BMM_WEBHOOK_URL")
        return self


class LoggingConfig(StrictBaseModel):
    level: str = "INFO"


class ApiConfig(StrictBaseModel):
    bind_host: str = "127.0.0.1"
    bind_port: int = Field(default=8000, ge=1, le=65_535)
    authentication_required: bool = False
    allowlist_cidrs: tuple[str, ...] = ()

    @model_validator(mode="after")
    def reject_unauthenticated_public_bind(self) -> Self:
        loopback_hosts = {"127.0.0.1", "localhost", "::1"}
        is_public_bind = self.bind_host not in loopback_hosts
        has_network_protection = self.authentication_required or bool(self.allowlist_cidrs)
        if is_public_bind and not has_network_protection:
            raise ValueError(
                "public bind host requires authentication_required=true or allowlist_cidrs"
            )
        return self


class AppConfig(StrictBaseModel):
    universe: UniverseConfig = Field(default_factory=UniverseConfig)
    stage1: Stage1Config = Field(default_factory=Stage1Config)
    stage2: Stage2Config = Field(default_factory=Stage2Config)
    features: FeatureWindowConfig = Field(default_factory=FeatureWindowConfig)
    alerts: AlertsConfig = Field(default_factory=AlertsConfig)
    stale_thresholds: StaleThresholdsConfig = Field(default_factory=StaleThresholdsConfig)
    storage: StorageConfig = Field(default_factory=StorageConfig)
    webhook: WebhookConfig = Field(default_factory=WebhookConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    api: ApiConfig = Field(default_factory=ApiConfig)

    @classmethod
    def from_yaml(cls, path: str | Path, env: EnvMapping | None = None) -> Self:
        config_path = Path(path)
        raw_config = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
        if not isinstance(raw_config, dict):
            raise ValueError("top-level config must be a mapping")

        merged = _deep_copy_mapping(raw_config)
        _apply_env_overrides(merged, os.environ if env is None else env)
        return cls.model_validate(merged)

    def safe_dump(self) -> dict[str, Any]:
        return self.model_dump(mode="json")


def _deep_copy_mapping(value: Mapping[str, Any]) -> dict[str, Any]:
    copied: dict[str, Any] = {}
    for key, item in value.items():
        if isinstance(item, Mapping):
            copied[key] = _deep_copy_mapping(item)
        else:
            copied[key] = item
    return copied


def _apply_env_overrides(config: dict[str, Any], env: EnvMapping) -> None:
    webhook_url = env.get("BMM_WEBHOOK_URL")
    if webhook_url:
        webhook = config.setdefault("webhook", {})
        if not isinstance(webhook, dict):
            raise ValueError("webhook config must be a mapping")
        webhook["url"] = webhook_url
