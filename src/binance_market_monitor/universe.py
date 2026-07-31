from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal

DEFAULT_STABLE_ASSETS = frozenset(
    {
        "USDT",
        "USDC",
        "FDUSD",
        "TUSD",
        "BUSD",
        "DAI",
        "USDP",
        "USDD",
        "PYUSD",
        "USD1",
        "RLUSD",
        "USDE",
        "USDS",
        "AEUR",
        "EURI",
        "EUR",
        "TRY",
        "BRL",
        "GBP",
        "AUD",
        "PLN",
        "RON",
        "UAH",
        "RUB",
    }
)

LEVERAGED_SUFFIXES = ("DOWN", "BULL", "BEAR", "UP")
MIN_NON_SUFFIX_BASE_LENGTH = 2


@dataclass(frozen=True, slots=True)
class SpotSymbolMetadata:
    """Minimal Spot exchange metadata needed for deterministic universe filtering."""

    symbol: str
    base_asset: str
    quote_asset: str
    status: str
    listed_at: datetime | None = None
    history_minutes: int | None = None


@dataclass(frozen=True, slots=True)
class UniverseFilterConfig:
    """Configurable deterministic gates for Spot universe eligibility."""

    allowed_quote_assets: tuple[str, ...] = ("USDT",)
    min_quote_volume: Decimal = Decimal("10000000")
    min_history_minutes: int = 60
    min_listing_age: timedelta = timedelta(days=7)
    denied_symbols: tuple[str, ...] = ()
    stable_assets: frozenset[str] = DEFAULT_STABLE_ASSETS
    now: datetime | None = None

    def __post_init__(self) -> None:
        if self.min_quote_volume < 0:
            raise ValueError("min_quote_volume must be non-negative")
        if self.min_history_minutes < 0:
            raise ValueError("min_history_minutes must be non-negative")
        if self.min_listing_age < timedelta(0):
            raise ValueError("min_listing_age must be non-negative")


@dataclass(frozen=True, slots=True)
class UniverseFilterResult:
    """Included symbols and deterministic first exclusion reason per rejected symbol."""

    included: tuple[SpotSymbolMetadata, ...]
    exclusion_reasons: dict[str, str] = field(default_factory=dict)


def filter_spot_universe(
    symbols: Sequence[SpotSymbolMetadata],
    *,
    quote_volumes: Mapping[str, Decimal],
    config: UniverseFilterConfig,
) -> UniverseFilterResult:
    """Return eligible Spot symbols in input order with explicit exclusion reasons.

    The caller supplies current 24h quote volumes and observed local history durations. This keeps
    the filter deterministic and fully offline-testable; data acquisition remains a connector
    concern.
    """

    included: list[SpotSymbolMetadata] = []
    exclusion_reasons: dict[str, str] = {}

    allowed_quotes = {_normalize_asset(asset) for asset in config.allowed_quote_assets}
    denied_symbols = {_normalize_symbol(symbol) for symbol in config.denied_symbols}
    stable_assets = {_normalize_asset(asset) for asset in config.stable_assets}
    now = _normalize_now(config.now)

    for symbol in symbols:
        normalized_symbol = _normalize_symbol(symbol.symbol)
        base_asset = _normalize_asset(symbol.base_asset)
        quote_asset = _normalize_asset(symbol.quote_asset)
        quote_volume = quote_volumes.get(normalized_symbol)

        reason = _first_exclusion_reason(
            metadata=symbol,
            normalized_symbol=normalized_symbol,
            base_asset=base_asset,
            quote_asset=quote_asset,
            quote_volume=quote_volume,
            allowed_quotes=allowed_quotes,
            denied_symbols=denied_symbols,
            stable_assets=stable_assets,
            config=config,
            now=now,
        )
        if reason is None:
            included.append(symbol)
        else:
            exclusion_reasons[normalized_symbol] = reason

    return UniverseFilterResult(included=tuple(included), exclusion_reasons=exclusion_reasons)


def select_spot_universe(
    symbols: Sequence[SpotSymbolMetadata],
    *,
    quote_volumes: Mapping[str, Decimal],
    config: UniverseFilterConfig,
    max_symbols: int,
) -> UniverseFilterResult:
    """Filter first, rank eligible markets by liquidity, then apply the cardinality cap."""

    if max_symbols < 1:
        raise ValueError("max_symbols must be positive")
    filtered = filter_spot_universe(symbols, quote_volumes=quote_volumes, config=config)
    ranked = sorted(
        filtered.included,
        key=lambda item: (
            -quote_volumes.get(_normalize_symbol(item.symbol), Decimal("0")),
            item.symbol,
        ),
    )
    selected = ranked[:max_symbols]
    exclusion_reasons = dict(filtered.exclusion_reasons)
    for symbol in ranked[max_symbols:]:
        exclusion_reasons[_normalize_symbol(symbol.symbol)] = "universe_cardinality_cap"
    return UniverseFilterResult(included=tuple(selected), exclusion_reasons=exclusion_reasons)


def _first_exclusion_reason(
    *,
    metadata: SpotSymbolMetadata,
    normalized_symbol: str,
    base_asset: str,
    quote_asset: str,
    quote_volume: Decimal | None,
    allowed_quotes: set[str],
    denied_symbols: set[str],
    stable_assets: set[str],
    config: UniverseFilterConfig,
    now: datetime,
) -> str | None:
    if normalized_symbol in denied_symbols:
        return "denylist"
    if metadata.status.upper() != "TRADING":
        return "status_not_trading"
    if quote_asset not in allowed_quotes:
        return "quote_asset_not_allowed"
    if base_asset in stable_assets and quote_asset in stable_assets:
        return "stablecoin_pair"
    if _has_leveraged_suffix(base_asset):
        return "leveraged_token_suffix"
    if quote_volume is None:
        return "missing_quote_volume"
    if quote_volume < config.min_quote_volume:
        return "quote_volume_below_minimum"
    if metadata.history_minutes is None:
        return "insufficient_history"
    if metadata.history_minutes < config.min_history_minutes:
        return "insufficient_history"
    if _is_recent_listing(metadata.listed_at, now=now, min_listing_age=config.min_listing_age):
        return "recent_listing"
    return None


def _has_leveraged_suffix(base_asset: str) -> bool:
    for suffix in LEVERAGED_SUFFIXES:
        prefix = base_asset.removesuffix(suffix)
        if prefix != base_asset and len(prefix) >= MIN_NON_SUFFIX_BASE_LENGTH:
            return True
    return False


def _is_recent_listing(
    listed_at: datetime | None, *, now: datetime, min_listing_age: timedelta
) -> bool:
    if listed_at is None:
        return False
    comparable_listed_at = _with_timezone(listed_at)
    return now - comparable_listed_at < min_listing_age


def _normalize_now(now: datetime | None) -> datetime:
    if now is None:
        return datetime.now(tz=UTC)
    return _with_timezone(now)


def _with_timezone(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _normalize_symbol(symbol: str) -> str:
    normalized = symbol.strip().upper()
    if not normalized.isalnum():
        raise ValueError(f"invalid symbol: {symbol}")
    return normalized


def _normalize_asset(asset: str) -> str:
    normalized = asset.strip().upper()
    if not normalized.isalnum():
        raise ValueError(f"invalid asset: {asset}")
    return normalized
