from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal

from binance_market_monitor.scanner.stage1 import Stage1CandidateRecord, rank_stage1_candidates


@dataclass(frozen=True, slots=True)
class CandidateManagerConfig:
    permanent_symbols: tuple[str, ...] = ("BTCUSDT", "ETHUSDT")
    max_dynamic_candidates: int = 20
    min_residence_seconds: int = 300
    warmup_seconds: int = 60
    hysteresis_score_margin: Decimal = Decimal("5")


@dataclass(frozen=True, slots=True)
class CandidateSelection:
    warmup: bool
    permanent_symbols: tuple[str, ...]
    dynamic_symbols: tuple[str, ...]
    reason_codes: tuple[str, ...] = field(default_factory=tuple)


class CandidateManager:
    def __init__(self, config: CandidateManagerConfig, *, started_at: datetime) -> None:
        if config.max_dynamic_candidates < 0:
            raise ValueError("max_dynamic_candidates must be non-negative")
        self.config = config
        self.started_at = _to_utc(started_at)
        self._dynamic: dict[str, Stage1CandidateRecord] = {}
        self._entered_at: dict[str, datetime] = {}

    @property
    def permanent_symbols(self) -> tuple[str, ...]:
        return tuple(symbol.upper() for symbol in self.config.permanent_symbols)

    @property
    def dynamic_symbols(self) -> tuple[str, ...]:
        return tuple(self._dynamic)

    @property
    def current_symbols(self) -> tuple[str, ...]:
        return self.permanent_symbols + self.dynamic_symbols

    def update(
        self, candidates: list[Stage1CandidateRecord], *, now: datetime
    ) -> CandidateSelection:
        normalized_now = _to_utc(now)
        if (normalized_now - self.started_at).total_seconds() < self.config.warmup_seconds:
            return CandidateSelection(
                warmup=True,
                permanent_symbols=self.permanent_symbols,
                dynamic_symbols=self.dynamic_symbols,
                reason_codes=("candidate_manager_warmup",),
            )

        ranked = rank_stage1_candidates(candidates)
        by_symbol = {candidate.symbol: candidate for candidate in ranked}
        retained: dict[str, Stage1CandidateRecord] = {}
        for symbol, existing in self._dynamic.items():
            replacement = by_symbol.pop(symbol, existing)
            retained[symbol] = replacement

        available_slots = max(0, self.config.max_dynamic_candidates - len(retained))
        for candidate in by_symbol.values():
            if available_slots <= 0:
                break
            if candidate.symbol in self.permanent_symbols:
                continue
            retained[candidate.symbol] = candidate
            self._entered_at.setdefault(candidate.symbol, normalized_now)
            available_slots -= 1

        if len(retained) >= self.config.max_dynamic_candidates and by_symbol:
            retained = self._apply_hysteresis(retained, list(by_symbol.values()), normalized_now)

        self._dynamic = dict(
            sorted(
                retained.items(),
                key=lambda pair: rank_stage1_candidates([pair[1]])[0].symbol,
            )
        )
        for symbol in list(self._entered_at):
            if symbol not in self._dynamic:
                del self._entered_at[symbol]
        for symbol in self._dynamic:
            self._entered_at.setdefault(symbol, normalized_now)

        return CandidateSelection(False, self.permanent_symbols, self.dynamic_symbols)

    def _apply_hysteresis(
        self,
        retained: dict[str, Stage1CandidateRecord],
        challengers: list[Stage1CandidateRecord],
        now: datetime,
    ) -> dict[str, Stage1CandidateRecord]:
        selected = retained.copy()
        ranked_selected = rank_stage1_candidates(list(selected.values()))
        for challenger in rank_stage1_candidates(challengers):
            if challenger.symbol in selected or challenger.symbol in self.permanent_symbols:
                continue
            weakest = ranked_selected[-1]
            entered_at = self._entered_at.get(weakest.symbol, now)
            residence_met = (now - entered_at).total_seconds() >= self.config.min_residence_seconds
            margin_met = challenger.score >= weakest.score + self.config.hysteresis_score_margin
            if residence_met and margin_met:
                del selected[weakest.symbol]
                selected[challenger.symbol] = challenger
                self._entered_at.setdefault(challenger.symbol, now)
                ranked_selected = rank_stage1_candidates(list(selected.values()))
        return dict(list(selected.items())[: self.config.max_dynamic_candidates])


def _to_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)
