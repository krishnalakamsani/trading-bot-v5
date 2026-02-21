from __future__ import annotations

from dataclasses import dataclass
from collections import deque
from typing import Optional

from .score_mds import decide_entry_mds, decide_exit_mds
from config import config


@dataclass(frozen=True)
class StrategyEntryDecision:
    should_enter: bool
    option_type: str = ""  # 'CE' | 'PE'
    reason: str = ""
    confirm_count: int = 0
    confirm_needed: int = 0


@dataclass(frozen=True)
class StrategyExitDecision:
    should_exit: bool
    reason: str = ""


class ScoreMdsRunner:
    """Decision-only runner for the MDS/ScoreEngine strategy.

    Owns the multi-candle confirmation state.
    """

    def __init__(self) -> None:
        self._last_direction: Optional[str] = None
        self._confirm_count: int = 0
        # Keep recent raw MDS scores to detect rising trend for entries
        self._recent_scores: deque[float] = deque(maxlen=5)

    def reset(self) -> None:
        self._last_direction = None
        self._confirm_count = 0
        self._recent_scores.clear()

    def on_entry_attempted(self) -> None:
        """Call after an entry attempt (success or blocked downstream)."""
        self._confirm_count = 0

    def decide_exit(self, *, position_type: str, score: float, slope: float, slow_mom: float) -> StrategyExitDecision:
        # Append latest score for trend-aware exit decisions
        try:
            self._recent_scores.append(float(score or 0.0))
        except Exception:
            pass
        # Do not auto-exit on short-term MDS drops; keep exits deterministic and
        # limited to target, trailing SL, or neutral/reversal signals from
        # `decide_exit_mds`.

        d = decide_exit_mds(
            position_type=str(position_type or ""),
            score=float(score or 0.0),
            slope=float(slope or 0.0),
            slow_mom=float(slow_mom or 0.0),
        )
        return StrategyExitDecision(bool(d.should_exit), str(d.reason or ""))

    def decide_entry(
        self,
        *,
        ready: bool,
        is_choppy: bool,
        direction: str,
        score: float,
        slope: float,
        confirm_needed: int,
    ) -> StrategyEntryDecision:
        direction = str(direction or "NONE")
        # Track recent MDS scores for trend-based entry gating
        try:
            self._recent_scores.append(float(score or 0.0))
        except Exception:
            pass

        if not ready:
            return StrategyEntryDecision(False, "", "mds_not_ready")
        if is_choppy:
            return StrategyEntryDecision(False, "", "mds_choppy")

        if direction == "NONE":
            self._last_direction = direction
            self._confirm_count = 0
            return StrategyEntryDecision(False, "", "neutral_band")

        # Select thresholds based on legacy vs tuned config
        if bool(config.get('use_legacy_thresholds', False)):
            score_min = 10.0
            slope_min = 1.0
        else:
            score_min = 12.0
            slope_min = 1.5

        if abs(float(score or 0.0)) < float(score_min):
            self._last_direction = direction
            self._confirm_count = 0
            return StrategyEntryDecision(False, "", "score_too_low")

        if abs(float(slope or 0.0)) < float(slope_min):
            self._last_direction = direction
            self._confirm_count = 0
            return StrategyEntryDecision(False, "", "slope_too_low")

        # Entry gating: require last 3 MDS scores to be strictly increasing (CE)
        # or strictly decreasing (PE) for immediate entry.
        recent = list(self._recent_scores)
        rising_ok = False
        falling_ok = False
        if len(recent) >= 3:
            a, b, c = recent[-3], recent[-2], recent[-1]
            if c > b and b > a:
                rising_ok = True
            if c < b and b < a:
                falling_ok = True

        # Immediate-entry override: when direction is CE and MDS rose for last 3 ticks,
        # or when direction is PE and MDS fell for last 3 ticks, enter immediately
        # (user requested) without waiting for slope/confidence.
        if rising_ok and direction == "CE":
            # set confirm_count so UI/telemetry shows arming progress as satisfied
            self._last_direction = direction
            self._confirm_count = int(confirm_needed or 0) or 1
            return StrategyEntryDecision(True, "CE", "rising_mds_immediate", confirm_count=int(self._confirm_count), confirm_needed=int(confirm_needed or 0))

        if falling_ok and direction == "PE":
            self._last_direction = direction
            self._confirm_count = int(confirm_needed or 0) or 1
            return StrategyEntryDecision(True, "PE", "falling_mds_immediate", confirm_count=int(self._confirm_count), confirm_needed=int(confirm_needed or 0))

        if rising_ok:
            if self._last_direction == direction:
                self._confirm_count += 1
            else:
                self._last_direction = direction
                self._confirm_count = 1
        else:
            # Reset confirm count until we observe a 3-tick rising pattern
            self._last_direction = direction
            self._confirm_count = 0

        d = decide_entry_mds(
            ready=bool(ready),
            is_choppy=bool(is_choppy),
            direction=direction,
            score=float(score or 0.0),
            slope=float(slope or 0.0),
            confirm_count=int(self._confirm_count),
            confirm_needed=int(confirm_needed or 0),
        )

        return StrategyEntryDecision(
            bool(d.should_enter),
            str(d.option_type or ""),
            str(d.reason or ""),
            confirm_count=int(self._confirm_count),
            confirm_needed=int(confirm_needed or 0),
        )
