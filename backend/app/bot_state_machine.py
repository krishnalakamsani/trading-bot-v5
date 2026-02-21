"""BotStateMachine — single source of truth for bot lifecycle state.

States:
    IDLE        — bot stopped, no position
    WARMING_UP  — bot started, seeding indicators from history
    SCANNING    — bot running, watching for entry signal, no position
    ENTERING    — entry order placed, waiting for fill confirmation
    IN_POSITION — position open, monitoring SL/target/signals
    EXITING     — exit order placed, waiting for fill confirmation
    COOLDOWN    — brief post-exit pause before next entry is allowed
    ERROR       — unrecoverable error, requires manual intervention

Legal transitions:
    IDLE        → WARMING_UP (on bot start)
    WARMING_UP  → SCANNING   (seed complete)
    SCANNING    → ENTERING   (signal fires, order placed)
    ENTERING    → IN_POSITION (order confirmed filled)
    ENTERING    → SCANNING   (order rejected/timed out)
    IN_POSITION → EXITING    (SL/target/signal/manual)
    EXITING     → COOLDOWN   (exit confirmed)
    EXITING     → IN_POSITION (exit failed, still open)
    COOLDOWN    → SCANNING   (cooldown elapsed)
    ANY         → IDLE       (bot stopped)
    ANY         → ERROR      (unhandled exception)
"""
from __future__ import annotations

import logging
from enum import Enum, auto
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)


class BotPhase(Enum):
    IDLE        = auto()
    WARMING_UP  = auto()
    SCANNING    = auto()
    ENTERING    = auto()
    IN_POSITION = auto()
    EXITING     = auto()
    COOLDOWN    = auto()
    ERROR       = auto()


# Which transitions are legal
_ALLOWED: dict[BotPhase, set[BotPhase]] = {
    BotPhase.IDLE:        {BotPhase.WARMING_UP, BotPhase.SCANNING},
    BotPhase.WARMING_UP:  {BotPhase.SCANNING,   BotPhase.IDLE, BotPhase.ERROR},
    BotPhase.SCANNING:    {BotPhase.ENTERING,    BotPhase.IDLE, BotPhase.ERROR},
    BotPhase.ENTERING:    {BotPhase.IN_POSITION, BotPhase.SCANNING, BotPhase.IDLE, BotPhase.ERROR},
    BotPhase.IN_POSITION: {BotPhase.EXITING,     BotPhase.IDLE, BotPhase.ERROR},
    BotPhase.EXITING:     {BotPhase.COOLDOWN,    BotPhase.IN_POSITION, BotPhase.IDLE, BotPhase.ERROR},
    BotPhase.COOLDOWN:    {BotPhase.SCANNING,    BotPhase.IDLE, BotPhase.ERROR},
    BotPhase.ERROR:       {BotPhase.IDLE},
}


class BotStateMachine:
    """Governs bot phase transitions with logging and guard checks."""

    def __init__(self) -> None:
        self._phase = BotPhase.IDLE
        self._entered_at: Optional[datetime] = None
        self._previous: Optional[BotPhase] = None

    # ── read ──────────────────────────────────────────────────────────────────

    @property
    def phase(self) -> BotPhase:
        return self._phase

    @property
    def phase_name(self) -> str:
        return self._phase.name

    # ── convenience guards — used throughout trading_bot.py ──────────────────

    @property
    def can_enter(self) -> bool:
        """True only when it's safe to place an entry order."""
        return self._phase == BotPhase.SCANNING

    @property
    def can_exit(self) -> bool:
        """True only when a position is open and no exit is pending."""
        return self._phase == BotPhase.IN_POSITION

    @property
    def is_active(self) -> bool:
        """True whenever the bot loop should be running."""
        return self._phase not in (BotPhase.IDLE, BotPhase.ERROR)

    # ── transitions ──────────────────────────────────────────────────────────

    def transition(self, new_phase: BotPhase, reason: str = "") -> bool:
        """Attempt a phase transition. Returns True if allowed, False if blocked."""
        allowed = _ALLOWED.get(self._phase, set())

        if new_phase not in allowed:
            logger.warning(
                f"[STATE] Blocked illegal transition {self._phase.name} → {new_phase.name}"
                + (f" ({reason})" if reason else "")
            )
            return False

        self._previous = self._phase
        self._phase = new_phase
        self._entered_at = datetime.now(timezone.utc)
        logger.info(
            f"[STATE] {self._previous.name} → {self._phase.name}"
            + (f" | {reason}" if reason else "")
        )
        return True

    # Explicit named transitions — cleaner call sites

    def start(self) -> bool:
        return self.transition(BotPhase.WARMING_UP, "bot started")

    def warmed_up(self) -> bool:
        return self.transition(BotPhase.SCANNING, "indicators seeded")

    def placing_entry(self) -> bool:
        return self.transition(BotPhase.ENTERING, "entry order placed")

    def entry_confirmed(self) -> bool:
        return self.transition(BotPhase.IN_POSITION, "fill confirmed")

    def entry_failed(self) -> bool:
        return self.transition(BotPhase.SCANNING, "entry rejected/timeout")

    def placing_exit(self) -> bool:
        return self.transition(BotPhase.EXITING, "exit order placed")

    def exit_confirmed(self) -> bool:
        return self.transition(BotPhase.COOLDOWN, "exit fill confirmed")

    def exit_failed(self) -> bool:
        return self.transition(BotPhase.IN_POSITION, "exit failed — still open")

    def cooldown_done(self) -> bool:
        return self.transition(BotPhase.SCANNING, "cooldown elapsed")

    def stop(self) -> bool:
        return self.transition(BotPhase.IDLE, "bot stopped")

    def error(self, reason: str = "") -> bool:
        return self.transition(BotPhase.ERROR, reason or "unhandled error")

    # ── serialisation for broadcast ──────────────────────────────────────────

    def to_dict(self) -> dict:
        return {
            "phase": self._phase.name,
            "previous_phase": self._previous.name if self._previous else None,
            "entered_at": self._entered_at.isoformat() if self._entered_at else None,
        }


# Global singleton — imported by trading_bot.py
state_machine = BotStateMachine()
