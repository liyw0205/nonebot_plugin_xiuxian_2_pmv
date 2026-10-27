"""Room lifetime rules for the entertainment games (pure, no I/O).

Room snapshots survive a restart: ``entertainment_game_rooms`` keeps them and each
game module restores its manager at startup. The in-memory timeout tasks do NOT
survive, so before this module existed a restored room kept binding its players
forever — a waiting room whose 180s window had long passed still answered
``你已在其它棋局中`` and the player could never open a new game.

The rules here are deliberately free of I/O, NoneBot and asyncio so the restore
path can decide "expired" versus "still live" synchronously, and a unit test can
pin the boundaries without a clock or a driver.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Mapping

# Mirrors of the per-game constants. These are the same values the runtime
# timeout tasks use (``gomoku.ROOM_TIMEOUT``/``MOVE_TIMEOUT``,
# ``half_ten.ROOM_TIMEOUT``, ``minesweeper.GAME_TIMEOUT``); the pure module keeps
# its own copy so it stays importable without NoneBot side effects, and
# ``tests/test_room_lifecycle.py`` pins them against the runtime constants.
GOMOKU_ROOM_TIMEOUT_SECONDS = 180
GOMOKU_MOVE_TIMEOUT_SECONDS = 120
HALF_TEN_ROOM_TIMEOUT_SECONDS = 180
MINESWEEPER_GAME_TIMEOUT_SECONDS = 600

_TIME_FORMAT = "%Y-%m-%d %H:%M:%S"

# Which states a game can be expired FROM. Anything already settled/closed has
# already had its cleanup, so it must never be treated as an expiry candidate
# (re-deleting is harmless but keying on it would misreport).
_TIMEOUT_STATES = {
    "gomoku": frozenset({"waiting", "playing"}),
    "half_ten": frozenset({"waiting"}),
    "minesweeper": frozenset({"playing"}),
}

# Which state field holds the timestamp the timeout is measured from.
# ``gomoku`` switches anchor once play begins (move clock vs. join clock);
# minesweeper uses the action clock so a long game is not killed mid-play.
_ANCHOR_FIELD = {
    "gomoku": "last_move_time",
    "half_ten": "create_time",
    "minesweeper": "last_action_time",
}

# Fallback anchor when the primary field is absent or unusable (e.g. a gomoku
# room restored in ``playing`` state whose ``last_move_time`` was never written).
_FALLBACK_ANCHOR_FIELD = {
    "gomoku": "create_time",
    "half_ten": "create_time",
    "minesweeper": "create_time",
}


def _parse_timestamp(value: Any) -> datetime | None:
    """Parse a room snapshot timestamp, returning None when it is unusable.

    A snapshot written by an older version may carry ``None`` or a malformed
    string. Callers treat None as "cannot judge" and leave the room alone rather
    than deleting a player's game on a guess.
    """
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        return datetime.strptime(value.strip(), _TIME_FORMAT)
    except ValueError:
        return None


def timeout_seconds(game_type: str, state: Mapping[str, Any]) -> int | None:
    """Return the timeout that applies to *state*, or None when it does not participate."""
    if game_type not in _TIMEOUT_STATES:
        return None
    status = state.get("status")
    if not isinstance(status, str) or status not in _TIMEOUT_STATES[game_type]:
        return None
    if game_type == "gomoku" and status == "playing":
        return GOMOKU_MOVE_TIMEOUT_SECONDS
    if game_type == "minesweeper":
        return MINESWEEPER_GAME_TIMEOUT_SECONDS
    if game_type == "gomoku":
        return GOMOKU_ROOM_TIMEOUT_SECONDS
    return HALF_TEN_ROOM_TIMEOUT_SECONDS


def room_deadline(game_type: str, state: Mapping[str, Any]) -> datetime | None:
    """Return when *state* should have timed out, or None when it has no clock.

    None means "not a candidate" (settled/closed/unknown state) or "the snapshot
    carries no usable timestamp"; both cases must be left untouched by cleanup.
    """
    seconds = timeout_seconds(game_type, state)
    if seconds is None:
        return None
    anchor = _parse_timestamp(state.get(_ANCHOR_FIELD.get(game_type, "create_time")))
    if anchor is None:
        anchor = _parse_timestamp(
            state.get(_FALLBACK_ANCHOR_FIELD.get(game_type, "create_time"))
        )
    if anchor is None:
        return None
    return anchor + timedelta(seconds=seconds)


def is_expired(game_type: str, state: Mapping[str, Any], now: datetime) -> bool:
    """True when *state* passed its deadline at *now*.

    A room exactly at its deadline is NOT expired: the runtime task sleeps the
    full timeout before acting, so only a strictly later instant counts. This
    keeps the restored and the live paths consistent.
    """
    deadline = room_deadline(game_type, state)
    if deadline is None:
        return False
    return now > deadline


def remaining_seconds(
    game_type: str, state: Mapping[str, Any], now: datetime
) -> int | None:
    """Seconds left before *state* times out; None when it is not a candidate.

    Expired rooms return 0 so the caller can act immediately instead of sleeping
    a negative duration. The anchor clock of a restore is "now", so a room that
    still has time keeps only its REMAINDER — a restart must not hand the player
    a fresh full timeout.
    """
    deadline = room_deadline(game_type, state)
    if deadline is None:
        return None
    delta = (deadline - now).total_seconds()
    return max(0, int(delta))


def expired_rooms(
    snapshots_by_type: Mapping[str, Any], now: datetime
) -> dict[str, list[str]]:
    """Group expired room ids per game type.

    ``snapshots_by_type`` maps a game type to an iterable of state dicts; the id
    field is the type's identity field (``game_id`` for minesweeper, ``room_id``
    otherwise). Unknown game types and states without an identity are skipped.
    Every known type is present in the result, so callers can rely on the keys.
    """
    identity_field = {"gomoku": "room_id", "half_ten": "room_id", "minesweeper": "game_id"}
    result: dict[str, list[str]] = {game_type: [] for game_type in _TIMEOUT_STATES}
    for game_type, states in snapshots_by_type.items():
        if game_type not in _TIMEOUT_STATES:
            continue
        for state in states:
            if not isinstance(state, Mapping):
                continue
            if not is_expired(game_type, state, now):
                continue
            room_id = state.get(identity_field[game_type])
            if isinstance(room_id, str) and room_id:
                result[game_type].append(room_id)
    return result


__all__ = [
    "GOMOKU_MOVE_TIMEOUT_SECONDS",
    "GOMOKU_ROOM_TIMEOUT_SECONDS",
    "HALF_TEN_ROOM_TIMEOUT_SECONDS",
    "MINESWEEPER_GAME_TIMEOUT_SECONDS",
    "expired_rooms",
    "is_expired",
    "remaining_seconds",
    "room_deadline",
    "timeout_seconds",
]
