"""One-shot startup restore for the entertainment rooms.

Rooms persist in ``entertainment_game_rooms`` and each game module rebuilds its
manager at startup, but the asyncio timeout tasks do not survive the process.
Restoring per game type would also open one connection per type; this module
takes the single batch snapshot (``room_states_all``, one read-only unit of work)
and then lets each game release what already expired and re-arm the rest.
"""

from __future__ import annotations

from datetime import datetime

# Module-level guard: the three compatibility startup hooks each call this, but
# the batch read and the restore pass must happen exactly once per process.
_restored = False


def reset_restore_state() -> None:
    """Clear the one-shot guard (tests and explicit re-restores only)."""
    global _restored
    _restored = False


def _managers():
    """Import lazily: the game modules import ``room_store`` at module level."""
    from . import gomoku, half_ten, minesweeper

    return gomoku, half_ten, minesweeper


async def restore_entertainment_rooms(now: datetime | None = None) -> dict:
    """Load every room type from ONE snapshot, then release/expire and re-arm.

    Idempotent: the first call performs the batch read and the full restore, and
    later calls (the other two startup hooks) return the empty result without
    touching the database again.
    """
    global _restored
    if _restored:
        return {}

    gomoku, half_ten, minesweeper = _managers()
    application = gomoku.entertainment_application
    states = application.room_states_all()

    gomoku.room_manager.load_rooms(states.get("gomoku"))
    half_ten.half_manager.load_rooms(states.get("half_ten"))
    minesweeper.ms_manager.load_rooms(states.get("minesweeper"))

    gomoku_expired = await gomoku.rearm_room_timeouts(now)
    half_expired = await half_ten.rearm_half_ten_timeouts(now)
    mines_expired = await minesweeper.rearm_game_timeouts(now)

    _restored = True
    return {
        "gomoku": gomoku_expired["gomoku"],
        "half_ten": half_expired["half_ten"],
        "minesweeper": mines_expired["minesweeper"],
    }


__all__ = ["reset_restore_state", "restore_entertainment_rooms"]
