from __future__ import annotations

import hashlib
import json
import os
import stat
from datetime import datetime
from pathlib import Path
from typing import Any

from ...infrastructure.database import DatabaseUnitOfWork

MAX_LEGACY_ROOM_FILES = 4096
MAX_LEGACY_ROOM_FILE_BYTES = 8 * 1024 * 1024
MAX_LEGACY_ROOM_TOTAL_BYTES = 128 * 1024 * 1024


def _legacy_room_directory() -> Path:
    package_root = Path(__file__).resolve().parents[2]
    return package_root / "xiuxian" / "xiuxian_entertainment" / "mod" / "data" / "rooms"


def _room_identity(data: dict[str, Any]) -> tuple[str, str, str] | None:
    if {
        "room_id", "creator_id", "player_black", "player_white", "current_player",
        "board", "moves", "status", "winner", "create_time",
    } <= data.keys():
        return "gomoku", str(data["room_id"]), str(data["status"])
    if {"room_id", "creator_id", "players", "status", "create_time", "cards", "rankings"} <= data.keys():
        return "half_ten", str(data["room_id"]), str(data["status"])
    if {
        "game_id", "user_id", "user_name", "width", "height", "mines", "status",
        "create_time", "last_action_time", "first_click_done", "board", "revealed", "flagged",
    } <= data.keys():
        return "minesweeper", str(data["game_id"]), str(data["status"])
    return None


def _validate_room_state(game_type: str, room_id: str, data: dict[str, Any]) -> None:
    if not room_id or len(room_id) > 256:
        raise ValueError("invalid entertainment room identity")
    valid_statuses = {
        "gomoku": {"waiting", "playing", "finished"},
        "half_ten": {"waiting", "finished", "closed"},
        "minesweeper": {"playing", "win", "lose", "closed"},
    }
    if data.get("status") not in valid_statuses[game_type]:
        raise ValueError("invalid entertainment room status")
    if game_type == "gomoku":
        board = data.get("board")
        if (
            not isinstance(board, list)
            or len(board) != 15
            or any(not isinstance(row, list) or len(row) != 15 for row in board)
            or not isinstance(data.get("moves"), list)
            or not isinstance(data.get("player_names"), dict)
        ):
            raise ValueError("invalid gomoku room snapshot")
    elif game_type == "half_ten":
        if (
            not isinstance(data.get("players"), list)
            or not isinstance(data.get("player_names"), dict)
            or not isinstance(data.get("cards"), dict)
            or not isinstance(data.get("points"), dict)
            or not isinstance(data.get("rankings"), list)
        ):
            raise ValueError("invalid half-ten room snapshot")
    else:
        width, height = data.get("width"), data.get("height")
        if (
            not isinstance(width, int)
            or isinstance(width, bool)
            or not isinstance(height, int)
            or isinstance(height, bool)
            or width <= 0
            or height <= 0
            or width * height > 1_000_000
        ):
            raise ValueError("invalid minesweeper dimensions")
        for field in ("board", "revealed", "flagged"):
            matrix = data.get(field)
            if (
                not isinstance(matrix, list)
                or len(matrix) != height
                or any(not isinstance(row, list) or len(row) != width for row in matrix)
            ):
                raise ValueError("invalid minesweeper room snapshot")


def _read_legacy_room_snapshots(directory: Path) -> tuple[list[tuple[str, str, str, str]], str, int, int]:
    digest = hashlib.sha256()
    imported: list[tuple[str, str, str, str]] = []
    total_bytes = 0
    ignored_count = 0
    if not directory.exists():
        return imported, digest.hexdigest(), 0, 0
    if not directory.is_dir():
        raise ValueError("entertainment room snapshot path is not a directory")

    paths: list[Path] = []
    try:
        with os.scandir(directory) as entries:
            for entry in entries:
                if not entry.name.endswith(".json"):
                    continue
                paths.append(directory / entry.name)
                if len(paths) > MAX_LEGACY_ROOM_FILES:
                    raise ValueError("entertainment room snapshot count exceeds migration limit")
    except OSError as exc:
        raise ValueError("could not enumerate entertainment room snapshots") from exc
    paths.sort(key=lambda item: item.name)
    seen_ids: set[tuple[str, str]] = set()
    for path in paths:
        if path.is_symlink():
            raise ValueError("entertainment room snapshots must not be symlinks")
        try:
            mode = path.stat().st_mode
            if not stat.S_ISREG(mode):
                raise ValueError("entertainment room snapshot is not a regular file")
            with path.open("rb") as handle:
                raw = handle.read(MAX_LEGACY_ROOM_FILE_BYTES + 1)
        except OSError as exc:
            raise ValueError("could not read entertainment room snapshot") from exc
        if len(raw) > MAX_LEGACY_ROOM_FILE_BYTES:
            raise ValueError("entertainment room snapshot exceeds per-file migration limit")
        total_bytes += len(raw)
        if total_bytes > MAX_LEGACY_ROOM_TOTAL_BYTES:
            raise ValueError("entertainment room snapshots exceed total migration limit")
        digest.update(path.name.encode("utf-8", "surrogatepass"))
        digest.update(b"\0")
        digest.update(raw)
        digest.update(b"\0")
        try:
            data = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, ValueError, RecursionError):
            ignored_count += 1
            continue
        if not isinstance(data, dict):
            ignored_count += 1
            continue
        identity = _room_identity(data)
        if identity is None:
            ignored_count += 1
            continue
        game_type, room_id, status = identity
        _validate_room_state(game_type, room_id, data)
        key = (game_type, room_id)
        if key in seen_ids:
            raise ValueError("duplicate entertainment room identity in legacy snapshots")
        seen_ids.add(key)
        payload = json.dumps(data, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        imported.append((game_type, room_id, status, payload))
    return imported, digest.hexdigest(), total_bytes, ignored_count


def apply_entertainment(uow: DatabaseUnitOfWork) -> None:
    uow.execute("CREATE TABLE IF NOT EXISTS entertainment_feature_migrations (version TEXT PRIMARY KEY)")
    uow.execute("INSERT OR IGNORE INTO entertainment_feature_migrations(version) VALUES ('legacy.entertainment.001')")


def apply_entertainment_rooms(
    uow: DatabaseUnitOfWork,
    legacy_directory: str | Path | None = None,
    occurred_at: str | None = None,
) -> None:
    """Import valid legacy game rooms once, before the runtime restores managers."""
    uow.execute(
        "CREATE TABLE IF NOT EXISTS entertainment_game_rooms("
        "game_type TEXT NOT NULL,room_id TEXT NOT NULL,status TEXT NOT NULL,"
        "state_json TEXT NOT NULL,updated_at TEXT NOT NULL,"
        "PRIMARY KEY(game_type,room_id))"
    )
    uow.execute(
        "CREATE INDEX IF NOT EXISTS entertainment_game_rooms_status_idx "
        "ON entertainment_game_rooms(game_type,status,room_id)"
    )
    uow.execute(
        "CREATE TABLE IF NOT EXISTS entertainment_game_room_migrations("
        "migration_key TEXT PRIMARY KEY,snapshot_sha256 TEXT NOT NULL,"
        "imported_count INTEGER NOT NULL,ignored_count INTEGER NOT NULL,"
        "snapshot_bytes INTEGER NOT NULL,migrated_at TEXT NOT NULL)"
    )
    migration_key = "legacy.entertainment.rooms-json-v1"
    if uow.query_one(
        "SELECT 1 AS found FROM entertainment_game_room_migrations WHERE migration_key=?",
        (migration_key,),
    ) is not None:
        return
    directory = Path(legacy_directory) if legacy_directory is not None else _legacy_room_directory()
    imported, snapshot_hash, snapshot_bytes, ignored_count = _read_legacy_room_snapshots(directory)
    if uow.query_one("SELECT 1 AS found FROM entertainment_game_rooms LIMIT 1") is not None:
        raise RuntimeError("entertainment room rows exist without a legacy migration receipt")
    migrated_at = str(occurred_at or datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    uow.executemany(
        "INSERT INTO entertainment_game_rooms(game_type,room_id,status,state_json,updated_at) "
        "VALUES(?,?,?,?,?)",
        ((game_type, room_id, status, payload, migrated_at) for game_type, room_id, status, payload in imported),
    )
    uow.execute(
        "INSERT INTO entertainment_game_room_migrations(migration_key,snapshot_sha256,"
        "imported_count,ignored_count,snapshot_bytes,migrated_at) VALUES(?,?,?,?,?,?)",
        (migration_key, snapshot_hash, len(imported), ignored_count, snapshot_bytes, migrated_at),
    )


__all__ = ["apply_entertainment", "apply_entertainment_rooms"]
