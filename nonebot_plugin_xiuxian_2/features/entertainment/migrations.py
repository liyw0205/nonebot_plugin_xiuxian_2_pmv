from __future__ import annotations

import hashlib
import json
import os
import stat
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit

from ...infrastructure.database import DatabaseUnitOfWork, OperationLedger
from .newapi_policy import (
    MAX_ACCOUNT_ID_CHARS,
    MAX_ACCOUNT_LABEL_CHARS,
    MAX_ACCOUNT_LIST_FILE_BYTES,
    MAX_ACCOUNT_LIST_ROWS,
    MAX_CHECKIN_BASE_URL_CHARS,
    MAX_CHECKIN_HISTORY_BYTES,
    MAX_CHECKIN_HISTORY_ROWS,
    MAX_CHECKIN_HISTORY_SUMMARY_CHARS,
    MAX_CHECKIN_SECRET_CHARS,
    MAX_NEWAPI_LEGACY_FILES,
    MAX_NEWAPI_LEGACY_TOTAL_BYTES,
)

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


def apply_entertainment_guess_sessions(uow: DatabaseUnitOfWork) -> None:
    uow.execute(
        "CREATE TABLE IF NOT EXISTS entertainment_guess_sessions("
        "game_type TEXT NOT NULL CHECK(game_type IN ('number','puzzle')),"
        "user_id TEXT NOT NULL,state_json TEXT NOT NULL,session_token TEXT NOT NULL,"
        "expires_at REAL NOT NULL,PRIMARY KEY(game_type,user_id))"
    )
    uow.execute(
        "CREATE INDEX IF NOT EXISTS entertainment_guess_sessions_expiry_idx "
        "ON entertainment_guess_sessions(expires_at)"
    )

NEWAPI_ACCOUNTS_TABLE = "entertainment_newapi_accounts"
NEWAPI_HISTORY_TABLE = "entertainment_newapi_checkin_history"


def _legacy_newapi_directories() -> tuple[Path, Path]:
    package_root = Path(__file__).resolve().parents[2]
    module_data = package_root / "xiuxian" / "xiuxian_entertainment" / "mod" / "data"
    return module_data / "newapi_bindings", module_data / "newapi_checkin_history"


def _read_newapi_json_files(directory: Path, max_file_bytes: int, digest, state) -> list[tuple[str, Any]]:
    if not directory.exists():
        return []
    if not directory.is_dir():
        raise ValueError("NewAPI legacy state path is not a directory")
    paths: list[Path] = []
    try:
        with os.scandir(directory) as entries:
            for entry in entries:
                if not entry.name.endswith(".json"):
                    continue
                paths.append(directory / entry.name)
                if len(paths) + state["files"] > MAX_NEWAPI_LEGACY_FILES:
                    raise ValueError("NewAPI legacy file count exceeds migration limit")
    except OSError as exc:
        raise ValueError("could not enumerate NewAPI legacy state") from exc
    paths.sort(key=lambda item: item.name)
    result: list[tuple[str, Any]] = []
    for path in paths:
        if path.is_symlink():
            raise ValueError("NewAPI legacy state files must not be symlinks")
        try:
            if not stat.S_ISREG(path.stat().st_mode):
                raise ValueError("NewAPI legacy state is not a regular file")
            with path.open("rb") as handle:
                raw = handle.read(max_file_bytes + 1)
        except OSError as exc:
            raise ValueError("could not read NewAPI legacy state") from exc
        if len(raw) > max_file_bytes:
            raise ValueError("NewAPI legacy state exceeds per-file migration limit")
        state["files"] += 1
        state["bytes"] += len(raw)
        if state["bytes"] > MAX_NEWAPI_LEGACY_TOTAL_BYTES:
            raise ValueError("NewAPI legacy state exceeds total migration limit")
        digest.update(path.name.encode("utf-8", "surrogatepass"))
        digest.update(b"\0")
        digest.update(raw)
        digest.update(b"\0")
        try:
            payload = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, ValueError, RecursionError) as exc:
            raise ValueError("NewAPI legacy state contains invalid JSON") from exc
        result.append((path.stem, payload))
    return result


def _legacy_newapi_account(row: Any) -> tuple[str, str, str, str, str, int, str]:
    if not isinstance(row, dict):
        raise ValueError("NewAPI legacy account entry must be an object")
    raw_id = row.get("api_user_id")
    api_user_id = str(raw_id) if isinstance(raw_id, (str, int)) and not isinstance(raw_id, bool) else ""
    mode = row.get("mode") or ""
    secret = row.get("secret") or ""
    base_url = row.get("base_url") or ""
    label = row.get("label") or ""
    if (
        not api_user_id
        or len(api_user_id) > MAX_ACCOUNT_ID_CHARS
        or not api_user_id.isdigit()
        or not isinstance(mode, str)
        or len(mode) > 16
        or not isinstance(secret, str)
        or len(secret) > MAX_CHECKIN_SECRET_CHARS
        or not isinstance(base_url, str)
        or len(base_url) > MAX_CHECKIN_BASE_URL_CHARS
        or not isinstance(label, (str, int))
        or isinstance(label, bool)
        or len(str(label)) > MAX_ACCOUNT_LABEL_CHARS
    ):
        raise ValueError("NewAPI legacy account fields exceed migration limits")
    known = {"api_user_id", "mode", "secret", "base_url", "label", "auto_checkin"}
    extras = {key: value for key, value in row.items() if key not in known}
    extra_json = json.dumps(extras, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return (
        api_user_id,
        mode,
        secret,
        base_url,
        str(label),
        int(bool(row.get("auto_checkin"))),
        extra_json,
    )


def _legacy_newapi_history(row: Any) -> tuple[str, int, str, str, str, str]:
    if not isinstance(row, dict):
        raise ValueError("NewAPI legacy history entry must be an object")
    at = row.get("at") or "—"
    raw_index = row.get("index", 0)
    try:
        index = int(raw_index)
    except (TypeError, ValueError, OverflowError):
        index = 0
    api_user_id = row.get("api_user_id") or "?"
    base_url = row.get("base_url") or ""
    summary = row.get("summary") or "—"
    source = row.get("source") or "manual"
    values = (at, api_user_id, base_url, summary, source)
    if any(not isinstance(value, str) for value in values):
        raise ValueError("NewAPI legacy history fields must be text")
    if len(at) > 32 or len(api_user_id) > MAX_ACCOUNT_ID_CHARS or len(base_url) > MAX_CHECKIN_BASE_URL_CHARS:
        raise ValueError("NewAPI legacy history fields exceed migration limits")
    if len(summary) > MAX_CHECKIN_HISTORY_SUMMARY_CHARS:
        summary = summary[:MAX_CHECKIN_HISTORY_SUMMARY_CHARS]
    if source not in {"manual", "auto"}:
        source = "manual"
    candidate = base_url if "://" in base_url else f"https://{base_url}"
    try:
        parsed = urlsplit(candidate)
        hostname = parsed.hostname
        port = parsed.port
    except ValueError:
        hostname = None
        port = None
    if hostname and parsed.scheme.casefold() in {"http", "https"}:
        if ":" in hostname:
            hostname = f"[{hostname}]"
        authority = f"{hostname}:{port}" if port is not None else hostname
        base_url = urlunsplit((parsed.scheme.casefold(), authority, parsed.path[:96], "", ""))
    else:
        base_url = ""
    return at, index, api_user_id, base_url, summary, source


def apply_entertainment_newapi(
    uow: DatabaseUnitOfWork,
    *,
    accounts_directory: str | Path | None = None,
    history_directory: str | Path | None = None,
    occurred_at: str | None = None,
) -> None:
    """Import legacy NewAPI credentials and history without deleting source files."""
    uow.execute(
        f"CREATE TABLE IF NOT EXISTS {NEWAPI_ACCOUNTS_TABLE}("
        "account_id INTEGER PRIMARY KEY AUTOINCREMENT,user_id TEXT NOT NULL,position INTEGER NOT NULL,"
        "api_user_id TEXT NOT NULL,mode TEXT NOT NULL,secret TEXT NOT NULL,base_url TEXT NOT NULL,"
        "label TEXT NOT NULL,auto_checkin INTEGER NOT NULL,extra_json TEXT NOT NULL DEFAULT '{}',"
        "UNIQUE(user_id,position))"
    )
    uow.execute(
        f"CREATE INDEX IF NOT EXISTS entertainment_newapi_auto_idx "
        f"ON {NEWAPI_ACCOUNTS_TABLE}(auto_checkin,account_id)"
    )
    uow.execute(
        f"CREATE TABLE IF NOT EXISTS {NEWAPI_HISTORY_TABLE}("
        "user_id TEXT NOT NULL,position INTEGER NOT NULL,at TEXT NOT NULL,account_index INTEGER NOT NULL,"
        "api_user_id TEXT NOT NULL,base_url TEXT NOT NULL,summary TEXT NOT NULL,source TEXT NOT NULL,"
        "PRIMARY KEY(user_id,position))"
    )
    uow.execute(
        "CREATE TABLE IF NOT EXISTS entertainment_newapi_migrations("
        "migration_key TEXT PRIMARY KEY,snapshot_sha256 TEXT NOT NULL,account_files INTEGER NOT NULL,"
        "history_files INTEGER NOT NULL,account_rows INTEGER NOT NULL,history_rows INTEGER NOT NULL,"
        "snapshot_bytes INTEGER NOT NULL,migrated_at TEXT NOT NULL)"
    )
    OperationLedger().ensure_schema(uow)
    migration_key = "legacy.entertainment.newapi-json-v1"
    if uow.query_one(
        "SELECT 1 AS found FROM entertainment_newapi_migrations WHERE migration_key=?",
        (migration_key,),
    ) is not None:
        return

    defaults = _legacy_newapi_directories()
    accounts_dir = Path(accounts_directory) if accounts_directory is not None else defaults[0]
    history_dir = Path(history_directory) if history_directory is not None else defaults[1]
    digest = hashlib.sha256()
    state = {"files": 0, "bytes": 0}
    digest.update(b"accounts\0")
    account_files = _read_newapi_json_files(accounts_dir, MAX_ACCOUNT_LIST_FILE_BYTES, digest, state)
    digest.update(b"history\0")
    history_files = _read_newapi_json_files(history_dir, MAX_CHECKIN_HISTORY_BYTES, digest, state)
    imported_accounts: list[tuple[str, int, str, str, str, str, str, int, str]] = []
    imported_history: list[tuple[str, int, str, int, str, str, str, str]] = []
    for user_id, rows in account_files:
        if not user_id or len(user_id) > 128 or not isinstance(rows, list) or len(rows) > MAX_ACCOUNT_LIST_ROWS:
            raise ValueError("NewAPI legacy account file has invalid owner or row count")
        for position, row in enumerate(rows, start=1):
            api_user_id, mode, secret, base_url, label, auto_checkin, extra_json = _legacy_newapi_account(row)
            imported_accounts.append(
                (user_id, position, api_user_id, mode, secret, base_url, label, auto_checkin, extra_json)
            )
    for user_id, rows in history_files:
        if not user_id or len(user_id) > 128 or not isinstance(rows, list):
            raise ValueError("NewAPI legacy history file has invalid owner or root type")
        for position, row in enumerate(rows[:MAX_CHECKIN_HISTORY_ROWS], start=1):
            at, account_index, api_user_id, base_url, summary, source = _legacy_newapi_history(row)
            imported_history.append(
                (user_id, position, at, account_index, api_user_id, base_url, summary, source)
            )

    if uow.query_one(f"SELECT 1 AS found FROM {NEWAPI_ACCOUNTS_TABLE} LIMIT 1") is not None:
        raise RuntimeError("NewAPI account rows exist without a legacy migration receipt")
    if uow.query_one(f"SELECT 1 AS found FROM {NEWAPI_HISTORY_TABLE} LIMIT 1") is not None:
        raise RuntimeError("NewAPI history rows exist without a legacy migration receipt")
    migrated_at = str(occurred_at or datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    uow.executemany(
        f"INSERT INTO {NEWAPI_ACCOUNTS_TABLE}(user_id,position,api_user_id,mode,secret,base_url,label,auto_checkin,extra_json) "
        "VALUES(?,?,?,?,?,?,?,?,?)",
        imported_accounts,
    )
    uow.executemany(
        f"INSERT INTO {NEWAPI_HISTORY_TABLE}(user_id,position,at,account_index,api_user_id,base_url,summary,source) "
        "VALUES(?,?,?,?,?,?,?,?)",
        imported_history,
    )
    uow.execute(
        "INSERT INTO entertainment_newapi_migrations(migration_key,snapshot_sha256,account_files,history_files,"
        "account_rows,history_rows,snapshot_bytes,migrated_at) VALUES(?,?,?,?,?,?,?,?)",
        (
            migration_key,
            digest.hexdigest(),
            len(account_files),
            len(history_files),
            len(imported_accounts),
            len(imported_history),
            state["bytes"],
            migrated_at,
        ),
    )


__all__ = [
    "apply_entertainment",
    "apply_entertainment_newapi",
    "apply_entertainment_rooms",
]
