from copy import deepcopy
from datetime import datetime
import json
import os
from pathlib import Path
from threading import RLock
from typing import Any

try:
    from nonebot.log import logger
except Exception:  # pragma: no cover
    logger = None


_LOCKS_GUARD = RLock()
_PATH_LOCKS: dict[Path, RLock] = {}


def _path_lock(path: Path) -> RLock:
    resolved = path.expanduser().resolve()
    with _LOCKS_GUARD:
        return _PATH_LOCKS.setdefault(resolved, RLock())


def _backup_invalid_json(file_path: Path) -> Path | None:
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    backup_path = file_path.with_name(f"{file_path.name}.invalid.{timestamp}.bak")
    try:
        file_path.replace(backup_path)
        if logger:
            logger.warning(f"Invalid JSON backed up before reset: {file_path} -> {backup_path}")
        return backup_path
    except OSError as exc:
        if logger:
            logger.warning(f"Failed to backup invalid JSON {file_path}: {exc}")
        return None


def load_json_file(
    path: str | Path,
    default: Any,
    expected_type: type | tuple[type, ...] | None = None,
) -> Any:
    file_path = Path(path)
    with _path_lock(file_path):
        try:
            with file_path.open("r", encoding="utf-8") as f:
                data = json.load(f)
            if expected_type is not None and not isinstance(data, expected_type):
                raise TypeError(f"JSON 根类型不是 {expected_type!r}")
            return data
        except FileNotFoundError:
            save_json_file(file_path, default)
            return deepcopy(default)
        except (json.JSONDecodeError, TypeError):
            _backup_invalid_json(file_path)
            save_json_file(file_path, default)
            return deepcopy(default)


def save_json_file(path: str | Path, data: Any, **dump_kwargs) -> None:
    file_path = Path(path)
    with _path_lock(file_path):
        file_path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = file_path.with_name(f".{file_path.name}.{os.getpid()}.tmp")
        try:
            dump_options = {"ensure_ascii": False, "indent": 4}
            dump_options.update(dump_kwargs)
            with temp_path.open("w", encoding="utf-8") as f:
                json.dump(data, f, **dump_options)
                f.flush()
                os.fsync(f.fileno())
            temp_path.replace(file_path)
        finally:
            temp_path.unlink(missing_ok=True)


def update_json_file(
    path: str | Path,
    default: Any,
    updater,
    *,
    expected_type: type | tuple[type, ...] | None = None,
    **dump_kwargs,
) -> Any:
    file_path = Path(path)
    with _path_lock(file_path):
        current = load_json_file(file_path, default, expected_type)
        updated = updater(deepcopy(current))
        if updated is None:
            updated = current
        if expected_type is not None and not isinstance(updated, expected_type):
            raise TypeError(f"更新后的 JSON 根类型不是 {expected_type!r}")
        save_json_file(file_path, updated, **dump_kwargs)
        return updated


def delete_json_file(path: str | Path) -> bool:
    """Delete a JSON state file while serializing against readers and writers."""
    file_path = Path(path)
    with _path_lock(file_path):
        try:
            file_path.unlink()
            return True
        except FileNotFoundError:
            return False


def safe_json_dumps(value: Any, default: Any = None) -> str:
    if value is None and default is not None:
        value = default
    try:
        return json.dumps(value, ensure_ascii=False)
    except (TypeError, ValueError):
        return json.dumps(str(value), ensure_ascii=False)


def safe_json_loads(value: Any, default: Any, expected_type: type | tuple[type, ...] | None = None) -> Any:
    if value in (None, ""):
        return deepcopy(default)
    try:
        data = json.loads(value)
    except (TypeError, ValueError):
        return deepcopy(default)
    if expected_type is not None and not isinstance(data, expected_type):
        return deepcopy(default)
    return data
