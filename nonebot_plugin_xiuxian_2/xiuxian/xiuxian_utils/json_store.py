from copy import deepcopy
from datetime import datetime
import json
from pathlib import Path
from typing import Any

try:
    from nonebot.log import logger
except Exception:  # pragma: no cover
    logger = None


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


def load_json_file(path: str | Path, default: Any) -> Any:
    file_path = Path(path)
    try:
        with file_path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        save_json_file(file_path, default)
        return deepcopy(default)
    except json.JSONDecodeError:
        _backup_invalid_json(file_path)
        save_json_file(file_path, default)
        return deepcopy(default)


def save_json_file(path: str | Path, data: Any, **dump_kwargs) -> None:
    file_path = Path(path)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    with file_path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4, **dump_kwargs)


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
