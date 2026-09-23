from __future__ import annotations

import json
import os
from copy import deepcopy
from pathlib import Path


def load_json_list(path: str | Path) -> list[dict]:
    file_path = Path(path)
    try:
        value = json.loads(file_path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        save_json_list(file_path, [])
        return []
    except (OSError, json.JSONDecodeError, TypeError):
        return []
    return [dict(row) for row in value if isinstance(row, dict)] if isinstance(value, list) else []


def save_json_list(path: str | Path, value: list[dict]) -> None:
    file_path = Path(path)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = file_path.with_name(f".{file_path.name}.{os.getpid()}.tmp")
    try:
        with temp_path.open("w", encoding="utf-8") as handle:
            json.dump(deepcopy(value), handle, ensure_ascii=False, indent=2)
            handle.flush()
            os.fsync(handle.fileno())
        temp_path.replace(file_path)
    finally:
        temp_path.unlink(missing_ok=True)
