from __future__ import annotations

import json
from pathlib import Path
from typing import Any


class JsonDocumentReader:
    def read_object(self, path: str | Path) -> dict[str, Any]:
        source = Path(path)
        if not source.is_file():
            raise FileNotFoundError(source)
        try:
            value = json.loads(source.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            raise ValueError(f"invalid JSON document: {source}") from exc
        if not isinstance(value, dict):
            raise ValueError(f"JSON document must contain an object: {source}")
        return value


__all__ = ["JsonDocumentReader"]
