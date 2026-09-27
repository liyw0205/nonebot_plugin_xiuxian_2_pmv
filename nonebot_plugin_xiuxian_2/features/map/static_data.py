from __future__ import annotations

from pathlib import Path
from typing import Any, Protocol


class JsonObjectReader(Protocol):
    def read_object(self, path: str | Path) -> dict[str, Any]: ...


class MapStaticDataProvider:
    """Read the configured map document without retaining a parsed copy."""

    def __init__(self, reader: JsonObjectReader, path: str | Path) -> None:
        self.reader = reader
        self.path = Path(path)

    def load(self) -> dict[str, Any]:
        return self.reader.read_object(self.path)


__all__ = ["JsonObjectReader", "MapStaticDataProvider"]
