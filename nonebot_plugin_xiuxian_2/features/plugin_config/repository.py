from __future__ import annotations

from pathlib import Path
from typing import Any, Protocol


class ConfigWriter(Protocol):
    def __call__(
        self, path: Path, values: dict[str, Any], field_types: dict[str, str]
    ) -> tuple[bool, str]: ...


__all__ = ["ConfigWriter"]
