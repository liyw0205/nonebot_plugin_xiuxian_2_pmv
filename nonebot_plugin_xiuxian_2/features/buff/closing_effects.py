from __future__ import annotations

from typing import Any, Protocol


class ClosingEffects(Protocol):
    def on_closing_settled(self, *, payload: dict[str, Any], event_id: str) -> None: ...


class NullClosingEffects:
    def on_closing_settled(self, *, payload: dict[str, Any], event_id: str) -> None:
        del payload, event_id


__all__ = ["ClosingEffects", "NullClosingEffects"]
