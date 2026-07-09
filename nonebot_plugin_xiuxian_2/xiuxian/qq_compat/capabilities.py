from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping


def _as_bool(value: Any, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    return default


@dataclass(frozen=True)
class QQCapabilities:
    markdown: bool = True
    keyboard: bool = True
    interaction: bool = True
    full_message: bool = True

    @classmethod
    def from_mapping(cls, values: Mapping[str, Any] | None) -> "QQCapabilities":
        values = values or {}
        return cls(
            markdown=_as_bool(values.get("markdown"), True),
            keyboard=_as_bool(values.get("keyboard"), True),
            interaction=_as_bool(values.get("interaction"), True),
            full_message=_as_bool(values.get("full_message"), True),
        )


class QQCapabilityRegistry:
    def __init__(
        self,
        values: Mapping[str, Mapping[str, Any]] | None = None,
        *,
        default: QQCapabilities | None = None,
    ) -> None:
        self._default = default or QQCapabilities()
        self._values = {
            str(app_id): QQCapabilities.from_mapping(capabilities)
            for app_id, capabilities in (values or {}).items()
        }

    def get(self, bot_or_app_id: Any) -> QQCapabilities:
        app_id = str(getattr(bot_or_app_id, "self_id", bot_or_app_id) or "")
        return self._values.get(app_id, self._default)


__all__ = ["QQCapabilities", "QQCapabilityRegistry"]
