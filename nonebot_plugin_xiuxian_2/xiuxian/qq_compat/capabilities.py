from __future__ import annotations

import json
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

    @classmethod
    def from_config(cls, config: Any) -> "QQCapabilityRegistry":
        raw = getattr(config, "xiuxian_qq_capabilities", None)
        if raw in (None, ""):
            return cls()
        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except json.JSONDecodeError as exc:
                raise ValueError("xiuxian_qq_capabilities 必须是合法 JSON") from exc
        if not isinstance(raw, Mapping):
            raise ValueError("xiuxian_qq_capabilities 必须是 AppID 到能力配置的映射")
        default_values = raw.get("default")
        bots = raw.get("bots", raw)
        if not isinstance(bots, Mapping):
            raise ValueError("xiuxian_qq_capabilities.bots 必须是映射")
        values = {
            str(app_id): capability
            for app_id, capability in bots.items()
            if app_id != "default" and isinstance(capability, Mapping)
        }
        default = QQCapabilities.from_mapping(
            default_values if isinstance(default_values, Mapping) else None
        )
        return cls(values, default=default)


__all__ = ["QQCapabilities", "QQCapabilityRegistry"]
