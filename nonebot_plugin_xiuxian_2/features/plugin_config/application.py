from __future__ import annotations

import ast
from pathlib import Path
from typing import Any, Callable

from ...paths import get_paths
from .schema import CONFIG_EDITABLE_FIELDS


class PluginConfigApplication:
    """Own the legacy XiuConfig panel schema and its source-file boundary."""

    def __init__(
        self,
        config_factory: Callable[[], Any] | None = None,
        config_file: str | Path | None = None,
        writer: Callable[[Path, dict, dict[str, str]], tuple[bool, str]] | None = None,
    ) -> None:
        self._config_factory = config_factory or self._default_config_factory
        self._config_file = Path(config_file) if config_file else (
            get_paths().package_root / "xiuxian" / "xiuxian_config.py"
        )
        self._writer = writer

    @staticmethod
    def _default_config_factory():
        from ...xiuxian.xiuxian_config import XiuConfig

        return XiuConfig()

    @property
    def editable_fields(self) -> dict:
        return CONFIG_EDITABLE_FIELDS

    @property
    def config_file(self) -> Path:
        return self._config_file

    def get_values(self) -> dict[str, Any]:
        config = self._config_factory()
        return {
            field_name: getattr(config, field_name)
            for field_name in CONFIG_EDITABLE_FIELDS
            if hasattr(config, field_name)
        }

    def config_by_category(self) -> dict[str, list[dict[str, Any]]]:
        current_config = self.get_values()
        for field_name, value in current_config.items():
            field_type = CONFIG_EDITABLE_FIELDS[field_name]["type"]
            if field_type in ("list[int]", "list[str]"):
                current_config[field_name] = self.format_list_value_for_display(
                    value, field_type
                )

        grouped: dict[str, list[dict[str, Any]]] = {}
        for field_name, field_info in CONFIG_EDITABLE_FIELDS.items():
            category = field_info["category"]
            config_item = {
                "field_name": field_name,
                "name": field_info["name"],
                "description": field_info["description"],
                "type": field_info["type"],
                "value": current_config.get(field_name, ""),
            }
            if field_info["type"] == "select" and "options" in field_info:
                config_item["options"] = field_info["options"]
            grouped.setdefault(category, []).append(config_item)
        return grouped

    @staticmethod
    def format_list_value_for_display(value: Any, field_type: str) -> str:
        if not value:
            return ""

        try:
            if isinstance(value, str):
                value = ast.literal_eval(value)
            if isinstance(value, (list, tuple)):
                if field_type == "list[int]":
                    return ", ".join(str(item) for item in value)
                return ", ".join(str(item).strip("\"'") for item in value)
            return str(value)
        except (ValueError, SyntaxError):
            return (
                str(value)
                .replace("[", "")
                .replace("]", "")
                .replace('"', "")
                .replace("'", "")
            )

    def save_values(
        self, new_values: dict | None, *, include_restart_notice: bool = False
    ) -> tuple[bool, str]:
        writer = self._writer
        if writer is None:
            from ...xiuxian.xiuxian_utils.config_literal import write_config_values

            writer = write_config_values
        field_types = {
            name: meta.get("type", "str")
            for name, meta in CONFIG_EDITABLE_FIELDS.items()
        }
        ok, message = writer(self._config_file, new_values or {}, field_types)
        if include_restart_notice and ok and "重启" not in message:
            message = message.rstrip("。") + "，重启机器人后生效。"
        return ok, message


__all__ = ["PluginConfigApplication"]
