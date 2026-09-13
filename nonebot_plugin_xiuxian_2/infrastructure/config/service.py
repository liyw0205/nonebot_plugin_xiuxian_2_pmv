from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Any, Callable, Mapping

from ..filesystem import atomic_write


@dataclass(frozen=True)
class SettingDefinition:
    name: str
    type: type | str
    default: Any = None
    secret: bool = False
    reloadable: bool = False
    description: str = ""
    env_name: str | None = None

    @property
    def environment_name(self) -> str:
        return self.env_name or self.name.upper()


class Settings:
    """Immutable, typed settings snapshot with documented source precedence."""

    def __init__(self, values: Mapping[str, Any], sources: Mapping[str, str], definitions: Mapping[str, SettingDefinition]):
        self._values = MappingProxyType(dict(values))
        self._sources = MappingProxyType(dict(sources))
        self._definitions = MappingProxyType(dict(definitions))

    def get(self, name: str, default: Any = None) -> Any:
        return self._values.get(name, default)

    def __getattr__(self, name: str) -> Any:
        try:
            return self._values[name]
        except KeyError as exc:
            raise AttributeError(name) from exc

    def source(self, name: str) -> str:
        return self._sources.get(name, "default")

    def definition(self, name: str) -> SettingDefinition:
        try:
            return self._definitions[name]
        except KeyError as exc:
            raise KeyError(name) from exc

    def items(self):
        return tuple(self._values.items())

    def as_dict(self, *, redact_secrets: bool = True) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for name, value in self._values.items():
            definition = self._definitions.get(name)
            result[name] = "***" if redact_secrets and definition and definition.secret else value
        return result

    def summary(self) -> dict[str, dict[str, Any]]:
        return {
            name: {"configured": value not in (None, ""), "source": self.source(name)}
            for name, value in self._values.items()
        }

    def to_dict(self, *, redact_secrets: bool = True) -> dict[str, Any]:
        return self.as_dict(redact_secrets=redact_secrets)


def _coerce(value: Any, target: type | str) -> Any:
    if value is None:
        return None
    target_name = target if isinstance(target, str) else target.__name__
    if target_name in {"str", "string"}:
        return str(value)
    if target_name in {"int", "integer"}:
        return int(float(value))
    if target_name in {"float", "number"}:
        return float(value)
    if target_name in {"bool", "boolean"}:
        if isinstance(value, str):
            normalized = value.strip().lower()
            if normalized in {"1", "true", "yes", "on"}:
                return True
            if normalized in {"0", "false", "no", "off", ""}:
                return False
            raise ValueError(f"invalid boolean: {value}")
        return bool(value)
    if target_name in {"json", "object", "list", "dict"} and isinstance(value, str):
        return json.loads(value)
    if target in (list, dict) and not isinstance(value, target):
        return target(value)
    return value


class ConfigService:
    """Reads env > deployment config > JSON config > code defaults."""

    def __init__(
        self,
        definitions: list[SettingDefinition] | tuple[SettingDefinition, ...],
        *,
        config_path: str | Path | None = None,
        environment: Mapping[str, str] | None = None,
        deployment_provider: Callable[[], Mapping[str, Any]] | None = None,
    ) -> None:
        self.definitions = {item.name: item for item in definitions}
        self.config_path = Path(config_path) if config_path else None
        self.environment = environment if environment is not None else os.environ
        self.deployment_provider = deployment_provider or (lambda: {})

    def _persisted(self) -> Mapping[str, Any]:
        if self.config_path is None or not self.config_path.is_file():
            return {}
        try:
            data = json.loads(self.config_path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            raise ValueError(f"invalid config file: {self.config_path}") from exc
        if not isinstance(data, dict):
            raise ValueError("config file must contain an object")
        return data

    def load(self) -> Settings:
        persisted = self._persisted()
        deployment = self.deployment_provider() or {}
        values: dict[str, Any] = {}
        sources: dict[str, str] = {}
        for name, definition in self.definitions.items():
            if definition.environment_name in self.environment:
                raw, source = self.environment[definition.environment_name], "environment"
            elif name in deployment:
                raw, source = deployment[name], "deployment"
            elif name in persisted:
                raw, source = persisted[name], "file"
            else:
                raw, source = definition.default, "default"
            try:
                values[name] = _coerce(raw, definition.type)
            except (TypeError, ValueError, json.JSONDecodeError) as exc:
                raise ValueError(f"invalid setting {name}: {exc}") from exc
            sources[name] = source
        return Settings(values, sources, self.definitions)

    def update(self, values: Mapping[str, Any]) -> Settings:
        current = dict(self._persisted())
        for name, value in values.items():
            definition = self.definitions.get(name)
            if definition is None:
                raise KeyError(name)
            if definition.secret and value in (None, ""):
                continue
            current[name] = _coerce(value, definition.type)
        if self.config_path is None:
            raise RuntimeError("config_path is required to persist settings")
        atomic_write(self.config_path, json.dumps(current, ensure_ascii=False, indent=2).encode("utf-8"))
        return self.load()

    def reload(self) -> Settings:
        """Read a fresh immutable snapshot without mutating the service."""
        return self.load()


__all__ = ["ConfigService", "SettingDefinition", "Settings"]
