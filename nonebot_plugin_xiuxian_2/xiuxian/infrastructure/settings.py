from __future__ import annotations

from threading import RLock
from typing import Any, Callable


_MISSING = object()


class SettingsProvider:
    """统一 NoneBot 配置、持久配置与默认值的读取优先级。"""

    def __init__(
        self,
        driver_config_provider: Callable[[], Any],
        persisted_provider: Callable[[], Any],
    ) -> None:
        self._driver_config_provider = driver_config_provider
        self._persisted_provider = persisted_provider
        self._lock = RLock()
        self._driver_config: Any = None
        self._persisted: Any = None
        self.reload()

    def reload(self) -> None:
        with self._lock:
            self._driver_config = self._driver_config_provider()
            self._persisted = self._persisted_provider()

    def get(self, name: str, default: Any = None) -> Any:
        with self._lock:
            value = getattr(self._driver_config, name, _MISSING)
            if value is not _MISSING and value is not None:
                return value
            value = getattr(self._persisted, name, _MISSING)
            if value is not _MISSING and value is not None:
                return value
            return default

    def get_bool(self, name: str, default: bool = False) -> bool:
        value = self.get(name, default)
        if isinstance(value, str):
            return value.strip().lower() in {"1", "true", "yes", "on"}
        return bool(value)

    def get_int(self, name: str, default: int, *, minimum: int | None = None) -> int:
        try:
            value = int(float(self.get(name, default)))
        except (TypeError, ValueError):
            value = default
        return max(minimum, value) if minimum is not None else value

    def get_str(self, name: str, default: str = "") -> str:
        value = self.get(name, default)
        return default if value is None else str(value)


def _driver_config() -> Any:
    from nonebot import get_driver

    return get_driver().config


def _persisted_config() -> Any:
    from ..xiuxian_config import XiuConfig

    return XiuConfig()


settings = SettingsProvider(_driver_config, _persisted_config)


def get_xiuxian_settings() -> SettingsProvider:
    return settings


__all__ = ["SettingsProvider", "get_xiuxian_settings", "settings"]
