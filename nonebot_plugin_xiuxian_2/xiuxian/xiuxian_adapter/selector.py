from __future__ import annotations

import os
from dataclasses import dataclass
from importlib.machinery import PathFinder
from pathlib import Path
from typing import Literal


AdapterSource = Literal["vendor", "installed", "auto"]
ADAPTER_SOURCE_ENV = "XIUXIAN_ADAPTER_SOURCE"
VENDOR_ROOT = Path(__file__).resolve().parent / "vendor"

_ADAPTER_PATHS = {
    "qq": VENDOR_ROOT / "adapter_qq" / "nonebot" / "adapters",
    "onebot": VENDOR_ROOT / "adapter_onebot" / "nonebot" / "adapters",
}


@dataclass(frozen=True, slots=True)
class AdapterSelection:
    requested: AdapterSource
    effective: dict[str, str]


def normalize_adapter_source(value: object) -> AdapterSource:
    source = str(value or "vendor").strip().lower()
    if source not in {"vendor", "installed", "auto"}:
        raise ValueError(
            "xiuxian_adapter_source must be one of: vendor, installed, auto"
        )
    return source  # type: ignore[return-value]


def get_requested_adapter_source() -> AdapterSource:
    configured = os.getenv(ADAPTER_SOURCE_ENV)
    if configured is None:
        try:
            from nonebot import get_driver

            configured = getattr(get_driver().config, "xiuxian_adapter_source", None)
        except Exception:
            configured = None
    return normalize_adapter_source(configured)


def _installed_adapter_available(adapter_name: str) -> bool:
    try:
        import nonebot.adapters as nonebot_adapters

        vendor_paths = {str(path) for path in _ADAPTER_PATHS.values()}
        search_paths = [
            str(path)
            for path in nonebot_adapters.__path__
            if str(path) not in vendor_paths
        ]
        return (
            PathFinder.find_spec(
                f"nonebot.adapters.{adapter_name}",
                search_paths,
            )
            is not None
        )
    except (ImportError, ModuleNotFoundError, ValueError):
        return False


def configure_adapter_paths(source: object = None) -> AdapterSelection:
    requested = normalize_adapter_source(
        get_requested_adapter_source() if source is None else source
    )
    import nonebot.adapters as nonebot_adapters

    adapter_path = getattr(nonebot_adapters, "__path__", None)
    if adapter_path is None:
        return AdapterSelection(requested=requested, effective={})

    vendor_paths = {str(path) for path in _ADAPTER_PATHS.values()}
    current = [path for path in map(str, adapter_path) if path not in vendor_paths]
    while len(adapter_path):
        adapter_path.pop()
    for path in current:
        adapter_path.append(path)

    installed = {
        name: _installed_adapter_available(name)
        for name in _ADAPTER_PATHS
    }
    effective: dict[str, str] = {}

    if requested == "installed":
        effective = {
            name: "installed" if available else "unavailable"
            for name, available in installed.items()
        }
        return AdapterSelection(requested=requested, effective=effective)

    for name, path in _ADAPTER_PATHS.items():
        if not path.exists():
            effective[name] = "installed" if installed[name] else "unavailable"
            continue

        path_text = str(path)
        if requested == "vendor":
            adapter_path.insert(0, path_text)
            effective[name] = "vendor"
        elif installed[name]:
            adapter_path.append(path_text)
            effective[name] = "installed"
        else:
            adapter_path.append(path_text)
            effective[name] = "vendor"

    return AdapterSelection(requested=requested, effective=effective)


def classify_module_source(module_file: object) -> str:
    if not module_file:
        return "unavailable"
    path = Path(str(module_file)).resolve(strict=False)
    try:
        path.relative_to(VENDOR_ROOT)
    except ValueError:
        return "installed"
    return "vendor"
