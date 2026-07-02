from __future__ import annotations

from pathlib import Path
from typing import Iterable


VENDOR_ROOT = Path(__file__).resolve().parent / "vendor"


def _vendor_adapter_paths() -> Iterable[Path]:
    yield VENDOR_ROOT / "adapter_qq" / "nonebot" / "adapters"
    yield VENDOR_ROOT / "adapter_onebot" / "nonebot" / "adapters"


def ensure_vendored_adapters(prefer_vendored: bool = True) -> None:
    """
    Expose bundled NoneBot adapter packages through nonebot.adapters.

    Upstream adapters keep the canonical package path `nonebot.adapters.*`.
    Instead of rewriting upstream imports, this extends `nonebot.adapters.__path__`
    so imports such as `nonebot.adapters.qq` can resolve to bundled sources.
    """
    try:
        import nonebot.adapters as nonebot_adapters
    except Exception:
        return

    adapter_path = getattr(nonebot_adapters, "__path__", None)
    if adapter_path is None:
        return

    current = [str(path) for path in adapter_path]
    for path in _vendor_adapter_paths():
        if not path.exists():
            continue

        path_text = str(path)
        if path_text in current:
            continue

        if prefer_vendored and hasattr(adapter_path, "insert"):
            adapter_path.insert(0, path_text)
            current.insert(0, path_text)
        elif hasattr(adapter_path, "append"):
            adapter_path.append(path_text)
            current.append(path_text)
