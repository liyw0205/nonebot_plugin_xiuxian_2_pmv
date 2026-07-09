from __future__ import annotations

import importlib
from dataclasses import asdict

from .selector import classify_module_source, configure_adapter_paths


def get_adapter_diagnostics(source: object = None) -> dict:
    selection = configure_adapter_paths(source)
    adapters = {}
    for name in ("onebot", "qq"):
        module_name = f"nonebot.adapters.{name}"
        try:
            module = importlib.import_module(module_name)
            module_file = getattr(module, "__file__", None)
            adapters[name] = {
                "source": classify_module_source(module_file),
                "module": module_name,
                "file": str(module_file or ""),
                "version": str(getattr(module, "__version__", "") or ""),
            }
        except Exception as exc:
            adapters[name] = {
                "source": "unavailable",
                "module": module_name,
                "file": "",
                "version": "",
                "error": str(exc),
            }

    return {"selection": asdict(selection), "adapters": adapters}
