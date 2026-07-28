from __future__ import annotations

import os

_TRUE_VALUES = {"1", "true", "yes", "on"}
_FALSE_VALUES = {"0", "false", "no", "off"}


def web_enabled_from_env() -> bool:
    value = os.environ.setdefault("XIUXIAN_WEB_STATUS", "true").strip().lower()
    if value in _TRUE_VALUES:
        return True
    if value in _FALSE_VALUES:
        return False
    raise ValueError(
        "XIUXIAN_WEB_STATUS 必须为 true/false、1/0、yes/no 或 on/off"
    )
