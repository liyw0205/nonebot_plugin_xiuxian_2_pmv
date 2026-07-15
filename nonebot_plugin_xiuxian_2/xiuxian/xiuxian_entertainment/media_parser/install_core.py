"""从 GitHub 拉取 core 的旧逻辑已废弃。

媒体解析改为本插件 `native.py` 直接提链并请求各平台，无需下载外部插件。
保留此模块仅为兼容旧 import；调用 ensure_vendor_core 不再联网。
"""
from __future__ import annotations

from pathlib import Path

from nonebot.log import logger

_VENDOR_ROOT = Path(__file__).resolve().parent / "vendor"


def vendor_core_ready() -> bool:
    # 原生实现始终可用
    return True


def ensure_vendor_core(*, force: bool = False) -> Path:
    if force:
        logger.info("娱乐媒体解析：已改用本插件原生实现，忽略 force 下载 core")
    return _VENDOR_ROOT
