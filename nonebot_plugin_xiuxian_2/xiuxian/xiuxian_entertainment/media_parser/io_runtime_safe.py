"""native 解析在线程池执行，避免阻塞 event loop。"""
from __future__ import annotations

from typing import Any

from ..io_runtime import run_blocking_io
from .native import parse_text_native


async def run_native_parse(text: str) -> list[dict[str, Any]]:
    # 小红书等需代理平台：单页超时已压短；总超时略放宽避免误杀
    return await run_blocking_io(parse_text_native, text, timeout=90)
