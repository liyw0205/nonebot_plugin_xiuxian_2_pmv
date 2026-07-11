"""娱乐模块的有界网络 I/O 与媒体发送运行时。"""
from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from functools import partial
from typing import Any, TypeVar

from nonebot.log import logger


T = TypeVar("T")

BLOCKING_IO_TIMEOUT = 35.0
IMAGE_SEND_TIMEOUT = 60.0
AUDIO_SEND_TIMEOUT = 120.0
VIDEO_SEND_TIMEOUT = 240.0

_blocking_io_slots = asyncio.Semaphore(4)
_media_send_slots = asyncio.Semaphore(3)


class EntertainmentIOTimeout(TimeoutError):
    """娱乐模块的网络或媒体操作超过允许等待时间。"""


def _consume_task_result(task: asyncio.Task[Any]) -> None:
    try:
        task.result()
    except asyncio.CancelledError:
        logger.debug("娱乐模块超时后台任务已取消")
    except Exception as exc:
        logger.warning(f"娱乐模块超时后台任务最终失败: {exc}")


async def run_blocking_io(
    func: Callable[..., T],
    /,
    *args: Any,
    timeout: float = BLOCKING_IO_TIMEOUT,
    **kwargs: Any,
) -> T:
    """在线程中执行同步 I/O，并限制并发和调用方等待时间。

    超时后底层线程无法被 Python 强制终止，因此任务会在后台自然结束，并在结束前
    继续占用并发槽，避免慢请求越积越多。
    """

    acquired = asyncio.Event()

    async def runner() -> T:
        async with _blocking_io_slots:
            acquired.set()
            call = partial(func, *args, **kwargs)
            return await asyncio.to_thread(call)

    task = asyncio.create_task(runner())
    try:
        return await asyncio.wait_for(asyncio.shield(task), timeout=timeout)
    except asyncio.TimeoutError as exc:
        if acquired.is_set():
            task.add_done_callback(_consume_task_result)
        else:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        raise EntertainmentIOTimeout(f"网络操作超过 {timeout:g} 秒") from exc
    except asyncio.CancelledError:
        if acquired.is_set():
            task.add_done_callback(_consume_task_result)
        else:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        raise


async def run_media_send(
    send: Callable[[], Awaitable[T]],
    *,
    timeout: float,
    media_type: str,
) -> T:
    """限制媒体发送并发，并在适配器请求长时间无响应时主动结束等待。"""
    try:
        async with asyncio.timeout(timeout):
            async with _media_send_slots:
                return await send()
    except asyncio.TimeoutError as exc:
        raise EntertainmentIOTimeout(
            f"{media_type}发送超过 {timeout:g} 秒，请稍后重试"
        ) from exc
