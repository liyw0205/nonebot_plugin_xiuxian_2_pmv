"""管理员/全服批处理后台任务：避免 Matcher 同步 while 卡死事件循环。"""
from __future__ import annotations

import asyncio
import inspect
import time
from collections.abc import Awaitable, Callable
from typing import Any

from nonebot.log import logger

from .utils import handle_send

# job_key -> (task, started_at)
_JOBS: dict[str, tuple[asyncio.Task, float]] = {}


def is_job_running(job_key: str) -> bool:
    task = _JOBS.get(str(job_key))
    return bool(task and not task[0].done())


async def spawn_admin_job(
    bot: Any,
    event: Any,
    *,
    job_key: str,
    start_msg: str,
    work: Callable[[], Any],
    done_msg: Callable[[Any], str],
    fail_prefix: str = "后台任务失败",
) -> bool:
    """启动后台任务。

    返回 True 表示已启动；False 表示同 key 仍在跑。
    work 可为同步或协程函数；同步会丢到 to_thread。
    """
    key = str(job_key).strip() or f"job:{time.time_ns()}"
    if is_job_running(key):
        await handle_send(
            bot,
            event,
            f"⏳ 同类后台任务仍在执行中（{key}），请稍后再试。",
        )
        return False

    await handle_send(bot, event, start_msg)

    async def _runner() -> None:
        try:
            if inspect.iscoroutinefunction(work):
                result = await work()  # type: ignore[misc]
            else:
                result = await asyncio.to_thread(work)
            msg = done_msg(result)
            if msg:
                await handle_send(bot, event, msg)
        except Exception as e:
            logger.opt(exception=e).error(f"后台任务失败 key={key}")
            try:
                await handle_send(bot, event, f"{fail_prefix}：{e}")
            except Exception:
                pass
        finally:
            cur = _JOBS.get(key)
            if cur and cur[0] is asyncio.current_task():
                _JOBS.pop(key, None)

    task = asyncio.create_task(_runner(), name=f"xiuxian-bg:{key}")
    _JOBS[key] = (task, time.monotonic())
    return True


def run_chunked_until_done(step: Callable[[], Any], *, sleep_every: int = 1) -> Any:
    """同步批处理：反复 step() 直到 status!='applied' 或 completed>=total。

    step 返回对象需有 status/completed/total（与现有 batch service 一致）。
    """
    last = None
    n = 0
    while True:
        last = step()
        n += 1
        status = getattr(last, "status", None)
        completed = int(getattr(last, "completed", 0) or 0)
        total = int(getattr(last, "total", 0) or 0)
        if status != "applied" or completed >= total:
            return last
        # 纯同步路径：给 CPU/锁一点喘息（to_thread 内 sleep 不堵事件循环）
        if sleep_every > 0 and n % sleep_every == 0:
            time.sleep(0)


__all__ = [
    "is_job_running",
    "spawn_admin_job",
    "run_chunked_until_done",
]
