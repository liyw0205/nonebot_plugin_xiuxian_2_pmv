from __future__ import annotations

import inspect
from collections import Counter
from typing import Any, Callable

from nonebot.log import logger

from ..xiuxian_utils import db_backend


_FAILURE_COUNTS: Counter[tuple[str, str]] = Counter()


def _failure_category(exc: Exception) -> str:
    if isinstance(exc, db_backend.Error):
        return "database"
    if isinstance(exc, OSError):
        return "filesystem"
    if isinstance(exc, (TypeError, ValueError)):
        return "state"
    return "unexpected"


def get_auction_job_failure_count(job_type: str, category: str | None = None) -> int:
    if category is not None:
        return _FAILURE_COUNTS[(str(job_type), str(category))]
    return sum(
        count
        for (recorded_job, _), count in _FAILURE_COUNTS.items()
        if recorded_job == str(job_type)
    )


def reset_auction_job_failure_counts() -> None:
    _FAILURE_COUNTS.clear()


async def run_auction_job(
    job_type: str,
    operation: Callable[[], Any],
    *,
    suppress: bool = False,
) -> Any:
    """Run an auction background job with categorized failure accounting."""
    try:
        result = operation()
        if inspect.isawaitable(result):
            result = await result
        return result
    except Exception as exc:
        category = _failure_category(exc)
        key = (str(job_type), category)
        _FAILURE_COUNTS[key] += 1
        count = _FAILURE_COUNTS[key]
        logger.opt(exception=exc).error(
            "拍卖后台任务失败：task_type={} category={} failure_count={}",
            job_type,
            category,
            count,
        )
        if not suppress:
            raise
        return None


__all__ = [
    "get_auction_job_failure_count",
    "reset_auction_job_failure_counts",
    "run_auction_job",
]
