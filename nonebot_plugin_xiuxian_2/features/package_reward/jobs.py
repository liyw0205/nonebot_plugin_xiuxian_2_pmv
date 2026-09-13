"""This feature has no scheduled jobs."""

from typing import Any


def jobs() -> tuple[Any, ...]:
    return ()


__all__ = ["jobs"]
