from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping
from contextlib import AbstractContextManager
from datetime import datetime
from typing import Any, Callable, Protocol, TypeVar

from ..result import ReplyPlan


class Clock(Protocol):
    def now(self) -> datetime: ...


class RandomSource(Protocol):
    def random(self) -> float: ...

    def randint(self, start: int, end: int) -> int: ...


class IdGenerator(Protocol):
    def new_id(self) -> str: ...


class MessageGateway(Protocol):
    def send(self, context: Any, reply: ReplyPlan) -> Any: ...


T = TypeVar("T")


class Repository(Protocol[T]):
    def get(self, identifier: str) -> T | None: ...


class UnitOfWork(AbstractContextManager["UnitOfWork"], Protocol):
    """Transaction boundary shared by application use cases."""

    def __enter__(self) -> "UnitOfWork": ...

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> bool | None: ...

    def execute(self, sql: str, params: Any = None) -> Any: ...

    def query_one(self, sql: str, params: Any = None) -> Mapping[str, Any] | None: ...

    def query_all(self, sql: str, params: Any = None) -> list[Mapping[str, Any]]: ...


class OperationLedgerPort(Protocol):
    def begin(self, uow: UnitOfWork, operation_id: str, action: str, payload: Any) -> Any: ...

    def finish(self, uow: UnitOfWork, outcome: Any) -> None: ...


__all__ = [
    "Clock",
    "IdGenerator",
    "MessageGateway",
    "RandomSource",
    "Repository",
    "UnitOfWork",
    "OperationLedgerPort",
]
