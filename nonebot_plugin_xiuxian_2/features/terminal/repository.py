"""Injected ports of the Web terminal owner.

Everything the terminal touches is process-scoped and therefore hostile to test:
the password, the PTY factory, the ``os`` syscalls and ``select``.  The four
protocols below name those ports so ``TerminalApplication`` documents its real
dependencies instead of importing modules at call time.  Nothing here performs
I/O; the production implementations are ``SubprocessPtyRunner``, ``os`` and
``select.select``, wired by ``TerminalApplication.__init__``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol


if TYPE_CHECKING:  # pragma: no cover - annotation only
    from .application import TerminalSession


class PasswordProvider(Protocol):
    """Return the configured terminal password, or ``None`` when unset."""

    def __call__(self) -> str | None: ...


class PtySessionFactory(Protocol):
    """Start one child shell attached to one PTY for one administrator."""

    def __call__(self, admin_id: str) -> "TerminalSession": ...


class OsAdapterPort(Protocol):
    """The four ``os`` syscalls the terminal needs on a session descriptor."""

    def close(self, fd: int) -> None: ...

    def read(self, fd: int, length: int) -> bytes: ...

    def write(self, fd: int, payload: bytes) -> int: ...

    def readlink(self, path: str) -> str: ...


class SelectPort(Protocol):
    """Wait for readability on a session descriptor with a timeout."""

    def __call__(
        self,
        read_list: list[Any],
        write_list: list[Any],
        error_list: list[Any],
        timeout: float,
    ) -> tuple[list[Any], list[Any], list[Any]]: ...


__all__ = ["OsAdapterPort", "PasswordProvider", "PtySessionFactory", "SelectPort"]
