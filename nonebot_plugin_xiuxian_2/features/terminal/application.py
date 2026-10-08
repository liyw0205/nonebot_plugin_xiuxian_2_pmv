"""Application boundary for the Web terminal.

The terminal is intentionally process-local.  A session is backed by one
PTY and one child process in the worker that created it; it is not a shared
multi-worker resource.  The owner process check makes accidental reuse of an
application instance after a fork fail closed instead of claiming that the
session store is transparently shared.
"""

from __future__ import annotations

import hmac
import os
import select
import subprocess
import threading
from dataclasses import dataclass
from typing import Any, Callable, MutableMapping

try:
    import pty
except ImportError:  # pragma: no cover - exercised by Windows deployments
    pty = None


class TerminalError(RuntimeError):
    """Base error for terminal lifecycle and I/O failures."""


class TerminalUnavailable(TerminalError):
    """Raised when this platform or worker cannot service the terminal."""


@dataclass
class TerminalSession:
    """The minimal process/FD contract used by the application."""

    fd: int
    pid: int
    process: Any
    closed: bool = False


class SubprocessPtyRunner:
    """Create an interactive login shell attached to a PTY."""

    def __init__(self, *, environ: MutableMapping[str, str] | None = None) -> None:
        self.environ = environ

    def __call__(self, _admin_id: str) -> TerminalSession:
        if os.name == "nt" or pty is None:
            raise TerminalUnavailable("Web终端功能仅支持 Linux/Unix 环境，Windows 不支持。")

        master_fd, slave_fd = pty.openpty()
        env = dict(self.environ if self.environ is not None else os.environ)
        # The authorization secret is process configuration, never shell data.
        env.pop("XIUXIAN_WEB_TERMINAL_PASSWORD", None)
        env.update(
            {
                "TERM": "xterm-256color",
                "LANG": "zh_CN.UTF-8",
                "PS1": r"\[\033[01;32m\]\u\[\033[00m\]:\[\033[01;34m\]\w\[\033[00m\]\$ ",
            }
        )
        try:
            process = subprocess.Popen(
                ["/bin/bash", "--login", "-i"],
                stdin=slave_fd,
                stdout=slave_fd,
                stderr=slave_fd,
                env=env,
                start_new_session=True,
                close_fds=True,
            )
        except Exception:
            os.close(master_fd)
            raise
        finally:
            os.close(slave_fd)

        try:
            import fcntl

            flags = fcntl.fcntl(master_fd, fcntl.F_GETFL)
            fcntl.fcntl(master_fd, fcntl.F_SETFL, flags | os.O_NONBLOCK)
        except Exception:
            try:
                os.close(master_fd)
            finally:
                process.terminate()
                process.wait(timeout=1)
            raise
        return TerminalSession(fd=master_fd, pid=process.pid, process=process)


class _DefaultOsAdapter:
    close = staticmethod(os.close)
    read = staticmethod(os.read)
    write = staticmethod(os.write)
    readlink = staticmethod(os.readlink)


class TerminalApplication:
    """Own terminal authorization and process-local PTY sessions."""

    AUTHORIZATION_TTL = 300

    def __init__(
        self,
        *,
        secret_provider: Callable[[], str | None] | None = None,
        runner: Callable[[str], TerminalSession] | None = None,
        session_factory: Callable[[str], TerminalSession] | None = None,
        os_adapter: Any | None = None,
        select_fn: Callable[..., Any] | None = None,
        owner_pid: int | None = None,
        clock: Callable[[], float] | None = None,
    ) -> None:
        self._secret_provider = secret_provider or (lambda: os.environ.get("XIUXIAN_WEB_TERMINAL_PASSWORD"))
        if runner is not None and session_factory is not None:
            raise ValueError("runner and session_factory are mutually exclusive")
        self._runner = session_factory or runner or SubprocessPtyRunner()
        self._os = os_adapter or _DefaultOsAdapter()
        self._select = select_fn or select.select
        self._owner_pid = os.getpid() if owner_pid is None else owner_pid
        self._clock = clock or __import__("time").time
        self._lock = threading.RLock()
        self._sessions: dict[str, TerminalSession] = {}

    @property
    def process_local(self) -> bool:
        return True

    def _check_owner(self) -> None:
        if os.getpid() != self._owner_pid:
            raise TerminalUnavailable("Web终端仅支持单进程 worker，不能跨 worker 共享会话。")

    def password_configured(self) -> bool:
        return bool((self._secret_provider() or "").strip())

    def authorize(self, session: MutableMapping[str, Any], password: str) -> bool:
        """Authorize a Flask session without ever persisting the password."""
        secret = self._secret_provider() or ""
        if not secret:
            return False
        if not hmac.compare_digest(str(password or ""), secret):
            return False
        session["terminal_authorized_until"] = self._clock() + self.AUTHORIZATION_TTL
        return True

    @staticmethod
    def _poll(session: TerminalSession) -> Any:
        poll = getattr(session.process, "poll", None)
        return poll() if callable(poll) else None

    def _close_locked(self, admin_id: str, session: TerminalSession) -> None:
        if session.closed:
            self._sessions.pop(admin_id, None)
            return
        session.closed = True
        if self._poll(session) is None:
            terminate = getattr(session.process, "terminate", None)
            if callable(terminate):
                try:
                    terminate()
                except OSError:
                    pass
            wait = getattr(session.process, "wait", None)
            if callable(wait):
                try:
                    wait(timeout=1)
                except (TypeError, TimeoutError, subprocess.TimeoutExpired):
                    kill = getattr(session.process, "kill", None)
                    if callable(kill):
                        try:
                            kill()
                        except OSError:
                            pass
        try:
            self._os.close(session.fd)
        except OSError:
            pass
        wait = getattr(session.process, "wait", None)
        if callable(wait):
            try:
                wait(timeout=0)
            except (TypeError, TimeoutError, subprocess.TimeoutExpired):
                # A process already observed as dead should be reapable without
                # blocking the request; live processes are terminated by close_all.
                pass
        self._sessions.pop(admin_id, None)

    def _refresh_locked(self, admin_id: str, session: TerminalSession) -> bool:
        if session.closed or self._poll(session) is not None:
            self._close_locked(admin_id, session)
            return False
        return True

    def get_or_create(self, admin_id: str) -> TerminalSession:
        self._check_owner()
        key = str(admin_id)
        with self._lock:
            existing = self._sessions.get(key)
            if existing is not None and self._refresh_locked(key, existing):
                return existing
            try:
                session = self._runner(key)
            except TerminalUnavailable:
                raise
            except OSError as exc:
                raise TerminalUnavailable("终端会话创建失败") from exc
            self._sessions[key] = session
            return session

    def output(self, admin_id: str):
        """Return a stream that cleans up/reaps when the child exits."""
        session = self.get_or_create(admin_id)

        def generate():
            while True:
                try:
                    ready, _, _ = self._select([session.fd], [], [], 0.5)
                except (OSError, ValueError):
                    with self._lock:
                        self._close_locked(str(admin_id), session)
                    break
                if ready:
                    try:
                        with self._lock:
                            if self._sessions.get(str(admin_id)) is not session:
                                break
                            data = self._os.read(session.fd, 1024 * 16)
                    except (OSError, ValueError):
                        with self._lock:
                            self._close_locked(str(admin_id), session)
                        break
                    if not data:
                        with self._lock:
                            self._close_locked(str(admin_id), session)
                        break
                    yield data.decode("utf-8", errors="replace")

                with self._lock:
                    if self._sessions.get(str(admin_id)) is not session:
                        break
                    if not self._refresh_locked(str(admin_id), session):
                        yield "\n[Session Terminated]\n"
                        break

        return generate()

    def write(self, admin_id: str, input_text: str) -> None:
        if not isinstance(input_text, str):
            raise ValueError("input must be text")
        with self._lock:
            session = self.get_or_create(admin_id)
            try:
                self._os.write(session.fd, input_text.encode("utf-8"))
            except (OSError, ValueError) as exc:
                self._close_locked(str(admin_id), session)
                raise TerminalUnavailable("终端写入失败") from exc

    def cwd(self, admin_id: str) -> str:
        self._check_owner()
        with self._lock:
            session = self._sessions.get(str(admin_id))
            if session is None or not self._refresh_locked(str(admin_id), session):
                return "~"
            try:
                return self._os.readlink(f"/proc/{session.pid}/cwd")
            except (OSError, ValueError):
                return "~"

    def close_all(self) -> None:
        """Terminate, reap, and close every session owned by this worker."""
        self._check_owner()
        with self._lock:
            for admin_id, session in list(self._sessions.items()):
                terminate = getattr(session.process, "terminate", None)
                if callable(terminate) and self._poll(session) is None:
                    try:
                        terminate()
                    except OSError:
                        pass
                wait = getattr(session.process, "wait", None)
                if callable(wait):
                    try:
                        wait(timeout=1)
                    except (TypeError, TimeoutError, subprocess.TimeoutExpired):
                        kill = getattr(session.process, "kill", None)
                        if callable(kill):
                            try:
                                kill()
                            except OSError:
                                pass
                        try:
                            wait(timeout=1)
                        except (TypeError, TimeoutError, subprocess.TimeoutExpired):
                            pass
                self._close_locked(admin_id, session)


__all__ = [
    "SubprocessPtyRunner",
    "TerminalApplication",
    "TerminalError",
    "TerminalSession",
    "TerminalUnavailable",
]
