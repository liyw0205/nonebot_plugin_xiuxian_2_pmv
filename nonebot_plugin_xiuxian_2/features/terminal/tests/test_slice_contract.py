from __future__ import annotations

import unittest

from .. import (
    application,
    commands,
    jobs,
    migrations,
    repository,
    schemas,
    web,
)
from ..manifest import FEATURE
from tests.slice_contract import assert_contract_is_single_source, assert_no_autonomous_surface

SCHEMA_NAMES = (
    "AUTHORIZATION_SESSION_KEY",
    "AUTHORIZATION_TTL_SECONDS",
    "PROCESS_POLL_WAIT_SECONDS",
    "PROCESS_REAP_WAIT_SECONDS",
    "PTY_LANGUAGE",
    "PTY_PROMPT",
    "PTY_READ_BYTES",
    "PTY_SELECT_TIMEOUT_SECONDS",
    "PTY_SESSION_TERMINATED_NOTICE",
    "PTY_SHELL_ARGUMENTS",
    "PTY_TERMINAL_TYPE",
    "TERMINAL_PASSWORD_ENVIRONMENT_VARIABLE",
    "UNKNOWN_WORKING_DIRECTORY",
    "WORKING_DIRECTORY_LINK_TEMPLATE",
)


class _Process:
    def __init__(self, returncode=None) -> None:
        self.returncode = returncode
        self.wait_calls: list[int] = []
        self.terminated = 0

    def poll(self):
        return self.returncode

    def wait(self, timeout=None):
        self.wait_calls.append(timeout)
        if self.returncode is None:
            raise TimeoutError
        return self.returncode

    def terminate(self):
        self.terminated += 1
        self.returncode = -15


class _Os:
    def __init__(self, reads=None) -> None:
        self.reads = list(reads or [])
        self.sizes: list[int] = []
        self.closed: list[int] = []
        self.writes: list[tuple[int, bytes]] = []
        self.links: list[str] = []

    def close(self, fd):
        self.closed.append(fd)

    def read(self, fd, size):
        self.sizes.append(size)
        return self.reads.pop(0) if self.reads else b""

    def write(self, fd, value):
        self.writes.append((fd, value))
        return len(value)

    def readlink(self, path):
        self.links.append(path)
        return "/srv/xiuxian"


class TerminalSliceContractTests(unittest.TestCase):
    def make_app(self, *, secret="let-me-in", reads=None, select_fn=None, returncode=None):
        self.os = _Os(reads=reads)
        self.process = _Process(returncode=returncode)
        self.app = application.TerminalApplication(
            secret_provider=lambda: secret,
            session_factory=lambda _admin_id: application.TerminalSession(
                fd=7, pid=4242, process=self.process
            ),
            os_adapter=self.os,
            select_fn=select_fn or (lambda *_args: ([], [], [])),
            clock=lambda: 1000.0,
        )
        return self.app

    def test_lifecycle_bounds_have_one_home(self) -> None:
        assert_contract_is_single_source(self, schemas, ((application, SCHEMA_NAMES),))

    def test_slice_declares_no_autonomous_surface(self) -> None:
        assert_no_autonomous_surface(
            self,
            commands=commands.COMMANDS,
            routes=web.ROUTES,
            jobs=jobs.JOBS,
            migrations=migrations.MIGRATIONS,
            legacy_routes=web.LEGACY_ROUTES,
        )
        self.assertEqual(
            list(web.LEGACY_ROUTES),
            [
                ("POST", "/terminal/confirm", "TerminalApplication.authorize"),
                ("GET", "/terminal/output", "TerminalApplication.output"),
                ("POST", "/terminal/write", "TerminalApplication.write"),
                ("GET", "/terminal/pwd", "TerminalApplication.cwd"),
            ],
        )

    def test_manifest_matches_the_slice(self) -> None:
        self.assertEqual(FEATURE.key, "terminal")
        self.assertEqual(FEATURE.test_tag, "terminal")
        self.assertEqual(FEATURE.owner, "operations")
        self.assertEqual(FEATURE.commands, ())
        self.assertEqual(FEATURE.routes, ())
        self.assertEqual(FEATURE.jobs, ())
        self.assertIsNone(FEATURE.migration_version)

    def test_declared_delegation_is_a_real_application_method(self) -> None:
        for _method, _path, target in web.LEGACY_ROUTES:
            class_name, _, attribute = target.partition(".")
            self.assertIs(getattr(application, class_name), application.TerminalApplication)
            self.assertTrue(callable(getattr(application.TerminalApplication, attribute)))

    def test_ports_are_declared_once(self) -> None:
        for name in ("PasswordProvider", "PtySessionFactory", "OsAdapterPort", "SelectPort"):
            self.assertTrue(hasattr(repository, name), name)

    def test_authorization_stores_only_a_deadline_under_the_declared_key(self) -> None:
        app = self.make_app()
        session: dict[str, object] = {}
        self.assertTrue(app.authorize(session, "let-me-in"))
        self.assertEqual(
            session,
            {
                schemas.AUTHORIZATION_SESSION_KEY: 1000.0
                + schemas.AUTHORIZATION_TTL_SECONDS
            },
        )
        self.assertEqual(app.AUTHORIZATION_TTL, schemas.AUTHORIZATION_TTL_SECONDS)

    def test_missing_password_fails_closed_without_touching_the_session(self) -> None:
        app = self.make_app(secret=None)
        session: dict[str, object] = {}
        self.assertFalse(app.password_configured())
        self.assertFalse(app.authorize(session, "anything"))
        self.assertEqual(session, {})

    def test_output_reads_in_declared_blocks_and_polls_on_declared_timeout(self) -> None:
        seen: list[float] = []

        def select_fn(read_list, write_list, error_list, timeout):
            seen.append(timeout)
            return (read_list if len(seen) == 1 else [], write_list, error_list)

        app = self.make_app(reads=[b"hello\n"], select_fn=select_fn, returncode=0)
        chunks = list(app.output("admin-1"))
        self.assertEqual(chunks[0], "hello\n")
        self.assertEqual(self.os.sizes[0], schemas.PTY_READ_BYTES)
        self.assertEqual(set(seen), {schemas.PTY_SELECT_TIMEOUT_SECONDS})

    def test_dead_child_ends_the_stream_with_the_declared_notice(self) -> None:
        app = self.make_app(returncode=0)
        self.assertEqual(list(app.output("admin-1")), [schemas.PTY_SESSION_TERMINATED_NOTICE])
        self.assertEqual(self.os.closed, [7])
        self.assertEqual(self.process.wait_calls[-1], schemas.PROCESS_POLL_WAIT_SECONDS)

    def test_working_directory_uses_the_declared_link_template_and_fallback(self) -> None:
        app = self.make_app()
        self.assertEqual(app.cwd("admin-1"), schemas.UNKNOWN_WORKING_DIRECTORY)
        app.get_or_create("admin-1")
        self.assertEqual(app.cwd("admin-1"), "/srv/xiuxian")
        self.assertEqual(
            self.os.links[-1], schemas.WORKING_DIRECTORY_LINK_TEMPLATE.format(pid=4242)
        )


if __name__ == "__main__":
    unittest.main()
