import os
import unittest
from unittest.mock import patch

from nonebot_plugin_xiuxian_2.features.terminal.application import (
    TerminalApplication,
    TerminalSession,
    TerminalUnavailable,
)


class FakeProcess:
    def __init__(self):
        self.returncode = None
        self.wait_calls = []
        self.terminate_calls = 0
        self.kill_calls = 0

    def poll(self):
        return self.returncode

    def wait(self, timeout=None):
        self.wait_calls.append(timeout)
        if self.returncode is None:
            raise TimeoutError
        return self.returncode

    def terminate(self):
        self.terminate_calls += 1
        self.returncode = -15

    def kill(self):
        self.kill_calls += 1
        self.returncode = -9


class FakeOs:
    def __init__(self):
        self.closed = []
        self.writes = []
        self.read_values = [b"hello\n"]

    def close(self, fd):
        self.closed.append(fd)

    def read(self, fd, size):
        return self.read_values.pop(0) if self.read_values else b""

    def write(self, fd, value):
        self.writes.append((fd, value))
        return len(value)

    def readlink(self, path):
        return "/srv/xiuxian"


class TerminalApplicationTests(unittest.TestCase):
    def make_app(self, *, secret="correct", select_fn=None):
        self.fake_os = FakeOs()
        self.processes = []

        def runner(_admin_id):
            process = FakeProcess()
            self.processes.append(process)
            return TerminalSession(fd=100 + len(self.processes), pid=500 + len(self.processes), process=process)

        return TerminalApplication(
            secret_provider=lambda: secret,
            runner=runner,
            os_adapter=self.fake_os,
            select_fn=select_fn or (lambda *_args: ([], [], [])),
        )

    def test_authorization_requires_secret_and_does_not_store_password(self):
        app = self.make_app(secret="correct")
        session = {}
        self.assertFalse(app.authorize(session, "wrong"))
        self.assertEqual(session, {})
        self.assertTrue(app.authorize(session, "correct"))
        self.assertIn("terminal_authorized_until", session)
        self.assertNotIn("correct", repr(session))

        unconfigured = self.make_app(secret="")
        session = {}
        self.assertFalse(unconfigured.authorize(session, "correct"))
        self.assertEqual(session, {})

    def test_dead_session_is_reaped_closed_and_replaced(self):
        app = self.make_app()
        first = app.get_or_create("admin")
        self.processes[0].returncode = 9
        second = app.get_or_create("admin")
        self.assertIsNot(first, second)
        self.assertIn(first.fd, self.fake_os.closed)
        self.assertTrue(self.processes[0].wait_calls)

    def test_output_reads_under_owner_and_emits_termination_marker(self):
        calls = []

        def select_fn(*_args):
            calls.append(True)
            if len(calls) == 1:
                return ([101], [], [])
            self.processes[0].returncode = 1
            return ([], [], [])

        app = self.make_app(select_fn=select_fn)
        self.assertEqual(list(app.output("admin")), ["hello\n", "\n[Session Terminated]\n"])
        self.assertIn(101, self.fake_os.closed)
        self.assertTrue(self.processes[0].wait_calls)

    def test_write_and_cwd_use_feature_owned_session(self):
        app = self.make_app()
        app.write("admin", "ls\n")
        self.assertEqual(self.fake_os.writes, [(101, b"ls\n")])
        self.assertEqual(app.cwd("admin"), "/srv/xiuxian")

    def test_close_all_terminates_reaps_and_closes_sessions(self):
        app = self.make_app()
        app.get_or_create("admin")
        app.close_all()
        self.assertEqual(self.processes[0].terminate_calls, 1)
        self.assertIn(101, self.fake_os.closed)
        self.assertEqual(app.cwd("admin"), "~")

    def test_a_forked_or_second_worker_cannot_reuse_application(self):
        app = self.make_app()
        with patch.object(os, "getpid", return_value=app._owner_pid + 1):
            with self.assertRaises(TerminalUnavailable):
                app.get_or_create("admin")


if __name__ == "__main__":
    unittest.main()
