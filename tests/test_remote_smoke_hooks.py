"""P6 remote-deployment smoke hook guards.

These tests protect the operator hooks that ``scripts/remote_smoke.sh`` drives
on the remote host.  The smoke run itself needs a real host, so the hooks must
carry their own regression coverage: a hook that silently loses its isolation or
rollback property would otherwise only fail during a live deployment window.
"""

from __future__ import annotations

import os
import subprocess
import tempfile
import unittest
from pathlib import Path

from scripts.check_architecture import check_remote_smoke_contract

HOOKS = Path(__file__).resolve().parents[1] / "scripts" / "remote_smoke_hooks"


class RemoteSmokeContractTests(unittest.TestCase):
    def test_contract_check_passes(self) -> None:
        self.assertEqual(check_remote_smoke_contract(), [])

    def test_every_hook_is_executable_and_shell_syntax_valid(self) -> None:
        hooks = sorted(HOOKS.glob("*.sh"))
        self.assertTrue(hooks, "no remote smoke hooks found")
        for hook in hooks:
            self.assertTrue(os.access(hook, os.X_OK), f"{hook.name} is not executable")
            result = subprocess.run(["sh", "-n", str(hook)], capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, f"{hook.name} has a syntax error: {result.stderr}")


class StopHookTests(unittest.TestCase):
    def test_stop_is_idempotent_without_a_pid_file(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            env = dict(os.environ, XIUXIAN_DATA_DIR=str(Path(directory) / "data"))
            result = subprocess.run(["sh", str(HOOKS / "stop.sh")], capture_output=True, text=True, env=env)
            self.assertEqual(result.returncode, 0, result.stderr)

    def test_stop_kills_the_recorded_pid_and_removes_the_file(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            rundir = Path(directory)
            process = subprocess.Popen(["sleep", "300"])
            try:
                (rundir / "instance.pid").write_text(str(process.pid), encoding="utf-8")
                env = dict(os.environ, XIUXIAN_DATA_DIR=str(rundir / "data"))
                result = subprocess.run(["sh", str(HOOKS / "stop.sh")], capture_output=True, text=True, env=env)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertIsNotNone(process.poll(), "stop hook did not terminate the recorded process")
                self.assertFalse((rundir / "instance.pid").exists())
            finally:
                process.kill()
                process.wait()


class RollbackHookTests(unittest.TestCase):
    def test_rollback_removes_the_marker_it_created(self) -> None:
        """The hook owns undoing the smoke write; a database restore cannot."""
        with tempfile.TemporaryDirectory() as directory:
            data_dir = Path(directory) / "data"
            data_dir.mkdir()
            marker = data_dir / "remote-smoke-marker.json"
            marker.write_text("{}", encoding="utf-8")
            # A stub stands in for the real restore so the test needs no database.
            stub = Path(directory) / "stub-python"
            stub.write_text('#!/bin/sh\nexit 0\n', encoding="utf-8")
            stub.chmod(0o755)
            env = dict(
                os.environ,
                XIUXIAN_DATA_DIR=str(data_dir),
                BACKUP_PATH=str(Path(directory) / "backup"),
                XIUXIAN_PYTHON=str(stub),
                XIUXIAN_PROJECT_DIR=str(directory),
                XIUXIAN_OLD_DIR=str(Path(directory) / "no-old"),
            )
            result = subprocess.run(["sh", str(HOOKS / "rollback.sh")], capture_output=True, text=True, env=env)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertFalse(marker.exists(), "rollback left the smoke marker behind")

    def test_rollback_fails_when_the_restore_fails(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_dir = Path(directory) / "data"
            data_dir.mkdir()
            stub = Path(directory) / "stub-python"
            stub.write_text('#!/bin/sh\necho "checksum mismatch" >&2\nexit 1\n', encoding="utf-8")
            stub.chmod(0o755)
            env = dict(
                os.environ,
                XIUXIAN_DATA_DIR=str(data_dir),
                BACKUP_PATH=str(Path(directory) / "backup"),
                XIUXIAN_PYTHON=str(stub),
                XIUXIAN_PROJECT_DIR=str(directory),
                XIUXIAN_OLD_DIR=str(Path(directory) / "no-old"),
            )
            result = subprocess.run(["sh", str(HOOKS / "rollback.sh")], capture_output=True, text=True, env=env)
            self.assertNotEqual(result.returncode, 0, "a failed restore must fail the rollback")


if __name__ == "__main__":
    unittest.main()
