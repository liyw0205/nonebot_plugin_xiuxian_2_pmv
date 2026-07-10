from __future__ import annotations

import os
import shlex
import subprocess
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"


class InstallerScriptTests(unittest.TestCase):
    def test_shell_scripts_are_valid_bash(self) -> None:
        for name in ("install.sh", "install_termux.sh"):
            subprocess.run(
                ["bash", "-n", str(SCRIPTS / name)],
                check=True,
                cwd=ROOT,
            )

    def test_installers_are_maintained_in_this_repository(self) -> None:
        readme = (ROOT / "README.md").read_text(encoding="utf-8")
        termux = (SCRIPTS / "install_termux.sh").read_text(encoding="utf-8")
        self.assertNotIn("nonebot_plugin_xiuxian_2_pmv_file", readme)
        self.assertNotIn("nonebot_plugin_xiuxian_2_pmv_file", termux)
        self.assertIn("DEFAULT_PROJECT_NAME=\"xiu2\"", (SCRIPTS / "install.sh").read_text(encoding="utf-8"))

    def test_transient_databases_are_excluded_from_script_backups(self) -> None:
        for name in ("install.sh", "install_termux.sh"):
            source = (SCRIPTS / name).read_text(encoding="utf-8")
            self.assertIn("data/xiuxian/message.db*", source)
            self.assertIn("data/xiuxian/activity/activity.db*", source)

    def test_managed_start_commands_disable_development_reloader(self) -> None:
        for name in ("install.sh", "install_termux.sh", "install.bat"):
            source = (SCRIPTS / name).read_text(encoding="utf-8")
            self.assertNotIn("nb run --reload", source)

    def test_git_archive_layout_is_extracted_without_losing_top_directory(self) -> None:
        for name in ("install.sh", "install_termux.sh"):
            with self.subTest(installer=name), tempfile.TemporaryDirectory() as temp:
                temp_path = Path(temp)
                archive = temp_path / "project.tar.gz"
                extracted = temp_path / "extracted"
                plugin_entry = extracted / "nonebot_plugin_xiuxian_2" / "__init__.py"
                subprocess.run(
                    [
                        "git",
                        "archive",
                        "--format=tar.gz",
                        f"--output={archive}",
                        "HEAD",
                    ],
                    check=True,
                    cwd=ROOT,
                )
                command = (
                    "set -e; "
                    "export XIUXIAN_INSTALLER_LIBRARY_ONLY=1; "
                    f"source {self._quote(SCRIPTS / name)}; "
                    f"extract_release_resource {self._quote(archive)} {self._quote(extracted)}; "
                    f"test -f {self._quote(plugin_entry)}"
                )
                subprocess.run(["bash", "-c", command], check=True, cwd=ROOT)

    def test_invalid_archive_fails_before_deployment(self) -> None:
        for name in ("install.sh", "install_termux.sh"):
            with self.subTest(installer=name), tempfile.TemporaryDirectory() as temp:
                temp_path = Path(temp)
                payload = temp_path / "payload"
                payload.mkdir()
                (payload / "README.txt").write_text("invalid", encoding="ascii")
                archive = temp_path / "project.tar.gz"
                extracted = temp_path / "out"
                subprocess.run(
                    ["tar", "-czf", str(archive), "-C", str(payload), "."],
                    check=True,
                )
                env = os.environ | {"XIUXIAN_INSTALLER_LIBRARY_ONLY": "1"}
                command = (
                    f"source {self._quote(SCRIPTS / name)}; "
                    f"extract_release_resource {self._quote(archive)} {self._quote(extracted)}"
                )
                result = subprocess.run(["bash", "-c", command], cwd=ROOT, env=env)
                self.assertNotEqual(result.returncode, 0)

    @staticmethod
    def _quote(path: Path) -> str:
        return shlex.quote(str(path))


if __name__ == "__main__":
    unittest.main()
