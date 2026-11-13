from __future__ import annotations

import os
import shlex
import subprocess
import sys
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

    def test_release_contract_and_proxy_fallback_are_explicit(self) -> None:
        install = (SCRIPTS / "install.sh").read_text(encoding="utf-8")
        termux = (SCRIPTS / "install_termux.sh").read_text(encoding="utf-8")
        workflow = (ROOT / ".github" / "workflows" / "main.yml").read_text(encoding="utf-8")
        for source in (install, termux):
            self.assertIn('RELEASE_TAG="${XIUXIAN_RELEASE_TAG:-latest}"', source)
            self.assertIn('RELEASE_ASSET="project.tar.gz"', source)
            self.assertIn(r"^v[0-9]+\.[0-9]+\.[0-9]+$", source)
            self.assertIn("validate_release_reference", source)
            self.assertIn("https://gh-proxy.com/", source)
            self.assertIn("https://ghfast.top/", source)
            self.assertIn("https://ghproxy.vip/", source)
            self.assertIn("https://gh-proxy.org/", source)
            self.assertNotIn("gh.jasonzeng.dev", source)
            self.assertNotIn("ghproxy.imciel.com", source)
            self.assertIn("tar -tzf", source)
        bat = (SCRIPTS / "install.bat").read_text(encoding="utf-8")
        self.assertIn("https://ghfast.top/", bat)
        self.assertIn("https://ghproxy.vip/", bat)
        self.assertIn("https://gh-proxy.org/", bat)
        self.assertNotIn("ghproxy.imciel.com", bat)
        docker = (SCRIPTS / "install_docker.sh").read_text(encoding="utf-8")
        self.assertIn("https://gh-proxy.com/", docker)
        self.assertNotIn("https://ghproxy.net/", docker)
        self.assertIn("^v[0-9]+\\.[0-9]+\\.[0-9]+$", workflow)
        self.assertIn("workflow_dispatch 必须提供 tag_name", workflow)
        self.assertIn("project.tar.gz", workflow)
        self.assertIn('git archive --format=tar.gz --output=project.tar.gz "$GITHUB_SHA"', workflow)
        # The quality job audits real tags via released_tags(); a shallow
        # checkout never receives them, so it must fetch full history + tags.
        self.assertIn("fetch-depth: 0", workflow)
        self.assertIn("fetch-tags: true", workflow)

    def test_installers_are_maintained_in_this_repository(self) -> None:
        readme = (ROOT / "README.md").read_text(encoding="utf-8")
        termux = (SCRIPTS / "install_termux.sh").read_text(encoding="utf-8")
        self.assertNotIn("nonebot_plugin_xiuxian_2_pmv_file", readme)
        self.assertNotIn("nonebot_plugin_xiuxian_2_pmv_file", termux)
        self.assertIn("DEFAULT_PROJECT_NAME=\"xiu2\"", (SCRIPTS / "install.sh").read_text(encoding="utf-8"))

    def test_managed_update_entrypoints_fetch_current_installers(self) -> None:
        linux = (SCRIPTS / "install.sh").read_text(encoding="utf-8")
        termux = (SCRIPTS / "install_termux.sh").read_text(encoding="utf-8")
        readme = (ROOT / "README.md").read_text(encoding="utf-8")
        docs = (ROOT / "docs" / "installation.md").read_text(encoding="utf-8")

        self.assertIn(
            'curl -fsSL "https://raw.githubusercontent.com/$REPO_OWNER/$REPO_NAME/main/scripts/install.sh"',
            linux,
        )
        self.assertIn(
            'curl -fsSL "https://raw.githubusercontent.com/$REPO_OWNER/$REPO_NAME/main/scripts/install_termux.sh"',
            termux,
        )
        self.assertIn("main/scripts/install.bat", readme)
        self.assertIn("bash /root/install.sh update", docs)
        self.assertIn("已保存的批处理文件不会由 `project.tar.gz` 自动更新", docs)

    def test_linux_and_termux_installers_share_lifecycle_and_option_contract(self) -> None:
        actions = ("install", "update", "uninstall", "start", "stop", "restart", "status", "logs", "login")
        options = ("--source-mode", "--source", "--mirror", "--target", "--venv")
        for name in ("install.sh", "install_termux.sh"):
            source = (SCRIPTS / name).read_text(encoding="utf-8")
            with self.subTest(installer=name):
                for token in (*actions, *options):
                    self.assertIn(token, source)

    def test_login_routes_to_qq_bind_panel_and_onebot_auth_is_optional(self) -> None:
        for name in ("install.sh", "install_termux.sh"):
            source = (SCRIPTS / name).read_text(encoding="utf-8")
            with self.subTest(installer=name):
                self.assertIn('"QQ 官方机器人配置入口: http://127.0.0.1:5888/config"', source)
                self.assertIn("OneBot 登录仍由 NapCat 客户端处理", source)

        readme = (ROOT / "README.md").read_text(encoding="utf-8")
        configuration = (ROOT / "docs" / "configuration.md").read_text(encoding="utf-8")
        installation = (ROOT / "docs" / "installation.md").read_text(encoding="utf-8")
        for document in (readme, configuration, installation):
            self.assertIn("可选鉴权", document)
            self.assertIn("ONEBOT_V11_ACCESS_TOKEN", document)
        self.assertIn("默认无需生成或填写", readme)
        self.assertIn("默认不启用，无需预先生成随机 token", configuration)

    def test_linux_installer_help_and_invalid_source_mode_exit_before_install(self) -> None:
        help_result = subprocess.run(
            ["bash", str(SCRIPTS / "install.sh"), "--help"],
            check=True,
            capture_output=True,
            text=True,
            cwd=ROOT,
        )
        self.assertIn("--source-mode release|source", help_result.stdout)
        self.assertIn("--target PATH", help_result.stdout)

        invalid = subprocess.run(
            ["bash", str(SCRIPTS / "install.sh"), "install", "--source-mode", "source"],
            capture_output=True,
            text=True,
            cwd=ROOT,
        )
        self.assertEqual(invalid.returncode, 2)
        self.assertIn("必须同时指定 --source PATH", invalid.stdout + invalid.stderr)

    def test_termux_installer_help_is_safe_outside_termux(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            prefix = Path(temp) / "prefix"
            home = Path(temp) / "home"
            (prefix / "bin").mkdir(parents=True)
            (prefix / "bin" / "pkg").write_text("#!/bin/sh\nexit 0\n", encoding="ascii")
            (prefix / "bin" / "pkg").chmod(0o755)
            result = subprocess.run(
                ["bash", str(SCRIPTS / "install_termux.sh"), "--help"],
                check=True,
                capture_output=True,
                text=True,
                cwd=ROOT,
                env=os.environ | {"PREFIX": str(prefix), "HOME": str(home)},
            )
        self.assertIn("--source-mode release|source", result.stdout)
        self.assertIn("--venv PATH", result.stdout)

    def test_transient_databases_are_excluded_from_script_backups(self) -> None:
        for name in ("install.sh", "install_termux.sh"):
            source = (SCRIPTS / name).read_text(encoding="utf-8")
            self.assertIn("data/xiuxian/message.db*", source)
            self.assertIn("data/xiuxian/.message.db.*.migrating", source)
            self.assertIn("data/xiuxian/activity/activity.db*", source)

    def test_managed_start_commands_disable_development_reloader(self) -> None:
        for name in ("install.sh", "install_termux.sh", "install.bat"):
            source = (SCRIPTS / name).read_text(encoding="utf-8")
            self.assertNotIn("nb run --reload", source)

    def test_managed_start_commands_load_plugin_runtime_env(self) -> None:
        for name in ("install.sh", "install_termux.sh"):
            source = (SCRIPTS / name).read_text(encoding="utf-8")
            self.assertIn('runtime.env', source)
            self.assertIn('XIUXIAN_WEB_STATUS=true', source)
            self.assertIn('source "$DIR/runtime.env"', source)
            self.assertIn('set -a', source)

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

    def test_release_update_layout_excludes_test_fixture_but_rejects_runtime_import(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            temp_path = Path(temp)
            archive = temp_path / "project.tar.gz"
            subprocess.run(
                ["git", "archive", "--format=tar.gz", f"--output={archive}", "HEAD"],
                check=True,
                cwd=ROOT,
            )
            release_source = temp_path / "release-source"
            release_source.mkdir()
            subprocess.run(["tar", "-xzf", str(archive), "-C", str(release_source)], check=True)

            for installer in ("install.sh", "install_termux.sh"):
                with self.subTest(installer=installer):
                    deployment = temp_path / installer / "deployment"
                    extracted = temp_path / installer / "extracted"
                    fixture = extracted / "nonebot_plugin_xiuxian_2/features/buff/tests/test_closing_repository.py"
                    runtime_module = extracted / "nonebot_plugin_xiuxian_2/features/buff/repository.py"
                    command = (
                        "set -e; export XIUXIAN_INSTALLER_LIBRARY_ONLY=1; "
                        f"source {self._quote(release_source / 'scripts' / installer)}; "
                        f"DIR={self._quote(deployment)}; "
                        f"mkdir -p {self._quote(deployment / 'src/plugins')}; "
                        f"extract_release_resource {self._quote(archive)} {self._quote(extracted)} >/dev/null; "
                        f"test -f {self._quote(fixture)}; "
                        f"replace_plugin_source {self._quote(extracted / 'nonebot_plugin_xiuxian_2')}; "
                        "validate_deployment; "
                        f"printf '%s\\n' {self._quote('from nonebot_plugin_xiuxian_2.plugin import x')} "
                        f"> {self._quote(runtime_module)}; "
                        f"replace_plugin_source {self._quote(extracted / 'nonebot_plugin_xiuxian_2')}; "
                        "if validate_deployment; then exit 81; fi"
                    )
                    subprocess.run(["bash", "-c", command], check=True, cwd=ROOT, capture_output=True, text=True)

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

    def _run_validate_deployment(self, installer: str, extra_files: dict[str, str]) -> int:
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)
            staged = directory / "src" / "plugins" / "nonebot_plugin_xiuxian_2"
            for name, content in {"__init__.py": "", **extra_files}.items():
                target = staged / name
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_text(content, encoding="utf-8")
            command = (
                "set -u; DIR=" + self._quote(directory) + "; "
                "export XIUXIAN_INSTALLER_LIBRARY_ONLY=1; "
                f"source {self._quote(SCRIPTS / installer)}; "
                "if validate_deployment; then exit 0; else exit 1; fi"
            )
            result = subprocess.run(["bash", "-c", command], cwd=ROOT, capture_output=True, text=True)
            return result.returncode

    def test_validate_deployment_allows_tests_fixtures_and_flags_runtime_imports(self) -> None:
        fixture = (ROOT / "nonebot_plugin_xiuxian_2/features/buff/tests/test_closing_repository.py").read_text(
            encoding="utf-8"
        )
        runtime_module = "from nonebot_plugin_xiuxian_2.plugin import x\n"
        for installer in ("install.sh", "install_termux.sh"):
            with self.subTest(installer=installer):
                self.assertEqual(
                    0,
                    self._run_validate_deployment(
                        installer,
                        {"features/buff/tests/test_closing_repository.py": fixture},
                    ),
                    "the shipped recursive tests fixture must not block deploy",
                )
                self.assertEqual(
                    1,
                    self._run_validate_deployment(
                        installer,
                        {"features/buff/repository.py": runtime_module},
                    ),
                    "runtime absolute imports must still fail validate_deployment",
                )

    def test_windows_validate_deployment_excludes_nested_tests_only(self) -> None:
        source = (SCRIPTS / "install.bat").read_text(encoding="utf-8")
        scanner_line = next(
            line.strip()
            for line in source.splitlines()
            if line.strip().startswith('python -c "') and "XIUXIAN_PLUGIN_DIR" in line
        )
        self.assertTrue(scanner_line.endswith('"'))
        validator = scanner_line[len('python -c "') : -1]
        self.assertIn("if errorlevel 1 (", source[source.index(scanner_line) :])
        self.assertNotIn("if not errorlevel 1 (", source[source.index(scanner_line) :])
        self.assertIn("部署代码仍包含不兼容 src.plugins 命名空间的绝对导入", source)

        with tempfile.TemporaryDirectory() as temp:
            plugin_dir = Path(temp) / "src/plugins/nonebot_plugin_xiuxian_2"
            fixture = plugin_dir / "features/buff/tests/test_closing_repository.py"
            fixture.parent.mkdir(parents=True)
            fixture.write_text("from nonebot_plugin_xiuxian_2.plugin import x\n", encoding="utf-8")
            env = os.environ | {"XIUXIAN_PLUGIN_DIR": str(plugin_dir)}
            accepted = subprocess.run(
                [sys.executable, "-c", validator],
                cwd=ROOT,
                env=env,
                capture_output=True,
                text=True,
            )
            self.assertEqual(0, accepted.returncode, accepted.stdout + accepted.stderr)

            runtime_module = plugin_dir / "features/buff/repository.py"
            runtime_module.parent.mkdir(parents=True, exist_ok=True)
            runtime_module.write_text("from nonebot_plugin_xiuxian_2.plugin import x\n", encoding="utf-8")
            rejected = subprocess.run(
                [sys.executable, "-c", validator],
                cwd=ROOT,
                env=env,
                capture_output=True,
                text=True,
            )
            self.assertEqual(1, rejected.returncode)
            self.assertIn("repository.py", rejected.stdout)

    @staticmethod
    def _quote(path: Path) -> str:
        return shlex.quote(str(path))


if __name__ == "__main__":
    unittest.main()
