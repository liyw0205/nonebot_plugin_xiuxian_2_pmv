from __future__ import annotations

from pathlib import Path

from .. import ManualBackupApplication
from ...plugin_backups.creation_application import (
    PluginBackupCreationResult,
)


class FakePluginBackup:
    def __init__(self, result: PluginBackupCreationResult, calls: list[object]) -> None:
        self.result = result
        self.calls = calls

    def create_backup_with_details(self, *, defer_cloud_cleanup: bool):
        self.calls.append(("plugin", defer_cloud_cleanup))
        return self.result

    def cleanup_cloud_backups(self) -> None:
        self.calls.append(("cloud_cleanup",))


class FakeConfigBackup:
    def __init__(self, result, calls: list[object]) -> None:
        self.result = result
        self.calls = calls

    def backup_all_configs_with_details(self, *, defer_cloud_cleanup: bool = False):
        self.calls.append(("config", defer_cloud_cleanup))
        return self.result


def test_manual_backup_is_serial_and_cleans_shared_cloud_once(tmp_path: Path) -> None:
    calls: list[object] = []
    plugin_path = tmp_path / "plugin.zip"
    config_path = tmp_path / "config.json"
    application = ManualBackupApplication(
        FakePluginBackup(PluginBackupCreationResult(True, plugin_path, True), calls),
        FakeConfigBackup((True, config_path, True), calls),
    )

    result = application.create_backup()

    assert result.success is True
    assert result.plugin_backup == plugin_path
    assert result.config_backup == config_path
    assert calls == [
        ("plugin", True),
        ("config", True),
        ("cloud_cleanup",),
    ]


def test_plugin_failure_still_runs_config_backup_and_reports_partial_result(tmp_path: Path) -> None:
    calls: list[object] = []
    config_path = tmp_path / "config.json"
    application = ManualBackupApplication(
        FakePluginBackup(PluginBackupCreationResult(False, "disk error"), calls),
        FakeConfigBackup((True, config_path, False), calls),
    )

    result = application.create_backup()

    assert result.success is False
    assert result.plugin_backup == "disk error"
    assert result.config_backup == config_path
    assert result.error == "插件备份失败: disk error"
    assert [call[0] for call in calls] == ["plugin", "config"]


def test_config_failure_does_not_discard_plugin_backup(tmp_path: Path) -> None:
    calls: list[object] = []
    plugin_path = tmp_path / "plugin.zip"
    application = ManualBackupApplication(
        FakePluginBackup(PluginBackupCreationResult(True, plugin_path, False), calls),
        FakeConfigBackup((False, "config error", False), calls),
    )

    result = application.create_backup()

    assert result.success is False
    assert result.plugin_backup == plugin_path
    assert result.config_backup == "config error"
    assert result.error == "配置备份失败: config error"
