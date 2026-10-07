from __future__ import annotations

from datetime import datetime
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest

from nonebot_plugin_xiuxian_2.features.status.application import StatusApplication
from nonebot_plugin_xiuxian_2.features.status.system_info import (
    SystemInfoProvider,
    SystemInfoSnapshot,
)


GIB = 1024**3
BOOT_TIME = 1_699_999_900
NOW = 1_700_000_000


class FakePlatform:
    @staticmethod
    def platform() -> str:
        return "Linux-6.1-test-x86_64"

    @staticmethod
    def system() -> str:
        return "Linux"

    @staticmethod
    def version() -> str:
        return "#1 SMP test"

    @staticmethod
    def machine() -> str:
        return "x86_64"

    @staticmethod
    def processor() -> str:
        return "Test CPU"

    @staticmethod
    def python_version() -> str:
        return "3.12.1"


class NormalPsutil:
    @staticmethod
    def cpu_count(*, logical: bool) -> int:
        return 8 if logical else 4

    @staticmethod
    def cpu_percent() -> float:
        return 12.5

    @staticmethod
    def cpu_freq() -> SimpleNamespace:
        return SimpleNamespace(current=3200.126)

    @staticmethod
    def virtual_memory() -> SimpleNamespace:
        return SimpleNamespace(total=8 * GIB, used=2 * GIB, percent=25.0)

    @staticmethod
    def disk_usage(path: str) -> SimpleNamespace:
        if path != "/":
            raise AssertionError(f"unexpected disk path: {path}")
        return SimpleNamespace(total=100 * GIB, used=40 * GIB, percent=40.0)

    @staticmethod
    def boot_time() -> int:
        return BOOT_TIME


class StatusSystemInfoContractTests(unittest.TestCase):
    def make_provider(self, psutil_api=NormalPsutil):
        return SystemInfoProvider(
            platform_module=FakePlatform,
            psutil_module=psutil_api,
            clock=lambda: NOW,
            local_datetime=lambda _: datetime(2023, 11, 14, 22, 11, 40),
        )

    def test_application_returns_provider_report(self) -> None:
        class FixedProvider:
            def snapshot(self) -> SystemInfoSnapshot:
                return SystemInfoSnapshot((("系统信息", (("状态", "固定报告"),)),))

        app = StatusApplication(":memory:", system_info_provider=FixedProvider())
        snapshot = app.system_info()

        self.assertIsInstance(snapshot, SystemInfoSnapshot)
        self.assertEqual(snapshot.render(), "系统信息\n\n【系统信息】\n状态: 固定报告")

    def test_application_read_does_not_create_feature_database(self) -> None:
        class FixedProvider:
            def snapshot(self) -> SystemInfoSnapshot:
                return SystemInfoSnapshot(())

        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "status.db"
            app = StatusApplication(database, system_info_provider=FixedProvider())

            app.system_info()

            self.assertFalse(database.exists())

    def test_provider_returns_snapshot_with_rendered_legacy_report(self) -> None:
        snapshot = self.make_provider().snapshot()

        self.assertIsInstance(snapshot, SystemInfoSnapshot)
        self.assertIn("【CPU信息】", snapshot.render())

    def test_missing_psutil_keeps_platform_data_and_marks_metrics_unavailable(self) -> None:
        report = self.make_provider(psutil_api=None).snapshot().render()

        self.assertIn("平台: Linux-6.1-test-x86_64", report)
        self.assertIn("系统: Linux", report)
        self.assertIn("物理核心数: psutil未安装", report)
        self.assertIn("系统启动时间: psutil未安装", report)
        self.assertIn("总内存: psutil未安装", report)
        self.assertIn("总磁盘空间: psutil未安装", report)

    def test_normal_values_preserve_legacy_report_order_and_format(self) -> None:
        report = self.make_provider().snapshot().render()
        boot_at = "2023-11-14 22:11:40"

        self.assertEqual(
            report,
            "系统信息\n"
            "\n【运行时间】\n"
            f"系统启动时间: {boot_at}\n"
            "系统运行时间: 0天0小时1分40秒\n"
            "\n【系统信息】\n"
            "平台: Linux-6.1-test-x86_64\n"
            "系统: Linux\n"
            "版本: #1 SMP test\n"
            "机器: x86_64\n"
            "处理器: Test CPU\n"
            "Python版本: 3.12.1\n"
            "\n【CPU信息】\n"
            "物理核心数: 4\n"
            "逻辑核心数: 8\n"
            "CPU使用率: 12.5%\n"
            "CPU频率: 3200.13MHz\n"
            "\n【内存信息】\n"
            "总内存: 8.00GB\n"
            "已用内存: 2.00GB\n"
            "内存使用率: 25.0%\n"
            "\n【磁盘信息】\n"
            "总磁盘空间: 100.00GB\n"
            "已用空间: 40.00GB\n"
            "磁盘使用率: 40.0%"
        )

    def test_each_psutil_collection_failure_is_local_to_its_section(self) -> None:
        cases = (
            ("cpu_count", "物理核心数: 获取失败", "总内存: 8.00GB"),
            ("virtual_memory", "总内存: 获取失败", "总磁盘空间: 100.00GB"),
            ("disk_usage", "总磁盘空间: 获取失败", "系统启动时间: "),
            ("boot_time", "系统启动时间: 获取失败", "平台: Linux-6.1-test-x86_64"),
        )

        for failing_method, expected_failure, unaffected_value in cases:
            with self.subTest(failing_method=failing_method):
                class FailingPsutil(NormalPsutil):
                    pass

                def fail(*args, **kwargs):
                    raise OSError(f"{failing_method} unavailable")

                setattr(FailingPsutil, failing_method, staticmethod(fail))
                report = self.make_provider(FailingPsutil).snapshot().render()

                self.assertIn(expected_failure, report)
                self.assertIn(unaffected_value, report)


if __name__ == "__main__":
    unittest.main()
