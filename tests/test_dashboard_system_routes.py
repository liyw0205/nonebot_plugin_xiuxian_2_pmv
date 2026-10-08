from __future__ import annotations

from contextlib import ExitStack
from datetime import datetime
import unittest
from unittest.mock import patch

import nonebot
nonebot.init()

from nonebot_plugin_xiuxian_2.features.status.system_info import SystemInfoSnapshot
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_web import core, system


class DashboardStatusApplicationFake:
    def __init__(self) -> None:
        self.stats_calls = 0
        self.system_info_calls = 0
        self.process_calls: list[int] = []
        self.process_info_available = True

    def dashboard_stats(self, *, now: datetime | None = None) -> dict[str, int]:
        self.stats_calls += 1
        return {
            "total_users": 11,
            "total_sects": 4,
            "active_users": 3,
            "yesterday_users": 2,
            "seven_days_avg": 8,
            "msg_received": 31,
            "msg_sent": 27,
        }

    def system_info(self) -> SystemInfoSnapshot:
        self.system_info_calls += 1
        return SystemInfoSnapshot(
            (
                ("运行时间", (("系统启动时间", "2026-10-08 10:00:00"), ("系统运行时间", "1天2小时"))),
                ("系统信息", (("平台", "Linux-test"), ("系统", "Linux"))),
                ("CPU信息", (("CPU使用率", "12.5%"),)),
                ("内存信息", (("总内存", "8.00GB"),)),
                ("磁盘信息", (("总磁盘空间", "100.00GB"),)),
            )
        )

    def process_info(self, *, limit: int = 5) -> list[dict[str, object]]:
        self.process_calls.append(limit)
        return [
            {
                "pid": 1234,
                "name": "xiuxian-test",
                "memory": "8.0MB",
                "memory_mb": 8.0,
                "time": "0:01:00",
            }
        ][:limit]


class DashboardSystemRouteHttpTests(unittest.TestCase):
    ROUTES = (
        "/get_stats",
        "/get_system_info_extended",
        "/get_process_info",
        "/api/dashboard/summary",
    )

    def setUp(self) -> None:
        core.app.config.update(TESTING=True, SECRET_KEY="dashboard-route-test")
        self.client = core.app.test_client()
        self.application = DashboardStatusApplicationFake()

    def _login(self) -> None:
        with self.client.session_transaction() as session:
            session["admin_id"] = "admin-1"

    def _patch_dashboard_dependencies(self) -> ExitStack:
        class FakeAdapter:
            @staticmethod
            def get_name() -> str:
                return "test-adapter"

        fake_bots = {"bot-1": type("FakeBot", (), {"adapter": FakeAdapter()})()}
        stack = ExitStack()
        stack.enter_context(patch.object(system, "status_application", self.application, create=True))
        stack.enter_context(patch.object(system, "get_bots", return_value=fake_bots))
        stack.enter_context(patch.object(system, "psutil_available", False))
        return stack

    def test_all_dashboard_routes_require_an_admin_session(self) -> None:
        with patch.object(core, "ADMIN_IDS", {"admin-1"}):
            for route in self.ROUTES:
                with self.subTest(route=route):
                    response = self.client.get(route)
                    self.assertEqual(response.status_code, 401)
                    self.assertEqual(
                        response.get_json(),
                        {"success": False, "error": "未登录"},
                    )

        self.assertEqual(self.application.stats_calls, 0)
        self.assertEqual(self.application.system_info_calls, 0)
        self.assertEqual(self.application.process_calls, [])

    def test_stats_route_keeps_legacy_json_fields(self) -> None:
        self._login()
        with patch.object(core, "ADMIN_IDS", {"admin-1"}), self._patch_dashboard_dependencies():
            response = self.client.get("/get_stats")

        self.assertEqual(response.status_code, 200)
        body = response.get_json()
        self.assertTrue(body["success"])
        self.assertEqual(
            {key: body[key] for key in (
                "total_users", "total_sects", "active_users", "yesterday_users",
                "seven_days_avg", "msg_received", "msg_sent", "bot_count",
                "bots", "bot_uptime", "nb_version",
            )},
            {
                "total_users": 11,
                "total_sects": 4,
                "active_users": 3,
                "yesterday_users": 2,
                "seven_days_avg": 8,
                "msg_received": 31,
                "msg_sent": 27,
                "bot_count": 1,
                "bots": [{"bot_id": "bot-1", "adapter": "test-adapter"}],
                "bot_uptime": "未知",
                "nb_version": core.nb_version,
            },
        )
        self.assertEqual(self.application.stats_calls, 1)

    def test_extended_system_route_keeps_legacy_grouped_payload(self) -> None:
        self._login()
        with patch.object(core, "ADMIN_IDS", {"admin-1"}), self._patch_dashboard_dependencies():
            response = self.client.get("/get_system_info_extended")

        self.assertEqual(response.status_code, 200)
        body = response.get_json()
        self.assertTrue(body["success"])
        self.assertEqual(
            {key for key in body if key != "success"},
            {"system_info", "cpu_info", "mem_info", "disk_info", "system_uptime"},
        )
        self.assertEqual(body["system_info"]["系统"], "Linux")
        self.assertEqual(body["cpu_info"]["CPU使用率"], "12.5%")
        self.assertEqual(body["mem_info"]["总内存"], "8.00GB")
        self.assertEqual(body["disk_info"]["总磁盘空间"], "100.00GB")
        self.assertEqual(body["system_uptime"]["系统启动时间"], "2026-10-08 10:00:00")
        self.assertEqual(self.application.system_info_calls, 1)

    def test_process_route_keeps_legacy_process_rows(self) -> None:
        self._login()
        with patch.object(core, "ADMIN_IDS", {"admin-1"}), self._patch_dashboard_dependencies():
            response = self.client.get("/get_process_info")

        self.assertEqual(response.status_code, 200)
        body = response.get_json()
        self.assertTrue(body["success"])
        self.assertEqual(
            body["processes"],
            [{
                "pid": 1234,
                "name": "xiuxian-test",
                "memory": "8.0MB",
                "memory_mb": 8.0,
                "time": "0:01:00",
            }],
        )
        self.assertEqual(self.application.process_calls, [5])

    def test_process_route_keeps_missing_psutil_error_envelope(self) -> None:
        self._login()
        self.application.process_info_available = False
        with patch.object(core, "ADMIN_IDS", {"admin-1"}), self._patch_dashboard_dependencies():
            response = self.client.get("/get_process_info")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.get_json(),
            {
                "success": False,
                "error": "psutil未安装，无法获取进程信息",
                "processes": [],
            },
        )
        self.assertEqual(self.application.process_calls, [])

    def test_dashboard_summary_preserves_nested_legacy_contract(self) -> None:
        self._login()
        with patch.object(core, "ADMIN_IDS", {"admin-1"}), self._patch_dashboard_dependencies():
            response = self.client.get("/api/dashboard/summary")

        self.assertEqual(response.status_code, 200)
        body = response.get_json()
        self.assertTrue(body["success"])
        self.assertRegex(body["generated_at"], r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$")
        self.assertEqual(body["stats"]["total_users"], 11)
        self.assertEqual(body["stats"]["seven_days_avg"], 8)
        self.assertEqual(body["stats"]["msg_received"], 31)
        self.assertEqual(body["system"]["system_info"]["系统"], "Linux")
        self.assertEqual(body["system"]["cpu_info"]["CPU使用率"], "12.5%")
        self.assertEqual(body["processes"][0]["pid"], 1234)
        self.assertEqual(self.application.stats_calls, 1)
        self.assertEqual(self.application.system_info_calls, 1)
        self.assertEqual(self.application.process_calls, [5])


if __name__ == "__main__":
    unittest.main()
