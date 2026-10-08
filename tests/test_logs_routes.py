from __future__ import annotations

import unittest
from unittest.mock import patch

import nonebot
nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_web import core
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_web import logs as logs_routes


class LogsApplicationFake:
    def __init__(self) -> None:
        self.calls = []

    def users(self, **kwargs):
        self.calls.append(("users", kwargs))
        return {"success": True, "rows": [{"user_id": "u1"}]}

    def user_messages(self, **kwargs):
        self.calls.append(("user_messages", kwargs))
        return {"success": False, "error": "缺少 user_id"}

    def files(self):
        self.calls.append(("files", {}))
        return {"success": True, "files": []}

    def read(self, **kwargs):
        self.calls.append(("read", kwargs))
        return {"success": True, "total": 0, "rows": []}

    def tail(self, **kwargs):
        self.calls.append(("tail", kwargs))
        return {"success": True, "offset": 0, "next_offset": 0, "lines": []}


class LegacyLogsRouteTests(unittest.TestCase):
    def setUp(self) -> None:
        core.app.config.update(TESTING=True, SECRET_KEY="legacy-logs-routes")
        self.client = core.app.test_client()
        self.application = LogsApplicationFake()
        self.patch = patch.object(logs_routes, "logs_application", self.application)
        self.patch.start()
        self.addCleanup(self.patch.stop)

    def _login(self) -> None:
        with self.client.session_transaction() as session:
            session["admin_id"] = "admin-1"

    def test_legacy_page_redirect_and_api_auth_envelopes_remain(self) -> None:
        views = (
            ("/logs", logs_routes.logs),
            ("/api/logs/users", logs_routes.api_logs_users),
            ("/api/logs/user_messages", logs_routes.api_logs_user_messages),
            ("/api/logs/files", logs_routes.api_logs_files),
            ("/api/logs/read", logs_routes.api_logs_read),
            ("/api/logs/tail", logs_routes.api_logs_tail),
        )

        for path, view in views:
            with self.subTest(path=path), core.app.test_request_context(path):
                response = view()
                if path == "/logs":
                    self.assertEqual(response.status_code, 302)
                else:
                    self.assertEqual(response.status_code, 200)
                    self.assertEqual(response.get_json(), {"success": False, "error": "未登录"})
        self.assertEqual(self.application.calls, [])

        self._login()
        page = self.client.get("/logs")
        self.assertEqual(page.status_code, 200)
        self.assertIn("日志查看", page.get_data(as_text=True))

    def test_api_arguments_and_response_envelopes_remain(self) -> None:
        self._login()
        users = self.client.get("/api/logs/users?query=道友&limit=9").get_json()
        files = self.client.get("/api/logs/files").get_json()
        read = self.client.get("/api/logs/read?file=service.log&page=2&page_size=300").get_json()
        tail = self.client.get("/api/logs/tail?file=service.log&offset=12&ignore_unknown=1&ignore_keywords=Bot%7CGET").get_json()
        messages = self.client.get("/api/logs/user_messages").get_json()

        self.assertEqual(users, {"success": True, "rows": [{"user_id": "u1"}]})
        self.assertEqual(files, {"success": True, "files": []})
        self.assertEqual(read, {"success": True, "total": 0, "rows": []})
        self.assertEqual(tail, {"success": True, "offset": 0, "next_offset": 0, "lines": []})
        self.assertEqual(messages, {"success": False, "error": "缺少 user_id"})
        self.assertEqual(self.application.calls[0], ("users", {"query": "道友", "limit": "9"}))
        self.assertEqual(self.application.calls[2][0], "read")
        self.assertEqual(self.application.calls[3][1]["ignore_keywords"], "Bot|GET")


if __name__ == "__main__":
    unittest.main()
