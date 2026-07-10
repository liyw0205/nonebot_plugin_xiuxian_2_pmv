from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import patch

import nonebot
nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_web import app
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_web import core, pages, scheduler, system
class WebLoginRouteTests(unittest.TestCase):
    def setUp(self) -> None:
        app.config.update(TESTING=True, SECRET_KEY="test-secret")
        self.client = app.test_client()

    def _post_login(self, admin_id: str):
        with self.client.session_transaction() as session:
            session["_csrf_token"] = "csrf-token"
        return self.client.post(
            "/login",
            data={
                "admin_id": admin_id,
                "_csrf_token": "csrf-token",
            },
        )

    def test_login_accepts_configured_superuser(self) -> None:
        with (
            patch.object(core, "ADMIN_IDS", {"admin-1"}),
            patch.object(pages, "ADMIN_IDS", {"admin-1"}),
        ):
            response = self._post_login("other-admin")
            self.assertEqual(response.status_code, 401)
            with self.client.session_transaction() as session:
                self.assertNotIn("admin_id", session)

            response = self._post_login("admin-1")
            self.assertEqual(response.status_code, 302)
            with self.client.session_transaction() as session:
                self.assertEqual(session["admin_id"], "admin-1")

    def test_empty_superusers_bypasses_login(self) -> None:
        with (
            patch.object(core, "ADMIN_IDS", set()),
            patch.object(pages, "ADMIN_IDS", set()),
        ):
            response = self.client.get("/api/messages/bots")
            self.assertEqual(response.status_code, 200)
            self.assertTrue(response.get_json()["success"])
            with self.client.session_transaction() as session:
                self.assertEqual(session["admin_id"], "local")

            response = self.client.get("/login")
            self.assertEqual(response.status_code, 302)

    def test_login_requires_csrf_token(self) -> None:
        with (
            patch.object(core, "ADMIN_IDS", {"admin-1"}),
            patch.object(pages, "ADMIN_IDS", {"admin-1"}),
        ):
            response = self.client.post(
                "/login",
                data={"admin_id": "admin-1"},
            )
        self.assertEqual(response.status_code, 403)


class WebAuthorizationTests(unittest.TestCase):
    def setUp(self) -> None:
        app.config.update(TESTING=True, SECRET_KEY="test-secret")
        self.client = app.test_client()

    def _login_session(self) -> None:
        with self.client.session_transaction() as session:
            session["admin_id"] = "admin-1"
            session["_csrf_token"] = "csrf-token"

    def test_every_web_endpoint_declares_permission(self) -> None:
        self.assertEqual(core.undeclared_web_endpoints(), set())

    def test_api_read_requires_login(self) -> None:
        with patch.object(core, "ADMIN_IDS", {"admin-1"}):
            response = self.client.get("/api/dashboard/summary")
        self.assertEqual(response.status_code, 401)
        self.assertFalse(response.get_json()["success"])

    def test_database_write_requires_feature_flag(self) -> None:
        self._login_session()
        with (
            patch.object(core, "ADMIN_IDS", {"admin-1"}),
            patch.object(core, "web_feature_enabled", return_value=False),
        ):
            response = self.client.post(
                "/execute_command",
                json={"command": "status"},
                headers={"X-CSRF-Token": "csrf-token"},
            )
        self.assertEqual(response.status_code, 403)

    def test_terminal_confirmation_uses_superuser_session(self) -> None:
        self._login_session()
        with (
            patch.object(core, "ADMIN_IDS", {"admin-1"}),
            patch.object(core, "web_feature_enabled", return_value=True),
        ):
            response = self.client.get("/terminal")
            self.assertEqual(response.status_code, 302)
            self.assertTrue(response.headers["Location"].endswith("/terminal/confirm"))

            response = self.client.get("/terminal/confirm")
            self.assertEqual(response.status_code, 302)

            with self.client.session_transaction() as session:
                self.assertGreater(session["terminal_authorized_until"], core.time.time())
                session["terminal_authorized_until"] = core.time.time() - 1

            response = self.client.get("/terminal/pwd")
            self.assertEqual(response.status_code, 403)

    def test_dashboard_process_snapshot_uses_imported_psutil(self) -> None:
        class FakeProcess:
            pid = 1001

            def memory_info(self):
                return SimpleNamespace(rss=8 * 1024 * 1024)

            def create_time(self):
                return core.time.time() - 60

            def name(self):
                return "fake-process"

        fake_psutil = SimpleNamespace(
            process_iter=lambda attrs: [FakeProcess()],
            NoSuchProcess=RuntimeError,
            AccessDenied=PermissionError,
        )
        with (
            patch.object(system, "psutil_available", True),
            patch.object(system, "psutil", fake_psutil),
        ):
            processes = system._collect_process_snapshot(5)

        self.assertEqual(processes[0]["name"], "fake-process")
        self.assertEqual(processes[0]["memory_mb"], 8.0)

    def test_local_upload_uses_direct_peer_address(self) -> None:
        with (
            patch.object(core, "ADMIN_IDS", {"admin-1"}),
            patch.object(core, "web_feature_enabled", return_value=True),
        ):
            response = self.client.post(
                "/upload_image",
                headers={"X-Forwarded-For": "127.0.0.1"},
                environ_base={"REMOTE_ADDR": "203.0.113.5"},
            )
        self.assertEqual(response.status_code, 401)

    def test_scheduler_management_requires_its_feature_flag(self) -> None:
        self._login_session()
        with (
            patch.object(core, "ADMIN_IDS", {"admin-1"}),
            patch.object(core, "web_feature_enabled", return_value=False),
        ):
            response = self.client.get("/scheduler")
        self.assertEqual(response.status_code, 403)

    def test_scheduler_api_can_toggle_and_queue_registered_job(self) -> None:
        class FakeJobManager:
            def list_jobs(self):
                return [{"id": "daily-reset", "enabled": True}]

            def set_enabled(self, job_id, enabled):
                return {"id": job_id, "enabled": enabled}

            def queue_manual_run(self, job_id):
                return {"id": job_id, "queued": True}

            def reschedule(self, job_id, trigger):
                return {"id": job_id, "trigger": trigger}

        self._login_session()
        with (
            patch.object(core, "ADMIN_IDS", {"admin-1"}),
            patch.object(core, "web_feature_enabled", return_value=True),
            patch.object(scheduler, "job_manager", FakeJobManager()),
        ):
            response = self.client.get("/api/scheduler/jobs")
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.get_json()["jobs"][0]["id"], "daily-reset")

            response = self.client.post(
                "/api/scheduler/jobs/daily-reset/enabled",
                json={"enabled": False},
                headers={"X-CSRF-Token": "csrf-token"},
            )
            self.assertFalse(response.get_json()["job"]["enabled"])

            response = self.client.post(
                "/api/scheduler/jobs/daily-reset/run",
                json={},
                headers={"X-CSRF-Token": "csrf-token"},
            )
            self.assertTrue(response.get_json()["queued"])

            response = self.client.post(
                "/api/scheduler/jobs/daily-reset/schedule",
                json={"trigger": {"type": "interval", "seconds": 60}},
                headers={"X-CSRF-Token": "csrf-token"},
            )
            self.assertEqual(response.get_json()["job"]["trigger"]["seconds"], 60)

    def test_scheduler_write_requires_csrf_token(self) -> None:
        self._login_session()
        with (
            patch.object(core, "ADMIN_IDS", {"admin-1"}),
            patch.object(core, "web_feature_enabled", return_value=True),
        ):
            response = self.client.post(
                "/api/scheduler/jobs/daily-reset/enabled",
                json={"enabled": False},
            )
        self.assertEqual(response.status_code, 403)


if __name__ == "__main__":
    unittest.main()
