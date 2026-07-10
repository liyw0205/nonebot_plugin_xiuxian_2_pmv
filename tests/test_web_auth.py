from __future__ import annotations

import unittest
from unittest.mock import patch

import nonebot
from werkzeug.security import generate_password_hash


nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_web import app
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_web import core, pages, scheduler, system
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_web.auth import (
    LoginAttemptLimiter,
    is_supported_password_hash,
    verify_password_hash,
)


class WebPasswordTests(unittest.TestCase):
    def test_password_hash_requires_supported_werkzeug_format(self) -> None:
        password_hash = generate_password_hash("correct horse battery staple")
        self.assertTrue(is_supported_password_hash(password_hash))
        self.assertTrue(
            verify_password_hash(password_hash, "correct horse battery staple")
        )
        self.assertFalse(verify_password_hash(password_hash, "wrong"))
        self.assertFalse(verify_password_hash("plain-text-password", "plain-text-password"))

    def test_login_limiter_locks_and_expires(self) -> None:
        now = [100.0]
        limiter = LoginAttemptLimiter(
            max_attempts=2,
            window_seconds=10,
            lock_seconds=20,
            clock=lambda: now[0],
        )

        self.assertFalse(limiter.record_failure("client"))
        self.assertTrue(limiter.record_failure("client"))
        self.assertTrue(limiter.is_blocked("client"))

        now[0] += 21
        self.assertFalse(limiter.is_blocked("client"))


class WebLoginRouteTests(unittest.TestCase):
    def setUp(self) -> None:
        app.config.update(TESTING=True, SECRET_KEY="test-secret")
        pages.web_login_limiter.clear()
        self.client = app.test_client()

    def _post_login(self, admin_id: str, password: str):
        with self.client.session_transaction() as session:
            session["_csrf_token"] = "csrf-token"
        return self.client.post(
            "/login",
            data={
                "admin_id": admin_id,
                "password": password,
                "_csrf_token": "csrf-token",
            },
        )

    def test_login_requires_independent_password(self) -> None:
        password_hash = generate_password_hash("panel-password")
        with (
            patch.object(pages, "ADMIN_IDS", {"admin-1"}),
            patch.object(pages, "web_auth_is_configured", return_value=True),
            patch.object(
                pages,
                "verify_web_password",
                side_effect=lambda value: verify_password_hash(password_hash, value),
            ),
        ):
            response = self._post_login("admin-1", "wrong-password")
            self.assertEqual(response.status_code, 401)
            with self.client.session_transaction() as session:
                self.assertNotIn("admin_id", session)

            response = self._post_login("admin-1", "panel-password")
            self.assertEqual(response.status_code, 302)
            with self.client.session_transaction() as session:
                self.assertEqual(session["admin_id"], "admin-1")

    def test_login_rejects_missing_auth_configuration(self) -> None:
        with (
            patch.object(pages, "ADMIN_IDS", {"admin-1"}),
            patch.object(pages, "web_auth_is_configured", return_value=False),
        ):
            response = self._post_login("admin-1", "anything")
        self.assertEqual(response.status_code, 503)

    def test_login_requires_csrf_token(self) -> None:
        with (
            patch.object(pages, "ADMIN_IDS", {"admin-1"}),
            patch.object(pages, "web_auth_is_configured", return_value=True),
            patch.object(pages, "verify_web_password", return_value=True),
        ):
            response = self.client.post(
                "/login",
                data={"admin_id": "admin-1", "password": "panel-password"},
            )
        self.assertEqual(response.status_code, 403)

    def test_login_rate_limit_blocks_repeated_failures(self) -> None:
        limiter = LoginAttemptLimiter(max_attempts=2, lock_seconds=60)
        with (
            patch.object(pages, "ADMIN_IDS", {"admin-1"}),
            patch.object(pages, "web_auth_is_configured", return_value=True),
            patch.object(pages, "verify_web_password", return_value=False),
            patch.object(pages, "web_login_limiter", limiter),
        ):
            self.assertEqual(
                self._post_login("admin-1", "wrong-password").status_code,
                401,
            )
            self.assertEqual(
                self._post_login("admin-1", "wrong-password").status_code,
                429,
            )
            self.assertEqual(
                self._post_login("admin-1", "wrong-password").status_code,
                429,
            )


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

    def test_terminal_requires_password_confirmation_and_expires(self) -> None:
        self._login_session()
        with (
            patch.object(core, "ADMIN_IDS", {"admin-1"}),
            patch.object(core, "web_feature_enabled", return_value=True),
        ):
            response = self.client.get("/terminal")
            self.assertEqual(response.status_code, 302)
            self.assertTrue(response.headers["Location"].endswith("/terminal/confirm"))

            with patch.object(system, "verify_web_password", return_value=False):
                response = self.client.post(
                    "/terminal/confirm",
                    data={"password": "wrong", "_csrf_token": "csrf-token"},
                )
            self.assertEqual(response.status_code, 401)

            with patch.object(system, "verify_web_password", return_value=True):
                response = self.client.post(
                    "/terminal/confirm",
                    data={"password": "correct", "_csrf_token": "csrf-token"},
                )
            self.assertEqual(response.status_code, 302)

            with self.client.session_transaction() as session:
                self.assertGreater(session["terminal_authorized_until"], core.time.time())
                session["terminal_authorized_until"] = core.time.time() - 1

            response = self.client.get("/terminal/pwd")
            self.assertEqual(response.status_code, 403)

    def test_local_upload_uses_direct_peer_address(self) -> None:
        with patch.object(core, "web_feature_enabled", return_value=True):
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
