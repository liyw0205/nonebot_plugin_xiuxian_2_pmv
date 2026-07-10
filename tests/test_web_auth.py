from __future__ import annotations

import unittest
from unittest.mock import patch

import nonebot
from werkzeug.security import generate_password_hash


nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_web import app
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_web import pages
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


if __name__ == "__main__":
    unittest.main()
