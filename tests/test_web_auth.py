from __future__ import annotations

import unittest
from unittest.mock import patch

import nonebot
nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_web import app
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_web import core, pages, scheduler, system
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_web import reward_center
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

    def test_home_and_update_pages_require_admin_session(self) -> None:
        with patch.object(core, "ADMIN_IDS", {"admin-1"}):
            home = self.client.get("/")
            update = self.client.get("/update")
        self.assertEqual(home.status_code, 302)
        self.assertTrue(home.headers["Location"].endswith("/login"))
        self.assertEqual(update.status_code, 302)
        self.assertTrue(update.headers["Location"].endswith("/login"))

        with self.client.session_transaction() as session:
            session["admin_id"] = "admin-1"
            session["_csrf_token"] = "csrf-token"
        with patch.object(core, "ADMIN_IDS", {"admin-1"}):
            home = self.client.get("/")
            update = self.client.get("/update")

        self.assertEqual(home.status_code, 200)
        self.assertIn("admin-1", home.get_data(as_text=True))
        self.assertEqual(update.status_code, 200)

    def test_public_static_page_routes_keep_fixed_responses(self) -> None:
        with patch.object(core, "ADMIN_IDS", {"admin-1"}):
            favicon = self.client.get("/favicon.ico")
            robots = self.client.get("/robots.txt")

        self.assertEqual(favicon.status_code, 204)
        self.assertEqual(favicon.get_data(), b"")
        self.assertEqual(robots.status_code, 200)
        self.assertEqual(robots.get_data(as_text=True), "User-agent: *\nDisallow: /\n")
        self.assertEqual(robots.mimetype, "text/plain")

    def test_logout_clears_admin_session(self) -> None:
        with self.client.session_transaction() as session:
            session["admin_id"] = "admin-1"
            session["_csrf_token"] = "csrf-token"
        with patch.object(core, "ADMIN_IDS", {"admin-1"}):
            response = self.client.get("/logout")

        self.assertEqual(response.status_code, 302)
        self.assertTrue(response.headers["Location"].endswith("/login"))
        with self.client.session_transaction() as session:
            self.assertNotIn("admin_id", session)


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

    def test_reward_center_read_uses_one_feature_snapshot_and_keeps_legacy_shape(self) -> None:
        with patch.object(core, "ADMIN_IDS", {"admin-1"}):
            anonymous = self.client.get("/api/reward-center/records?kind=compensation")
        self.assertEqual(anonymous.status_code, 401)
        self.assertFalse(anonymous.get_json()["success"])

        self._login_session()
        records = [{
            "id": "C1",
            "kind": "compensation",
            "items": [],
            "items_text": "",
            "reward_text": "",
            "reason": "maintenance",
            "start_time": None,
            "expire_time": "无限",
            "create_time": "2026-10-02 12:00:00",
            "usage_limit": 0,
            "used_count": 2,
            "claimed_count": 2,
            "version": 1,
        }]
        with (
            patch.object(core, "ADMIN_IDS", {"admin-1"}),
            patch.object(reward_center, "reward_center_records", return_value=[
                {
                    "id": "C1",
                    "record": {
                        "items": [],
                        "reason": "maintenance",
                        "create_time": "2026-10-02 12:00:00",
                        "_definition_version": 1,
                    },
                    "used_count": 2,
                    "claimed_count": 2,
                }
            ]) as snapshot,
            patch.object(reward_center, "create_item_message", return_value=[]),
        ):
            response = self.client.get("/api/reward-center/records?kind=compensation")

        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.get_json()["success"])
        self.assertEqual(response.get_json()["records"], records)
        snapshot.assert_called_once_with(reward_center.DATA_CONFIG["补偿"])

    def test_reward_center_editor_remains_an_authenticated_compatibility_page(self) -> None:
        with patch.object(core, "ADMIN_IDS", {"admin-1"}):
            anonymous = self.client.get("/reward-center")
        self.assertEqual(anonymous.status_code, 302)
        self.assertTrue(anonymous.headers["Location"].endswith("/login"))

        self._login_session()
        with patch.object(core, "ADMIN_IDS", {"admin-1"}):
            response = self.client.get("/reward-center")

        self.assertEqual(response.status_code, 200)
        self.assertIn(b'id="rewardForm"', response.data)
        self.assertIn(b"/api/reward-center/records", response.data)

    def test_reward_center_save_uses_versioned_application_for_compensation(self) -> None:
        from types import SimpleNamespace

        self._login_session()
        saved = {"id": "C1", "kind": "compensation", "version": 4}
        with (
            patch.object(core, "ADMIN_IDS", {"admin-1"}),
            patch.object(
                reward_center,
                "_normalize_payload",
                return_value=("compensation", "C1", {"items": [], "_definition_version": 3}),
            ),
            patch.object(
                reward_center,
                "upsert_reward_definition",
                return_value=SimpleNamespace(succeeded=True, status="updated"),
            ) as upsert,
            patch.object(reward_center, "_serialize_records", return_value=[saved]),
        ):
            response = self.client.post(
                "/api/reward-center/records",
                json={"kind": "compensation", "id": "C1"},
                headers={"X-CSRF-Token": "csrf-token", "Idempotency-Key": "save-1"},
            )

        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.get_json()["success"])
        self.assertEqual(response.get_json()["record"], saved)
        self.assertEqual(upsert.call_args.args[0], reward_center.DATA_CONFIG["补偿"])
        self.assertEqual(upsert.call_args.args[1], "reward-center:admin-1:save-1")
        self.assertEqual(upsert.call_args.args[3], "C1")
        self.assertEqual(upsert.call_args.args[4]["_definition_version"], 3)

    def test_reward_center_delete_and_clear_keep_legacy_response_contract(self) -> None:
        from types import SimpleNamespace

        self._login_session()
        with (
            patch.object(core, "ADMIN_IDS", {"admin-1"}),
            patch.object(reward_center, "delete_record", return_value=SimpleNamespace(succeeded=True)) as delete,
            patch.object(reward_center, "clear_records", return_value=SimpleNamespace(succeeded=True)) as clear,
            patch.object(reward_center, "_serialize_records", return_value=[]),
        ):
            deleted = self.client.delete(
                "/api/reward-center/records/gift/G1",
                headers={"X-CSRF-Token": "csrf-token"},
            )
            cleared = self.client.post(
                "/api/reward-center/records/gift/clear",
                headers={"X-CSRF-Token": "csrf-token"},
            )

        self.assertTrue(deleted.get_json()["success"])
        self.assertTrue(cleared.get_json()["success"])
        self.assertEqual(delete.call_args.args[:2], ("G1", reward_center.DATA_CONFIG["礼包"]))
        self.assertEqual(clear.call_args.args[0], reward_center.DATA_CONFIG["礼包"])

    def test_terminal_confirmation_uses_superuser_session(self) -> None:
        self._login_session()
        with patch.object(core, "ADMIN_IDS", {"admin-1"}):
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

    def test_dashboard_process_snapshot_delegates_to_status_owner(self) -> None:
        class FakeStatusApplication:
            def process_info(self, limit=5):
                self.limit = limit
                return [{"pid": 1001, "name": "fake-process", "memory_mb": 8.0}]

        status_application = FakeStatusApplication()
        with patch.object(system, "status_application", status_application):
            processes = system._collect_process_snapshot(5)

        self.assertEqual(processes[0]["name"], "fake-process")
        self.assertEqual(processes[0]["memory_mb"], 8.0)
        self.assertEqual(status_application.limit, 5)

    def test_local_upload_uses_direct_peer_address(self) -> None:
        with patch.object(core, "ADMIN_IDS", {"admin-1"}):
            response = self.client.post(
                "/upload_image",
                headers={"X-Forwarded-For": "127.0.0.1"},
                environ_base={"REMOTE_ADDR": "203.0.113.5"},
            )
        self.assertEqual(response.status_code, 401)

    def test_scheduler_api_can_toggle_and_queue_registered_job(self) -> None:
        class FakeSchedulerApplication:
            def list_jobs(self):
                return [{"id": "daily-reset", "enabled": True}]

            def set_enabled(self, job_id, enabled):
                return {"id": job_id, "enabled": enabled}

            def queue_manual_run(self, job_id):
                return {"id": job_id, "queued": True, "run_id": "run-1", "status": "queued"}

            def get_run(self, run_id):
                return {"run_id": run_id, "job_id": "daily-reset", "status": "succeeded"}

            def reschedule(self, job_id, trigger):
                return {"id": job_id, "trigger": trigger}

        self._login_session()
        with (
            patch.object(core, "ADMIN_IDS", {"admin-1"}),
            patch.object(scheduler, "scheduler_admin_application", FakeSchedulerApplication()),
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

            response = self.client.get("/api/scheduler/runs/run-1")
            self.assertEqual(response.get_json()["run"]["status"], "succeeded")

            response = self.client.post(
                "/api/scheduler/jobs/daily-reset/schedule",
                json={"trigger": {"type": "interval", "seconds": 60}},
                headers={"X-CSRF-Token": "csrf-token"},
            )
            self.assertEqual(response.get_json()["job"]["trigger"]["seconds"], 60)

    def test_scheduler_write_requires_csrf_token(self) -> None:
        self._login_session()
        with patch.object(core, "ADMIN_IDS", {"admin-1"}):
            response = self.client.post(
                "/api/scheduler/jobs/daily-reset/enabled",
                json={"enabled": False},
            )
        self.assertEqual(response.status_code, 403)


if __name__ == "__main__":
    unittest.main()
