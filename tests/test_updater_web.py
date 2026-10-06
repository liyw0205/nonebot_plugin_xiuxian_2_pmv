from __future__ import annotations

import unittest
from unittest.mock import patch

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_web import app, core, pages


class FakeUpdateApplication:
    def __init__(self) -> None:
        self.calls: list[tuple[object, ...]] = []

    def current_version(self) -> str:
        return "v1.0.0"

    def check_update(self):
        self.calls.append(("check_update",))
        return (
            {"tag_name": "v2.0.0", "name": "Release", "published_at": "date", "body": "notes"},
            "new release",
        )

    def latest_releases(self, count: int):
        self.calls.append(("latest_releases", count))
        return [{"tag_name": "v2.0.0", "name": "Release"}]

    def perform_update_with_backup(self, release_tag: object):
        self.calls.append(("perform_update", release_tag))
        return True, "updated"


class UpdaterWebRouteTests(unittest.TestCase):
    def setUp(self) -> None:
        app.config.update(TESTING=True, SECRET_KEY="test-secret")
        self.client = app.test_client()
        self.application = FakeUpdateApplication()

    def _login_session(self) -> None:
        with self.client.session_transaction() as session:
            session["admin_id"] = "admin-1"
            session["_csrf_token"] = "csrf-token"

    def test_update_routes_require_admin_and_keep_update_permission(self) -> None:
        with patch.object(core, "ADMIN_IDS", {"admin-1"}):
            response = self.client.get("/get_releases")
            self.assertEqual(response.status_code, 401)
            self._login_session()
            with patch.object(pages, "update_application", self.application):
                check = self.client.get("/check_update")
                releases = self.client.get("/get_releases")
                missing_csrf = self.client.post(
                    "/perform_update", json={"release_tag": "v2.0.0"}
                )
                update = self.client.post(
                    "/perform_update",
                    json={"release_tag": "v2.0.0"},
                    headers={"X-CSRF-Token": "csrf-token"},
                )

        self.assertEqual(check.status_code, 200)
        self.assertEqual(check.get_json()["latest_version"], "v2.0.0")
        self.assertEqual(releases.get_json()["releases"][0]["tag_name"], "v2.0.0")
        self.assertEqual(missing_csrf.status_code, 403)
        self.assertEqual(update.get_json(), {"success": True, "message": "updated"})
        self.assertEqual(
            self.application.calls,
            [("check_update",), ("latest_releases", 10), ("perform_update", "v2.0.0")],
        )

    def test_update_page_does_not_interpolate_release_metadata_as_html(self) -> None:
        with app.test_request_context("/update"):
            source = pages.render_template("update.html")
        self.assertNotIn("showChangelog('${data.latest_version}'", source)
        self.assertNotIn("onclick=\"performUpdate('${release.tag_name}')\"", source)
        self.assertIn("changelog.textContent = text", source)
        self.assertIn("button.addEventListener('click', () => performUpdate(release.tag_name))", source)


if __name__ == "__main__":
    unittest.main()
