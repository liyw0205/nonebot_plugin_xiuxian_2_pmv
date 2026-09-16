from __future__ import annotations

import re
import tempfile
import unittest

from nonebot_plugin_xiuxian_2.adapters.web.app import create_app
from nonebot_plugin_xiuxian_2.adapters.web.auth import HostPolicy
from nonebot_plugin_xiuxian_2.adapters.web.blueprints.pages import PAGE_ENDPOINTS
from nonebot_plugin_xiuxian_2.bootstrap import build_runtime_context
from nonebot_plugin_xiuxian_2.compatibility.commands import compatibility_hits
from nonebot_plugin_xiuxian_2.plugin import build_registry


class RefactoredWebTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.app = create_app(context=build_runtime_context(data_dir=self.directory.name))
        self.client = self.app.test_client()

    def tearDown(self) -> None:
        self.directory.cleanup()

    def test_public_health_registry_and_static_module(self) -> None:
        self.assertEqual(self.client.get("/health/live").status_code, 200)
        registry = self.client.get("/api/v1/registry")
        self.assertTrue(registry.get_json()["ok"])
        static = self.client.get("/static/pages/admin.js")
        try:
            self.assertEqual(static.status_code, 200)
        finally:
            static.close()

    def test_admin_page_and_permission_boundary(self) -> None:
        self.assertEqual(self.client.get("/pages/update").status_code, 403)
        response = self.client.get("/pages/update", headers={"X-Role": "admin"})
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"data-admin-page", response.data)

    def test_admin_pages_link_the_layout_stylesheet(self) -> None:
        """The JSON output panel must stay inside the mobile viewport.

        Without the shared stylesheet a large ``<pre>`` payload stretched the
        dashboard past the phone viewport; the browser smoke catches it live and
        this guard keeps the link from being dropped.
        """
        css = self.client.get("/static/app.css")
        try:
            self.assertEqual(css.status_code, 200)
            body = css.get_data(as_text=True)
        finally:
            css.close()
        self.assertIn("overflow-x: auto", body)
        self.assertIn("pre-wrap", body)

        for path in ("/pages/update", "/messages"):
            response = self.client.get(path, headers={"X-Role": "admin"})
            try:
                self.assertEqual(response.status_code, 200)
                self.assertIn(b"app.css", response.data)
            finally:
                response.close()

    def test_legacy_url_redirect_records_compatibility_hit(self) -> None:
        before = compatibility_hits().get("web:/config", 0)
        response = self.client.get("/config", headers={"X-Role": "admin"})
        self.assertEqual(response.status_code, 308)
        self.assertEqual(response.headers["Location"], "/pages/config")
        self.assertGreaterEqual(compatibility_hits().get("web:/config", 0), before + 1)

    def test_write_requires_csrf_and_returns_envelope(self) -> None:
        missing = self.client.post("/api/v1/config", headers={"X-Role": "admin"}, json={})
        self.assertEqual(missing.status_code, 403)
        token = self.client.get("/api/v1/csrf").get_json()["data"]["token"]
        response = self.client.post(
            "/api/v1/config",
            headers={"X-Role": "admin", "X-CSRF-Token": token},
            json={"web_port": 5999},
        )
        self.assertIn(response.status_code, {200, 400})
        self.assertIn("request_id", response.get_json())

    def test_reconcile_requires_platform_migration(self) -> None:
        response = self.client.get("/api/v1/reconcile", headers={"X-Role": "admin"})
        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.get_json()["error"]["code"], "migrations_required")

    def test_browser_login_home_and_logout_flow(self) -> None:
        class Settings:
            def get(self, name, default=None):
                return ["smoke-admin"] if name == "web_admin_ids" else default

        context = build_runtime_context(data_dir=self.directory.name, settings=Settings())
        client = create_app(context=context).test_client()
        self.assertEqual(client.get("/").status_code, 302)
        login = client.get("/login")
        self.assertEqual(login.status_code, 200)
        marker = b'name="_csrf_token" value="'
        start = login.data.index(marker) + len(marker)
        token = login.data[start:].split(b'"', 1)[0].decode()
        response = client.post(
            "/login",
            data={"admin_id": "smoke-admin", "_csrf_token": token},
            follow_redirects=True,
        )
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"data-admin-page", response.data)
        self.assertEqual(client.get("/logout").status_code, 302)

    def test_host_policy_preserves_ipv6_literals_and_ports(self) -> None:
        policy = HostPolicy(("127.0.0.1", "[::1]"))
        self.assertTrue(policy.allows("127.0.0.1:5888"))
        self.assertTrue(policy.allows("[::1]:5888"))
        self.assertTrue(policy.allows("::1"))
        self.assertFalse(policy.allows("203.0.113.10:5888"))

    def test_configured_host_allow_list_is_used_by_factory(self) -> None:
        class Settings:
            def get(self, name, default=None):
                return ["panel.example.test"] if name == "web_allowed_hosts" else default

        context = build_runtime_context(data_dir=self.directory.name, settings=Settings())
        client = create_app(context=context).test_client()
        self.assertEqual(client.get("/health/live", headers={"Host": "panel.example.test"}).status_code, 200)
        self.assertEqual(client.get("/health/live", headers={"Host": "127.0.0.1"}).status_code, 400)

    def test_all_admin_pages_have_anonymous_and_admin_states(self) -> None:
        for page in ("/", *PAGE_ENDPOINTS.values()):
            anonymous = self.client.get(page)
            self.assertIn(anonymous.status_code, {200, 302, 403}, page)
            administrator = self.client.get(page, headers={"X-Role": "admin"})
            self.assertNotEqual(administrator.status_code, 500, page)
            self.assertIn("X-Request-ID", administrator.headers, page)

    def test_declared_api_routes_have_csrf_and_validation_contracts(self) -> None:
        registry = build_registry()
        token = self.client.get("/api/v1/csrf").get_json()["data"]["token"]
        for feature in registry.features:
            for route in feature.routes:
                for method in route.methods:
                    if not route.path.startswith("/api/"):
                        continue
                    path = re.sub(r"<[^>]+>", "missing", route.path)
                    request = getattr(self.client, method.lower())
                    anonymous = request(path, json={} if method != "GET" else None)
                    self.assertNotEqual(anonymous.status_code, 500, f"anonymous {method} {path}")
                    if method in {"POST", "PUT", "PATCH", "DELETE"}:
                        missing_csrf = request(path, headers={"X-Role": "admin"}, json={})
                        self.assertEqual(missing_csrf.status_code, 403, f"csrf {method} {path}")
                        self.assertEqual(missing_csrf.get_json()["error"]["code"], "csrf_failed")
                    response = request(
                        path,
                        headers={"X-Role": "admin", "X-CSRF-Token": token},
                        json={} if method != "GET" else None,
                    )
                    self.assertNotEqual(response.status_code, 500, f"admin {method} {path}")
                    if response.content_type.startswith("application/json"):
                        body = response.get_json()
                        self.assertIn("request_id", body, f"request id {method} {path}")
                        self.assertNotEqual(body.get("error", {}).get("code"), "internal_error", f"error {method} {path}")


if __name__ == "__main__":
    unittest.main()
