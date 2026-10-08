from __future__ import annotations

import csv
import io
import sqlite3
import tempfile
import unittest

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.adapters.web.app import create_app
from nonebot_plugin_xiuxian_2.adapters.web.economy_ledger_csv import build_csv_response
from nonebot_plugin_xiuxian_2.bootstrap import build_runtime_context
from nonebot_plugin_xiuxian_2.features.economy_ledger.repository import ECONOMY_LOG_FIELDS
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_web.access import WebPermission, resolve_endpoint_permission


class EconomyLedgerWebTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        context = build_runtime_context(data_dir=self.directory.name)
        self.database = context.database.path("game_db")
        with sqlite3.connect(self.database) as connection:
            connection.execute(
                """CREATE TABLE economy_log (
                    id INTEGER PRIMARY KEY, user_id TEXT, sect_id TEXT, source TEXT,
                    action TEXT, stone_delta INTEGER, exp_delta INTEGER,
                    sect_contribution_delta INTEGER, sect_scale_delta INTEGER,
                    sect_materials_delta INTEGER, item_delta TEXT, detail TEXT,
                    trace_id TEXT, created_at TEXT
                )"""
            )
            connection.executemany(
                """INSERT INTO economy_log (
                    id, user_id, sect_id, source, action, stone_delta, exp_delta,
                    sect_contribution_delta, sect_scale_delta, sect_materials_delta,
                    item_delta, detail, trace_id, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    (1, "u1", "s1", "quest", "claim", 50, 4, 0, 0, 0, "[]", "{}", "t1", "2026-01-01 10:00:00"),
                    (2, "u2", "s1", "shop", "buy", -20, 0, 0, 0, 0, "[{\"item\":\"x\"}]", "{}", "t2", "2026-01-02 10:00:00"),
                ),
            )
        self.app = create_app(context=context)
        self.client = self.app.test_client()
        self.admin = {"X-Role": "admin"}

    def tearDown(self) -> None:
        self.directory.cleanup()

    def test_admin_api_and_ssr_page_use_economy_ledger(self) -> None:
        denied = self.client.get("/api/v1/economy-logs")
        self.assertEqual(denied.status_code, 403)

        response = self.client.get(
            "/api/v1/economy-logs?source=quest&page=1&page_size=1",
            headers=self.admin,
        )
        self.assertEqual(response.status_code, 200)
        body = response.get_json()
        self.assertTrue(body["ok"])
        self.assertEqual(body["data"]["total"], 1)
        self.assertEqual(body["data"]["rows"][0]["user_id"], "u1")
        self.assertIn("request_id", body)

        page = self.client.get("/pages/economy_logs?source=quest", headers=self.admin)
        self.assertEqual(page.status_code, 200)
        self.assertIn("经济流水".encode(), page.data)
        self.assertIn(b"quest", page.data)

    def test_legacy_urls_preserve_query_and_export_csv_contract(self) -> None:
        page = self.client.get("/economy_logs?user_id=u1", headers=self.admin)
        self.assertEqual(page.status_code, 308)
        self.assertEqual(page.headers["Location"], "/pages/economy_logs?user_id=u1")

        legacy_export = self.client.get(
            "/economy_logs/export?user_id=u1",
            headers=self.admin,
        )
        self.assertEqual(legacy_export.status_code, 308)
        self.assertEqual(
            legacy_export.headers["Location"],
            "/api/v1/economy-logs/export?user_id=u1",
        )

        response = self.client.get(legacy_export.headers["Location"], headers=self.admin)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.mimetype, "text/csv")
        self.assertEqual(
            response.headers["Content-Disposition"],
            "attachment; filename=economy_logs.csv",
        )
        rows = list(csv.DictReader(io.StringIO(response.get_data(as_text=True))))
        self.assertEqual(response.get_data(as_text=True).splitlines()[0].split(","), list(ECONOMY_LOG_FIELDS))
        self.assertEqual([row["user_id"] for row in rows], ["u1"])

    def test_export_query_failures_are_not_returned_as_empty_csv(self) -> None:
        class FailingApplication:
            def iter_export_rows(self, _filters):
                raise RuntimeError("ledger read failed")
                yield {}

        with self.assertRaisesRegex(RuntimeError, "ledger read failed"):
            build_csv_response(FailingApplication(), {})


class EconomyLedgerLegacyPermissionTests(unittest.TestCase):
    def test_default_wsgi_blueprint_endpoints_are_declared_as_read(self) -> None:
        for endpoint in (
            "economy_logs.page",
            "economy_logs.query",
            "economy_logs.export_csv",
        ):
            with self.subTest(endpoint=endpoint):
                self.assertEqual(
                    WebPermission.READ,
                    resolve_endpoint_permission(endpoint, "GET"),
                )


if __name__ == "__main__":
    unittest.main()
