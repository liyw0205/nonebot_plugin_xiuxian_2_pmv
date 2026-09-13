from __future__ import annotations

import unittest
from unittest.mock import Mock

from flask import Flask

from nonebot_plugin_xiuxian_2.adapters.web.blueprints.bank_first_use import create_upgrade_blueprint


class BankUpgradeWebTests(unittest.TestCase):
    def test_upgrade_route_passes_idempotency_and_payload(self) -> None:
        application = Mock()
        application.upgrade.return_value = {"status": "applied", "bank_level": "2"}
        app = Flask(__name__)
        app.secret_key = "test"
        app.register_blueprint(create_upgrade_blueprint(application=application, permission=lambda _: True))
        client = app.test_client()
        with client.session_transaction() as session:
            session["_csrf_token"] = "csrf"
        response = client.post(
            "/api/v1/bank/v2/upgrade",
            headers={"Idempotency-Key": "upgrade-web-1", "X-CSRF-Token": "csrf"},
            json={"user_id": "u1", "expected_level": "1", "next_level": "2", "cost": 200000, "settled_at": "2026-09-13T00:00:00Z"},
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(application.upgrade.call_args.kwargs["operation_id"], "upgrade-web-1")
        self.assertEqual(application.upgrade.call_args.kwargs["next_level"], "2")


if __name__ == "__main__":
    unittest.main()
