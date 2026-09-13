from __future__ import annotations

import unittest
from unittest.mock import Mock

from flask import Flask

from nonebot_plugin_xiuxian_2.adapters.web.blueprints.bank_first_use import create_first_use_blueprint


class BankFirstUseWebTests(unittest.TestCase):
    def test_deposit_route_calls_new_application(self) -> None:
        application = Mock()
        application.deposit.return_value = {"status": "applied", "saved_stone": 100}
        app = Flask(__name__)
        app.secret_key = "test"
        app.register_blueprint(create_first_use_blueprint(application=application, permission=lambda _: True))
        client = app.test_client()
        with client.session_transaction() as session:
            session["_csrf_token"] = "csrf"
        response = client.post("/api/v1/bank/v2/deposit", headers={"Idempotency-Key": "bank-web-1", "X-CSRF-Token": "csrf"}, json={"user_id": "u1", "amount": 100, "interest": 0, "limit": 1000, "bank_level": "1", "settled_at": "2026-09-13T00:00:00Z"})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(application.deposit.call_args.kwargs["operation_id"], "bank-web-1")
        self.assertEqual(response.get_json()["data"]["status"], "applied")


if __name__ == "__main__":
    unittest.main()
