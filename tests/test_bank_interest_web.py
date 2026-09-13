from __future__ import annotations

import unittest
from unittest.mock import Mock

from flask import Flask

from nonebot_plugin_xiuxian_2.adapters.web.blueprints.bank_first_use import create_interest_blueprint


class BankInterestWebTests(unittest.TestCase):
    def test_interest_route_calls_new_application(self) -> None:
        application = Mock()
        application.settle_interest.return_value = {"status": "applied", "interest": 7}
        app = Flask(__name__)
        app.secret_key = "test"
        app.register_blueprint(create_interest_blueprint(application=application, permission=lambda _: True))
        client = app.test_client()
        with client.session_transaction() as session:
            session["_csrf_token"] = "csrf"
        response = client.post("/api/v1/bank/v2/interest", headers={"Idempotency-Key": "interest-web-1", "X-CSRF-Token": "csrf"}, json={"user_id": "u1", "interest": 7, "bank_level": "1", "settled_at": "2026-09-13T00:00:00Z"})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(application.settle_interest.call_args.kwargs["operation_id"], "interest-web-1")


if __name__ == "__main__":
    unittest.main()
