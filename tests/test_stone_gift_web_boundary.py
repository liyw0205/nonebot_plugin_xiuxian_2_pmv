from __future__ import annotations

import unittest
from unittest.mock import Mock

from flask import Flask

from nonebot_plugin_xiuxian_2.adapters.web.api import create_stone_gift_blueprint


class StoneGiftWebBoundaryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.application = Mock()
        outcome = Mock()
        outcome.ok = True
        outcome.to_dict.return_value = {"status": "applied", "operation_id": "web-op"}
        self.application.transfer.return_value = outcome
        app = Flask(__name__)
        app.secret_key = "test-secret"
        app.register_blueprint(create_stone_gift_blueprint(self.application, permission=lambda _: True))
        self.client = app.test_client()
        with self.client.session_transaction() as session:
            session["_csrf_token"] = "csrf"

    def test_idempotency_key_and_payload_are_passed_to_application(self) -> None:
        response = self.client.post(
            "/api/v1/stone-gift",
            headers={"X-CSRF-Token": "csrf", "Idempotency-Key": "web-op"},
            json={"sender_id": "sender", "recipient_id": "recipient", "gross_amount": 500},
        )

        self.assertEqual(response.status_code, 200)
        kwargs = self.application.transfer.call_args.kwargs
        self.assertEqual(kwargs["operation_id"], "web-op")
        self.assertEqual(kwargs["gross_amount"], 500)
        self.assertNotIn("transaction_service", type(self.application).__module__)

    def test_permission_is_checked_before_application(self) -> None:
        app = Flask(__name__)
        app.secret_key = "test-secret"
        app.register_blueprint(create_stone_gift_blueprint(self.application, permission=lambda _: False))
        response = app.test_client().post(
            "/api/v1/stone-gift",
            json={"sender_id": "sender", "recipient_id": "recipient", "gross_amount": 500},
        )

        self.assertEqual(response.status_code, 403)
        self.application.transfer.assert_not_called()


if __name__ == "__main__":
    unittest.main()
