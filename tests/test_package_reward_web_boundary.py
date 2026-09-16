from __future__ import annotations

import unittest
from unittest.mock import Mock

from flask import Flask

from nonebot_plugin_xiuxian_2.adapters.web.blueprints.package_reward import create_blueprint


class PackageRewardWebTests(unittest.TestCase):
    def test_open_route_passes_idempotency_and_rewards_to_application(self) -> None:
        application = Mock()
        outcome = Mock(ok=True, code=None, message="", to_dict=lambda: {"status": "applied"})
        application.open_package.return_value = outcome
        app = Flask(__name__)
        app.secret_key = "test"
        app.register_blueprint(create_blueprint(application=application, permission=lambda _: True))

        client = app.test_client()
        with client.session_transaction() as session:
            session["_csrf_token"] = "csrf"
        response = client.post(
            "/api/v1/package-reward/open",
            headers={"Idempotency-Key": "package-1", "X-CSRF-Token": "csrf"},
            json={
                "user_id": "u1",
                "package_id": 9001,
                "quantity": 1,
                "max_goods_num": 100,
                "rewards": [{"item_id": 7, "name": "丹药", "item_type": "丹药", "quantity": 2}],
            },
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["data"], {"status": "applied"})
        kwargs = application.open_package.call_args.kwargs
        self.assertEqual(kwargs["operation_id"], "package-1")
        self.assertEqual(kwargs["user_id"], "u1")
        self.assertEqual(kwargs["rewards"][0].item_id, 7)


if __name__ == "__main__":
    unittest.main()
