from __future__ import annotations

import unittest
from unittest.mock import Mock

from flask import Flask

from nonebot_plugin_xiuxian_2.adapters.web.blueprints.bank_first_use import create_info_blueprint


class BankInfoWebTests(unittest.TestCase):
    def test_info_route_reads_application(self) -> None:
        application = Mock()
        application.get_info.return_value = {"status": "ok", "saved_stone": 20}
        app = Flask(__name__)
        app.secret_key = "test"
        app.register_blueprint(create_info_blueprint(application=application, permission=lambda _: True))
        response = app.test_client().get("/api/v1/bank/v2/info?user_id=u1")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(application.get_info.call_args.kwargs["user_id"], "u1")


if __name__ == "__main__":
    unittest.main()
