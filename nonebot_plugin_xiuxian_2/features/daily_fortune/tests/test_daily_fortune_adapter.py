from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from flask import Flask

from ..application import DailyFortuneApplication
from ..web import blueprint


class DailyFortuneAdapterTests(unittest.TestCase):
    def test_api_uses_uniform_response_shape(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            app = Flask(__name__)
            app.secret_key = "test"
            app.register_blueprint(blueprint(DailyFortuneApplication(str(Path(directory) / "game.db"))))
            with app.test_client() as client:
                response = client.get("/api/v1/daily-fortune?user_id=u")
                self.assertEqual(response.status_code, 200)
                self.assertTrue(response.json["ok"])
                self.assertIn("request_id", response.json)
                self.assertEqual(client.post("/api/v1/daily-fortune?user_id=v").status_code, 403)


if __name__ == "__main__":
    unittest.main()
