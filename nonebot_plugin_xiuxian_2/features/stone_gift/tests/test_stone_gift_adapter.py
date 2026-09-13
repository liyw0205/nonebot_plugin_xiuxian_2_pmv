from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from flask import Flask

from ....infrastructure.database import DatabaseUnitOfWork
from ..application import StoneGiftApplication
from ..web import blueprint


class StoneGiftAdapterTests(unittest.TestCase):
    def test_api_requires_csrf_and_replays_idempotently(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "game.db"
            with DatabaseUnitOfWork(database) as uow:
                uow.execute("CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY, stone INTEGER NOT NULL)")
                uow.execute("INSERT INTO user_xiuxian VALUES (?, ?)", ("sender", 1000))
                uow.execute("INSERT INTO user_xiuxian VALUES (?, ?)", ("recipient", 100))
            app = Flask(__name__)
            app.secret_key = "test"
            app.register_blueprint(blueprint(StoneGiftApplication(database), permission=lambda _: True))
            with app.test_client() as client:
                self.assertEqual(client.post("/api/v1/stone-gift", json={}).status_code, 403)
                with client.session_transaction() as session:
                    session["_csrf_token"] = "token"
                headers = {"X-CSRF-Token": "token", "Idempotency-Key": "gift-web"}
                first = client.post(
                    "/api/v1/stone-gift", headers=headers,
                    json={"sender_id": "sender", "recipient_id": "recipient", "gross_amount": 500},
                )
                second = client.post(
                    "/api/v1/stone-gift", headers=headers,
                    json={"sender_id": "sender", "recipient_id": "recipient", "gross_amount": 500},
                )
                self.assertEqual((first.status_code, second.status_code), (200, 200))
                self.assertEqual(second.json["data"]["status"], "replayed")
                self.assertEqual(second.json["data"]["data"], first.json["data"]["data"])


if __name__ == "__main__":
    unittest.main()
