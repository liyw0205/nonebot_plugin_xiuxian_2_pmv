from __future__ import annotations

import tempfile
from pathlib import Path

from flask import Flask

from nonebot_plugin_xiuxian_2.features.base.application import BaseApplication
from nonebot_plugin_xiuxian_2.features.base.migrations import apply_base_stone_contest_operations
from nonebot_plugin_xiuxian_2.features.base.web import blueprint
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork


def _database(root: Path) -> tuple[Path, Path]:
    game, player = root / "game.db", root / "player.db"
    with DatabaseUnitOfWork(game, immediate=True) as uow:
        uow.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER NOT NULL)")
        uow.execute("INSERT INTO user_xiuxian VALUES('payer',100)")
        uow.execute("INSERT INTO user_xiuxian VALUES('receiver',10)")
        apply_base_stone_contest_operations(uow)
    with DatabaseUnitOfWork(player, immediate=True):
        pass
    return game, player


def test_real_web_stone_contest_route_uses_feature_application_and_replays():
    with tempfile.TemporaryDirectory() as directory:
        game, player = _database(Path(directory))
        app = Flask(__name__)
        app.secret_key = "test"
        app.register_blueprint(blueprint(BaseApplication(game, player), permission=lambda _: True))
        with app.test_client() as client:
            with client.session_transaction() as session:
                session["_csrf_token"] = "token"
            headers = {"X-CSRF-Token": "token", "Idempotency-Key": "web-contest"}
            payload = {"payer_id": "payer", "receiver_id": "receiver", "requested_amount": 30}
            first = client.post("/api/v1/base/stone_contest", headers=headers, json=payload)
            second = client.post("/api/v1/base/stone_contest", headers=headers, json=payload)
            assert first.status_code == second.status_code == 200
            assert first.json["data"]["status"] == "transferred"
            assert second.json["data"]["status"] == "duplicate"
