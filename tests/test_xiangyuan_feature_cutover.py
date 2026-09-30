from __future__ import annotations

import tempfile
from pathlib import Path

import nonebot
import pytest
from flask import Flask

nonebot.init()

from nonebot_plugin_xiuxian_2.features.base.application import BaseApplication
from nonebot_plugin_xiuxian_2.features.base.migrations import (
    apply_base_xiangyuan,
    apply_base_xiangyuan_player,
)
from nonebot_plugin_xiuxian_2.features.base.web import blueprint
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.plugin import build_migrations, migrations_for_database


def _databases(root: Path, *, migrate: bool = True) -> tuple[Path, Path]:
    game, player = root / "game.db", root / "player.db"
    with DatabaseUnitOfWork(game) as uow:
        uow.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY, stone INTEGER NOT NULL)")
        uow.execute("INSERT INTO user_xiuxian VALUES (?, ?)", ("giver", 2_000_000))
        uow.execute(
            "CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,"
            "goods_num INTEGER,create_time TEXT,update_time TEXT,bind_num INTEGER,state INTEGER,"
            "UNIQUE(user_id,goods_id))"
        )
        if migrate:
            apply_base_xiangyuan(uow)
    with DatabaseUnitOfWork(player) as uow:
        if migrate:
            apply_base_xiangyuan_player(uow)
    return game, player


def test_xiangyuan_migrations_are_split_by_database() -> None:
    migrations = build_migrations()
    game = {item.version for item in migrations_for_database(migrations, "game_db")}
    player = {item.version for item in migrations_for_database(migrations, "player_db")}
    assert "base.006" in game and "base.006" not in player
    assert "base.007" in player and "base.007" not in game


def test_xiangyuan_missing_startup_schema_fails_closed_without_ddl() -> None:
    with tempfile.TemporaryDirectory() as directory:
        game, player = _databases(Path(directory), migrate=False)
        application = BaseApplication(game, player)
        with pytest.raises(RuntimeError, match="schema_missing"):
            application.xiangyuan_create(
                operation_id="missing-schema",
                user_id="giver",
                group_id="group",
                giver_name="道友",
                stone=1_000_000,
                items=(),
                receiver_count=1,
            )
        with DatabaseUnitOfWork(game, read_only=True) as uow:
            assert uow.query_one(
                "SELECT 1 AS present FROM sqlite_master WHERE name='xiangyuan_gifts'"
            ) is None


def test_real_web_route_creates_and_replays_xiangyuan() -> None:
    with tempfile.TemporaryDirectory() as directory:
        game, player = _databases(Path(directory))
        app = Flask(__name__)
        app.secret_key = "test"
        app.register_blueprint(blueprint(BaseApplication(game, player), permission=lambda _: True))
        with app.test_client() as client:
            with client.session_transaction() as session:
                session["_csrf_token"] = "token"
            payload = {
                "user_id": "giver",
                "group_id": "group",
                "giver_name": "道友",
                "stone": 1_000_000,
                "items": [],
                "receiver_count": 1,
            }
            headers = {"X-CSRF-Token": "token", "Idempotency-Key": "web-xiangyuan"}
            first = client.post("/api/v1/base/xiangyuan/create", headers=headers, json=payload)
            second = client.post("/api/v1/base/xiangyuan/create", headers=headers, json=payload)
            assert first.status_code == second.status_code == 200
            assert first.json["data"]["status"] == "applied"
            assert second.json["data"]["status"] == "duplicate"
