from __future__ import annotations

import json
import sqlite3

from nonebot_plugin_xiuxian_2.adapters.web.app import create_app
from nonebot_plugin_xiuxian_2.bootstrap import build_runtime_context
from nonebot_plugin_xiuxian_2.features.world_events.migrations import (
    apply_world_events_claim,
    apply_world_events_claim_statistics,
    apply_world_events_player,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork, OperationLedger


def test_real_web_route_uses_feature_owned_claim_repository(tmp_path):
    context = build_runtime_context(data_dir=tmp_path)
    game = context.database.path("game_db")
    player = context.database.path("player_db")
    with DatabaseUnitOfWork(game, immediate=True) as uow:
        uow.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER,exp INTEGER)")
        uow.execute("INSERT INTO user_xiuxian VALUES('u',10,20)")
        uow.execute(
            "CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,"
            "goods_num INTEGER,create_time TEXT,update_time TEXT,bind_num INTEGER,"
            "UNIQUE(user_id,goods_id))"
        )
        apply_world_events_claim(uow)
        OperationLedger().ensure_schema(uow)
    with DatabaseUnitOfWork(player, immediate=True) as uow:
        apply_world_events_player(uow)
        apply_world_events_claim_statistics(uow)
        uow.execute(
            "INSERT INTO world_event_state(user_id,event_id,claimed) VALUES(?,?,?)",
            ("global", "event-1", "{}"),
        )

    client = create_app(context=context).test_client()
    csrf = client.get("/api/v1/csrf").get_json()["data"]["token"]
    response = client.post(
        "/api/v1/world-events/demon/claim",
        headers={"Idempotency-Key": "web-claim-1", "X-CSRF-Token": csrf},
        json={
            "event_key": "global",
            "event_id": "event-1",
            "user_id": "u",
            "expected_claimed": {},
            "stone": 3,
            "exp": 5,
            "items": [],
            "max_goods_num": 20,
        },
    )

    assert response.status_code == 200
    assert response.get_json()["data"]["data"]["status"] == "applied"
    with sqlite3.connect(game) as conn:
        assert conn.execute("SELECT stone,exp FROM user_xiuxian WHERE user_id='u'").fetchone() == (13, 25)
        assert conn.execute("SELECT COUNT(*) FROM demon_claim_operations").fetchone()[0] == 1
    with sqlite3.connect(player) as conn:
        assert json.loads(conn.execute("SELECT claimed FROM world_event_state WHERE user_id='global'").fetchone()[0]) == {"u": True}
        assert conn.execute('SELECT "魔修入侵领奖" FROM statistics WHERE user_id=\'u\'').fetchone() == (1,)
