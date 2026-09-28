from __future__ import annotations

import json
import sqlite3

import pytest

from nonebot_plugin_xiuxian_2.core.errors import OperationConflictError
from nonebot_plugin_xiuxian_2.features.world_events.application import DemonClaimApplication
from nonebot_plugin_xiuxian_2.features.world_events.migrations import (
    apply_world_events_claim,
    apply_world_events_player,
)
from nonebot_plugin_xiuxian_2.features.world_events.repository import WorldEventClaimSqlRepository
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork, OperationLedger
from nonebot_plugin_xiuxian_2.plugin import build_migrations, migrations_for_database


def prepare_databases(tmp_path):
    game = tmp_path / "game.db"
    player = tmp_path / "player.db"
    with DatabaseUnitOfWork(game, immediate=True) as uow:
        uow.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone,exp)")
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
        uow.execute(
            "INSERT INTO world_event_state(user_id,event_id,claimed) VALUES(?,?,?)",
            ("global", "event-1", "{}"),
        )
    return game, player


def claim_kwargs(operation_id="claim-1", **changes):
    values = {
        "operation_id": operation_id,
        "event_key": "global",
        "event_id": "event-1",
        "user_id": "u",
        "expected_claimed": {},
        "stone": 100,
        "exp": 200,
        "items": ({"id": 7, "name": "灵签", "type": "特殊物品", "amount": 2},),
        "max_goods_num": 20,
    }
    return values | changes


def read_state(game, player):
    with sqlite3.connect(game) as conn:
        balance = conn.execute("SELECT stone,exp FROM user_xiuxian WHERE user_id='u'").fetchone()
        item = conn.execute("SELECT goods_num,bind_num FROM back WHERE user_id='u' AND goods_id=7").fetchone()
        operations = conn.execute("SELECT COUNT(*) FROM demon_claim_operations").fetchone()[0]
    with sqlite3.connect(player) as conn:
        claimed = json.loads(conn.execute("SELECT claimed FROM world_event_state WHERE user_id='global'").fetchone()[0])
    return balance, item, operations, claimed


def test_claim_application_owns_atomic_sql_claim_and_replay(tmp_path):
    game, player = prepare_databases(tmp_path)
    app = DemonClaimApplication(
        game,
        player,
        repository=WorldEventClaimSqlRepository(game, player),
    )

    first = app.claim(**claim_kwargs())
    replay = app.get_result("claim-1")
    second = app.claim(**claim_kwargs())

    assert first.ok and first.data["status"] == "applied"
    assert replay is not None and replay.ok and replay.replayed
    assert replay.data["stone"] == 100 and replay.data["exp"] == 200
    assert second.replayed
    assert read_state(game, player) == ((110, 220), (2, 2), 1, {"u": True})
    with pytest.raises(OperationConflictError):
        app.claim(**claim_kwargs("claim-1", stone=101))
    assert read_state(game, player) == ((110, 220), (2, 2), 1, {"u": True})


def test_rejected_claim_result_is_replayed_without_mutating_either_database(tmp_path):
    game, player = prepare_databases(tmp_path)
    with DatabaseUnitOfWork(game, immediate=True) as uow:
        uow.execute(
            "INSERT INTO back VALUES(?,?,?,?,?,?,?,?)",
            ("u", 7, "灵签", "特殊物品", 19, "", "", 19),
        )
    app = DemonClaimApplication(game, player)

    rejected = app.claim(**claim_kwargs("full"))
    replay = app.get_result("full")

    assert not rejected.ok and rejected.code == "inventory_full"
    assert replay is not None and not replay.ok and replay.replayed
    assert replay.code == "inventory_full"
    assert read_state(game, player) == ((10, 20), (19, 19), 0, {})


def test_claim_failure_rolls_back_game_and_player_changes(tmp_path):
    game, player = prepare_databases(tmp_path)
    with DatabaseUnitOfWork(game, immediate=True) as uow:
        uow.execute(
            "CREATE TRIGGER reject_claim_operation BEFORE INSERT ON demon_claim_operations "
            "BEGIN SELECT RAISE(ABORT,'reject claim operation'); END"
        )
    app = DemonClaimApplication(game, player)

    with pytest.raises(sqlite3.IntegrityError, match="reject claim operation"):
        app.claim(**claim_kwargs("rollback"))

    assert read_state(game, player) == ((10, 20), None, 0, {})


def test_large_claim_reward_uses_overflow_safe_sqlite_binding(tmp_path):
    game, player = prepare_databases(tmp_path)
    with DatabaseUnitOfWork(game, immediate=True) as uow:
        uow.execute("UPDATE user_xiuxian SET exp=? WHERE user_id='u'", (5.654041500655189e19,))
    app = DemonClaimApplication(game, player)

    result = app.claim(**claim_kwargs("large", items=(), exp=565404150065519040))

    assert result.ok
    with sqlite3.connect(game) as conn:
        exp = conn.execute("SELECT exp FROM user_xiuxian WHERE user_id='u'").fetchone()[0]
    assert float(exp) > 5.6e19
    assert abs(float(exp) - (5.654041500655189e19 + 565404150065519040)) / float(exp) < 1e-12


def test_claim_migration_is_game_only_and_upgrades_legacy_table(tmp_path):
    game = tmp_path / "legacy-game.db"
    with DatabaseUnitOfWork(game, immediate=True) as uow:
        uow.execute(
            "CREATE TABLE demon_claim_operations(operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,"
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )
        uow.execute("INSERT INTO demon_claim_operations(operation_id,payload) VALUES('old','payload')")
        apply_world_events_claim(uow)

    with sqlite3.connect(game) as conn:
        row = conn.execute(
            "SELECT operation_id,payload,stone,exp FROM demon_claim_operations WHERE operation_id='old'"
        ).fetchone()
    assert row == ("old", "payload", 0, 0)

    migrations = build_migrations()
    game_versions = {item.version for item in migrations_for_database(migrations, "game_db")}
    player_versions = {item.version for item in migrations_for_database(migrations, "player_db")}
    assert "world_events.003" in game_versions
    assert "world_events.003" not in player_versions
