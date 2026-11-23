from __future__ import annotations

import sqlite3
from types import SimpleNamespace

import pytest

from nonebot_plugin_xiuxian_2.core.errors import DomainError
from nonebot_plugin_xiuxian_2.features.dungeon.application import DungeonApplication
from nonebot_plugin_xiuxian_2.features.dungeon import migrations
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork


@pytest.fixture
def exploration(tmp_path):
    game, player = tmp_path / "game.db", tmp_path / "player.db"
    with DatabaseUnitOfWork(game, immediate=True) as uow:
        migrations.apply_dungeon_explore(uow)
        migrations.apply_dungeon_explore_resolution_intent(uow)
        uow.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,hp INTEGER,mp INTEGER,stone INTEGER,exp INTEGER)")
        uow.execute("INSERT INTO user_xiuxian VALUES('u',80,30,100,1000),('v',60,20,200,800)")
        uow.execute("CREATE TABLE user_cd(user_id TEXT PRIMARY KEY,type INTEGER)")
        uow.execute("INSERT INTO user_cd VALUES('u',0),('v',0)")
        uow.execute("CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,goods_num INTEGER,"
                    "create_time TEXT,update_time TEXT,bind_num INTEGER,UNIQUE(user_id,goods_id))")
    with DatabaseUnitOfWork(player, immediate=True) as uow:
        uow.execute("CREATE TABLE player_dungeon_status(user_id TEXT PRIMARY KEY,dungeon_id TEXT,dungeon_name TEXT,"
                    "dungeon_status TEXT,current_layer INTEGER,total_layers INTEGER,last_reset_date TEXT,"
                    "reset_generation INTEGER,reset_operation_id TEXT)")
        uow.execute("INSERT INTO player_dungeon_status VALUES('u','d','D','not_started',0,3,'2026-11-20',1,'reset')")
        uow.execute("CREATE TABLE teams(user_id TEXT PRIMARY KEY,leader TEXT,members TEXT,version INTEGER)")
        status = uow.query_one("SELECT * FROM player_dungeon_status WHERE user_id='u'")
        status.pop("user_id")
    plan = {"expected_status": status, "team": None, "members": [{"user_id": "u",
            "expected": {"hp": 80, "mp": 30, "stone": 100, "exp": 1000, "cd_type": 0},
            "final_hp": 55, "final_mp": 12, "stone_delta": 7, "exp_delta": 9,
            "items": [{"id": 9, "name": "reward", "type": "item", "amount": 1, "expected_num": 0, "expected_bind_num": 0}]}],
            "advance": True, "complete": False, "response": {"message": "settled", "battle_messages": []}}
    app = DungeonApplication(game, player)
    return SimpleNamespace(game=game, player=player, app=app, plan=plan)


def snapshot(fixture):
    result = []
    for database in (fixture.game, fixture.player):
        with DatabaseUnitOfWork(database, read_only=True) as uow:
            result.append({row["name"]: uow.query_all('SELECT * FROM "' + row["name"] + '"')
                           for row in uow.query_all("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")})
            assert uow.query_one("PRAGMA integrity_check") == {"integrity_check": "ok"}
    return result


def execute(database, sql):
    with DatabaseUnitOfWork(database, immediate=True) as uow:
        uow.execute(sql)


def prepare(fixture):
    assert fixture.app.prepare_intent(operation_id="op", user_id="u", intent={"seed": "fixed"})["phase"] == "intent"
    assert fixture.app.prepare_resolution(operation_id="op", user_id="u", plan=fixture.plan)["phase"] == "prepared"


@pytest.mark.parametrize("step", ["intent", "prepared", "rejection_insert", "rejection_update", "conflict"])
@pytest.mark.parametrize("fault", ["ignore", "rewrite"])
def test_durable_stage_rejection_rolls_back_and_same_operation_recovers(exploration, step, fault):
    fixture = exploration
    if step in {"prepared", "rejection_update"}:
        fixture.app.prepare_intent(operation_id="op", user_id="u", intent={"seed": "fixed"})
    elif step == "conflict":
        prepare(fixture)
        execute(fixture.game, "UPDATE user_xiuxian SET hp=79 WHERE user_id='u'")
    timing = "INSERT" if step in {"intent", "rejection_insert"} else "UPDATE"
    body = "SELECT RAISE(IGNORE)" if fault == "ignore" else "UPDATE dungeon_explore_operations SET request_identity='changed' WHERE operation_id=NEW.operation_id"
    before = snapshot(fixture)
    execute(fixture.game, f"CREATE TRIGGER fault {'BEFORE' if fault == 'ignore' else 'AFTER'} {timing} ON dungeon_explore_operations "
            f"BEGIN UPDATE user_cd SET type=99 WHERE user_id='v'; {body}; END")

    def action():
        if step == "intent":
            return fixture.app.prepare_intent(operation_id="op", user_id="u", intent={"seed": "fixed"})
        if step == "prepared":
            return fixture.app.prepare_resolution(operation_id="op", user_id="u", plan=fixture.plan)
        if step == "conflict":
            return fixture.app.settle(operation_id="op", user_id="u", max_goods_num=99)
        return fixture.app.resolve_rejection(operation_id="op", user_id="u", result_status="member_busy",
                                             response={"message": "busy"}, max_goods_num=99)

    reason_step = step if step in {"intent", "prepared"} else "receipt"
    reason = f"dungeon_explore_{reason_step}_" + ("write_rejected" if fault == "ignore" else "projection_mismatch")
    with pytest.raises(DomainError) as error:
        action()
    assert (error.value.code, error.value.details["subject"]) == (reason, "u")
    assert snapshot(fixture) == before
    execute(fixture.game, "DROP TRIGGER fault")
    result = action()
    assert result["phase"] == (step if step in {"intent", "prepared"} else "completed")
    if step not in {"intent", "prepared"}:
        committed = snapshot(fixture)
        replay = fixture.app.replay(operation_id="op", user_id="u")
        assert replay["response"] == result["response"]
        assert snapshot(fixture) == committed


SETTLEMENT_FAULTS = [
    ("game", "BEFORE UPDATE ON user_xiuxian", "NEW.user_id='u'", "SELECT RAISE(IGNORE)", "resources_write_rejected", "u"),
    ("game", "AFTER UPDATE ON user_xiuxian", "NEW.user_id='u'", "UPDATE user_xiuxian SET hp=1 WHERE user_id=NEW.user_id", "resources_projection_mismatch", "u"),
    ("game", "BEFORE INSERT ON back", "1", "SELECT RAISE(IGNORE)", "inventory_write_rejected", "u:9"),
    ("game", "AFTER INSERT ON back", "1", "UPDATE back SET bind_num=0 WHERE user_id=NEW.user_id", "inventory_projection_mismatch", "u:9"),
    ("player", "BEFORE UPDATE ON player_dungeon_status", "1", "SELECT RAISE(IGNORE)", "progress_write_rejected", "u"),
    ("player", "AFTER UPDATE ON player_dungeon_status", "1", "UPDATE player_dungeon_status SET dungeon_name='changed' WHERE user_id=NEW.user_id", "progress_projection_mismatch", "u"),
    ("game", "BEFORE UPDATE ON dungeon_explore_operations", "NEW.phase='completed'", "SELECT RAISE(IGNORE)", "receipt_write_rejected", "u"),
    ("game", "AFTER UPDATE ON dungeon_explore_operations", "NEW.phase='completed'", "UPDATE dungeon_explore_operations SET prepared_json='{}' WHERE operation_id=NEW.operation_id", "receipt_projection_mismatch", "u"),
    ("game", "AFTER UPDATE ON dungeon_explore_operations", "NEW.phase='completed'", "UPDATE user_xiuxian SET stone=0 WHERE user_id='u'", "resources_projection_mismatch", "u"),
    ("game", "AFTER UPDATE ON dungeon_explore_operations", "NEW.phase='completed'", "UPDATE back SET goods_num=0,bind_num=0 WHERE user_id='u'", "inventory_projection_mismatch", "u:9"),
    ("player", "AFTER UPDATE ON player_dungeon_status", "1", "UPDATE player_dungeon_status SET total_layers=99 WHERE user_id=NEW.user_id", "progress_projection_mismatch", "u"),
]


@pytest.mark.parametrize("database,timing,condition,body,suffix,subject", SETTLEMENT_FAULTS)
def test_settlement_write_rejection_preserves_frozen_plan_and_all_assets(exploration, database, timing, condition, body, suffix, subject):
    fixture = exploration
    prepare(fixture)
    before = snapshot(fixture)
    side_effect = "UPDATE user_cd SET type=99 WHERE user_id='v'; " if database == "game" else ""
    execute(getattr(fixture, database), f"CREATE TRIGGER fault {timing} WHEN {condition} BEGIN {side_effect}{body}; END")
    reason = ("dungeon_" if suffix.startswith("progress_") else "dungeon_explore_") + suffix
    with pytest.raises(DomainError) as error:
        fixture.app.settle(operation_id="op", user_id="u", max_goods_num=99)
    assert (error.value.code, error.value.details["subject"]) == (reason, subject)
    assert snapshot(fixture) == before
    execute(getattr(fixture, database), "DROP TRIGGER fault")
    result = fixture.app.settle(operation_id="op", user_id="u", max_goods_num=99)
    assert (result["phase"], result["result_status"], result["current_layer"]) == ("completed", "applied", 1)
    with DatabaseUnitOfWork(fixture.game, read_only=True) as uow:
        assert uow.query_one("SELECT hp,mp,stone,exp FROM user_xiuxian WHERE user_id='u'") == {"hp": 55, "mp": 12, "stone": 107, "exp": 1009}
        assert uow.query_one("SELECT goods_num,bind_num FROM back WHERE user_id='u'") == {"goods_num": 1, "bind_num": 1}
    committed = snapshot(fixture)
    assert fixture.app.settle(operation_id="op", user_id="u", max_goods_num=99)["status"] == "duplicate"
    assert snapshot(fixture) == committed


@pytest.mark.parametrize("target", ["intent", "prepared", "resources", "inventory", "progress", "receipt"])
def test_sqlite_abort_remains_original_and_entire_transaction_recovers(exploration, target):
    fixture = exploration
    if target != "intent":
        fixture.app.prepare_intent(operation_id="op", user_id="u", intent={"seed": "fixed"})
    if target not in {"intent", "prepared"}:
        fixture.app.prepare_resolution(operation_id="op", user_id="u", plan=fixture.plan)
    database, timing = {
        "intent": (fixture.game, "INSERT ON dungeon_explore_operations"),
        "prepared": (fixture.game, "UPDATE ON dungeon_explore_operations"),
        "resources": (fixture.game, "UPDATE ON user_xiuxian"),
        "inventory": (fixture.game, "INSERT ON back"),
        "progress": (fixture.player, "UPDATE ON player_dungeon_status"),
        "receipt": (fixture.game, "UPDATE ON dungeon_explore_operations"),
    }[target]
    before = snapshot(fixture)
    execute(database, f"CREATE TRIGGER fault BEFORE {timing} BEGIN SELECT RAISE(ABORT,'isolated exploration abort'); END")

    def action():
        if target == "intent":
            return fixture.app.prepare_intent(operation_id="op", user_id="u", intent={"seed": "fixed"})
        if target == "prepared":
            return fixture.app.prepare_resolution(operation_id="op", user_id="u", plan=fixture.plan)
        return fixture.app.settle(operation_id="op", user_id="u", max_goods_num=99)

    with pytest.raises(sqlite3.IntegrityError, match="isolated exploration abort"):
        action()
    assert snapshot(fixture) == before
    execute(database, "DROP TRIGGER fault")
    assert action()["phase"] in {"intent", "prepared", "completed"}


def test_non_numeric_resource_cannot_masquerade_as_successful_zero_mp(exploration):
    fixture = exploration
    fixture.plan["members"][0]["final_mp"] = 0
    prepare(fixture)
    before = snapshot(fixture)
    execute(fixture.game, "CREATE TRIGGER fault AFTER UPDATE ON user_xiuxian WHEN NEW.user_id='u' "
            "BEGIN UPDATE user_xiuxian SET mp='not-a-number' WHERE user_id=NEW.user_id; END")
    with pytest.raises(DomainError) as error:
        fixture.app.settle(operation_id="op", user_id="u", max_goods_num=99)
    assert (error.value.code, error.value.details["subject"]) == ("dungeon_explore_resources_projection_mismatch", "u")
    assert snapshot(fixture) == before
    execute(fixture.game, "DROP TRIGGER fault")
    assert fixture.app.settle(operation_id="op", user_id="u", max_goods_num=99)["result_status"] == "applied"
    with DatabaseUnitOfWork(fixture.game, read_only=True) as uow:
        assert uow.query_one("SELECT mp FROM user_xiuxian WHERE user_id='u'") == {"mp": 0}
