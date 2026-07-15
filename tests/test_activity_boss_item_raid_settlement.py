import sqlite3

import nonebot
import pytest

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_activity.transaction_service import (
    ActivityBossItemRaidSettlementService,
)
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_activity.activity_boss import _fixed_item_damage


def create_database(tmp_path, inventory=4):
    database = tmp_path / "activity.db"
    conn = sqlite3.connect(database)
    conn.executescript(
        """
        CREATE TABLE activity_item_inventory(activity_key TEXT,user_id TEXT,item_id TEXT,count INTEGER,update_time TEXT,PRIMARY KEY(activity_key,user_id,item_id));
        CREATE TABLE activity_boss_state(activity_key TEXT PRIMARY KEY,hp_left INTEGER,max_hp INTEGER,update_time TEXT);
        CREATE TABLE activity_boss_damage(activity_key TEXT,user_id TEXT,total_damage INTEGER,update_time TEXT,PRIMARY KEY(activity_key,user_id));
        CREATE TABLE activity_boss_fight_log(id INTEGER PRIMARY KEY AUTOINCREMENT,activity_key TEXT,user_id TEXT,damage INTEGER,fight_date TEXT,source TEXT,create_time TEXT);
        CREATE TABLE activity_boss_milestone(activity_key TEXT,milestone_key TEXT,unlocked_time TEXT,PRIMARY KEY(activity_key,milestone_key));
        INSERT INTO activity_boss_state VALUES('boss',500,500,'');
        """
    )
    conn.execute("INSERT INTO activity_item_inventory VALUES('boss','u','firework',?,'')", (inventory,))
    conn.commit()
    conn.close()
    return database


def settle(service, operation_id="item-op", damage=180, expected_inventory=4, expected_hp=500):
    return service.settle(
        operation_id, "u", "boss", "firework", expected_inventory, 2,
        expected_hp, 500, 0, 3, damage, "2026-07-14", "2026-07-14 10:00:00",
        [{"key": "p70", "hp_percent": 70}],
    )


def test_inventory_damage_score_hp_log_and_operation_are_atomic(tmp_path):
    database = create_database(tmp_path)
    result = settle(ActivityBossItemRaidSettlementService(database))
    assert (result.status, result.damage, result.hp_left, result.inventory) == ("applied", 180, 320, 2)
    conn = sqlite3.connect(database)
    assert conn.execute("SELECT count FROM activity_item_inventory").fetchone()[0] == 2
    assert conn.execute("SELECT hp_left FROM activity_boss_state").fetchone()[0] == 320
    assert conn.execute("SELECT total_damage FROM activity_boss_damage").fetchone()[0] == 180
    assert conn.execute("SELECT damage,source FROM activity_boss_fight_log").fetchone() == (180, "item")
    assert conn.execute("SELECT milestone_key FROM activity_boss_milestone").fetchone()[0] == "p70"
    conn.close()


def test_replay_conflict_and_inventory_change_do_not_double_consume(tmp_path):
    database = create_database(tmp_path)
    service = ActivityBossItemRaidSettlementService(database)
    assert settle(service).status == "applied"
    assert settle(service).status == "duplicate"
    assert settle(service, damage=181).status == "operation_conflict"
    assert settle(service, operation_id="new", expected_inventory=4).status == "state_changed"
    conn = sqlite3.connect(database)
    assert conn.execute("SELECT count FROM activity_item_inventory").fetchone()[0] == 2
    assert conn.execute("SELECT COUNT(*) FROM activity_boss_fight_log").fetchone()[0] == 1
    conn.close()


def test_insufficient_inventory_and_operation_failure_roll_back(tmp_path):
    database = create_database(tmp_path, inventory=1)
    service = ActivityBossItemRaidSettlementService(database)
    assert settle(service, expected_inventory=1).status == "item_insufficient"
    conn = sqlite3.connect(database)
    conn.execute(
        "CREATE TABLE activity_boss_settlement_operations(operation_id TEXT PRIMARY KEY,payload TEXT,damage INTEGER,"
        "hp_left INTEGER,max_hp INTEGER,fight_count INTEGER,inventory INTEGER,created_at TEXT)"
    )
    conn.execute("UPDATE activity_item_inventory SET count=4")
    conn.execute(
        "CREATE TRIGGER reject_item_operation BEFORE INSERT ON activity_boss_settlement_operations "
        "BEGIN SELECT RAISE(ABORT,'reject operation'); END"
    )
    conn.commit()
    conn.close()
    with pytest.raises(Exception, match="reject operation"):
        settle(service)
    conn = sqlite3.connect(database)
    assert conn.execute("SELECT count FROM activity_item_inventory").fetchone()[0] == 4
    assert conn.execute("SELECT hp_left FROM activity_boss_state").fetchone()[0] == 500
    assert conn.execute("SELECT COUNT(*) FROM activity_boss_damage").fetchone()[0] == 0
    conn.close()


def test_real_entry_passes_event_operation_to_item_service():
    text = open(
        "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_activity/__init__.py", encoding="utf-8"
    ).read()
    handler = text[text.index("@activity_boss_atk_cmd.handle"):text.index("@activity_boss_claim_cmd.handle")]
    assert "use_item_on_boss(uid, raw, operation_id)" in handler


def test_item_damage_is_fixed_by_operation_id():
    damage = _fixed_item_damage("same-operation", 100, 500)
    assert damage == _fixed_item_damage("same-operation", 100, 500)
    assert 100 <= damage <= 500
