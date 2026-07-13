import sqlite3

import nonebot
import pytest

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_activity.boss_coop_settlement_service import (
    ActivityBossCoopSettlementService,
)


def create_database(tmp_path):
    database = tmp_path / "activity.db"
    conn = sqlite3.connect(database)
    conn.executescript(
        """
        CREATE TABLE activity_boss_state(activity_key TEXT PRIMARY KEY,hp_left INTEGER,max_hp INTEGER,update_time TEXT);
        CREATE TABLE activity_boss_damage(activity_key TEXT,user_id TEXT,total_damage INTEGER,update_time TEXT,PRIMARY KEY(activity_key,user_id));
        CREATE TABLE activity_boss_fight_log(id INTEGER PRIMARY KEY AUTOINCREMENT,activity_key TEXT,user_id TEXT,damage INTEGER,fight_date TEXT,source TEXT,create_time TEXT);
        CREATE TABLE activity_boss_milestone(activity_key TEXT,milestone_key TEXT,unlocked_time TEXT,PRIMARY KEY(activity_key,milestone_key));
        INSERT INTO activity_boss_state VALUES('boss',1000,1000,'');
        """
    )
    conn.commit()
    conn.close()
    return database


def settle(service, operation_id="op", damage=250, expected_hp=1000, expected_count=0):
    return service.settle(
        operation_id, "u", "boss", expected_hp, 1000, expected_count, 3,
        damage, "2026-07-14", "2026-07-14 10:00:00",
        [{"key": "p80", "hp_percent": 80}],
    )


def test_fixed_damage_state_and_operation_are_atomic(tmp_path):
    database = create_database(tmp_path)
    result = settle(ActivityBossCoopSettlementService(database))
    assert (result.status, result.damage, result.hp_left, result.fight_count) == ("applied", 250, 750, 1)
    conn = sqlite3.connect(database)
    assert conn.execute("SELECT hp_left FROM activity_boss_state").fetchone()[0] == 750
    assert conn.execute("SELECT total_damage FROM activity_boss_damage").fetchone()[0] == 250
    assert conn.execute("SELECT damage,source FROM activity_boss_fight_log").fetchone() == (250, "coop")
    assert conn.execute("SELECT milestone_key FROM activity_boss_milestone").fetchone()[0] == "p80"
    assert conn.execute("SELECT COUNT(*) FROM activity_boss_settlement_operations").fetchone()[0] == 1
    conn.close()


def test_replay_conflict_and_snapshot_checks(tmp_path):
    database = create_database(tmp_path)
    service = ActivityBossCoopSettlementService(database)
    assert settle(service).status == "applied"
    assert settle(service).status == "duplicate"
    assert settle(service, damage=251).status == "operation_conflict"
    assert settle(service, operation_id="new", expected_hp=1000).status == "state_changed"
    conn = sqlite3.connect(database)
    assert conn.execute("SELECT COUNT(*) FROM activity_boss_fight_log").fetchone()[0] == 1
    conn.close()


def test_operation_failure_rolls_back_all_boss_writes(tmp_path):
    database = create_database(tmp_path)
    conn = sqlite3.connect(database)
    conn.execute(
        "CREATE TABLE activity_boss_settlement_operations(operation_id TEXT PRIMARY KEY,payload TEXT,damage INTEGER,"
        "hp_left INTEGER,max_hp INTEGER,fight_count INTEGER,inventory INTEGER,created_at TEXT)"
    )
    conn.execute(
        "CREATE TRIGGER reject_boss_operation BEFORE INSERT ON activity_boss_settlement_operations "
        "BEGIN SELECT RAISE(ABORT,'reject operation'); END"
    )
    conn.commit()
    conn.close()
    with pytest.raises(Exception, match="reject operation"):
        settle(ActivityBossCoopSettlementService(database))
    conn = sqlite3.connect(database)
    assert conn.execute("SELECT hp_left FROM activity_boss_state").fetchone()[0] == 1000
    assert conn.execute("SELECT COUNT(*) FROM activity_boss_damage").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM activity_boss_fight_log").fetchone()[0] == 0
    conn.close()


def test_real_entry_passes_event_operation_to_coop_service():
    text = open(
        "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_activity/__init__.py", encoding="utf-8"
    ).read()
    handler = text[text.index("@activity_boss_atk_cmd.handle"):text.index("@activity_boss_claim_cmd.handle")]
    assert '_activity_operation_id(event, "boss-item" if raw else "boss-coop", uid)' in handler
    assert "fight_cooperative_boss(uid, operation_id=operation_id)" in handler
