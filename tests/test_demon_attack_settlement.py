import json
import sqlite3

import nonebot
import pytest

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_world_events.demon_attack_settlement_service import DemonAttackSettlementService


def create_db(path):
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE world_event_state (user_id TEXT PRIMARY KEY,status TEXT,event_id TEXT,bosses TEXT,participants TEXT,claimed TEXT)")
    boss = {"wave": 1, "boss_hp": 1000, "boss_max_hp": 1000, "battle_hp": 100, "battle_max_hp": 100, "reward_multiplier": 1.0}
    conn.execute("INSERT INTO world_event_state VALUES (?,?,?,?,?,?)", ("global", "active", "event-1", json.dumps({"练气境": boss}), "{}", "{}"))
    conn.commit()
    conn.close()
    return boss


def settle(service, boss, operation_id="op-1", participants=None):
    return service.settle(
        operation_id, "global", "10001", "测试道友", "练气境", 1,
        {"status": "active", "event_id": "event-1"}, boss, participants or {},
        attack_limit=3, real_hp_multiplier=100, max_damage_ratio=0.2, max_pursuit_ratio=0.1,
    )


def test_settlement_atomically_updates_boss_participant_stats_and_operation(tmp_path):
    db = tmp_path / "player.db"
    boss = create_db(db)
    result = settle(DemonAttackSettlementService(db), boss)
    assert (result.status, result.real_damage) == ("applied", 100)
    conn = sqlite3.connect(db)
    bosses, participants = map(json.loads, conn.execute("SELECT bosses,participants FROM world_event_state WHERE user_id='global'").fetchone())
    assert bosses["练气境"]["boss_hp"] == 900
    assert participants["练气境:1:10001"]["damage"] == 100
    assert participants["练气境:1:10001"]["attacks"] == 1
    assert conn.execute('SELECT "魔修入侵参与","魔修入侵伤害" FROM statistics WHERE user_id=?', ("10001",)).fetchone() == (1, 100)
    assert conn.execute("SELECT COUNT(*) FROM demon_attack_settlement_operations").fetchone()[0] == 1
    conn.close()


def test_operation_replay_is_idempotent(tmp_path):
    db = tmp_path / "player.db"
    boss = create_db(db)
    service = DemonAttackSettlementService(db)
    assert settle(service, boss) == settle(service, boss)
    conn = sqlite3.connect(db)
    participants = json.loads(conn.execute("SELECT participants FROM world_event_state").fetchone()[0])
    assert participants["练气境:1:10001"]["attacks"] == 1
    conn.close()


def test_expected_snapshot_change_rejects_settlement(tmp_path):
    db = tmp_path / "player.db"
    boss = create_db(db)
    changed = dict(boss)
    changed["boss_hp"] = 999
    assert settle(DemonAttackSettlementService(db), changed).status == "state_changed"
    conn = sqlite3.connect(db)
    assert conn.execute("SELECT participants FROM world_event_state").fetchone()[0] == "{}"
    assert conn.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='demon_attack_settlement_operations'").fetchone()[0] == 0
    conn.close()


def test_failure_rolls_back_state_stats_and_operation(tmp_path):
    db = tmp_path / "player.db"
    boss = create_db(db)
    conn = sqlite3.connect(db)
    conn.execute("CREATE TABLE demon_attack_settlement_operations (operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_json TEXT NOT NULL,created_at TEXT NOT NULL)")
    conn.execute("CREATE TRIGGER reject_demon_operation BEFORE INSERT ON demon_attack_settlement_operations BEGIN SELECT RAISE(ABORT, 'reject operation'); END")
    conn.commit()
    conn.close()
    with pytest.raises(Exception, match="reject operation"):
        settle(DemonAttackSettlementService(db), boss)
    conn = sqlite3.connect(db)
    bosses, participants = conn.execute("SELECT bosses,participants FROM world_event_state").fetchone()
    assert json.loads(bosses)["练气境"]["boss_hp"] == 1000
    assert participants == "{}"
    assert conn.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='statistics'").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM demon_attack_settlement_operations").fetchone()[0] == 0
    conn.close()


def test_real_entry_uses_transaction_service_without_old_side_paths():
    source = "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_world_events/__init__.py"
    with open(source, encoding="utf-8") as source_file:
        text = source_file.read()
    handler = text[text.index("async def attack_demon_invasion_"):text.index("async def claim_demon_reward_")]
    assert "demon_attack_settlement_service.settle(" in handler
    post_battle = handler[handler.index("result, victor, bossinfo_new, status_list"):]
    assert "_save_state(state)" not in post_battle
    assert 'update_statistics_value(user_id, "魔修入侵参与")' not in handler
