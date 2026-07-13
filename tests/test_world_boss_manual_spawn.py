import json
import sqlite3

import nonebot
import pytest

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_boss.manual_spawn_service import (
    WorldBossManualSpawnService,
)


OLD_BOSS = {
    "name": "旧首领", "jj": "练气境", "气血": 1000, "总血量": 1000,
    "真元": 100, "攻击": 100, "max_stone": 5000, "stone": 5000,
}
OTHER_BOSS = {
    "name": "别境首领", "jj": "筑基境", "气血": 2000, "总血量": 2000,
    "真元": 200, "攻击": 200, "max_stone": 8000, "stone": 8000,
}
NEW_BOSS = {
    "name": "新首领", "jj": "练气境", "气血": 1200, "总血量": 1200,
    "真元": 120, "攻击": 120, "max_stone": 6000, "stone": 6000,
}
CONFIG = {
    "Boss名字": ["新首领"],
    "Boss灵石": {"练气境": [6000]},
    "Boss倍率": {"气血": 300, "真元": 10, "攻击": 0.5},
}


def create_service(tmp_path):
    database = tmp_path / "player.db"
    conn = sqlite3.connect(database)
    conn.execute(
        "CREATE TABLE world_boss_state("
        "state_key TEXT PRIMARY KEY,bosses TEXT NOT NULL,updated_at TEXT NOT NULL)"
    )
    conn.execute(
        "INSERT INTO world_boss_state VALUES ('global',?,'old')",
        (json.dumps([OLD_BOSS, OTHER_BOSS]),),
    )
    conn.commit()
    conn.close()
    return database, WorldBossManualSpawnService(database, lambda: CONFIG)


def spawn(service, operation_id="spawn-1", expected=None, config=None, boss=None):
    expected = [OLD_BOSS, OTHER_BOSS] if expected is None else expected
    config = service.config_snapshot(CONFIG, "练气境") if config is None else config
    return service.spawn(
        operation_id=operation_id,
        expected_bosses=expected,
        expected_config=config,
        boss=NEW_BOSS if boss is None else boss,
    )


def test_spawn_rechecks_session_replaces_conflict_and_records_operation(tmp_path):
    database, service = create_service(tmp_path)
    result = spawn(service)
    assert result.status == "spawned"
    assert list(result.bosses) == [OTHER_BOSS, NEW_BOSS]
    conn = sqlite3.connect(database)
    assert json.loads(conn.execute("SELECT bosses FROM world_boss_state").fetchone()[0]) == [OTHER_BOSS, NEW_BOSS]
    assert conn.execute("SELECT operation_id FROM world_boss_manual_spawn_operations").fetchone()[0] == "spawn-1"
    conn.close()


def test_operation_replay_is_idempotent_and_conflict_is_rejected(tmp_path):
    database, service = create_service(tmp_path)
    assert spawn(service).status == "spawned"
    assert spawn(service).status == "duplicate"
    assert spawn(service, boss={**NEW_BOSS, "气血": 1300}).status == "operation_conflict"
    conn = sqlite3.connect(database)
    assert conn.execute("SELECT COUNT(*) FROM world_boss_manual_spawn_operations").fetchone()[0] == 1
    assert json.loads(conn.execute("SELECT bosses FROM world_boss_state").fetchone()[0]) == [OTHER_BOSS, NEW_BOSS]
    conn.close()


def test_session_or_config_change_does_not_create_boss_or_operation(tmp_path):
    database, service = create_service(tmp_path)
    assert spawn(service, expected=[]).status == "session_changed"
    changed = service.config_snapshot(CONFIG, "练气境")
    changed["stones"] = [9999]
    assert spawn(service, operation_id="spawn-2", config=changed).status == "config_changed"
    conn = sqlite3.connect(database)
    assert json.loads(conn.execute("SELECT bosses FROM world_boss_state").fetchone()[0]) == [OLD_BOSS, OTHER_BOSS]
    assert conn.execute("SELECT COUNT(*) FROM world_boss_manual_spawn_operations").fetchone()[0] == 0
    conn.close()


def test_operation_failure_rolls_back_boss_state(tmp_path):
    database, service = create_service(tmp_path)
    conn = sqlite3.connect(database)
    service._ensure_schema(conn)
    conn.execute(
        "CREATE TRIGGER reject_manual_spawn BEFORE INSERT ON world_boss_manual_spawn_operations "
        "BEGIN SELECT RAISE(ABORT,'reject manual spawn'); END"
    )
    conn.commit()
    conn.close()
    with pytest.raises(Exception, match="reject manual spawn"):
        spawn(service)
    conn = sqlite3.connect(database)
    assert json.loads(conn.execute("SELECT bosses FROM world_boss_state").fetchone()[0]) == [OLD_BOSS, OTHER_BOSS]
    assert conn.execute("SELECT COUNT(*) FROM world_boss_manual_spawn_operations").fetchone()[0] == 0
    conn.close()


def test_real_entry_uses_transaction_service_without_segmented_side_paths():
    path = "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_boss/__init__.py"
    with open(path, encoding="utf-8") as source_file:
        text = source_file.read()
    handler = text[text.index("async def create_("):text.index("@create_appoint.handle")]
    assert "world_boss_manual_spawn_service.spawn(" in handler
    assert "expected_bosses=" in handler
    assert "expected_config=" in handler
    assert "old_boss_info.save_boss(group_boss)" not in handler
    assert "group_boss[GLOBAL_BOSS_KEY].remove(" not in handler
    assert "group_boss[GLOBAL_BOSS_KEY].append(" not in handler
