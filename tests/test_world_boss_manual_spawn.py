import json
import sqlite3

import nonebot
import pytest

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_boss.transaction_service import (
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


def spawn(
    service,
    operation_id="spawn-1",
    expected_revision=0,
    expected=None,
    config=None,
    boss=None,
):
    expected = [OLD_BOSS, OTHER_BOSS] if expected is None else expected
    config = service.config_snapshot(CONFIG, "练气境") if config is None else config
    return service.spawn(
        operation_id=operation_id,
        expected_revision=expected_revision,
        expected_bosses=expected,
        expected_config=config,
        boss=NEW_BOSS if boss is None else boss,
    )


def test_spawn_rechecks_session_replaces_conflict_and_records_operation(tmp_path):
    database, service = create_service(tmp_path)
    result = spawn(service)
    assert result.status == "spawned"
    assert result.revision == 1
    assert list(result.bosses) == [OTHER_BOSS, NEW_BOSS]
    conn = sqlite3.connect(database)
    row = conn.execute("SELECT bosses,revision FROM world_boss_state").fetchone()
    assert json.loads(row[0]) == [OTHER_BOSS, NEW_BOSS]
    assert row[1] == 1
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


def test_revision_change_rejects_stale_spawn(tmp_path):
    database, service = create_service(tmp_path)
    assert service.snapshot() == ([OLD_BOSS, OTHER_BOSS], 0)
    with sqlite3.connect(database) as conn:
        conn.execute(
            "UPDATE world_boss_state SET revision=1 WHERE state_key='global'"
        )
    assert spawn(service, expected_revision=0).status == "session_changed"


def test_operation_failure_rolls_back_boss_state(tmp_path):
    database, service = create_service(tmp_path)
    service.snapshot()
    conn = sqlite3.connect(database)
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


def test_random_and_appointed_entries_share_transaction_without_side_paths():
    path = "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_boss/__init__.py"
    with open(path, encoding="utf-8") as source_file:
        text = source_file.read()
    helper = text[
        text.index("def _spawn_world_boss(") : text.index(
            "async def generate_all_bosses_task"
        )
    ]
    assert helper.index("world_boss_manual_spawn_service.get_result(") < helper.index(
        "createboss_jj("
    )
    assert "world_boss_manual_spawn_service.snapshot()" in helper
    assert "expected_revision=" in helper
    assert "world_boss_manual_spawn_service.spawn(" in helper

    random_handler = text[
        text.index("async def create_(") : text.index("@create_appoint.handle")
    ]
    appointed_handler = text[
        text.index("@create_appoint.handle") : text.index(
            "@boss_integral_store.handle"
        )
    ]
    for handler in (random_handler, appointed_handler):
        assert "_spawn_world_boss(" in handler
        assert "old_boss_info.save_boss(group_boss)" not in handler
        assert "group_boss[GLOBAL_BOSS_KEY].remove(" not in handler
        assert "group_boss[GLOBAL_BOSS_KEY].append(" not in handler
    assert "world-boss-appointed-spawn:{event_id}" in appointed_handler
