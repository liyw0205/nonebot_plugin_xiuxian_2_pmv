from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import nonebot
import pytest

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_boss.full_refresh_service import (
    WorldBossFullRefreshService,
)


OLD_BOSS = {
    "name": "旧首领",
    "jj": "练气境",
    "气血": 1000,
    "总血量": 1000,
    "真元": 100,
    "攻击": 100,
    "max_stone": 5000,
    "stone": 5000,
}
NEW_BOSSES = [
    {
        "name": "新首领",
        "jj": "感气境",
        "气血": 1100,
        "总血量": 1100,
        "真元": 110,
        "攻击": 110,
        "max_stone": 4000,
        "stone": 4000,
    },
    {
        "name": "新首领",
        "jj": "练气境",
        "气血": 2200,
        "总血量": 2200,
        "真元": 220,
        "攻击": 220,
        "max_stone": 6000,
        "stone": 6000,
    },
]
CONFIG = {
    "Boss名字": ["新首领"],
    "Boss灵石": {"感气境": [4000], "练气境": [6000]},
    "Boss倍率": {"气血": 300, "真元": 10, "攻击": 0.5},
}


def create_service(tmp_path):
    database = tmp_path / "player.db"
    with sqlite3.connect(database) as conn:
        conn.execute(
            "CREATE TABLE world_boss_state("
            "state_key TEXT PRIMARY KEY,bosses TEXT NOT NULL,updated_at TEXT NOT NULL)"
        )
        conn.execute(
            "INSERT INTO world_boss_state VALUES('global',?,'old')",
            (json.dumps([OLD_BOSS]),),
        )
    return database, WorldBossFullRefreshService(database, lambda: CONFIG)


def refresh(
    service,
    operation_id="refresh-1",
    trigger="manual",
    expected_revision=0,
    expected_bosses=None,
    expected_config=None,
    bosses=None,
):
    expected_bosses = [OLD_BOSS] if expected_bosses is None else expected_bosses
    bosses = NEW_BOSSES if bosses is None else bosses
    expected_config = expected_config or service.config_snapshot(
        CONFIG,
        [boss["jj"] for boss in bosses],
    )
    return service.refresh(
        operation_id=operation_id,
        trigger=trigger,
        expected_revision=expected_revision,
        expected_bosses=expected_bosses,
        expected_config=expected_config,
        bosses=bosses,
    )


def read_session(database):
    with sqlite3.connect(database) as conn:
        row = conn.execute(
            "SELECT bosses,revision FROM world_boss_state WHERE state_key='global'"
        ).fetchone()
    return json.loads(row[0]), int(row[1])


def test_full_refresh_migrates_revision_and_commits_fixed_plan(tmp_path):
    database, service = create_service(tmp_path)
    assert service.snapshot() == ([OLD_BOSS], 0)
    result = refresh(service)
    assert (
        result.status,
        result.revision,
        list(result.bosses),
        result.trigger,
    ) == ("refreshed", 1, NEW_BOSSES, "manual")
    assert read_session(database) == (NEW_BOSSES, 1)
    with sqlite3.connect(database) as conn:
        operation = conn.execute(
            "SELECT operation_id FROM world_boss_full_refresh_operations"
        ).fetchone()[0]
    assert operation == "refresh-1"


def test_operation_replay_and_conflict_do_not_regenerate_or_advance_revision(tmp_path):
    database, service = create_service(tmp_path)
    first = refresh(service)
    duplicate = refresh(service)
    changed = [{**NEW_BOSSES[0], "气血": 999}, NEW_BOSSES[1]]
    conflict = refresh(service, bosses=changed)
    assert (first.status, duplicate.status, conflict.status) == (
        "refreshed",
        "duplicate",
        "operation_conflict",
    )
    assert service.get_result("refresh-1").status == "duplicate"
    assert read_session(database) == (NEW_BOSSES, 1)


def test_session_revision_or_boss_snapshot_change_rejects_refresh(tmp_path):
    database, service = create_service(tmp_path)
    assert refresh(service).status == "refreshed"
    stale = refresh(
        service,
        operation_id="stale-revision",
        expected_revision=0,
        expected_bosses=[OLD_BOSS],
    )
    assert stale.status == "session_changed"

    current_bosses, revision = service.snapshot()
    with sqlite3.connect(database) as conn:
        conn.execute(
            "UPDATE world_boss_state SET bosses=? WHERE state_key='global'",
            (json.dumps([{**current_bosses[0], "气血": 1}, current_bosses[1]]),),
        )
    changed = refresh(
        service,
        operation_id="changed-boss",
        expected_revision=revision,
        expected_bosses=current_bosses,
    )
    assert changed.status == "session_changed"


def test_config_change_or_invalid_generated_boss_is_rejected(tmp_path):
    database, service = create_service(tmp_path)
    stale_config = service.config_snapshot(CONFIG, ["感气境", "练气境"])
    stale_config["stones"]["练气境"] = [9999]
    assert refresh(service, expected_config=stale_config).status == "config_changed"
    invalid = [{**NEW_BOSSES[0], "name": "配置外首领"}, NEW_BOSSES[1]]
    assert refresh(
        service,
        operation_id="invalid-boss",
        bosses=invalid,
    ).status == "config_changed"
    assert read_session(database) == ([OLD_BOSS], 0)


def test_operation_failure_rolls_back_bosses_and_revision(tmp_path):
    database, service = create_service(tmp_path)
    service.snapshot()
    with sqlite3.connect(database) as conn:
        conn.execute(
            "CREATE TRIGGER reject_full_refresh BEFORE INSERT "
            "ON world_boss_full_refresh_operations "
            "BEGIN SELECT RAISE(ABORT,'reject full refresh'); END"
        )
    with pytest.raises(Exception, match="reject full refresh"):
        refresh(service)
    assert read_session(database) == ([OLD_BOSS], 0)
    with sqlite3.connect(database) as conn:
        count = conn.execute(
            "SELECT COUNT(*) FROM world_boss_full_refresh_operations"
        ).fetchone()[0]
    assert count == 0


def test_scheduled_and_manual_entries_share_full_refresh_operation():
    source = (
        Path(__file__).parents[1]
        / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_boss/__init__.py"
    ).read_text(encoding="utf-8")
    helper = source[
        source.index("def _refresh_all_world_bosses") : source.index(
            "async def generate_all_bosses_task"
        )
    ]
    assert helper.index("world_boss_full_refresh_service.get_result(") < helper.index(
        "create_all_bosses()"
    )
    assert "world_boss_full_refresh_service.snapshot()" in helper
    assert "world_boss_full_refresh_service.refresh(" in helper
    assert "_sync_world_boss_cache(" in helper

    scheduled = source[
        source.index("async def generate_all_bosses_task") : source.index(
            "@DRIVER.on_shutdown"
        )
    ]
    manual = source[
        source.index("async def generate_all_bosses(") : source.index(
            "@create.handle"
        )
    ]
    for section in (scheduled, manual):
        assert "_refresh_all_world_bosses(" in section
        assert "create_all_bosses()" not in section
        assert "old_boss_info.save_boss(" not in section
    assert 'result.status == "duplicate"' in scheduled
    assert "world-boss-full-refresh:manual:{event_id}" in manual
