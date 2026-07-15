from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import nonebot
import pytest

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_boss.transaction_service import (
    WorldBossPunishmentService,
)


BOSSES = [
    {"name": "甲", "jj": "感气境", "气血": 100},
    {"name": "乙", "jj": "练气境", "气血": 200},
    {"name": "丙", "jj": "筑基境", "气血": 300},
]


def create_service(tmp_path):
    database = tmp_path / "player.db"
    with sqlite3.connect(database) as conn:
        conn.execute(
            "CREATE TABLE world_boss_state("
            "state_key TEXT PRIMARY KEY,bosses TEXT NOT NULL,updated_at TEXT NOT NULL)"
        )
        conn.execute(
            "INSERT INTO world_boss_state VALUES('global',?,'old')",
            (json.dumps(BOSSES),),
        )
    return database, WorldBossPunishmentService(database)


def punish(
    service,
    *,
    operation_id="punish-1",
    action="single",
    expected_revision=0,
    expected_bosses=None,
    boss_number=2,
):
    return service.punish(
        operation_id=operation_id,
        action=action,
        expected_revision=expected_revision,
        expected_bosses=BOSSES if expected_bosses is None else expected_bosses,
        boss_number=boss_number,
    )


def read_session(database):
    with sqlite3.connect(database) as conn:
        row = conn.execute(
            "SELECT bosses,revision FROM world_boss_state WHERE state_key='global'"
        ).fetchone()
    return json.loads(row[0]), int(row[1])


def test_single_punishment_migrates_revision_and_removes_fixed_target(tmp_path):
    database, service = create_service(tmp_path)
    assert service.snapshot() == (BOSSES, 0)
    result = punish(service)
    assert (
        result.status,
        result.action,
        result.revision,
        list(result.bosses),
        list(result.deleted_bosses),
    ) == ("punished", "single", 1, [BOSSES[0], BOSSES[2]], [BOSSES[1]])
    assert read_session(database) == ([BOSSES[0], BOSSES[2]], 1)


def test_all_punishment_clears_session_in_one_operation(tmp_path):
    database, service = create_service(tmp_path)
    result = punish(
        service,
        operation_id="punish-all",
        action="all",
        boss_number=None,
    )
    assert (result.status, result.action, list(result.deleted_bosses)) == (
        "punished",
        "all",
        BOSSES,
    )
    assert read_session(database) == ([], 1)


def test_invalid_target_and_empty_session_are_rejected_without_operation(tmp_path):
    database, service = create_service(tmp_path)
    invalid = punish(service, boss_number=9)
    assert invalid.status == "invalid_target"
    with sqlite3.connect(database) as conn:
        conn.execute(
            "UPDATE world_boss_state SET bosses='[]' WHERE state_key='global'"
        )
    empty = punish(
        service,
        operation_id="empty",
        action="all",
        expected_bosses=[],
        boss_number=None,
    )
    assert empty.status == "empty"
    with sqlite3.connect(database) as conn:
        count = conn.execute(
            "SELECT COUNT(*) FROM world_boss_punishment_operations"
        ).fetchone()[0]
    assert count == 0


def test_session_revision_or_boss_snapshot_change_rejects_punishment(tmp_path):
    database, service = create_service(tmp_path)
    with sqlite3.connect(database) as conn:
        conn.execute(
            "UPDATE world_boss_state SET bosses=? WHERE state_key='global'",
            (json.dumps([{**BOSSES[0], "气血": 1}, *BOSSES[1:]]),),
        )
    assert punish(service).status == "session_changed"

    current, revision = service.snapshot()
    with sqlite3.connect(database) as conn:
        conn.execute(
            "UPDATE world_boss_state SET revision=revision+1 WHERE state_key='global'"
        )
    assert punish(
        service,
        operation_id="revision-changed",
        expected_revision=revision,
        expected_bosses=current,
    ).status == "session_changed"


def test_operation_replay_and_payload_conflict_do_not_delete_twice(tmp_path):
    database, service = create_service(tmp_path)
    first = punish(service)
    duplicate = punish(service)
    conflict = punish(service, boss_number=1)
    stored = service.get_result("punish-1")
    assert (first.status, duplicate.status, conflict.status, stored.status) == (
        "punished",
        "duplicate",
        "operation_conflict",
        "duplicate",
    )
    assert read_session(database) == ([BOSSES[0], BOSSES[2]], 1)


def test_operation_failure_rolls_back_bosses_and_revision(tmp_path):
    database, service = create_service(tmp_path)
    service.snapshot()
    with sqlite3.connect(database) as conn:
        conn.execute(
            "CREATE TRIGGER reject_punishment BEFORE INSERT "
            "ON world_boss_punishment_operations "
            "BEGIN SELECT RAISE(ABORT,'reject punishment'); END"
        )
    with pytest.raises(Exception, match="reject punishment"):
        punish(service)
    assert read_session(database) == (BOSSES, 0)


def test_single_and_all_handlers_share_service_without_direct_save():
    source = (
        Path(__file__).parents[1]
        / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_boss/__init__.py"
    ).read_text(encoding="utf-8")
    single = source[
        source.index("async def boss_delete_(") : source.index(
            "@boss_delete_all.handle"
        )
    ]
    delete_all = source[
        source.index("async def boss_delete_all_(") : source.index("@battle.handle")
    ]
    for section in (single, delete_all):
        assert "_punish_world_bosses(" in section
        assert "old_boss_info.save_boss(" not in section
        assert ".remove(" not in section
    assert '"single"' in single
    assert '"all"' in delete_all
