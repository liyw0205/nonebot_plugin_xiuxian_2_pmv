from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import nonebot
import pytest

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_tasks.transaction_service import (
    TaskProgressEventService,
)


PERIODS = {"daily": "2026-07-14", "weekly": "2026-W29"}
TASKS = (
    {
        "key": "daily_sign",
        "cycle": "daily",
        "name": "今日问道",
        "target": 1,
        "amount": 1,
    },
    {
        "key": "weekly_sign",
        "cycle": "weekly",
        "name": "七日勤修",
        "target": 2,
        "amount": 1,
    },
)


def read_state(database: Path):
    with sqlite3.connect(database) as conn:
        row = conn.execute(
            "SELECT daily_period,daily_progress,daily_claimed,"
            "weekly_period,weekly_progress,weekly_claimed "
            "FROM xiuxian_tasks WHERE user_id='u'"
        ).fetchone()
    return (
        row[0],
        json.loads(row[1]),
        json.loads(row[2]),
        row[3],
        json.loads(row[4]),
        json.loads(row[5]),
    )


def test_event_updates_all_mappings_and_cycles_in_one_operation(tmp_path: Path) -> None:
    database = tmp_path / "player.db"
    service = TaskProgressEventService(database)
    tasks = (
        {
            "key": "daily_close",
            "cycle": "daily",
            "name": "闭关归元",
            "target": 3,
            "amount": 3,
        },
        {
            "key": "weekly_training",
            "cycle": "weekly",
            "name": "道心不辍",
            "target": 5,
            "amount": 5,
        },
        {
            "key": "weekly_other",
            "cycle": "weekly",
            "name": "并行周常",
            "target": 2,
            "amount": 2,
        },
    )

    result = service.record(
        "event-1",
        "u",
        (("out_closing", 3), ("cultivation_time", 2)),
        PERIODS,
        tasks,
    )

    assert result.status == "applied"
    assert result.completed == ("闭关归元", "道心不辍", "并行周常")
    state = read_state(database)
    assert state == (
        "2026-07-14",
        {"daily_close": 3},
        [],
        "2026-W29",
        {"weekly_other": 2, "weekly_training": 5},
        [],
    )


def test_replay_returns_first_completion_and_conflict_does_not_mutate(tmp_path: Path) -> None:
    database = tmp_path / "player.db"
    service = TaskProgressEventService(database)

    first = service.record("same", "u", (("sign_in", 1),), PERIODS, TASKS)
    duplicate = service.record(
        "same",
        "u",
        (("sign_in", 1),),
        {"daily": "2099-01-01", "weekly": "2099-W01"},
        (
            {**TASKS[0], "name": "配置已变化", "target": 99},
            {**TASKS[1], "target": 99},
        ),
    )
    conflict = service.record(
        "same", "u", (("sign_in", 2),), PERIODS, TASKS
    )

    assert (first.status, duplicate.status, conflict.status) == (
        "applied",
        "duplicate",
        "operation_conflict",
    )
    assert first.completed == duplicate.completed == ("今日问道",)
    assert read_state(database)[1:5:3] == (
        {"daily_sign": 1},
        {"weekly_sign": 1},
    )
    with sqlite3.connect(database) as conn:
        assert conn.execute(
            "SELECT COUNT(*) FROM task_progress_event_operations"
        ).fetchone()[0] == 1


def test_new_event_can_finish_task_but_replay_cannot_increment_twice(tmp_path: Path) -> None:
    database = tmp_path / "player.db"
    service = TaskProgressEventService(database)

    service.record("first", "u", (("sign_in", 1),), PERIODS, TASKS)
    second = service.record("second", "u", (("sign_in", 1),), PERIODS, TASKS)
    replay = service.record("second", "u", (("sign_in", 1),), PERIODS, TASKS)

    assert second.completed == replay.completed == ("七日勤修",)
    assert read_state(database)[4] == {"weekly_sign": 2}


def test_period_rollover_resets_progress_and_claimed_together(tmp_path: Path) -> None:
    database = tmp_path / "player.db"
    service = TaskProgressEventService(database)
    service.get_states("u", PERIODS)
    with sqlite3.connect(database) as conn:
        conn.execute(
            "UPDATE xiuxian_tasks SET daily_progress=?,daily_claimed=?,"
            "weekly_progress=?,weekly_claimed=? WHERE user_id='u'",
            (
                json.dumps({"old_daily": 8}),
                json.dumps(["old_daily"]),
                json.dumps({"old_weekly": 9}),
                json.dumps(["old_weekly"]),
            ),
        )
        conn.commit()

    states = service.get_states(
        "u", {"daily": "2026-07-15", "weekly": "2026-W30"}
    )

    assert states == {
        "daily": ({}, [], "2026-07-15"),
        "weekly": ({}, [], "2026-W30"),
    }
    assert read_state(database) == (
        "2026-07-15",
        {},
        [],
        "2026-W30",
        {},
        [],
    )


def test_operation_insert_failure_rolls_back_both_cycle_updates(tmp_path: Path) -> None:
    database = tmp_path / "player.db"
    service = TaskProgressEventService(database)
    service.get_states("u", PERIODS)
    with sqlite3.connect(database) as conn:
        conn.execute(
            "UPDATE xiuxian_tasks SET daily_progress=?,weekly_progress=? "
            "WHERE user_id='u'",
            (json.dumps({"before_daily": 4}), json.dumps({"before_weekly": 5})),
        )
        conn.execute(
            "CREATE TRIGGER fail_task_progress_operation BEFORE INSERT ON "
            "task_progress_event_operations BEGIN SELECT RAISE(ABORT,'failed'); END"
        )
        conn.commit()

    with pytest.raises(sqlite3.IntegrityError, match="failed"):
        service.record(
            "failed",
            "u",
            (("sign_in", 1),),
            {"daily": "2026-07-15", "weekly": "2026-W30"},
            TASKS,
        )

    assert read_state(database) == (
        "2026-07-14",
        {"before_daily": 4},
        [],
        "2026-W29",
        {"before_weekly": 5},
        [],
    )


def test_event_without_matching_task_still_gets_an_idempotency_record(tmp_path: Path) -> None:
    database = tmp_path / "player.db"
    service = TaskProgressEventService(database)

    first = service.record("unknown", "u", (("unknown", 1),), {}, ())
    duplicate = service.record("unknown", "u", (("unknown", 1),), {}, ())

    assert (first.status, duplicate.status) == ("applied", "duplicate")
    assert first.completed == duplicate.completed == ()


def test_production_entries_use_batched_idempotent_task_events() -> None:
    root = Path(__file__).parents[1] / "nonebot_plugin_xiuxian_2/xiuxian"
    task_source = (root / "xiuxian_tasks/task_data.py").read_text(encoding="utf-8")
    event_source = (root / "xiuxian_utils/game_events.py").read_text(encoding="utf-8")
    base_source = (root / "xiuxian_base/__init__.py").read_text(encoding="utf-8")
    buff_source = (root / "xiuxian_buff/__init__.py").read_text(encoding="utf-8")
    impart_source = (root / "xiuxian_impart_pk/__init__.py").read_text(encoding="utf-8")
    work_source = (root / "xiuxian_work/__init__.py").read_text(encoding="utf-8")
    pet_source = (root / "xiuxian_pet/__init__.py").read_text(encoding="utf-8")

    assert "TaskProgressEventService(get_paths().player_db)" in task_source
    assert "update_or_write_data" not in task_source
    assert "record_task_progress_event(user_id, updates, operation_id)" in event_source
    assert "completed.extend(record_task_progress(" not in event_source
    for source in (base_source, buff_source, impart_source, work_source):
        assert "operation_id=f\"task-progress:" in source
    assert "def _grant_pet_travel_rewards" not in pet_source
    travel_handler = pet_source[pet_source.index("@pet_travel_claim.handle"):]
    assert '"trace_id": operation_id' in travel_handler
    assert "safe_record_game_event(" in travel_handler
