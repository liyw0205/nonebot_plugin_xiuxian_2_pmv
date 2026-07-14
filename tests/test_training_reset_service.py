from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_training.reset_service import (
    TrainingResetService,
)


STATE_COLUMNS = (
    "progress,last_time,points,completed,max_progress,last_event,weekly_purchases"
)


def create_databases(tmp_path):
    game_database = tmp_path / "game.db"
    player_database = tmp_path / "player.db"
    with sqlite3.connect(game_database) as conn:
        conn.executescript(
            "CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY);"
            "INSERT INTO user_xiuxian VALUES('u1');"
            "INSERT INTO user_xiuxian VALUES('u2');"
            "INSERT INTO user_xiuxian VALUES('u3');"
        )
    with sqlite3.connect(player_database) as conn:
        conn.execute(
            "CREATE TABLE training("
            "user_id TEXT PRIMARY KEY,progress INTEGER,last_time TEXT,points INTEGER,"
            "completed INTEGER,max_progress INTEGER,last_event TEXT,weekly_purchases TEXT)"
        )
        conn.executemany(
            "INSERT INTO training VALUES(?,?,?,?,?,?,?,?)",
            (
                (
                    "u1",
                    8,
                    "2026-07-14 07:30:00",
                    91,
                    4,
                    12,
                    "旧事件",
                    json.dumps({"1": 2, "_last_reset": "2026-07-07"}),
                ),
                (
                    "u2",
                    0,
                    None,
                    0,
                    0,
                    0,
                    "",
                    json.dumps({"_last_reset": "2026-07-14"}),
                ),
            ),
        )
    return game_database, player_database


def get_training(database, user_id):
    with sqlite3.connect(database) as conn:
        return conn.execute(
            f"SELECT {STATE_COLUMNS} FROM training WHERE user_id=?",
            (user_id,),
        ).fetchone()


def test_reset_freezes_players_and_resets_complete_state_in_chunks(tmp_path):
    game_database, player_database = create_databases(tmp_path)
    service = TrainingResetService(game_database, player_database)

    first = service.reset(
        "message-1",
        "admin",
        chunk_size=1,
        reset_date="2026-07-14",
        updated_at="2026-07-14 08:00:00",
    )
    assert (
        first.status,
        first.task_status,
        first.total,
        first.completed,
        first.changed,
        first.skipped,
    ) == ("applied", "running", 3, 1, 1, 0)
    expected = (0, None, 0, 0, 0, "", '{"_last_reset":"2026-07-14"}')
    assert get_training(player_database, "u1") == expected
    assert get_training(player_database, "u2")[6] == (
        '{"_last_reset": "2026-07-14"}'
    )

    with sqlite3.connect(game_database) as conn:
        conn.execute("INSERT INTO user_xiuxian VALUES('u4')")
    with sqlite3.connect(player_database) as conn:
        conn.execute(
            "INSERT INTO training VALUES(?,?,?,?,?,?,?,?)",
            ("u4", 6, "now", 9, 3, 7, "new", "{}"),
        )

    completed = service.reset(
        "message-1",
        "admin",
        chunk_size=10,
        reset_date="2026-07-15",
    )
    duplicate = service.reset("message-1", "admin")
    assert (
        completed.task_status,
        completed.total,
        completed.completed,
        completed.changed,
        completed.skipped,
    ) == ("completed", 3, 3, 1, 1)
    assert duplicate.status == "duplicate"
    assert get_training(player_database, "u2") == expected
    assert get_training(player_database, "u4") == (6, "now", 9, 3, 7, "new", "{}")

    with sqlite3.connect(game_database) as conn:
        targets = conn.execute(
            "SELECT user_id,status,previous_state FROM admin_training_reset_targets "
            "WHERE operation_id='message-1' ORDER BY user_id"
        ).fetchall()
    assert [(row[0], row[1]) for row in targets] == [
        ("u1", "applied"),
        ("u2", "applied"),
        ("u3", "skipped"),
    ]
    assert json.loads(targets[0][2])["points"] == 91


def test_deleted_frozen_player_is_skipped_without_touching_training(tmp_path):
    game_database, player_database = create_databases(tmp_path)
    service = TrainingResetService(game_database, player_database)
    service.reset(
        "deleted",
        "admin",
        chunk_size=1,
        reset_date="2026-07-14",
    )
    before = get_training(player_database, "u2")
    with sqlite3.connect(game_database) as conn:
        conn.execute("DELETE FROM user_xiuxian WHERE user_id='u2'")

    completed = service.reset("deleted", "admin", chunk_size=10)
    assert (
        completed.task_status,
        completed.completed,
        completed.skipped,
    ) == ("completed", 3, 2)
    assert get_training(player_database, "u2") == before


def test_failed_chunk_rolls_back_and_resumes_pending_target(tmp_path):
    game_database, player_database = create_databases(tmp_path)
    service = TrainingResetService(game_database, player_database)
    first = service.reset(
        "resume",
        "admin",
        chunk_size=1,
        reset_date="2026-07-14",
    )
    assert (first.completed, first.changed) == (1, 1)

    with sqlite3.connect(game_database) as conn:
        conn.execute(
            "CREATE TRIGGER fail_second_reset BEFORE UPDATE OF status "
            "ON admin_training_reset_targets "
            "WHEN NEW.user_id='u2' AND NEW.status='applied' "
            "BEGIN SELECT RAISE(ABORT,'failed'); END"
        )
    before = get_training(player_database, "u2")
    try:
        service.reset("resume", "admin", chunk_size=1)
    except Exception as exc:
        assert "failed" in str(exc)
    else:
        raise AssertionError("failed chunk must raise")
    assert get_training(player_database, "u2") == before
    with sqlite3.connect(game_database) as conn:
        status = conn.execute(
            "SELECT status FROM admin_training_reset_targets "
            "WHERE operation_id='resume' AND user_id='u2'"
        ).fetchone()[0]
        assert status == "pending"
        conn.execute("DROP TRIGGER fail_second_reset")

    resumed = service.reset("resume", "admin", chunk_size=10)
    assert (
        resumed.status,
        resumed.task_status,
        resumed.completed,
        resumed.changed,
        resumed.skipped,
    ) == ("applied", "completed", 3, 1, 1)


def test_same_operation_rejects_different_operator_and_empty_set_is_idempotent(
    tmp_path,
):
    game_database, player_database = create_databases(tmp_path)
    service = TrainingResetService(game_database, player_database)
    applied = service.reset("conflict", "admin-a", reset_date="2026-07-14")
    assert applied.task_status == "completed"
    assert service.reset("conflict", "admin-a").status == "duplicate"
    assert service.reset("conflict", "admin-b").status == "operation_conflict"

    with sqlite3.connect(game_database) as conn:
        conn.execute("DELETE FROM user_xiuxian")
    empty = service.reset("empty", "admin-a", reset_date="2026-07-15")
    assert (empty.status, empty.task_status, empty.total) == (
        "applied",
        "completed",
        0,
    )
    assert service.reset("empty", "admin-a").status == "duplicate"


def test_duplicate_historical_user_rows_create_one_reset_target(tmp_path):
    game_database, player_database = create_databases(tmp_path)
    with sqlite3.connect(game_database) as conn:
        conn.executescript(
            "DROP TABLE user_xiuxian;"
            "CREATE TABLE user_xiuxian(user_id TEXT);"
            "INSERT INTO user_xiuxian VALUES('u1');"
            "INSERT INTO user_xiuxian VALUES('u1');"
        )
    service = TrainingResetService(game_database, player_database)

    result = service.reset(
        "duplicate-user",
        "admin",
        reset_date="2026-07-14",
    )

    assert (
        result.status,
        result.task_status,
        result.total,
        result.completed,
        result.changed,
    ) == ("applied", "completed", 1, 1, 1)
    with sqlite3.connect(game_database) as conn:
        assert conn.execute(
            "SELECT user_id,status FROM admin_training_reset_targets "
            "WHERE operation_id='duplicate-user'"
        ).fetchall() == [("u1", "applied")]


def test_production_entry_uses_resumable_reset_service():
    root = Path(__file__).parents[1]
    source = (
        root
        / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_admin/__init__.py"
    ).read_text(encoding="utf-8")
    handler = source.split("async def training_reset_", 1)[1].split(
        "@tower_reset.handle", 1
    )[0]
    assert "training_reset_limits(operation_id, operator_id)" in handler
    assert "await asyncio.sleep(0)" in handler
    assert "training_limit.reset_limits(" not in handler
    limit_source = (
        root
        / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_training/training_limit.py"
    ).read_text(encoding="utf-8")
    assert 'update_all_records("training", "last_time"' not in limit_source
