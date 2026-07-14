from __future__ import annotations

import sqlite3
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_boss.daily_limit_reset_service import (
    WorldBossDailyLimitResetService,
)


def create_service(tmp_path):
    database = tmp_path / "player.db"
    with sqlite3.connect(database) as conn:
        conn.execute(
            "CREATE TABLE boss(user_id TEXT PRIMARY KEY,boss_integral INTEGER,"
            "boss_stone INTEGER,boss_battle_count INTEGER)"
        )
        conn.executemany(
            "INSERT INTO boss VALUES(?,?,?,?)",
            (
                ("u1", 12, 300, 2),
                ("u2", 0, 0, 0),
                ("u3", 8, 100, 1),
            ),
        )
    return database, WorldBossDailyLimitResetService(database)


def reset(service, business_date="2026-07-14", chunk_size=500):
    return service.reset(
        business_date,
        chunk_size=chunk_size,
        updated_at=f"{business_date} 00:01:40",
    )


def read_limits(database):
    with sqlite3.connect(database) as conn:
        return {
            str(row[0]): tuple(int(value or 0) for value in row[1:])
            for row in conn.execute(
                "SELECT user_id,boss_integral,boss_stone,boss_battle_count "
                "FROM boss ORDER BY user_id"
            ).fetchall()
        }


def read_targets(database, business_date="2026-07-14"):
    with sqlite3.connect(database) as conn:
        return conn.execute(
            "SELECT user_id,status,previous_integral,previous_stone,"
            "previous_battle_count FROM world_boss_daily_limit_reset_targets "
            "WHERE business_date=? ORDER BY user_id",
            (business_date,),
        ).fetchall()


def test_reset_freezes_rows_and_resumes_in_chunks(tmp_path):
    database, service = create_service(tmp_path)
    first = reset(service, chunk_size=1)
    assert (
        first.status,
        first.task_status,
        first.total,
        first.completed,
        first.changed,
        first.skipped,
    ) == ("applied", "running", 3, 1, 1, 0)
    assert read_limits(database)["u1"] == (0, 0, 0)

    with sqlite3.connect(database) as conn:
        conn.execute("DELETE FROM boss WHERE user_id='u2'")
        conn.execute("INSERT INTO boss VALUES('u4',9,90,3)")
    completed = reset(service, chunk_size=10)
    duplicate = reset(service)
    assert (
        completed.task_status,
        completed.total,
        completed.completed,
        completed.changed,
        completed.skipped,
        duplicate.status,
    ) == ("completed", 3, 3, 2, 1, "duplicate")
    assert read_limits(database) == {
        "u1": (0, 0, 0),
        "u3": (0, 0, 0),
        "u4": (9, 90, 3),
    }
    assert read_targets(database) == [
        ("u1", "applied", 12, 300, 2),
        ("u2", "skipped", None, None, None),
        ("u3", "applied", 8, 100, 1),
    ]


def test_business_date_is_idempotent_and_next_date_runs_again(tmp_path):
    database, service = create_service(tmp_path)
    assert reset(service).task_status == "completed"
    with sqlite3.connect(database) as conn:
        conn.execute(
            "UPDATE boss SET boss_integral=5,boss_stone=50,boss_battle_count=1 "
            "WHERE user_id='u1'"
        )
    assert reset(service).status == "duplicate"
    assert read_limits(database)["u1"] == (5, 50, 1)
    next_day = reset(service, business_date="2026-07-15")
    assert (next_day.status, next_day.changed) == ("applied", 1)
    assert read_limits(database)["u1"] == (0, 0, 0)


def test_failed_chunk_rolls_back_all_three_fields_and_resumes(tmp_path):
    database, service = create_service(tmp_path)
    first = reset(service, chunk_size=2)
    assert first.completed == 2
    with sqlite3.connect(database) as conn:
        conn.execute(
            "CREATE TRIGGER reject_second_reset BEFORE UPDATE OF status "
            "ON world_boss_daily_limit_reset_targets "
            "WHEN NEW.user_id='u3' AND NEW.status='applied' "
            "BEGIN SELECT RAISE(ABORT,'reject reset'); END"
        )
    before = read_limits(database)["u3"]
    try:
        reset(service, chunk_size=1)
    except Exception as exc:
        assert "reject reset" in str(exc)
    else:
        raise AssertionError("failed chunk must raise")
    assert read_limits(database)["u3"] == before
    assert read_targets(database)[2][1] == "pending"

    with sqlite3.connect(database) as conn:
        conn.execute("DROP TRIGGER reject_second_reset")
    resumed = reset(service, chunk_size=10)
    assert (
        resumed.status,
        resumed.task_status,
        resumed.completed,
        resumed.changed,
    ) == ("applied", "completed", 3, 2)


def test_empty_table_completes_and_missing_columns_are_migrated(tmp_path):
    database = tmp_path / "empty.db"
    with sqlite3.connect(database) as conn:
        conn.execute("CREATE TABLE boss(user_id TEXT PRIMARY KEY)")
    service = WorldBossDailyLimitResetService(database)
    first = reset(service)
    duplicate = reset(service)
    assert (
        first.status,
        first.task_status,
        first.total,
        duplicate.status,
    ) == ("applied", "completed", 0, "duplicate")
    with sqlite3.connect(database) as conn:
        columns = {row[1] for row in conn.execute("PRAGMA table_info(boss)")}
    assert {"boss_integral", "boss_stone", "boss_battle_count"}.issubset(columns)


def test_scheduler_and_admin_entries_share_resumable_daily_service():
    root = Path(__file__).parents[1]
    boss_source = (
        root / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_boss/__init__.py"
    ).read_text(encoding="utf-8")
    reset_entry = boss_source[
        boss_source.index("async def set_boss_limits_reset(") : boss_source.index(
            "@boss_help.handle"
        )
    ]
    assert "world_boss_daily_limit_reset_service.reset(" in reset_entry
    assert "await asyncio.sleep(0)" in reset_entry
    assert "boss_limit.reset_limits(" not in reset_entry

    scheduler_source = (
        root / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_scheduler/__init__.py"
    ).read_text(encoding="utf-8")
    admin_source = (
        root / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_admin/__init__.py"
    ).read_text(encoding="utf-8")
    assert 'set_boss_limits_reset)' in scheduler_source
    admin_handler = admin_source[
        admin_source.index("async def boss_reset_(") : admin_source.index(
            "@items_refresh.handle"
        )
    ]
    assert "result = await set_boss_limits_reset()" in admin_handler

    limit_source = (
        root / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_boss/boss_limit.py"
    ).read_text(encoding="utf-8")
    assert "def reset_limits(" not in limit_source
