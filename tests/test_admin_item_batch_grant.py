import sqlite3

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_admin.admin_item_batch_grant_service import AdminItemBatchGrantService


def create_database(tmp_path):
    database = tmp_path / "game.db"
    conn = sqlite3.connect(database)
    conn.executescript(
        "CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY);"
        "CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,goods_num INTEGER,"
        "create_time TEXT,update_time TEXT,bind_num INTEGER DEFAULT 0,PRIMARY KEY(user_id,goods_id));"
        "INSERT INTO user_xiuxian VALUES('1'); INSERT INTO user_xiuxian VALUES('2'); INSERT INTO user_xiuxian VALUES('3');"
    )
    conn.commit(); conn.close()
    return database


def test_batch_grant_resumes_without_duplicate_delivery(tmp_path):
    database = create_database(tmp_path)
    service = AdminItemBatchGrantService(database)
    first = service.grant("op", "admin", ["1", "2", "3"], 10, "丹药", "道具", 2, 99, chunk_size=1)
    assert (first.status, first.completed, first.added, first.granted_users) == ("applied", 1, 2, 1)
    second = service.grant("op", "admin", ["1", "2", "3"], 10, "丹药", "道具", 2, 99, chunk_size=10)
    assert (second.status, second.completed, second.added, second.granted_users) == ("applied", 3, 6, 3)
    duplicate = service.grant("op", "admin", ["1", "2", "3"], 10, "丹药", "道具", 2, 99)
    assert (duplicate.status, duplicate.completed, duplicate.added, duplicate.granted_users) == (
        "duplicate", 3, 6, 3
    )
    conn = sqlite3.connect(database)
    assert conn.execute("SELECT SUM(goods_num) FROM back").fetchone()[0] == 6
    logs = conn.execute(
        "SELECT user_id,item_delta,trace_id FROM economy_log ORDER BY user_id"
    ).fetchall()
    assert len(logs) == 3
    assert {row[0] for row in logs} == {"1", "2", "3"}
    assert all('"amount":2' in row[1] and row[2] == "op" for row in logs)
    conn.close()


def test_batch_conflict_and_failed_chunk_roll_back(tmp_path):
    database = create_database(tmp_path)
    service = AdminItemBatchGrantService(database)
    service.grant("op", "admin", ["1", "2"], 10, "丹药", "道具", 1, 99, chunk_size=1)
    assert service.grant("op", "admin", ["1", "2"], 10, "丹药", "道具", 2, 99).status == "operation_conflict"
    conn = sqlite3.connect(database)
    conn.execute("CREATE TRIGGER reject_progress BEFORE INSERT ON admin_item_batch_grant_progress WHEN NEW.user_id='2' BEGIN SELECT RAISE(ABORT,'reject progress'); END")
    conn.commit(); conn.close()
    try:
        service.grant("op", "admin", ["1", "2"], 10, "丹药", "道具", 1, 99, chunk_size=2)
    except Exception as exc:
        assert "reject progress" in str(exc)
    conn = sqlite3.connect(database)
    assert conn.execute("SELECT goods_num FROM back WHERE user_id='2'").fetchone() is None
    assert conn.execute("SELECT completed FROM admin_item_batch_grant_operations").fetchone()[0] == 1
    assert conn.execute("SELECT COUNT(*) FROM economy_log WHERE user_id='2'").fetchone()[0] == 0
    conn.close()


def test_capacity_is_all_or_nothing_per_user_and_is_idempotent(tmp_path):
    database = create_database(tmp_path)
    conn = sqlite3.connect(database)
    conn.execute(
        "INSERT INTO back VALUES('1',10,'丹药','道具',98,'now','now',7)"
    )
    conn.execute(
        "INSERT INTO back VALUES('2',10,'丹药','道具',97,'now','now',0)"
    )
    conn.commit()
    conn.close()

    service = AdminItemBatchGrantService(database)
    result = service.grant("capacity", "admin", ["1", "2"], 10, "丹药", "道具", 2, 99)
    assert (result.completed, result.added, result.granted_users) == (2, 2, 1)
    assert service.grant("capacity", "admin", ["2", "1"], 10, "丹药", "道具", 2, 99).status == "duplicate"

    conn = sqlite3.connect(database)
    assert conn.execute("SELECT goods_num,bind_num FROM back WHERE user_id='1'").fetchone() == (98, 7)
    assert conn.execute("SELECT goods_num FROM back WHERE user_id='2'").fetchone()[0] == 99
    assert conn.execute("SELECT COUNT(*) FROM economy_log").fetchone()[0] == 1
    assert conn.execute(
        "SELECT added FROM admin_item_batch_grant_progress WHERE operation_id='capacity' AND user_id='1'"
    ).fetchone()[0] == 0
    conn.close()


def test_economy_log_failure_rolls_back_inventory_and_progress(tmp_path):
    database = create_database(tmp_path)
    service = AdminItemBatchGrantService(database)
    service.grant("seed", "admin", ["3"], 10, "丹药", "道具", 1, 99)
    conn = sqlite3.connect(database)
    conn.execute(
        "CREATE TRIGGER reject_batch_log BEFORE INSERT ON economy_log "
        "WHEN NEW.trace_id='log-failure' BEGIN SELECT RAISE(ABORT,'reject log'); END"
    )
    conn.commit()
    conn.close()

    try:
        service.grant("log-failure", "admin", ["1"], 10, "丹药", "道具", 2, 99)
    except Exception as exc:
        assert "reject log" in str(exc)
    else:
        raise AssertionError("economy log failure must abort the batch chunk")

    conn = sqlite3.connect(database)
    assert conn.execute("SELECT goods_num FROM back WHERE user_id='1'").fetchone() is None
    assert conn.execute(
        "SELECT COUNT(*) FROM admin_item_batch_grant_operations WHERE operation_id='log-failure'"
    ).fetchone()[0] == 0
    assert conn.execute(
        "SELECT COUNT(*) FROM admin_item_batch_grant_progress WHERE operation_id='log-failure'"
    ).fetchone()[0] == 0
    conn.close()


def test_payload_includes_operator_and_normalizes_duplicate_users(tmp_path):
    database = create_database(tmp_path)
    service = AdminItemBatchGrantService(database)
    result = service.grant("stable", "admin-a", ["2", "1", "2"], 10, "丹药", "道具", 1, 99)
    assert (result.total, result.completed, result.added) == (2, 2, 2)
    assert service.grant(
        "stable", "admin-a", ["1", "2"], 10, "丹药", "道具", 1, 99
    ).status == "duplicate"
    assert service.grant(
        "stable", "admin-b", ["1", "2"], 10, "丹药", "道具", 1, 99
    ).status == "operation_conflict"


def test_resume_uses_frozen_recipient_snapshot(tmp_path):
    database = create_database(tmp_path)
    service = AdminItemBatchGrantService(database)
    first = service.grant(
        "frozen-users", "admin", ["1", "2"], 10, "丹药", "道具", 1, 99, chunk_size=1
    )
    assert (first.total, first.completed) == (2, 1)

    resumed = service.grant(
        "frozen-users", "admin", ["1", "2", "3"], 10, "丹药", "道具", 1, 99
    )
    assert (resumed.total, resumed.completed, resumed.added) == (2, 2, 2)

    conn = sqlite3.connect(database)
    assert conn.execute("SELECT goods_num FROM back WHERE user_id='3'").fetchone() is None
    conn.close()
