from __future__ import annotations

from datetime import datetime
import sqlite3

from ..message_reply_repository import MessageReplyRepository


NOW = datetime(2026, 10, 8, 12, 0, 0)


def _database(path, rows=()):
    with sqlite3.connect(path) as connection:
        connection.execute(
            "CREATE TABLE messages("
            "id INTEGER PRIMARY KEY,adapter TEXT,direction TEXT,scene TEXT,message_id TEXT,"
            "reference_id TEXT,group_id TEXT,user_id TEXT,reply_used_count INTEGER,created_at TEXT)"
        )
        connection.executemany(
            "INSERT INTO messages VALUES(?,?,?,?,?,?,?,?,?,?)",
            rows,
        )


def _row(
    row_id,
    *,
    scene="group",
    message_id=None,
    reference_id="",
    group_id="g1",
    user_id="u1",
    created_at="2026-10-08 11:59:00",
    reply_used_count=0,
    adapter="QQ",
    direction="recv",
):
    return (
        row_id,
        adapter,
        direction,
        scene,
        message_id,
        reference_id,
        group_id,
        user_id,
        reply_used_count,
        created_at,
    )


def test_latest_candidates_preserve_group_window_limit_filters_and_order(tmp_path):
    database = tmp_path / "message.db"
    _database(
        database,
        [
            _row(1, message_id="boundary", created_at="2026-10-08 11:56:00"),
            _row(2, message_id="too-old", created_at="2026-10-08 11:55:59"),
            _row(3, message_id="used-up", reply_used_count=5),
            _row(4, message_id="wrong-group", group_id="g2"),
            _row(5, message_id="sent", direction="send"),
            _row(6, message_id="other-adapter", adapter="OneBot V11"),
            _row(7, message_id="", created_at="2026-10-08 11:59:30"),
            _row(8, message_id="tie-1", created_at="2026-10-08 11:58:00"),
            _row(9, message_id="tie-2", created_at="2026-10-08 11:58:00"),
        ],
    )
    repository = MessageReplyRepository(database, now=lambda: NOW)

    rows = repository.get_latest_reply_candidates_for_qq("group", "g1", limit=2)

    assert [row["id"] for row in rows] == [9, 8]
    assert repository.get_latest_reply_candidates_for_qq("group", "g1", limit=10)[-1]["id"] == 1
    assert repository.get_latest_reply_candidates_for_qq("group", "missing") == []
    assert repository.get_latest_reply_candidates_for_qq("invalid", "g1") == []


def test_latest_private_candidates_use_private_window_and_user_scope(tmp_path):
    database = tmp_path / "message.db"
    _database(
        database,
        [
            _row(1, scene="private", message_id="private-boundary", created_at="2026-10-08 11:00:00"),
            _row(2, scene="private", message_id="private-old", created_at="2026-10-08 10:59:59"),
            _row(3, scene="private", message_id="other-user", user_id="u2"),
            _row(4, scene="channel_private", message_id="channel-private", created_at="2026-10-08 11:30:00"),
        ],
    )
    repository = MessageReplyRepository(database, now=lambda: NOW)

    assert [row["id"] for row in repository.get_latest_reply_candidates_for_qq("private", "u1")] == [1]
    assert [row["id"] for row in repository.get_latest_reply_candidates_for_qq("channel_private", "u1")] == [4]


def test_specific_reply_validates_runtime_age_used_count_and_session(tmp_path):
    database = tmp_path / "message.db"
    _database(
        database,
        [
            _row(1, message_id="group-boundary", created_at="2026-10-08 11:55:00"),
            _row(2, message_id="group-expired", created_at="2026-10-08 11:54:59"),
            _row(3, message_id="too-used", reply_used_count=5),
            _row(4, message_id="private-boundary", scene="private", created_at="2026-10-08 11:00:00"),
            _row(5, message_id="wrong-target", group_id="g2"),
        ],
    )
    repository = MessageReplyRepository(database, now=lambda: NOW)

    assert repository.get_specific_reply_candidate_for_qq(
        scene="group", target_id="g1", message_id="group-boundary"
    )["id"] == 1
    assert repository.get_specific_reply_candidate_for_qq(
        scene="group", target_id="g1", message_id="group-expired"
    ) is None
    assert repository.get_specific_reply_candidate_for_qq(
        scene="group", target_id="g1", message_id="too-used"
    ) is None
    assert repository.get_specific_reply_candidate_for_qq(
        scene="group", target_id="g1", message_id="wrong-target"
    ) is None
    assert repository.get_specific_reply_candidate_for_qq(
        scene="private", target_id="u1", message_id="private-boundary"
    )["id"] == 4
    assert repository.get_specific_reply_candidate_for_qq(
        scene="group", target_id="g1", message_id=""
    ) is None


def test_specific_reference_matches_message_or_reference_without_age_filter(tmp_path):
    database = tmp_path / "message.db"
    _database(
        database,
        [
            _row(1, message_id="direct", reference_id="ref-old", created_at="2026-10-07 00:00:00", reply_used_count=5),
            _row(2, message_id="other", reference_id="ref-new", created_at="2026-10-08 11:59:00"),
            _row(3, message_id="direct", reference_id="", created_at="2026-10-08 11:58:00", group_id="g2"),
            _row(4, message_id="private", reference_id="private-ref", scene="private"),
        ],
    )
    repository = MessageReplyRepository(database, now=lambda: NOW)

    assert repository.get_specific_reference_candidate_for_qq(
        scene="group", target_id="g1", message_id="direct", reference_id="ref-new"
    )["id"] == 2
    assert repository.get_specific_reference_candidate_for_qq(
        scene="group", target_id="g1", message_id="", reference_id="ref-old"
    )["id"] == 1
    assert repository.get_specific_reference_candidate_for_qq(
        scene="private", target_id="u1", message_id="private"
    )["id"] == 4
    assert repository.get_specific_reference_candidate_for_qq(
        scene="group", target_id="g1"
    ) is None


def test_missing_database_or_table_returns_empty_without_creation(tmp_path):
    missing = tmp_path / "missing.db"
    repository = MessageReplyRepository(missing, now=lambda: NOW)

    assert repository.get_latest_reply_candidates_for_qq("group", "g1") == []
    assert repository.get_specific_reply_candidate_for_qq(
        scene="group", target_id="g1", message_id="m1"
    ) is None
    assert repository.get_specific_reference_candidate_for_qq(
        scene="group", target_id="g1", reference_id="r1"
    ) is None
    assert not missing.exists()

    empty = tmp_path / "empty.db"
    sqlite3.connect(empty).close()
    repository = MessageReplyRepository(empty, now=lambda: NOW)
    assert repository.get_latest_reply_candidates_for_qq("group", "g1") == []
    assert repository.get_specific_reply_candidate_for_qq(
        scene="group", target_id="g1", message_id="m1"
    ) is None
    assert repository.get_specific_reference_candidate_for_qq(
        scene="group", target_id="g1", reference_id="r1"
    ) is None
    with sqlite3.connect(empty) as connection:
        assert connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall() == []


def test_candidate_queries_do_not_modify_existing_database(tmp_path):
    database = tmp_path / "message.db"
    _database(database, [_row(1, message_id="m1", reference_id="r1")])
    repository = MessageReplyRepository(database, now=lambda: NOW)

    repository.get_latest_reply_candidates_for_qq("group", "g1")
    repository.get_specific_reply_candidate_for_qq(
        scene="group", target_id="g1", message_id="m1"
    )
    repository.get_specific_reference_candidate_for_qq(
        scene="group", target_id="g1", reference_id="r1"
    )

    with sqlite3.connect(database) as connection:
        assert connection.execute("SELECT COUNT(*) FROM messages").fetchone()[0] == 1
        assert connection.execute("SELECT reply_used_count FROM messages").fetchone()[0] == 0
        assert connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall() == [("messages",)]
