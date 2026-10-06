from __future__ import annotations

import sqlite3
from datetime import datetime

import tests  # Establish isolated paths before importing plugin modules.
import pytest

from ....infrastructure.database import DatabaseUnitOfWork
from ..broadcast_history_repository import AdminBroadcastHistoryRepository


NOW = datetime(2026, 10, 7, 12, 0, 0)


@pytest.fixture
def history(tmp_path):
    database = tmp_path / "message.db"
    with DatabaseUnitOfWork(database) as uow:
        uow.execute(
            "CREATE TABLE messages(id INTEGER PRIMARY KEY,adapter TEXT,bot_id TEXT,"
            "direction TEXT,scene TEXT,group_id TEXT,user_id TEXT,message_id TEXT,"
            "created_at TEXT,content TEXT)"
        )
    return AdminBroadcastHistoryRepository(database)


def _insert(history, **changes):
    values = {
        "adapter": "QQ", "bot_id": "bot-a", "direction": "recv", "scene": "group",
        "group_id": "group-a", "user_id": "user-a", "message_id": "message-a",
        "created_at": "2026-10-07 11:59:30", "content": "not needed for broadcast targets",
    }
    values.update(changes)
    with DatabaseUnitOfWork(history.database) as uow:
        uow.execute(
            "INSERT INTO messages(adapter,bot_id,direction,scene,group_id,user_id,message_id,created_at,content) "
            "VALUES(?,?,?,?,?,?,?,?,?)", tuple(values.values()),
        )


def test_qq_cutoff_direction_and_nonempty_message_id(history):
    _insert(history, group_id="boundary", message_id="at-cutoff", created_at="2026-10-07 11:59:00")
    _insert(history, group_id="expired", message_id="too-old", created_at="2026-10-07 11:58:59")
    _insert(history, group_id="sent", direction="send")
    _insert(history, group_id="empty-message", message_id="")
    _insert(history, group_id="null-message", message_id=None)
    _insert(history, group_id="", message_id="empty-target")
    _insert(history, group_id=None, message_id="null-target")
    assert history.targets("QQ", "bot-a", "global", NOW) == [
        {"scene": "group", "target_id": "boundary", "message_id": "at-cutoff"}
    ]


def test_qq_chooses_latest_created_at_then_id_per_scene_and_target(history):
    _insert(history, group_id="same", message_id="newest-time", created_at="2026-10-07 11:59:50")
    _insert(history, group_id="same", message_id="later-id-older-time", created_at="2026-10-07 11:59:20")
    _insert(history, group_id="same", message_id="tie-first", created_at="2026-10-07 11:59:50")
    _insert(history, group_id="same", message_id="tie-last", created_at="2026-10-07 11:59:50")
    _insert(history, scene="channel_group", group_id="same", message_id="channel", created_at="2026-10-07 11:59:40")
    _insert(history, scene="private", user_id="same", message_id="private", created_at="2026-10-07 11:59:55")
    assert history.targets("QQ", "bot-a", "global", NOW) == [
        {"scene": "private", "target_id": "same", "message_id": "private"},
        {"scene": "group", "target_id": "same", "message_id": "tie-last"},
        {"scene": "channel_group", "target_id": "same", "message_id": "channel"},
    ]


@pytest.mark.parametrize("adapter", ["QQ", "OneBot V11", "ob11", "v11", "OneBot-V11", "onebot_v11"])
def test_adapter_and_bot_id_are_strictly_isolated(history, adapter):
    _insert(history, adapter=adapter, group_id="mine")
    _insert(history, adapter=adapter, bot_id="bot-b", group_id="another-bot")
    _insert(history, adapter=adapter, bot_id="", group_id="legacy-without-bot")
    _insert(history, adapter=adapter, bot_id=None, group_id="legacy-null-bot")
    _insert(history, adapter="another-adapter", group_id="another-adapter")
    assert [target["target_id"] for target in history.targets(adapter, "bot-a", "global", NOW)] == ["mine"]
    assert history.targets(adapter, "", "global", NOW) == []
    assert history.targets(adapter, "bot-c", "global", NOW) == []


@pytest.mark.parametrize("adapter", ["QQ", "OneBot V11"])
@pytest.mark.parametrize("kind,expected", [
    ("group", {"group", "channel_group"}),
    ("private", {"private", "channel_private"}),
    ("global", {"group", "channel_group", "private", "channel_private"}),
    ("invalid", set()),
])
def test_kind_selects_only_eligible_scenes(history, adapter, kind, expected):
    for scene in ("group", "channel_group", "private", "channel_private", "unknown"):
        _insert(history, adapter=adapter, scene=scene)
    assert {target["scene"] for target in history.targets(adapter, "bot-a", kind, NOW)} == expected


def test_ob11_preserves_all_historical_directions_and_group_before_private_order(history):
    _insert(history, adapter="OneBot V11", group_id="send-only", direction="send",
            message_id=None, created_at="2020-01-01 00:00:00")
    _insert(history, adapter="OneBot V11", group_id="old-group", direction="send",
            message_id="", created_at="2020-01-01 00:00:00")
    _insert(history, adapter="OneBot V11", group_id="new-group", direction="recv")
    _insert(history, adapter="OneBot V11", group_id="old-group", direction="recv")
    _insert(history, adapter="OneBot V11", scene="channel_group", group_id="old-group")
    _insert(history, adapter="OneBot V11", scene="private", user_id="person", direction="send")
    _insert(history, adapter="OneBot V11", scene="channel_private", user_id="person", direction="recv")
    _insert(history, adapter="OneBot V11", scene="private", user_id="person", direction="send")
    _insert(history, adapter="OneBot V11", scene="private", user_id="")
    _insert(history, adapter="OneBot V11", scene="group", group_id=None)
    assert history.targets("OneBot V11", "bot-a", "global", NOW) == [
        {"scene": "channel_group", "target_id": "old-group", "message_id": ""},
        {"scene": "group", "target_id": "old-group", "message_id": ""},
        {"scene": "group", "target_id": "new-group", "message_id": ""},
        {"scene": "group", "target_id": "send-only", "message_id": ""},
        {"scene": "private", "target_id": "person", "message_id": ""},
        {"scene": "channel_private", "target_id": "person", "message_id": ""},
    ]


@pytest.mark.parametrize("adapter", ["unsupported", "OneBot V12", "unsupported-v11", "unknownv11", "qq"])
def test_unsupported_adapter_returns_no_targets(history, adapter):
    _insert(history, adapter=adapter)
    assert history.targets(adapter, "bot-a", "global", NOW) == []


def test_missing_database_and_missing_messages_table_return_empty_without_schema_creation(tmp_path):
    database = tmp_path / "absent" / "message.db"
    assert AdminBroadcastHistoryRepository(database).targets("QQ", "bot-a", "global", NOW) == []
    assert not database.parent.exists()
    database = tmp_path / "empty.db"
    with DatabaseUnitOfWork(database) as uow:
        uow.execute("CREATE TABLE unrelated(value TEXT)")
    original = database.read_bytes()
    assert AdminBroadcastHistoryRepository(database).targets("QQ", "bot-a", "global", NOW) == []
    assert database.read_bytes() == original
    with DatabaseUnitOfWork(database, read_only=True) as uow:
        assert uow.query_all("SELECT name FROM sqlite_master WHERE type='table'") == [{"name": "unrelated"}]


@pytest.mark.parametrize("adapter", ["QQ", "OneBot V11"])
def test_incomplete_message_schema_fails_without_migration(tmp_path, adapter):
    database = tmp_path / "message.db"
    with DatabaseUnitOfWork(database) as uow:
        uow.execute("CREATE TABLE messages(id INTEGER PRIMARY KEY,adapter TEXT,scene TEXT)")
    original = database.read_bytes()
    with pytest.raises(RuntimeError, match="broadcast history schema incomplete.*bot_id"):
        AdminBroadcastHistoryRepository(database).targets(adapter, "bot-a", "global", NOW)
    assert database.read_bytes() == original
    with DatabaseUnitOfWork(database, read_only=True) as uow:
        assert {row["name"] for row in uow.query_all("PRAGMA table_info(messages)")} == {"id", "adapter", "scene"}


@pytest.mark.parametrize("adapter", ["QQ", "OneBot V11"])
def test_queries_are_readonly_do_not_load_content_and_close_before_return(history, monkeypatch, adapter):
    _insert(history, adapter=adapter)
    original_enter = DatabaseUnitOfWork.__enter__
    connections = []

    def enter(uow):
        assert uow.read_only
        result = original_enter(uow)
        connection = uow.connection
        connections.append(connection)
        with pytest.raises(sqlite3.OperationalError, match="readonly"):
            connection.execute("DELETE FROM messages")

        def authorize(action, table, column, database, source):
            if action == sqlite3.SQLITE_READ and table == "messages" and column == "content":
                return sqlite3.SQLITE_DENY
            return sqlite3.SQLITE_OK

        connection.set_authorizer(authorize)
        return result

    monkeypatch.setattr(DatabaseUnitOfWork, "__enter__", enter)
    assert history.targets(adapter, "bot-a", "global", NOW)
    assert len(connections) == 1
    with pytest.raises(sqlite3.ProgrammingError, match="closed"):
        connections[0].execute("SELECT 1")
