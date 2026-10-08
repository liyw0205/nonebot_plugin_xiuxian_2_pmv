from __future__ import annotations

import asyncio
import sqlite3

from ..message_recall_application import MessageRecallApplication
from ..message_recall_repository import MessageRecallRepository


def _message_database(path):
    with sqlite3.connect(path) as connection:
        connection.execute(
            "CREATE TABLE messages("
            "id INTEGER PRIMARY KEY,adapter TEXT,scene TEXT,message_id TEXT,content TEXT)"
        )
        connection.executemany(
            "INSERT INTO messages VALUES(?,?,?,?,?)",
            [
                (1, "QQ", "group", "m1", "original one"),
                (2, "OneBot V11", "group", "m1", "original two"),
            ],
        )


class _Delivery:
    def __init__(self, error=None):
        self.error = error
        self.calls = []

    async def recall(self, bot, **kwargs):
        self.calls.append((bot, kwargs))
        if self.error:
            raise self.error
        return None


def _application(delivery, database):
    return MessageRecallApplication(delivery, MessageRecallRepository(database))


def test_delivery_failure_does_not_update_history(tmp_path):
    database = tmp_path / "message.db"
    _message_database(database)
    delivery = _Delivery(RuntimeError("platform rejected recall"))
    application = _application(delivery, database)

    try:
        asyncio.run(
            application.revoke(
                object(), adapter="QQ", scene="group", message_id="m1", row_id="1"
            )
        )
    except RuntimeError as exc:
        assert str(exc) == "platform rejected recall"
    else:
        raise AssertionError("delivery failure should propagate")

    with sqlite3.connect(database) as connection:
        content = connection.execute(
            "SELECT content FROM messages WHERE id=1"
        ).fetchone()[0]
        assert content == "original one"


def test_successful_platform_recall_updates_row_id_first(tmp_path):
    database = tmp_path / "message.db"
    _message_database(database)
    delivery = _Delivery()
    application = _application(delivery, database)

    result = asyncio.run(
        application.revoke(
            "bot",
            adapter="wrong adapter is ignored when row_id is present",
            scene="private",
            message_id="unrelated",
            row_id="1",
        )
    )

    assert result["success"] is True
    assert result["log_updated"] is True
    assert result["log_error"] == ""
    assert delivery.calls[0][1] == {
        "scene": "private",
        "message_id": "unrelated",
        "group_id": "",
        "user_id": "",
    }
    with sqlite3.connect(database) as connection:
        recalled = connection.execute(
            "SELECT content FROM messages WHERE id=1"
        ).fetchone()[0]
        untouched = connection.execute(
            "SELECT content FROM messages WHERE id=2"
        ).fetchone()[0]
        assert recalled == "[该消息已撤回]"
        assert untouched == "original two"


def test_history_write_failure_keeps_platform_success_result(tmp_path):
    database = tmp_path / "message.db"
    _message_database(database)
    delivery = _Delivery()

    class FailingRepository:
        def mark_recalled(self, **kwargs):
            raise sqlite3.OperationalError("database is locked")

    result = asyncio.run(
        MessageRecallApplication(delivery, FailingRepository()).revoke(
            "bot", adapter="QQ", scene="group", message_id="m1"
        )
    )

    assert result == {
        "success": True,
        "message": "撤回成功",
        "log_updated": False,
        "log_error": "database is locked",
    }
    assert len(delivery.calls) == 1


def test_missing_database_does_not_get_created(tmp_path):
    database = tmp_path / "missing" / "message.db"
    delivery = _Delivery()

    result = asyncio.run(
        _application(delivery, database).revoke(
            "bot", adapter="QQ", scene="group", message_id="m1"
        )
    )

    assert result["success"] is True
    assert result["log_updated"] is False
    assert result["log_error"] == "消息日志数据库不存在"
    assert not database.exists()


def test_history_update_uses_adapter_scene_and_message_id_without_row_id(tmp_path):
    database = tmp_path / "message.db"
    _message_database(database)
    repository = MessageRecallRepository(database)

    updated = repository.mark_recalled(
        adapter="QQ", scene="group", message_id="m1"
    )

    assert updated == 1
    with sqlite3.connect(database) as connection:
        recalled = connection.execute(
            "SELECT content FROM messages WHERE id=1"
        ).fetchone()[0]
        untouched = connection.execute(
            "SELECT content FROM messages WHERE id=2"
        ).fetchone()[0]
        assert recalled == "[该消息已撤回]"
        assert untouched == "original two"
