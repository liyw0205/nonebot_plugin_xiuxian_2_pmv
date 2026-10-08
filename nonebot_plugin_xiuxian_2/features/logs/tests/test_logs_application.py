from __future__ import annotations

from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

import nonebot
nonebot.init()

from .. import LogsApplication
from ..file_repository import LogFileRepository
from ..message_repository import MessageLogsRepository
from ....infrastructure.database import DatabaseUnitOfWork
from ....xiuxian.xiuxian_web import messages as web_messages


class LogsApplicationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory(prefix="logs-owner-")
        self.root = Path(self.temp.name)
        self.game = self.root / "game.db"
        self.messages = self.root / "message.db"
        self._create_game_db()
        self._create_message_db()

    def tearDown(self) -> None:
        self.temp.cleanup()

    def _create_game_db(self) -> None:
        with sqlite3.connect(self.game) as conn:
            conn.execute(
                "CREATE TABLE user_xiuxian(user_id TEXT,user_name TEXT,level TEXT,root_type TEXT)"
            )
            conn.executemany(
                "INSERT INTO user_xiuxian VALUES(?,?,?,?)",
                [("u1", "道友一", "筑基", "木"), ("u2", "道友二", "炼气", "水")],
            )

    def _create_message_db(self) -> None:
        with sqlite3.connect(self.messages) as conn:
            conn.execute(
                "CREATE TABLE messages("
                "id INTEGER PRIMARY KEY,user_id TEXT,direction TEXT,scene TEXT,adapter TEXT,bot_id TEXT,"
                "username TEXT,nickname TEXT,avatar TEXT,created_at TEXT,message_id TEXT,source_message_id TEXT,"
                "content TEXT,group_name TEXT,group_id TEXT,reference_id TEXT,reply_used_count INTEGER)"
            )
            conn.execute("CREATE TABLE user_nicknames(user_id TEXT PRIMARY KEY,username TEXT)")
            conn.executemany(
                "INSERT INTO messages(id,user_id,direction,scene,adapter,bot_id,username,nickname,avatar,"
                "created_at,message_id,source_message_id,content,group_name,group_id,reference_id,reply_used_count) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                [
                    (1, "u1", "recv", "group", "OneBot V11", "bot", "群昵称", "备用昵称", "", "2026-10-08 10:00:00", "m1", "", "hello", "群", "g1", "", 0),
                    (2, "u1", "send", "group", "OneBot V11", "bot", "Bot", "Bot", "", "2026-10-08 10:00:01", "s1", "m1", "reply", "群", "g1", "", 0),
                    (3, "u2", "recv", "private", "OneBot V11", "bot", "私聊昵称", "", "", "2026-10-08 11:00:00", "m2", "", "needle", "", "", "", 0),
                ],
            )
            conn.execute("INSERT INTO user_nicknames VALUES('u1','缓存昵称')")

    def test_users_aggregates_candidates_and_linked_bot_replies(self) -> None:
        app = LogsApplication(self.game, self.messages, self.root / "module.py", cwd=self.root, home=self.root)

        result = app.users(limit=20)

        self.assertTrue(result["success"])
        rows = {row["user_id"]: row for row in result["rows"]}
        self.assertEqual(set(rows), {"u1", "u2"})
        self.assertEqual(rows["u1"]["user_name"], "道友一")
        self.assertEqual(rows["u1"]["title"], "道友一")
        self.assertEqual(rows["u1"]["message_count"], 2)
        self.assertEqual(rows["u1"]["recv_count"], 1)
        self.assertEqual(rows["u1"]["send_count"], 1)
        self.assertEqual(rows["u1"]["last_row_id"], 2)
        self.assertEqual(rows["u2"]["title"], "道友二")

    def test_user_messages_preserves_reply_link_filters_and_pagination(self) -> None:
        repository = MessageLogsRepository(self.game, self.messages)

        first = repository.user_messages(
            "u1", scene="ALL", direction="ALL", keyword="", adapter="",
            start="", end="", page=1, page_size=1,
        )
        sent_only = repository.user_messages(
            "u1", scene="group", direction="send", keyword="reply", adapter="OneBot V11",
            start="2026-10-08T10:00:00", end="2026-10-08T10:00:02", page=1, page_size=20,
        )

        self.assertEqual(first["total"], 2)
        self.assertEqual(first["rows"][0]["id"], 2)
        self.assertEqual(sent_only["total"], 1)
        self.assertEqual(sent_only["rows"][0]["source_message_id"], "m1")
        self.assertEqual(sent_only["user"]["user_name"], "道友一")

    def test_read_only_message_lookup_does_not_create_missing_database(self) -> None:
        missing = self.root / "missing.db"
        repository = MessageLogsRepository(self.game, missing)

        with self.assertRaises(FileNotFoundError):
            repository.message_user_candidates("", 10)

        self.assertFalse(missing.exists())

    def test_message_row_presenter_reuses_read_only_connection(self) -> None:
        with DatabaseUnitOfWork(self.messages, read_only=True) as uow:
            rows = uow.query_all("SELECT * FROM messages WHERE user_id='u2'")
            statements = []
            uow.connection.set_trace_callback(statements.append)
            names = web_messages.get_latest_human_names_by_user_ids(uow.connection, ["u1", "u2"])
            name_query_count = sum(statement.lstrip().upper().startswith("SELECT") for statement in statements)
            statements.clear()
            with patch.object(web_messages, "get_message_db_connection", side_effect=AssertionError("unexpected init")):
                prepared = web_messages._prepare_message_rows(rows, conn=uow.connection)

        self.assertEqual(names, {"u1": "缓存昵称", "u2": "私聊昵称"})
        self.assertEqual(name_query_count, 2)
        self.assertEqual(prepared[0]["display_content"], "needle")
        self.assertEqual(prepared[0]["username"], "私聊昵称")
        self.assertEqual(prepared[0]["mention_names"], {})


class LogFileRepositoryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory(prefix="logs-files-")
        self.root = Path(self.temp.name)
        self.log = self.root / "service.log"
        self.repository = LogFileRepository(
            self.root / "data" / "game.db",
            self.root / "module.py",
            cwd=self.root,
            home=self.root,
        )

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_read_streams_pages_and_reuses_bounded_index(self) -> None:
        self.log.write_text("2026-10-08 10:00:00 INFO first\n2026-10-08 10:00:01 ERROR second\n", encoding="utf-8")

        with patch.object(self.repository, "_filter_row", wraps=self.repository._filter_row) as filter_row:
            first = self.repository.read("service.log", "", "ALL", "", "", 1, 1)
            scanned_rows = filter_row.call_count
            second = self.repository.read("service.log", "", "ALL", "", "", 2, 1)

        self.assertEqual(first["total"], 2)
        self.assertEqual(first["rows"][0]["text"], "2026-10-08 10:00:00 INFO first")
        self.assertEqual(second["rows"][0]["level"], "ERROR")
        self.assertEqual(filter_row.call_count, scanned_rows)
        self.assertEqual(len(self.repository._read_indexes), 1)

    def test_tail_retains_an_unfinished_line_until_next_poll(self) -> None:
        first_line = b"2026-10-08 10:00:00 INFO first\n"
        self.log.write_bytes(first_line + b"2026-10-08 10:00:01 INFO par")
        file_id = str(self.log.resolve())

        first = self.repository.tail(file_id, 0, "", "ALL", "", "", False, [])
        partial = self.repository.tail(file_id, first["next_offset"], "", "ALL", "", "", False, [])
        with self.log.open("ab") as stream:
            stream.write(b"tial\n")
        complete = self.repository.tail(file_id, partial["next_offset"], "", "ALL", "", "", False, [])

        self.assertEqual(first["lines"][0]["text"], "2026-10-08 10:00:00 INFO first")
        self.assertEqual(partial["lines"], [])
        self.assertEqual(partial["next_offset"], len(first_line))
        self.assertEqual(complete["lines"][0]["text"], "2026-10-08 10:00:01 INFO partial")

    def test_tail_resets_offset_after_truncate_and_rotate(self) -> None:
        self.log.write_bytes(b"INFO old record that is long\n")
        file_id = str(self.log.resolve())
        initial = self.repository.tail(file_id, 0, "", "ALL", "", "", False, [])
        old_offset = initial["next_offset"]
        self.log.write_bytes(b"INFO new\n")

        truncated = self.repository.tail(file_id, old_offset, "", "ALL", "", "", False, [])

        self.assertEqual(truncated["offset"], 0)
        self.assertEqual(truncated["lines"][0]["text"], "INFO new")

        rotated_file = self.root / "service.log.new"
        rotated_file.write_bytes(b"INFO replacement file with a larger size\n")
        rotated_file.replace(self.log)
        rotated = self.repository.tail(file_id, len(b"INFO new\n"), "", "ALL", "", "", False, [])

        self.assertEqual(rotated["offset"], 0)
        self.assertEqual(rotated["lines"][0]["text"], "INFO replacement file with a larger size")


if __name__ == "__main__":
    unittest.main()
