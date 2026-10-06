from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import nonebot

nonebot.init()

from ..application import ActivityApplication
from ..collect_exchange_repository import ActivityCollectExchangeSqlRepository
from ..migrations import apply_activity_state_legacy, apply_activity_state_schema
from ..repository import ActivityRepository
from ....infrastructure.database import DatabaseUnitOfWork, OperationLedger
from ....xiuxian.xiuxian_activity import service


class ActivityCollectExchangeRepositoryTests(unittest.TestCase):
    rewards = (
        {"type": "stone", "quantity": 50, "name": "灵石", "desc": "获得 50 灵石"},
        {"id": 101, "type": "道具", "quantity": 2, "name": "福袋", "desc": "获得 福袋x2"},
    )

    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.database = Path(self.temp.name) / "game.db"
        with DatabaseUnitOfWork(self.database) as uow:
            apply_activity_state_schema(uow)
            OperationLedger().ensure_schema(uow)
            uow.execute(
                "CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER)"
            )
            uow.execute("INSERT INTO user_xiuxian VALUES('u',10)")
            uow.execute(
                "CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,"
                "goods_type TEXT,goods_num INTEGER,create_time TEXT,update_time TEXT,"
                "bind_num INTEGER,UNIQUE(user_id,goods_id))"
            )
            uow.executemany(
                "INSERT INTO activity_collect_inventory(activity_key,user_id,word_char,count) "
                "VALUES('festival','u',?,?)",
                (("端", 2), ("午", 2), ("安", 1), ("康", 1)),
            )
        self.repository = ActivityCollectExchangeSqlRepository(self.database)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def exchange(self, operation_id: str = "exchange", **overrides):
        arguments = {
            "operation_id": operation_id,
            "user_id": "u",
            "activity_key": "festival",
            "phrase": "端午安康",
            "required_tokens": {"端": 1, "午": 1, "安": 1, "康": 1},
            "limit": 1,
            "rewards": self.rewards,
            "max_goods_num": 100,
        }
        arguments.update(overrides)
        return self.repository.exchange(**arguments)

    def test_exchange_and_business_receipt_are_atomic_and_replay_once(self) -> None:
        first = self.exchange()
        replay = self.exchange()

        self.assertEqual("applied", first.status)
        self.assertEqual("duplicate", replay.status)
        self.assertEqual(1, replay.claim_count)
        self.assertEqual(("获得 50 灵石", "获得 福袋x2"), replay.rewards)
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            self.assertEqual(60, uow.query_one("SELECT stone FROM user_xiuxian WHERE user_id='u'")["stone"])
            self.assertEqual(2, uow.query_one("SELECT goods_num FROM back WHERE user_id='u'")["goods_num"])
            self.assertEqual(
                1,
                uow.query_one("SELECT COUNT(*) AS count FROM activity_collect_exchange_operations")["count"],
            )
            self.assertEqual(
                2,
                uow.query_one("SELECT SUM(count) AS count FROM activity_collect_inventory")["count"],
            )

    def test_conflict_limit_and_inventory_rejections_do_not_mutate(self) -> None:
        self.assertEqual("applied", self.exchange().status)
        self.assertEqual(
            "operation_conflict",
            self.exchange(
                rewards=({"type": "stone", "quantity": 51, "name": "灵石"},)
            ).status,
        )
        self.assertEqual("limit_reached", self.exchange("second").status)

        other = self.exchange(
            "insufficient",
            phrase="端午安康2",
            required_tokens={"端": 2, "午": 1},
            limit=0,
        )
        self.assertEqual("tokens_insufficient", other.status)
        self.assertEqual((("端", 1),), other.missing)

    def test_inventory_full_rejects_entire_exchange(self) -> None:
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute(
                "INSERT INTO back VALUES('u',101,'福袋','道具',99,'','',99)"
            )

        result = self.exchange()

        self.assertEqual("inventory_full", result.status)
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            self.assertEqual(
                1,
                uow.query_one(
                    "SELECT count FROM activity_collect_inventory "
                    "WHERE activity_key='festival' AND user_id='u' AND word_char='安'"
                )["count"],
            )
            self.assertEqual(10, uow.query_one("SELECT stone FROM user_xiuxian WHERE user_id='u'")["stone"])

    def test_receipt_failure_rolls_back_tokens_and_assets(self) -> None:
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute(
                "CREATE TRIGGER fail_collect_receipt BEFORE INSERT "
                "ON activity_collect_exchange_operations "
                "BEGIN SELECT RAISE(ABORT,'forced failure'); END"
            )

        with self.assertRaisesRegex(sqlite3.IntegrityError, "forced failure"):
            self.exchange("rollback")

        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            self.assertEqual(10, uow.query_one("SELECT stone FROM user_xiuxian WHERE user_id='u'")["stone"])
            self.assertEqual(
                0,
                uow.query_one("SELECT COUNT(*) AS count FROM activity_collect_claim")["count"],
            )
            self.assertEqual(
                0,
                uow.query_one("SELECT COUNT(*) AS count FROM back")["count"],
            )
            self.assertEqual(
                1,
                uow.query_one(
                    "SELECT count FROM activity_collect_inventory "
                    "WHERE activity_key='festival' AND user_id='u' AND word_char='安'"
                )["count"],
            )

    def test_default_application_uses_feature_repository_and_replays_once(self) -> None:
        application = ActivityApplication(
            self.database,
            repository=ActivityRepository(self.database),
        )
        activity = {"key": "festival", "name": "庆典", "type": "collect_words"}
        phrase = {
            "phrase": "端午安康",
            "name": "端午安康",
            "reward": "fixture",
            "limit": 1,
        }
        with (
            patch.object(service, "load_config", return_value={"festival_name": "庆典"}),
            patch.object(
                service,
                "activity_runtime_state",
                return_value={"ok": True, "features": ["exchange"], "stage_name": "兑换"},
            ),
            patch.object(service, "_find_collect_phrase", return_value=(activity, phrase)),
            patch.object(service, "_phrase_need_counter", return_value={"端": 1, "午": 1, "安": 1, "康": 1}),
            patch.object(service, "parse_reward", return_value=self.rewards),
            patch.object(service, "XiuConfig", return_value=SimpleNamespace(max_goods_num=100)),
        ):
            applied = application.execute(
                operation_id="activity:collect-exchange:u:message-1",
                user_id="u",
                payload={"action": "claim_collect_phrase", "query": "端午安康"},
            )
            replay = application.execute(
                operation_id="activity:collect-exchange:u:message-1",
                user_id="u",
                payload={"action": "claim_collect_phrase", "query": "端午安康"},
            )

        self.assertTrue(applied.ok)
        self.assertTrue(replay.replayed)
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            self.assertEqual(60, uow.query_one("SELECT stone FROM user_xiuxian WHERE user_id='u'")["stone"])

    def test_started_recovery_precedes_changed_activity_configuration(self) -> None:
        operation_id = "activity:collect-exchange:u:config-changed"
        self.exchange(operation_id)
        with DatabaseUnitOfWork(self.database) as uow:
            OperationLedger().begin(
                uow,
                operation_id,
                "activity.claim_collect_phrase",
                {"query": "端午安康", "user_id": "u"},
            )

        application = ActivityApplication(
            self.database,
            repository=ActivityRepository(self.database),
        )
        with (
            patch.object(service, "load_config", side_effect=AssertionError("config reloaded")),
            patch.object(service, "activity_runtime_state", side_effect=AssertionError("runtime rechecked")),
        ):
            recovered = application.execute(
                operation_id=operation_id,
                user_id="u",
                payload={"action": "claim_collect_phrase", "query": "端午安康"},
            )

        self.assertTrue(recovered.ok)
        self.assertIn("集字兑换成功：端午安康", recovered.data["message"])
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            self.assertEqual(60, uow.query_one("SELECT stone FROM user_xiuxian WHERE user_id='u'")["stone"])

    def test_started_retry_without_business_receipt_runs_current_request(self) -> None:
        operation_id = "activity:collect-exchange:u:first-retry"
        with DatabaseUnitOfWork(self.database) as uow:
            OperationLedger().begin(
                uow,
                operation_id,
                "activity.claim_collect_phrase",
                {"query": "端午安康", "user_id": "u"},
            )

        application = ActivityApplication(
            self.database,
            repository=ActivityRepository(self.database),
        )
        activity = {"key": "festival", "name": "庆典", "type": "collect_words"}
        phrase = {
            "phrase": "端午安康",
            "name": "端午安康",
            "reward": "fixture",
            "limit": 1,
        }
        with (
            patch.object(service, "load_config", return_value={"festival_name": "庆典"}),
            patch.object(
                service,
                "activity_runtime_state",
                return_value={"ok": True, "features": ["exchange"], "stage_name": "兑换"},
            ),
            patch.object(service, "_find_collect_phrase", return_value=(activity, phrase)),
            patch.object(service, "_phrase_need_counter", return_value={"端": 1, "午": 1, "安": 1, "康": 1}),
            patch.object(service, "parse_reward", return_value=self.rewards),
            patch.object(service, "XiuConfig", return_value=SimpleNamespace(max_goods_num=100)),
        ):
            recovered = application.execute(
                operation_id=operation_id,
                user_id="u",
                payload={"action": "claim_collect_phrase", "query": "端午安康"},
            )

        self.assertTrue(recovered.ok)
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            self.assertEqual(60, uow.query_one("SELECT stone FROM user_xiuxian WHERE user_id='u'")["stone"])

    def test_backfilled_legacy_receipt_replays_without_duplicate_award(self) -> None:
        root = Path(self.temp.name) / "legacy-replay"
        root.mkdir()
        database = root / "game.db"
        legacy_dir = root / "activity"
        legacy_dir.mkdir()
        legacy_database = legacy_dir / "activity.db"
        with sqlite3.connect(legacy_database) as conn:
            conn.executescript(
                "CREATE TABLE activity_collect_inventory(activity_key TEXT,user_id TEXT,"
                "word_char TEXT,count INTEGER,update_time TEXT,PRIMARY KEY(activity_key,user_id,word_char));"
                "CREATE TABLE activity_collect_claim(activity_key TEXT,user_id TEXT,phrase TEXT,"
                "count INTEGER,update_time TEXT,PRIMARY KEY(activity_key,user_id,phrase));"
                "CREATE TABLE activity_collect_exchange_operations(operation_id TEXT PRIMARY KEY,"
                "payload TEXT NOT NULL,result_json TEXT NOT NULL);"
            )
            conn.executemany(
                "INSERT INTO activity_collect_inventory VALUES('festival','u',?,?, '')",
                (("端", 1), ("午", 1), ("安", 0), ("康", 0)),
            )
            conn.execute(
                "INSERT INTO activity_collect_claim VALUES('festival','u','端午安康',1,'')"
            )
            conn.execute(
                "INSERT INTO activity_collect_exchange_operations VALUES(?,?,?)",
                (
                    "legacy-op",
                    '["u","festival","端午安康",[["午","1"],["安","1"],["康","1"],["端","1"]],"1","50",[["101","福袋","道具","2"]],"100"]',
                    '[1,["获得 50 灵石","获得 福袋x2"]]',
                ),
            )

        with DatabaseUnitOfWork(database) as uow:
            apply_activity_state_schema(uow)
            apply_activity_state_legacy(uow)
            uow.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER)")
            uow.execute("INSERT INTO user_xiuxian VALUES('u',60)")
            uow.execute(
                "CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,"
                "goods_type TEXT,goods_num INTEGER,create_time TEXT,update_time TEXT,"
                "bind_num INTEGER,UNIQUE(user_id,goods_id))"
            )
            uow.execute("INSERT INTO back VALUES('u',101,'福袋','道具',2,'','',2)")

        repository = ActivityCollectExchangeSqlRepository(database)
        replay = repository.exchange(
            "legacy-op",
            "u",
            "festival",
            "端午安康",
            {"端": 1, "午": 1, "安": 1, "康": 1},
            1,
            self.rewards,
            100,
        )

        self.assertEqual("duplicate", replay.status)
        self.assertEqual(1, replay.claim_count)
        with DatabaseUnitOfWork(database, read_only=True) as uow:
            self.assertEqual(60, uow.query_one("SELECT stone FROM user_xiuxian WHERE user_id='u'")["stone"])
            self.assertEqual(2, uow.query_one("SELECT goods_num FROM back WHERE user_id='u'")["goods_num"])
            self.assertEqual(
                1,
                uow.query_one("SELECT COUNT(*) AS count FROM activity_collect_exchange_operations")["count"],
            )

    def test_stale_started_ledger_resumes_against_existing_exchange_receipt(self) -> None:
        operation_id = "activity:collect-exchange:u:interrupted"
        self.exchange(operation_id)
        with DatabaseUnitOfWork(self.database) as uow:
            OperationLedger().begin(
                uow,
                operation_id,
                "activity.claim_collect_phrase",
                {"query": "端午安康", "user_id": "u"},
            )

        application = ActivityApplication(
            self.database,
            repository=ActivityRepository(self.database),
        )
        activity = {"key": "festival", "name": "庆典", "type": "collect_words"}
        phrase = {
            "phrase": "端午安康",
            "name": "端午安康",
            "reward": "fixture",
            "limit": 1,
        }
        with (
            patch.object(service, "load_config", return_value={"festival_name": "庆典"}),
            patch.object(
                service,
                "activity_runtime_state",
                return_value={"ok": True, "features": ["exchange"], "stage_name": "兑换"},
            ),
            patch.object(service, "_find_collect_phrase", return_value=(activity, phrase)),
            patch.object(service, "_phrase_need_counter", return_value={"端": 1, "午": 1, "安": 1, "康": 1}),
            patch.object(service, "parse_reward", return_value=self.rewards),
            patch.object(service, "XiuConfig", return_value=SimpleNamespace(max_goods_num=100)),
        ):
            recovered = application.execute(
                operation_id=operation_id,
                user_id="u",
                payload={"action": "claim_collect_phrase", "query": "端午安康"},
            )

        self.assertTrue(recovered.ok)
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            self.assertEqual(
                1,
                uow.query_one("SELECT COUNT(*) AS count FROM activity_collect_exchange_operations")["count"],
            )
            self.assertEqual(60, uow.query_one("SELECT stone FROM user_xiuxian WHERE user_id='u'")["stone"])


if __name__ == "__main__":
    unittest.main()
