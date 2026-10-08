from __future__ import annotations

import tempfile
import json
import unittest
from datetime import datetime as DateTime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_work.transaction_service import (
    WorkRefreshSettlementService,
)
from tests.test_db_backend import db_backend


class WorkRefreshSettlementTests(unittest.TestCase):
    def test_work_facade_uses_feature_refresh_application(self):
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_work

        self.assertTrue(hasattr(xiuxian_work, "work_refresh_application"))
        self.assertFalse(hasattr(xiuxian_work, "_work_refresh_service_instance"))

    def test_offer_projection_read_without_migration_does_not_create_schema(self):
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_work import reward_data_source

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database = root / "game.sqlite3"
            players = root / "players"
            player_dir = players / "u"
            player_dir.mkdir(parents=True)
            offer = {"tasks": {"采药": {"time": 5}}, "status": 1}
            (player_dir / "workinfo.json").write_text(
                json.dumps(offer), encoding="utf-8"
            )
            paths = SimpleNamespace(game_db=database)
            with patch.object(reward_data_source, "PLAYERSDATA", players), patch.object(
                reward_data_source, "get_paths", return_value=paths
            ):
                self.assertEqual(reward_data_source.readf("u"), offer)

            with db_backend.connection(database) as conn:
                tables = {
                    row[0]
                    for row in conn.execute(
                        "SELECT name FROM sqlite_master WHERE type='table'"
                    ).fetchall()
                }
        self.assertNotIn("work_offer_snapshots", tables)

    def test_legacy_json_fallback_is_not_imported_during_read(self):
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_work import reward_data_source

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database = root / "game.sqlite3"
            players = root / "players"
            player_dir = players / "u"
            player_dir.mkdir(parents=True)
            offer = {"tasks": {"采药": {"time": 5}}, "status": 1}
            (player_dir / "workinfo.json").write_text(json.dumps(offer), encoding="utf-8")
            with db_backend.transaction(database) as conn:
                conn.execute(
                    "CREATE TABLE work_offer_snapshots(user_id TEXT PRIMARY KEY,snapshot TEXT,updated_at TEXT)"
                )
            paths = SimpleNamespace(game_db=database)
            with patch.object(reward_data_source, "PLAYERSDATA", players), patch.object(
                reward_data_source, "get_paths", return_value=paths
            ):
                self.assertEqual(reward_data_source.readf("u"), offer)

            with db_backend.connection(database) as conn:
                count = conn.execute(
                    "SELECT COUNT(*) FROM work_offer_snapshots WHERE user_id='u'"
                ).fetchone()[0]
        self.assertEqual(count, 0)

    def test_expired_offer_uses_feature_transition_and_json_only_projection(self):
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_work import reward_data_source

        offer = {
            "tasks": {"采药": {"time": 5}},
            "status": 1,
            "refresh_time": "2026-10-04 09:00:00",
        }
        expired = {**offer, "status": 0}
        callback = Mock(return_value=SimpleNamespace(status="applied", offer=expired))

        class FrozenDateTime:
            @staticmethod
            def now():
                return DateTime(2026, 10, 4, 10, 0, 0)

            @staticmethod
            def strptime(value, date_format):
                return DateTime.strptime(value, date_format)

        with (
            patch.object(reward_data_source, "datetime", FrozenDateTime),
            patch.object(reward_data_source, "readf", return_value=offer),
            patch.object(reward_data_source, "savef") as save,
        ):
            has_work, current = reward_data_source.has_unaccepted_work(
                "u", mark_expired=callback
            )

        self.assertFalse(has_work)
        self.assertEqual(current, expired)
        callback.assert_called_once_with("u", offer)
        save.assert_called_once_with("u", expired, sync_snapshot=False)

    def test_expiration_conflict_preserves_a_new_unaccepted_offer(self):
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_work import reward_data_source

        stale = {
            "tasks": {"采药": {"time": 5}},
            "status": 1,
            "refresh_time": "2026-10-04 09:00:00",
        }
        current = {**stale, "refresh_time": "2026-10-04 09:59:00"}
        callback = Mock(return_value=SimpleNamespace(status="state_changed", offer=current))

        class FrozenDateTime:
            @staticmethod
            def now():
                return DateTime(2026, 10, 4, 10, 0, 0)

            @staticmethod
            def strptime(value, date_format):
                return DateTime.strptime(value, date_format)

        with (
            patch.object(reward_data_source, "datetime", FrozenDateTime),
            patch.object(reward_data_source, "readf", return_value=stale),
            patch.object(reward_data_source, "savef"),
        ):
            has_work, result = reward_data_source.has_unaccepted_work(
                "u", mark_expired=callback
            )

        self.assertTrue(has_work)
        self.assertEqual(result, current)

    def test_offer_projection_write_without_migration_fails_without_creating_schema(self):
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_work import reward_data_source

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database = root / "game.sqlite3"
            players = root / "players"
            paths = SimpleNamespace(game_db=database)
            with patch.object(reward_data_source, "PLAYERSDATA", players), patch.object(
                reward_data_source, "get_paths", return_value=paths
            ):
                with self.assertRaisesRegex(RuntimeError, "work_offer_snapshots"):
                    reward_data_source.savef(
                        "u", {"tasks": {"采药": {"time": 5}}, "status": 1}
                    )
            self.assertFalse((players / "u" / "workinfo.json").exists())
            if database.exists():
                with db_backend.connection(database) as conn:
                    tables = conn.execute(
                        "SELECT name FROM sqlite_master WHERE type='table'"
                    ).fetchall()
                self.assertEqual(tables, [])

    def test_offer_generation_uses_supplied_profile_without_eager_item_cache(self):
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_work import work_handle

        observed = {}

        def build_offer(work_level, exp, user_level, **kwargs):
            observed.update(work_level=work_level, exp=exp, user_level=user_level)
            return {"采药": [80, 12, 5, 101, "成功", "失败"]}

        with patch.object(work_handle, "workmake", side_effect=build_offer):
            handler = object.__new__(work_handle.workhandle)
            task_list, offer = handler.do_work(
                0,
                level="筑基",
                exp=100,
                user_id="u",
                persist=False,
            )

        self.assertEqual(
            observed,
            {"work_level": "筑基", "exp": 100, "user_level": "筑基"},
        )
        self.assertEqual(task_list[0][0], "采药")
        self.assertEqual(offer["user_level"], "筑基")
        self.assertFalse(hasattr(work_handle, "items"))

    def test_settlement_calculation_uses_supplied_active_snapshot(self):
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_work import work_handle

        handler = object.__new__(work_handle.workhandle)
        offer = {
            "tasks": {
                "采药": {
                    "rate": 80, "award": 10, "item_id": 0,
                    "success_msg": "完成", "fail_msg": "失败",
                }
            },
            "status": 2,
            "scheduled_time": "采药",
        }
        with patch.object(work_handle, "readf", side_effect=AssertionError("legacy read used")):
            result = handler.do_work(
                2,
                work_list="采药",
                user_id="u",
                offer_snapshot=offer,
                random_source=SimpleNamespace(randint=lambda lower, upper: 1),
            )

        self.assertEqual(result, ("完成", 10, True, 0, False))


class WorkSettlementHandlerTests(unittest.IsolatedAsyncioTestCase):
    async def test_handler_passes_matching_feature_snapshot_to_reward_calculator(self):
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_work

        active = {
            "tasks": {"采药": {"rate": 80, "award": 10, "time": 5, "item_id": 0}},
            "status": 2,
            "scheduled_time": "采药",
            "create_time": "2026-10-04 10:00:00",
        }
        outcome = SimpleNamespace(
            status="applied",
            replayed=False,
            data={"status": "applied", "exp": 10, "item_awarded": False},
        )
        calculator = SimpleNamespace(do_work=Mock(return_value=("完成", 10, True, 0, False)))
        event = SimpleNamespace(message_id="settle-1")

        with (
            patch.object(xiuxian_work.work_claim_application, "get_active_snapshot", return_value=active),
            patch.object(xiuxian_work, "_sql_message", return_value=SimpleNamespace(
                get_user_info_with_id=lambda user_id: {"level": "筑基", "exp": 0}
            )),
            patch.object(xiuxian_work, "workhandle", return_value=calculator),
            patch.object(
                xiuxian_work.work_status_application,
                "get_offer",
                side_effect=AssertionError("offer fallback read used"),
            ),
            patch.object(xiuxian_work, "OtherSet", return_value=SimpleNamespace(set_closing_type=lambda level: 1)),
            patch.object(xiuxian_work, "XiuConfig", return_value=SimpleNamespace(
                closing_exp_upper_limit=100, max_goods_num=99
            )),
            patch.object(xiuxian_work.work_settlement_application, "settle", return_value=outcome),
            patch.object(xiuxian_work, "log_message"),
            patch.object(xiuxian_work, "update_statistics_value"),
            patch.object(xiuxian_work, "record_task_progress"),
            patch.object(xiuxian_work, "handle_send", new=AsyncMock()),
            patch.object(xiuxian_work, "number_to", side_effect=str),
        ):
            await xiuxian_work.settle_work(
                object(), event, "u",
                {"create_time": "2026-10-04 10:00:00", "scheduled_time": "采药"},
            )

        calculator.do_work.assert_called_once()
        self.assertIs(calculator.do_work.call_args.kwargs["offer_snapshot"], active)

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.db = Path(self.temp.name) / "game.db"
        with db_backend.transaction(self.db) as conn:
            conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,work_num INTEGER)")
            conn.execute(
                "CREATE TABLE user_cd(user_id TEXT PRIMARY KEY,type INTEGER,create_time TEXT,scheduled_time TEXT)"
            )
            conn.execute("INSERT INTO user_xiuxian VALUES(%s,%s)", ("u", 3))
            conn.execute("INSERT INTO user_cd VALUES(%s,%s,%s,%s)", ("u", 0, "0", None))
        self.service = WorkRefreshSettlementService(self.db)
        self.cd = {"type": 0, "create_time": "0", "scheduled_time": None}
        self.offer = {
            "tasks": {"采药": {"rate": 80, "award": 10, "time": 5, "item_id": 0}},
            "status": 1,
            "refresh_time": "2026-07-14 10:00:00",
            "user_level": "筑基",
        }

    def tearDown(self):
        self.temp.cleanup()

    def refresh(self, operation="refresh", **changes):
        values = {"count": 3, "cd": self.cd, "old": None, "offer": self.offer, "force": False}
        values.update(changes)
        return self.service.refresh(
            operation, "u", values["count"], values["cd"], values["old"],
            values["offer"], values["force"],
        )

    def state(self):
        with db_backend.connection(self.db) as conn:
            count = conn.execute("SELECT work_num FROM user_xiuxian WHERE user_id=%s", ("u",)).fetchone()[0]
            try:
                offer = conn.execute("SELECT snapshot FROM work_offer_snapshots WHERE user_id=%s", ("u",)).fetchone()
            except db_backend.OperationalError:
                offer = None
        return int(count), offer[0] if offer else None

    def test_fixed_offer_and_count_are_committed_atomically(self):
        result = self.refresh()
        self.assertEqual((result.status, result.remaining_count), ("applied", 2))
        self.assertEqual(self.state()[0], 2)
        self.assertIn("采药", __import__("json").loads(self.state()[1])["tasks"])

    def test_duplicate_conflict_and_stale_count(self):
        self.assertEqual(self.refresh("same").status, "applied")
        # mutable offer blob must not break same-op replay (identity is user+force)
        changed = dict(self.offer)
        changed["refresh_time"] = "later"
        self.assertEqual(self.refresh("same", offer=changed).status, "duplicate")
        # force flag is request identity
        self.assertEqual(self.refresh("same", force=True, offer=changed).status, "operation_conflict")
        self.assertEqual(self.refresh("stale", count=3, old=self.offer, force=True).status, "state_changed")
        prior = self.service.get_result("same")
        self.assertIsNotNone(prior)
        self.assertEqual(prior.status, "duplicate")
        self.assertEqual(prior.remaining_count, 2)

    def test_force_refresh_replaces_expected_legacy_offer(self):
        old = dict(self.offer)
        old["refresh_time"] = "old"
        with db_backend.transaction(self.db) as conn:
            conn.execute(
                "CREATE TABLE work_offer_snapshots(user_id TEXT PRIMARY KEY,snapshot TEXT,updated_at TEXT)"
            )
            import json
            conn.execute("INSERT INTO work_offer_snapshots VALUES(%s,%s,%s)", ("u", json.dumps(old), "old"))
        result = self.refresh("force", old=old, force=True)
        self.assertEqual(result.status, "applied")
        self.assertNotIn('"refresh_time": "old"', self.state()[1])

    def test_operation_failure_rolls_back_offer_and_count(self):
        with db_backend.transaction(self.db) as conn:
            conn.execute(
                "CREATE TABLE work_refresh_operations(operation_id TEXT PRIMARY KEY,payload TEXT,"
                "remaining_count INTEGER,offer_snapshot TEXT,created_at TEXT)"
            )
            conn.execute(
                "CREATE TRIGGER fail_refresh BEFORE INSERT ON work_refresh_operations "
                "BEGIN SELECT RAISE(ABORT,'failed'); END"
            )
        with self.assertRaises(db_backend.IntegrityError):
            self.refresh("fail")
        self.assertEqual(self.state(), (3, None))


if __name__ == "__main__":
    unittest.main()
