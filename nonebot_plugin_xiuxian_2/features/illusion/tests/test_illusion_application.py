from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from ..application import IllusionApplication
from ..migrations import apply_illusion, apply_illusion_state
from ....infrastructure.database import DatabaseUnitOfWork, OperationLedger


class IllusionApplicationTest(unittest.TestCase):
    def test_manifest_tracks_registered_state_migration(self) -> None:
        from ..manifest import FEATURE

        self.assertEqual(FEATURE.migration_version, "illusion.002")

    def _database(self, directory: str) -> Path:
        database = Path(directory) / "game.db"
        with DatabaseUnitOfWork(database) as uow:
            OperationLedger().ensure_schema(uow)
            uow.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY, stone INTEGER, exp INTEGER)")
            uow.execute("INSERT INTO user_xiuxian VALUES ('u', 0, 0)")
            uow.execute("CREATE TABLE back(user_id TEXT, goods_id INTEGER, goods_name TEXT, goods_type TEXT, goods_num INTEGER, create_time TEXT, update_time TEXT, bind_num INTEGER, UNIQUE(user_id, goods_id))")
            apply_illusion(uow)
            apply_illusion_state(uow)
        return database

    @staticmethod
    def _request(**overrides):
        request = {
            "action": "choose",
            "period": "2026-09-12",
            "question_index": 0,
            "choice_index": 0,
            "selected_option": "option",
            "stone": 1,
            "exp": 2,
            "item": None,
            "max_goods_num": 99,
        }
        request.update(overrides)
        return request

    def test_execute_is_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = self._database(directory)
            app = IllusionApplication(database)
            request = self._request()
            first = app.execute(operation_id="op-1", user_id="u", payload=request)
            second = app.execute(operation_id="op-1", user_id="u", payload=request)
            self.assertEqual(first.operation_id, second.operation_id)
            self.assertTrue(second.replayed)
            self.assertEqual(first.data["status"], "applied")

    def test_rejected_choice_does_not_change_assets(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = self._database(directory)
            app = IllusionApplication(database)
            app.execute(operation_id="op-1", user_id="u", payload=self._request())
            rejected = app.execute(
                operation_id="op-2",
                user_id="u",
                payload=self._request(choice_index=1, selected_option="other"),
            )
            self.assertEqual((rejected.status, rejected.code), ("rejected", "already_chosen"))
            with DatabaseUnitOfWork(database) as uow:
                row = uow.query_one("SELECT stone, exp FROM user_xiuxian WHERE user_id = 'u'")
            self.assertEqual((row["stone"], row["exp"]), (1, 2))

    def test_operation_conflict_is_not_replayed(self) -> None:
        from ....core.errors import OperationConflictError

        with tempfile.TemporaryDirectory() as directory:
            database = self._database(directory)
            app = IllusionApplication(database)
            app.execute(operation_id="op-1", user_id="u", payload=self._request())
            with self.assertRaises(OperationConflictError):
                app.execute(operation_id="op-1", user_id="u", payload=self._request(stone=9))

    def test_state_and_stats_are_feature_owned(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = self._database(directory)
            app = IllusionApplication(database)
            first = app.get_state("u", question_count=3, period="2026-09-12")
            again = app.get_state("u", question_count=3, period="2026-09-12")
            next_day = app.get_state("u", question_count=3, period="2026-09-13")

            self.assertEqual(first, again)
            self.assertEqual(next_day["period"], "2026-09-13")
            self.assertEqual(len(app.get_question_stats(period="2026-09-13", question_index=0, option_count=3)), 3)

    def test_clear_keeps_replay_receipts_and_allows_new_period_state(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = self._database(directory)
            app = IllusionApplication(database)
            request = self._request()
            first = app.execute(operation_id="op-1", user_id="u", payload=request)
            state = app.get_state("u", question_count=3, period=request["period"])
            cleared = app.execute(
                operation_id="clear-1",
                user_id="admin",
                payload={"action": "clear"},
            )
            replay = app.execute(operation_id="op-1", user_id="u", payload=request)
            second = app.execute(operation_id="op-2", user_id="u", payload=request)

            self.assertEqual(first.data["status"], "applied")
            self.assertEqual(state["today_choice"], "option")
            self.assertEqual(cleared.data["stats_reset"], 0)
            self.assertTrue(replay.replayed)
            self.assertEqual(second.data["status"], "applied")

    def test_legacy_state_imports_once_and_clear_does_not_restore_it(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = self._database(directory)
            app = IllusionApplication(database)
            loads = []

            def load_legacy():
                loads.append(True)
                return {"question_index": 2, "today_choice": "旧选择"}

            state = app.get_state(
                "u", question_count=3, period="2026-09-12", legacy_loader=load_legacy
            )
            again = app.get_state(
                "u", question_count=3, period="2026-09-12", legacy_loader=load_legacy
            )
            app.clear(operation_id="clear-1", user_id="admin", reset_stats=False)
            cleared = app.get_state(
                "u", question_count=3, period="2026-09-12", legacy_loader=load_legacy
            )

            self.assertEqual(state["today_choice"], "旧选择")
            self.assertEqual(again, state)
            self.assertEqual(len(loads), 1)
            self.assertIsNone(cleared["today_choice"])
            self.assertEqual(len(loads), 1)

    def test_legacy_stats_import_once_and_remain_cumulative_across_periods(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = self._database(directory)
            app = IllusionApplication(database)
            loads = []
            legacy = [[5, 2, 0]]

            def load_legacy():
                loads.append(True)
                return legacy

            first = app.get_question_stats(
                period="2026-09-12", question_index=0, option_count=3, legacy_loader=load_legacy
            )
            next_day = app.get_question_stats(
                period="2026-09-13", question_index=0, option_count=3, legacy_loader=load_legacy
            )
            app.clear(operation_id="reset-1", user_id="admin", reset_stats=True)
            reset = app.get_question_stats(
                period="2026-09-14", question_index=0, option_count=3, legacy_loader=load_legacy
            )

            self.assertEqual(first, [5, 2, 0])
            self.assertEqual(next_day, [5, 2, 0])
            self.assertEqual(reset, [0, 0, 0])
            self.assertEqual(len(loads), 1)


if __name__ == "__main__":
    unittest.main()
