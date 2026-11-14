from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import sqlite3
import tempfile
from pathlib import Path
from threading import Barrier
import unittest

from ....core.errors import DomainError
from ....core.numeric import as_int_like
from ....infrastructure.database import DatabaseUnitOfWork
from ..application import ImpartApplication
from ..migrations import (
    apply_impart_base_schema,
    apply_impart_crystal_and_card_operations,
    apply_impart_draw_player_statistics,
    apply_impart_exact_crystal_balances,
)


class ImpartCrystalDrawStateIntegrityTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        root = Path(self.directory.name)
        self.game = root / "game.db"
        self.impart = root / "impart.db"
        self.player = root / "player.db"
        with DatabaseUnitOfWork(self.impart) as uow:
            apply_impart_base_schema(uow)
            apply_impart_crystal_and_card_operations(uow)
            apply_impart_exact_crystal_balances(uow)
            uow.execute(
                "INSERT INTO xiuxian_impart(user_id,stone_num,wish,exp_day) VALUES(?,?,?,?)",
                ("actor", 100, 7, 5),
            )
            uow.execute(
                "INSERT INTO xiuxian_impart(user_id,stone_num,wish,exp_day) VALUES(?,?,?,?)",
                ("other", 777, 2, 9),
            )
        with DatabaseUnitOfWork(self.player) as uow:
            apply_impart_draw_player_statistics(uow)
        self.app = ImpartApplication(
            self.game,
            impart_database=self.impart,
            player_database=self.player,
        )
        self.definitions = {"A": {"type": "impart_atk_per", "vale": 0.1}}

    def tearDown(self) -> None:
        self.directory.cleanup()

    def _draw(
        self,
        operation_id: str,
        *,
        user_id: str = "actor",
        expected_stone: int = 100,
        expected_wish: int = 7,
        cost: int = 20,
        requested_pulls: int = 2,
    ):
        return self.app.crystal_draw(
            operation_id=operation_id,
            user_id=user_id,
            expected_stone=expected_stone,
            expected_wish=expected_wish,
            cost=cost,
            new_wish=9,
            exp_minutes=60,
            cards=("A",),
            card_definitions=self.definitions,
            statistics_user_id=user_id,
            statistics={
                "传承祈愿": cost,
                "传承祈愿次数": requested_pulls,
                "思恋结晶消耗": cost,
                "虚神界时间获取": 60,
                "传承保底次数": 0,
            },
            requested_pulls=requested_pulls,
        )

    def _rows(self, database: Path, sql: str, params=()):
        with DatabaseUnitOfWork(database, read_only=True) as uow:
            return [tuple(row.values()) for row in uow.query_all(sql, params)]

    def _state(self, user_id: str):
        return self._rows(
            self.impart,
            "SELECT rowid,stone_num,wish,exp_day FROM xiuxian_impart "
            "WHERE user_id=? ORDER BY rowid",
            (user_id,),
        )

    def _snapshot(self):
        return (
            self._rows(self.impart, "SELECT * FROM xiuxian_impart ORDER BY rowid"),
            self._rows(self.impart, "SELECT * FROM impart_cards ORDER BY user_id,card_name"),
            self._rows(self.impart, "SELECT * FROM impart_crystal_draw_operations ORDER BY operation_id"),
            self._rows(self.player, "SELECT * FROM statistics ORDER BY user_id"),
        )

    def _add_mirror(self, user_id: str, stone_num: int | str | None = None) -> None:
        with DatabaseUnitOfWork(self.impart) as uow:
            source = uow.query_one(
                "SELECT stone_num,stone_num_exact,wish,exp_day FROM xiuxian_impart WHERE user_id=? ORDER BY rowid LIMIT 1",
                (user_id,),
            )
            if source is None:
                self.fail(f"missing fixture state for {user_id}")
            uow.execute(
                "INSERT INTO xiuxian_impart(user_id,stone_num,stone_num_exact,"
                "stone_num_precision_state,wish,exp_day) VALUES(?,?,?,'exact',?,?)",
                (
                    user_id,
                    source["stone_num"] if stone_num is None else stone_num,
                    str(source["stone_num_exact"] if stone_num is None else stone_num),
                    source["wish"],
                    source["exp_day"],
                ),
            )

    def test_consistent_legacy_mirrors_settle_only_the_effective_row(self) -> None:
        self._add_mirror("actor", stone_num="100")
        before = self._state("actor")
        other_before = self._state("other")
        self.assertEqual(len(before), 2)
        self.assertEqual(before[0][1:], before[1][1:])

        result = self._draw("mirror-settlement")

        self.assertEqual((result.status, result.stone_num), ("applied", 80))
        after = self._state("actor")
        self.assertEqual(len(after), 2)
        self.assertEqual([row[1:] for row in after], [(80, 9, 65), (100, 7, 5)])
        self.assertEqual(self._state("other"), other_before)
        self.assertEqual(len(self._rows(self.impart, "SELECT * FROM impart_crystal_draw_operations")), 1)
        self.assertEqual(self._rows(self.impart, "SELECT user_id,card_name,quantity FROM impart_cards"),
                         [("actor", "A", 1)])

        after_once = self._snapshot()
        replay = self._draw("mirror-settlement")
        self.assertEqual(replay.status, "duplicate")
        self.assertEqual(self._snapshot(), after_once)

    def test_new_player_state_starts_with_exact_zero_balance(self) -> None:
        state = self.app.state("new-player", ensure=True)

        self.assertEqual(state["stone_num"], 0)
        self.assertEqual(
            self._rows(
                self.impart,
                "SELECT stone_num_exact,stone_num_precision_state "
                "FROM xiuxian_impart WHERE user_id='new-player'",
            ),
            [("0", "exact")],
        )

    def test_state_read_rejects_divergent_historical_crystal_balances(self) -> None:
        self._add_mirror("actor", stone_num=99)

        with self.assertRaises(DomainError) as caught:
            self.app.state("actor", validate_crystal_consistency=True)

        self.assertEqual(caught.exception.code, "impart_rows_inconsistent")
        self.assertEqual(
            caught.exception.details,
            {"reason": "impart_rows_stone_num", "subject": "actor"},
        )

    def test_same_operation_rejects_changed_payload_or_character_without_writes(self) -> None:
        self.assertEqual(self._draw("payload-conflict").status, "applied")
        after_once = self._snapshot()

        changed_count = self._draw("payload-conflict", cost=30, requested_pulls=3)
        changed_character = self._draw(
            "payload-conflict", user_id="other", expected_stone=777, expected_wish=2
        )

        self.assertEqual((changed_count.status, changed_character.status),
                         ("operation_conflict", "operation_conflict"))
        self.assertEqual(self._snapshot(), after_once)

    def test_concurrent_same_request_commits_once_and_returns_replay(self) -> None:
        barrier = Barrier(3)

        def submit():
            barrier.wait(timeout=10)
            return self._draw("concurrent-prayer")

        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [executor.submit(submit) for _ in range(2)]
            barrier.wait(timeout=10)
            results = [future.result(timeout=20) for future in futures]

        self.assertCountEqual([result.status for result in results], ["applied", "duplicate"])
        self.assertEqual(self._state("actor")[0][1:], (80, 9, 65))
        self.assertEqual(len(self._rows(self.impart, "SELECT * FROM impart_crystal_draw_operations")), 1)
        stats = self._rows(
            self.player,
            'SELECT "传承祈愿","传承祈愿次数","思恋结晶消耗" FROM statistics WHERE user_id=?',
            ("actor",),
        )
        self.assertEqual(stats, [(20, 2, 20)])

    def test_oversized_text_balance_remains_exact_through_settlement_and_receipt(self) -> None:
        huge = 10**45 + 123
        with DatabaseUnitOfWork(self.impart) as uow:
            uow.execute(
                "UPDATE xiuxian_impart SET stone_num=?,stone_num_exact=?,"
                "stone_num_precision_state='exact' WHERE user_id='actor'",
                (huge, str(huge)),
            )
        self._add_mirror("actor")

        result = self._draw("large-prayer", expected_stone=huge)
        expected = huge - 20

        self.assertEqual(result.status, "applied")
        self.assertEqual(as_int_like(result.stone_num), expected)
        exact_rows = self._rows(
            self.impart,
            "SELECT stone_num_exact FROM xiuxian_impart WHERE user_id='actor' ORDER BY rowid",
        )
        self.assertEqual([int(row[0]) for row in exact_rows], [expected, huge])
        receipt = self._rows(
            self.impart,
            "SELECT stone_num_exact FROM impart_crystal_draw_operations WHERE operation_id=?",
            ("large-prayer",),
        )
        self.assertEqual(as_int_like(receipt[0][0]), expected)
        after_once = self._snapshot()
        replay = self._draw("large-prayer", expected_stone=huge)
        self.assertEqual((replay.status, as_int_like(replay.stone_num)), ("duplicate", expected))
        self.assertEqual(self._snapshot(), after_once)

    def test_different_concurrent_requests_reject_stale_snapshot_without_second_charge(self):
        barrier = Barrier(3)

        def submit(key):
            barrier.wait(timeout=10)
            return self._draw(key)

        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(submit, key) for key in ("parallel-a", "parallel-b")]
            barrier.wait(timeout=10)
            results = [future.result(timeout=20) for future in futures]
        self.assertCountEqual([r.status for r in results], ["applied", "state_changed"])
        rejected = next(r for r in results if r.status == "state_changed")
        self.assertEqual((rejected.reason, rejected.subject), ("crystal_changed", "actor"))
        self.assertEqual(self._state("actor")[0][1:], (80, 9, 65))
        self.assertEqual(len(self._rows(self.impart, "SELECT * FROM impart_crystal_draw_operations")), 1)

    def test_invalid_exact_receipt_never_replays_as_zero_balance_success(self):
        self.assertEqual(self._draw("bad-shadow").status, "applied")
        with DatabaseUnitOfWork(self.impart) as uow:
            uow.execute("UPDATE impart_crystal_draw_operations SET stone_num_exact='1.5' WHERE operation_id='bad-shadow'")
        before = self._snapshot()
        for replay in (
            lambda: self._draw("bad-shadow"),
            lambda: self.app.crystal_draw_result("bad-shadow", "actor", 20, 2),
        ):
            with self.subTest(route=replay):
                with self.assertRaises(DomainError) as caught:
                    replay()
                self.assertEqual(caught.exception.details, {"reason": "crystal_invalid_numeric_value", "subject": "actor"})
                self.assertEqual(self._snapshot(), before)

    def test_unrecoverable_legacy_real_is_rejected_without_any_write(self) -> None:
        with DatabaseUnitOfWork(self.impart) as uow:
            uow.execute("DROP TRIGGER impart_crystal_exact_write_guard")
            uow.execute(
                "UPDATE xiuxian_impart SET stone_num=CAST(? AS REAL),"
                "stone_num_exact=NULL,stone_num_precision_state='exact' "
                "WHERE user_id='actor'",
                (float(2**63),),
            )
            apply_impart_exact_crystal_balances(uow)
        before = self._snapshot()

        with self.assertRaises(DomainError) as caught:
            self._draw("unrecoverable-real")

        self.assertEqual(
            caught.exception.details,
            {"reason": "crystal_precision_lost_real", "subject": "actor"},
        )
        self.assertEqual(self._snapshot(), before)
        self.assertIsNone(
            self.app.crystal_draw_result("unrecoverable-real", "actor", 20, 2)
        )

    def test_divergent_history_targets_effective_row_and_keeps_audit_rejection(self) -> None:
        self._add_mirror("actor", stone_num=99)
        before = self._snapshot()

        result = self._draw("divergent-prayer")
        self.assertEqual((result.status, result.stone_num), ("applied", 80))
        after = self._state("actor")
        self.assertEqual([row[1:] for row in after], [(80, 9, 65), (99, 7, 5)])
        self.assertNotEqual(self._snapshot(), before)
        with self.assertRaises(DomainError) as caught:
            self.app.state("actor", validate_crystal_consistency=True)
        self.assertEqual(caught.exception.code, "impart_rows_inconsistent")
        self.assertEqual(caught.exception.details,
                         {"reason": "impart_rows_stone_num", "subject": "actor"})

    def test_history_audit_remains_specific_for_divergent_rows(self) -> None:
        self._add_mirror("actor", stone_num=99)
        with self.assertRaises(DomainError) as caught:
            self.app.state("actor", validate_crystal_consistency=True)
        self.assertEqual(caught.exception.code, "impart_rows_inconsistent")
        self.assertEqual(caught.exception.details,
                         {"reason": "impart_rows_stone_num", "subject": "actor"})

    def test_receipt_write_failure_rolls_back_balance_cards_and_statistics(self) -> None:
        with DatabaseUnitOfWork(self.impart) as uow:
            uow.execute(
                "CREATE TRIGGER fail_crystal_draw_receipt BEFORE INSERT "
                "ON impart_crystal_draw_operations BEGIN SELECT RAISE(ABORT,'receipt failed'); END"
            )
        before = self._snapshot()

        with self.assertRaises(sqlite3.IntegrityError):
            self._draw("receipt-failure")

        self.assertEqual(self._snapshot(), before)
        self.assertEqual(self.app.crystal_draw_result("receipt-failure", "actor", 20, 2), None)


if __name__ == "__main__":
    unittest.main()
