from __future__ import annotations

import sqlite3
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier

from ....core.errors import DomainError
from ....infrastructure.database import DatabaseUnitOfWork
from ..legacy_crystal_repository import LegacyCrystalSqlRepository
from ..migrations import apply_impart_base_schema, apply_impart_exact_crystal_balances


class LegacyCrystalWriterTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.impart = Path(self.temp.name) / "impart.db"
        self.game = Path(self.temp.name) / "xiuxian.db"
        with DatabaseUnitOfWork(self.impart) as uow:
            apply_impart_base_schema(uow)
            uow.executemany(
                "INSERT INTO xiuxian_impart(user_id,stone_num) VALUES(?,?)",
                [("actor", 250), ("actor", 250), ("other", 300)],
            )
            apply_impart_exact_crystal_balances(uow)
        with DatabaseUnitOfWork(self.game) as uow:
            uow.execute(
                "CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,"
                "goods_type TEXT,goods_num INTEGER,bind_num INTEGER,UNIQUE(user_id,goods_id))"
            )
        self.repo = LegacyCrystalSqlRepository(self.impart, self.game)

    def snapshot(self):
        result = []
        for database in (self.game, self.impart):
            with DatabaseUnitOfWork(database, read_only=True) as uow:
                result.append(list(uow.connection.iterdump()))
        return result

    def balance(self, uid):
        with DatabaseUnitOfWork(self.impart, read_only=True) as uow:
            return [int(row["stone_num_exact"]) for row in uow.query_all(
                "SELECT stone_num_exact FROM xiuxian_impart WHERE user_id=? ORDER BY rowid", (uid,)
            )]

    def corrupt(self, *, real=False):
        with DatabaseUnitOfWork(self.impart) as uow:
            uow.execute("DROP TRIGGER impart_crystal_exact_write_guard")
            if real:
                uow.execute(
                    "UPDATE xiuxian_impart SET stone_num=CAST(? AS REAL),stone_num_exact=NULL "
                    "WHERE user_id='other'", (float(2**63),),
                )
            else:
                uow.execute(
                    "UPDATE xiuxian_impart SET stone_num=249,stone_num_exact='249' "
                    "WHERE rowid=(SELECT MIN(rowid) FROM xiuxian_impart WHERE user_id='actor')"
                )
            apply_impart_exact_crystal_balances(uow)

    def test_huge_all_grant_debit_replay_and_payload_conflict(self):
        huge = 10**45 + 123
        self.assertEqual(self.repo.change("all-grant", None, huge), 3)
        self.assertEqual(self.balance("actor"), [huge + 250] * 2)
        self.assertEqual(self.balance("other"), [huge + 300])
        once = self.snapshot()
        self.assertEqual(self.repo.change("all-grant", None, huge), 3)
        self.assertEqual(self.snapshot(), once)
        with self.assertRaises(DomainError) as caught:
            self.repo.change("all-grant", None, huge + 1)
        self.assertEqual(caught.exception.details["reason"], "operation_conflict")
        self.assertEqual(self.snapshot(), once)
        self.assertEqual(self.repo.change("actor-debit", "actor", -20), 2)
        self.assertEqual(self.balance("actor"), [huge + 230] * 2)

    def test_bulk_bad_precision_and_divergent_history_are_zero_write(self):
        for real, reason, subject in (
            (False, "impart_rows_stone_num", "actor"),
            (True, "crystal_precision_lost_real", "other"),
        ):
            with self.subTest(reason=reason):
                # Each branch starts from a separately migrated fixture.
                fixture = LegacyCrystalWriterTests()
                fixture.setUp()
                try:
                    fixture.corrupt(real=real)
                    before = fixture.snapshot()
                    with self.assertRaises(DomainError) as caught:
                        fixture.repo.change("bad-all", None, 10)
                    self.assertEqual(caught.exception.details, {"reason": reason, "subject": subject})
                    self.assertEqual(fixture.snapshot(), before)
                finally:
                    fixture.doCleanups()

    def test_overdraft_is_specific_and_zero_write(self):
        before = self.snapshot()
        with self.assertRaises(DomainError) as caught:
            self.repo.change("overdraft", "actor", -251)
        self.assertEqual(caught.exception.details, {"reason": "crystal_missing", "subject": "actor"})
        self.assertEqual(self.snapshot(), before)

    def test_conversion_mirrors_capacity_concurrency_and_replay(self):
        before = self.snapshot()
        refused = self.repo.convert("convert", "actor", 1)
        self.assertEqual(refused.status, "inventory_full")
        self.assertEqual(self.snapshot(), before)
        barrier = Barrier(3)

        def submit():
            barrier.wait(timeout=10)
            return self.repo.convert("convert", "actor", 100)

        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(submit) for _ in range(2)]
            barrier.wait(timeout=10)
            results = [future.result(timeout=20) for future in futures]
        self.assertTrue(all(r.succeeded and r.applied == 2 for r in results))
        self.assertEqual(self.balance("actor"), [0, 0])
        self.assertEqual(self.balance("other"), [300])
        with DatabaseUnitOfWork(self.game, read_only=True) as uow:
            self.assertEqual(tuple(uow.execute("SELECT goods_num,bind_num FROM back").fetchone()), (2, 2))
        once = self.snapshot()
        self.assertEqual(self.repo.convert("convert", "actor", 100).applied, 2)
        self.assertEqual(self.snapshot(), once)
        with self.assertRaises(DomainError):
            self.repo.convert("convert", "other", 100)
        self.assertEqual(self.snapshot(), once)

    def test_conversion_receipt_failure_rolls_back_both_databases(self):
        with DatabaseUnitOfWork(self.impart) as uow:
            uow.execute(
                "CREATE TRIGGER fail_legacy_receipt BEFORE INSERT ON impart_legacy_crystal_operations "
                "BEGIN SELECT RAISE(ABORT,'receipt failure'); END"
            )
        before = self.snapshot()
        with self.assertRaises(sqlite3.IntegrityError):
            self.repo.convert("failed-convert", "actor", 100)
        self.assertEqual(self.snapshot(), before)

    def test_conversion_divergent_mirrors_and_real_reject_without_item_grant(self):
        self.corrupt()
        before = self.snapshot()
        with self.assertRaises(DomainError) as caught:
            self.repo.convert("bad-convert", "actor", 100)
        self.assertEqual(caught.exception.details["reason"], "impart_rows_stone_num")
        self.assertEqual(self.snapshot(), before)
        self.corrupt(real=True)
        before = self.snapshot()
        with self.assertRaises(DomainError) as caught:
            self.repo.convert("real-convert", "other", 100)
        self.assertEqual(caught.exception.details["reason"], "crystal_precision_lost_real")
        self.assertEqual(self.snapshot(), before)


if __name__ == "__main__":
    unittest.main()
