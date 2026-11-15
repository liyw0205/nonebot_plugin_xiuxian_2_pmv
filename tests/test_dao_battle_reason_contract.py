from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.combat_settlement.application import CombatSettlementApplication
from nonebot_plugin_xiuxian_2.features.combat_settlement.migrations import (
    apply_dao_battle_operations,
    apply_dao_battle_record,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.infrastructure.database.ledger import OperationLedger


class DaoBattleReasonContractTests(unittest.TestCase):
    @staticmethod
    def _application(directory: str, *, include_target: bool = False) -> CombatSettlementApplication:
        root = Path(directory)
        game, player = root / "game.db", root / "player.db"
        with DatabaseUnitOfWork(game) as uow:
            uow.execute("CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY)")
            uow.execute("INSERT INTO user_xiuxian(user_id) VALUES ('challenger')")
            if include_target:
                uow.execute("INSERT INTO user_xiuxian(user_id) VALUES ('target')")
        with DatabaseUnitOfWork(player) as uow:
            uow.execute(
                "CREATE TABLE map_status (user_id TEXT PRIMARY KEY,realm TEXT,heaven TEXT,node_id TEXT)"
            )
            uow.executemany(
                "INSERT INTO map_status VALUES (?,?,?,?)",
                (("challenger", "凡界", "一重天", "n1"), ("target", "凡界", "一重天", "n1")),
            )
            apply_dao_battle_operations(uow)
            apply_dao_battle_record(uow)
            OperationLedger().ensure_schema(uow)
        return CombatSettlementApplication(game, player)

    def test_known_reason_and_subject_survive_ledger_replay(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            app = self._application(directory)
            kwargs = {
                "operation_id": "missing-target-op",
                "challenger_id": "challenger",
                "target_id": "target",
                "expected_position": {"realm": "凡界", "heaven": "一重天", "node_id": "n1"},
                "challenger_won": True,
            }

            first = app.settle_dao_battle(**kwargs)
            replay = app.settle_dao_battle(**kwargs)

            self.assertFalse(first.ok)
            self.assertEqual((first.code, first.data["reason"], first.data["subject"]),
                             ("user_missing", "user_missing", "target"))
            self.assertTrue(replay.replayed)
            self.assertEqual(replay.data, first.data)
            self.assertEqual(first.message, "未找到本次操作所需的角色资料，请确认角色已创建。")
            with DatabaseUnitOfWork(Path(directory) / "player.db", read_only=True) as uow:
                self.assertIsNone(uow.query_one("SELECT 1 FROM dao_record"))
                row = uow.query_one(
                    "SELECT status,result_json FROM operation_ledger WHERE operation_id=?",
                    ("missing-target-op",),
                )
            self.assertEqual(row["status"], "rejected")
            stored = json.loads(row["result_json"])
            self.assertEqual(stored["data"]["reason"], "user_missing")
            self.assertEqual(stored["data"]["subject"], "target")

    def test_operation_conflict_reports_actor_without_overwriting_first_receipt(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            app = self._application(directory, include_target=True)
            kwargs = {
                "operation_id": "conflicting-op",
                "challenger_id": "challenger",
                "target_id": "target",
                "expected_position": {"realm": "凡界", "heaven": "一重天", "node_id": "n1"},
                "challenger_won": True,
            }

            first = app.settle_dao_battle(**kwargs)
            conflict = app.settle_dao_battle(**{**kwargs, "challenger_won": False})

            self.assertTrue(first.ok)
            self.assertFalse(conflict.ok)
            self.assertEqual(
                (conflict.code, conflict.data["status"], conflict.data["reason"], conflict.data["subject"]),
                ("operation_conflict", "state_changed", "operation_conflict", "actor"),
            )
            self.assertEqual(conflict.message, "该操作号已用于不同内容的请求，请发送一条新命令。")
            with DatabaseUnitOfWork(Path(directory) / "player.db", read_only=True) as uow:
                ledger = uow.query_one(
                    "SELECT status,result_json FROM operation_ledger WHERE operation_id=? AND action=?",
                    ("conflicting-op", "combat.dao_battle"),
                )
                records = uow.query_all("SELECT user_id,total,win,lose FROM dao_record ORDER BY user_id")
            self.assertEqual(ledger["status"], "applied")
            self.assertEqual(json.loads(ledger["result_json"])["data"]["status"], "applied")
            self.assertEqual(records, [{"user_id": "challenger", "total": 1, "win": 1, "lose": 0}, {"user_id": "target", "total": 1, "win": 0, "lose": 1}])


if __name__ == "__main__":
    unittest.main()
