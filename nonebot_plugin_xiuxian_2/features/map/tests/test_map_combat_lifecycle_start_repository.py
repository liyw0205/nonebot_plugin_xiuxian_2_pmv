import unittest
from tests.test_db_backend import db_backend
from tests.test_map_combat_lifecycle import MapCombatLifecycleTests
from ..repository import MapCombatLifecycleStartSqlRepository


class CombatLifecycleStartRepositoryTests(MapCombatLifecycleTests):
    def setUp(self):
        super().setUp()
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE map_combat_start_operations(operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_status TEXT NOT NULL,stamina INTEGER NOT NULL DEFAULT 0,task_json TEXT NOT NULL DEFAULT '{}',created_at TEXT DEFAULT CURRENT_TIMESTAMP)")
        self.repository = MapCombatLifecycleStartSqlRepository(self.game, self.player)

    def start_feature(self, operation_id="feature-start", **changes):
        task = self.task(operation_id)
        task.update(changes.pop("task", {}))
        values = {"stamina": 12, "cost": 8, "position": self.position, "daily": self.daily, "limit": 7, "cooldown": "", "task": task}
        values.update(changes)
        return self.repository.start(operation_id, "u", values["stamina"], values["cost"], values["position"], values["daily"], values["limit"], values["cooldown"], values["task"])

    def test_success_and_replay_are_idempotent(self):
        first = self.start_feature()
        duplicate = self.start_feature()
        self.assertEqual(("applied", "duplicate"), (first["status"], duplicate["status"]))
        self.assertEqual(4, self.state()[0])

    def test_daily_limit_is_recorded_without_spending(self):
        result = self.start_feature("limited", limit=2)
        self.assertEqual("limit_reached", result["status"])
        self.assertEqual(12, self.state()[0])

    def test_conflicting_operation_does_not_change_state(self):
        self.start_feature("conflict")
        result = self.start_feature("conflict", task=self.task("conflict", enemy={"name": "other"}))
        self.assertEqual("duplicate", result["status"])
        self.assertEqual(4, self.state()[0])

    def test_operation_failure_rolls_back_new_path(self):
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TRIGGER fail_feature_start BEFORE INSERT ON map_combat_start_operations BEGIN SELECT RAISE(ABORT,'forced'); END")
        with self.assertRaises(Exception):
            self.start_feature("rollback-feature")
        self.assertEqual((12, "", ""), self.state())

    def test_start_operation_failure_rolls_back_cost_cooldown_and_task(self):
        self.test_operation_failure_rolls_back_new_path()


if __name__ == "__main__": unittest.main()
