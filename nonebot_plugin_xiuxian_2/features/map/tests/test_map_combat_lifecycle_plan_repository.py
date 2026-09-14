import json
import unittest
from tests.test_map_combat_lifecycle import MapCombatLifecycleTests
from tests.test_db_backend import db_backend
from ..repository import MapCombatLifecyclePlanSqlRepository


class CombatLifecyclePlanRepositoryTests(MapCombatLifecycleTests):
    def setUp(self):
        super().setUp()
        with db_backend.transaction(self.player) as conn:
            conn.execute("CREATE TABLE map_combat_plan_operations(operation_id TEXT PRIMARY KEY,user_id TEXT,task_id TEXT,payload TEXT,snapshot TEXT)")
        self.repository = MapCombatLifecyclePlanSqlRepository(self.player)

    def save_feature(self, operation_id="plan"):
        self.start(operation_id)
        pending = self.service.get_pending("u")
        plan = dict(pending.task)
        plan.update({"status":"planned","items":[],"rewards":[],"stone":0,"title":"战败","won":False})
        return self.repository.save_plan("plan-op-" + operation_id, "u", operation_id, plan)

    def test_success_and_duplicate(self):
        first = self.save_feature()
        duplicate = self.repository.save_plan("plan-op-plan", "u", "plan", first["task"])
        self.assertEqual(("applied", "duplicate"), (first["status"], duplicate["status"]))

    def test_missing_task_is_rejected_without_write(self):
        self.assertEqual("state_changed", self.repository.save_plan("missing", "u", "missing", {"task_id":"missing","status":"planned"})["status"])


if __name__ == "__main__": unittest.main()
