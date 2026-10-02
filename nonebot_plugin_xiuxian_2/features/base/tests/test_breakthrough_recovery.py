from __future__ import annotations

import json
import ast
import multiprocessing
import os
import sqlite3
import sys
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from threading import RLock
from unittest.mock import Mock, patch

from ....compatibility.base_breakthrough_effects import LegacyDirectBreakthroughEffects, _power_for, _cap_for
from ....infrastructure.database import DatabaseUnitOfWork, OutboxStore
from ...game_events.migrations import apply_game_event_statistics_player
from ..application import BaseApplication
from ..breakthrough_repository import BaseDirectBreakthroughSqlRepository
from ..migrations import (
    apply_base_direct_breakthrough_operations, apply_base_direct_breakthrough_plans,
    apply_base_direct_breakthrough_player,
)


class DirectBreakthroughRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.game, self.player = self.root / "game.db", self.root / "player.db"
        with DatabaseUnitOfWork(self.game, immediate=True) as uow:
            uow.execute(
                "CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,level TEXT,exp INTEGER,hp INTEGER,mp INTEGER,"
                "atk INTEGER,power INTEGER,level_up_rate INTEGER,level_up_cd TEXT,root_type TEXT,user_name TEXT)"
            )
            uow.executemany(
                "INSERT INTO user_xiuxian VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                [(user, "before", 10000, 5000, 10000, 1000, 10000, 5, None, "root", user) for user in ("a", "m")],
            )
            apply_base_direct_breakthrough_operations(uow)
            OutboxStore().ensure_schema(uow)
            apply_base_direct_breakthrough_plans(uow)
            uow.execute(
                "CREATE INDEX IF NOT EXISTS direct_breakthrough_pending_outbox "
                "ON domain_outbox(attempts,created_at,event_id) "
                "WHERE event_type='base.direct_breakthrough.effects' AND status='pending'"
            )
        with DatabaseUnitOfWork(self.player, immediate=True) as uow:
            apply_base_direct_breakthrough_player(uow)
            apply_game_event_statistics_player(uow)
            uow.executemany(
                "INSERT INTO mentor(user_id,mentor_id,apprentice_ids,bind_time,breakthrough_reward_count,mentor_history) VALUES(?,?,?,?,?,?)",
                [("a", "m", "[]", "binding", 26, "[]"), ("m", None, '["a"]', "", 0, "[]")],
            )
            uow.executemany(
                "INSERT INTO partner(user_id,partner_id,bind_time,affection) VALUES(?,?,?,?)",
                [("a", "m", "pair", 1000), ("m", "a", "pair", 1000)],
            )
        self.effects = LegacyDirectBreakthroughEffects(
            self.game, self.player, players_dir=self.root / "players", power_for=lambda row, exp: exp * 2,
            cap_for=lambda row: 20000, refresh_titles=Mock(),
        )
        self.app = BaseApplication(self.game, self.player, direct_breakthrough_effects=self.effects)
        self.repo = self.app._direct_breakthrough_repository
        self.expected = self.row(self.game, "user_xiuxian", "user_id='a'")
        self.factory = Mock(side_effect=self.plan)
        self.relation_plans = []

    def row(self, database, table, where="1=1"):
        with DatabaseUnitOfWork(database, read_only=True) as uow:
            return uow.query_one(f"SELECT * FROM {table} WHERE {where}")

    def relation(self, kind="mentor"):
        return {
            "kind": kind, "source_id": "a", "target_id": "m", "bind_time": "binding" if kind == "mentor" else "pair",
            "target_bind_time": "pair", "reward_exp": 100, "max_exp": 20000,
            "reward_limit": 27, "history_limit": 50, "occurred_at": "2026-10-02 12:00:00",
            "source_description": "source reward", "target_description": "target reward", "message": " reward",
        }

    def plan(self, uow, snapshot, occurred_at):
        return {
            "core": {
                "outcome": "success", "expected_level": "before", "target_level": "after", "expected_exp": 10000,
                "expected_hp": 5000, "expected_mp": 10000, "expected_rate": 5, "root_rate": 1, "level_spend": 2,
            },
            "message": "success",
            "effects": {"statistics": {"突破次数": 1, "突破成功": 1}, "log_message": "success", "relations": self.relation_plans},
        }

    def resolve(self, *, repository_only=False, operation="root"):
        if repository_only:
            return self.repo.resolve(operation, "a", expected=self.expected, plan_factory=self.factory)
        return self.app.resolve_direct_breakthrough(operation_id=operation, user_id="a", expected=self.expected, plan_factory=self.factory)

    def test_core_crash_recovery_does_not_resample(self):
        self.resolve(repository_only=True)
        self.assertEqual(self.row(self.game, "domain_outbox")["status"], "pending")
        result = self.resolve()
        self.assertEqual(result.status, "duplicate")
        self.factory.assert_called_once()
        self.assertEqual(self.row(self.game, "domain_outbox")["status"], "sent")
        self.assertEqual(self.row(self.player, "statistics")["突破次数"], 1)
        self.resolve()
        logs = list((self.root / "players/a/logs").glob("*.log"))
        self.assertEqual(len(json.loads(logs[0].read_text())), 1)

    def test_concurrent_same_message_freezes_one_plan(self):
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(lambda _: self.resolve(repository_only=True), range(2)))
        self.assertEqual(sorted(result.status for result in results), ["applied", "duplicate"])
        self.factory.assert_called_once()

    def test_outbox_failure_rolls_back_plan_and_core(self):
        with DatabaseUnitOfWork(self.game, immediate=True) as uow:
            uow.execute("CREATE TRIGGER fail BEFORE INSERT ON domain_outbox BEGIN SELECT RAISE(ABORT,'outbox failed'); END")
        with self.assertRaises(sqlite3.IntegrityError):
            self.resolve(repository_only=True)
        self.assertEqual(self.row(self.game, "user_xiuxian", "user_id='a'")["level"], "before")
        self.assertIsNone(self.row(self.game, "direct_breakthrough_plans"))

    def test_missing_outbox_schema_prevents_random_draw(self):
        with DatabaseUnitOfWork(self.game, immediate=True) as uow:
            uow.execute("DROP TABLE domain_outbox")
        self.assertEqual(self.resolve().status, "schema_missing")
        self.factory.assert_not_called()

    def test_pending_batch_uses_partial_index_instead_of_sorting_all_receipts(self):
        with DatabaseUnitOfWork(self.game, read_only=True) as uow:
            plan = uow.query_all(
                "EXPLAIN QUERY PLAN SELECT payload_json FROM domain_outbox "
                "WHERE event_type=? AND status='pending' ORDER BY attempts,created_at,event_id LIMIT ?",
                (self.repo.EVENT_TYPE, 5),
            )
        self.assertTrue(any("direct_breakthrough_pending_outbox" in row["detail"] for row in plan))
        self.assertFalse(any("TEMP B-TREE" in row["detail"] for row in plan))

    def test_historical_receipt_does_not_invent_effects(self):
        self.repo.apply("old", "a", "success", "before", "after", 10000, 5000, 10000, 5, root_rate=1, level_spend=2)
        result = self.app.direct_breakthrough_replay("old", "a")
        self.assertEqual(result.effects_event_id, "")
        self.assertIsNone(self.row(self.game, "domain_outbox"))
        self.assertEqual(self.app.direct_breakthrough_replay("old", "m").status, "operation_conflict")

    def test_player_prepare_survives_game_reward_rollback(self):
        self.relation_plans = [self.relation()]
        with patch.object(self.effects.relations, "_grant", side_effect=RuntimeError("crash after prepare")):
            self.resolve()
        self.assertEqual(self.row(self.player, "mentor", "user_id='a'")["breakthrough_reward_count"], 27)
        self.assertEqual(self.row(self.player, "direct_breakthrough_relation_receipts")["status"], "prepared")
        self.assertEqual(self.row(self.game, "user_xiuxian", "user_id='m'")["exp"], 10000)
        self.app.direct_breakthrough_replay("root", "a")
        self.app.direct_breakthrough_replay("root", "a")
        self.assertEqual(self.row(self.game, "user_xiuxian", "user_id='m'")["exp"], 10100)
        self.assertEqual(self.row(self.player, "mentor", "user_id='a'")["breakthrough_reward_count"], 27)
        self.assertEqual(self.row(self.player, "statistics", "user_id='m'")["师父突破返修"], 100)

    def test_game_commit_then_rebind_does_not_restore_old_count(self):
        self.relation_plans = [self.relation()]
        with patch.object(self.effects.relations, "_finalize", side_effect=RuntimeError("crash before finalize")):
            self.resolve()
        self.assertEqual(self.row(self.game, "user_xiuxian", "user_id='m'")["exp"], 10100)
        with DatabaseUnitOfWork(self.player, immediate=True) as uow:
            uow.execute("UPDATE mentor SET mentor_id='other',bind_time='new',breakthrough_reward_count=0 WHERE user_id='a'")
        self.app.direct_breakthrough_replay("root", "a")
        self.assertEqual(self.row(self.player, "mentor", "user_id='a'")["breakthrough_reward_count"], 0)
        self.assertEqual(self.row(self.game, "user_xiuxian", "user_id='m'")["exp"], 10100)
        history = json.loads(self.row(self.player, "mentor", "user_id='a'")["mentor_history"])
        self.assertEqual(len(history), 1)
        self.assertEqual(history[0]["time"], self.relation()["occurred_at"])

    def test_finalize_then_partial_log_failure_replays_without_duplicate_effects(self):
        self.relation_plans = [self.relation()]
        original = self.effects._log
        def fail_target(user_id, message, event_id, occurred_at):
            if event_id.endswith(":mentor:target:log"):
                raise OSError("log unavailable")
            original(user_id, message, event_id, occurred_at)
        with patch.object(self.effects, "_log", side_effect=fail_target):
            self.resolve()
        self.assertEqual(self.row(self.player, "direct_breakthrough_relation_receipts")["status"], "applied")
        self.app.direct_breakthrough_replay("root", "a")
        self.assertEqual(self.row(self.game, "user_xiuxian", "user_id='m'")["exp"], 10100)
        for user_id, count in (("a", 2), ("m", 1)):
            logs = [item for path in (self.root / f"players/{user_id}/logs").glob("*.log") for item in json.loads(path.read_text())]
            self.assertEqual(len(logs), count)
        self.assertEqual(self.row(self.player, "statistics", "user_id='m'")["师父突破返修"], 100)

    def test_relation_change_before_prepare_is_a_durable_skip(self):
        self.relation_plans = [self.relation()]
        self.resolve(repository_only=True)
        with DatabaseUnitOfWork(self.player, immediate=True) as uow:
            uow.execute("UPDATE mentor SET bind_time='new' WHERE user_id='a'")
        self.app.direct_breakthrough_replay("root", "a")
        self.assertEqual(self.row(self.player, "direct_breakthrough_relation_receipts")["status"], "skipped")
        self.assertEqual(self.row(self.game, "user_xiuxian", "user_id='m'")["exp"], 10000)

    def test_exp_cap_conflict_retains_prepared_entitlement(self):
        self.relation_plans = [self.relation()]
        self.resolve(repository_only=True)
        with DatabaseUnitOfWork(self.game, immediate=True) as uow:
            uow.execute("UPDATE user_xiuxian SET exp=20000 WHERE user_id='m'")
        self.app.direct_breakthrough_replay("root", "a")
        self.assertEqual(self.row(self.player, "direct_breakthrough_relation_receipts")["status"], "prepared")
        self.assertEqual(self.row(self.game, "domain_outbox")["status"], "pending")
        with DatabaseUnitOfWork(self.game, immediate=True) as uow:
            uow.execute("UPDATE user_xiuxian SET exp=15000 WHERE user_id='m'")
        self.app.direct_breakthrough_replay("root", "a")
        self.assertEqual(self.row(self.game, "user_xiuxian", "user_id='m'")["exp"], 15100)
        self.assertEqual(self.row(self.game, "user_xiuxian", "user_id='m'")["power"], 30200)

    def test_finalize_failure_rolls_back_statistics_and_history_together(self):
        self.relation_plans = [self.relation()]
        with DatabaseUnitOfWork(self.player, immediate=True) as uow:
            uow.execute("CREATE TRIGGER fail BEFORE UPDATE OF status ON direct_breakthrough_relation_receipts BEGIN SELECT RAISE(ABORT,'finalize failed'); END")
        self.resolve()
        self.assertIsNone(self.row(self.player, "statistics", "user_id='m'"))
        self.assertEqual(self.row(self.player, "mentor", "user_id='a'")["mentor_history"], "[]")
        with DatabaseUnitOfWork(self.player, immediate=True) as uow:
            uow.execute("DROP TRIGGER fail")
        self.app.resume_pending_direct_breakthroughs()
        self.assertEqual(self.row(self.player, "statistics", "user_id='m'")["师父突破返修"], 100)

    def test_partner_and_mentor_same_recipient_partial_replay(self):
        self.relation_plans = [self.relation("partner"), {**self.relation(), "depends_on_partner": True}]
        original = self.effects.relations._grant
        def fail_mentor(game, event_id, payload):
            if payload["kind"] == "mentor":
                raise RuntimeError("mentor unavailable")
            original(game, event_id, payload)
        with patch.object(self.effects.relations, "_grant", side_effect=fail_mentor):
            self.resolve()
        self.assertEqual(self.row(self.game, "user_xiuxian", "user_id='m'")["exp"], 10100)
        self.app.resume_pending_direct_breakthroughs()
        self.assertEqual(self.row(self.game, "user_xiuxian", "user_id='m'")["exp"], 10200)
        self.assertEqual(self.row(self.game, "domain_outbox")["status"], "sent")

    def test_real_process_exit_at_each_cross_file_commit_boundary(self):
        self.relation_plans = [self.relation()]
        self.resolve(repository_only=True)
        ctx = multiprocessing.get_context("fork")
        for method, expected_exp in (("_grant", 10000), ("_finalize", 10100)):
            def crash():
                setattr(self.effects.relations, method, lambda *args: os._exit(71))
                self.app.direct_breakthrough_replay("root", "a")
            process = ctx.Process(target=crash)
            process.start()
            process.join(10)
            if process.is_alive():
                process.terminate()
                process.join()
                self.fail("crash probe timed out")
            self.assertEqual(process.exitcode, 71)
            self.assertEqual(self.row(self.game, "user_xiuxian", "user_id='m'")["exp"], expected_exp)
            self.assertEqual(self.row(self.player, "mentor", "user_id='a'")["breakthrough_reward_count"], 27)
            self.assertEqual(self.row(self.player, "direct_breakthrough_relation_receipts")["status"], "prepared")
        self.app.direct_breakthrough_replay("root", "a")
        self.assertEqual(self.row(self.player, "direct_breakthrough_relation_receipts")["status"], "applied")

    def test_large_statistics_increment_is_replay_safe(self):
        kwargs = dict(event_id="huge", user_id="a", increments={"突破损失修为": 10**25 + 12345}, occurred_at="2026-10-02")
        self.assertTrue(self.effects.statistics.record(**kwargs))
        self.assertFalse(self.effects.statistics.record(**kwargs))

    def test_real_planner_freezes_partner_then_mentor_amount_and_no_trigger(self):
        random = Mock(return_value=1)
        prefix = "nonebot_plugin_xiuxian_2.xiuxian."
        modules = {
            prefix + "xiuxian_buff.partner": SimpleNamespace(
                MENTOR_BREAKTHROUGH_REWARD_LIMIT=27, MENTOR_HISTORY_LIMIT=50,
                _mentor_breakthrough_reward_rate=lambda _: 0.01, runtime_random=SimpleNamespace(randint=random),
            ),
            prefix + "xiuxian_utils.numeric_bind": SimpleNamespace(percent_exp_reward=lambda *args, **kwargs: 100),
            prefix + "xiuxian_utils.utils": SimpleNamespace(number_to=str),
        }
        with patch.dict(sys.modules, modules), DatabaseUnitOfWork(self.game, immediate=True) as uow:
            plans = self.effects.plan_relations(uow, self.expected, "after", "2026-10-02 12:00:00")
            self.assertEqual([plan["kind"] for plan in plans], ["partner", "mentor"])
            self.assertEqual([plan["reward_exp"] for plan in plans], [100, 101])
            self.assertTrue(plans[1]["depends_on_partner"])
            random.return_value = 100
            plans = self.effects.plan_relations(uow, self.expected, "after", "2026-10-02 12:00:00")
            self.assertEqual([plan["kind"] for plan in plans], ["mentor"])
            self.assertEqual(plans[0]["reward_exp"], 100)

    def test_default_power_and_cap_read_only_configuration_not_database_managers(self):
        prefix = "nonebot_plugin_xiuxian_2.xiuxian."
        fate = Mock(return_value=3)
        modules = {
            prefix + "xiuxian_utils.data_source": SimpleNamespace(jsondata=SimpleNamespace(
                level_data=lambda: {"before": {"spend": 2}, "after": {"power": 10000}},
                root_data=lambda: {"root": {"type_speeds": 1.5}, "命运道果": {"type_speeds": 1}, "永恒道果": {"type_speeds": 2}},
            )),
            prefix + "xiuxian_utils.numeric_bind": SimpleNamespace(compute_fate_root_rate=fate),
            prefix + "xiuxian_config": SimpleNamespace(XiuConfig=lambda: SimpleNamespace(level=["before", "after"], closing_exp_upper_limit=2)),
        }
        with patch.dict(sys.modules, modules):
            self.assertEqual(_power_for(self.expected, 100), 300)
            self.assertEqual(_cap_for(self.expected), 20000)
            self.assertEqual(_cap_for({**self.expected, "level": "after"}), 0)
            self.assertEqual(_power_for({**self.expected, "root_type": "命运道果", "root_level": 9}, 100), 600)
            fate.assert_called_once_with(9, 2, 1)

    def test_reconcile_dispatches_committed_core_without_a_message_retry(self):
        from ....infrastructure.database.reconcile import ReconcileService
        from ....infrastructure.database import OperationLedger

        self.relation_plans = [self.relation()]
        self.resolve(repository_only=True)
        with DatabaseUnitOfWork(self.game, immediate=True) as uow:
            OperationLedger().ensure_schema(uow)
        with DatabaseUnitOfWork(self.game) as uow:
            report = ReconcileService().run(uow, handlers={self.repo.EVENT_TYPE: self.app.reconcile_direct_breakthrough_event})
        self.assertTrue(report.clean)
        self.assertEqual(self.row(self.game, "domain_outbox")["status"], "sent")
        self.assertEqual(self.row(self.game, "user_xiuxian", "user_id='m'")["exp"], 10100)
        self.assertEqual(self.app.direct_breakthrough_replay("root", "a").message, "success reward")

    def test_failed_oldest_batch_does_not_starve_later_events(self):
        for index in range(6):
            with DatabaseUnitOfWork(self.game, immediate=True) as uow:
                uow.execute("UPDATE user_xiuxian SET level='before',level_up_cd=NULL,level_up_rate=5 WHERE user_id='a'")
            self.assertTrue(self.resolve(repository_only=True, operation=f"op-{index}").applied)
        actual = self.effects.on_settled
        def deferred(*, payload, event_id):
            if payload["operation_id"] != "op-5":
                raise RuntimeError("deferred")
            return actual(payload=payload, event_id=event_id)
        with patch.object(self.effects, "on_settled", side_effect=deferred) as effect:
            self.app.resume_pending_direct_breakthroughs(limit=500)
            self.assertEqual(effect.call_count, 5)
            self.app.resume_pending_direct_breakthroughs()
            self.assertEqual(effect.call_count, 10)
        self.assertEqual(self.row(self.game, "domain_outbox", "event_id='direct-breakthrough:op-5:effects'")["status"], "sent")

    def test_missing_player_schema_keeps_core_pending_without_ddl(self):
        self.relation_plans = [self.relation()]
        with DatabaseUnitOfWork(self.player, immediate=True) as uow:
            uow.execute("DROP TABLE direct_breakthrough_relation_receipts")
        self.resolve()
        self.assertEqual(self.row(self.game, "domain_outbox")["status"], "pending")
        self.assertEqual(self.row(self.player, "mentor", "user_id='a'")["breakthrough_reward_count"], 26)
        with DatabaseUnitOfWork(self.player, read_only=True) as uow:
            self.assertIsNone(uow.query_one("SELECT name FROM sqlite_master WHERE name='direct_breakthrough_relation_receipts'"))

    def test_legacy_mentor_service_sees_reserved_count_and_cannot_overpay(self):
        from tests.test_db_backend import db_backend

        self.relation_plans = [self.relation()]
        with patch.object(self.effects.relations, "_grant", side_effect=RuntimeError("crash")):
            self.resolve()
        namespace = dict(
            __name__=__name__, json=json, datetime=datetime, Path=Path, closing=closing,
            RLock=RLock, dataclass=dataclass, db_backend=db_backend,
            _as_int_like_num=int, _sql_num_nonneg=lambda value: max(int(value), 0),
        )
        root = Path(__file__).parents[3] / "xiuxian/xiuxian_buff"
        # Execute the real attached legacy transaction with isolated numeric ports.
        for filename, select in (
            ("relation_transaction_utils.py", lambda node: isinstance(node, ast.FunctionDef)),
            ("transaction_service.py", lambda node: isinstance(node, ast.ClassDef) and node.name in {"MentorBreakthroughRewardResult", "MentorBreakthroughRewardService"}),
        ):
            source = root / filename
            selected = [node for node in ast.parse(source.read_text()).body if select(node)]
            exec(compile(ast.Module(body=selected, type_ignores=[]), str(source), "exec"), namespace)
        legacy = namespace["MentorBreakthroughRewardService"](self.game, self.player)
        result = legacy.apply(
            "legacy", "m", "a", "after", "legacy", expected_mentor_exp=10000,
            expected_apprentice_exp=10000, expected_reward_count=26, reward_limit=27,
            reward_exp=100, max_mentor_exp=20000, mentor_power=20200, history_limit=50,
            mentor_desc="mentor", apprentice_desc="apprentice",
        )
        self.assertEqual(result.status, "state_changed")
        self.assertEqual(self.row(self.game, "user_xiuxian", "user_id='m'")["exp"], 10000)
        self.app.direct_breakthrough_replay("root", "a")
        self.assertEqual(self.row(self.game, "user_xiuxian", "user_id='m'")["exp"], 10100)

    def test_expired_rebind_check_never_writes_a_stale_mentor_snapshot(self):
        source = Path(__file__).parents[3] / "xiuxian/xiuxian_buff/partner.py"
        node = next(node for node in ast.parse(source.read_text()).body if isinstance(node, ast.FunctionDef) and node.name == "_get_pair_rebind_remaining")
        save = Mock(side_effect=AssertionError("read must not write"))
        namespace = dict(
            load_mentor=lambda _: {"mentor_rebind_cd": {"m": "2026-10-01 12:00:00"}},
            _normalize_dict=lambda value: value, _parse_datetime=datetime.fromisoformat,
            runtime_clock=SimpleNamespace(now=lambda: datetime(2026, 10, 2)), save_mentor=save,
        )
        exec(compile(ast.Module(body=[node], type_ignores=[]), str(source), "exec"), namespace)
        self.assertEqual(namespace["_get_pair_rebind_remaining"]("a", "m"), 0)
        save.assert_not_called()

    def test_migrations_repeat_and_route_without_copying_receipts(self):
        from ....plugin import build_migrations, migrations_for_database

        with DatabaseUnitOfWork(self.game, immediate=True) as uow:
            apply_base_direct_breakthrough_plans(uow)
        with DatabaseUnitOfWork(self.player, immediate=True) as uow:
            apply_base_direct_breakthrough_player(uow)
        self.assertEqual(self.row(self.player, "mentor", "user_id='a'")["breakthrough_reward_count"], 26)
        for database in ("game_db", "player_db", "trade_db", "impart_db", "message_db"):
            versions = {m.version for m in migrations_for_database(build_migrations(), database)}
            self.assertEqual("base.010" in versions, database == "game_db")
            self.assertEqual("base.011" in versions, database == "player_db")

    def test_plan_migration_can_precede_platform_on_a_fresh_database(self):
        with DatabaseUnitOfWork(self.root / "fresh.db", immediate=True) as uow:
            apply_base_direct_breakthrough_plans(uow)
            self.assertIsNotNone(uow.query_one("SELECT name FROM sqlite_master WHERE name='direct_breakthrough_plans'"))


if __name__ == "__main__":
    unittest.main()
