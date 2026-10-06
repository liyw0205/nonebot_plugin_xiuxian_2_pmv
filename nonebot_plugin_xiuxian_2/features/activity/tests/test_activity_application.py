from __future__ import annotations

import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import nonebot

nonebot.init()

from ..application import ActivityApplication
from ..migrations import apply_activity
from ..repository import ActivityRepository
from ....infrastructure.database import DatabaseUnitOfWork, OperationLedger


class ActivityApplicationTest(unittest.TestCase):
    def test_execute_is_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = f"{directory}/game.db"
            with DatabaseUnitOfWork(database) as uow:
                apply_activity(uow)
                OperationLedger().ensure_schema(uow)
            app = ActivityApplication(database)
            first = app.execute(operation_id="op-1", user_id="u")
            second = app.execute(operation_id="op-1", user_id="u")
            self.assertEqual(first.operation_id, second.operation_id)
            self.assertEqual("rejected", first.status)
            self.assertTrue(second.replayed)

    def test_reward_repositories_forward_operation_id(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = f"{directory}/game.db"
            with DatabaseUnitOfWork(database) as uow:
                OperationLedger().ensure_schema(uow)
            application = ActivityApplication(database, repository=ActivityRepository(database))
            with (
                patch(
                    "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_activity.service.claim_activity_tasks",
                    return_value=(True, "任务奖励"),
                ) as tasks,
                patch(
                    "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_activity.service.claim_activity_pass_rewards",
                    return_value=(True, "战令奖励"),
                ) as pass_claim,
                patch(
                    "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_activity.activity_boss.claim_boss_rewards",
                    return_value=(True, "首领奖励"),
                ) as boss,
                patch(
                    "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_activity.service.claim_sign",
                    return_value=(True, "签到成功"),
                ) as sign,
            ):
                self.assertTrue(application.execute(
                    operation_id="activity:task-claim:u:1", user_id="u",
                    payload={"action": "claim_activity_tasks", "query": ""},
                ).ok)
                self.assertTrue(application.execute(
                    operation_id="activity:pass-claim:u:2", user_id="u",
                    payload={"action": "claim_activity_pass_rewards", "query": ""},
                ).ok)
                self.assertTrue(application.execute(
                    operation_id="activity:boss-claim:u:3", user_id="u",
                    payload={"action": "activity_boss.claim_boss_rewards", "query": "进度"},
                ).ok)
                self.assertTrue(application.execute(
                    operation_id="activity:sign:u:4", user_id="u",
                    payload={"action": "claim_sign"},
                ).ok)

            tasks.assert_called_once_with("u", "", "activity:task-claim:u:1")
            pass_claim.assert_called_once_with("u", "", "activity:pass-claim:u:2")
            boss.assert_called_once_with("u", "进度", "activity:boss-claim:u:3")
            sign.assert_called_once()
            self.assertEqual("u", sign.call_args.args[0])
            self.assertEqual("activity:sign:u:4", sign.call_args.args[1])
            self.assertEqual(database, str(sign.call_args.kwargs["settlement_repository"].database))

    def test_point_shop_action_uses_feature_repository(self) -> None:
        from ....xiuxian.xiuxian_activity import service
        from ..migrations import apply_activity_event_receipts, apply_activity_state_schema

        with tempfile.TemporaryDirectory() as directory:
            database = f"{directory}/game.db"
            with DatabaseUnitOfWork(database) as uow:
                apply_activity_state_schema(uow)
                apply_activity_event_receipts(uow)
                OperationLedger().ensure_schema(uow)
                uow.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER)")
                uow.execute("INSERT INTO user_xiuxian VALUES('u',10)")
                uow.execute(
                    "CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,"
                    "goods_type TEXT,goods_num INTEGER,create_time TEXT,update_time TEXT,"
                    "bind_num INTEGER,UNIQUE(user_id,goods_id))"
                )
                uow.execute(
                    "INSERT INTO activity_point_balance(activity_key,user_id,points,total_points) "
                    "VALUES('a','u',1000,1000)"
                )
            application = ActivityApplication(
                database, repository=ActivityRepository(database)
            )
            activity = {"key": "a", "name": "测试活动", "point_name": "测试积分"}
            item = {
                "item_key": "pack",
                "name": "补给",
                "cost": 100,
                "limit": 3,
                "stock_limit": 4,
                "reward": "fixture",
            }
            with (
                patch.object(service, "load_config", return_value={}),
                patch.object(
                    service,
                    "activity_runtime_state",
                    return_value={"ok": True, "features": ["shop"]},
                ),
                patch.object(service, "_find_point_shop_item", return_value=(activity, item)),
                patch.object(
                    service,
                    "parse_reward",
                    return_value=[{"type": "stone", "quantity": 50}],
                ),
                patch.object(service, "ensure_activity_files") as ensure_files,
                patch.object(service, "XiuConfig", return_value=SimpleNamespace(max_goods_num=100)),
                patch.object(
                    service,
                    "_point_shop_purchase_service",
                    side_effect=AssertionError("legacy purchase service used"),
                ),
            ):
                outcome = application.execute(
                    operation_id="activity:point-shop:u:message-1",
                    user_id="u",
                    payload={"action": "claim_point_shop_item", "query": "补给"},
                )

            self.assertTrue(outcome.ok)
            self.assertIn("测试活动兑换成功：补给", outcome.data["message"])
            ensure_files.assert_not_called()
            with DatabaseUnitOfWork(database, read_only=True) as uow:
                self.assertEqual(900, uow.query_one(
                    "SELECT points FROM activity_point_balance WHERE user_id='u'"
                )["points"])
                self.assertEqual(60, uow.query_one(
                    "SELECT stone FROM user_xiuxian WHERE user_id='u'"
                )["stone"])

    def test_point_shop_started_operation_is_not_blindly_retried(self) -> None:
        from ....core.errors import ConflictError

        with tempfile.TemporaryDirectory() as directory:
            database = f"{directory}/game.db"
            with DatabaseUnitOfWork(database) as uow:
                OperationLedger().ensure_schema(uow)
                OperationLedger().begin(
                    uow,
                    "activity:point-shop:u:started",
                    "activity.claim_point_shop_item",
                    {"query": "補給", "user_id": "u"},
                )
            application = ActivityApplication(
                database, repository=ActivityRepository(database)
            )

            with self.assertRaisesRegex(ConflictError, "操作正在处理中"):
                application.execute(
                    operation_id="activity:point-shop:u:started",
                    user_id="u",
                    payload={"action": "claim_point_shop_item", "query": "補給"},
                )

    def test_boss_item_action_uses_feature_settlement_repository(self) -> None:
        from unittest.mock import patch

        from ....features.activity.migrations import apply_activity_state_schema

        with tempfile.TemporaryDirectory() as directory:
            database = f"{directory}/game.db"
            with DatabaseUnitOfWork(database) as uow:
                apply_activity_state_schema(uow)
                OperationLedger().ensure_schema(uow)
                uow.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,atk INTEGER,user_name TEXT)")
                uow.execute("INSERT INTO user_xiuxian VALUES('u',100,'道号')")
                uow.execute(
                    "INSERT INTO activity_boss_state(activity_key,hp_left,max_hp) VALUES('boss',1000,1000)"
                )
                uow.execute(
                    "INSERT INTO activity_item_inventory(activity_key,user_id,item_id,count) "
                    "VALUES('boss','u','firework',2)"
                )
            activity = {
                "key": "boss",
                "type": "activity_boss",
                "name": "测试首领",
                "boss_name": "测试首领",
                "mode": "item_raid",
                "max_hp": 1000,
                "daily_fight_limit": 3,
                "items": [{"id": "firework", "name": "爆竹", "cost": 1, "damage_min": 100, "damage_max": 100}],
                "server_milestones": [],
            }
            application = ActivityApplication(
                database, repository=ActivityRepository(database)
            )
            with (
                patch(
                    "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_activity.activity_config.load_config",
                    return_value={"enabled": True},
                ),
                patch(
                    "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_activity.activity_config.activity_runtime_state",
                    return_value={"ok": True, "features": ["boss"], "multiplier": 1},
                ),
                patch(
                    "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_activity.activity_rules.get_gameplay_activities",
                    return_value=[activity],
                ),
                patch(
                    "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_activity.activity_config.activity_state",
                    return_value=(True, ""),
                ),
            ):
                outcome = application.execute(
                    operation_id="activity:boss-item:u:1",
                    user_id="u",
                    payload={"action": "activity_boss.use_item_on_boss", "query": "firework"},
                )

            self.assertTrue(outcome.ok)
            with DatabaseUnitOfWork(database, read_only=True) as uow:
                self.assertEqual(
                    1,
                    uow.query_one(
                        "SELECT count FROM activity_item_inventory WHERE user_id='u'"
                    )["count"],
                )
                self.assertEqual(
                    900,
                    uow.query_one(
                        "SELECT hp_left FROM activity_boss_state WHERE activity_key='boss'"
                    )["hp_left"],
                )

    def test_sign_projection_failure_replays_with_a_stable_event_receipt(self) -> None:
        from ....xiuxian.xiuxian_activity import service

        settlement = SimpleNamespace(
            get_result=lambda operation_id: SimpleNamespace(sign_days=3)
        )
        with (
            patch.object(service, "load_config", return_value={"festival_name": "测试活动"}),
            patch.object(
                service,
                "activity_runtime_state",
                return_value={"ok": True, "features": ["sign"], "stage_name": "签到"},
            ),
            patch.object(service, "today_str", return_value="2026-10-06"),
            patch.object(
                service,
                "record_activity_event",
                side_effect=[RuntimeError("projection unavailable"), []],
            ) as record_event,
        ):
            with self.assertRaisesRegex(RuntimeError, "projection unavailable"):
                service.claim_sign(
                    "u", "activity:sign:u:message-1", settlement_repository=settlement
                )
            ok, text = service.claim_sign(
                "u", "activity:sign:u:message-1", settlement_repository=settlement
            )

        self.assertTrue(ok)
        self.assertIn("累计签到：3 天", text)
        self.assertEqual(2, record_event.call_count)
        self.assertEqual(record_event.call_args_list[0], record_event.call_args_list[1])
        self.assertEqual("activity-sign:u:2026-10-06", record_event.call_args.kwargs["event_id"])
        self.assertIn("2026-10-06T12:00:00", record_event.call_args.kwargs["occurred_at"])


if __name__ == "__main__":
    unittest.main()
