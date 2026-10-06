from __future__ import annotations

import copy
import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.activity.config_application import (
    ActivityConfigApplication,
)
from nonebot_plugin_xiuxian_2.features.activity.config_repository import (
    ActivityConfigSqlRepository,
)
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_activity import activity_config
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_activity.config_event_service import (
    ActivityConfigEventService,
)


class ActivityConfigApplicationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.initial = activity_config._migrate_config(
            activity_config._load_default_config()
        )[0]
        self.initial["enabled"] = False
        self.initial.setdefault("extensions", {}).setdefault(
            "activity_pass", {}
        )["enabled"] = False
        for activity in self.initial.get("gameplay_activities") or []:
            if isinstance(activity, dict):
                activity["enabled"] = False

    def tearDown(self) -> None:
        self.temp.cleanup()

    def _application(self, database: Path, projection_writer=None):
        event_service = ActivityConfigEventService(database)
        repository = ActivityConfigSqlRepository(
            database,
            event_service=event_service,
            config_loader=lambda: copy.deepcopy(self.initial),
            projection_writer=projection_writer,
        )
        return ActivityConfigApplication(repository), repository

    def test_enabled_targets_preserve_legacy_mapping_and_only_write_config_store(self) -> None:
        cases = [
            (None, "已开启签到活动", "sign", None),
            ("全部", "已开启全部活动", "all", None),
            ("所有", "已开启全部活动", "all", None),
            ("签到", "已开启签到活动", "sign", None),
            ("玩法", "已开启3个玩法活动", "gameplay", None),
            ("玩法活动", "已开启3个玩法活动", "gameplay", None),
            ("战令", "已开启活动战令", "pass", None),
            ("活动战令", "已开启活动战令", "pass", None),
            ("集字", "已开启1个集字", "type", "collect_words"),
            ("集字活动", "已开启1个集字活动", "type", "collect_words"),
            ("积分", "已开启1个积分", "type", "event_points"),
            ("活动商店", "已开启1个活动商店", "type", "event_points"),
            ("首领", "已开启1个首领", "type", "activity_boss"),
            ("BOSS", "已开启1个BOSS", "type", "activity_boss"),
            ("festival_collect_words", "已开启festival_collect_words", "name", "collect_words"),
            ("missing", "未找到活动：missing", "unchanged", None),
        ]
        game_database = self.root / "game.db"

        for index, (target, expected_text, expected_change, target_type) in enumerate(cases):
            with self.subTest(target=target):
                config_database = self.root / f"activity-{index}.db"
                application, repository = self._application(config_database)
                result = application.set_enabled(
                    operation_id=f"toggle-{index}",
                    enabled=True,
                    target=target,
                    operator_id="admin-1",
                )
                config = repository.read().config
                activities = config.get("gameplay_activities") or []

                self.assertEqual(expected_text, result)
                self.assertTrue(config_database.is_file())
                self.assertFalse(game_database.exists())
                if expected_change == "sign":
                    self.assertTrue(config["enabled"])
                elif expected_change == "all":
                    self.assertTrue(config["enabled"])
                    self.assertTrue(all(activity["enabled"] for activity in activities))
                    self.assertTrue(config["extensions"]["activity_pass"]["enabled"])
                elif expected_change == "gameplay":
                    self.assertFalse(config["enabled"])
                    self.assertTrue(all(activity["enabled"] for activity in activities))
                elif expected_change == "pass":
                    self.assertTrue(config["extensions"]["activity_pass"]["enabled"])
                    self.assertTrue(all(not activity["enabled"] for activity in activities))
                elif expected_change == "type":
                    matching = [activity for activity in activities if activity["type"] == target_type]
                    self.assertTrue(matching)
                    self.assertTrue(all(activity["enabled"] for activity in matching))
                    self.assertTrue(all(
                        activity["enabled"] == (activity["type"] == target_type)
                        for activity in activities
                    ))
                elif expected_change == "name":
                    self.assertTrue(activities[0]["enabled"])
                    self.assertTrue(all(not activity["enabled"] for activity in activities[1:]))
                else:
                    self.assertTrue(all(not activity["enabled"] for activity in activities))

    def test_duplicate_replay_repairs_projection_after_commit_failure(self) -> None:
        database = self.root / "activity.db"
        projections = []

        def write_projection(config):
            if not projections:
                projections.append("failed")
                raise OSError("projection unavailable")
            projections.append(copy.deepcopy(config))

        application, repository = self._application(database, write_projection)
        with self.assertRaisesRegex(OSError, "projection unavailable"):
            application.set_enabled(
                operation_id="toggle-open-1",
                enabled=True,
                target="签到",
                operator_id="admin-1",
            )

        self.assertTrue(repository.read().config["enabled"])
        result = application.set_enabled(
            operation_id="toggle-open-1",
            enabled=True,
            target="签到",
            operator_id="admin-1",
        )
        self.assertEqual("已开启签到活动", result)
        self.assertTrue(projections[-1]["enabled"])

    def test_read_is_non_mutating_and_replace_preserves_web_revision_contract(self) -> None:
        database = self.root / "activity.db"
        projections = []
        application, repository = self._application(
            database,
            lambda config: projections.append(copy.deepcopy(config)),
        )

        initial = application.read()
        self.assertEqual(initial.revision, 0)
        self.assertFalse(database.exists())

        config = copy.deepcopy(initial.config)
        config["name"] = "Web replacement"
        first = application.replace(
            operation_id="web-config-1",
            expected_revision=0,
            config=config,
            operator_id="admin-1",
        )
        replay = application.replace(
            operation_id="web-config-1",
            expected_revision=0,
            config=config,
            operator_id="admin-1",
        )
        conflict = application.replace(
            operation_id="web-config-1",
            expected_revision=0,
            config={**config, "name": "Different payload"},
            operator_id="admin-1",
        )
        stale = application.replace(
            operation_id="web-config-stale",
            expected_revision=0,
            config=config,
            operator_id="admin-1",
        )

        self.assertEqual((first.status, first.revision), ("applied", 1))
        self.assertEqual((replay.status, replay.revision), ("duplicate", 1))
        self.assertEqual(conflict.status, "operation_conflict")
        self.assertEqual(stale.status, "state_changed")
        self.assertEqual(repository.read().config["name"], "Web replacement")
        self.assertEqual(len(projections), 2)

    def test_conflict_and_stale_revision_do_not_write_projection(self) -> None:
        database = self.root / "activity.db"
        projections = []
        application, repository = self._application(
            database, lambda config: projections.append(copy.deepcopy(config))
        )
        application.set_enabled(
            operation_id="toggle-1",
            enabled=True,
            target="签到",
            operator_id="admin-1",
        )
        self.assertEqual(1, len(projections))

        conflict = application.set_enabled(
            operation_id="toggle-1",
            enabled=False,
            target="签到",
            operator_id="admin-1",
        )
        self.assertEqual("同一消息事件不能用于不同的活动配置操作", conflict)
        self.assertEqual(1, len(projections))

        stale = repository.replace(
            "toggle-stale",
            {"action": "toggle", "enabled": False, "operator_id": "admin-1", "target": ""},
            0,
            {"enabled": False},
        )
        self.assertEqual("state_changed", stale.status)
        self.assertEqual(1, len(projections))
