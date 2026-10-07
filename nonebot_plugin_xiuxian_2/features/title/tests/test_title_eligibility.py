from __future__ import annotations

import importlib
import sqlite3
import sys
import tempfile
from types import ModuleType, SimpleNamespace
import unittest
from pathlib import Path
from unittest.mock import patch

from ..eligibility import TitleEligibilityApplication


def _load_title_data_without_plugin_registration():
    package_name = "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_title"
    module_name = f"{package_name}.title_data"
    if package_name in sys.modules:
        return importlib.import_module(module_name)

    package = ModuleType(package_name)
    package.__path__ = [
        str(Path(__file__).resolve().parents[3] / "xiuxian" / "xiuxian_title")
    ]
    sys.modules[package_name] = package
    try:
        return importlib.import_module(module_name)
    finally:
        sys.modules.pop(module_name, None)
        sys.modules.pop(package_name, None)


title_data = _load_title_data_without_plugin_registration()


class TitleEligibilityApplicationTest(unittest.TestCase):
    def test_snapshot_batches_profile_statistics_and_tower_reads(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            game_database = Path(directory) / "game.db"
            player_database = Path(directory) / "player.db"
            with sqlite3.connect(game_database) as connection:
                connection.execute(
                    "CREATE TABLE user_xiuxian(user_id TEXT, level TEXT, user_name TEXT)"
                )
                connection.execute(
                    "INSERT INTO user_xiuxian VALUES(?, ?, ?)",
                    ("u", "筑基初期", "道友"),
                )
            with sqlite3.connect(player_database) as connection:
                connection.execute(
                    'CREATE TABLE statistics(user_id TEXT PRIMARY KEY, "修仙签到" TEXT, "忽略" TEXT)'
                )
                connection.execute(
                    'INSERT INTO statistics VALUES(?, ?, ?)',
                    ("admin-target", "12", "not requested"),
                )
                connection.execute(
                    "CREATE TABLE tower(user_id TEXT PRIMARY KEY, max_floor INTEGER, score INTEGER)"
                )
                connection.execute(
                    "INSERT INTO tower VALUES(?, ?, ?)", ("u", 8, 130)
                )

            snapshot = TitleEligibilityApplication(
                game_database, player_database
            ).read_snapshot(
                user_id="u",
                statistics_user_id="admin-target",
                condition_keys=("境界", "修仙签到", "通天塔最高层", "通天塔积分"),
            )

            self.assertEqual(snapshot.profile["level"], "筑基初期")
            self.assertEqual(snapshot.statistics, {"修仙签到": 12})
            self.assertEqual(snapshot.tower, {"max_floor": 8, "score": 130})

    def test_missing_database_reads_do_not_create_files(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            game_database = Path(directory) / "missing-game.db"
            player_database = Path(directory) / "missing-player.db"

            snapshot = TitleEligibilityApplication(
                game_database, player_database
            ).read_snapshot(
                user_id="u",
                statistics_user_id="u",
                condition_keys=("修仙签到", "通天塔最高层"),
            )

            self.assertIsNone(snapshot.profile)
            self.assertEqual(snapshot.statistics, {})
            self.assertEqual(snapshot.tower, {})
            self.assertFalse(game_database.exists())
            self.assertFalse(player_database.exists())

    def test_missing_condition_column_is_not_added_on_read(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            game_database = Path(directory) / "game.db"
            player_database = Path(directory) / "player.db"
            with sqlite3.connect(game_database) as connection:
                connection.execute("CREATE TABLE user_xiuxian(user_id TEXT, level TEXT)")
                connection.execute("INSERT INTO user_xiuxian VALUES('u', '炼气初期')")
            with sqlite3.connect(player_database) as connection:
                connection.execute("CREATE TABLE statistics(user_id TEXT PRIMARY KEY)")
                connection.execute("INSERT INTO statistics VALUES('u')")

            snapshot = TitleEligibilityApplication(
                game_database, player_database
            ).read_snapshot(
                user_id="u",
                statistics_user_id="u",
                condition_keys=("缺失字段",),
            )

            self.assertEqual(snapshot.statistics, {})
            with sqlite3.connect(player_database) as connection:
                columns = [
                    row[1]
                    for row in connection.execute('PRAGMA table_info("statistics")')
                ]
            self.assertEqual(columns, ["user_id"])


class TitleConditionEvaluationTest(unittest.TestCase):
    def test_statistics_value_drives_condition_and_progress(self) -> None:
        snapshot = SimpleNamespace(
            profile={"level": "筑基初期"},
            statistics={"修仙签到": "12"},
            tower={},
        )
        conditions = [("修仙签到", ">=", "10")]

        self.assertTrue(title_data._condition_matches(conditions, snapshot))
        self.assertEqual(
            title_data._condition_progress(conditions, snapshot),
            [{
                "key": "修仙签到",
                "operator": ">=",
                "actual": "12",
                "expected": "10",
                "display": "12 / 10",
                "ratio": 1,
                "satisfied": True,
            }],
        )

    def test_missing_declared_counter_defaults_to_zero(self) -> None:
        snapshot = SimpleNamespace(profile=None, statistics={}, tower={})
        conditions = [("修仙签到", "==", "0")]

        with patch.object(title_data, "get_title_condition_keys", return_value={"修仙签到"}):
            self.assertTrue(title_data._condition_matches(conditions, snapshot))
            self.assertFalse(
                title_data._condition_matches([("修仙签到", ">", "0")], snapshot)
            )

        progress = title_data._condition_progress(conditions, snapshot)
        self.assertEqual(progress[0]["actual"], "0")
        self.assertTrue(progress[0]["satisfied"])

    def test_tower_values_are_fallback_when_statistics_are_absent(self) -> None:
        snapshot = SimpleNamespace(
            profile=None,
            statistics={},
            tower={"max_floor": "8", "score": 130},
        )
        conditions = [("通天塔最高层", ">=", "8")]

        self.assertTrue(title_data._condition_matches(conditions, snapshot))
        progress = title_data._condition_progress(conditions, snapshot)
        self.assertEqual(progress[0]["actual"], "8")
        self.assertEqual(progress[0]["display"], "8 / 8")

    def test_realm_ordering_and_progress_formatting(self) -> None:
        levels = ["炼气初期", "筑基初期", "筑基圆满", "金丹初期"]
        snapshot = SimpleNamespace(
            profile={"level": "金丹初期"},
            statistics={},
            tower={},
        )
        conditions = [("境界", ">=", "筑基圆满")]

        with patch.object(title_data, "convert_rank", return_value=(0, levels)):
            self.assertTrue(title_data._condition_matches(conditions, snapshot))
            progress = title_data._condition_progress(conditions, snapshot)

        self.assertTrue(progress[0]["satisfied"])
        self.assertEqual(progress[0]["actual"], "金丹初期")
        self.assertEqual(progress[0]["expected"], "筑基圆满")
        self.assertEqual(progress[0]["ratio"], 1)
        self.assertEqual(title_data._format_progress_number("12.50"), "12.5")
        self.assertEqual(title_data._format_progress_number(3.0), "3")
        self.assertEqual(title_data._format_progress_number(None), "0")

    def test_supplied_snapshot_and_unlocked_ids_skip_legacy_reads(self) -> None:
        titles = {
            "1": {
                "name": "签到修士",
                "desc": "",
                "condition": "修仙签到>=10",
            }
        }
        snapshot = SimpleNamespace(
            profile=None,
            statistics={"修仙签到": 12},
            tower={},
        )

        with (
            patch.object(title_data, "load_title_data", return_value=titles),
            patch.object(
                title_data,
                "_read_condition_snapshot",
                side_effect=AssertionError("provided snapshot must be reused"),
            ),
            patch.object(
                title_data,
                "get_user_unlocked_titles",
                side_effect=AssertionError("provided unlocked ids must be reused"),
            ),
        ):
            unlockable = title_data.find_unlockable_titles(
                "u", snapshot=snapshot, unlocked_title_ids=[]
            )
            records = title_data.get_title_achievement_records(
                "u", snapshot=snapshot, unlocked_title_ids=[]
            )

        self.assertEqual([title["id"] for title in unlockable], ["1"])
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["id"], "1")
        self.assertTrue(records[0]["satisfied"])

    def test_title_condition_snapshot_reads_all_configured_conditions_once(self) -> None:
        titles = {
            "1": {"condition": "修仙签到>=10;历练次数>=5"},
            "2": {"condition": "通天塔最高层>=8"},
            "3": {"condition": ""},
        }
        expected_conditions = [
            ("修仙签到", ">=", "10"),
            ("历练次数", ">=", "5"),
            ("通天塔最高层", ">=", "8"),
        ]
        snapshot = object()

        with (
            patch.object(title_data, "load_title_data", return_value=titles),
            patch.object(
                title_data, "_read_condition_snapshot", return_value=snapshot
            ) as read_snapshot,
        ):
            result = title_data.get_title_condition_snapshot("u")

        self.assertIs(result, snapshot)
        read_snapshot.assert_called_once_with("u", expected_conditions)

if __name__ == "__main__":
    unittest.main()
