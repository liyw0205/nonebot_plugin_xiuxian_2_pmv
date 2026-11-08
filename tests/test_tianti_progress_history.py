"""我的炼体记录：只读个人炼体成长回执视图（P1）。"""

from __future__ import annotations

import json
import sqlite3
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

from nonebot_plugin_xiuxian_2.features.tianti_training.progress_history import (
    EMPTY_TEXT,
    MISSING_TEXT,
    TiantiProgressHistoryApplication,
    format_tianti_progress_history,
)

BREAKTHROUGH_DDL = (
    "CREATE TABLE tianti_breakthrough_operations ("
    "operation_id TEXT PRIMARY KEY, user_id TEXT NOT NULL, cultivation_rank INTEGER NOT NULL, "
    "roll_success INTEGER NOT NULL, old_level TEXT NOT NULL, new_level TEXT NOT NULL, "
    "hp_cost INTEGER NOT NULL, new_hp INTEGER NOT NULL, success INTEGER NOT NULL, "
    "created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)"
)
QIAOXUE_DDL = (
    "CREATE TABLE tianti_qiaoxue_operations ("
    "operation_id TEXT PRIMARY KEY, user_id TEXT NOT NULL, roll INTEGER NOT NULL, "
    "qiaoxue_json TEXT NOT NULL, hp_cost INTEGER NOT NULL, new_hp INTEGER NOT NULL, "
    "opened_count INTEGER NOT NULL, unlock_limit INTEGER NOT NULL, "
    "created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)"
)
STONE_DDL = (
    "CREATE TABLE tianti_stone_training_operations ("
    "operation_id TEXT PRIMARY KEY, user_id TEXT NOT NULL, requested_stone INTEGER NOT NULL, "
    "stone_cost INTEGER NOT NULL, hp_gain INTEGER NOT NULL, new_hp INTEGER NOT NULL, "
    "created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)"
)


class _Clock:
    def __init__(self, moment: datetime) -> None:
        self.moment = moment

    def now(self) -> datetime:
        return self.moment


class ProgressHistoryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.game = Path(self.tmp.name) / "game.db"
        self.player = Path(self.tmp.name) / "player.db"
        self.now = datetime(2026, 11, 9, 12, 0, tzinfo=timezone.utc)
        with sqlite3.connect(self.player) as conn:
            conn.execute(BREAKTHROUGH_DDL)
            conn.execute(QIAOXUE_DDL)
            conn.commit()
        with sqlite3.connect(self.game) as conn:
            conn.execute(STONE_DDL)
            conn.commit()
        self.app = TiantiProgressHistoryApplication(
            self.game, self.player, clock=_Clock(self.now)
        )

    def _at(self, hours_ago: float = 1.0) -> str:
        return (self.now - timedelta(hours=hours_ago)).strftime("%Y-%m-%d %H:%M:%S")

    def _breakthrough(self, op, user, old, new, hp_cost, success, created=None):
        with sqlite3.connect(self.player) as conn:
            conn.execute(
                "INSERT INTO tianti_breakthrough_operations(operation_id,user_id,cultivation_rank,"
                "roll_success,old_level,new_level,hp_cost,new_hp,success,created_at) "
                "VALUES(?,?,?,?,?,?,?,?,?,?)",
                (op, user, 1, int(success), old, new, hp_cost, 0, int(success), created or self._at()),
            )
            conn.commit()

    def _qiaoxue(self, op, user, name, group, hp_cost, opened_count, created=None, raw=None):
        payload = raw if raw is not None else json.dumps(
            {"name": name, "group": group, "effect_type": "base_per_min_ratio", "effect_value": 0.1},
            ensure_ascii=False,
        )
        with sqlite3.connect(self.player) as conn:
            conn.execute(
                "INSERT INTO tianti_qiaoxue_operations(operation_id,user_id,roll,qiaoxue_json,"
                "hp_cost,new_hp,opened_count,unlock_limit,created_at) VALUES(?,?,?,?,?,?,?,?,?)",
                (op, user, 0, payload, hp_cost, 0, opened_count, 108, created or self._at()),
            )
            conn.commit()

    def _stone(self, op, user, requested, cost, gain, created=None):
        with sqlite3.connect(self.game) as conn:
            conn.execute(
                "INSERT INTO tianti_stone_training_operations(operation_id,user_id,requested_stone,"
                "stone_cost,hp_gain,new_hp,created_at) VALUES(?,?,?,?,?,?,?)",
                (op, user, requested, cost, gain, 0, created or self._at()),
            )
            conn.commit()

    # ---------------------------------------------------------------- sections
    def test_all_three_sections_aggregate(self):
        self._breakthrough("b1", "u1", "淬体境", "金刚境", 10, True)
        self._breakthrough("b2", "u1", "金刚境", "金刚境", 12, False)
        self._qiaoxue("q1", "u1", "天罡·破军", "天罡", 30, 1)
        self._qiaoxue("q2", "u1", "地煞·贪狼", "地煞", 40, 2)
        self._stone("s1", "u1", 100, 100, 5)
        self._stone("s2", "u1", 50, 50, 3)

        summary = self.app.summary("u1")
        self.assertTrue(summary["ok"])
        self.assertEqual(summary["breakthrough"]["total"], 2)
        self.assertEqual(summary["breakthrough"]["success"], 1)
        self.assertEqual(summary["breakthrough"]["failed"], 1)
        self.assertEqual(summary["breakthrough"]["hp_cost"], 22)
        self.assertEqual(summary["qiaoxue"]["total"], 2)
        self.assertEqual(summary["qiaoxue"]["hp_cost"], 70)
        self.assertEqual(summary["qiaoxue"]["opened_count"], 2)
        self.assertEqual(summary["stone"]["total"], 2)
        self.assertEqual(summary["stone"]["stone_cost"], 150)
        self.assertEqual(summary["stone"]["hp_gain"], 8)

    def test_missing_section_is_none_not_error(self):
        self._stone("s1", "u1", 100, 100, 5)
        summary = self.app.summary("u1")
        self.assertTrue(summary["ok"])
        self.assertIsNone(summary["breakthrough"])
        self.assertIsNone(summary["qiaoxue"])
        self.assertIsNotNone(summary["stone"])

    def test_recent_is_newest_first_and_capped(self):
        for index in range(12):
            self._breakthrough(f"b{index}", "u1", "淬体境", "金刚境", 1, True,
                               created=(self.now - timedelta(hours=index + 1)).strftime("%Y-%m-%d %H:%M:%S"))
        recent = self.app.summary("u1")["breakthrough"]["recent"]
        self.assertEqual(len(recent), 10)
        self.assertEqual(recent[0]["created_at"], self._at(1))

    def test_window_lower_bound_only_and_future_rows_kept(self):
        self._breakthrough("old", "u1", "淬体境", "金刚境", 5, True,
                           created=(self.now - timedelta(days=8)).strftime("%Y-%m-%d %H:%M:%S"))
        self._breakthrough("future", "u1", "淬体境", "金刚境", 7, True,
                           created=(self.now + timedelta(hours=3)).strftime("%Y-%m-%d %H:%M:%S"))
        section = self.app.summary("u1")["breakthrough"]
        self.assertEqual(section["total"], 1)
        self.assertEqual(section["hp_cost"], 7)

    def test_player_isolation(self):
        self._breakthrough("mine", "u1", "淬体境", "金刚境", 5, True)
        self._breakthrough("theirs", "u2", "淬体境", "永恒体", 9999, True)
        self._stone("theirs", "u2", 9999, 9999, 9999)
        summary = self.app.summary("u1")
        self.assertEqual(summary["breakthrough"]["total"], 1)
        self.assertIsNone(summary["stone"])
        self.assertNotIn("u2", json.dumps(summary, ensure_ascii=False))

    # ---------------------------------------------------------------- degrade
    def test_bad_qiaoxue_json_is_tolerated(self):
        self._qiaoxue("bad", "u1", "", "", 20, 1, raw="{not json")
        section = self.app.summary("u1")["qiaoxue"]
        self.assertEqual(section["total"], 1)
        self.assertEqual(section["recent"][0]["name"], "")
        self.assertEqual(section["hp_cost"], 20)

    def test_missing_player_tables_degrade_without_creating(self):
        with TemporaryDirectory() as other:
            empty_player = Path(other) / "player.db"
            sqlite3.connect(empty_player).close()
            app = TiantiProgressHistoryApplication(self.game, empty_player, clock=_Clock(self.now))
            self._stone("s1", "u1", 10, 10, 1)
            summary = app.summary("u1")
            self.assertTrue(summary["ok"])
            self.assertIsNone(summary["breakthrough"])
            self.assertIsNone(summary["qiaoxue"])
            with sqlite3.connect(empty_player) as conn:
                tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            self.assertNotIn("tianti_breakthrough_operations", tables)

    def test_missing_database_files_do_not_create_them(self):
        with TemporaryDirectory() as other:
            absent_game = Path(other) / "nope.db"
            absent_player = Path(other) / "nope2.db"
            app = TiantiProgressHistoryApplication(absent_game, absent_player, clock=_Clock(self.now))
            summary = app.summary("u1")
            self.assertFalse(summary["ok"])
            self.assertEqual(summary["notice"], EMPTY_TEXT)
            self.assertFalse(absent_game.exists())
            self.assertFalse(absent_player.exists())

    def test_half_migrated_table_is_skipped(self):
        with TemporaryDirectory() as other:
            partial = Path(other) / "player.db"
            with sqlite3.connect(partial) as conn:
                conn.execute("CREATE TABLE tianti_breakthrough_operations (operation_id TEXT PRIMARY KEY)")
                conn.commit()
            app = TiantiProgressHistoryApplication(self.game, partial, clock=_Clock(self.now))
            self.assertIsNone(app.summary("u1")["breakthrough"])

    def test_empty_user_id_is_refused(self):
        summary = self.app.summary("")
        self.assertFalse(summary["ok"])
        self.assertEqual(summary["notice"], MISSING_TEXT)

    # ---------------------------------------------------------------- render
    def test_render_sections_and_empty(self):
        self._breakthrough("b1", "u1", "淬体境", "金刚境", 10, True)
        self._qiaoxue("q1", "u1", "天罡·破军", "天罡", 30, 1)
        self._stone("s1", "u1", 100, 100, 5)
        text = format_tianti_progress_history(self.app.summary("u1"))
        self.assertIn("近7天炼体记录", text)
        self.assertIn("突破：1 次（成功 1 · 失败 0）", text)
        self.assertIn("冲窍：1 次", text)
        self.assertIn("开启【天罡·破军】", text)
        self.assertIn("灵石炼体：1 次", text)
        self.assertEqual(format_tianti_progress_history({"ok": False, "notice": None}), EMPTY_TEXT)

    def test_render_does_not_mutate_input(self):
        self._breakthrough("b1", "u1", "淬体境", "金刚境", 10, True)
        summary = self.app.summary("u1")
        snapshot = json.dumps(summary, ensure_ascii=False, sort_keys=True)
        format_tianti_progress_history(summary)
        self.assertEqual(json.dumps(summary, ensure_ascii=False, sort_keys=True), snapshot)

    # ---------------------------------------------------------------- wiring
    def test_handler_help_and_manifest_contract(self):
        root = Path(__file__).resolve().parents[1]
        source = (root / "nonebot_plugin_xiuxian_2" / "xiuxian" / "xiuxian_tianti" / "__init__.py").read_text(encoding="utf-8")
        self.assertIn('"我的炼体记录", aliases={"炼体记录"}', source)
        self.assertIn("format_tianti_progress_history(summary)", source)
        self.assertIn('tianti_progress_history_application.summary(str(user_info["user_id"]))', source)
        self.assertIn("- 我的炼体记录", source)
        self.assertNotIn(
            "args.extract_plain_text",
            source.split("tianti_progress_history.handle")[1],
        )
        # 三处冲窍上限与突破门控必须走共享纯函数（P2/P3）。
        self.assertIn("qiaoxue_unlock_limit(", source)
        self.assertIn("breakthrough_progress(", source)
        repository = (
            root / "nonebot_plugin_xiuxian_2" / "features" / "tianti_training" / "repository.py"
        ).read_text(encoding="utf-8")
        self.assertIn("qiaoxue_unlock_limit(", repository)
        self.assertNotIn("min(int(self.profile.levels()", repository)


class BreakthroughAndQiaoxueRulesTests(unittest.TestCase):
    """P2/P3/P4 纯函数：门控与上限单一来源。"""

    def test_qiaoxue_unlock_limit_boundaries(self):
        from nonebot_plugin_xiuxian_2.features.tianti_training.presentation import qiaoxue_unlock_limit

        self.assertEqual(qiaoxue_unlock_limit(0), 0)
        self.assertEqual(qiaoxue_unlock_limit(1), 3)
        self.assertEqual(qiaoxue_unlock_limit(36), 108)
        self.assertEqual(qiaoxue_unlock_limit(1000), 108)
        self.assertEqual(qiaoxue_unlock_limit(-5), 0)
        self.assertEqual(qiaoxue_unlock_limit(None), 0)
        self.assertEqual(qiaoxue_unlock_limit("bad"), 0)

    def test_breakthrough_progress_reasons(self):
        from nonebot_plugin_xiuxian_2.features.tianti_training.presentation import breakthrough_progress

        # 比较方向与写入路径一致：cultivation_rank 数值越大境界越低，需 <= required_rank。
        ready = breakthrough_progress(
            {"tianti_hp": 500}, next_level="金刚境", next_config={"need_hp": 100},
            required_rank=10, cultivation_rank=10,
        )
        self.assertTrue(ready.ready)
        self.assertEqual(ready.reason, "ready")
        self.assertEqual(ready.hp_shortfall, 0)

        low_hp = breakthrough_progress(
            {"tianti_hp": 40}, next_level="金刚境", next_config={"need_hp": 100},
            required_rank=10, cultivation_rank=10,
        )
        self.assertFalse(low_hp.ready)
        self.assertEqual(low_hp.reason, "hp_insufficient")
        self.assertEqual(low_hp.hp_shortfall, 60)

        low_realm = breakthrough_progress(
            {"tianti_hp": 500}, next_level="金刚境", next_config={"need_hp": 100},
            required_rank=3, cultivation_rank=10,
        )
        self.assertEqual(low_realm.reason, "cultivation_insufficient")

        top = breakthrough_progress(
            {"tianti_hp": 500}, next_level=None, next_config=None,
            required_rank=0, cultivation_rank=0,
        )
        self.assertTrue(top.max_level)
        self.assertEqual(top.reason, "max_level")
        self.assertFalse(top.ready)

    def test_breakthrough_progress_tolerates_bad_data(self):
        from nonebot_plugin_xiuxian_2.features.tianti_training.presentation import breakthrough_progress

        broken = breakthrough_progress(
            None, next_level="金刚境", next_config={"need_hp": "bad"},
            required_rank=None, cultivation_rank=None,
        )
        self.assertEqual(broken.current_hp, 0)
        self.assertEqual(broken.required_hp, 0)
        # 坏 need_hp fail-closed：绝不显示「可以突破」。
        self.assertFalse(broken.ready)
        self.assertEqual(broken.reason, "hp_insufficient")
        self.assertEqual(broken.hp_shortfall, 0)

    def test_render_breakthrough_status_four_states(self):
        from nonebot_plugin_xiuxian_2.features.tianti_training.presentation import (
            breakthrough_progress,
            render_breakthrough_status,
        )

        def line(**overrides):
            kwargs = dict(
                data={"tianti_hp": 500}, next_level="金刚境", next_config={"need_hp": 100},
                required_rank=10, cultivation_rank=10,
            )
            kwargs.update(overrides)
            return render_breakthrough_status(breakthrough_progress(kwargs.pop("data"), **kwargs))

        self.assertIn("现在可以突破", line())
        self.assertIn("还差60炼体气血", line(data={"tianti_hp": 40}))
        self.assertIn("修仙境界不足", line(required_rank=3))
        self.assertIn("已达炼体最高境界", line(next_level=None, next_config=None))


if __name__ == "__main__":
    unittest.main()
