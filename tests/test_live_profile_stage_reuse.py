from __future__ import annotations

import ast
from pathlib import Path
import sqlite3
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock


SOURCE = Path(__file__).resolve().parents[1] / "nonebot_plugin_xiuxian_2/xiuxian"
BREAKTHROUGHS = (
    ("level_up_", "level_up"),
    ("level_up_lx_continuous", "level_up_lx"),
    ("level_up_drjd_", "level_up_drjd"),
    ("level_up_dr_", "level_up_dr"),
    ("level_up_dr_lx_continuous", "level_up_dr_lx"),
    ("level_up_drjd_lx_continuous", "level_up_drjd_lx"),
)


class FinishedStage(Exception):
    pass


class LiveProfileStageReuseTests(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        cls.handlers = {}
        for relative in ("xiuxian_buff/__init__.py", "xiuxian_base/breakthrough_tribulation.py"):
            path = SOURCE / relative
            tree = ast.parse(path.read_text(encoding="utf-8"))
            for node in tree.body:
                if isinstance(node, ast.AsyncFunctionDef):
                    node.decorator_list = []
                    cls.handlers[node.name] = compile(
                        ast.Module(body=[node], type_ignores=[]), str(path), "exec"
                    )

    def setUp(self):
        self.database = sqlite3.connect(":memory:")
        self.database.row_factory = sqlite3.Row
        self.addCleanup(self.database.close)
        self.database.execute(
            "CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,level TEXT,exp TEXT,"
            "stone TEXT,hp INTEGER,mp INTEGER,atk INTEGER,user_stamina INTEGER,"
            "level_up_cd TEXT,level_up_rate INTEGER,root_type TEXT,root_level INTEGER)"
        )
        self.database.executemany(
            "INSERT INTO user_xiuxian VALUES(?,'realm','1000','9000',1,2,3,100,'recent',7,'root',4)",
            [("u",), ("v",)],
        )
        self.selects = []
        self.database.set_trace_callback(
            lambda sql: self.selects.append(sql) if sql.startswith("SELECT * FROM user_xiuxian") else None
        )
        self.manager = SimpleNamespace(get_user_info_with_id=Mock(side_effect=self._profile))
        self.bot = SimpleNamespace(self_id="bot")
        self.event = SimpleNamespace(message_id="event", message="出关")

    def _profile(self, user_id):
        row = self.database.execute(
            "SELECT * FROM user_xiuxian WHERE user_id=? ORDER BY rowid ASC LIMIT 1", (user_id,)
        ).fetchone()
        if row is None:
            return None
        profile = dict(row)
        profile["exp"], profile["stone"] = int(profile["exp"]), int(profile["stone"])
        profile["user_name"] = user_id
        return profile

    def _initialize(self, user_id, profile):
        self.database.execute(
            "UPDATE user_xiuxian SET hp=1,mp=22,level_up_cd='recent' WHERE user_id=?", (user_id,)
        )
        profile["hp"], profile["mp"] = 1, 22

    def _environment(self, *matchers):
        namespace = {
            "assign_bot": AsyncMock(return_value=(self.bot, None)),
            "check_user": Mock(side_effect=lambda _: (True, self._profile("u"), "")),
            "_sql_message": lambda: self.manager,
            "_initialize_player_state": Mock(side_effect=self._initialize),
            "handle_send": AsyncMock(),
            "restore_player_stamina": Mock(),
            "OtherSet": lambda: SimpleNamespace(date_diff=lambda *_: 0),
            "XiuConfig": lambda: SimpleNamespace(level_up_cd=60, max_stamina=100),
            "datetime": SimpleNamespace(now=lambda: "now"),
            "CommandArg": lambda: None,
        }
        for matcher in matchers:
            namespace[matcher] = SimpleNamespace(finish=AsyncMock(side_effect=FinishedStage))
        return namespace

    def _handler(self, name, namespace):
        exec(self.handlers[name], namespace)
        return namespace[name]

    async def _run(self, name, namespace, **arguments):
        with self.assertRaises(FinishedStage):
            await self._handler(name, namespace)(self.bot, self.event, **arguments)

    async def test_six_breakthrough_stages_reuse_profile_without_initialization(self):
        for name, matcher in BREAKTHROUGHS:
            with self.subTest(handler=name):
                self.selects.clear()
                self.manager.get_user_info_with_id.reset_mock()
                namespace = self._environment(matcher)
                await self._run(name, namespace)
                self.assertEqual(len(self.selects), 1)
                self.manager.get_user_info_with_id.assert_not_called()
                namespace["_initialize_player_state"].assert_not_called()
                self.assertIn("冷却中", namespace["handle_send"].await_args.args[2])

    async def test_six_breakthrough_stages_reread_after_initialization_write(self):
        for name, matcher in BREAKTHROUGHS:
            with self.subTest(handler=name):
                self.database.execute("UPDATE user_xiuxian SET hp=NULL,level_up_cd=NULL WHERE user_id='u'")
                self.selects.clear()
                self.manager.get_user_info_with_id.reset_mock()
                namespace = self._environment(matcher)
                await self._run(name, namespace)
                self.assertEqual(len(self.selects), 2)
                self.manager.get_user_info_with_id.assert_called_once_with("u")
                namespace["_initialize_player_state"].assert_called_once()
                self.assertIn("冷却中", namespace["handle_send"].await_args.args[2])

    async def test_direct_breakthrough_keeps_live_read_after_pending_write(self):
        self.database.execute("UPDATE user_xiuxian SET level_up_cd=NULL WHERE user_id='u'")
        application = SimpleNamespace(
            resume_pending_direct_breakthroughs=Mock(side_effect=lambda **_: self.database.execute(
                "UPDATE user_xiuxian SET level_up_cd='recent' WHERE user_id='u'"
            )),
            direct_breakthrough_replay=Mock(return_value=None),
        )
        namespace = self._environment("level_up_zj")
        namespace.update(_direct_breakthrough_application=lambda: application,
                         _breakthrough_operation_id=lambda *_: "operation")
        await self._run("level_up_zj_", namespace)
        self.manager.get_user_info_with_id.assert_called_once_with("u")
        self.assertEqual(len(self.selects), 2)
        application.resume_pending_direct_breakthroughs.assert_called_once_with(limit=5)
        self.assertIn("冷却中", namespace["handle_send"].await_args.args[2])

    async def test_probability_view_reads_fresh_per_request_without_nested_profile(self):
        for rate, expected in ((7, "22%"), (17, "32%")):
            self.database.execute("UPDATE user_xiuxian SET level_up_rate=? WHERE user_id='u'", (rate,))
            self.selects.clear()
            namespace = self._environment("user_leveluprate")
            namespace.update(
                jsondata=SimpleNamespace(level_rate_data=lambda: {"realm": 10}),
                UserBuffDate=lambda _: SimpleNamespace(get_user_main_buff_data=lambda: {"number": 5}),
            )
            await self._run("user_leveluprate_", namespace)
            self.assertIn(expected, namespace["handle_send"].await_args.args[2])
            self.assertEqual(len(self.selects), 1)
        self.manager.get_user_info_with_id.assert_not_called()

    async def test_training_reuses_exact_profile_values_for_live_settlement_request(self):
        exp, stone = 10**30 + 17, 10**31 + 19
        self.database.execute("UPDATE user_xiuxian SET exp=?,stone=? WHERE user_id='u'", (str(exp), str(stone)))
        self.manager.get_root_rate = Mock(return_value=2)
        application = SimpleNamespace(training_start=Mock(return_value=SimpleNamespace(ok=False)))
        namespace = self._environment("up_exp")
        namespace.update(
            check_user_type=lambda *_: (True, ""),
            OtherSet=lambda: SimpleNamespace(set_closing_type=lambda _: 10**33),
            XiuConfig=lambda: SimpleNamespace(closing_exp_upper_limit=1, closing_exp=1),
            jsondata=SimpleNamespace(level_data=lambda: {"realm": {"spend": 3}}),
            UserBuffDate=lambda _: SimpleNamespace(BuffInfo={"blessed_spot": 0},
                                                   get_user_main_buff_data=lambda: None),
            runtime_random=SimpleNamespace(uniform=lambda *_: 1),
            _normal_training_operation_id=lambda *_: "operation",
            _apply_spirit_vein_exp_bonus=lambda reward, _: (reward, ""),
            buff_application=application,
            result_failure=lambda *_: "settlement rejected",
        )
        await self._run("up_exp_", namespace)
        request = application.training_start.call_args.kwargs
        self.assertEqual((request["expected_exp"], request["expected_stone"], request["reward"]), (exp, stone, 6))
        self.manager.get_root_rate.assert_called_once_with("root", "u", root_level=4)
        self.assertEqual(len(self.selects), 1)
        self.manager.get_user_info_with_id.assert_not_called()
        self.assertEqual(namespace["handle_send"].await_args.args[2], "settlement rejected")

    async def test_closing_reuses_profile_only_after_nonreplay_and_preserves_live_request(self):
        self.manager.get_user_cd = Mock(return_value={"create_time": "closing"})
        application = SimpleNamespace(
            closing_replay=Mock(return_value=None),
            calculate_closing_reward=Mock(return_value=SimpleNamespace(
                exp_gain=7, stone_cost=3, new_hp=4, new_mp=5, new_atk=6, new_power=7, exp_time=8,
            )),
            closing_settle=Mock(return_value=SimpleNamespace(ok=False)),
        )
        snapshot = Mock(side_effect=lambda _, row, cd: {
            "expected_create_time": cd["create_time"], "current_exp": row["exp"], "current_stone": row["stone"],
        })
        namespace = self._environment("out_closing")
        namespace.update(
            buff_application=application, _closing_settlement_operation_id=lambda *_: "operation",
            check_user_type=lambda *_: (True, ""), _closing_reward_inputs=snapshot,
            result_failure=lambda *_: "settlement rejected",
        )
        await self._run("out_closing_", namespace)
        application.calculate_closing_reward.assert_called_once_with(current_exp=1000, current_stone=9000, stone_exit=False)
        self.assertEqual(application.closing_settle.call_args.kwargs["expected_create_time"], "closing")
        self.assertEqual(len(self.selects), 1)
        self.manager.get_user_info_with_id.assert_not_called()

    async def test_closing_replay_can_write_but_never_continues_with_initial_profile(self):
        replay = SimpleNamespace(ok=True, data={"exp_gain": 7}, message="")

        def recover(_):
            self.database.execute("UPDATE user_xiuxian SET exp='1007' WHERE user_id='u'")
            return replay

        application = SimpleNamespace(closing_replay=Mock(side_effect=recover))
        snapshot = Mock(side_effect=AssertionError("replay must not calculate a new reward"))
        namespace = self._environment("out_closing")
        namespace.update(buff_application=application, _closing_settlement_operation_id=lambda *_: "operation",
                         _closing_reward_inputs=snapshot, number_to=str)
        await self._run("out_closing_", namespace)
        snapshot.assert_not_called()
        self.manager.get_user_info_with_id.assert_not_called()
        self.assertEqual(len(self.selects), 1)
        self.assertIn("无需重复提交", namespace["handle_send"].await_args.args[2])
        self.assertEqual(self.database.execute("SELECT exp FROM user_xiuxian WHERE user_id='u'").fetchone()[0], "1007")

    async def test_pvp_keeps_target_live_and_rereads_actor_after_null_or_zero_hp_repair(self):
        for hp, actor_reads in ((1, 1), (None, 2), (0, 2)):
            with self.subTest(hp=hp):
                self.database.execute("UPDATE user_xiuxian SET hp=? WHERE user_id='u'", (hp,))
                self.selects.clear()
                self.manager.get_user_info_with_id.reset_mock()
                namespace = self._environment("qc")
                namespace.update(
                    get_at_user_id=lambda _: "v", get_player_attributes=lambda _: {"valid": True},
                    _normal_pvp_operation_id=lambda *_: "operation",
                    buff_application=SimpleNamespace(pvp_replay=Mock(return_value=None)),
                    leave_harm_time=lambda _: 0,
                )
                await self._run("qc_", namespace, args=object())
                self.assertEqual(len(self.selects), actor_reads + 1)
                self.assertEqual([call.args[0] for call in self.manager.get_user_info_with_id.call_args_list],
                                 ["v"] + (["u"] if hp in (None, 0) else []))
                self.assertIn("重伤未愈", namespace["handle_send"].await_args.args[2])


if __name__ == "__main__":
    unittest.main()
