"""Reproduce command wiring failures through the actual registry, DI and storage."""
from __future__ import annotations

import asyncio
import json
import sqlite3
import unittest
from unittest.mock import patch

from tests import _data_dir
from tests.command_probe import FixtureSnapshot, inventory, prepare, run_case, isolate_source, install_boundary, check_routes


class RegisteredCommandDiagnosticsTests(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        cls.context, cls.managers = asyncio.run(prepare())
        cls.entries = inventory()
        cls.snapshot = FixtureSnapshot(_data_dir, cls.managers)

    async def asyncSetUp(self):
        self.snapshot.restore()

    async def test_aliases_and_unknown_command_routing(self):
        result = await check_routes(self.entries)
        self.assertEqual(result["unknown_command_matches"], [])
        self.assertTrue(result["routes"])
        self.assertEqual([row for row in result["routes"] if not row["selected"]], [])

    async def test_migrated_commands_use_real_gateway_and_bare_work_replies(self):
        for name in ("卜卦", "签到", "补签", "送灵石", "赠送流水", "悬赏令"):
            with self.subTest(command=name):
                self.snapshot.restore()
                await self.command(name)

    async def test_activity_collect_conversion_runs_registered_handler(self):
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_activity import activity_config

        config = activity_config._load_default_config()
        key = config["gameplay_activities"][0]["key"]
        with sqlite3.connect(self.context.paths.game_db) as conn:
            conn.execute(
                "INSERT INTO activity_collect_inventory(activity_key,user_id,word_char,count) "
                "VALUES(?,'900001','福',6) ON CONFLICT(activity_key,user_id,word_char) DO UPDATE SET count=6",
                (key,),
            )
        with patch.object(activity_config, "load_config", return_value=config):
            result = await self.command("活动换字", text="活动换字 福 运")
            self.assertIn("换字成功", "\n".join(result["replies"]))
            replay = await self.command("活动换字", text="活动换字 福 运")
            self.assertIn("换字成功", "\n".join(replay["replies"]))
            denied = await self.command("活动换字", text="集字换字 福 运", number=900101)
            self.assertIn("源字【福】不足", "\n".join(denied["replies"]))
        with sqlite3.connect(self.context.paths.game_db) as conn:
            tokens = dict(conn.execute(
                "SELECT word_char,count FROM activity_collect_inventory WHERE activity_key=? AND user_id='900001'",
                (key,),
            ))
            self.assertEqual({"福": 1, "运": 1}, tokens)
            self.assertEqual(1, conn.execute("SELECT COUNT(*) FROM activity_collect_synthesis_operations").fetchone()[0])

    def update_player(self, **values):
        game = self.managers[0]
        game.conn.execute(
            "UPDATE user_xiuxian SET " + ",".join(f"{name}=?" for name in values) + " WHERE user_id='900001'",
            tuple(values.values()),
        )
        game.conn.commit()
        game._clear_read_cache()

    def assets(self):
        with sqlite3.connect(self.context.paths.game_db) as conn:
            return conn.execute("SELECT exp,stone FROM user_xiuxian WHERE user_id='900001'").fetchone()

    async def command(self, name, *, text=None, scenario="missing", number=900100, expected="pass"):
        entry, = [entry for entry in self.entries if entry["command"] == name]
        result = await run_case(entry, scenario, number, input_text=text)
        self.assertEqual(result["status"], expected, json.dumps(result, ensure_ascii=False))
        return result

    async def test_rename_rejections_preserve_the_actual_reason(self):
        for name, text, hint in (
            ("修仙改名", "修仙改名", "灵石不足"),
            ("修仙改名", "修仙改名 新道号", "易名符"),
            ("灵根改名", "灵根改名 新灵根", "灵根改名卡"),
        ):
            with self.subTest(command=text):
                self.snapshot.restore()
                before = self.assets()
                result = await self.command(name, text=text)
                self.assertIn(hint, "\n".join(result["replies"]))
                self.assertEqual(self.assets(), before)

    async def test_rename_with_item_reaches_real_repository(self):
        self.managers[0].send_back("900001", 20011, "道号改名卡", "特殊道具", 1)
        result = await self.command("修仙改名", text="修仙改名 新道号")
        self.assertIn("道号更新", "\n".join(result["replies"]))
        self.assertEqual(self.managers[0].get_user_info_with_id("900001")["user_name"], "新道号")

    async def test_backpack_repair_accepts_feature_result(self):
        result = await self.command("背包检测", scenario="admin_missing")
        self.assertTrue(result["replies"])

    async def test_blessed_spot_purchase_charges_once(self):
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_buff as buff

        self.update_player(stone=buff.BLESSEDSPOTCOST + 100)
        await self.command("洞天福地购买")
        self.assertEqual(self.assets()[1], 100)
        await self.command("洞天福地购买")
        self.assertEqual(self.assets()[1], 100)

    async def test_stone_training_success_and_replay_do_not_charge_twice(self):
        self.update_player(exp=0, stone=1000)
        result = await self.command("灵石修炼", text="灵石修炼 100")
        first = self.assets()
        self.assertIn("修炼结束", "\n".join(result["replies"]))
        self.assertGreater(first[0], 0)
        self.assertLess(first[1], 1000)
        repeat = await self.command("灵石修炼", text="灵石修炼 100")
        self.assertIn("已经处理", "\n".join(repeat["replies"]))
        self.assertEqual(self.assets(), first)

    async def test_normal_training_runs_completion_after_fixture_wait(self):
        self.update_player(exp=0)
        result = await self.command("修炼")
        self.assertIn("修炼结束", "\n".join(result["replies"]))
        self.assertGreater(self.assets()[0], 0)

    async def test_no_pet_or_unknown_uid_is_a_rejection_not_unpack_error(self):
        for text in ("宠物启明", "宠物启明 unknown-pet"):
            with self.subTest(text=text):
                result = await self.command("宠物启明", text=text)
                self.assertIn("未找到可启明的宠物", "\n".join(result["replies"]))

    async def test_existing_pet_reroll_checks_item_then_applies(self):
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import pet_system

        pet = pet_system.create_pet_instance(next(iter(pet_system.load_pet_pool().values())))
        pet_system.grant_pet_as_active("900001", pet)
        result = await self.command("宠物启明")
        self.assertIn("没有启明石", "\n".join(result["replies"]))
        self.managers[0].send_back("900001", 20032, "启明石", "特殊道具", 1)
        result = await self.command("宠物启明", number=900101)
        self.assertIn("启明成功", "\n".join(result["replies"]))
        self.assertEqual(self.managers[0].goods_num("900001", 20032), 0)

    async def test_qiaoxue_missing_profile_and_existing_profile(self):
        result = await self.command("冲窍")
        self.assertIn("未找到炼体档案", "\n".join(result["replies"]))
        from nonebot_plugin_xiuxian_2.features.tianti_training.repository import TiantiBreakthroughSqlRepository

        TiantiBreakthroughSqlRepository(self.context.paths.player_db).ensure_profile("900001")
        with sqlite3.connect(self.context.paths.player_db) as conn:
            conn.execute("UPDATE tianti_info SET tianti_hp=1000 WHERE user_id='900001'")
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.lay_out import _shutdown_cooldown_runtimes

        _shutdown_cooldown_runtimes()
        result = await self.command("冲窍", number=900101)
        self.assertIn("冲窍成功", "\n".join(result["replies"]))
        with sqlite3.connect(self.context.paths.player_db) as conn:
            self.assertEqual(int(conn.execute("SELECT tianti_hp FROM tianti_info WHERE user_id='900001'").fetchone()[0]), 900)

    async def test_season_rank_reads_weekly_fallback_without_hidden_exception(self):
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_base as base

        week = base.get_current_season("weekly").key
        with sqlite3.connect(self.context.paths.game_db) as conn:
            conn.execute("INSERT INTO sect_weekly_goal(sect_id,week_key,goal_key,progress,target) VALUES(1,?,'probe',7,10)", (week,))
        rows = base._get_sect_weekly_rank_fallback()
        self.assertEqual(int(rows[0]["total_progress"]), 7)
        await self.command("赛季榜")

    async def test_sect_help_and_membership_preconditions(self):
        await self.command("宗门帮助")
        result = await self.command("宗门职位变更")
        self.assertIn("还未加入", "\n".join(result["replies"]))
        self.update_player(sect_id=1, sect_position=4)
        result = await self.command("宗门职位变更")
        self.assertIn("无权", "\n".join(result["replies"]))
        self.update_player(sect_position=0)
        result = await self.command("宗门职位变更")
        self.assertIn("请输入正确指令", "\n".join(result["replies"]))

    async def test_auction_quota_uses_the_feature_queue(self):
        with sqlite3.connect(self.context.paths.trade_db) as conn:
            conn.execute("INSERT INTO auction_player_upload VALUES(?,?,?,?,?)", ("900001", 1, "诊断拍品", 600000, "诊断道友"))
        result = await self.command("拍卖活动")
        self.assertIn("等待区1/", "\n".join(result["replies"]))

    async def test_legacy_sync_empty_and_partner_file(self):
        result = await self.command("player数据同步", scenario="admin_missing")
        self.assertIn("同步完成", "\n".join(result["replies"]))
        directory = self.context.paths.players / "900001"
        directory.mkdir(parents=True, exist_ok=True)
        (directory / "partner.json").write_text(json.dumps({"partner_id": "900002", "affection": 3, "bind_time": None}), encoding="utf-8")
        await self.command("player数据同步", scenario="admin_missing", number=900101)
        with sqlite3.connect(self.context.paths.player_db) as conn:
            self.assertEqual(conn.execute("SELECT partner_id FROM partner WHERE user_id='900001'").fetchone()[0], "900002")

    async def test_changelog_empty_and_image_paths_preserve_finish_flow(self):
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_info import changelog_command as module

        with patch.object(module, "get_commits", return_value=[]):
            result = await self.command("更新日志")
            self.assertIn("无法获取更新日志", "\n".join(result["replies"]))
        self.snapshot.restore()
        with patch.object(module, "get_commits", return_value=[{"message": "fixture"}]), patch.object(module, "create_changelog_image", return_value=b"fixture-image"):
            await self.command("更新日志")

    async def test_changelog_delivery_exception_remains_a_failed_diagnostic(self):
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_info import changelog_command as module

        with patch.object(module, "get_commits", return_value=[{"message": "fixture"}]), patch.object(module, "create_changelog_image", return_value=b"fixture-image"), patch.object(module, "handle_pic_send", side_effect=RuntimeError("fixture delivery down")):
            result = await self.command("更新日志", expected="exception")
        log = next(log for log in result["logs"] if "[command] 更新日志" in log["message"])
        self.assertIn("actor=900001", log["message"])
        self.assertIn("fixture delivery down", log["exception"])
        self.assertIn("定位号", "\n".join(result["replies"]))

    async def test_xianshi_rejects_missing_listing_and_success_replays_once(self):
        result = await self.command("仙肆购买", text="仙肆购买 absent")
        self.assertIn("未找到仙肆ID", "\n".join(result["replies"]))
        with sqlite3.connect(self.context.paths.game_db) as conn:
            conn.execute("INSERT INTO xianshi_item(id,user_id,goods_id,name,type,price,quantity) VALUES('probe','900002',20032,'启明石','特殊道具',100,1)")
        result = await self.command("仙肆购买", text="仙肆购买 probe 1", number=900101)
        self.assertIn("成功", "\n".join(result["replies"]))
        after = self.assets()
        await self.command("仙肆购买", text="仙肆购买 probe 1", number=900101)
        self.assertEqual(self.assets(), after)

    async def test_diagnostic_fails_even_when_command_guard_replies(self):
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_buff as buff

        self.update_player(exp=0)
        with patch.object(buff.buff_application, "stone_training", side_effect=ValueError("diagnostic injected failure")):
            result = await self.command("灵石修炼", text="灵石修炼 100", expected="exception")
        log = next(log for log in result["logs"] if "[command]" in log["message"])
        self.assertIn("灵石修炼", log["message"])
        self.assertIn("actor=900001", log["message"])
        self.assertIn("subject=actor", log["message"])
        self.assertIn("ValueError", log["exception"])
        self.assertTrue(any("Traceback" in error and "diagnostic injected failure" in error for error in result["errors"]))

    async def test_unhandled_exception_is_logged_with_command_actor_and_stack(self):
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_sect as sect

        with patch.object(sect, "paginate_text_blocks", side_effect=ValueError("unhandled fixture failure")):
            result = await self.command("宗门帮助", expected="exception")
        log = next(log for log in result["logs"] if "phase=未捕获异常" in log["message"])
        self.assertIn("宗门帮助", log["message"])
        self.assertIn("actor=900001", log["message"])
        self.assertIn("ValueError", log["exception"])

    async def test_unretrieved_background_failure_fails_the_diagnostic(self):
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_sect as sect

        paginate = sect.paginate_text_blocks

        async def fail():
            await asyncio.sleep(0)
            raise ValueError("unretrieved background fixture failure")

        def start_background(*args, **kwargs):
            asyncio.create_task(fail())
            return paginate(*args, **kwargs)

        with patch.object(sect, "paginate_text_blocks", side_effect=start_background):
            result = await self.command("宗门帮助", expected="exception")
        self.assertTrue(any("unretrieved background fixture failure" in error for error in result["errors"]))
        log = next(log for log in result["logs"] if "phase=未捕获后台异常" in log["message"])
        self.assertIn("宗门帮助 actor=900001 subject=actor", log["message"])
        self.assertIn("ValueError", log["exception"])

        async def handled():
            try:
                await asyncio.create_task(fail())
            except ValueError:
                pass

        def start_handled(*args, **kwargs):
            asyncio.create_task(handled())
            return paginate(*args, **kwargs)

        with patch.object(sect, "paginate_text_blocks", side_effect=start_handled):
            await self.command("宗门帮助", number=900101)


if __name__ == "__main__":
    isolate_source()
    install_boundary(_data_dir)
    unittest.main()
