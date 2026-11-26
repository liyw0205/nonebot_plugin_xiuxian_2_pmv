"""Live relation displays retain backlinks, roles and exact personal data."""
from contextlib import ExitStack
from datetime import datetime, timezone
import unittest
from unittest.mock import AsyncMock, Mock, patch


def modules():
    import nonebot
    try:
        nonebot.get_driver()
    except ValueError:
        nonebot.init(log_level="ERROR")
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_buff import partner, partner_storage
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_info import user_info
    return partner, partner_storage, user_info


class DisplayBacklinkTests(unittest.TestCase):
    def test_backlinks_preserve_duplicates_order_and_historical_fanout(self):
        _, storage, _ = modules()
        manager = Mock()
        rows = {str(i): {"mentor_id": "m"} for i in range(7)}
        rows.update(wrong={"mentor_id": "other"}, unbound={"mentor_id": " None "})
        manager.get_fields_by_ids.return_value = rows
        original = ["4", "wrong", "0", "4", "missing", "unbound", "null", "1", "2", "3", "5", "6"]
        with patch.object(storage, "_player_data_manager", return_value=manager):
            self.assertEqual(["4", "0", "4", "1", "2", "3", "5", "6"],
                             storage.get_display_apprentice_ids("m", {"apprentice_ids": original}))
            manager.get_fields_by_ids.assert_called_once_with(
                "mentor", [value for value in original if value != "null"],
                ("mentor_id",), ensure_fields=False)
            manager.get_fields_by_ids.reset_mock()
            self.assertEqual([], storage.get_display_apprentice_ids("m", {}))
            manager.get_fields_by_ids.assert_not_called()
            manager.get_fields_by_ids.side_effect = RuntimeError("read denied")
            with self.assertRaisesRegex(RuntimeError, "read denied"):
                storage.get_display_apprentice_ids("m", {"apprentice_ids": ["4"]})

    def test_cooldown_snapshot_reuse_does_not_change_default_live_action_read(self):
        partner, _, _ = modules()
        until = {"mentor_cd_until": "2030-01-01 00:00:00"}
        with (patch.object(partner, "load_mentor", return_value=until) as reader,
              patch.object(partner, "runtime_clock") as clock):
            clock.now.return_value = datetime(2029, 12, 31, tzinfo=timezone.utc)
            self.assertEqual(86400, partner._cooldown_remaining("m", "mentor_cd_until", mentor_data=until))
            self.assertEqual(0, partner._cooldown_remaining("m", "mentor_cd_until", mentor_data={}))
            reader.assert_not_called()
            self.assertEqual(86400, partner._cooldown_remaining("m", "mentor_cd_until"))
            reader.assert_called_once_with("m")


class RelationHandlerReadTests(unittest.IsolatedAsyncioTestCase):
    async def test_partner_reuses_one_current_profile_and_answers_missing_profile(self):
        partner, _, _ = modules()
        from nonebot.exception import FinishedException
        from nonebot_plugin_xiuxian_2.core.errors import DomainError
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import utils
        async def finish():
            raise FinishedException
        with (patch.object(partner, "assign_bot", AsyncMock(return_value=("bot", "group"))),
              patch.object(partner, "check_user", return_value=(True, {"user_id": "m"}, "")),
              patch.object(partner, "load_partner", return_value={"partner_id": "p", "bind_time": None, "affection": 10}),
              patch.object(partner, "get_user_profile") as reader,
              patch.object(partner, "_sql_message", side_effect=AssertionError("duplicate full profile")),
              patch.object(partner, "handle_send", AsyncMock()) as sender,
              patch.object(utils, "handle_send", AsyncMock()) as failure_sender,
              patch.object(partner.my_partner, "finish", side_effect=finish)):
            for profile in ({"user_name": "Partner", "level": "Realm", "exp": 2**80 + 1},
                            {"user_name": "Updated", "level": "NewRealm", "exp": 21}, None):
                reader.return_value = profile
                with self.assertRaises(FinishedException):
                    await partner.my_partner_("bot", "event")
                text = sender.call_args.args[2]
                self.assertIn(profile["user_name"] if profile else "道侣档案不存在", text)
                reader.assert_called_with("p", raise_errors=True)
            self.assertEqual(3, reader.call_count)
            reader.side_effect = DomainError(
                "profile_read_failed", "read denied",
                {"reason": "profile_read_failed", "subject": "player_profile"},
            )
            await partner.my_partner_("bot", "event")
            text = failure_sender.call_args.args[2]
            self.assertIn("玩家档案读取失败", text)
            self.assertNotIn("档案不存在", text)
            reader.side_effect = None
            reader.return_value = {"user_name": "Recovered", "level": "Realm", "exp": 21}
            with self.assertRaises(FinishedException):
                await partner.my_partner_("bot", "event")
            self.assertIn("Recovered", sender.call_args.args[2])

    async def test_mentor_projection_reuses_self_and_reports_batch_failure_then_recovers(self):
        partner, _, _ = modules()
        from nonebot.exception import FinishedException
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import utils
        async def finish():
            raise FinishedException
        own = {"mentor_id": "p", "apprentice_ids": ["a", "a"], "mentor_protect": "off"}
        profiles = {"p": {"user_name": "Parent", "level": "R1"},
                    "a": {"user_name": "", "level": "R2"}}
        with (patch.object(partner, "assign_bot", AsyncMock(return_value=("bot", "group"))),
              patch.object(partner, "check_user", return_value=(True, {"user_id": "m"}, "")),
              patch.object(partner, "load_mentor", return_value=own) as self_reader,
              patch.object(partner, "get_display_apprentice_ids", return_value=["a", "a"]),
              patch.object(partner, "get_user_display_profiles_by_ids", return_value=profiles) as reader,
              patch.object(partner, "get_user_profile", side_effect=AssertionError("full related profile")),
              patch.object(partner, "_sql_message", side_effect=AssertionError("cold manager")),
              patch.object(partner, "_player_data_manager", return_value=Mock()),
              patch.object(partner, "_get_remaining_mentor_transmission", return_value=1),
              patch.object(partner, "_count_pending_mentor_invites", return_value=2),
              patch.object(partner, "handle_send", AsyncMock()) as sender,
              patch.object(utils, "handle_send", AsyncMock()) as failure_sender,
              patch.object(partner.my_mentor, "finish", side_effect=finish)):
            reader.side_effect = RuntimeError("batch unavailable")
            await partner.my_mentor_("bot", "event", ("我的师徒",))
            self.assertIn("我的师徒", failure_sender.call_args.args[2])
            reader.side_effect = None
            for command in ("我的师徒", "我的徒弟", "我的师父", "我的师傅"):
                with self.assertRaises(FinishedException):
                    await partner.my_mentor_("bot", "event", (command,))
                text = sender.call_args.args[2]
                self.assertIn("Parent" if command != "我的徒弟" else "徒弟（2/", text)
                if command in {"我的师徒", "我的徒弟"}:
                    self.assertEqual(2, text.count("- （R2）"))
            self.assertEqual(5, self_reader.call_count)
            self.assertEqual(5, reader.call_count)


class ProfileRelationProjectionTests(unittest.IsolatedAsyncioTestCase):
    async def test_profile_batches_names_with_missing_empty_and_cross_role_rows(self):
        _, _, info = modules()
        own = {"user_id": "m", "user_name": "Self", "id": 1, "level": "江湖好手", "root_type": "天灵根",
               "root": "金", "sect_id": None, "exp": 100, "stone": 200, "atk": 30, "level_up_rate": 0,
               "atkpractice": 0, "hppractice": 0, "mppractice": 0}
        manager = Mock()
        manager.get_exp_and_stone_ranks.return_value = (1, 2)
        manager.get_root_rate.return_value = 1
        manager.get_level_power.return_value = 1000
        manager.get_user_names_by_ids.return_value = {"p": "", "a": "A", "b": "B"}
        buff = Mock()
        for method in ("main_buff_data", "sub_buff_data", "sec_buff_data", "effect1_buff_data",
                       "effect2_buff_data", "weapon_data", "armor_buff_data"):
            getattr(buff, "get_user_" + method).return_value = None
        with ExitStack() as stack:
            values = {"_sql_message": manager, "get_player_attributes": {"final_atk": 30},
                      "OtherSet": Mock(level=["江湖好手", "筑基"]), "UserBuffDate": buff,
                      "load_partner": {"partner_id": "p", "affection": 0},
                      "load_mentor": {"mentor_id": "p", "apprentice_ids": ["a", "p", "a", "missing", "b"]},
                      "get_display_apprentice_ids": ["a", "p", "a", "missing", "b"],
                      "get_equipped_title_display": "", "update_last_check_info_time": None}
            for name, value in values.items():
                stack.enter_context(patch.object(info, name, return_value=value))
            get_full = stack.enter_context(patch.object(info, "get_user_profile", side_effect=AssertionError("N+1")))
            stack.enter_context(patch.object(info.NatalTreasure, "read_existing_data", return_value=None))
            stack.enter_context(patch.object(info.jsondata, "level_data", return_value={"江湖好手": {"spend": 1}}))
            stack.enter_context(patch.object(info.jsondata, "level_rate_data", return_value={"江湖好手": 0}))
            detail, text = await info.get_user_xiuxian_info("m", user_info=own)
            self.assertEqual("", detail["师父"])
            self.assertEqual("A、、A等4人", detail["徒弟"])
            self.assertTrue(detail["道侣"].startswith(" ("))
            self.assertIn("修为排行: 第1位", text)
            manager.get_user_names_by_ids.assert_called_once_with(["p", "p", "a", "p", "a", "missing", "b"])
            manager.get_user_names_by_ids.return_value = {}
            detail, _ = await info.get_user_xiuxian_info("m", user_info=own)
            self.assertEqual(("无", "数据异常", "无"), (detail["道侣"], detail["师父"], detail["徒弟"]))
            get_full.assert_not_called()
