"""Read-stage reuse must preserve iterative gains and live settlement boundaries."""
from contextlib import ExitStack
from copy import deepcopy
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch

import nonebot

try:
    nonebot.get_driver()
except ValueError:
    nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_buff import partner


class PartnerCultivationReadStageTests(unittest.IsolatedAsyncioTestCase):
    def stage(self, stack, *, exp=1000, cap=100_000):
        profiles = {uid: dict(user_id=uid, user_name=uid, exp=exp, level="test",
                             root_type="命运道果", root_level=4, hp=10, mp=20)
                    for uid in ("a", "b")}
        manager = Mock(get_user_info_with_id=Mock(side_effect=lambda uid: deepcopy(profiles[uid])),
                       get_root_rate=Mock(return_value=2))
        buffs = {uid: SimpleNamespace(BuffInfo={"blessed_spot": 2},
                    get_user_main_buff_data=Mock(return_value={
                        "two_buff": 0, "ratebuff": 1, "clo_exp": 2})) for uid in profiles}
        application = Mock()
        application.apply.return_value = SimpleNamespace(succeeded=True)
        for target, replacement in (
            ("_sql_message", Mock(return_value=manager)),
            ("UserBuffDate", Mock(side_effect=buffs.get)),
            ("get_user_profile", Mock(side_effect=lambda uid: {**profiles[uid], "user_name": "live-" + uid})),
            ("_partner_cultivation_application", Mock(return_value=application)),
            ("OtherSet", Mock(return_value=SimpleNamespace(set_closing_type=lambda _: cap))),
            ("XiuConfig", Mock(return_value=SimpleNamespace(closing_exp_upper_limit=1))),
            ("convert_rank", Mock(return_value=(3,))),
            ("handle_send", AsyncMock()), ("log_message", Mock()),
        ):
            stack.enter_context(patch.object(partner, target, replacement))
        stack.enter_context(patch.object(partner.two_exp_cd, "find_user", return_value=0))
        stack.enter_context(patch.object(partner.xiuxian_impart, "get_user_impart_info_with_id", return_value=None))
        stack.enter_context(patch.object(partner.runtime_random, "randint", return_value=100))
        stack.enter_context(patch.object(partner.runtime_random, "choice", side_effect=lambda values: values[0]))
        stack.enter_context(patch.object(partner.jsondata, "level_data", return_value={"test": {"spend": 1}}))
        return profiles, manager, buffs, application

    async def test_three_rounds_reuse_rows_but_recalculate_exp_and_reread_after_write(self):
        with ExitStack() as stack:
            profiles, manager, buffs, application = self.stage(stack)
            await partner.direct_two_exp(None, SimpleNamespace(message_id=4), "a", "b", 3,
                                         expected_target_protection="off")
            values = application.apply.call_args.kwargs
            # Each round uses accumulated exp: floor(2*exp*.005)*2*3*2.
            current = 1000
            for _ in range(3):
                current += int(int((current + current) * .005) * 2 * 3 * 2)
            self.assertEqual(values["exp_1"], current - 1000)
            self.assertEqual(values["exp_2"], current - 1000)
            self.assertEqual((values["expected_exp_1"], values["expected_exp_2"]), (1000, 1000))
            self.assertEqual((values["used_count"], values["expected_used_count_1"],
                              values["expected_used_count_2"]), (3, 0, 0))
            self.assertEqual(values["expected_target_protection"], "off")
            self.assertEqual(manager.get_user_info_with_id.call_count, 2)
            self.assertEqual(partner.UserBuffDate.call_count, 2)
            self.assertTrue(all(buff.get_user_main_buff_data.call_count == 1 for buff in buffs.values()))
            self.assertEqual(partner.get_user_profile.call_count, 2)
            self.assertIn("live-a获得修为", partner.handle_send.await_args.args[2])
            self.assertEqual(profiles["a"]["exp"], 1000)
            manager.get_root_rate.assert_any_call("命运道果", "a", root_level=4)

    async def test_cap_stops_later_rounds_without_spending_extra_usage(self):
        with ExitStack() as stack:
            _, _, _, application = self.stage(stack, exp=990, cap=1000)
            await partner.direct_two_exp(None, SimpleNamespace(message_id=5), "a", "b", 3)
            values = application.apply.call_args.kwargs
            self.assertEqual((values["exp_1"], values["exp_2"], values["used_count"]), (10, 10, 1))
            self.assertIn("已达上限", partner.handle_send.await_args.args[2])

    async def test_live_single_round_wrapper_keeps_cap_and_missing_read_behavior(self):
        with ExitStack() as stack:
            profiles, manager, buffs, _ = self.stage(stack, exp=1000, cap=1000)
            partner.get_user_profile.side_effect = lambda uid: profiles[uid]
            one, two, reason = await partner.process_two_exp("a", "b")
            self.assertEqual((one, two), (0, 0))
            self.assertIn("已达上限", reason)
            self.assertEqual(manager.get_user_info_with_id.call_count, 2)
            partner.UserBuffDate.assert_not_called()
            self.assertTrue(all(buff.get_user_main_buff_data.call_count == 0 for buff in buffs.values()))

    async def test_special_partner_rounds_and_live_wrapper_match_reused_inputs(self):
        with ExitStack() as stack:
            profiles, _, buffs, _ = self.stage(stack, exp=10**24, cap=10**24 + 100)
            partner.get_user_profile.side_effect = lambda uid: profiles[uid]
            partner.runtime_random.randint.return_value = 1
            actual = await partner.process_two_exp("a", "b", is_partner=True)
            reused = partner._process_two_exp_from_profiles(
                profiles["a"], profiles["b"], profiles["a"], profiles["b"], is_partner=True,
                buff_inputs=(buffs["a"].get_user_main_buff_data(),
                             buffs["b"].get_user_main_buff_data(), 1, 1))
            self.assertEqual(actual, reused)
            self.assertEqual(actual[:2], (100, 100))
            self.assertIn("天降异象", actual[2])

    async def test_failed_live_settlement_does_not_reread_or_emit_success(self):
        with ExitStack() as stack:
            _, _, _, application = self.stage(stack)
            application.apply.return_value = SimpleNamespace(
                succeeded=False, subject="a", reason="exp_changed", status="state_changed")
            await partner.direct_two_exp(None, SimpleNamespace(message_id=6), "a", "b", 3, invite_id="invite")
            partner.get_user_profile.assert_not_called()
            self.assertEqual(application.apply.call_args.kwargs["invite_id"], "invite")
            self.assertNotIn("获得修为", partner.handle_send.await_args.args[2])


if __name__ == "__main__":
    unittest.main()
