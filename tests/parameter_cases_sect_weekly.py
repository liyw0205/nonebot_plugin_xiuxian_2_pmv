"""Real command-path coverage for sect weekly reward claims."""
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from tests.parameter_cases_sect_body import seed_sect, sect_module
from tests.parameter_cases_guishi_return import integrity


def weekly_manager():
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_sect.sect_weekly import (
        SECT_WEEKLY_GOALS,
    )
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_sect.sect_weekly_commands import (
        sect_weekly_goal_manager,
    )

    return SECT_WEEKLY_GOALS, sect_weekly_goal_manager


def complete_goals(probe, sect_id, definitions=None):
    goals, manager = weekly_manager()
    for goal in definitions or goals:
        changed = manager.record_event(
            "900001", goal.event_keys[0], goal.target, {"sect_id": sect_id}
        )
        assert any(row["goal_key"] == goal.key and row["completed"] for row in changed), (
            goal.key,
            changed,
        )
    return goals, manager, manager.current_week_key()


def weekly_snapshot(probe, sect_id, week_key):
    game, player = probe.context.paths.game_db, probe.context.paths.player_db
    return (
        probe.rows(game, "SELECT stone,exp,sect_contribution FROM user_xiuxian WHERE user_id='900001'"),
        probe.rows(game, "SELECT sect_scale,sect_materials FROM sects WHERE sect_id=?", (sect_id,)),
        probe.rows(game, "SELECT goods_id,goods_num,bind_num FROM back WHERE user_id='900001' ORDER BY goods_id"),
        probe.rows(
            game,
            "SELECT goal_key,claimed_users FROM sect_weekly_goal WHERE sect_id=? AND week_key=? ORDER BY goal_key",
            (sect_id, week_key),
        ),
        probe.rows(game, "SELECT operation_id,payload,result_json FROM sect_weekly_reward_operations ORDER BY operation_id"),
        probe.rows(player, "SELECT integral FROM boss_limit WHERE user_id='900001'"),
    )


async def transport_contract(reply):
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import utils

    event = SimpleNamespace(get_user_id=lambda: "900001")
    for onebot, markdown, send_fails in (
        (True, True, False),
        (False, False, False),
        (False, True, False),
        (False, True, True),
    ):
        bot = SimpleNamespace(
            adapter=SimpleNamespace(get_name=lambda: "OneBot V11" if onebot else "QQ")
        )
        config = SimpleNamespace(
            markdown_status=markdown,
            markdown_id="",
            markdown_id2="",
            markdown_button_status=False,
            at_sender=False,
            message_optimization=False,
            img=False,
        )
        plain = AsyncMock()
        markdown_send = AsyncMock(
            side_effect=RuntimeError("isolated weekly reply failure") if send_fails else None
        )
        with patch.object(utils, "XiuConfig", return_value=config), patch.object(
            utils, "get_chat_scene", return_value="group"
        ), patch.object(utils, "is_group_event", return_value=True), patch.object(
            utils, "get_real_id_async", new=AsyncMock(return_value="")
        ), patch.object(utils, "_send_event_message", plain), patch.object(
            utils, "_send_markdown_or_keyboard", markdown_send
        ):
            await utils.handle_send(
                bot, event, reply, md_type="宗门", k1="周常", v1="宗门周常"
            )
        if onebot or not markdown or send_fails:
            plain.assert_awaited_once()
            assert plain.call_args.kwargs["message"] == reply
        else:
            markdown_send.assert_awaited_once()
            rendered = str(markdown_send.call_args.args[2])
            assert "领取成功" in rendered or "写入未完成" in rendered
        if onebot:
            markdown_send.assert_not_awaited()


def register(case):
    @case(
        "sect-weekly-full-claim-and-replay",
        "领取宗门周常",
        "无参数全领",
        "真实宗门和本周全部五项目标已完成",
        "全部玩家/宗门/背包/BOSS积分/领取标记与永久回执写入，应用重放零重复",
        group="sect-weekly-claim",
    )
    async def full_claim(probe):
        module = sect_module()
        definitions, manager = weekly_manager()
        sect_id = seed_sect(probe)
        definitions, manager, week_key = complete_goals(probe, sect_id, definitions)
        before = weekly_snapshot(probe, sect_id, week_key)

        reply = await probe.command("领取宗门周常", number=986001)
        assert "领取成功" in reply and "宗门贡献" in reply and "宗门资材" in reply, reply
        claimed = probe.rows(
            probe.context.paths.game_db,
            "SELECT goal_key,claimed_users FROM sect_weekly_goal WHERE sect_id=? AND week_key=? ORDER BY goal_key",
            (sect_id, week_key),
        )
        assert len(claimed) == len(definitions) and all(
            "900001" in row[1] for row in claimed
        ), (reply, claimed)
        assert len(probe.rows(
            probe.context.paths.game_db,
            "SELECT operation_id FROM sect_weekly_reward_operations",
        )) == 1
        assert probe.rows(
            probe.context.paths.game_db,
            "SELECT goods_id,goods_num,bind_num FROM back WHERE user_id='900001' AND goods_id IN (18172,20015) ORDER BY goods_id",
        ) == [(18172, 1, 1), (20015, 2, 2)]
        assert probe.rows(
            probe.context.paths.player_db,
            "SELECT integral FROM boss_limit WHERE user_id='900001'",
        )[0][0] == 0

        after = weekly_snapshot(probe, sect_id, week_key)
        operation_id = "sect-weekly:986001:900001:all"
        command_module = __import__(
            "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_sect.sect_weekly_commands",
            fromlist=["_prepare_claim_goals"],
        )
        raw_goals = manager.list_goals(sect_id, week_key)
        direct_replay = module.sect_application.claim_weekly(
            operation_id=operation_id,
            user_id="900001",
            sect_id=sect_id,
            week_key=week_key,
            goals=command_module._prepare_claim_goals(raw_goals),
            max_goods_num=module.XiuConfig().max_goods_num,
        )
        assert direct_replay.ok and direct_replay.replayed
        assert direct_replay.data["status"] == "duplicate"
        assert weekly_snapshot(probe, sect_id, week_key) == after
        await transport_contract(reply)
        integrity(probe)
        probe.evidence.update(
            completed_goals=len(definitions),
            user_asset_delta=(after[0], before[0]),
            claimed_goals=len(claimed),
            operation_id=operation_id,
            replay_zero_write=True,
            adapter_contracts=4,
            integrity="ok",
        )

    @case(
        "sect-weekly-name-boundaries-and-single-claim",
        "领取宗门周常",
        "模糊目标名、未知目标、未完成目标、重复领取",
        "真实宗门，同门勤务完成且广纳资粮仅部分进度",
        "仅指定目标领取，所有拒绝保留具体提示且不产生额外写入",
        group="sect-weekly-claim",
        outcome="success",
    )
    async def name_and_boundaries(probe):
        definitions, manager = weekly_manager()
        sect_id = seed_sect(probe)
        diligence = next(goal for goal in definitions if goal.key == "sect_diligence")
        supply = next(goal for goal in definitions if goal.key == "sect_supply")
        complete_goals(probe, sect_id, (diligence,))
        manager.record_event("900001", supply.event_keys[0], 1, {"sect_id": sect_id})
        week_key = manager.current_week_key()
        manager.list_goals(sect_id, week_key)
        before = weekly_snapshot(probe, sect_id, week_key)

        unknown = await probe.command(
            "领取宗门周常", "领取宗门周常 不存在的目标", number=986011
        )
        assert "未找到对应宗门周常目标" in unknown and weekly_snapshot(probe, sect_id, week_key) == before
        incomplete = await probe.command(
            "领取宗门周常", "领取宗门周常 广纳资粮", number=986012
        )
        assert "尚未完成或已经领取" in incomplete
        assert weekly_snapshot(probe, sect_id, week_key) == before

        reply = await probe.command(
            "领取宗门周常", "领取宗门周常 勤务", number=986013
        )
        assert "领取成功" in reply and "同门勤务" in reply, reply
        after = weekly_snapshot(probe, sect_id, week_key)
        rows = probe.rows(
            probe.context.paths.game_db,
            "SELECT goal_key,claimed_users FROM sect_weekly_goal WHERE sect_id=? AND week_key=? ORDER BY goal_key",
            (sect_id, week_key),
        )
        assert next(row[1] for row in rows if row[0] == "sect_diligence") == '["900001"]'
        assert next(row[1] for row in rows if row[0] == "sect_supply") == "[]"

        repeated = await probe.command(
            "领取宗门周常", "领取宗门周常 同门勤务", number=986014
        )
        assert "已经领取" in repeated and weekly_snapshot(probe, sect_id, week_key) == after
        await transport_contract(reply)
        integrity(probe)
        probe.evidence.update(
            alias="勤务",
            unknown_rejected=True,
            incomplete_rejected=True,
            repeat_zero_write=True,
            selected_goal="sect_diligence",
            adapter_contracts=4,
            integrity="ok",
        )

    @case(
        "sect-weekly-after-leaving-sect",
        "领取宗门周常",
        "退宗后指定目标",
        "周常目标已完成，玩家随后退出宗门",
        "明确提示尚未入宗，资产、目标领取状态和回执不变",
        group="sect-weekly-claim",
    )
    async def after_leaving_sect(probe):
        definitions, _manager = weekly_manager()
        sect_id = seed_sect(probe)
        diligence = next(goal for goal in definitions if goal.key == "sect_diligence")
        _definitions, manager, week_key = complete_goals(probe, sect_id, (diligence,))
        manager.list_goals(sect_id, week_key)
        game = probe.context.paths.game_db
        probe.seed(
            game,
            "UPDATE user_xiuxian SET sect_id=NULL,sect_position=NULL WHERE user_id='900001'",
        )
        before = weekly_snapshot(probe, sect_id, week_key)

        reply = await probe.command(
            "领取宗门周常", "领取宗门周常 同门勤务", number=986015
        )
        assert "尚未加入宗门" in reply and "无法领取宗门周常" in reply, reply
        assert weekly_snapshot(probe, sect_id, week_key) == before
        assert probe.rows(
            game,
            "SELECT sect_id,sect_position FROM user_xiuxian WHERE user_id='900001'",
        ) == [(None, None)]
        integrity(probe)
        probe.evidence.update(
            rejected_after_leave=True,
            membership=(None, None),
            zero_reward_writes=True,
            integrity="ok",
        )

    @case(
        "sect-weekly-write-recovery-and-inventory-limit",
        "领取宗门周常",
        "指定目标、写拒绝恢复、背包容量上限",
        "真实已完成目标，永久回执写入可由隔离trigger忽略后移除",
        "具体reason/subject可见，三库与trigger整笔回滚，同消息恢复及回放不重复",
        group="sect-weekly-claim",
    )
    async def recovery(probe):
        import sqlite3
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.item_json import Items

        module = sect_module()
        definitions, _manager = weekly_manager()
        sect_id = seed_sect(probe)
        diligence = next(goal for goal in definitions if goal.key == "sect_diligence")
        _definitions, manager, week_key = complete_goals(probe, sect_id, (diligence,))
        manager.list_goals(sect_id, week_key)
        game = probe.context.paths.game_db
        probe.seed(game, "CREATE TABLE isolated_effect(name TEXT)")
        probe.seed(
            game,
            "CREATE TRIGGER ignore_weekly_receipt BEFORE INSERT ON sect_weekly_reward_operations "
            "BEGIN INSERT INTO isolated_effect VALUES('receipt'); SELECT RAISE(IGNORE); END",
        )
        before = weekly_snapshot(probe, sect_id, week_key)
        failed = await probe.command(
            "领取宗门周常", "领取宗门周常 同门勤务", number=986021
        )
        operation_id = "sect-weekly:986021:900001:sect_diligence"
        assert "sect_weekly_receipt_write_rejected" in failed and operation_id in failed, failed
        assert weekly_snapshot(probe, sect_id, week_key) == before
        assert probe.rows(game, "SELECT COUNT(*) FROM isolated_effect") == [(0,)]

        probe.seed(game, "DROP TRIGGER ignore_weekly_receipt")
        recovered = await probe.command(
            "领取宗门周常", "领取宗门周常 同门勤务", number=986021
        )
        assert "领取成功" in recovered, recovered
        committed = weekly_snapshot(probe, sect_id, week_key)
        assert committed != before
        assert len(probe.rows(game, "SELECT operation_id FROM sect_weekly_reward_operations")) == 1

        command_module = __import__(
            "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_sect.sect_weekly_commands",
            fromlist=["_prepare_claim_goals"],
        )
        goal = manager.list_goals(sect_id, week_key)[0]
        duplicate = module.sect_application.claim_weekly(
            operation_id=operation_id,
            user_id="900001",
            sect_id=sect_id,
            week_key=week_key,
            goals=command_module._prepare_claim_goals([goal]),
            max_goods_num=module.XiuConfig().max_goods_num,
        )
        assert duplicate.ok and duplicate.replayed and duplicate.data["status"] == "duplicate"
        assert weekly_snapshot(probe, sect_id, week_key) == committed
        await transport_contract(failed)
        await transport_contract(recovered)

        elixir = next(goal for goal in definitions if goal.key == "sect_elixir")
        complete_goals(probe, sect_id, (elixir,))
        max_goods_num = module.XiuConfig().max_goods_num
        probe.grant_item(20015, max_goods_num)
        inventory_before = weekly_snapshot(probe, sect_id, week_key)
        full = await probe.command(
            "领取宗门周常", "领取宗门周常 丹火不熄", number=986022
        )
        assert "背包物品已达上限" in full, full
        assert weekly_snapshot(probe, sect_id, week_key) == inventory_before
        definition = Items().get_data_by_item_id(20015)
        assert definition is not None
        integrity(probe)
        probe.evidence.update(
            failure_reason="sect_weekly_receipt_write_rejected",
            failure_subject=operation_id,
            trigger_side_effect_rolled_back=True,
            recovery_same_message=True,
            duplicate_zero_write=True,
            inventory_limit=max_goods_num,
            adapters=4,
            integrity="ok",
        )

    @case(
        "sect-weekly-abort-propagates-and-recovers",
        "领取宗门周常",
        "指定目标与永久回执SQLite ABORT",
        "真实已完成目标，隔离数据库触发器抛出未知SQLite异常",
        "保留原异常栈且整笔回滚，移除故障后同消息完成",
        group="sect-weekly-claim",
    )
    async def abort_and_recover(probe):
        definitions, _manager = weekly_manager()
        sect_id = seed_sect(probe)
        diligence = next(goal for goal in definitions if goal.key == "sect_diligence")
        _definitions, manager, week_key = complete_goals(probe, sect_id, (diligence,))
        manager.list_goals(sect_id, week_key)
        game = probe.context.paths.game_db
        probe.seed(game, "CREATE TABLE isolated_effect(name TEXT)")
        probe.seed(
            game,
            "CREATE TRIGGER abort_weekly_receipt BEFORE INSERT ON sect_weekly_reward_operations "
            "BEGIN INSERT INTO isolated_effect VALUES('receipt'); "
            "SELECT RAISE(ABORT,'isolated weekly sqlite abort'); END",
        )
        before = weekly_snapshot(probe, sect_id, week_key)
        try:
            await probe.command(
                "领取宗门周常", "领取宗门周常 同门勤务", number=986031
            )
        except AssertionError:
            pass
        else:
            raise AssertionError("the original SQLite exception was swallowed")
        error_result = probe.calls[-1]
        trace = "\n".join(error_result.get("errors", ()))
        assert error_result["status"] == "exception" and "isolated weekly sqlite abort" in trace, error_result
        assert weekly_snapshot(probe, sect_id, week_key) == before
        assert probe.rows(game, "SELECT COUNT(*) FROM isolated_effect") == [(0,)]

        probe.seed(game, "DROP TRIGGER abort_weekly_receipt")
        recovered = await probe.command(
            "领取宗门周常", "领取宗门周常 同门勤务", number=986031
        )
        assert "领取成功" in recovered, recovered
        assert len(probe.rows(game, "SELECT operation_id FROM sect_weekly_reward_operations")) == 1
        await transport_contract(recovered)
        integrity(probe)
        probe.evidence.update(
            original_sqlite_error="isolated weekly sqlite abort",
            rollback_including_trigger=True,
            same_message_recovery=True,
            adapter_contracts=4,
            integrity="ok",
        )
