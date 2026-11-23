"""Real matcher contracts for opening a time-windowed body-training bath."""
from __future__ import annotations

import json
from datetime import datetime, timedelta
from unittest.mock import patch

from tests.parameter_cases_team_leave import integrity
from tests.parameter_cases_team_view import database_projection


NOW = datetime(2026, 11, 21, 12, 30)


def _seed_profile(probe, user_id: str, *, hp: int = 0, last_settlement: datetime = NOW):
    from nonebot_plugin_xiuxian_2.features.tianti_training.repository import (
        TiantiBreakthroughSqlRepository,
    )

    player = probe.context.paths.player_db
    TiantiBreakthroughSqlRepository(player).ensure_profile(user_id)
    probe.seed(
        player,
        "UPDATE tianti_info SET tianti_hp=?,last_settle_time=?,"
        "medicine_last_time=NULL,medicine_end_time=NULL,medicine_effect='0',"
        "medicine_name='' WHERE user_id=?",
        (str(hp), last_settlement.strftime("%Y-%m-%d %H:%M:%S"), user_id),
    )


def _item(module, name: str):
    item_id, definition = module.items.get_data_by_item_name(name)
    assert item_id and definition and definition.get("item_type") == "药材", (name, item_id, definition)
    return item_id, definition


def _replace_inventory(probe, module, user_id: str, name: str, quantity: int) -> int:
    item_id, definition = _item(module, name)
    probe.seed(
        probe.context.paths.game_db,
        "DELETE FROM back WHERE user_id=? AND goods_id=?",
        (user_id, item_id),
    )
    probe.grant_item(item_id, quantity, uid=user_id)
    return item_id


def _bath_projection(probe, user_id: str = "900001"):
    game, player = probe.context.paths.game_db, probe.context.paths.player_db
    return {
        "inventory": probe.rows(
            game,
            "SELECT goods_id,goods_num,bind_num FROM back WHERE user_id=? ORDER BY goods_id",
            (user_id,),
        ),
        "profile": probe.rows(
            player,
            "SELECT tianti_hp,last_settle_time,medicine_last_time,medicine_end_time,"
            "medicine_effect,medicine_name FROM tianti_info WHERE user_id=?",
            (user_id,),
        ),
        "receipts": probe.rows(
            game,
            "SELECT operation_id,user_id,request_json,result_json "
            "FROM tianti_medicine_bath_operations WHERE user_id=? ORDER BY operation_id",
            (user_id,),
        ),
    }


def _clock(at: datetime):
    class FrozenClock:
        @staticmethod
        def now():
            return at

    return FrozenClock()


def register(case):
    @case(
        "tianti-bath-arguments-window-replay",
        "炼体药浴",
        "时段、无参/非法/不足/默认1份/重复/有效期内再次开启",
        "隔离玩家档案、背包和当前/其他时段真实药材定义",
        "只有当前时段有效且库存足够才扣料；默认1份、回执重放和生效中拒绝均符合状态",
        group="tianti-bath-parameters",
    )
    async def arguments_window_and_replay(probe):
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_tianti as module

        user_id = "900001"
        now = NOW
        _seed_profile(probe, user_id)
        slot = module._medicine_bath_slot(now.hour)
        herb = sorted(slot["herbs"])[0]
        other_herb = sorted(
            set().union(*(set(row["herbs"]) for row in module.MEDICINE_BATH_TIME_CONFIG))
            - set(slot["herbs"])
        )[0]
        herb_id = _replace_inventory(probe, module, user_id, herb, 1)
        other_id = _replace_inventory(probe, module, user_id, other_herb, 4)
        before = _bath_projection(probe, user_id)

        with patch.object(module, "runtime_clock", _clock(now)):
            no_args = await probe.command("炼体药浴", number=979_801)
            assert "当前时段" in no_args and "药浴时段" in no_args, no_args
            assert _bath_projection(probe, user_id) == before

            for offset, text, expected in (
                (1, "炼体药浴 虚构药材x1", "不存在或不是药材"),
                (2, f"炼体药浴 {herb}x0", "用法"),
                (3, f"炼体药浴 {other_herb}x1", "当前时段无效，未消耗"),
            ):
                probe.clear_cooldowns()
                reply = await probe.command("炼体药浴", text, number=979_801 + offset)
                assert expected in reply, (text, reply)
                assert _bath_projection(probe, user_id) == before

            probe.clear_cooldowns()
            insufficient = await probe.command(
                "炼体药浴", f"炼体药浴 {herb}x2", number=979_805
            )
            assert "药材不足" in insufficient and "现有1份" in insufficient, insufficient
            assert _bath_projection(probe, user_id) == before

            probe.clear_cooldowns()
            success = await probe.command(
                "炼体药浴", f"炼体药浴 {herb}", number=979_806
            )
            after = _bath_projection(probe, user_id)
            assert "消耗有效药材：" + herb + "x1" in success, success
            assert after["profile"][0][4] == "1.0333", after
            inventory = {row[0]: (row[1], row[2]) for row in after["inventory"]}
            assert inventory[herb_id] == (0, 0) and inventory[other_id] == (4, 0), after
            assert len(after["receipts"]) == 1, after

            probe.clear_cooldowns()
            replay = await probe.command(
                "炼体药浴", f"炼体药浴 {herb}", number=979_806
            )
            assert "已经处理" in replay and _bath_projection(probe, user_id) == after

            probe.clear_cooldowns()
            active = await probe.command(
                "炼体药浴", f"炼体药浴 {herb}x1", number=979_807
            )
            assert "当前药浴仍在生效" in active, active
            assert _bath_projection(probe, user_id) == after

        integrity(probe)
        probe.evidence.update(
            slot=slot["name"], valid_herb=herb, out_of_slot_herb=other_herb,
            default_amount=1, insufficient_zero_write=True,
            same_message_replay=True, active_bath_zero_write=True,
            inventory_and_receipt=after,
        )

    @case(
        "tianti-bath-over-200-cap-and-settlement",
        "炼体药浴",
        "重复药材参数总量超过200份",
        "隔离档案距上次炼体结算150分钟、当前时段两种有效药材均有库存",
        "请求按30份封顶、其他超量未消费，先结算旧炼体气血，再持久化新效果和完整回执",
        group="tianti-bath-parameters",
    )
    async def cap_and_pre_settlement(probe):
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_tianti as module
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.utils import number_to

        user_id = "900001"
        now = NOW
        prior = now - timedelta(minutes=150)
        _seed_profile(probe, user_id, hp=0, last_settlement=prior)
        slot = module._medicine_bath_slot(now.hour)
        names = sorted(slot["herbs"])[:2]
        first_id = _replace_inventory(probe, module, user_id, names[0], 250)
        second_id = _replace_inventory(probe, module, user_id, names[1], 5)
        before_hp = probe.rows(
            probe.context.paths.player_db,
            "SELECT tianti_hp FROM tianti_info WHERE user_id=?",
            (user_id,),
        )[0][0]
        probe.evidence["before"] = _bath_projection(probe, user_id)
        request = f"炼体药浴 {names[0]}x120 {names[0]}x120 {names[1]}x5"

        with patch.object(module, "runtime_clock", _clock(now)):
            reply = await probe.command("炼体药浴", request, number=979_811)

        after = _bath_projection(probe, user_id)
        probe.evidence.update(reply=reply, after=after)
        assert "消耗有效药材：" + names[0] + "x30" in reply, reply
        assert "超过上限的有效药材未消耗：215份" in reply, reply
        assert "药浴前已自动结算炼体气血：" + number_to(12_000) in reply, reply
        assert after["inventory"] == [(first_id, 220, 0), (second_id, 5, 0)], after
        profile = after["profile"][0]
        assert int(profile[0]) > int(before_hp), (before_hp, profile)
        assert profile[1] == now.strftime("%Y-%m-%d %H:%M:%S"), profile
        assert profile[3] == (now + timedelta(minutes=360)).strftime("%Y-%m-%d %H:%M:%S"), profile
        assert float(profile[4]) == 2.0, profile
        assert names[1] not in profile[5], profile
        assert len(after["receipts"]) == 1, after
        receipt = json.loads(after["receipts"][0][3])
        assert receipt["consumed"] == [{"item_id": first_id, "name": names[0], "amount": 30}], receipt
        assert receipt["settlement"]["status"] == "settle", receipt
        integrity(probe)
        probe.evidence.update(
            requested_units=245, consumed=receipt["consumed"],
            pre_settlement=receipt["settlement"], persisted_profile=profile,
            inventory_and_receipt=after,
        )

    @case(
        "tianti-bath-receipt-ignore-recovery",
        "炼体药浴",
        "合法药材参数与同消息重试",
        "隔离收据表 INSERT RAISE(IGNORE)，解除后复用原消息号",
        "保留具体回执拒绝原因、药材/档案/收据整笔回滚，pending请求同消息恢复并幂等",
        group="tianti-bath-parameters",
    )
    async def receipt_ignore_recovery(probe):
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_tianti as module

        user_id = "900001"
        now = NOW
        _seed_profile(probe, user_id)
        slot = module._medicine_bath_slot(now.hour)
        herb = sorted(slot["herbs"])[0]
        item_id = _replace_inventory(probe, module, user_id, herb, 3)
        game = probe.context.paths.game_db
        probe.seed(
            game,
            "CREATE TRIGGER ignore_tianti_bath_receipt BEFORE INSERT "
            "ON tianti_medicine_bath_operations BEGIN SELECT RAISE(IGNORE); END",
        )
        before = _bath_projection(probe, user_id)
        operation_id = f"tianti-bath:979821:{user_id}"

        with patch.object(module, "runtime_clock", _clock(now)):
            failed = await probe.command(
                "炼体药浴", f"炼体药浴 {herb}x2", number=979_821
            )
            after_failure = _bath_projection(probe, user_id)
            ledger = probe.rows(
                game,
                "SELECT status,result_json FROM operation_ledger "
                "WHERE operation_id=? AND action='tianti.bath'",
                (operation_id,),
            )
            assert "药浴操作回执保存失败" in failed and "本次药浴已撤销" in failed, failed
            assert "开启成功" not in failed, failed
            assert after_failure == before, (before, after_failure)
            assert ledger and ledger[0][0] == "failed", ledger
            failure = json.loads(ledger[0][1])
            assert failure["code"] == "tianti_bath_receipt_write_rejected", failure
            assert "tianti_bath_receipt_write_rejected" in failure["message"], failure

            probe.seed(game, "DROP TRIGGER ignore_tianti_bath_receipt")
            probe.clear_cooldowns()
            recovered = await probe.command(
                "炼体药浴", f"炼体药浴 {herb}x2", number=979_821
            )
            after_recovery = _bath_projection(probe, user_id)
            assert "药浴开启成功" in recovered, recovered
            assert next(row[1] for row in after_recovery["inventory"] if row[0] == item_id) == 1
            assert len(after_recovery["receipts"]) == 1, after_recovery

            probe.clear_cooldowns()
            replay = await probe.command(
                "炼体药浴", f"炼体药浴 {herb}x2", number=979_821
            )
            assert "已经处理" in replay, replay
            assert _bath_projection(probe, user_id) == after_recovery

        integrity(probe)
        probe.evidence.update(
            failed_reply=failed, before=before, after_failure=after_failure,
            failed_ledger=ledger, recovered_reply=recovered,
            after_recovery=after_recovery, replay_zero_write=True,
        )
