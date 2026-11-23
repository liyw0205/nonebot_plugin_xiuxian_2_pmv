from __future__ import annotations

from unittest.mock import patch


def register(case):
    @case(
        "dufang-bet-new-plan-keeps-parsed-cost",
        "鉴石",
        "带灵石数量参数",
        "真实注册玩家有足够灵石；首次请求由 plan_for_bet 生成冻结结果",
        "新计划保留 matcher 解析/限额后的成本并完成真实隔离库下注与派彩",
        group="dufang-bet",
    )
    async def new_plan_keeps_cost(probe):
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_dufang as module

        probe.player(stone=1_000_000_000, user_stamina=100)
        game = probe.context.paths.game_db
        resolution = {
            "entity": {"name": "隔离尘封物", "desc": "隔离测试结果"},
            "process": "封印已开启",
            "result_type": "success",
            "event": {"title": "隔离事件", "desc": "测试事件", "outcome": "成功"},
            "payout_outcome": "win",
            "gain": 1,
            "requested_loss": 0,
            "sharing": None,
        }
        before = probe.rows(
            game,
            "SELECT stone FROM user_xiuxian WHERE user_id='900001'",
        )
        with patch.object(module, "_draw_unseal_resolution", return_value=resolution):
            text = await probe.command(
                "鉴石",
                "鉴石 5776577177427",
                number=981_127,
            )

        bets = probe.rows(
            game,
            "SELECT cost,status,placed_at FROM dufang_bets WHERE user_id='900001'",
        )
        operations = probe.rows(
            game,
            "SELECT operation_id,cost,bet_id FROM dufang_bet_operations",
        )
        resolutions = probe.rows(
            game,
            "SELECT plan_json FROM dufang_bet_resolutions",
        )
        wallet = probe.rows(
            game,
            "SELECT stone FROM user_xiuxian WHERE user_id='900001'",
        )
        assert len(bets) == len(operations) == len(resolutions) == 1, (
            bets,
            operations,
            resolutions,
        )
        assert bets[0][0] == 100_000_000 and bets[0][1] == "win" and bets[0][2], bets
        operation_id = "dufang-bet:981127:900001"
        assert operations == [(operation_id, 100_000_000, operation_id)], operations
        assert wallet == [(before[0][0] - 100_000_000 + 1,)], wallet
        assert "隔离尘封物" in text and "消耗：" in text, text
        player_stats = probe.rows(
            probe.context.paths.player_db,
            "SELECT count,total_cost FROM unseal_data WHERE user_id='900001'",
        )
        assert player_stats == [(1, 100_000_000)], player_stats
        probe.evidence.update(
            parsed_cost=100_000_000,
            bet=bets[0],
            operation_id=operation_id,
            frozen_plan_count=len(resolutions),
            wallet_after=wallet[0][0],
            player_stats=player_stats[0],
        )
