from __future__ import annotations


def register(case):
    @case(
        "mixelixir-refine-success-replay",
        "配方",
        "自定义主药/药引/辅药/丹炉参数",
        "真实注册玩家持有合法药材与丹炉，配置命中洗髓丹",
        "真实扣材、结算丹药与双库回执；同消息重放返回原结果且零二次写",
        group="mixelixir-refine",
    )
    async def refine_success_replay(probe):
        probe.grant_item(3001, 7)
        probe.grant_item(4001, 1)
        game = probe.context.paths.game_db
        player = probe.context.paths.player_db
        recipe = "配方 主药恒心草3药引恒心草1辅药恒心草3丹炉寒铁铸心炉"

        def inventory():
            rows = probe.rows(
                game,
                "SELECT goods_id,goods_num FROM back WHERE user_id='900001' "
                "AND goods_id IN (2000,3001,4001) ORDER BY goods_id",
            )
            return tuple((int(item_id), int(quantity)) for item_id, quantity in rows)

        first_text = await probe.command("配方", recipe, number=919001)
        assert "成功炼成丹药" in first_text, first_text
        after_first = inventory()
        assert after_first and any(item_id == 2000 and quantity > 0 for item_id, quantity in after_first), after_first
        assert not any(item_id == 3001 and quantity > 0 for item_id, quantity in after_first), after_first
        assert any(item_id == 4001 and quantity == 1 for item_id, quantity in after_first), after_first

        tasks = probe.rows(
            game,
            "SELECT status,recipe_key,reward_id,reward_quantity FROM mixelixir_refine_tasks "
            "WHERE user_id='900001'",
        )
        costs = probe.rows(game, "SELECT operation_id,task_id FROM mixelixir_refine_cost_operations")
        rewards = probe.rows(game, "SELECT operation_id,task_id,reward_id,reward_quantity FROM mixelixir_refine_reward_operations")
        daily = probe.rows(game, "SELECT mixelixir_num FROM user_xiuxian WHERE user_id='900001'")
        mix_state = probe.rows(
            player,
            "SELECT 炼丹记录,炼丹经验 FROM mix_elixir_info WHERE user_id='900001'",
        )
        assert len(tasks) == len(costs) == len(rewards) == 1, (tasks, costs, rewards)
        assert tasks[0][0] == "claimed" and int(tasks[0][2]) == 2000, tasks
        assert daily == [(1,)], daily
        assert len(mix_state) == 1, mix_state

        probe.player(mixelixir_num=100)
        replay_text = await probe.command("配方", recipe, number=919001)
        assert "已经处理" in replay_text, replay_text
        assert inventory() == after_first
        assert probe.rows(game, "SELECT status,recipe_key,reward_id,reward_quantity FROM mixelixir_refine_tasks WHERE user_id='900001'") == tasks
        assert probe.rows(game, "SELECT operation_id,task_id FROM mixelixir_refine_cost_operations") == costs
        assert probe.rows(game, "SELECT operation_id,task_id,reward_id,reward_quantity FROM mixelixir_refine_reward_operations") == rewards
        capped_daily = probe.rows(game, "SELECT mixelixir_num FROM user_xiuxian WHERE user_id='900001'")
        assert capped_daily == [(100,)], capped_daily
        assert probe.rows(player, "SELECT 炼丹记录,炼丹经验 FROM mix_elixir_info WHERE user_id='900001'") == mix_state
        changed_text = await probe.command(
            "配方",
            "配方 主药恒心草4药引恒心草1辅药恒心草3丹炉寒铁铸心炉",
            number=919001,
            allow_exception=True,
        )
        assert "不同内容的请求" in changed_text, changed_text
        assert inventory() == after_first
        assert probe.rows(game, "SELECT mixelixir_num FROM user_xiuxian WHERE user_id='900001'") == capped_daily
        probe.evidence.update(
            inventory=after_first,
            task=tasks[0],
            cost_receipts=len(costs),
            reward_receipts=len(rewards),
            daily=daily[0][0],
            same_message_replay_unchanged=True,
            replay_bypasses_daily_cap=True,
            changed_payload_rejected_before_inventory=True,
        )

    @case(
        "mixelixir-refine-material-ignore",
        "配方",
        "自定义配方 + 药材 UPDATE RAISE(IGNORE)",
        "真实注册玩家持有合法药材与丹炉，隔离库只拒绝主药数量写入",
        "返回材料写拒绝的具体原因；药材、丹药、经验、次数、任务与回执全部回滚",
        group="mixelixir-refine",
        outcome="recovery",
    )
    async def refine_material_ignore(probe):
        probe.grant_item(3001, 7)
        probe.grant_item(4001, 1)
        game = probe.context.paths.game_db
        player = probe.context.paths.player_db
        probe.seed(
            game,
            "CREATE TRIGGER ignore_mixelixir_main_material BEFORE UPDATE OF goods_num ON back "
            "WHEN OLD.user_id='900001' AND OLD.goods_id=3001 "
            "BEGIN SELECT RAISE(IGNORE); END",
        )
        text = await probe.command(
            "配方",
            "配方 主药恒心草3药引恒心草1辅药恒心草3丹炉寒铁铸心炉",
            number=919002,
            allow_exception=True,
        )
        materials = probe.rows(game, "SELECT goods_num FROM back WHERE user_id='900001' AND goods_id=3001")
        furnace = probe.rows(game, "SELECT goods_num FROM back WHERE user_id='900001' AND goods_id=4001")
        reward = probe.rows(game, "SELECT goods_num FROM back WHERE user_id='900001' AND goods_id=2000")
        daily = probe.rows(game, "SELECT mixelixir_num FROM user_xiuxian WHERE user_id='900001'")
        tasks = probe.rows(game, "SELECT status FROM mixelixir_refine_tasks WHERE user_id='900001'")
        costs = probe.rows(game, "SELECT operation_id FROM mixelixir_refine_cost_operations")
        rewards = probe.rows(game, "SELECT operation_id FROM mixelixir_refine_reward_operations")
        mix_state = probe.rows(player, "SELECT 炼丹记录,炼丹经验 FROM mix_elixir_info WHERE user_id='900001'")
        probe.evidence.update(
            reply=text,
            materials=materials,
            furnace=furnace,
            reward=reward,
            daily=daily,
            tasks=tasks,
            cost_receipts=costs,
            reward_receipts=rewards,
            mix_state=mix_state,
        )
        assert "炼丹材料扣除保存失败" in text and "3001" in text, text
        assert materials == [(7,)] and furnace == [(1,)], (materials, furnace)
        assert reward == [] and daily == [(0,)], (reward, daily)
        assert tasks == costs == rewards == [], (tasks, costs, rewards)
        assert mix_state == [("{}", "0")], mix_state
        probe.seed(game, "DROP TRIGGER ignore_mixelixir_main_material")
        recovered = await probe.command(
            "配方",
            "配方 主药恒心草3药引恒心草1辅药恒心草3丹炉寒铁铸心炉",
            number=919002,
        )
        assert "成功炼成丹药" in recovered, recovered
        assert probe.rows(game, "SELECT goods_num FROM back WHERE user_id='900001' AND goods_id=3001") == [(0,)]
        assert probe.rows(game, "SELECT mixelixir_num FROM user_xiuxian WHERE user_id='900001'") == [(1,)]
        probe.evidence["same_message_recovery"] = True

    @case(
        "mixelixir-refine-cost-receipt-ignore",
        "配方",
        "自定义配方 + 成本操作回执 INSERT RAISE(IGNORE)",
        "真实注册玩家持有合法药材与丹炉，隔离库拒绝炼丹成本永久回执",
        "明确失败并完整回滚材料、奖励、炼丹次数、任务、玩家状态和双阶段回执",
        group="mixelixir-refine",
        outcome="recovery",
    )
    async def refine_cost_receipt_ignore(probe):
        probe.grant_item(3001, 7)
        probe.grant_item(4001, 1)
        game = probe.context.paths.game_db
        player = probe.context.paths.player_db
        probe.seed(
            game,
            "CREATE TRIGGER ignore_mixelixir_cost_receipt BEFORE INSERT ON mixelixir_refine_cost_operations "
            "BEGIN SELECT RAISE(IGNORE); END",
        )
        text = await probe.command(
            "配方",
            "配方 主药恒心草3药引恒心草1辅药恒心草3丹炉寒铁铸心炉",
            number=919003,
            allow_exception=True,
        )
        materials = probe.rows(game, "SELECT goods_num FROM back WHERE user_id='900001' AND goods_id=3001")
        furnace = probe.rows(game, "SELECT goods_num FROM back WHERE user_id='900001' AND goods_id=4001")
        reward = probe.rows(game, "SELECT goods_num FROM back WHERE user_id='900001' AND goods_id=2000")
        daily = probe.rows(game, "SELECT mixelixir_num FROM user_xiuxian WHERE user_id='900001'")
        tasks = probe.rows(game, "SELECT status FROM mixelixir_refine_tasks WHERE user_id='900001'")
        costs = probe.rows(game, "SELECT operation_id FROM mixelixir_refine_cost_operations")
        rewards = probe.rows(game, "SELECT operation_id FROM mixelixir_refine_reward_operations")
        mix_state = probe.rows(player, "SELECT 炼丹记录,炼丹经验 FROM mix_elixir_info WHERE user_id='900001'")
        probe.evidence.update(
            reply=text,
            materials=materials,
            furnace=furnace,
            reward=reward,
            daily=daily,
            tasks=tasks,
            cost_receipts=costs,
            reward_receipts=rewards,
            mix_state=mix_state,
        )
        assert "炼丹成本回执保存失败" in text and "操作回执" in text, text
        assert materials == [(7,)] and furnace == [(1,)], (materials, furnace)
        assert reward == [] and daily == [(0,)], (reward, daily)
        assert tasks == costs == rewards == [], (tasks, costs, rewards)
        assert mix_state == [("{}", "0")], mix_state
        probe.seed(game, "DROP TRIGGER ignore_mixelixir_cost_receipt")
        recovered = await probe.command(
            "配方",
            "配方 主药恒心草3药引恒心草1辅药恒心草3丹炉寒铁铸心炉",
            number=919003,
        )
        assert "成功炼成丹药" in recovered, recovered
        assert probe.rows(game, "SELECT goods_num FROM back WHERE user_id='900001' AND goods_id=3001") == [(0,)]
        assert probe.rows(game, "SELECT mixelixir_num FROM user_xiuxian WHERE user_id='900001'") == [(1,)]
        probe.evidence["same_message_recovery"] = True

    @case(
        "mixelixir-refine-invalid-amount",
        "配方",
        "主药数量为0",
        "真实注册玩家，配方命中药材但主药数量不合法",
        "拒绝写入并保留背包、次数和炼丹记录",
        group="mixelixir-refine",
    )
    async def refine_invalid_amount(probe):
        probe.grant_item(3001, 7)
        probe.grant_item(4001, 1)
        game = probe.context.paths.game_db
        player = probe.context.paths.player_db
        player_tables = probe.rows(
            player,
            "SELECT name FROM sqlite_master WHERE type='table' AND name='mix_elixir_info'",
        )
        before_mix = (
            probe.rows(player, "SELECT 炼丹记录,炼丹经验 FROM mix_elixir_info WHERE user_id='900001'")
            if player_tables else []
        )
        before = (
            probe.rows(game, "SELECT goods_id,goods_num FROM back WHERE user_id='900001' ORDER BY goods_id"),
            probe.rows(game, "SELECT mixelixir_num FROM user_xiuxian WHERE user_id='900001'"),
            before_mix,
        )
        text = await probe.command(
            "配方",
            "配方 主药恒心草0药引恒心草1辅药恒心草3丹炉寒铁铸心炉",
            number=919004,
        )
        after = (
            probe.rows(game, "SELECT goods_id,goods_num FROM back WHERE user_id='900001' ORDER BY goods_id"),
            probe.rows(game, "SELECT mixelixir_num FROM user_xiuxian WHERE user_id='900001'"),
            probe.rows(player, "SELECT 炼丹记录,炼丹经验 FROM mix_elixir_info WHERE user_id='900001'")
            if probe.rows(
                player,
                "SELECT name FROM sqlite_master WHERE type='table' AND name='mix_elixir_info'",
            ) else [],
        )
        assert "主药" in text and "数量是否足够" in text, text
        assert after == before, (before, after)
        assert probe.rows(game, "SELECT COUNT(*) FROM mixelixir_refine_tasks")[0][0] == 0
        probe.evidence.update(reply=text, unchanged=True)

    @case(
        "mixelixir-refine-daily-limit",
        "配方",
        "合法配方但每日次数达到100",
        "真实玩家背包有完整材料和丹炉，游戏库炼丹次数为100",
        "明确拒绝且所有材料、丹药、玩家记录和回执不变",
        group="mixelixir-refine",
    )
    async def refine_daily_limit(probe):
        probe.grant_item(3001, 7)
        probe.grant_item(4001, 1)
        probe.player(mixelixir_num=100)
        game = probe.context.paths.game_db
        player = probe.context.paths.player_db
        before_back = probe.rows(game, "SELECT goods_id,goods_num FROM back WHERE user_id='900001' ORDER BY goods_id")
        text = await probe.command(
            "配方",
            "配方 主药恒心草3药引恒心草1辅药恒心草3丹炉寒铁铸心炉",
            number=919008,
        )
        assert "已达上限" in text, text
        assert probe.rows(game, "SELECT goods_id,goods_num FROM back WHERE user_id='900001' ORDER BY goods_id") == before_back
        assert probe.rows(game, "SELECT mixelixir_num FROM user_xiuxian WHERE user_id='900001'") == [(100,)]
        assert probe.rows(game, "SELECT COUNT(*) FROM mixelixir_refine_tasks")[0][0] == 0
        assert probe.rows(game, "SELECT COUNT(*) FROM mixelixir_refine_cost_operations")[0][0] == 0
        probe.evidence.update(reply=text, inventory_unchanged=True, daily_count=100)

    @case(
        "mixelixir-refine-player-isolation",
        "配方",
        "私聊合法配方",
        "两名注册玩家都持有材料与丹炉，真实 OneBot 私聊 matcher 只为发起方结算",
        "发起方双库变化，另一玩家背包、次数和炼丹记录不变",
        group="mixelixir-refine",
    )
    async def refine_player_isolation(probe):
        for uid in ("900001", "900002"):
            probe.grant_item(3001, 7, uid=uid)
            probe.grant_item(4001, 1, uid=uid)
        game = probe.context.paths.game_db
        player = probe.context.paths.player_db
        def mix_rows(uid):
            if not probe.rows(
                player,
                "SELECT name FROM sqlite_master WHERE type='table' AND name='mix_elixir_info'",
            ):
                return []
            return probe.rows(
                player,
                f"SELECT 炼丹记录,炼丹经验 FROM mix_elixir_info WHERE user_id='{uid}'",
            )

        other_before = (
            probe.rows(game, "SELECT goods_id,goods_num FROM back WHERE user_id='900002' ORDER BY goods_id"),
            probe.rows(game, "SELECT mixelixir_num FROM user_xiuxian WHERE user_id='900002'"),
            mix_rows("900002"),
        )
        text = await probe.command(
            "配方",
            "配方 主药恒心草3药引恒心草1辅药恒心草3丹炉寒铁铸心炉",
            actor="900001",
            number=919009,
            event_type="private",
        )
        assert "成功炼成丹药" in text, text
        assert probe.rows(game, "SELECT goods_num FROM back WHERE user_id='900001' AND goods_id=3001") == [(0,)]
        assert probe.rows(game, "SELECT mixelixir_num FROM user_xiuxian WHERE user_id='900001'") == [(1,)]
        other_after = (
            probe.rows(game, "SELECT goods_id,goods_num FROM back WHERE user_id='900002' ORDER BY goods_id"),
            probe.rows(game, "SELECT mixelixir_num FROM user_xiuxian WHERE user_id='900002'"),
            mix_rows("900002"),
        )
        assert other_after == other_before, (other_before, other_after)
        probe.evidence.update(actor="900001", isolated_other_player="900002", other_player_unchanged=True)

    @case(
        "mixelixir-refine-harmony-rejection",
        "配方",
        "真实药材冷热调和失败",
        "天青花为正冷热值，合法数量导致真实 tiaohe 拒绝",
        "明确未炼成且不扣材、不加次数、不建结算任务",
        group="mixelixir-refine",
    )
    async def refine_harmony_rejection(probe):
        probe.grant_item(3004, 4)
        probe.grant_item(3001, 1)
        probe.grant_item(4001, 1)
        game = probe.context.paths.game_db
        before = probe.rows(
            game,
            "SELECT goods_id,goods_num FROM back WHERE user_id='900001' ORDER BY goods_id",
        )
        text = await probe.command(
            "配方",
            "配方 主药天青花2药引天青花1辅药恒心草1丹炉寒铁铸心炉",
            number=919005,
        )
        after = probe.rows(
            game,
            "SELECT goods_id,goods_num FROM back WHERE user_id='900001' ORDER BY goods_id",
        )
        assert "冷热调和失败" in text, text
        assert after == before, (before, after)
        assert probe.rows(game, "SELECT mixelixir_num FROM user_xiuxian WHERE user_id='900001'") == [(0,)]
        assert probe.rows(game, "SELECT COUNT(*) FROM mixelixir_refine_tasks")[0][0] == 0
        probe.evidence.update(reply=text, inventory_before=before, inventory_after=after)

    @case(
        "mixelixir-refine-pending-failure-stops",
        "配方",
        "已有扣材待领取任务且丹药回执拒写",
        "首条真实指令扣材后触发领取回执 IGNORE，再补充第二份材料",
        "领取失败保留 pending 任务并停止新扣材；故障解除后同操作恢复",
        group="mixelixir-refine",
        outcome="recovery",
    )
    async def refine_pending_failure_stops(probe):
        probe.grant_item(3001, 7)
        probe.grant_item(4001, 1)
        game = probe.context.paths.game_db
        player = probe.context.paths.player_db
        recipe = "配方 主药恒心草3药引恒心草1辅药恒心草3丹炉寒铁铸心炉"
        probe.seed(
            game,
            "CREATE TRIGGER ignore_mixelixir_reward_receipt BEFORE INSERT ON mixelixir_refine_reward_operations "
            "BEGIN SELECT RAISE(IGNORE); END",
        )
        first = await probe.command("配方", recipe, number=919006)
        assert "丹药领取回执保存失败" in first, first
        assert probe.rows(game, "SELECT status FROM mixelixir_refine_tasks WHERE user_id='900001'") == [("ready",)]
        assert probe.rows(game, "SELECT goods_num FROM back WHERE user_id='900001' AND goods_id=3001") == [(0,)]
        assert probe.rows(game, "SELECT mixelixir_num FROM user_xiuxian WHERE user_id='900001'") == [(1,)]

        probe.grant_item(3001, 7)
        before_retry = probe.rows(game, "SELECT goods_num FROM back WHERE user_id='900001' AND goods_id=3001")
        second = await probe.command("配方", recipe, number=919007)
        assert "丹药领取回执保存失败" in second, second
        assert probe.rows(game, "SELECT goods_num FROM back WHERE user_id='900001' AND goods_id=3001") == before_retry
        assert probe.rows(game, "SELECT mixelixir_num FROM user_xiuxian WHERE user_id='900001'") == [(1,)]
        assert probe.rows(game, "SELECT status FROM mixelixir_refine_tasks WHERE user_id='900001'") == [("ready",)]

        probe.seed(game, "DROP TRIGGER ignore_mixelixir_reward_receipt")
        recovered = await probe.command("配方", recipe, number=919007)
        assert "已补领此前扣材未发" in recovered, recovered
        assert probe.rows(game, "SELECT goods_num FROM back WHERE user_id='900001' AND goods_id=3001") == before_retry
        assert probe.rows(game, "SELECT status FROM mixelixir_refine_tasks WHERE user_id='900001'") == [("claimed",)]
        assert probe.rows(game, "SELECT COUNT(*) FROM mixelixir_refine_cost_operations")[0][0] == 1
        assert probe.rows(game, "SELECT COUNT(*) FROM mixelixir_refine_reward_operations")[0][0] == 1
        assert probe.rows(player, "SELECT 炼丹经验 FROM mix_elixir_info WHERE user_id='900001'") == [("30",)]
        probe.evidence.update(pending_failure_stopped_new_cost=True, recovery_after_drop=True)

    @case(
        "mixelixir-refine-reward-abort-recovery",
        "配方",
        "合法配方 + 丹药领取回执 ABORT",
        "真实注册玩家，隔离双库只对永久奖励回执注入 SQLite ABORT",
        "保留原异常日志和定位回复，成本任务 pending 且无部分领取；故障解除后同消息完成",
        group="mixelixir-refine",
        outcome="recovery",
    )
    async def refine_reward_abort_recovery(probe):
        probe.grant_item(3001, 7)
        probe.grant_item(4001, 1)
        game = probe.context.paths.game_db
        player = probe.context.paths.player_db
        recipe = "配方 主药恒心草3药引恒心草1辅药恒心草3丹炉寒铁铸心炉"
        probe.seed(
            game,
            "CREATE TRIGGER abort_mixelixir_reward_receipt BEFORE INSERT ON mixelixir_refine_reward_operations "
            "BEGIN SELECT RAISE(ABORT,'isolated alchemy reward receipt failure'); END",
        )
        failed = await probe.command("配方", recipe, number=919010, allow_exception=True)
        failure_call = probe.calls[-1]
        logged = " ".join(
            str(item.get("message", "")) + str(item.get("exception", ""))
            for item in failure_call.get("logs", [])
        )
        assert "定位号" in failed and "isolated alchemy reward receipt failure" not in failed, failed
        assert "isolated alchemy reward receipt failure" in logged, logged
        assert probe.rows(game, "SELECT goods_num FROM back WHERE user_id='900001' AND goods_id=3001") == [(0,)]
        assert probe.rows(game, "SELECT mixelixir_num FROM user_xiuxian WHERE user_id='900001'") == [(1,)]
        assert probe.rows(game, "SELECT status FROM mixelixir_refine_tasks WHERE user_id='900001'") == [("ready",)]
        assert probe.rows(game, "SELECT COUNT(*) FROM mixelixir_refine_reward_operations")[0][0] == 0
        assert probe.rows(game, "SELECT goods_num FROM back WHERE user_id='900001' AND goods_id=2000") == []
        assert probe.rows(player, "SELECT 炼丹记录,炼丹经验 FROM mix_elixir_info WHERE user_id='900001'") == [("{}", "0")]

        probe.seed(game, "DROP TRIGGER abort_mixelixir_reward_receipt")
        recovered = await probe.command("配方", recipe, number=919010)
        assert "成功炼成丹药" in recovered, recovered
        assert probe.rows(game, "SELECT status FROM mixelixir_refine_tasks WHERE user_id='900001'") == [("claimed",)]
        assert probe.rows(game, "SELECT goods_num FROM back WHERE user_id='900001' AND goods_id=3001") == [(0,)]
        assert probe.rows(game, "SELECT goods_num FROM back WHERE user_id='900001' AND goods_id=2000") == [(3,)]
        assert probe.rows(game, "SELECT COUNT(*) FROM mixelixir_refine_cost_operations")[0][0] == 1
        assert probe.rows(game, "SELECT COUNT(*) FROM mixelixir_refine_reward_operations")[0][0] == 1
        assert probe.rows(player, "SELECT 炼丹经验 FROM mix_elixir_info WHERE user_id='900001'") == [("30",)]
        probe.evidence.update(original_abort_logged=True, locator_replied=True, same_message_recovery=True)
