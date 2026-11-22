"""Real matcher coverage for the daily map mission lifecycle."""

import asyncio
import json
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch


def _setup(probe, *, mission_type="gather", target=1, progress=None, claimed=0, settlement="", date=None):
    from nonebot_plugin_xiuxian_2.features.map.migrations import (
        apply_map_mission_claim,
        apply_map_player_state_schema,
    )
    from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
    from nonebot_plugin_xiuxian_2.plugin import apply_platform_schema
    from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_map as module

    game, player = probe.context.paths.game_db, probe.context.paths.player_db
    with DatabaseUnitOfWork(game) as uow:
        apply_platform_schema(uow)
        apply_map_mission_claim(uow)
    with DatabaseUnitOfWork(player) as uow:
        apply_map_player_state_schema(uow)

    fixed = datetime(2026, 11, 23, 12, 0, 0)
    clock = SimpleNamespace(now=lambda: fixed)
    mission_date = date or fixed.strftime("%Y-%m-%d")
    with patch.object(module, "runtime_clock", clock):
        module.map_application.save_mission(
            "900001",
            {
                "date": mission_date,
                "mission_type": mission_type,
                "target": target,
                "claimed": claimed,
                "settlement": settlement,
            },
        )
    progress_key = module.MAP_MISSION_CONFIG.get(mission_type, module.MAP_MISSION_CONFIG["gather"])["count_key"]
    daily = {
        "date": fixed.strftime("%Y-%m-%d"),
        "gather_count": 0,
        "combat_count": 0,
        "explore_count": 0,
        "fish_count": 0,
        "mine_count": 0,
    }
    daily[progress_key] = target if progress is None else progress
    module.map_application.save_daily_limit("900001", daily)
    return module, clock, progress_key


def _projection(probe, *, user_id="900001"):
    game, player = probe.context.paths.game_db, probe.context.paths.player_db
    return {
        "profile": probe.rows(game, "SELECT stone FROM user_xiuxian WHERE user_id=?", (user_id,)),
        "mission": probe.rows(
            player,
            "SELECT date,mission_type,target,claimed,settlement FROM map_mission WHERE user_id=?",
            (user_id,),
        ),
        "inventory": probe.rows(
            game,
            "SELECT goods_id,goods_num,bind_num FROM back WHERE user_id=? ORDER BY goods_id",
            (user_id,),
        ),
        "receipt": probe.rows(
            game,
            "SELECT operation_id,payload,stone,rewards FROM map_mission_claim_operations ORDER BY operation_id",
        ),
        "outbox": probe.rows(
            game,
            "SELECT event_id,payload_json FROM domain_outbox WHERE event_id LIKE 'map.mission.effects:%' ORDER BY event_id",
        ),
    }


def _integrity(probe):
    from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork

    for path in (probe.context.paths.game_db, probe.context.paths.player_db):
        with DatabaseUnitOfWork(path, read_only=True) as uow:
            assert uow.query_one("PRAGMA integrity_check")["integrity_check"] == "ok"


def register(case):
    expected_keys = []

    def register_case(key, command, form, precondition, expected_branch, *, outcome="success"):
        def decorate(function):
            case(
                key, command, form, precondition, expected_branch,
                group="map-mission", outcome=outcome,
            )(function)
            expected_keys.append(key)
            return function
        return decorate

    @case(
        "map-mission-claim-receipt-ignore",
        "委托完成",
        "合法且已完成的采集委托，永久回执写入被 SQLite 忽略",
        "已注册玩家、生产地图状态/领取迁移、隔离 game/player SQLite",
        "拒绝并保留具体原因；claimed、灵石、背包、回执、outbox 整体不变",
        group="map-mission",
        outcome="recovery",
    )
    async def receipt_ignore_is_atomic(probe):
        module, clock, _ = _setup(probe)
        probe.player(stone=100000)
        reward_meta = {
            "stone_delta": 12345,
            "item_delta": [{"id": 99001, "name": "委托灵玉", "type": "材料", "amount": 2}],
        }
        with patch.object(
            module.map_reward_resolver,
            "roll_mission_reward",
            return_value=(["灵石x12345", "委托灵玉x2"], reward_meta),
        ):
            probe.seed(
                probe.context.paths.game_db,
                "CREATE TRIGGER mission_receipt_ignore BEFORE INSERT ON map_mission_claim_operations "
                "BEGIN SELECT RAISE(IGNORE); END",
            )
            with patch.object(module, "runtime_clock", clock):
                before = _projection(probe)
                reply = await probe.command(
                    "委托完成", "委托完成", number=978001, allow_exception=True,
                )
                after = _projection(probe)
            probe.evidence["receipt_ignore"] = {
                "reply": reply,
                "before": before,
                "after": after,
            }
            from nonebot_plugin_xiuxian_2.core.player_reasons import reason_text

            assert reason_text("map_mission_claim_receipt_write_rejected") in reply, reply
            assert "委托永久回执" in reply, reply
            assert after["profile"] == before["profile"], after
            assert after["mission"][0][:4] == before["mission"][0][:4], after
            assert json.loads(after["mission"][0][4]) == {
                "rewards": ["灵石x12345", "委托灵玉x2"],
                "reward_meta": reward_meta,
            }, after
            for key in ("inventory", "receipt", "outbox"):
                assert after[key] == before[key], (key, before, after)
            probe.seed(probe.context.paths.game_db, "DROP TRIGGER mission_receipt_ignore")
            await asyncio.sleep(1.25)
            with patch.object(module, "runtime_clock", clock):
                recovered = await probe.command(
                    "委托完成", "委托完成", number=978001, allow_exception=True,
                )
            await asyncio.sleep(1.25)
            with patch.object(module, "runtime_clock", clock):
                committed = _projection(probe)
                repeated = await probe.command(
                    "委托完成", "委托完成", number=978001, allow_exception=True,
                )
            assert "地图委托完成" in recovered, recovered
            assert "地图委托完成" not in repeated, repeated
            assert committed["mission"][0][3] == 1 and len(committed["receipt"]) == 1, committed
            assert committed["profile"][0][0] == before["profile"][0][0] + 12345, committed
            assert committed["inventory"] == [(99001, 2, 2)], committed
            assert len(committed["outbox"]) == 1, committed
            assert _projection(probe) == committed
            _integrity(probe)

    @register_case(
        "map-mission-empty-query-accept",
        "地图委托",
        "未接取查询后接取并重放",
        "迁移完成的隔离玩家库，无今日委托",
        "未接取查询不伪称已有；接取保存实际随机任务和目标，重放不改状态",
    )
    async def empty_query_accept(probe):
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_map as module

        module, clock, _ = _setup(probe, mission_type="", target=0)
        with patch.object(module, "runtime_clock", clock):
            query = await probe.command("地图委托", "地图委托", number=978100)
        assert "尚未接取" in query, query
        state_before = probe.rows(
            probe.context.paths.player_db,
            "SELECT date,mission_type,target,claimed,settlement FROM map_mission WHERE user_id='900001'",
        )
        assert state_before == [("2026-11-23", "", 0, 0, "")], state_before

        probe.clear_cooldowns()
        with patch.object(module, "runtime_clock", clock):
            unaccepted = await probe.command("委托完成", "委托完成", number=978102)
        assert "尚未接取地图委托" in unaccepted, unaccepted
        assert probe.rows(
            probe.context.paths.player_db,
            "SELECT date,mission_type,target,claimed,settlement FROM map_mission WHERE user_id='900001'",
        ) == state_before

        class FirstChoice:
            @staticmethod
            def choice(values):
                return values[0]

        probe.clear_cooldowns()
        with patch.object(module, "runtime_clock", clock), patch.object(module, "runtime_random", FirstChoice()):
            accepted = await probe.command("接取委托", "接取委托", number=978103)
        expected_type = next(iter(module.MAP_MISSION_CONFIG))
        expected_target = module.MAP_MISSION_CONFIG[expected_type]["targets"][0]
        after = probe.rows(
            probe.context.paths.player_db,
            "SELECT date,mission_type,target,claimed,settlement FROM map_mission WHERE user_id='900001'",
        )
        assert "已接取今日地图委托" in accepted and expected_type in after[0], (accepted, after)
        assert after == [("2026-11-23", expected_type, expected_target, 0, "")], after
        probe.clear_cooldowns()
        with patch.object(module, "runtime_clock", clock), patch.object(module, "runtime_random", FirstChoice()):
            replay = await probe.command("接取委托", "接取委托", number=978103)
        assert "已接取今日地图委托" in replay
        assert probe.rows(
            probe.context.paths.player_db,
            "SELECT date,mission_type,target,claimed,settlement FROM map_mission WHERE user_id='900001'",
        ) == after
        _integrity(probe)
        probe.evidence.update(query=query, accepted=after, same_message_replay=True)

    @register_case(
        "map-mission-daily-progress-keys",
        "地图委托",
        "采集/战斗/探索任务的进度键",
        "三类完整任务状态和各自 map_daily_limit 实际计数",
        "查询读取对应的按日计数，显示的进度与目标/完成状态一致",
    )
    async def daily_progress_keys(probe):
        module, clock, _ = _setup(probe)
        database = probe.context.paths.player_db
        observed = {}
        for index, (mission_type, config) in enumerate(module.MAP_MISSION_CONFIG.items()):
            target = config["targets"][-1]
            progress = target - 1
            module.map_application.save_mission(
                "900001",
                {"date": "2026-11-23", "mission_type": "", "target": 0,
                 "claimed": 0, "settlement": ""},
            )
            daily = {"date": "2026-11-23", "gather_count": 0, "combat_count": 0,
                     "explore_count": 0, "fish_count": 0, "mine_count": 0}
            daily[config["count_key"]] = progress
            module.map_application.save_daily_limit("900001", daily)

            class FixedChoice:
                @staticmethod
                def choice(values):
                    if list(values) == list(module.MAP_MISSION_CONFIG):
                        return mission_type
                    return target

            probe.clear_cooldowns()
            with patch.object(module, "runtime_clock", clock), patch.object(module, "runtime_random", FixedChoice()):
                accepted = await probe.command("接取委托", "接取委托", number=978110 + index)
            persisted = probe.rows(
                database,
                "SELECT date,mission_type,target,claimed,settlement FROM map_mission WHERE user_id='900001'",
            )
            assert persisted == [("2026-11-23", mission_type, target, 0, "")], (mission_type, accepted, persisted)
            probe.clear_cooldowns()
            with patch.object(module, "runtime_clock", clock):
                reply = await probe.command("地图委托", "地图委托", number=978120 + index)
            expected_line = f"进度：{progress}/{target}"
            assert expected_line in reply and "状态：进行中" in reply, (mission_type, reply)
            observed[mission_type] = {
                "count_key": config["count_key"], "progress": progress, "target": target,
                "accepted": persisted[0][1:3], "reply": expected_line,
            }
        _integrity(probe)
        probe.evidence["daily_progress"] = observed

    @register_case(
        "map-mission-cross-day-and-isolation",
        "地图委托",
        "跨日读取与不同玩家隔离",
        "玩家1有昨天的已完成委托，玩家2无委托；业务钟为今天",
        "跨日旧快照清空，仅当前玩家状态重置",
    )
    async def cross_day_and_isolation(probe):
        module, clock, _ = _setup(
            probe, mission_type="gather", target=5, claimed=1,
            settlement='{"rewards": ["旧奖励"], "reward_meta": {}}', date="2026-11-22",
        )
        player_db = probe.context.paths.player_db
        probe.clear_cooldowns()
        with patch.object(module, "runtime_clock", clock):
            player_two = await probe.command("地图委托", "地图委托", actor="900002", number=978120)
        row_two = probe.rows(
            player_db,
            "SELECT date,mission_type,target,claimed,settlement FROM map_mission WHERE user_id='900002'",
        )
        assert "尚未接取" in player_two, player_two
        assert row_two == [("2026-11-23", "", 0, 0, "")], row_two
        probe.clear_cooldowns()
        with patch.object(module, "runtime_clock", clock):
            today = await probe.command("地图委托", "地图委托", number=978121)
        row_one = probe.rows(
            player_db,
            "SELECT date,mission_type,target,claimed,settlement FROM map_mission WHERE user_id='900001'",
        )
        assert "尚未接取" in today, today
        assert row_one == [("2026-11-23", "", 0, 0, "")], row_one
        assert probe.rows(player_db, "SELECT user_id FROM map_mission ORDER BY user_id") == [
            ("900001",), ("900002",),
        ]
        _integrity(probe)
        probe.evidence.update(player_two=row_two, player_one=row_one, independent_rows=True)

    @register_case(
        "map-mission-incomplete-claim-no-write",
        "委托完成",
        "合法领取指令但目标进度未达标",
        "已接取采集委托，当前进度 0/1",
        "明确拒领且不生成快照、不改奖励/回执/outbox",
    )
    async def incomplete_claim(probe):
        module, clock, _ = _setup(probe, progress=0)
        probe.player(stone=100000)
        before = _projection(probe)
        with patch.object(module, "runtime_clock", clock):
            reply = await probe.command("委托完成", "委托完成", number=978130)
        after = _projection(probe)
        assert "尚未完成" in reply and "0/1" in reply, reply
        assert after == before, (before, after)
        _integrity(probe)
        probe.evidence.update(reply=reply, projection_unchanged=True)

    @register_case(
        "map-mission-claim-send-failure-after-commit",
        "委托完成",
        "完成领取后发送失败，再以同消息重试",
        "已完成采集委托，奖励写入成功；OneBot 发送层只对成功回复注入故障",
        "发送失败不回滚已提交奖励，同消息重试不重复发奖",
        outcome="recovery",
    )
    async def send_failure_after_commit(probe):
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_map as module

        module, clock, _ = _setup(probe)
        probe.player(stone=100000)
        reward_meta = {
            "stone_delta": 12345,
            "item_delta": [{"id": 99001, "name": "委托灵玉", "type": "材料", "amount": 2}],
        }
        display = ["灵石x12345", "委托灵玉x2"]
        sender = AsyncMock(side_effect=RuntimeError("isolated success reply delivery failure"))
        message_number = 978135
        with patch.object(module.map_reward_resolver, "roll_mission_reward", return_value=(display, reward_meta)), \
                patch.object(module, "runtime_clock", clock), patch.object(module, "handle_send", sender):
            reply = await probe.command(
                "委托完成", "委托完成", number=message_number, allow_exception=True,
            )
        committed = _projection(probe)
        assert sender.await_count == 1
        assert "定位号" in reply, reply
        assert committed["mission"][0][3] == 1 and committed["profile"][0][0] == 112345, committed
        assert committed["inventory"] == [(99001, 2, 2)]
        assert len(committed["receipt"]) == len(committed["outbox"]) == 1, committed
        probe.clear_cooldowns()
        with patch.object(module, "runtime_clock", clock):
            repeated = await probe.command(
                "委托完成", "委托完成", number=message_number,
            )
        assert "已领取" in repeated, repeated
        assert _projection(probe) == committed
        _integrity(probe)
        probe.evidence.update(first_reply=reply, repeated_reply=repeated, committed=committed)

    @register_case(
        "map-mission-claim-existing-inventory-increments-binding",
        "委托完成",
        "奖励写入已有同类物品行",
        "已完成采集委托，背包既有数量3/绑定1，本次固定奖励数量2",
        "UPSERT 后数量与绑定数均精确累加且回执相符",
    )
    async def existing_inventory_increment(probe):
        module, clock, _ = _setup(probe)
        probe.player(stone=100000)
        game_db = probe.context.paths.game_db
        probe.seed(
            game_db,
            "INSERT INTO back(user_id,goods_id,goods_name,goods_type,goods_num,create_time,update_time,bind_num) "
            "VALUES('900001',99001,'委托灵玉','材料',3,'2026-11-23 11:00:00','2026-11-23 11:00:00',1)",
        )
        reward_meta = {
            "stone_delta": 12345,
            "item_delta": [{"id": 99001, "name": "委托灵玉", "type": "材料", "amount": 2}],
        }
        with patch.object(module.map_reward_resolver, "roll_mission_reward", return_value=(
            ["灵石x12345", "委托灵玉x2"], reward_meta,
        )), patch.object(module, "runtime_clock", clock):
            reply = await probe.command("委托完成", "委托完成", number=978136)
        state = _projection(probe)
        assert "地图委托完成" in reply
        assert state["inventory"] == [(99001, 5, 3)], state
        assert state["profile"] == [(112345,)]
        assert state["mission"][0][3] == 1 and len(state["receipt"]) == len(state["outbox"]) == 1
        _integrity(probe)
        probe.evidence["existing_inventory"] = state

    @register_case(
        "map-mission-claim-inventory-full-rejects-reward",
        "委托完成",
        "合法领取但背包物品已达容量上限",
        "已完成采集委托，奖励目标物品当前数量加奖励后超过配置上限",
        "明确拒领；钱包、物品、claimed、永久回执和 outbox 均不变",
        outcome="rejection",
    )
    async def inventory_full_rejection(probe):
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_config import XiuConfig
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_map as module

        module, clock, _ = _setup(probe)
        probe.player(stone=100000)
        game_db = probe.context.paths.game_db
        max_goods_num = int(XiuConfig().max_goods_num)
        current_num = max(0, max_goods_num - 1)
        probe.seed(
            game_db,
            "INSERT INTO back(user_id,goods_id,goods_name,goods_type,goods_num,create_time,update_time,bind_num) "
            "VALUES('900001',99001,'委托灵玉','材料',?,'2026-11-23 11:00:00','2026-11-23 11:00:00',0)",
            (current_num,),
        )
        reward_meta = {
            "stone_delta": 12345,
            "item_delta": [{"id": 99001, "name": "委托灵玉", "type": "材料", "amount": 2}],
        }
        before = _projection(probe)
        with patch.object(module.map_reward_resolver, "roll_mission_reward", return_value=(
            ["灵石x12345", "委托灵玉x2"], reward_meta,
        )), patch.object(module, "runtime_clock", clock):
            reply = await probe.command("委托完成", "委托完成", number=978137)
        after = _projection(probe)
        assert "背包物品已达上限" in reply and "地图委托完成" not in reply, reply
        assert after["profile"] == before["profile"]
        assert after["mission"][0][:4] == before["mission"][0][:4]
        assert json.loads(after["mission"][0][4]) == {
            "rewards": ["灵石x12345", "委托灵玉x2"], "reward_meta": reward_meta,
        }, after
        for key in ("inventory", "receipt", "outbox"):
            assert after[key] == before[key], (key, before, after)
        _integrity(probe)
        probe.evidence.update(before=before, after=after, capacity=max_goods_num)

    @register_case(
        "map-mission-query-initialization-write-rejected",
        "地图委托",
        "未接取状态查询触发首次状态初始化",
        "隔离库已迁移但玩家没有 map_mission 行，首次 INSERT 被忽略",
        "不把初始化失败伪装成未接取；故障解除后原查询可恢复",
        outcome="recovery",
    )
    async def query_initialization_recovery(probe):
        from nonebot_plugin_xiuxian_2.core.player_reasons import reason_text
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_map as module

        module, clock, _ = _setup(probe)
        player_db = probe.context.paths.player_db
        probe.seed(player_db, "DELETE FROM map_mission WHERE user_id='900001'")
        probe.seed(
            player_db,
            "CREATE TRIGGER mission_query_init_ignore BEFORE INSERT ON map_mission "
            "BEGIN SELECT RAISE(IGNORE); END",
        )
        with patch.object(module, "runtime_clock", clock):
            failed = await probe.command("地图委托", "地图委托", number=978140)
        assert reason_text("map_mission_state_write_rejected") in failed, failed
        assert probe.rows(player_db, "SELECT * FROM map_mission WHERE user_id='900001'") == []
        probe.seed(player_db, "DROP TRIGGER mission_query_init_ignore")
        probe.clear_cooldowns()
        with patch.object(module, "runtime_clock", clock):
            recovered = await probe.command("地图委托", "地图委托", number=978140)
        state = probe.rows(
            player_db,
            "SELECT date,mission_type,target,claimed,settlement FROM map_mission WHERE user_id='900001'",
        )
        assert "尚未接取" in recovered and state == [("2026-11-23", "", 0, 0, "")], (recovered, state)
        _integrity(probe)
        probe.evidence.update(recovered_state=state, first_reply=failed)

    state_save_specs = {
        "accept": {
            "event": "BEFORE UPDATE OF mission_type ON map_mission WHEN NEW.mission_type <> ''",
            "after": "UPDATE map_mission SET mission_type='',target=0 WHERE user_id=NEW.user_id;",
            "ignore_reason": "map_mission_state_write_rejected",
            "mismatch_reason": "map_mission_state_projection_mismatch",
        },
        "snapshot": {
            "event": "BEFORE UPDATE OF settlement ON map_mission WHEN NEW.settlement <> ''",
            "after": "UPDATE map_mission SET settlement='tampered' WHERE user_id=NEW.user_id;",
            "ignore_reason": "map_mission_state_write_rejected",
            "mismatch_reason": "map_mission_state_projection_mismatch",
        },
    }
    for save_path, spec in state_save_specs.items():
        for mode in ("ignore", "rewrite", "abort"):
            key = f"map-mission-{save_path}-state-{mode}"
            @register_case(
                key,
                "接取委托" if save_path == "accept" else "委托完成",
                f"真实 {save_path} 状态保存点注入 {mode}",
                "迁移完成的隔离库，任务随机源与奖励均固定",
                "具体失败回复且无不完整任务/奖励提交，解除故障后原消息可恢复",
                outcome="recovery",
            )
            async def state_save_recovery(probe, *, _key=key, _path=save_path, _spec=spec, _mode=mode):
                from nonebot_plugin_xiuxian_2.core.player_reasons import reason_text
                from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_map as module

                module, clock, _ = _setup(
                    probe,
                    mission_type="" if _path == "accept" else "gather",
                    target=0 if _path == "accept" else 1,
                    settlement="" if _path == "accept" else "",
                )
                player_db, game_db = probe.context.paths.player_db, probe.context.paths.game_db
                probe.player(stone=100000)
                reward_meta = {
                    "stone_delta": 12345,
                    "item_delta": [{"id": 99001, "name": "委托灵玉", "type": "材料", "amount": 2}],
                }
                reward_display = ["灵石x12345", "委托灵玉x2"]

                class FixedChoice:
                    @staticmethod
                    def choice(values):
                        if list(values) == list(module.MAP_MISSION_CONFIG):
                            return "gather"
                        return values[0]

                trigger = f"mission_{_path}_{_mode}"
                event = _spec["event"]
                if _mode == "abort":
                    create = f"CREATE TRIGGER {trigger} {event} BEGIN SELECT RAISE(ABORT,'mission state save rejected by test fixture'); END"
                elif _mode == "ignore":
                    create = f"CREATE TRIGGER {trigger} {event} BEGIN SELECT RAISE(IGNORE); END"
                else:
                    create = f"CREATE TRIGGER {trigger} {event.replace('BEFORE ', 'AFTER ', 1)} BEGIN {_spec['after']} END"
                probe.seed(player_db, create)

                def fixed_reward(**_kwargs):
                    return reward_display, reward_meta

                patch_reward = patch.object(
                    module.map_reward_resolver,
                    "roll_mission_reward",
                    side_effect=fixed_reward,
                )
                with patch_reward, patch.object(module, "runtime_random", FixedChoice()):
                    before_mission = probe.rows(
                        player_db,
                        "SELECT date,mission_type,target,claimed,settlement FROM map_mission WHERE user_id='900001'",
                    )
                    before_rewards = _projection(probe)
                    command = "接取委托" if _path == "accept" else "委托完成"
                    text = command
                    number = 978150 + len(probe.calls)
                    with patch.object(module, "runtime_clock", clock):
                        failed = await probe.command(command, text, number=number, allow_exception=True)
                    call = probe.calls[-1]
                    after_failure = probe.rows(
                        player_db,
                        "SELECT date,mission_type,target,claimed,settlement FROM map_mission WHERE user_id='900001'",
                    )
                    if _mode == "abort":
                        assert "定位号" in failed, (_path, failed)
                        assert call["logs"] and any(
                            "mission state save rejected by test fixture" in row["exception"]
                            for row in call["logs"]
                        ), call
                    else:
                        expected_reason = _spec["ignore_reason"] if _mode == "ignore" else _spec["mismatch_reason"]
                        assert reason_text(expected_reason) in failed, (_path, _mode, failed)
                        assert "地图委托记录" in failed, failed
                    assert after_failure == before_mission, (_path, _mode, before_mission, after_failure)
                    assert _projection(probe) == before_rewards, (_path, _mode, before_rewards, _projection(probe))

                    probe.seed(player_db, f"DROP TRIGGER {trigger}")
                    probe.clear_cooldowns()
                    with patch.object(module, "runtime_clock", clock):
                        if _path == "accept":
                            recovered = await probe.command(command, text, number=number)
                            expected_mission = "gather"
                        else:
                            recovered = await probe.command(command, text, number=number, allow_exception=True)
                            expected_mission = "gather"
                    state = probe.rows(
                        player_db,
                        "SELECT date,mission_type,target,claimed,settlement FROM map_mission WHERE user_id='900001'",
                    )
                    if _path == "accept":
                        assert "已接取今日地图委托" in recovered and state[0][1] == expected_mission, (recovered, state)
                    else:
                        assert "地图委托完成" in recovered, recovered
                        assert state[0][3] == 1 and json.loads(state[0][4]) == {
                            "rewards": reward_display, "reward_meta": reward_meta,
                        }, state
                        assert probe.rows(game_db, "SELECT stone FROM user_xiuxian WHERE user_id='900001'") == [(112345,)]
                        assert probe.rows(game_db, "SELECT goods_id,goods_num,bind_num FROM back WHERE user_id='900001'") == [(99001, 2, 2)]
                        assert probe.rows(game_db, "SELECT COUNT(*) FROM map_mission_claim_operations") == [(1,)]
                    _integrity(probe)
                    probe.evidence[_key] = {
                        "before": before_mission,
                        "after_failure": after_failure,
                        "recovered": state,
                        "expected_mission": expected_mission,
                    }

    fault_specs = {
        "mission": {
            "database": "player",
            "trigger": "BEFORE UPDATE OF claimed ON map_mission WHEN NEW.claimed=1",
            "after": "UPDATE map_mission SET claimed=0 WHERE user_id=NEW.user_id;",
            "ignore_reason": "map_mission_claim_mission_write_rejected",
            "mismatch_reason": "map_mission_claim_mission_projection_mismatch",
            "subject": "map_mission",
        },
        "stone": {
            "database": "game",
            "trigger": "BEFORE UPDATE OF stone ON user_xiuxian WHEN NEW.user_id='900001'",
            "after": "UPDATE user_xiuxian SET stone=OLD.stone WHERE user_id=NEW.user_id;",
            "ignore_reason": "map_mission_claim_stone_write_rejected",
            "mismatch_reason": "map_mission_claim_stone_projection_mismatch",
            "subject": "stone_wallet",
        },
        "inventory": {
            "database": "game",
            "trigger": "BEFORE INSERT ON back WHEN NEW.user_id='900001' AND NEW.goods_id=99001",
            "after": "UPDATE back SET goods_num=0 WHERE user_id=NEW.user_id AND goods_id=NEW.goods_id;",
            "ignore_reason": "map_mission_claim_inventory_write_rejected",
            "mismatch_reason": "map_mission_claim_inventory_projection_mismatch",
            "subject": "item:99001",
        },
        "receipt": {
            "database": "game",
            "trigger": "BEFORE INSERT ON map_mission_claim_operations",
            "after": "UPDATE map_mission_claim_operations SET payload='tampered' WHERE operation_id=NEW.operation_id;",
            "ignore_reason": "map_mission_claim_receipt_write_rejected",
            "mismatch_reason": "map_mission_claim_receipt_projection_mismatch",
            "subject": "receipt",
        },
        "outbox": {
            "database": "game",
            "trigger": "BEFORE INSERT ON domain_outbox",
            "after": "UPDATE domain_outbox SET payload_json='{}' WHERE event_id=NEW.event_id;",
            "ignore_reason": "map_mission_claim_outbox_write_rejected",
            "mismatch_reason": "map_mission_claim_outbox_projection_mismatch",
            "subject": "domain_outbox",
        },
    }

    for fault_name, spec in fault_specs.items():
        for mode in ("ignore", "rewrite", "abort"):
            if fault_name == "receipt" and mode == "ignore":
                continue
            key = f"map-mission-claim-{fault_name}-{mode}"
            @register_case(
                key,
                "委托完成",
                f"合法完成委托，注入 {fault_name} 写入 {mode}",
                "已完成采集委托和确定的灵石/物品奖励，隔离双库已迁移",
                "写入失败不谎报成功、完整回滚、故障解除后同消息领取成功且重放无二次奖励",
                outcome="recovery",
            )
            async def write_fault_recovery(probe, *, _key=key, _fault=fault_name, _mode=mode, _spec=spec):
                module, clock, _ = _setup(probe)
                probe.player(stone=100000)
                reward_meta = {
                    "stone_delta": 12345,
                    "item_delta": [{"id": 99001, "name": "委托灵玉", "type": "材料", "amount": 2}],
                }
                reward_display = ["灵石x12345", "委托灵玉x2"]
                trigger = f"mission_{_fault}_{_mode}"
                database = probe.context.paths.player_db if _spec["database"] == "player" else probe.context.paths.game_db
                if _mode == "abort":
                    create = (
                        f"CREATE TRIGGER {trigger} {_spec['trigger']} "
                        "BEGIN SELECT RAISE(ABORT,'mission write rejected by test fixture'); END"
                    )
                elif _mode == "ignore":
                    create = (
                        f"CREATE TRIGGER {trigger} {_spec['trigger']} "
                        "BEGIN SELECT RAISE(IGNORE); END"
                    )
                elif _fault == "inventory":
                    create = (
                        "CREATE TRIGGER mission_inventory_rewrite AFTER INSERT ON back "
                        "WHEN NEW.user_id='900001' AND NEW.goods_id=99001 BEGIN "
                        + _spec["after"] + " END"
                    )
                else:
                    event = _spec["trigger"].replace("BEFORE ", "AFTER ", 1)
                    create = f"CREATE TRIGGER {trigger} {event} BEGIN {_spec['after']} END"
                probe.seed(database, create)

                def faultless_reward(**_kwargs):
                    return reward_display, reward_meta

                with patch.object(module.map_reward_resolver, "roll_mission_reward", side_effect=faultless_reward):
                    before = _projection(probe)
                    message_number = 978200 + len(probe.calls)
                    with patch.object(module, "runtime_clock", clock):
                        failed_reply = await probe.command(
                            "委托完成", "委托完成", number=message_number,
                            allow_exception=True,
                        )
                    failure_call = probe.calls[-1]
                    after_failure = _projection(probe)
                    operation_id = f"map-mission:900001:2026-11-23:gather:{message_number}"
                    failed_ledger = probe.rows(
                        probe.context.paths.game_db,
                        "SELECT status FROM operation_ledger WHERE operation_id=? AND action='map.mission_claim'",
                        (operation_id,),
                    )
                    if _mode == "abort":
                        assert "定位号" in failed_reply, (_fault, failed_reply, failure_call)
                        assert failure_call["logs"] and any(
                            "mission write rejected by test fixture" in row["exception"]
                            for row in failure_call["logs"]
                        ), failure_call
                        assert failed_ledger == [("failed",)], failed_ledger
                    else:
                        expected_reason = _spec["ignore_reason"] if _mode == "ignore" else _spec["mismatch_reason"]
                        from nonebot_plugin_xiuxian_2.core.player_reasons import reason_text

                        assert reason_text(expected_reason) in failed_reply, (_fault, _mode, failed_reply)
                        expected_subject = f"背包物品（ID 99001）" if _fault == "inventory" else {
                            "map_mission": "地图委托记录",
                            "stone_wallet": "灵石钱包",
                            "receipt": "操作回执",
                            "domain_outbox": "奖励统计回执",
                        }[_spec["subject"]]
                        assert expected_subject in failed_reply, (_fault, _mode, failed_reply)
                        assert failed_ledger == [], failed_ledger
                    assert after_failure["profile"] == before["profile"], (_fault, _mode, before, after_failure)
                    assert after_failure["mission"][0][:4] == before["mission"][0][:4], (_fault, _mode, before, after_failure)
                    assert json.loads(after_failure["mission"][0][4]) == {
                        "rewards": reward_display, "reward_meta": reward_meta,
                    }, after_failure
                    for projection in ("inventory", "receipt", "outbox"):
                        assert after_failure[projection] == before[projection], (
                            _fault, _mode, projection, before, after_failure,
                        )

                    probe.seed(database, f"DROP TRIGGER {trigger}")
                    probe.clear_cooldowns()
                    with patch.object(module, "runtime_clock", clock):
                        recovered = await probe.command(
                            "委托完成", "委托完成", number=message_number, allow_exception=True,
                        )
                    committed = _projection(probe)
                    ledger = probe.rows(
                        probe.context.paths.game_db,
                        "SELECT status FROM operation_ledger WHERE operation_id=? AND action='map.mission_claim'",
                        (operation_id,),
                    )
                    assert ledger == [("applied",)], ledger
                    audit_before_replay = probe.rows(
                        probe.context.paths.game_db,
                        "SELECT status FROM operation_audit WHERE operation_id=? AND action='map.mission_claim' ORDER BY audit_id",
                        (operation_id,),
                    )
                    assert audit_before_replay == ([('failed',), ('applied',)] if _mode == "abort" else [('applied',)]), audit_before_replay
                    assert "地图委托完成" in recovered, (_fault, _mode, recovered)
                    assert committed["mission"][0][3] == 1 and len(committed["receipt"]) == 1, committed
                    assert committed["profile"][0][0] == before["profile"][0][0] + 12345, committed
                    assert committed["inventory"] == [(99001, 2, 2)], committed
                    assert len(committed["outbox"]) == 1, committed
                    probe.clear_cooldowns()
                    with patch.object(module, "runtime_clock", clock):
                        await probe.command(
                            "委托完成", "委托完成", number=message_number, allow_exception=True,
                        )
                    assert _projection(probe) == committed, (_fault, _mode, "duplicate changed projection")
                    assert probe.rows(
                        probe.context.paths.game_db,
                        "SELECT status FROM operation_ledger WHERE operation_id=? AND action='map.mission_claim'",
                        (operation_id,),
                    ) == ledger
                    assert probe.rows(
                        probe.context.paths.game_db,
                        "SELECT status FROM operation_audit WHERE operation_id=? AND action='map.mission_claim' ORDER BY audit_id",
                        (operation_id,),
                    ) == audit_before_replay
                _integrity(probe)
                probe.evidence[_key] = {
                    "fault": _fault, "mode": _mode, "before": before,
                    "after_failure": after_failure, "recovered": committed,
                    "aborted_traceback": _mode == "abort",
                }

    assert len(expected_keys) == 28, expected_keys
