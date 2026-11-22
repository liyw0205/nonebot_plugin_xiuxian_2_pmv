"""Real matcher cases for map gathering, fishing, and mining actions."""

from datetime import datetime
import asyncio
import json
from types import SimpleNamespace
from unittest.mock import patch

from tests.parameter_cases_map_explore_start import _integrity


ACTOR = "900001"
NOW = datetime(2026, 11, 22, 14, 0)


def _setup(probe, action_type="采集", wanted_node_type=None):
    from nonebot_plugin_xiuxian_2.features.map.migrations import (
        apply_map_interactive_player,
        apply_map_interactive_start,
        apply_map_resource_reward,
    )
    from nonebot_plugin_xiuxian_2.plugin import apply_platform_schema
    from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
    from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_map as module

    probe.clear_cooldowns()
    game, player = probe.context.paths.game_db, probe.context.paths.player_db
    with DatabaseUnitOfWork(game) as uow:
        apply_platform_schema(uow)
        apply_map_interactive_start(uow)
        apply_map_resource_reward(uow)
    with DatabaseUnitOfWork(player) as uow:
        apply_map_interactive_player(uow)

    data = module._load_map_data()
    node_type = wanted_node_type or next(
        node_type for node_type, config in module.NODE_ACTION_CONFIG.items()
        if config["cmd"] == action_type
    )
    realm, heaven, node = next(
        (realm, heaven, node)
        for realm in module._all_realms(data)
        for heaven in module._heaven_names(data, realm)
        for node in module._nodes(data, realm, heaven)
        if node["type"] == node_type
    )
    module.map_application.save_status(ACTOR, realm, heaven, node["id"], [node["id"]])
    module.map_application.save_daily_limit(ACTOR, {
        "date": "2026-11-22", "gather_count": 0, "combat_count": 0,
        "explore_count": 0, "resource_total_count": 0,
    })
    module.map_application.set_cooldown(ACTOR, "gather_cd_until", None)
    probe.player(user_stamina=100)
    return module, realm, heaven, node


def _command(probe, name, *, actor=ACTOR, number, allow_exception=False):
    probe.clear_cooldowns()
    return probe.command(name, actor=actor, number=number, allow_exception=allow_exception)


def _projection(probe, operation_id):
    game, player = probe.context.paths.game_db, probe.context.paths.player_db
    return {
        "stamina": probe.rows(game, "SELECT user_stamina FROM user_xiuxian WHERE user_id=?", (ACTOR,)),
        "active": probe.rows(player, "SELECT action_id,action_type,status,state_json,settlement_json,ready_at,expires_at,cooldown_seconds FROM map_interactive_actions WHERE user_id=?", (ACTOR,)),
        "cooldown": probe.rows(player, "SELECT gather_cd_until FROM map_cooldown WHERE user_id=?", (ACTOR,)),
        "start_receipt": probe.rows(game, "SELECT payload,result_status,stamina,action_json FROM map_interactive_start_operations WHERE operation_id=?", (operation_id,)),
        "ledger": probe.rows(game, "SELECT status FROM operation_ledger WHERE operation_id=? AND action='map.interactive_start'", (operation_id,)),
    }


def _seed_ready_action(
    probe, module, *, action_id, success, settlement="", action_type="采集",
    node_type="灵林", pool_key="herb_low", ready_ts="2026-11-22 13:59:01",
    expire_ts="2026-11-22 14:01:00",
):
    action = {
        "action_id": action_id, "action": action_type, "node_name": "诊断节点",
        "node_type": node_type, "pool_key": pool_key,
        "start_ts": "2026-11-22 13:59:00", "ready_ts": ready_ts,
        "expire_ts": expire_ts, "wait_sec": 1,
        "cost": 3, "cooldown_sec": 22, "success": success,
    }
    state = json.dumps(action, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    probe.seed(
        probe.context.paths.player_db,
        "INSERT INTO map_interactive_actions(user_id,action_id,action_type,status,state_json,settlement_json,ready_at,expires_at,cooldown_seconds,updated_at) "
        "VALUES(?,?,?,'active',?,?,?,?,?,?)",
        (ACTOR, action_id, action_type, state, settlement,
         ready_ts, expire_ts, 22, "2026-11-22 13:59:00"),
    )
    return action


def register(case):
    @case(
        "map-interactive-start-receipt-ignore", "采集", "永久回执写拒绝",
        "合法灵林节点与已注册玩家", "不伪成功；体力和活动状态保持不变",
        group="map-interactive", outcome="recovery",
    )
    async def start_receipt_ignore(probe):
        module, _, _, _ = _setup(probe)
        operation_id = module._map_operation_id(SimpleNamespace(message_id="975001"), "interactive-start", ACTOR)
        probe.seed(
            probe.context.paths.game_db,
            "CREATE TRIGGER interactive_receipt_ignore BEFORE INSERT ON map_interactive_start_operations BEGIN SELECT RAISE(IGNORE); END",
        )

        with (
            patch.object(module, "runtime_clock", SimpleNamespace(now=lambda: NOW)),
            patch.object(module, "runtime_random", SimpleNamespace(randint=lambda low, high: low, random=lambda: 0.0)),
            patch.object(module, "asyncio", SimpleNamespace(create_task=lambda coroutine: coroutine.close())),
        ):
            reply = await _command(probe, "采集", number=975001)
        projected = _projection(probe, operation_id)
        probe.evidence.update(reply=reply, projection=projected)
        assert "回执未能保存" in reply and "静候" not in reply, reply
        assert projected["stamina"] == [(100,)] and not projected["active"], projected
        assert not projected["start_receipt"] and not projected["ledger"], projected

        probe.seed(probe.context.paths.game_db, "DROP TRIGGER interactive_receipt_ignore")
        with (
            patch.object(module, "runtime_clock", SimpleNamespace(now=lambda: NOW)),
            patch.object(module, "runtime_random", SimpleNamespace(randint=lambda low, high: low, random=lambda: 0.0)),
            patch.object(module, "asyncio", SimpleNamespace(create_task=lambda coroutine: coroutine.close())),
        ):
            recovered = await _command(probe, "采集", number=975001)
        recovered_projection = _projection(probe, operation_id)
        assert "预计等待" in recovered, recovered
        assert recovered_projection["stamina"] == [(97,)], recovered_projection
        assert len(recovered_projection["active"]) == 1, recovered_projection
        assert len(recovered_projection["start_receipt"]) == 1, recovered_projection
        assert recovered_projection["ledger"] == [("applied",)], recovered_projection
        probe.evidence.update(recovered_reply=recovered, recovered_projection=recovered_projection)
        _integrity(probe)

    @case(
        "map-interactive-failure-action-ignore", "采收", "到时但失败的活动终结",
        "有效活动已到时机，失败分支的活动状态更新被拒",
        "不回复已完成失败；活动、冷却与终态回执保持原样",
        group="map-interactive", outcome="recovery",
    )
    async def failure_action_ignore(probe):
        module, _, _, _ = _setup(probe)
        action_id = "map-interactive-failure-ignore"
        _seed_ready_action(probe, module, action_id=action_id, success=False)
        probe.seed(
            probe.context.paths.player_db,
            "CREATE TRIGGER interactive_failure_ignore BEFORE UPDATE ON map_interactive_actions "
            "WHEN NEW.status='failed' BEGIN SELECT RAISE(IGNORE); END",
        )
        with patch.object(module, "runtime_clock", SimpleNamespace(now=lambda: NOW)):
            reply = await _command(probe, "采收", number=975002)
        action = probe.rows(
            probe.context.paths.player_db,
            "SELECT status,settlement_json FROM map_interactive_actions WHERE user_id=?",
            (ACTOR,),
        )
        cooldown = probe.rows(
            probe.context.paths.player_db,
            "SELECT gather_cd_until FROM map_cooldown WHERE user_id=?", (ACTOR,),
        )
        receipt = probe.rows(
            probe.context.paths.player_db,
            "SELECT result_status FROM map_interactive_terminal_operations WHERE operation_id=?",
            (f"map-interactive-failed:{action_id}",),
        )
        probe.evidence.update(reply=reply, action=action, cooldown=cooldown, receipt=receipt)
        assert "结束状态未能保存" in reply, reply
        assert action == [("active", "")], action
        assert cooldown == [(None,)], cooldown
        assert not receipt, receipt
        _integrity(probe)

    @case(
        "map-interactive-reward-receipt-ignore", "采收", "已冻结灵石奖励结算",
        "有效成功活动已到时机，奖励永久回执 INSERT 被忽略",
        "不伪成功；钱包、次数、活动、冷却与回执整笔保持不变",
        group="map-interactive", outcome="recovery",
    )
    async def reward_receipt_ignore(probe):
        module, _, _, _ = _setup(probe)
        action_id = "map-interactive-reward-ignore"
        settlement = {
            "daily": {"date": "2026-11-22", "gather_count": 0,
                      "combat_count": 0, "explore_count": 0,
                      "resource_total_count": 0},
            "decay": 1.0, "rewards": ["灵石 17"], "stone": 17,
            "items": [], "extra_msg": "",
        }
        settlement_json = json.dumps(settlement, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
        _seed_ready_action(probe, module, action_id=action_id, success=True, settlement=settlement_json)
        probe.seed(
            probe.context.paths.game_db,
            "CREATE TRIGGER interactive_reward_receipt_ignore BEFORE INSERT ON map_resource_reward_operations "
            "BEGIN SELECT RAISE(IGNORE); END",
        )
        stone_before = probe.rows(
            probe.context.paths.game_db,
            "SELECT stone FROM user_xiuxian WHERE user_id=?", (ACTOR,),
        )
        with patch.object(module, "runtime_clock", SimpleNamespace(now=lambda: NOW)):
            reply = await _command(probe, "采收", number=975003)
        stone_after = probe.rows(
            probe.context.paths.game_db,
            "SELECT stone FROM user_xiuxian WHERE user_id=?", (ACTOR,),
        )
        daily = probe.rows(
            probe.context.paths.player_db,
            "SELECT gather_count,resource_total_count FROM map_daily_limit WHERE user_id=?",
            (ACTOR,),
        )
        action = probe.rows(
            probe.context.paths.player_db,
            "SELECT status,settlement_json FROM map_interactive_actions WHERE user_id=?",
            (ACTOR,),
        )
        cooldown = probe.rows(
            probe.context.paths.player_db,
            "SELECT gather_cd_until FROM map_cooldown WHERE user_id=?", (ACTOR,),
        )
        receipt = probe.rows(
            probe.context.paths.game_db,
            "SELECT operation_id FROM map_resource_reward_operations WHERE operation_id=?",
            (f"map-resource:{action_id}",),
        )
        probe.evidence.update(reply=reply, stone_before=stone_before, stone_after=stone_after,
                              daily=daily, action=action, cooldown=cooldown, receipt=receipt)
        assert "奖励回执未能保存" in reply, reply
        assert stone_after == stone_before, (stone_before, stone_after)
        assert daily == [(0, 0)], daily
        assert action and action[0][0] == "active", action
        assert cooldown == [(None,)], cooldown
        assert not receipt, receipt
        _integrity(probe)

    @case(
        "map-interactive-all-start-commands", "钓鱼/挖矿/采集", "各节点资源行动合法启动",
        "已注册玩家位于对应实际水域/矿脉/灵林/仙山节点",
        "每条真实 matcher 扣除配置体力并持久化对应活动与回执",
        group="map-interactive", outcome="success",
    )
    async def all_start_commands(probe):
        from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork

        costs = (("钓鱼", "水域", 4), ("挖矿", "矿脉", 5),
                 ("采集", "灵林", 3), ("采集", "仙山", 4))
        results = []
        for index, (command, node_type, cost) in enumerate(costs):
            module, realm, heaven, node = _setup(probe, command, node_type)
            if index:
                probe.seed(
                    probe.context.paths.player_db,
                    "UPDATE map_interactive_actions SET status='expired' WHERE user_id=?",
                    (ACTOR,),
                )
            number = 975010 + index
            operation_id = f"map-interactive-start:{number}:{ACTOR}"
            with (
                patch.object(module, "runtime_clock", SimpleNamespace(now=lambda: NOW)),
                patch.object(module, "runtime_random", SimpleNamespace(randint=lambda low, high: low, random=lambda: 0.0)),
                patch.object(module, "asyncio", SimpleNamespace(create_task=lambda coroutine: coroutine.close())),
            ):
                reply = await _command(probe, command, number=number)
            stamina = probe.rows(
                probe.context.paths.game_db,
                "SELECT user_stamina FROM user_xiuxian WHERE user_id=?", (ACTOR,),
            )
            action = probe.rows(
                probe.context.paths.player_db,
                "SELECT action_type,status FROM map_interactive_actions WHERE user_id=?", (ACTOR,),
            )
            receipt = probe.rows(
                probe.context.paths.game_db,
                "SELECT result_status,stamina FROM map_interactive_start_operations WHERE operation_id=?",
                (operation_id,),
            )
            assert "预计等待" in reply, (command, node_type, reply)
            assert stamina == [(100 - cost,)], (command, node_type, stamina)
            assert action == [(command, "active")], (command, node_type, action)
            assert receipt == [("applied", 100 - cost)], (command, node_type, receipt)
            replay = await _command(probe, command, number=number)
            assert "预计等待" in replay, (command, replay)
            assert probe.rows(probe.context.paths.game_db, "SELECT user_stamina FROM user_xiuxian WHERE user_id=?", (ACTOR,)) == stamina
            if index == 0:
                conflict = await _command(probe, "挖矿", number=number)
                assert "已用于其他资源行动" in conflict, conflict
                assert probe.rows(probe.context.paths.game_db, "SELECT user_stamina FROM user_xiuxian WHERE user_id=?", (ACTOR,)) == stamina
            results.append((command, node_type, cost, reply))
            _integrity(probe)
        second_user = "900002"
        probe.player(second_user, user_stamina=100)
        module.map_application.save_status(
            second_user, realm, heaven, node["id"], [node["id"]]
        )
        module.map_application.save_daily_limit(second_user, {
            "date": "2026-11-22", "gather_count": 0, "combat_count": 0,
            "explore_count": 0, "resource_total_count": 0,
        })
        module.map_application.set_cooldown(second_user, "gather_cd_until", None)
        with (
            patch.object(module, "runtime_clock", SimpleNamespace(now=lambda: NOW)),
            patch.object(module, "runtime_random", SimpleNamespace(randint=lambda low, high: low, random=lambda: 0.0)),
            patch.object(module, "asyncio", SimpleNamespace(create_task=lambda coroutine: coroutine.close())),
        ):
            other_reply = await _command(probe, "采集", actor=second_user, number=975015)
        isolated_actions = probe.rows(
            probe.context.paths.player_db,
            "SELECT user_id,action_type,status FROM map_interactive_actions WHERE user_id IN (?,?) ORDER BY user_id",
            (ACTOR, second_user),
        )
        assert "预计等待" in other_reply, other_reply
        assert isolated_actions == [(ACTOR, "采集", "active"), (second_user, "采集", "active")], isolated_actions
        probe.evidence.update(starts=results, second_user=other_reply, isolated_actions=isolated_actions)

    @case(
        "map-interactive-all-resolve-commands", "收杆/落镐/采收", "各节点资源行动成功结算",
        "对应成功活动均到时机且已有冻结奖励，发放灵石奖励",
        "三种真实结算 matcher 均完成活动、递增次数、发放并保存永久回执",
        group="map-interactive", outcome="success",
    )
    async def all_resolve_commands(probe):
        cases = (("钓鱼", "水域", "fish", "收杆"),
                 ("挖矿", "矿脉", "ore", "落镐"),
                 ("采集", "灵林", "herb_low", "采收"))
        results = []
        for index, (action_type, node_type, pool_key, command) in enumerate(cases):
            module, _, _, _ = _setup(probe, action_type, node_type)
            probe.player(stone=1000000)
            action_id = f"map-interactive-resolve-{index}"
            settlement = {
                "daily": {"date": "2026-11-22", "gather_count": 0,
                          "combat_count": 0, "explore_count": 0,
                          "resource_total_count": 0},
                "decay": 1.0, "rewards": ["灵石 17"], "stone": 17,
                "items": [], "extra_msg": "",
            }
            settlement_json = json.dumps(settlement, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
            if index:
                probe.seed(probe.context.paths.player_db, "DELETE FROM map_interactive_actions WHERE user_id=?", (ACTOR,))
                probe.seed(probe.context.paths.player_db, "DELETE FROM map_cooldown WHERE user_id=?", (ACTOR,))
            _seed_ready_action(
                probe, module, action_id=action_id, success=True,
                settlement=settlement_json, action_type=action_type,
                node_type=node_type, pool_key=pool_key,
            )
            if index == 0:
                wrong_resolve = await _command(probe, "落镐", number=975019)
                assert "应使用【收杆】" in wrong_resolve, wrong_resolve
                assert probe.rows(probe.context.paths.player_db, "SELECT status FROM map_interactive_actions WHERE user_id=?", (ACTOR,)) == [("active",)]
                assert not probe.rows(probe.context.paths.game_db, "SELECT 1 FROM map_resource_reward_operations")
            with patch.object(module, "runtime_clock", SimpleNamespace(now=lambda: NOW)):
                reply = await _command(probe, command, number=975020 + index)
            state = probe.rows(
                probe.context.paths.player_db,
                "SELECT status FROM map_interactive_actions WHERE user_id=?", (ACTOR,),
            )
            daily = probe.rows(
                probe.context.paths.player_db,
                "SELECT gather_count,resource_total_count FROM map_daily_limit WHERE user_id=?", (ACTOR,),
            )
            stone = probe.rows(
                probe.context.paths.game_db,
                "SELECT stone FROM user_xiuxian WHERE user_id=?", (ACTOR,),
            )
            receipt = probe.rows(
                probe.context.paths.game_db,
                "SELECT stone,rewards FROM map_resource_reward_operations WHERE operation_id=?",
                (f"map-resource:{action_id}",),
            )
            assert "成功" in reply, (command, reply)
            assert state == [("completed",)], state
            assert daily == [(1, 1)], daily
            assert stone == [(1000017,)], stone
            assert receipt == [(17, "[]")], receipt
            results.append((action_type, command, reply))
            _integrity(probe)
        probe.evidence["resolves"] = results

    @case(
        "map-interactive-reward-write-recovery-matrix", "采收", "奖励各写点拒绝与同消息恢复",
        "活动已到时机且冻结了真实奖励参数；逐一注入各个 SQLite 写点拒绝",
        "每类拒绝都整笔回滚，移除故障后原消息只发放一次完整奖励",
        group="map-interactive", outcome="recovery",
    )
    async def reward_write_recovery_matrix(probe):
        faults = (
            ("daily", "player", "CREATE TRIGGER reject_reward_daily BEFORE UPDATE ON map_daily_limit BEGIN SELECT RAISE(IGNORE); END", "次数记录未能保存"),
            ("stone", "game", "CREATE TRIGGER reject_reward_stone BEFORE UPDATE OF stone ON user_xiuxian BEGIN SELECT RAISE(IGNORE); END", "灵石奖励未能保存"),
            ("inventory", "game", "CREATE TRIGGER reject_reward_inventory BEFORE INSERT ON back BEGIN SELECT RAISE(IGNORE); END", "物品奖励未能保存"),
            ("action", "player", "CREATE TRIGGER reject_reward_action BEFORE UPDATE ON map_interactive_actions WHEN NEW.status='completed' BEGIN SELECT RAISE(IGNORE); END", "完成状态未能保存"),
            ("cooldown", "player", "CREATE TRIGGER reject_reward_cooldown BEFORE UPDATE OF gather_cd_until ON map_cooldown WHEN NEW.gather_cd_until IS NOT NULL BEGIN SELECT RAISE(IGNORE); END", "冷却未能保存"),
            ("receipt", "game", "CREATE TRIGGER reject_reward_receipt BEFORE INSERT ON map_resource_reward_operations BEGIN SELECT RAISE(IGNORE); END", "奖励回执未能保存"),
        )
        outcomes = []
        for index, (name, database, trigger, expected_text) in enumerate(faults):
            module, _, _, _ = _setup(probe)
            action_id = f"map-interactive-reward-matrix-{name}"
            if index:
                probe.seed(probe.context.paths.player_db, "DELETE FROM map_interactive_actions WHERE user_id=?", (ACTOR,))
                probe.seed(probe.context.paths.player_db, "DELETE FROM map_cooldown WHERE user_id=?", (ACTOR,))
                probe.seed(probe.context.paths.game_db, "DELETE FROM back WHERE user_id=?", (ACTOR,))
                probe.player(stone=1000000)
            if name == "inventory":
                probe.seed(
                    probe.context.paths.game_db,
                    "INSERT INTO back(user_id,goods_id,goods_name,goods_type,goods_num,create_time,update_time,bind_num) "
                    "VALUES(?,1,'诊断灵草','灵草',4,'','',NULL)",
                    (ACTOR,),
                )
            item = {"id": 1, "name": "诊断灵草", "type": "灵草", "amount": 1}
            settlement = {
                "daily": {"date": "2026-11-22", "gather_count": 0,
                          "combat_count": 0, "explore_count": 0,
                          "resource_total_count": 0},
                "decay": 1.0, "rewards": ["灵石 17、诊断灵草 x1"],
                "stone": 17, "items": [item], "extra_msg": "",
            }
            settlement_json = json.dumps(settlement, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
            _seed_ready_action(probe, module, action_id=action_id, success=True, settlement=settlement_json)
            probe.seed(
                probe.context.paths.player_db,
                "INSERT INTO map_cooldown(user_id,gather_cd_until) VALUES(?,NULL) "
                "ON CONFLICT(user_id) DO UPDATE SET gather_cd_until=NULL",
                (ACTOR,),
            )
            fault_database = probe.context.paths.player_db if database == "player" else probe.context.paths.game_db
            trigger_name = f"reject_reward_{name}"
            probe.seed(fault_database, trigger)
            before_stone = probe.rows(probe.context.paths.game_db, "SELECT stone FROM user_xiuxian WHERE user_id=?", (ACTOR,))
            before_daily = probe.rows(probe.context.paths.player_db, "SELECT gather_count,resource_total_count FROM map_daily_limit WHERE user_id=?", (ACTOR,))
            before_cooldown = probe.rows(probe.context.paths.player_db, "SELECT gather_cd_until FROM map_cooldown WHERE user_id=?", (ACTOR,))
            before_item = probe.rows(probe.context.paths.game_db, "SELECT goods_num,bind_num FROM back WHERE user_id=? AND goods_id=1", (ACTOR,))
            number = 975030 + index
            with patch.object(module, "runtime_clock", SimpleNamespace(now=lambda: NOW)):
                failed = await _command(probe, "采收", number=number)
            action = probe.rows(probe.context.paths.player_db, "SELECT status FROM map_interactive_actions WHERE user_id=?", (ACTOR,))
            cooldown = probe.rows(probe.context.paths.player_db, "SELECT gather_cd_until FROM map_cooldown WHERE user_id=?", (ACTOR,))
            actual_stone = probe.rows(probe.context.paths.game_db, "SELECT stone FROM user_xiuxian WHERE user_id=?", (ACTOR,))
            actual_daily = probe.rows(probe.context.paths.player_db, "SELECT gather_count,resource_total_count FROM map_daily_limit WHERE user_id=?", (ACTOR,))
            actual_item = probe.rows(probe.context.paths.game_db, "SELECT goods_num,bind_num FROM back WHERE user_id=? AND goods_id=1", (ACTOR,))
            receipt = probe.rows(probe.context.paths.game_db, "SELECT operation_id FROM map_resource_reward_operations WHERE operation_id=?", (f"map-resource:{action_id}",))
            assert expected_text in failed, (name, failed)
            assert actual_stone == before_stone and actual_daily == before_daily, (name, actual_stone, actual_daily)
            assert action == [("active",)], (name, action)
            assert cooldown == before_cooldown, (name, before_cooldown, cooldown)
            assert actual_item == before_item and not receipt, (name, before_item, actual_item, receipt)
            probe.seed(fault_database, f"DROP TRIGGER {trigger_name}")
            with patch.object(module, "runtime_clock", SimpleNamespace(now=lambda: NOW)):
                recovered = await _command(probe, "采收", number=number)
            actual_stone = probe.rows(probe.context.paths.game_db, "SELECT stone FROM user_xiuxian WHERE user_id=?", (ACTOR,))
            actual_daily = probe.rows(probe.context.paths.player_db, "SELECT gather_count,resource_total_count FROM map_daily_limit WHERE user_id=?", (ACTOR,))
            actual_item = probe.rows(probe.context.paths.game_db, "SELECT goods_num,bind_num FROM back WHERE user_id=? AND goods_id=1", (ACTOR,))
            assert "成功" in recovered, (name, recovered)
            assert actual_stone == [(1000017,)], (name, actual_stone)
            assert actual_daily == [(1, 1)], (name, actual_daily)
            expected_item = [(5, 1)] if name == "inventory" else [(1, 1)]
            assert actual_item == expected_item, (name, actual_item)
            assert len(probe.rows(probe.context.paths.game_db, "SELECT 1 FROM map_resource_reward_operations WHERE operation_id=?", (f"map-resource:{action_id}",))) == 1
            outcomes.append((name, failed, recovered))
            _integrity(probe)
        probe.evidence["faults"] = outcomes

    @case(
        "map-interactive-ready-expiry-boundaries", "采收", "就绪/截止时间边界",
        "活动开始前、恰好ready、恰好expires及超时后一秒分别发送合法结算指令",
        "未到时不写；ready和expiry边界仍有效；超时后具体终结且不发奖励",
        group="map-interactive", outcome="success",
    )
    async def ready_expiry_boundaries(probe):
        from datetime import timedelta

        module, _, _, _ = _setup(probe)
        action_id = "map-interactive-ready-boundaries"
        settlement = {
            "daily": {"date": "2026-11-22", "gather_count": 0,
                      "combat_count": 0, "explore_count": 0,
                      "resource_total_count": 0},
            "decay": 1.0, "rewards": ["灵石 17"], "stone": 17,
            "items": [], "extra_msg": "",
        }
        settlement_json = json.dumps(settlement, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
        _seed_ready_action(
            probe, module, action_id=action_id, success=True,
            settlement=settlement_json, ready_ts="2026-11-22 14:00:00",
            expire_ts="2026-11-22 14:00:02",
        )
        with patch.object(module, "runtime_clock", SimpleNamespace(now=lambda: NOW - timedelta(seconds=1))):
            early = await _command(probe, "采收", number=975040)
        assert "还没到时机" in early, early
        assert probe.rows(probe.context.paths.game_db, "SELECT stone FROM user_xiuxian WHERE user_id=?", (ACTOR,)) == [(1000000,)]
        with patch.object(module, "runtime_clock", SimpleNamespace(now=lambda: NOW)):
            ready_edge = await _command(probe, "采收", number=975041)
        assert "成功" in ready_edge, ready_edge
        assert probe.rows(probe.context.paths.player_db, "SELECT status FROM map_interactive_actions WHERE user_id=?", (ACTOR,)) == [("completed",)]

        probe.seed(probe.context.paths.player_db, "DELETE FROM map_interactive_actions WHERE user_id=?", (ACTOR,))
        probe.seed(probe.context.paths.player_db, "DELETE FROM map_cooldown WHERE user_id=?", (ACTOR,))
        probe.seed(probe.context.paths.player_db, "UPDATE map_daily_limit SET gather_count=0,resource_total_count=0 WHERE user_id=?", (ACTOR,))
        probe.player(stone=1000000)
        timeout_id = "map-interactive-expiry-boundary"
        _seed_ready_action(
            probe, module, action_id=timeout_id, success=True,
            settlement=settlement_json, ready_ts="2026-11-22 13:59:00",
            expire_ts="2026-11-22 14:00:00",
        )
        with patch.object(module, "runtime_clock", SimpleNamespace(now=lambda: NOW)):
            expiry_edge = await _command(probe, "采收", number=975042)
        assert "成功" in expiry_edge, expiry_edge

        probe.seed(probe.context.paths.player_db, "DELETE FROM map_interactive_actions WHERE user_id=?", (ACTOR,))
        probe.seed(probe.context.paths.player_db, "DELETE FROM map_cooldown WHERE user_id=?", (ACTOR,))
        probe.seed(probe.context.paths.player_db, "UPDATE map_daily_limit SET gather_count=0,resource_total_count=0 WHERE user_id=?", (ACTOR,))
        probe.player(stone=1000000)
        late_id = "map-interactive-expired-boundary"
        _seed_ready_action(
            probe, module, action_id=late_id, success=True,
            settlement=settlement_json, ready_ts="2026-11-22 13:59:00",
            expire_ts="2026-11-22 13:59:59",
        )
        with patch.object(module, "runtime_clock", SimpleNamespace(now=lambda: NOW)):
            late = await _command(probe, "采收", number=975043)
        assert "时机已过" in late, late
        assert probe.rows(probe.context.paths.player_db, "SELECT status FROM map_interactive_actions WHERE user_id=?", (ACTOR,)) == [("expired",)]
        assert probe.rows(probe.context.paths.game_db, "SELECT stone FROM user_xiuxian WHERE user_id=?", (ACTOR,)) == [(1000000,)]
        probe.evidence.update(early=early, ready_edge=ready_edge, expiry_edge=expiry_edge, late=late)
        _integrity(probe)

    @case(
        "map-interactive-start-preconditions", "钓鱼/采集", "节点/体力/次数/冷却/活动前置",
        "真实玩家依次触发错误节点、体力不足、每日上限、冷却、活动进行中",
        "每种业务拒绝均给出正确分支，体力与启动回执没有额外写入",
        group="map-interactive", outcome="rejection",
    )
    async def start_preconditions(probe):
        async def dispatch(module, command, number):
            with (
                patch.object(module, "runtime_clock", SimpleNamespace(now=lambda: NOW)),
                patch.object(module, "runtime_random", SimpleNamespace(randint=lambda low, high: low, random=lambda: 0.0)),
                patch.object(module, "asyncio", SimpleNamespace(create_task=lambda coroutine: coroutine.close())),
            ):
                return await _command(probe, command, number=number)

        module, _, _, _ = _setup(probe, "钓鱼", "水域")
        data = module._load_map_data()
        forest = next(
            (realm, heaven, node)
            for realm in module._all_realms(data)
            for heaven in module._heaven_names(data, realm)
            for node in module._nodes(data, realm, heaven)
            if node["type"] == "灵林"
        )
        module.map_application.save_status(ACTOR, forest[0], forest[1], forest[2]["id"], [forest[2]["id"]])
        wrong = await dispatch(module, "钓鱼", 975050)
        assert "无法进行" in wrong, wrong
        assert probe.rows(probe.context.paths.game_db, "SELECT user_stamina FROM user_xiuxian WHERE user_id=?", (ACTOR,)) == [(100,)]
        assert not probe.rows(probe.context.paths.game_db, "SELECT 1 FROM map_interactive_start_operations")

        module, _, _, _ = _setup(probe, "采集", "灵林")
        probe.player(user_stamina=2)
        low = await dispatch(module, "采集", 975051)
        assert "体力不足" in low, low
        assert probe.rows(probe.context.paths.game_db, "SELECT user_stamina FROM user_xiuxian WHERE user_id=?", (ACTOR,)) == [(2,)]
        assert not probe.rows(probe.context.paths.player_db, "SELECT 1 FROM map_interactive_actions WHERE user_id=? AND status='active'", (ACTOR,))

        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_map import DAILY_LIMIT_CONFIG

        module, _, _, _ = _setup(probe, "采集", "灵林")
        cap = int(DAILY_LIMIT_CONFIG["gather"])
        module.map_application.save_daily_limit(ACTOR, {
            "date": "2026-11-22", "gather_count": cap, "combat_count": 0,
            "explore_count": 0, "resource_total_count": cap,
        })
        limited = await dispatch(module, "采集", 975052)
        assert "次数已达上限" in limited, limited
        assert probe.rows(probe.context.paths.game_db, "SELECT user_stamina FROM user_xiuxian WHERE user_id=?", (ACTOR,)) == [(100,)]

        module, _, _, _ = _setup(probe, "采集", "灵林")
        module.map_application.set_cooldown(ACTOR, "gather_cd_until", "2026-11-22 14:00:30")
        cooling = await dispatch(module, "采集", 975053)
        assert "冷却剩余" in cooling, cooling
        assert probe.rows(probe.context.paths.game_db, "SELECT user_stamina FROM user_xiuxian WHERE user_id=?", (ACTOR,)) == [(100,)]

        module, _, _, _ = _setup(probe, "采集", "灵林")
        _seed_ready_action(probe, module, action_id="already-active", success=True)
        running = await dispatch(module, "采集", 975054)
        assert "进行中的采集动作" in running, running
        assert probe.rows(probe.context.paths.game_db, "SELECT user_stamina FROM user_xiuxian WHERE user_id=?", (ACTOR,)) == [(100,)]
        probe.evidence.update(wrong_node=wrong, low_stamina=low, daily_limit=limited,
                              cooldown=cooling, active=running)
        _integrity(probe)

    @case(
        "map-interactive-normal-failure-terminal", "收杆/落镐/采收", "失败活动正常终结",
        "三类活动均到ready但随机结果为失败",
        "各自使用匹配结算命令，持久化失败状态/冷却/终态回执且不发奖",
        group="map-interactive", outcome="success",
    )
    async def normal_failure_terminal(probe):
        cases = (("钓鱼", "水域", "fish", "收杆"),
                 ("挖矿", "矿脉", "ore", "落镐"),
                 ("采集", "灵林", "herb_low", "采收"))
        evidence = []
        for index, (action_type, node_type, pool_key, command) in enumerate(cases):
            module, _, _, _ = _setup(probe, action_type, node_type)
            action_id = f"map-interactive-normal-failure-{index}"
            if index:
                probe.seed(probe.context.paths.player_db, "DELETE FROM map_interactive_actions WHERE user_id=?", (ACTOR,))
                probe.seed(probe.context.paths.player_db, "DELETE FROM map_cooldown WHERE user_id=?", (ACTOR,))
            _seed_ready_action(
                probe, module, action_id=action_id, success=False,
                action_type=action_type, node_type=node_type, pool_key=pool_key,
            )
            with patch.object(module, "runtime_clock", SimpleNamespace(now=lambda: NOW)):
                reply = await _command(probe, command, number=975060 + index)
            terminal = probe.rows(
                probe.context.paths.player_db,
                "SELECT result_status FROM map_interactive_terminal_operations WHERE operation_id=?",
                (f"map-interactive-failed:{action_id}",),
            )
            action = probe.rows(
                probe.context.paths.player_db,
                "SELECT status FROM map_interactive_actions WHERE user_id=?", (ACTOR,),
            )
            daily = probe.rows(
                probe.context.paths.player_db,
                "SELECT gather_count,resource_total_count FROM map_daily_limit WHERE user_id=?", (ACTOR,),
            )
            assert "动作慢了半拍" in reply, (command, reply)
            assert action == [("failed",)], action
            assert terminal == [("applied",)], terminal
            assert daily == [(0, 0)], daily
            evidence.append((command, reply, action, terminal))
            _integrity(probe)
        probe.evidence["failures"] = evidence

    @case(
        "map-interactive-settlement-snapshot-ignore", "采收", "奖励快照拒写与恢复",
        "真实成功结算入口生成确定奖励，活动 settlement_json UPDATE 被忽略",
        "不发奖且活动保持待领取；解除故障后同消息使用固定快照只结算一次",
        group="map-interactive", outcome="recovery",
    )
    async def settlement_snapshot_ignore(probe):
        module, _, _, _ = _setup(probe)
        action_id = "map-interactive-settlement-ignore"
        _seed_ready_action(probe, module, action_id=action_id, success=True)
        probe.seed(
            probe.context.paths.player_db,
            "CREATE TRIGGER interactive_settlement_ignore BEFORE UPDATE OF settlement_json ON map_interactive_actions "
            "BEGIN SELECT RAISE(IGNORE); END",
        )
        class DeterministicRewards:
            @staticmethod
            def roll_rewards(plan, decay, *, random_source):
                return ["灵石x17"], 17, []

            @staticmethod
            def roll_dongfu_material(node_type, *, random_source):
                return [], 0, []

            @staticmethod
            def roll_skill_equip_drop(user_info, drop_rate, *, random_source):
                return None, None

        with (
            patch.object(module, "runtime_clock", SimpleNamespace(now=lambda: NOW)),
            patch.object(module, "runtime_random", SimpleNamespace(random=lambda: 0.0)),
            patch.object(module, "map_reward_resolver", DeterministicRewards()),
        ):
            failed = await _command(probe, "采收", number=975070)
        action = probe.rows(probe.context.paths.player_db, "SELECT status,settlement_json FROM map_interactive_actions WHERE user_id=?", (ACTOR,))
        stone = probe.rows(probe.context.paths.game_db, "SELECT stone FROM user_xiuxian WHERE user_id=?", (ACTOR,))
        daily = probe.rows(probe.context.paths.player_db, "SELECT gather_count,resource_total_count FROM map_daily_limit WHERE user_id=?", (ACTOR,))
        assert "奖励快照未能保存" in failed, failed
        assert action == [("active", "")], action
        assert stone == [(1000000,)] and daily == [(0, 0)], (stone, daily)
        assert not probe.rows(probe.context.paths.game_db, "SELECT 1 FROM map_resource_reward_operations")

        probe.seed(probe.context.paths.player_db, "DROP TRIGGER interactive_settlement_ignore")
        with (
            patch.object(module, "runtime_clock", SimpleNamespace(now=lambda: NOW)),
            patch.object(module, "runtime_random", SimpleNamespace(random=lambda: 0.0)),
            patch.object(module, "map_reward_resolver", DeterministicRewards()),
        ):
            recovered = await _command(probe, "采收", number=975070)
        assert "成功" in recovered, recovered
        assert probe.rows(probe.context.paths.player_db, "SELECT status FROM map_interactive_actions WHERE user_id=?", (ACTOR,)) == [("completed",)]
        assert probe.rows(probe.context.paths.game_db, "SELECT stone FROM user_xiuxian WHERE user_id=?", (ACTOR,)) == [(1000017,)]
        assert probe.rows(probe.context.paths.player_db, "SELECT gather_count,resource_total_count FROM map_daily_limit WHERE user_id=?", (ACTOR,)) == [(1, 1)]
        probe.evidence.update(failed=failed, recovered=recovered)
        _integrity(probe)

    @case(
        "map-interactive-start-write-recovery-matrix", "采集", "启动体力/活动写点拒绝与恢复",
        "真实合法灵林启动期间逐个拒绝或改写体力及活动状态写入",
        "每次失败回滚且不留活动/回执/ledger；解除故障后同消息成功一次",
        group="map-interactive", outcome="recovery",
    )
    async def start_write_recovery_matrix(probe):
        faults = (
            ("stamina", "CREATE TRIGGER reject_interactive_stamina BEFORE UPDATE OF user_stamina ON user_xiuxian BEGIN SELECT RAISE(IGNORE); END", "体力扣除未能保存"),
            ("action", "CREATE TRIGGER reject_interactive_action BEFORE INSERT ON map_interactive_actions BEGIN SELECT RAISE(IGNORE); END", "行动状态未能保存"),
            ("projection", "CREATE TRIGGER distort_interactive_action AFTER INSERT ON map_interactive_actions BEGIN UPDATE map_interactive_actions SET state_json='{}' WHERE user_id=NEW.user_id; END", "行动状态与本次请求不一致"),
        )
        outcomes = []
        for index, (name, trigger, expected_text) in enumerate(faults):
            module, _, _, _ = _setup(probe, "采集", "灵林")
            probe.seed(probe.context.paths.player_db, "DELETE FROM map_interactive_actions WHERE user_id=?", (ACTOR,))
            probe.player(user_stamina=100)
            number = 975080 + index
            operation_id = f"map-interactive-start:{number}:{ACTOR}"
            trigger_name = f"reject_interactive_{name}" if name != "projection" else "distort_interactive_action"
            fault_database = probe.context.paths.game_db if name == "stamina" else probe.context.paths.player_db
            probe.seed(fault_database, trigger)
            with (
                patch.object(module, "runtime_clock", SimpleNamespace(now=lambda: NOW)),
                patch.object(module, "runtime_random", SimpleNamespace(randint=lambda low, high: low, random=lambda: 0.0)),
                patch.object(module, "asyncio", SimpleNamespace(create_task=lambda coroutine: coroutine.close())),
            ):
                failed = await _command(probe, "采集", number=number)
            assert expected_text in failed, (name, failed)
            assert probe.rows(probe.context.paths.game_db, "SELECT user_stamina FROM user_xiuxian WHERE user_id=?", (ACTOR,)) == [(100,)]
            assert not probe.rows(probe.context.paths.player_db, "SELECT 1 FROM map_interactive_actions WHERE user_id=? AND status='active'", (ACTOR,))
            assert not probe.rows(probe.context.paths.game_db, "SELECT 1 FROM map_interactive_start_operations WHERE operation_id=?", (operation_id,))
            assert not probe.rows(probe.context.paths.game_db, "SELECT 1 FROM operation_ledger WHERE operation_id=? AND action='map.interactive_start'", (operation_id,))
            probe.seed(fault_database, f"DROP TRIGGER {trigger_name}")
            with (
                patch.object(module, "runtime_clock", SimpleNamespace(now=lambda: NOW)),
                patch.object(module, "runtime_random", SimpleNamespace(randint=lambda low, high: low, random=lambda: 0.0)),
                patch.object(module, "asyncio", SimpleNamespace(create_task=lambda coroutine: coroutine.close())),
            ):
                recovered = await _command(probe, "采集", number=number)
            assert "预计等待" in recovered, (name, recovered)
            assert probe.rows(probe.context.paths.game_db, "SELECT user_stamina FROM user_xiuxian WHERE user_id=?", (ACTOR,)) == [(97,)]
            assert probe.rows(probe.context.paths.player_db, "SELECT status FROM map_interactive_actions WHERE user_id=?", (ACTOR,)) == [("active",)]
            assert probe.rows(probe.context.paths.game_db, "SELECT result_status FROM map_interactive_start_operations WHERE operation_id=?", (operation_id,)) == [("applied",)]
            outcomes.append((name, failed, recovered))
            _integrity(probe)
        probe.evidence["faults"] = outcomes

    @case(
        "map-interactive-terminal-write-recovery-matrix", "采收", "失败终结冷却/回执写点拒绝与恢复",
        "有效失败活动终结时分别拒绝冷却、回执及其实际投影",
        "活动保持active且无冷却/回执；恢复后同消息提交失败状态和唯一回执",
        group="map-interactive", outcome="recovery",
    )
    async def terminal_write_recovery_matrix(probe):
        faults = (
            ("cooldown", "CREATE TRIGGER reject_interactive_terminal_cooldown BEFORE UPDATE OF gather_cd_until ON map_cooldown WHEN NEW.gather_cd_until IS NOT NULL BEGIN SELECT RAISE(IGNORE); END", "冷却未能保存"),
            ("receipt", "CREATE TRIGGER reject_interactive_terminal_receipt BEFORE INSERT ON map_interactive_terminal_operations BEGIN SELECT RAISE(IGNORE); END", "结束回执未能保存"),
            ("projection", "CREATE TRIGGER distort_interactive_terminal_cooldown AFTER UPDATE OF gather_cd_until ON map_cooldown WHEN NEW.gather_cd_until IS NOT NULL BEGIN UPDATE map_cooldown SET gather_cd_until='2026-11-22 15:00:00' WHERE user_id=NEW.user_id; END", "冷却与请求不一致"),
        )
        outcomes = []
        for index, (name, trigger, expected_text) in enumerate(faults):
            module, _, _, _ = _setup(probe)
            action_id = f"map-interactive-terminal-matrix-{name}"
            if index:
                probe.seed(probe.context.paths.player_db, "DELETE FROM map_interactive_actions WHERE user_id=?", (ACTOR,))
            _seed_ready_action(probe, module, action_id=action_id, success=False)
            probe.seed(
                probe.context.paths.player_db,
                "INSERT INTO map_cooldown(user_id,gather_cd_until) VALUES(?,NULL) "
                "ON CONFLICT(user_id) DO UPDATE SET gather_cd_until=NULL",
                (ACTOR,),
            )
            trigger_name = f"reject_interactive_terminal_{name}" if name != "projection" else "distort_interactive_terminal_cooldown"
            probe.seed(probe.context.paths.player_db, trigger)
            number = 975090 + index
            with patch.object(module, "runtime_clock", SimpleNamespace(now=lambda: NOW)):
                failed = await _command(probe, "采收", number=number)
            action = probe.rows(probe.context.paths.player_db, "SELECT status FROM map_interactive_actions WHERE user_id=?", (ACTOR,))
            cooldown = probe.rows(probe.context.paths.player_db, "SELECT gather_cd_until FROM map_cooldown WHERE user_id=?", (ACTOR,))
            receipt = probe.rows(probe.context.paths.player_db, "SELECT result_status FROM map_interactive_terminal_operations WHERE operation_id=?", (f"map-interactive-failed:{action_id}",))
            assert expected_text in failed, (name, failed)
            assert action == [("active",)] and cooldown == [(None,)] and not receipt, (name, action, cooldown, receipt)
            probe.seed(probe.context.paths.player_db, f"DROP TRIGGER {trigger_name}")
            with patch.object(module, "runtime_clock", SimpleNamespace(now=lambda: NOW)):
                recovered = await _command(probe, "采收", number=number)
            action = probe.rows(probe.context.paths.player_db, "SELECT status FROM map_interactive_actions WHERE user_id=?", (ACTOR,))
            cooldown = probe.rows(probe.context.paths.player_db, "SELECT gather_cd_until FROM map_cooldown WHERE user_id=?", (ACTOR,))
            receipt = probe.rows(probe.context.paths.player_db, "SELECT result_status FROM map_interactive_terminal_operations WHERE operation_id=?", (f"map-interactive-failed:{action_id}",))
            assert "动作慢了半拍" in recovered, (name, recovered)
            assert action == [("failed",)] and cooldown == [("2026-11-22 14:00:22",)] and receipt == [("applied",)], (name, action, cooldown, receipt)
            outcomes.append((name, failed, recovered))
            _integrity(probe)
        probe.evidence["faults"] = outcomes

    @case(
        "map-interactive-start-abort-cross-second-recovery", "采集", "SQLite异常后跨秒同消息恢复",
        "合法启动的永久回执写入抛出 SQLite ABORT，恢复时业务时钟及随机动作快照变化",
        "原操作显示定位号、业务投影完全回滚；相同消息重试后按新生成快照成功一次",
        group="map-interactive", outcome="recovery",
    )
    async def start_abort_cross_second_recovery(probe):
        from datetime import timedelta

        module, _, _, _ = _setup(probe, "采集", "灵林")
        number = 975100
        operation_id = f"map-interactive-start:{number}:{ACTOR}"
        probe.seed(
            probe.context.paths.game_db,
            "CREATE TRIGGER abort_interactive_start_receipt BEFORE INSERT ON map_interactive_start_operations "
            "BEGIN SELECT RAISE(ABORT,'isolated interactive receipt failure'); END",
        )
        with (
            patch.object(module, "runtime_clock", SimpleNamespace(now=lambda: NOW)),
            patch.object(module, "runtime_random", SimpleNamespace(randint=lambda low, high: low, random=lambda: 0.0)),
            patch.object(module, "asyncio", SimpleNamespace(create_task=lambda coroutine: coroutine.close())),
        ):
            failed = await _command(probe, "采集", number=number, allow_exception=True)
        assert "定位号" in failed, failed
        assert probe.rows(probe.context.paths.game_db, "SELECT user_stamina FROM user_xiuxian WHERE user_id=?", (ACTOR,)) == [(100,)]
        assert not probe.rows(probe.context.paths.player_db, "SELECT 1 FROM map_interactive_actions WHERE user_id=? AND status='active'", (ACTOR,))
        assert not probe.rows(probe.context.paths.game_db, "SELECT 1 FROM map_interactive_start_operations WHERE operation_id=?", (operation_id,))
        assert probe.rows(probe.context.paths.game_db, "SELECT status FROM operation_ledger WHERE operation_id=? AND action='map.interactive_start'", (operation_id,)) == [("failed",)]

        probe.seed(probe.context.paths.game_db, "DROP TRIGGER abort_interactive_start_receipt")
        later = NOW + timedelta(seconds=3)
        with (
            patch.object(module, "runtime_clock", SimpleNamespace(now=lambda: later)),
            patch.object(module, "runtime_random", SimpleNamespace(randint=lambda low, high: high, random=lambda: 0.9)),
            patch.object(module, "asyncio", SimpleNamespace(create_task=lambda coroutine: coroutine.close())),
        ):
            recovered = await _command(probe, "采集", number=number, allow_exception=True)
        assert "预计等待" in recovered, recovered
        assert probe.rows(probe.context.paths.game_db, "SELECT user_stamina FROM user_xiuxian WHERE user_id=?", (ACTOR,)) == [(97,)]
        receipt = probe.rows(
            probe.context.paths.game_db,
            "SELECT result_status,action_json FROM map_interactive_start_operations WHERE operation_id=?",
            (operation_id,),
        )
        assert len(receipt) == 1 and json.loads(receipt[0][1])["start_ts"] == later.strftime("%Y-%m-%d %H:%M:%S"), receipt
        assert probe.rows(probe.context.paths.game_db, "SELECT status FROM operation_ledger WHERE operation_id=? AND action='map.interactive_start'", (operation_id,)) == [("applied",)]
        probe.evidence.update(failed=failed, recovered=recovered, receipt=receipt)
        _integrity(probe)

    @case(
        "map-interactive-ready-notice-abort-reports", "采集", "异步到期通知终结异常",
        "真实启动后由可控业务钟推进到截止点，终态回执写入触发 SQLite ABORT",
        "玩家收到定位号；活动、冷却和终态回执保持原子回滚",
        group="map-interactive", outcome="recovery",
    )
    async def ready_notice_abort_reports(probe):
        from datetime import timedelta

        module, _, _, _ = _setup(probe, "采集", "灵林")
        probe.seed(
            probe.context.paths.player_db,
            "CREATE TRIGGER abort_interactive_timeout_receipt "
            "BEFORE INSERT ON map_interactive_terminal_operations "
            "BEGIN SELECT RAISE(ABORT,'isolated interactive timeout failure'); END",
        )

        class Clock:
            current = NOW

            def now(self):
                return self.current

            async def sleep(self, seconds):
                self.current += timedelta(seconds=seconds)

        clock = Clock()
        with (
            patch.object(module, "runtime_clock", clock),
            patch.object(module, "runtime_random", SimpleNamespace(randint=lambda low, high: low, random=lambda: 0.0)),
            patch.object(module, "asyncio", SimpleNamespace(create_task=asyncio.create_task, sleep=clock.sleep)),
        ):
            reply = await _command(probe, "采集", number=975101, allow_exception=True)

        assert "预计等待" in reply and "请在" in reply, reply
        assert "定位号" in reply, reply
        action = probe.rows(
            probe.context.paths.player_db,
            "SELECT status FROM map_interactive_actions WHERE user_id=?", (ACTOR,),
        )
        cooldown = probe.rows(
            probe.context.paths.player_db,
            "SELECT gather_cd_until FROM map_cooldown WHERE user_id=?", (ACTOR,),
        )
        receipts = probe.rows(
            probe.context.paths.player_db,
            "SELECT 1 FROM map_interactive_terminal_operations",
        )
        assert action == [("active",)] and cooldown == [(None,)] and not receipts, (
            action, cooldown, receipts,
        )
        start_receipt = probe.rows(
            probe.context.paths.game_db,
            "SELECT action_json FROM map_interactive_start_operations WHERE operation_id=?",
            (f"map-interactive-start:975101:{ACTOR}",),
        )
        expiry = datetime.strptime(json.loads(start_receipt[0][0])["expire_ts"], "%Y-%m-%d %H:%M:%S")
        assert clock.current > expiry, (clock.current, expiry)
        probe.evidence.update(reply=reply, clock=clock.current.isoformat(), action=action,
                              expiry=expiry.isoformat(), cooldown=cooldown,
                              terminal_receipts=receipts)
        _integrity(probe)
