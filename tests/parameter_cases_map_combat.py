"""Real matcher coverage for map node combat lifecycle."""

from __future__ import annotations


def _setup(probe, actor="900001"):
    from nonebot_plugin_xiuxian_2.features.combat_settlement.migrations import (
        apply_combat_settlement_operations,
    )
    from nonebot_plugin_xiuxian_2.features.map.migrations import (
        apply_map_combat_plan,
        apply_map_combat_player,
        apply_map_combat_start,
        apply_map_player_state_schema,
    )
    from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
    from nonebot_plugin_xiuxian_2.plugin import apply_platform_schema
    from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_map as module

    probe.clear_cooldowns()
    game, player = probe.context.paths.game_db, probe.context.paths.player_db
    with DatabaseUnitOfWork(game) as uow:
        apply_platform_schema(uow)
        apply_map_combat_start(uow)
        apply_combat_settlement_operations(uow)
    with DatabaseUnitOfWork(player) as uow:
        apply_map_player_state_schema(uow)
        apply_map_combat_player(uow)
        apply_map_combat_plan(uow)

    data = module._load_map_data()
    realm, heaven, node = next(
        (realm, heaven, node)
        for realm in module._all_realms(data)
        for heaven in module._heaven_names(data, realm)
        for node in module._nodes(data, realm, heaven)
        if node["type"] in module.COMBAT_NODE_TYPES
    )
    module.map_application.save_status(actor, realm, heaven, node["id"], [node["id"]])
    module.map_application.save_daily_limit(actor, {
        "date": module._today_str(), "gather_count": 0, "combat_count": 0,
        "explore_count": 0, "resource_total_count": 0,
    })
    module.map_application.set_cooldown(actor, "combat_cd_until", None)
    return module, realm, heaven, node


def _projection(probe, actor="900001", operation_id="map-combat-start:974901:900001"):
    game, player = probe.context.paths.game_db, probe.context.paths.player_db
    return {
        "profile": probe.rows(
            game,
            "SELECT user_stamina,stone FROM user_xiuxian WHERE user_id=?",
            (actor,),
        ),
        "position": probe.rows(
            player,
            "SELECT realm,heaven,node_id FROM map_status WHERE user_id=?",
            (actor,),
        ),
        "daily": probe.rows(
            player,
            "SELECT date,combat_count,resource_total_count FROM map_daily_limit WHERE user_id=?",
            (actor,),
        ),
        "cooldown": probe.rows(
            player,
            "SELECT combat_cd_until FROM map_cooldown WHERE user_id=?",
            (actor,),
        ),
        "snapshot": probe.rows(
            player,
            "SELECT snapshot FROM map_combat_settlement WHERE user_id=?",
            (actor,),
        ),
        "start_receipt": probe.rows(
            game,
            "SELECT result_status,stamina,task_json FROM map_combat_start_operations WHERE operation_id=?",
            (operation_id,),
        ),
        "plan_receipt": probe.rows(
            player,
            "SELECT task_id,snapshot FROM map_combat_plan_operations WHERE operation_id=?",
            ("map-combat-plan:" + operation_id,),
        ),
        "settlement_receipt": probe.rows(
            game,
            "SELECT stone,rewards FROM map_combat_settlement_operations WHERE operation_id=?",
            ("map-combat-settle:" + operation_id,),
        ),
    }


async def _invoke(probe, module, command, number, *, actor="900001", fixed=None,
                  reward_items=(), stone=7, victor="群友赢了", allow_exception=False):
    import random
    from datetime import datetime
    from types import SimpleNamespace
    from unittest.mock import AsyncMock, patch

    fixed = fixed or datetime(2026, 11, 22, 21, 0, 0)
    battle = AsyncMock(
        return_value=([{"data": {"content": "隔离战斗结果", "name": "系统", "uin": 900000}}],
                      victor, {"群友": {"剩余气血": 1}})
    )
    reward_text = ["灵石x7", *(f"{item['name']}x{item['amount']}" for item in reward_items)]
    probe.clear_cooldowns()
    with (
        patch.object(module, "runtime_clock", SimpleNamespace(now=lambda: fixed)),
        patch.object(module, "runtime_random", random.Random(174)),
        patch.object(module.map_application, "combat_battle", battle),
        patch.object(module.map_reward_resolver, "roll_rewards", return_value=(reward_text, stone, list(reward_items))),
        patch.object(module.map_reward_resolver, "roll_skill_equip_drop", return_value=("", None)),
        patch.object(module.map_reward_resolver, "roll_dongfu_material", return_value=([], 0, [])),
    ):
        reply = await probe.command(
            command, command, actor=actor, number=number, allow_exception=allow_exception
        )
    return reply, battle


def _combat_item(module, amount=2):
    definition = module.Items().get_data_by_item_id(21001)
    assert definition, "diagnostic combat item 21001 is missing"
    return {"id": 21001, "name": definition["name"], "type": definition["type"], "amount": amount}


def _seed_planned(probe, module, number, *, actor="900001", item=None, stone=7,
                  fixed=None):
    import json
    from datetime import datetime

    fixed = fixed or datetime(2026, 11, 22, 21, 0, 0)
    date = fixed.date().isoformat()
    daily = {
        "date": date, "gather_count": 0, "combat_count": 0,
        "explore_count": 0, "resource_total_count": 0,
    }
    module.map_application.save_daily_limit(actor, daily)
    module.map_application.set_cooldown(actor, "combat_cd_until", None)
    probe.player(actor, user_stamina=100, stone=1000000, hp=1000, exp=100000)
    item = item or _combat_item(module)
    task_id = f"map-combat-start:pending-{number}:{actor}"
    plan = {
        "task_id": task_id, "status": "planned",
        "started_at": fixed.strftime("%Y-%m-%d %H:%M:%S"),
        "cooldown_until": fixed.strftime("%Y-%m-%d %H:%M:%S"),
        "daily": daily, "decay": 1.0,
        "enemy": {"name": "守关石傀", "气血": 1000, "攻击": 200},
        "node_name": "飞鹤试武台", "node_type": "试炼",
        "items": [item], "rewards": [f"{item['name']}x{item['amount']}", f"灵石x{stone}"],
        "stone": stone, "title": "战而胜之", "won": True,
    }
    snapshot = json.dumps(plan, ensure_ascii=False, sort_keys=True)
    probe.seed(
        probe.context.paths.player_db,
        "INSERT INTO map_combat_settlement(user_id,snapshot) VALUES(?,?) "
        "ON CONFLICT(user_id) DO UPDATE SET snapshot=excluded.snapshot",
        (actor, snapshot),
    )
    return task_id, snapshot, daily, item


def _install_fault(probe, database, name, table, event, condition, mode, rewrite=""):
    if mode == "ignore":
        action = "SELECT RAISE(IGNORE);"
        timing = "BEFORE"
    elif mode == "abort":
        action = f"SELECT RAISE(ABORT, 'forced map combat {name}');"
        timing = "BEFORE"
    else:
        action = rewrite
        timing = "AFTER"
    probe.seed(
        database,
        f"CREATE TRIGGER {name} {timing} {event} ON {table} "
        f"WHEN {condition} BEGIN {action} END",
    )


def register(case):
    @case(
        "map-combat-start-receipt-write-rejected",
        "节点战斗",
        "合法试炼/险地节点上的无参战斗命令",
        "真实 OneBot matcher、隔离 game/player SQLite、确定性战斗结果",
        "启动回执拒写必须拒绝且体力/冷却/快照/奖励全部不变",
        group="map-combat",
        outcome="recovery",
    )
    async def start_receipt_rejected(probe):
        import random
        import sqlite3
        from datetime import datetime
        from types import SimpleNamespace
        from unittest.mock import AsyncMock, patch

        module, realm, heaven, node = _setup(probe)
        actor = "900001"
        operation_id = "map-combat-start:974901:900001"
        fixed = datetime(2026, 11, 22, 21, 0, 0)
        module.map_application.save_daily_limit(actor, {
            "date": fixed.date().isoformat(), "gather_count": 0, "combat_count": 0,
            "explore_count": 0, "resource_total_count": 0,
        })
        probe.player(user_stamina=100, stone=1000000, hp=1000, exp=100000)
        database = probe.context.paths.game_db
        with sqlite3.connect(database) as connection:
            connection.execute(
                "CREATE TRIGGER ignore_map_combat_start_receipt "
                "BEFORE INSERT ON map_combat_start_operations "
                "BEGIN SELECT RAISE(IGNORE); END"
            )

        before = _projection(probe, operation_id=operation_id)
        battle = AsyncMock(
            return_value=([{"data": {"content": "隔离战斗结果", "name": "系统", "uin": 900000}}],
                          "群友赢了", {"群友": {"剩余气血": 1}})
        )
        with (
            patch.object(module, "runtime_clock", SimpleNamespace(now=lambda: fixed)),
            patch.object(module, "runtime_random", random.Random(174)),
            patch.object(module.map_application, "combat_battle", battle),
            patch.object(module.map_reward_resolver, "roll_rewards", return_value=(["灵石x7"], 7, [])),
            patch.object(module.map_reward_resolver, "roll_skill_equip_drop", return_value=("", None)),
            patch.object(module.map_reward_resolver, "roll_dongfu_material", return_value=([], 0, [])),
        ):
            reply = await probe.command("节点战斗", "节点战斗", number=974901)

        after = _projection(probe, operation_id=operation_id)
        with sqlite3.connect(database) as connection:
            connection.execute("DROP TRIGGER ignore_map_combat_start_receipt")
        probe.evidence["receipt_ignore"] = {
            "node": {"realm": realm, "heaven": heaven, "id": node["id"], "type": node["type"]},
            "reply": reply,
            "before": before,
            "after": after,
            "battle_calls": battle.await_count,
        }
        assert "节点战斗启动回执未能保存" in reply, reply
        assert after["profile"] == before["profile"], (before, after)
        assert after["daily"] == before["daily"], (before, after)
        assert after["cooldown"] == before["cooldown"], (before, after)
        assert after["snapshot"] == before["snapshot"], (before, after)
        assert after["start_receipt"] == before["start_receipt"], (before, after)
        assert after["plan_receipt"] == before["plan_receipt"], (before, after)
        assert after["settlement_receipt"] == before["settlement_receipt"], (before, after)
        assert battle.await_count == 0, probe.evidence["receipt_ignore"]
        from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork

        for database_path in (probe.context.paths.game_db, probe.context.paths.player_db):
            with DatabaseUnitOfWork(database_path, read_only=True) as uow:
                assert uow.query_one("PRAGMA integrity_check")["integrity_check"] == "ok"

    @case(
        "map-combat-alias-replay-and-player-isolation",
        "节点战斗",
        "主命令及试炼/险地别名，无参数",
        "两个注册玩家位于不同战斗节点，奖励固定、时钟固定",
        "两类节点各结算一次；同消息重放无重复消费且玩家数据隔离",
        group="map-combat",
        outcome="success",
    )
    async def aliases_replay_and_isolation(probe):
        import json
        from datetime import datetime

        module, realm, heaven, trial_node = _setup(probe)
        actor_a, actor_b = "900001", "900002"
        fixed = datetime(2026, 11, 22, 21, 0, 0)
        item = _combat_item(module)
        probe.player(actor_a, user_stamina=100, stone=1000000, hp=1000, exp=100000)
        risk_realm, risk_heaven, risk_node = next(
            (r, h, n)
            for r in module._all_realms(module._load_map_data())
            for h in module._heaven_names(module._load_map_data(), r)
            for n in module._nodes(module._load_map_data(), r, h)
            if n["type"] in module.COMBAT_NODE_TYPES and n["type"] != trial_node["type"]
        )
        module.map_application.save_status(actor_b, risk_realm, risk_heaven, risk_node["id"], [risk_node["id"]])
        module.map_application.save_daily_limit(actor_b, {
            "date": fixed.date().isoformat(), "gather_count": 0, "combat_count": 0,
            "explore_count": 0, "resource_total_count": 0,
        })
        module.map_application.set_cooldown(actor_b, "combat_cd_until", None)
        probe.player(actor_b, user_stamina=100, stone=1000000, hp=1000, exp=100000)

        op_a = "map-combat-start:974911:900001"
        first_reply, first_battle = await _invoke(
            probe, module, "试炼挑战", 974911, actor=actor_a, fixed=fixed,
            reward_items=(item,),
        )
        after_a = _projection(probe, actor=actor_a, operation_id=op_a)
        assert "战利品" in first_reply and trial_node["name"] in first_reply, first_reply
        trial_cost = module.COMBAT_CONFIG[trial_node["type"]]["stamina_cost"]
        assert after_a["profile"] == [(100 - trial_cost, 1000007)], after_a
        assert after_a["daily"] == [(fixed.date().isoformat(), 1, 1)], after_a
        assert after_a["start_receipt"][0][0] == "applied", after_a
        assert json.loads(after_a["plan_receipt"][0][1])["node_type"] == trial_node["type"], after_a
        assert after_a["settlement_receipt"] == [(7, "[[21001, 2]]")], after_a
        assert probe.rows(
            probe.context.paths.game_db,
            "SELECT goods_num,bind_num FROM back WHERE user_id=? AND goods_id=21001",
            (actor_a,),
        ) == [(2, 2)]

        replay, replay_battle = await _invoke(
            probe, module, "试炼挑战", 974911, actor=actor_a, fixed=fixed,
            reward_items=(item,),
        )
        assert "已经处理" in replay, replay
        assert replay_battle.await_count == 0 and first_battle.await_count == 1
        assert _projection(probe, actor=actor_a, operation_id=op_a) == after_a

        before_a_again = _projection(probe, actor=actor_a, operation_id=op_a)
        op_b = "map-combat-start:974912:900002"
        risk_reply, risk_battle = await _invoke(
            probe, module, "险地挑战", 974912, actor=actor_b, fixed=fixed,
            reward_items=(item,),
        )
        after_b = _projection(probe, actor=actor_b, operation_id=op_b)
        assert "战利品" in risk_reply and risk_node["name"] in risk_reply, risk_reply
        risk_cost = module.COMBAT_CONFIG[risk_node["type"]]["stamina_cost"]
        assert after_b["profile"] == [(100 - risk_cost, 1000007)], after_b
        assert after_b["daily"] == [(fixed.date().isoformat(), 1, 1)], after_b
        assert json.loads(after_b["plan_receipt"][0][1])["node_type"] == risk_node["type"], after_b
        assert risk_battle.await_count == 1
        assert _projection(probe, actor=actor_a, operation_id=op_a) == before_a_again

        actor_c = "900003"
        module.map_application.save_status(
            actor_c, realm, heaven, trial_node["id"], [trial_node["id"]]
        )
        module.map_application.save_daily_limit(actor_c, {
            "date": fixed.date().isoformat(), "gather_count": 0, "combat_count": 0,
            "explore_count": 0, "resource_total_count": 0,
        })
        module.map_application.set_cooldown(actor_c, "combat_cd_until", None)
        probe.player(actor_c, user_stamina=100, stone=1000000, hp=1000, exp=100000)
        op_c = "map-combat-start:974913:900003"
        loss_reply, loss_battle = await _invoke(
            probe, module, "节点战斗", 974913, actor=actor_c, fixed=fixed,
            victor="敌方胜利",
        )
        after_c = _projection(probe, actor=actor_c, operation_id=op_c)
        assert "试炼失利" in loss_reply and "战利品" not in loss_reply, loss_reply
        assert loss_battle.await_count == 1
        assert after_c["profile"] == [(100 - trial_cost, 1000000)], after_c
        assert after_c["daily"] == [(fixed.date().isoformat(), 1, 1)], after_c
        assert after_c["settlement_receipt"] == [(0, "[]")], after_c
        assert probe.rows(
            probe.context.paths.game_db,
            "SELECT 1 FROM back WHERE user_id=? AND goods_id=21001", (actor_c,),
        ) == []
        probe.evidence.update(
            trial={"node": [realm, heaven, trial_node["id"]], "reply": first_reply, "after": after_a},
            risk={"node": [risk_realm, risk_heaven, risk_node["id"]], "reply": risk_reply, "after": after_b},
            loss={"reply": loss_reply, "after": after_c},
            duplicate_no_write=True,
        )

    @case(
        "map-combat-registration-and-start-preconditions",
        "节点战斗",
        "无参数命令在不同注册/节点/体力/次数/冷却状态下执行",
        "真实 matcher、隔离 game/player SQLite、固定业务时钟",
        "正常拒绝不消耗体力/启动战斗，保留具体状态回执",
        group="map-combat",
        outcome="rejection",
    )
    async def registration_and_preconditions(probe):
        from datetime import datetime, timedelta

        module, realm, heaven, trial_node = _setup(probe)
        actor = "900001"
        fixed = datetime(2026, 11, 22, 21, 0, 0)
        probe.player(actor, user_stamina=100, stone=1000000, hp=1000, exp=100000)

        missing, battle = await _invoke(
            probe, module, "节点战斗", 974930, actor="900099", fixed=fixed,
        )
        assert "没有道友的信息" in missing and "我要修仙" in missing, missing
        assert probe.rows(
            probe.context.paths.game_db,
            "SELECT 1 FROM map_combat_start_operations WHERE operation_id='map-combat-start:974930:900099'",
        ) == []
        assert battle.await_count == 0

        map_data = module._load_map_data()
        other_realm, other_heaven, other_node = next(
            (r, h, node)
            for r in module._all_realms(map_data)
            for h in module._heaven_names(map_data, r)
            for node in module._nodes(map_data, r, h)
            if node["type"] not in module.COMBAT_NODE_TYPES
        )
        module.map_application.save_status(
            actor, other_realm, other_heaven, other_node["id"], [other_node["id"]]
        )
        noncombat, battle = await _invoke(
            probe, module, "节点战斗", 974931, actor=actor, fixed=fixed,
        )
        assert "不是对战节点" in noncombat, noncombat
        assert battle.await_count == 0
        assert probe.rows(
            probe.context.paths.game_db,
            "SELECT 1 FROM map_combat_start_operations WHERE operation_id='map-combat-start:974931:900001'",
        ) == []

        module.map_application.save_status(actor, realm, heaven, trial_node["id"], [trial_node["id"]])
        module.map_application.save_daily_limit(actor, {
            "date": fixed.date().isoformat(), "gather_count": 0, "combat_count": 0,
            "explore_count": 0, "resource_total_count": 0,
        })
        module.map_application.set_cooldown(actor, "combat_cd_until", None)
        cost = module.COMBAT_CONFIG[trial_node["type"]]["stamina_cost"]
        probe.player(actor, user_stamina=cost - 1)
        low_stamina, battle = await _invoke(
            probe, module, "节点战斗", 974932, actor=actor, fixed=fixed,
        )
        assert "体力不足" in low_stamina, low_stamina
        assert battle.await_count == 0
        assert probe.rows(
            probe.context.paths.game_db,
            "SELECT result_status FROM map_combat_start_operations WHERE operation_id='map-combat-start:974932:900001'",
        ) == [("stamina_insufficient",)]
        assert probe.rows(
            probe.context.paths.game_db,
            "SELECT user_stamina FROM user_xiuxian WHERE user_id=?", (actor,),
        ) == [(cost - 1,)]

        probe.player(actor, user_stamina=100)
        module.map_application.save_daily_limit(actor, {
            "date": fixed.date().isoformat(), "gather_count": 0,
            "combat_count": module.DAILY_LIMIT_CONFIG["combat"],
            "explore_count": 0, "resource_total_count": 0,
        })
        capped, battle = await _invoke(
            probe, module, "节点战斗", 974933, actor=actor, fixed=fixed,
        )
        assert "次数已达上限" in capped, capped
        assert battle.await_count == 0
        assert probe.rows(
            probe.context.paths.game_db,
            "SELECT result_status FROM map_combat_start_operations WHERE operation_id='map-combat-start:974933:900001'",
        ) == [("limit_reached",)]

        module.map_application.save_daily_limit(actor, {
            "date": fixed.date().isoformat(), "gather_count": 0, "combat_count": 0,
            "explore_count": 0, "resource_total_count": 0,
        })
        cooldown_until = (fixed + timedelta(seconds=30)).strftime("%Y-%m-%d %H:%M:%S")
        module.map_application.set_cooldown(actor, "combat_cd_until", cooldown_until)
        cooldown, battle = await _invoke(
            probe, module, "节点战斗", 974934, actor=actor, fixed=fixed,
        )
        assert "尚需冷却" in cooldown, cooldown
        assert battle.await_count == 0
        assert probe.rows(
            probe.context.paths.game_db,
            "SELECT result_status FROM map_combat_start_operations WHERE operation_id='map-combat-start:974934:900001'",
        ) == [("cooldown",)]
        assert probe.rows(
            probe.context.paths.game_db,
            "SELECT user_stamina FROM user_xiuxian WHERE user_id=?", (actor,),
        ) == [(100,)]
        module.map_application.set_cooldown(
            actor, "combat_cd_until", fixed.strftime("%Y-%m-%d %H:%M:%S")
        )
        at_boundary, boundary_battle = await _invoke(
            probe, module, "节点战斗", 974947, actor=actor, fixed=fixed,
        )
        assert "战利品" in at_boundary, at_boundary
        assert boundary_battle.await_count == 1
        probe.evidence["preconditions"] = {
            "unregistered": missing,
            "noncombat_node": {"node": other_node["name"], "reply": noncombat},
            "stamina": low_stamina,
            "daily_cap": capped,
            "cooldown_until": cooldown_until,
            "cooldown": cooldown,
            "cooldown_boundary": at_boundary,
        }

    @case(
        "map-combat-start-write-recovery",
        "节点战斗",
        "无参数战斗在体力、冷却、running 快照和启动回执写入故障后重试",
        "真实 matcher、隔离双库、四个启动写点和可控跨秒业务时钟",
        "IGNORE/改写/ABORT 全回滚；同消息跨时间重试只扣一次体力并完成结算",
        group="map-combat",
        outcome="recovery",
    )
    async def start_write_recovery(probe):
        import json
        from datetime import datetime, timedelta
        from nonebot_plugin_xiuxian_2.core.player_reasons import reason_text

        module, realm, heaven, node = _setup(probe)
        actor = "900001"
        game, player = probe.context.paths.game_db, probe.context.paths.player_db
        fixed = datetime(2026, 11, 22, 21, 0, 0)
        targets = (
            ("stamina", "game", "user_xiuxian", "UPDATE OF user_stamina",
             "NEW.user_id='900001'", "UPDATE user_xiuxian SET user_stamina=NEW.user_stamina+1 WHERE user_id=NEW.user_id;",
             "map_combat_start_stamina"),
            ("cooldown", "player", "map_cooldown", "UPDATE OF combat_cd_until",
             "NEW.user_id='900001'", "UPDATE map_cooldown SET combat_cd_until='tampered' WHERE user_id=NEW.user_id;",
             "map_combat_start_cooldown"),
            ("snapshot", "player", "map_combat_settlement", "INSERT",
             "NEW.user_id='900001'", "UPDATE map_combat_settlement SET snapshot='tampered' WHERE user_id=NEW.user_id;",
             "map_combat_start_snapshot"),
            ("receipt", "game", "map_combat_start_operations", "INSERT",
             "NEW.operation_id LIKE 'map-combat-start:%'", "UPDATE map_combat_start_operations SET task_json='tampered' WHERE operation_id=NEW.operation_id;",
             "map_combat_start_receipt"),
        )
        index = 0
        for point, db_name, table, event, condition, rewrite, reason_prefix in targets:
            for mode in ("ignore", "rewrite", "abort"):
                number = 974935 + index
                index += 1
                op_id = f"map-combat-start:{number}:{actor}"
                probe.player(actor, user_stamina=100, stone=1000000, hp=1000, exp=100000)
                module.map_application.save_status(actor, realm, heaven, node["id"], [node["id"]])
                module.map_application.save_daily_limit(actor, {
                    "date": fixed.date().isoformat(), "gather_count": 0, "combat_count": 0,
                    "explore_count": 0, "resource_total_count": 0,
                })
                module.map_application.set_cooldown(actor, "combat_cd_until", None)
                probe.seed(player, "DELETE FROM map_combat_settlement WHERE user_id=?", (actor,))
                database = game if db_name == "game" else player
                trigger = f"fault_start_{point}_{mode}"
                _install_fault(probe, database, trigger, table, event, condition, mode, rewrite)

                first_reply, first_battle = await _invoke(
                    probe, module, "节点战斗", number, actor=actor, fixed=fixed,
                    allow_exception=(mode == "abort"),
                )
                assert probe.rows(game, "SELECT user_stamina FROM user_xiuxian WHERE user_id=?", (actor,)) == [(100,)]
                assert probe.rows(player, "SELECT snapshot FROM map_combat_settlement WHERE user_id=?", (actor,)) == []
                assert probe.rows(player, "SELECT combat_cd_until FROM map_cooldown WHERE user_id=?", (actor,)) == [(None,)]
                assert probe.rows(game, "SELECT 1 FROM map_combat_start_operations WHERE operation_id=?", (op_id,)) == []
                assert first_battle.await_count == 0
                if mode == "abort":
                    assert "forced map combat" in json.dumps(probe.calls[-1]["errors"], ensure_ascii=False)
                    assert probe.rows(
                        game,
                        "SELECT status FROM operation_ledger WHERE operation_id=? AND action='map.combat_start'",
                        (op_id,),
                    ) == [("failed",)]
                else:
                    expected = f"{reason_prefix}_{'write_rejected' if mode == 'ignore' else 'projection_mismatch'}"
                    assert reason_text(expected) in first_reply, (point, mode, first_reply)

                probe.seed(database, f"DROP TRIGGER {trigger}")
                retry_reply, retry_battle = await _invoke(
                    probe, module, "节点战斗", number, actor=actor,
                    fixed=fixed + timedelta(seconds=5),
                )
                assert "战利品" in retry_reply, (point, mode, retry_reply)
                final = _projection(probe, actor=actor, operation_id=op_id)
                assert final["profile"] == [(92, 1000007)], (point, mode, final)
                assert final["daily"] == [(fixed.date().isoformat(), 1, 1)], (point, mode, final)
                assert final["snapshot"] == [("",)], (point, mode, final)
                assert final["start_receipt"] and final["plan_receipt"] and final["settlement_receipt"], final
                assert retry_battle.await_count == 1
                probe.evidence[f"{point}_{mode}"] = {
                    "failure_reply": first_reply,
                    "retry_reply": retry_reply,
                    "final": final,
                }

    @case(
        "map-combat-plan-write-recovery",
        "节点战斗",
        "无参数战斗，结果快照或回执写入故障后重试",
        "真实 matcher、隔离 SQLite、时钟与战斗随机源固定",
        "计划写拒绝/改写回滚为 running；故障解除后同消息完成且不重扣体力",
        group="map-combat",
        outcome="recovery",
    )
    async def plan_write_recovery(probe):
        import json
        from datetime import datetime
        from nonebot_plugin_xiuxian_2.core.player_reasons import reason_text

        module, *_ = _setup(probe)
        actor, fixed = "900001", datetime(2026, 11, 22, 21, 0, 0)
        cases = (
            ("plan_snapshot_ignore", "player", "map_combat_settlement", "UPDATE OF snapshot",
             "NEW.user_id='900001'", "ignore", "",
             "map_combat_plan_snapshot_write_rejected"),
            ("plan_snapshot_rewrite", "player", "map_combat_settlement", "UPDATE OF snapshot",
             "NEW.user_id='900001'", "rewrite",
             "UPDATE map_combat_settlement SET snapshot='tampered' WHERE user_id=NEW.user_id;",
             "map_combat_plan_snapshot_projection_mismatch"),
            ("plan_snapshot_abort", "player", "map_combat_settlement", "UPDATE OF snapshot",
             "NEW.user_id='900001'", "abort", "", ""),
            ("plan_receipt_ignore", "player", "map_combat_plan_operations", "INSERT",
             "NEW.user_id='900001'", "ignore", "",
             "map_combat_plan_receipt_write_rejected"),
            ("plan_receipt_rewrite", "player", "map_combat_plan_operations", "INSERT",
             "NEW.user_id='900001'", "rewrite",
             "UPDATE map_combat_plan_operations SET snapshot='tampered' WHERE operation_id=NEW.operation_id;",
             "map_combat_plan_receipt_projection_mismatch"),
            ("plan_receipt_abort", "player", "map_combat_plan_operations", "INSERT",
             "NEW.user_id='900001'", "abort", "", ""),
        )
        for index, (label, db_name, table, event, condition, mode, rewrite, reason) in enumerate(cases):
            number = 974920 + index
            game, player = probe.context.paths.game_db, probe.context.paths.player_db
            probe.player(actor, user_stamina=100, stone=1000000, hp=1000, exp=100000)
            module.map_application.save_daily_limit(actor, {
                "date": fixed.date().isoformat(), "gather_count": 0, "combat_count": 0,
                "explore_count": 0, "resource_total_count": 0,
            })
            module.map_application.set_cooldown(actor, "combat_cd_until", None)
            probe.seed(player, "DELETE FROM map_combat_settlement WHERE user_id=?", (actor,))
            probe.seed(game, "DELETE FROM back WHERE user_id=? AND goods_id=?", (actor, 21001))
            database = game if db_name == "game" else player
            trigger = f"fault_{label}"
            _install_fault(probe, database, trigger, table, event, condition, mode, rewrite)
            first_reply, first_battle = await _invoke(
                probe, module, "节点战斗", number, actor=actor, fixed=fixed,
                allow_exception=(mode == "abort"),
            )
            pending = probe.rows(player, "SELECT snapshot FROM map_combat_settlement WHERE user_id=?", (actor,))
            daily = probe.rows(player, "SELECT combat_count,resource_total_count FROM map_daily_limit WHERE user_id=?", (actor,))
            stamina = probe.rows(game, "SELECT user_stamina FROM user_xiuxian WHERE user_id=?", (actor,))
            assert stamina == [(92,)] and daily == [(0, 0)], (label, stamina, daily)
            assert pending and json.loads(pending[0][0])["status"] == "running", (label, pending)
            assert probe.rows(player, "SELECT COUNT(*) FROM map_combat_plan_operations WHERE operation_id=?", (f"map-combat-plan:map-combat-start:{number}:{actor}",)) == [(0,)], label
            if mode == "abort":
                latest = probe.calls[-1]
                assert "forced map combat" in json.dumps(latest["errors"], ensure_ascii=False), latest
                assert first_reply, label
            else:
                assert reason_text(reason) in first_reply, (label, first_reply)
            probe.seed(database, f"DROP TRIGGER {trigger}")
            retry_reply = ""
            retry_battle = None
            if mode == "abort":
                second_trigger = "fault_plan_receipt_after_abort"
                _install_fault(
                    probe, player, second_trigger,
                    "map_combat_plan_operations", "INSERT",
                    "NEW.user_id='900001'", "ignore",
                )
                intermediate_reply, _ = await _invoke(
                    probe, module, "节点战斗", number, actor=actor, fixed=fixed,
                )
                assert "节点战斗结果回执未能保存" in intermediate_reply, intermediate_reply
                assert probe.rows(
                    game,
                    "SELECT 1 FROM operation_ledger WHERE operation_id=? AND action='map.combat_save_plan'",
                    (f"map-combat-plan:map-combat-start:{number}:{actor}",),
                ) == [], intermediate_reply
                probe.seed(player, f"DROP TRIGGER {second_trigger}")
            retry_reply, retry_battle = await _invoke(
                probe, module, "节点战斗", number, actor=actor, fixed=fixed,
            )
            final = _projection(
                probe, actor=actor, operation_id=f"map-combat-start:{number}:{actor}"
            )
            assert "战利品" in retry_reply, (label, retry_reply)
            assert final["profile"] == [(92, 1000007)], (label, final)
            assert final["daily"] == [(fixed.date().isoformat(), 1, 1)], (label, final)
            assert final["snapshot"] == [("",)], (label, final)
            assert len(final["start_receipt"]) == len(final["plan_receipt"]) == len(final["settlement_receipt"]) == 1, (label, final)
            assert first_battle.await_count == 1
            assert retry_battle is not None and retry_battle.await_count == 1
            probe.evidence[label] = {"failure_reply": first_reply, "retry_reply": retry_reply, "final": final}

    @case(
        "map-combat-settlement-write-recovery",
        "节点战斗",
        "无参数命令结算已持久化的战斗计划",
        "隔离 game/player SQLite、五个结算写点、固定奖励和可控时钟",
        "拒写/改写/ABORT 完整回滚；解除故障后同消息结算一次",
        group="map-combat",
        outcome="recovery",
    )
    async def settlement_write_recovery(probe):
        import json
        from datetime import datetime
        from nonebot_plugin_xiuxian_2.core.player_reasons import reason_text
        from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork

        module, *_ = _setup(probe)
        actor, fixed = "900001", datetime(2026, 11, 22, 21, 0, 0)
        game, player = probe.context.paths.game_db, probe.context.paths.player_db
        item = _combat_item(module)
        targets = (
            ("daily", player, "map_daily_limit", "UPDATE OF combat_count,resource_total_count",
             "NEW.user_id='900001'", "UPDATE map_daily_limit SET combat_count=NEW.combat_count+1 WHERE user_id=NEW.user_id;", "daily_limit"),
            ("snapshot", player, "map_combat_settlement", "UPDATE OF snapshot",
             "NEW.user_id='900001'", "UPDATE map_combat_settlement SET snapshot='tampered' WHERE user_id=NEW.user_id;", "snapshot"),
            ("stone", game, "user_xiuxian", "UPDATE OF stone",
             "NEW.user_id='900001'", "UPDATE user_xiuxian SET stone=NEW.stone+1 WHERE user_id=NEW.user_id;", "stone"),
            ("inventory", game, "back", "INSERT",
             "NEW.user_id='900001' AND NEW.goods_id=21001", "UPDATE back SET goods_num=NEW.goods_num+1 WHERE user_id=NEW.user_id AND goods_id=NEW.goods_id;", "inventory"),
            ("receipt", game, "map_combat_settlement_operations", "INSERT",
             "NEW.operation_id LIKE 'map-combat-settle:%'", "UPDATE map_combat_settlement_operations SET rewards='[]' WHERE operation_id=NEW.operation_id;", "receipt"),
        )
        scenarios = []
        for point, database, table, event, condition, rewrite, subject in targets:
            for mode in ("ignore", "rewrite", "abort"):
                scenarios.append((point, database, table, event, condition, rewrite, subject, mode))

        for index, (point, database, table, event, condition, rewrite, subject, mode) in enumerate(scenarios):
            number = 974950 + index
            probe.player(actor, user_stamina=100, stone=1000000, hp=1000, exp=100000)
            probe.seed(game, "DELETE FROM back WHERE user_id=? AND goods_id=?", (actor, 21001))
            task_id, snapshot, daily, _ = _seed_planned(
                probe, module, number, actor=actor, item=item, fixed=fixed,
            )
            op_start = task_id
            op_settle = f"map-combat-settle:{task_id}"
            before = {
                "profile": probe.rows(game, "SELECT user_stamina,stone FROM user_xiuxian WHERE user_id=?", (actor,)),
                "daily": probe.rows(player, "SELECT date,combat_count,resource_total_count FROM map_daily_limit WHERE user_id=?", (actor,)),
                "snapshot": probe.rows(player, "SELECT snapshot FROM map_combat_settlement WHERE user_id=?", (actor,)),
                "inventory": probe.rows(game, "SELECT goods_num,bind_num FROM back WHERE user_id=? AND goods_id=21001", (actor,)),
                "receipt": probe.rows(game, "SELECT payload,stone,rewards FROM map_combat_settlement_operations WHERE operation_id=?", (op_settle,)),
            }
            trigger = f"fault_combat_settle_{index}"
            _install_fault(probe, database, trigger, table, event, condition, mode, rewrite)
            reply, _ = await _invoke(
                probe, module, "节点战斗", number, actor=actor, fixed=fixed,
                allow_exception=(mode == "abort"),
            )
            after = {
                "profile": probe.rows(game, "SELECT user_stamina,stone FROM user_xiuxian WHERE user_id=?", (actor,)),
                "daily": probe.rows(player, "SELECT date,combat_count,resource_total_count FROM map_daily_limit WHERE user_id=?", (actor,)),
                "snapshot": probe.rows(player, "SELECT snapshot FROM map_combat_settlement WHERE user_id=?", (actor,)),
                "inventory": probe.rows(game, "SELECT goods_num,bind_num FROM back WHERE user_id=? AND goods_id=21001", (actor,)),
                "receipt": probe.rows(game, "SELECT payload,stone,rewards FROM map_combat_settlement_operations WHERE operation_id=?", (op_settle,)),
            }
            assert after == before, (point, mode, before, after, reply)
            if mode == "abort":
                assert "forced map combat" in json.dumps(probe.calls[-1]["errors"], ensure_ascii=False), probe.calls[-1]
            else:
                expected_reason = f"map_combat_settlement_{point}_{'write_rejected' if mode == 'ignore' else 'projection_mismatch'}"
                assert reason_text(expected_reason) in reply, (point, mode, expected_reason, reply)
            if mode == "abort":
                assert probe.rows(game, "SELECT status FROM operation_ledger WHERE operation_id=? AND action='combat.settle'", (op_settle,)) == [("failed",)]
            else:
                assert probe.rows(game, "SELECT 1 FROM operation_ledger WHERE operation_id=? AND action='combat.settle'", (op_settle,)) == []
            probe.seed(database, f"DROP TRIGGER {trigger}")
            retry, _ = await _invoke(probe, module, "节点战斗", number, actor=actor, fixed=fixed)
            assert "战利品" in retry, (point, mode, retry)
            assert probe.rows(game, "SELECT user_stamina,stone FROM user_xiuxian WHERE user_id=?", (actor,)) == [(100, 1000007)]
            assert probe.rows(player, "SELECT combat_count,resource_total_count FROM map_daily_limit WHERE user_id=?", (actor,)) == [(1, 1)]
            assert probe.rows(player, "SELECT snapshot FROM map_combat_settlement WHERE user_id=?", (actor,)) == [("",)]
            assert probe.rows(game, "SELECT goods_num,bind_num FROM back WHERE user_id=? AND goods_id=21001", (actor,)) == [(2, 2)]
            assert probe.rows(game, "SELECT stone,rewards FROM map_combat_settlement_operations WHERE operation_id=?", (op_settle,)) == [(7, "[[21001, 2]]")]
            probe.evidence[f"{point}_{mode}"] = {"failure": reply, "retry": retry, "before": before, "after_failed": after}

        capacity_number = 974980
        task_id, snapshot, daily, item = _seed_planned(
            probe, module, capacity_number, actor=actor, item=item, fixed=fixed,
        )
        max_goods = int(module.XiuConfig().max_goods_num)
        probe.seed(
            game,
            "INSERT INTO back(user_id,goods_id,goods_name,goods_type,goods_num,create_time,update_time,bind_num) "
            "VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(user_id,goods_id) DO UPDATE SET goods_num=excluded.goods_num,bind_num=excluded.bind_num",
            (actor, item["id"], item["name"], item["type"], max_goods, fixed, fixed, max_goods),
        )
        full_reply, _ = await _invoke(probe, module, "节点战斗", capacity_number, actor=actor, fixed=fixed)
        op_settle = f"map-combat-settle:{task_id}"
        assert "背包物品已达上限" in full_reply, full_reply
        assert probe.rows(player, "SELECT snapshot FROM map_combat_settlement WHERE user_id=?", (actor,)) == [(snapshot,)]
        assert probe.rows(game, "SELECT stone FROM user_xiuxian WHERE user_id=?", (actor,)) == [(1000000,)]
        assert probe.rows(game, "SELECT 1 FROM map_combat_settlement_operations WHERE operation_id=?", (op_settle,)) == []
        probe.seed(game, "DELETE FROM back WHERE user_id=? AND goods_id=?", (actor, item["id"]))
        recovered, _ = await _invoke(probe, module, "节点战斗", capacity_number, actor=actor, fixed=fixed)
        assert "战利品" in recovered, recovered
        assert probe.rows(game, "SELECT goods_num,bind_num FROM back WHERE user_id=? AND goods_id=?", (actor, item["id"])) == [(item["amount"], item["amount"])]
        assert probe.rows(player, "SELECT combat_count FROM map_daily_limit WHERE user_id=?", (actor,)) == [(1,)]
        assert probe.rows(game, "SELECT stone,rewards FROM map_combat_settlement_operations WHERE operation_id=?", (op_settle,)) == [(7, f"[[{item['id']}, {item['amount']}]]")]
        for database_path in (game, player):
            with DatabaseUnitOfWork(database_path, read_only=True) as uow:
                assert uow.query_one("PRAGMA integrity_check")["integrity_check"] == "ok"
        probe.evidence["inventory_full_recovery"] = {
            "initial_reply": full_reply, "recovery_reply": recovered,
            "max_goods": max_goods, "snapshot_preserved": True,
        }
