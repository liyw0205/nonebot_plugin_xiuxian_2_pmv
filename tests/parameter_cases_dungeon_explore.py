"""Exercise real dungeon encounters and durable settlement in isolated databases."""

import json
from contextlib import nullcontext
from unittest.mock import AsyncMock, patch

from tests.parameter_cases_dungeon_exit import live_manager
from tests.parameter_cases_team_leave import integrity
from tests.parameter_cases_team_view import database_projection


def assets(probe):
    return {name: [table for table in tables if table[0]["name"] != "dungeon_explore_operations"]
            for name, tables in database_projection(probe).items()}


def resource_encounter_number(module, manager, level, exp, *, start=980000, kind="spirit_stone"):
    resolution = object.__new__(type(manager))
    resolution.__dict__.update(manager.__dict__)
    resolution.sync_current_dungeon = lambda: None
    for number in range(start, start + 500):
        operation = f"dungeon-explore:{number}:900001"
        event = resolution.trigger_event(level, exp, random_source=module._dungeon_explore_random_source(operation, "encounter"))
        if event["type"] == kind and (kind != "treasure" or event.get("drop_items")):
            return number
    raise AssertionError(("published template has no deterministic encounter", kind, level, manager.current_dungeon.id,
                          len(module.items.items), [(event.event_id, getattr(event, "drop_items", None)) for event in manager.current_dungeon.events],
                          len(module.items.get_random_id_list_by_rank_and_item_type(30, "功法"))))


def ready(probe, module, manager):
    probe.player(hp=50000, mp=10000, exp=100000)
    probe.seed(probe.context.paths.game_db, "INSERT INTO user_cd(user_id,type) SELECT '900001',0 WHERE NOT EXISTS(SELECT 1 FROM user_cd WHERE user_id='900001')")
    probe.seed(probe.context.paths.game_db, "UPDATE user_cd SET type=0 WHERE user_id='900001'")
    status = manager.get_player_status("900001")
    probe.seed(probe.context.paths.player_db,
               "UPDATE player_dungeon_status SET dungeon_status='not_started',current_layer=0 WHERE user_id='900001'")
    module.get_players_attributes("900001", None)
    return status


def register(case):
    @case("dungeon-explore-receipt-recovery", "探索副本", "合法单人灵石遭遇，结算永久回执UPDATE IGNORE",
          "已发布副本、真实遭遇RNG和隔离双库", "具体回执原因，资产/进度完整回滚，prepared保留，解除故障原消息恢复/replay",
          group="dungeon-explore-parameters")
    async def receipt_recovery(probe):
        game = probe.context.paths.game_db
        with live_manager() as (module, manager):
            ready(probe, module, manager)
            profile = module._sql_message().get_user_info_with_id("900001")
            number = resource_encounter_number(module, manager, profile["level"], int(profile["exp"]))
            operation = f"dungeon-explore:{number}:900001"
            before = assets(probe)
            probe.seed(game, "CREATE TRIGGER exploration_receipt_fault BEFORE UPDATE ON dungeon_explore_operations "
                       "WHEN NEW.phase='completed' AND NEW.result_status='applied' BEGIN "
                       "UPDATE user_cd SET type=99 WHERE user_id='900003'; SELECT RAISE(IGNORE); END")
            try:
                failed = await probe.command("探索副本", number=number)
                rows = probe.rows(game, "SELECT phase,result_status,prepared_json FROM dungeon_explore_operations WHERE operation_id=?", (operation,))
                probe.evidence.update(reply=failed, receipt=rows, assets_unchanged=assets(probe) == before)
                assert "副本探索结算回执保存失败" in failed and "获得" not in failed, failed
                assert assets(probe) == before
                assert len(rows) == 1 and rows[0][:2] == ("prepared", ""), rows
            finally:
                probe.seed(game, "DROP TRIGGER exploration_receipt_fault")
            prepared = json.loads(rows[0][2])
            reward = prepared["members"][0]["stone_delta"]
            assert reward > 0
            stone_before = prepared["members"][0]["expected"]["stone"]
            applied = await probe.command("副本探索", number=number)
            assert "获得" in applied and "当前：第2层" in applied, applied
            assert probe.rows(game, "SELECT stone FROM user_xiuxian WHERE user_id='900001'") == [(stone_before + reward,)]
            assert probe.rows(probe.context.paths.player_db,
                              "SELECT dungeon_status,current_layer FROM player_dungeon_status WHERE user_id='900001'") == [("exploring", 1)]
            committed = database_projection(probe)
            assert await probe.command("挑战副本", number=number) == applied
            assert database_projection(probe) == committed
        integrity(probe)
        probe.evidence.update(real_encounter=True, recovered=True, replay_zero_writes=True, integrity_check="ok")

    @case("dungeon-explore-stage-recovery", "探索副本", "合法灵石遭遇，输入/计划/资源/进度/回执拒写与改写",
          "真实manager/业务钟和实际双库trigger", "每个写点保留具体原因、资产完整回滚、pending阶段正确和同消息恢复/replay",
          group="dungeon-explore-parameters")
    async def stage_recovery(probe):
        game, player = probe.context.paths.game_db, probe.context.paths.player_db
        faults = (
            (game, "BEFORE INSERT ON dungeon_explore_operations", "1", "SELECT RAISE(IGNORE)", "输入保存失败", ""),
            (game, "AFTER INSERT ON dungeon_explore_operations", "NEW.phase='intent'", "UPDATE dungeon_explore_operations SET intent_json='{}' WHERE operation_id=NEW.operation_id", "保存的输入与请求不一致", ""),
            (game, "BEFORE UPDATE ON dungeon_explore_operations", "NEW.phase='prepared'", "SELECT RAISE(IGNORE)", "结算计划保存失败", "intent"),
            (game, "AFTER UPDATE ON dungeon_explore_operations", "NEW.phase='prepared'", "UPDATE dungeon_explore_operations SET prepared_json='{}' WHERE operation_id=NEW.operation_id", "保存的结算计划与请求不一致", "intent"),
            (game, "BEFORE UPDATE ON user_xiuxian", "NEW.user_id='900001'", "SELECT RAISE(IGNORE)", "玩家资源保存失败", "prepared"),
            (game, "AFTER UPDATE ON user_xiuxian", "NEW.user_id='900001'", "UPDATE user_xiuxian SET hp=1 WHERE user_id=NEW.user_id", "玩家资源与结算计划不一致", "prepared"),
            (player, "BEFORE UPDATE ON player_dungeon_status", "NEW.user_id='900001'", "SELECT RAISE(IGNORE)", "进度保存失败", "prepared"),
            (player, "AFTER UPDATE ON player_dungeon_status", "NEW.user_id='900001'", "UPDATE player_dungeon_status SET dungeon_name='changed' WHERE user_id=NEW.user_id", "进度与结算计划不一致", "prepared"),
            (game, "AFTER UPDATE ON dungeon_explore_operations", "NEW.phase='completed'", "UPDATE dungeon_explore_operations SET result_json='{}' WHERE operation_id=NEW.operation_id", "结算回执与请求不一致", "prepared"),
            (game, "AFTER UPDATE ON dungeon_explore_operations", "NEW.phase='completed'", "UPDATE user_xiuxian SET stone=0 WHERE user_id='900001'", "玩家资源与结算计划不一致", "prepared"),
        )
        with live_manager() as (module, manager):
            for index, (database, timing, condition, body, phrase, phase) in enumerate(faults):
                ready(probe, module, manager)
                profile = module._sql_message().get_user_info_with_id("900001")
                number = resource_encounter_number(module, manager, profile["level"], int(profile["exp"]), start=980100 + index * 500)
                operation = f"dungeon-explore:{number}:900001"
                before = assets(probe)
                side_effect = "UPDATE user_cd SET type=99 WHERE user_id='900003'; " if database == game else "INSERT OR IGNORE INTO team_cd(user_id,join_cd_until) VALUES('side-effect','touched'); "
                probe.seed(database, f"CREATE TRIGGER exploration_stage_fault {timing} WHEN {condition} BEGIN {side_effect}{body}; END")
                try:
                    failed = await probe.command("探索副本", number=number)
                    assert phrase in failed and "获得" not in failed, failed
                    assert assets(probe) == before
                    rows = probe.rows(game, "SELECT phase,result_status,intent_json,prepared_json FROM dungeon_explore_operations WHERE operation_id=?", (operation,))
                    assert (rows[0][0] if rows else "") == phase and (not rows or rows[0][1] == ""), rows
                finally:
                    probe.seed(database, "DROP TRIGGER exploration_stage_fault")
                no_resolution = patch.object(module, "_dungeon_explore_random_source", side_effect=AssertionError("prepared recovery must use frozen result")) if phase == "prepared" else nullcontext()
                with no_resolution:
                    applied = await probe.command("副本探索", number=number)
                assert "获得" in applied and "当前：第2层" in applied, applied
                committed = database_projection(probe)
                assert await probe.command("挑战副本", number=number) == applied
                assert database_projection(probe) == committed
                probe.evidence[str(index)] = dict(reply=failed, preserved_phase=phase, rollback=True, recovered=True, replay_zero_writes=True)
        integrity(probe)
        probe.evidence.update(integrity_check="ok")

    @case("dungeon-explore-intent-recovery", "探索副本", "冻结输入写入后、结算计划持久化前恢复",
          "真实普通怪遭遇，准备计划时SQLite ABORT后只改变玩家境界", "同消息用已冻结境界与战斗属性继续结算，不读取新资料或重新抽取",
          group="dungeon-explore-parameters")
    async def intent_recovery_uses_frozen_profile(probe):
        game = probe.context.paths.game_db
        with live_manager() as (module, manager):
            ready(probe, module, manager)
            profile = module._sql_message().get_user_info_with_id("900001")
            original_level = str(profile["level"])
            number = resource_encounter_number(
                module, manager, original_level, int(profile["exp"]), start=987000, kind="monster"
            )
            operation = f"dungeon-explore:{number}:900001"
            probe.seed(
                game,
                "CREATE TRIGGER exploration_intent_recovery_fault BEFORE UPDATE ON dungeon_explore_operations "
                "WHEN NEW.phase='prepared' BEGIN SELECT RAISE(ABORT,'isolated plan persistence failure'); END",
            )
            try:
                failed = await probe.command("探索副本", number=number, allow_exception=True)
                row = probe.rows(
                    game,
                    "SELECT phase,intent_json FROM dungeon_explore_operations WHERE operation_id=?",
                    (operation,),
                )
                assert row and row[0][0] == "intent", row
                assert "内部异常" in failed and "获得" not in failed, failed
            finally:
                probe.seed(game, "DROP TRIGGER exploration_intent_recovery_fault")

            probe.player(level="化神境初期")
            original_battle = module.pve_fight
            captured_battle_inputs = {}

            async def capture_battle_inputs(*args, **kwargs):
                captured_battle_inputs.update(
                    json.loads(json.dumps(kwargs["player_data_by_id"], ensure_ascii=True))
                )
                return await original_battle(*args, **kwargs)

            with patch.object(module, "pve_fight", side_effect=capture_battle_inputs) as battle:
                resumed = await probe.command("副本探索", number=number)
            battle.assert_awaited_once()
            receipt = probe.rows(
                game,
                "SELECT phase,result_status,intent_json,prepared_json,result_json "
                "FROM dungeon_explore_operations WHERE operation_id=?",
                (operation,),
            )[0]
            assert "当前：第2层" in resumed, resumed
            assert receipt[:2] == ("completed", "applied"), receipt[:2]
            frozen_intent = json.loads(receipt[2])
            frozen_plan = json.loads(receipt[3])
            assert frozen_intent["members"][0]["level"] == original_level
            assert frozen_plan["members"][0]["level"] == original_level
            frozen_battle_data = {
                member_id: entry["attributes"]
                for member_id, entry in frozen_intent["battle_inputs"]["players"].items()
            }
            assert captured_battle_inputs == frozen_battle_data
            assert frozen_plan["resolved"]["kind"] == "monster"
            assert json.loads(receipt[4])["message"].replace("\r", "\n") in resumed.replace("\r", "\n")
            committed = database_projection(probe)
            assert await probe.command("挑战副本", number=number) == resumed
            assert database_projection(probe) == committed
        integrity(probe)
        probe.evidence.update(
            original_level=original_level,
            changed_level="化神境初期",
            frozen_intent_used=True,
            recovered=True,
            replay_zero_writes=True,
            integrity_check="ok",
        )

    @case("dungeon-explore-intent-inventory-recovery", "探索副本", "冻结宝物库存后库存变化再恢复",
          "真实掉落遭遇，计划保存ABORT后增加实际背包数量", "计划仍引用冻结库存，变更按具体原因拒绝且不再发奖",
          group="dungeon-explore-parameters")
    async def intent_inventory_recovery(probe):
        game, player = probe.context.paths.game_db, probe.context.paths.player_db
        with live_manager() as (module, manager):
            ready(probe, module, manager)
            probe.player(level="筑基境初期")
            profile = module._sql_message().get_user_info_with_id("900001")
            number = resource_encounter_number(
                module, manager, profile["level"], int(profile["exp"]), start=987600, kind="treasure"
            )
            operation = f"dungeon-explore:{number}:900001"
            resolution = object.__new__(type(manager))
            resolution.__dict__.update(manager.__dict__)
            resolution.sync_current_dungeon = lambda: None
            event = resolution.trigger_event(
                profile["level"], int(profile["exp"]),
                random_source=module._dungeon_explore_random_source(operation, "encounter"),
            )
            item_id = int(event["drop_items"])
            probe.seed(game, "DELETE FROM back WHERE user_id='900001' AND goods_id=?", (item_id,))
            probe.seed(
                game,
                "CREATE TRIGGER exploration_intent_inventory_fault BEFORE UPDATE ON dungeon_explore_operations "
                "WHEN NEW.phase='prepared' BEGIN SELECT RAISE(ABORT,'isolated frozen inventory failure'); END",
            )
            try:
                failed = await probe.command("探索副本", number=number, allow_exception=True)
                row = probe.rows(game, "SELECT phase,intent_json FROM dungeon_explore_operations WHERE operation_id=?", (operation,))
                assert row and row[0][0] == "intent", row
                assert "内部异常" in failed and "当前：第2层" not in failed, failed
            finally:
                probe.seed(game, "DROP TRIGGER exploration_intent_inventory_fault")
            frozen = json.loads(row[0][1])
            expected_counts = frozen["inventory"]["900001"].get(str(item_id), {"goods_num": 0, "bind_num": 0})
            assert expected_counts == {"goods_num": 0, "bind_num": 0}, expected_counts
            probe.grant_item(item_id, quantity=1)
            before_recovery = assets(probe)
            reply = await probe.command("副本探索", number=number)
            record = probe.rows(game, "SELECT phase,result_status,prepared_json,result_json FROM dungeon_explore_operations WHERE operation_id=?", (operation,))[0]
            plan = json.loads(record[2])
            assert record[:2] == ("completed", "state_changed"), record[:2]
            planned_item = plan["members"][0]["items"][0]
            assert planned_item["id"] == item_id and planned_item["expected_num"] == 0
            rejection = json.loads(record[3])
            assert rejection["reason"] == "item_changed" and rejection["subject"] == "900001", rejection
            assert assets(probe) == before_recovery
            assert probe.rows(game, "SELECT goods_num,bind_num FROM back WHERE user_id='900001' AND goods_id=?", (item_id,)) == [(1, 0)]
            committed = database_projection(probe)
            assert await probe.command("挑战副本", number=number) == reply
            assert database_projection(probe) == committed
        integrity(probe)
        probe.evidence.update(item_id=item_id, frozen_expected=expected_counts, reply=reply,
                              no_second_grant=True, replay_zero_writes=True, integrity_check="ok")

    @case("dungeon-explore-intent-team-recovery", "探索副本", "冻结队伍后成员离队再恢复",
          "真实两人队伍与非战斗遭遇，计划保存ABORT后成员真实离队", "结算计划继续使用冻结成员，按队伍变化拒绝并不扣资源",
          group="dungeon-explore-parameters")
    async def intent_team_recovery(probe):
        game, player = probe.context.paths.game_db, probe.context.paths.player_db
        with live_manager() as (module, manager):
            ready(probe, module, manager)
            probe.player(uid="900002", hp=50000, mp=10000, exp=100000)
            manager.get_player_status("900002")
            module.get_players_attributes("900002", None)
            assert "创建成功" in await probe.command("创建队伍", "创建队伍 冻结队伍", number=989700)
            assert "发送组队邀请" in await probe.command("邀请组队", "邀请组队 诊断对手", number=989701)
            assert "成功加入队伍" in await probe.command("同意组队", actor="900002", number=989702)
            profile = module._sql_message().get_user_info_with_id("900001")
            number = resource_encounter_number(
                module, manager, profile["level"], int(profile["exp"]), start=989800, kind="spirit_stone"
            )
            operation = f"dungeon-explore:{number}:900001"
            probe.seed(
                game,
                "CREATE TRIGGER exploration_intent_team_fault BEFORE UPDATE ON dungeon_explore_operations "
                "WHEN NEW.phase='prepared' BEGIN SELECT RAISE(ABORT,'isolated frozen team failure'); END",
            )
            try:
                failed = await probe.command("探索副本", number=number, allow_exception=True)
                row = probe.rows(game, "SELECT phase,intent_json FROM dungeon_explore_operations WHERE operation_id=?", (operation,))
                assert row and row[0][0] == "intent", row
                assert "内部异常" in failed and "获得" not in failed, failed
            finally:
                probe.seed(game, "DROP TRIGGER exploration_intent_team_fault")
            intent = json.loads(row[0][1])
            assert intent["team"]["members"] == ["900001", "900002"]
            assert "你已离开队伍" in await probe.command("离开队伍", actor="900002", number=989703)
            before_recovery = assets(probe)
            reply = await probe.command("副本探索", number=number)
            result = probe.rows(game, "SELECT phase,result_status,prepared_json,result_json FROM dungeon_explore_operations WHERE operation_id=?", (operation,))[0]
            plan = json.loads(result[2])
            assert result[:2] == ("completed", "team_changed"), result[:2]
            assert [member["user_id"] for member in plan["members"]] == ["900001", "900002"]
            assert plan["team"] == intent["team"]
            assert "队伍已解散或成员变动" in json.loads(result[3])["message"]
            assert assets(probe) == before_recovery
            committed = database_projection(probe)
            assert await probe.command("挑战副本", number=number) == reply
            assert database_projection(probe) == committed
        integrity(probe)
        probe.evidence.update(frozen_members=intent["team"]["members"], reply=reply,
                              team_change_reason="team_changed", replay_zero_writes=True,
                              integrity_check="ok")

    @case("dungeon-explore-item-recovery", "探索副本", "真实宝物遭遇，新增/已有奖励物品拒写、改写及回执末步改写物品",
          "真实掉落生成与隔离背包，不替换奖励引擎", "具体物品原因/主体、全部资产回滚，原计划恢复只发一次",
          group="dungeon-explore-parameters")
    async def item_recovery(probe):
        game = probe.context.paths.game_db
        faults = (
            (False, "BEFORE INSERT ON back", "1", "SELECT RAISE(IGNORE)", "奖励物品保存失败"),
            (True, "BEFORE UPDATE ON back", "1", "SELECT RAISE(IGNORE)", "奖励物品保存失败"),
            (True, "AFTER UPDATE ON back", "1", "UPDATE back SET bind_num=0 WHERE user_id=NEW.user_id AND goods_id=NEW.goods_id", "奖励物品与结算计划不一致"),
            (False, "AFTER UPDATE ON dungeon_explore_operations", "NEW.phase='completed'", "UPDATE back SET goods_num=0,bind_num=0 WHERE user_id='900001'", "奖励物品与结算计划不一致"),
        )
        with live_manager() as (module, manager):
            probe.player(level="筑基境初期")
            resolution = object.__new__(type(manager))
            resolution.__dict__.update(manager.__dict__)
            resolution.sync_current_dungeon = lambda: None
            for index, (existing, timing, condition, body, phrase) in enumerate(faults):
                ready(probe, module, manager)
                profile = module._sql_message().get_user_info_with_id("900001")
                number = resource_encounter_number(module, manager, profile["level"], int(profile["exp"]), start=986000 + index * 500, kind="treasure")
                operation = f"dungeon-explore:{number}:900001"
                event = resolution.trigger_event(profile["level"], int(profile["exp"]), random_source=module._dungeon_explore_random_source(operation, "encounter"))
                item_id = int(event["drop_items"])
                probe.seed(game, "DELETE FROM back WHERE user_id='900001' AND goods_id=?", (item_id,))
                if existing:
                    probe.grant_item(item_id, quantity=2)
                before = assets(probe)
                probe.seed(game, f"CREATE TRIGGER exploration_item_fault {timing} WHEN {condition} BEGIN "
                           f"UPDATE user_cd SET type=99 WHERE user_id='900003'; {body}; END")
                try:
                    failed = await probe.command("探索副本", number=number)
                    assert phrase in failed and "玩家【900001】" in failed and f"物品【{item_id}】" in failed and "当前：第2层" not in failed, failed
                    assert assets(probe) == before
                    row = probe.rows(game, "SELECT phase,prepared_json FROM dungeon_explore_operations WHERE operation_id=?", (operation,))[0]
                    assert row[0] == "prepared"
                finally:
                    probe.seed(game, "DROP TRIGGER exploration_item_fault")
                item = json.loads(row[1])["members"][0]["items"][0]
                with patch.object(module, "_dungeon_explore_random_source", side_effect=AssertionError("must resume item plan")):
                    applied = await probe.command("副本探索", number=number)
                assert "当前：第2层" in applied, applied
                assert probe.rows(game, "SELECT goods_num,bind_num FROM back WHERE user_id='900001' AND goods_id=?", (item_id,)) == [(item["expected_num"] + 1, item["expected_bind_num"] + 1)]
                committed = database_projection(probe)
                assert await probe.command("挑战副本", number=number) == applied
                assert database_projection(probe) == committed
                probe.evidence[str(index)] = dict(item_id=item_id, reply=failed, rollback=True, recovered=True, replay_zero_writes=True)
        integrity(probe)
        probe.evidence.update(real_treasure=True, integrity_check="ok")

    @case("dungeon-explore-manager-isolation", "探索副本", "合法探索后再次查询，发布同ID新代际再探索",
          "真实单例manager与实际发布/结算，不替换业务方法", "探索冻结模板不能改写运行manager同步方法，刷新后读到新代际",
          group="dungeon-explore-parameters")
    async def manager_isolation(probe):
        with live_manager() as (module, manager):
            ready(probe, module, manager)
            sync = manager.sync_current_dungeon
            profile = module._sql_message().get_user_info_with_id("900001")
            number = resource_encounter_number(module, manager, profile["level"], int(profile["exp"]), start=989000)
            first = await probe.command("探索副本", number=number)
            probe.evidence.update(reply=first, singleton_sync_preserved=manager.sync_current_dungeon == sync)
            assert "当前：第2层" in first, first
            assert manager.sync_current_dungeon == sync, "real handler overwrote the live singleton sync method"
            previous = manager.get_player_status("900001")
            manager.reset_dungeon("explore-manager-new-generation", source="manual")
            current = manager.get_player_status("900001")
            assert current["reset_generation"] == previous["reset_generation"] + 1
            assert current["current_layer"] == 0 and current["dungeon_status"] == "not_started"
        integrity(probe)
        probe.evidence.update(real_reset=True, integrity_check="ok")

    @case("dungeon-explore-team-recovery", "探索副本", "真实两人队伍，第二成员资源拒写及非队长拒绝",
          "真实创建/邀请/入队和陷阱遭遇，合法健康玩家", "第二成员失败回滚第一成员，prepared恢复双方气血/队长进度且仅结算一次",
          group="dungeon-explore-parameters")
    async def team_recovery(probe):
        game, player = probe.context.paths.game_db, probe.context.paths.player_db
        with live_manager() as (module, manager):
            ready(probe, module, manager)
            probe.player(uid="900002", hp=50000, mp=10000, exp=100000)
            manager.get_player_status("900002")
            module.get_players_attributes("900002", None)
            assert "创建成功" in await probe.command("创建队伍", "创建队伍 探索验收队", number=989100)
            assert "发送组队邀请" in await probe.command("邀请组队", "邀请组队 诊断对手", number=989101)
            assert "成功加入队伍" in await probe.command("同意组队", actor="900002", number=989102)
            denied = await probe.command("探索副本", actor="900002", number=989103)
            assert "你不是队长" in denied, denied
            from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_buff.partner import get_mentor_team_attack_buffs

            get_mentor_team_attack_buffs(["900001", "900002"])
            profile = module._sql_message().get_user_info_with_id("900001")
            number = resource_encounter_number(module, manager, profile["level"], int(profile["exp"]), start=989200, kind="trap")
            operation = f"dungeon-explore:{number}:900001"
            before = assets(probe)
            probe.seed(game, "CREATE TRIGGER exploration_team_fault BEFORE UPDATE ON user_xiuxian WHEN NEW.user_id='900002' "
                       "BEGIN UPDATE user_cd SET type=99 WHERE user_id='900003'; SELECT RAISE(IGNORE); END")
            try:
                failed = await probe.command("探索副本", number=number)
                assert "玩家资源保存失败" in failed and "900002" in failed, failed
                assert assets(probe) == before
                row = probe.rows(game, "SELECT phase,prepared_json FROM dungeon_explore_operations WHERE operation_id=?", (operation,))[0]
                assert row[0] == "prepared"
            finally:
                probe.seed(game, "DROP TRIGGER exploration_team_fault")
            plan = json.loads(row[1])
            assert len(plan["members"]) == 2
            with patch.object(module, "_dungeon_explore_random_source", side_effect=AssertionError("must resume team plan")):
                applied = await probe.command("副本探索", number=number)
            assert "气血减少" in applied and "当前：第2层" in applied, applied
            for member in plan["members"]:
                assert member["final_hp"] < member["expected"]["hp"]
                assert probe.rows(game, "SELECT hp,mp,stone,exp FROM user_xiuxian WHERE user_id=?", (member["user_id"],)) == [
                    (member["final_hp"], member["final_mp"], member["expected"]["stone"], member["expected"]["exp"])]
            assert probe.rows(player, "SELECT user_id,dungeon_status,current_layer FROM player_dungeon_status ORDER BY user_id") == [
                ("900001", "exploring", 1), ("900002", "not_started", 0)]
            committed = database_projection(probe)
            assert await probe.command("挑战副本", number=number) == applied
            assert database_projection(probe) == committed
        integrity(probe)
        probe.evidence.update(non_leader_denied=True, members=2, second_member_rollback=True,
                              recovered=True, replay_zero_writes=True, integrity_check="ok")

    @case("dungeon-explore-rejection-contracts", "探索副本", "已完成/忙碌/重伤、拒绝回执拒写和恢复，私聊入口",
          "合法玩家与真实已发布进度和业务状态", "正常规则保留具体拒绝、业务资产零写；失败回执不得虚报已记录，恢复后稳定回放",
          group="dungeon-explore-parameters")
    async def rejection_contracts(probe):
        from nonebot.adapters.onebot.v11 import PrivateMessageEvent
        from tests import command_probe

        game, player = probe.context.paths.game_db, probe.context.paths.player_db
        with live_manager() as (module, manager):
            for index, (kind, phrase, result_status) in enumerate((
                ("completed", "今日副本已完成", "completed"),
                ("busy", "正处于其他修炼状态", "member_busy"),
                ("injured", "重伤未愈", "member_injured"),
            )):
                ready(probe, module, manager)
                if kind == "completed":
                    probe.seed(player, "UPDATE player_dungeon_status SET dungeon_status='completed',current_layer=total_layers WHERE user_id='900001'")
                elif kind == "busy":
                    probe.seed(game, "UPDATE user_cd SET type=1 WHERE user_id='900001'")
                else:
                    probe.player(hp=1)
                before = assets(probe)
                number = 989300 + index
                denied = await probe.command("探索副本", number=number)
                probe.evidence[kind] = {"reply": denied, "status": manager.get_player_status("900001"),
                                        "hp_cd": probe.rows(game, "SELECT hp,(SELECT type FROM user_cd WHERE user_id='900001' ORDER BY rowid DESC LIMIT 1) FROM user_xiuxian WHERE user_id='900001'")}
                assert phrase in denied, denied
                assert assets(probe) == before
                assert probe.rows(game, "SELECT phase,result_status FROM dungeon_explore_operations WHERE operation_id=?", (f"dungeon-explore:{number}:900001",)) == [("completed", result_status)]
                ready(probe, module, manager)
                committed = database_projection(probe)
                assert await probe.command("挑战副本", number=number) == denied
                assert database_projection(probe) == committed

            for index, (timing, body, phrase) in enumerate((
                ("BEFORE", "SELECT RAISE(IGNORE)", "结算回执保存失败"),
                ("AFTER", "UPDATE dungeon_explore_operations SET result_status='applied' WHERE operation_id=NEW.operation_id", "结算回执与请求不一致"),
            )):
                ready(probe, module, manager)
                probe.seed(game, "UPDATE user_cd SET type=1 WHERE user_id='900001'")
                before = assets(probe)
                number = 989310 + index
                probe.seed(game, f"CREATE TRIGGER exploration_rejection_fault {timing} INSERT ON dungeon_explore_operations "
                           f"BEGIN UPDATE user_cd SET type=99 WHERE user_id='900003'; {body}; END")
                try:
                    failed = await probe.command("探索副本", number=number)
                    assert phrase in failed and "正处于其他修炼状态" not in failed, failed
                    assert assets(probe) == before
                    assert probe.rows(game, "SELECT phase FROM dungeon_explore_operations WHERE operation_id=?", (f"dungeon-explore:{number}:900001",)) == []
                finally:
                    probe.seed(game, "DROP TRIGGER exploration_rejection_fault")
                denied = await probe.command("副本探索", number=number)
                assert "正处于其他修炼状态" in denied, denied
                committed = database_projection(probe)
                assert await probe.command("探索副本", number=number) == denied
                assert database_projection(probe) == committed

            ready(probe, module, manager)
            profile = module._sql_message().get_user_info_with_id("900001")
            number = resource_encounter_number(module, manager, profile["level"], int(profile["exp"]), start=989350)
            make_group = command_probe.make_event

            def make_private(text, actor, number):
                data = make_group(text, actor, number).model_dump()
                data.pop("group_id")
                data.update(message_type="private", sub_type="friend")
                return PrivateMessageEvent.model_validate(data)

            with patch.object(command_probe, "make_event", side_effect=make_private):
                private = await probe.command("副本探索", number=number)
            assert "获得" in private and "当前：第2层" in private, private
        integrity(probe)
        probe.evidence.update(normal_rejections=True, rejection_receipt_recovery=True, private=True, integrity_check="ok")

    @case("dungeon-explore-abort-reply", "探索副本", "真实六阶段ABORT、结算后发送失败",
          "真实遭遇与隔离双库trigger，发送失败mock", "原异常/日志/定位号、完整回滚、pending阶段正确；发送失败仍已提交且可回放",
          group="dungeon-explore-parameters")
    async def abort_reply(probe):
        game, player = probe.context.paths.game_db, probe.context.paths.player_db
        targets = (
            (game, "INSERT ON dungeon_explore_operations", "1", ""),
            (game, "UPDATE ON dungeon_explore_operations", "NEW.phase='prepared'", "intent"),
            (game, "UPDATE ON user_xiuxian", "NEW.user_id='900001'", "prepared"),
            (player, "UPDATE ON player_dungeon_status", "NEW.user_id='900001'", "prepared"),
            (game, "UPDATE ON dungeon_explore_operations", "NEW.phase='completed'", "prepared"),
            (game, "INSERT ON back", "1", "prepared"),
        )
        with live_manager() as (module, manager):
            for index, (database, timing, condition, phase) in enumerate(targets):
                ready(probe, module, manager)
                if index == 5:
                    probe.player(level="筑基境初期")
                profile = module._sql_message().get_user_info_with_id("900001")
                number = resource_encounter_number(module, manager, profile["level"], int(profile["exp"]),
                                                   start=990000 + index * 500, kind="treasure" if index == 5 else "spirit_stone")
                operation = f"dungeon-explore:{number}:900001"
                before = assets(probe)
                probe.seed(database, f"CREATE TRIGGER exploration_abort BEFORE {timing} WHEN {condition} "
                           "BEGIN SELECT RAISE(ABORT,'isolated real exploration abort'); END")
                try:
                    failed = await probe.command("探索副本", number=number, allow_exception=True)
                    assert "定位号：" in failed and "内部异常" in failed and "获得" not in failed, failed
                    assert any("isolated real exploration abort" in row["exception"] for row in probe.calls[-1]["logs"])
                    assert assets(probe) == before
                    rows = probe.rows(game, "SELECT phase FROM dungeon_explore_operations WHERE operation_id=?", (operation,))
                    assert (rows[0][0] if rows else "") == phase, rows
                finally:
                    probe.seed(database, "DROP TRIGGER exploration_abort")
                assert "当前：第2层" in await probe.command("探索副本", number=number)
            ready(probe, module, manager)
            profile = module._sql_message().get_user_info_with_id("900001")
            number = resource_encounter_number(module, manager, profile["level"], int(profile["exp"]), start=993500)
            with patch.object(module, "handle_send", AsyncMock(side_effect=RuntimeError("isolated committed exploration reply failure"))):
                failed = await probe.command("探索副本", number=number, allow_exception=True)
            assert "定位号：" in failed and "内部异常" in failed, failed
            assert any("isolated committed exploration reply failure" in row["exception"] for row in probe.calls[-1]["logs"])
            assert probe.rows(game, "SELECT phase,result_status FROM dungeon_explore_operations WHERE operation_id=?",
                              (f"dungeon-explore:{number}:900001",)) == [("completed", "applied")]
            committed = database_projection(probe)
            replay = await probe.command("挑战副本", number=number)
            assert "获得" in replay and "当前：第2层" in replay, replay
            assert database_projection(probe) == committed
        integrity(probe)
        probe.evidence.update(abort_rollback_recovery=True, reply_failure_committed=True, replay_zero_writes=True, integrity_check="ok")

    @case("dungeon-explore-capacity", "探索副本", "真实宝物数量已满及恰好装满",
          "合法真实掉落物品，实际背包达到配置上限", "超限拒绝资产零写且稳定回放，释放容量后新消息恰好装满",
          group="dungeon-explore-parameters")
    async def capacity(probe):
        game = probe.context.paths.game_db
        with live_manager() as (module, manager):
            ready(probe, module, manager)
            probe.player(level="筑基境初期")
            profile = module._sql_message().get_user_info_with_id("900001")
            number = resource_encounter_number(module, manager, profile["level"], int(profile["exp"]), start=994000, kind="treasure")
            operation = f"dungeon-explore:{number}:900001"
            resolution = object.__new__(type(manager))
            resolution.__dict__.update(manager.__dict__)
            resolution.sync_current_dungeon = lambda: None
            event = resolution.trigger_event(profile["level"], int(profile["exp"]), random_source=module._dungeon_explore_random_source(operation, "encounter"))
            item_id = int(event["drop_items"])
            maximum = module.XiuConfig().max_goods_num
            probe.grant_item(item_id, quantity=1)
            probe.seed(game, "UPDATE back SET goods_num=?,bind_num=0 WHERE user_id='900001' AND goods_id=?", (maximum, item_id))
            before = assets(probe)
            denied = await probe.command("探索副本", number=number)
            assert "背包中该物品数量已达上限，本次探索未结算" in denied, denied
            assert assets(probe) == before
            assert probe.rows(game, "SELECT phase,result_status FROM dungeon_explore_operations WHERE operation_id=?", (operation,)) == [("completed", "inventory_full")]
            probe.seed(game, "UPDATE back SET goods_num=? WHERE user_id='900001' AND goods_id=?", (maximum - 1, item_id))
            committed = database_projection(probe)
            assert await probe.command("挑战副本", number=number) == denied
            assert database_projection(probe) == committed
            # A new message has its own genuine encounter and may award a different item.
            next_number = resource_encounter_number(module, manager, profile["level"], int(profile["exp"]), start=number + 1, kind="treasure")
            next_operation = f"dungeon-explore:{next_number}:900001"
            event = resolution.trigger_event(profile["level"], int(profile["exp"]), random_source=module._dungeon_explore_random_source(next_operation, "encounter"))
            next_item = int(event["drop_items"])
            probe.grant_item(next_item, quantity=1)
            probe.seed(game, "UPDATE back SET goods_num=?,bind_num=0 WHERE user_id='900001' AND goods_id=?", (maximum - 1, next_item))
            applied = await probe.command("副本探索", number=next_number)
            assert "当前：第2层" in applied, applied
            assert probe.rows(game, "SELECT goods_num,bind_num FROM back WHERE user_id='900001' AND goods_id=?", (next_item,)) == [(maximum, 1)]
            committed = database_projection(probe)
            assert await probe.command("挑战副本", number=next_number) == applied
            assert database_projection(probe) == committed
        integrity(probe)
        probe.evidence.update(full_rejection=denied, exact_capacity=maximum, replay_zero_writes=True, integrity_check="ok")

    @case("dungeon-explore-real-battles", "探索副本", "普通怪物与最终BOSS的真实战斗结算",
          "合法玩家，真实遭遇/战斗/奖励引擎与隔离双库", "核实际胜负、HP/MP/奖励/进度/战报与永久回执，重放不再战斗",
          group="dungeon-explore-parameters")
    async def real_battles(probe):
        game, player = probe.context.paths.game_db, probe.context.paths.player_db
        with live_manager() as (module, manager):
            for kind in ("monster", "boss"):
                ready(probe, module, manager)
                probe.player(level="筑基境初期")
                if kind == "boss":
                    probe.seed(player, "UPDATE player_dungeon_status SET dungeon_status='exploring',current_layer=total_layers-1 WHERE user_id='900001'")
                profile = module._sql_message().get_user_info_with_id("900001")
                number = (resource_encounter_number(module, manager, profile["level"], int(profile["exp"]), start=995000, kind="monster")
                          if kind == "monster" else 996000)
                operation = f"dungeon-explore:{number}:900001"
                fight, rewards = module.pve_fight, module.build_battle_rewards
                with (patch.object(module, "pve_fight", wraps=fight) as battle,
                      patch.object(module, "build_battle_rewards", wraps=rewards) as reward):
                    applied = await probe.command("探索副本", number=number)
                battle.assert_awaited_once()
                row = probe.rows(game, "SELECT phase,result_status,prepared_json,result_json,current_layer,dungeon_status FROM dungeon_explore_operations WHERE operation_id=?", (operation,))[0]
                assert row[:2] == ("completed", "applied"), row
                plan, response = json.loads(row[2]), json.loads(row[3])
                assert plan["resolved"]["kind"] == kind and plan["resolved"]["winner"] in (0, 1)
                won = plan["resolved"]["winner"] == 0
                assert reward.call_count == int(won)
                assert bool(response["battle_messages"]) and response == plan["response"]
                assert response["message"].replace("\r", "\n") in applied.replace("\r", "\n")
                assert ("击败" if won else "不敌") in response["message"]
                member = plan["members"][0]
                assert member["final_hp"] >= 1 and member["final_mp"] >= 0
                assert probe.rows(game, "SELECT hp,mp,stone,exp FROM user_xiuxian WHERE user_id='900001'") == [(
                    member["final_hp"], member["final_mp"], member["expected"]["stone"] + member["stone_delta"],
                    member["expected"]["exp"] + member["exp_delta"])]
                if not won:
                    assert member["stone_delta"] == member["exp_delta"] == 0 and member["items"] == []
                for item in member["items"]:
                    assert probe.rows(game, "SELECT goods_num,bind_num FROM back WHERE user_id='900001' AND goods_id=?", (item["id"],)) == [(
                        item["expected_num"] + item["amount"], item["expected_bind_num"] + item["amount"])]
                expected = plan["expected_status"]
                final_layer = expected["total_layers"] if kind == "boss" and won else expected["current_layer"] + int(plan["advance"])
                final_status = "completed" if kind == "boss" and won else "exploring"
                assert row[4:] == (final_layer, final_status)
                assert probe.rows(player, "SELECT current_layer,dungeon_status FROM player_dungeon_status WHERE user_id='900001'") == [(final_layer, final_status)]
                committed = database_projection(probe)
                with patch.object(module, "pve_fight", side_effect=AssertionError("replay must not fight again")):
                    replay = await probe.command("挑战副本", number=number)
                assert response["message"].replace("\r", "\n") in replay.replace("\r", "\n")
                assert database_projection(probe) == committed
                probe.evidence[kind] = dict(winner=plan["resolved"]["winner"], reply=response["message"],
                                           hp=member["final_hp"], mp=member["final_mp"], reward=won, replay_zero_writes=True)
        integrity(probe)
        probe.evidence.update(real_battle_engine=True, integrity_check="ok")

    @case("dungeon-explore-nothing-encounter", "探索副本", "已发布模板的无事发生遭遇",
          "真实matcher、随机遭遇选择与隔离双库", "遭遇类型/战报/进度一致，重放不重复推进或写入",
          group="dungeon-explore-parameters")
    async def nothing_encounter(probe):
        game, player = probe.context.paths.game_db, probe.context.paths.player_db
        with live_manager() as (module, manager):
            ready(probe, module, manager)
            profile = module._sql_message().get_user_info_with_id("900001")
            number = resource_encounter_number(
                module, manager, profile["level"], int(profile["exp"]), start=997000, kind="nothing"
            )
            operation = f"dungeon-explore:{number}:900001"
            before = assets(probe)
            reply = await probe.command("探索副本", number=number)
            row = probe.rows(game, "SELECT phase,prepared_json,result_json FROM dungeon_explore_operations WHERE operation_id=?",
                             (operation,))[0]
            plan, response = json.loads(row[1]), json.loads(row[2])
            assert row[0] == "completed" and plan["resolved"]["kind"] == "nothing", row
            assert not response["battle_messages"] and "当前：第2层" in response["message"]
            assert response["message"].replace("\r", "\n") in reply.replace("\r", "\n")
            after = assets(probe)
            assert after != before
            committed = database_projection(probe)
            assert await probe.command("副本探索", number=number) == reply
            assert database_projection(probe) == committed
        integrity(probe)
        probe.evidence.update(reply=reply, resolved="nothing", replay_zero_writes=True, integrity_check="ok")

    @case("dungeon-explore-battle-win", "探索副本", "真实战斗引擎胜利与奖励结算",
          "真实遭遇选择/战斗引擎/奖励应用与隔离双库；仅将生成敌人属性压至测试值",
          "战斗胜者、奖励分配、资源/进度/回执与玩家回复一致，成功重放不再战斗",
          group="dungeon-explore-parameters")
    async def battle_win(probe):
        game, player = probe.context.paths.game_db, probe.context.paths.player_db
        with live_manager() as (module, manager):
            ready(probe, module, manager)
            probe.player(hp=50000, mp=10000, exp=100000)
            profile = module._sql_message().get_user_info_with_id("900001")
            number = resource_encounter_number(
                module, manager, profile["level"], int(profile["exp"]), start=998000, kind="monster"
            )
            operation = f"dungeon-explore:{number}:900001"
            create_monster = module.DungeonManager.creating_monsters

            def weak_enemy(self, user_level, user_exp, monsters_info, monster_type="minion", *, random_source=None):
                monster = create_monster(
                    self, user_level, user_exp, monsters_info, monster_type, random_source=random_source
                )
                monster.update({"气血": 1, "总血量": 1, "真元": 10, "攻击": 1, "skills": []})
                return monster

            with patch.object(module.DungeonManager, "creating_monsters", new=weak_enemy), \
                    patch.object(module, "pve_fight", wraps=module.pve_fight) as battle, \
                    patch.object(module, "build_battle_rewards", wraps=module.build_battle_rewards) as rewards:
                reply = await probe.command("探索副本", number=number)
            battle.assert_awaited_once()
            rewards.assert_called_once()
            row = probe.rows(game, "SELECT phase,prepared_json,result_json,current_layer,dungeon_status FROM dungeon_explore_operations WHERE operation_id=?",
                             (operation,))[0]
            plan, response = json.loads(row[1]), json.loads(row[2])
            assert row[0] == "completed" and plan["resolved"]["winner"] == 0, row
            assert plan["resolved"]["kind"] == "monster" and plan["advance"]
            member = plan["members"][0]
            assert member["stone_delta"] > 0 or member["exp_delta"] > 0 or member["items"]
            assert "击败敌人" in response["message"] and "当前：第2层" in reply, reply
            assert probe.rows(game, "SELECT stone,exp,hp,mp FROM user_xiuxian WHERE user_id='900001'") == [(
                member["expected"]["stone"] + member["stone_delta"],
                member["expected"]["exp"] + member["exp_delta"],
                member["final_hp"], member["final_mp"],
            )]
            assert probe.rows(player, "SELECT current_layer,dungeon_status FROM player_dungeon_status WHERE user_id='900001'") == [(1, "exploring")]
            committed = database_projection(probe)
            with patch.object(module, "pve_fight", side_effect=AssertionError("replay must not fight again")):
                assert await probe.command("挑战副本", number=number) == reply
            assert database_projection(probe) == committed
        integrity(probe)
        probe.evidence.update(reply=reply, winner=0, rewards_called=1, replay_zero_writes=True, integrity_check="ok")
