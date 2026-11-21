"""Bounded real-command coverage for cave infiltration."""

import asyncio
import json
from datetime import datetime, timezone
from unittest.mock import patch

from tests.parameter_cases_dongfu_harvest import _database_projection


def _seed(probe):
    from nonebot_plugin_xiuxian_2.features.dongfu.application import DongfuApplication
    from nonebot_plugin_xiuxian_2.features.dongfu.plant_slots import empty_plant_slot
    from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_dongfu as module

    game, player = probe.context.paths.game_db, probe.context.paths.player_db
    visitor_slots = [empty_plant_slot(index) for index in range(1, 4)]
    target_slots = [empty_plant_slot(index) for index in range(1, 4)]
    target_slots[0].update(seed_id=21001, seed_name=module.SEED_CONFIG[21001]["name"],
                           plant_start="2026-11-22 09:00:00", plant_finish="2026-11-22 11:00:00")
    for uid, name, slots in (("900001", "诊断道友", visitor_slots), ("900002", "诊断对手", target_slots)):
        probe.seed(player,
                   "INSERT INTO dongfu_status(user_id,built,realm,heaven,node_id,node_name,node_type,array_level,"
                   "plot_count,plant_slots,planting,plant_seed_id,plant_start,plant_finish,patrol_guard,"
                   "infiltrate_date,infiltrate_active_count,infiltrate_random_count,intrude_date,intrude_count) "
                   "VALUES(?,1,'金','天灵山','node-1','灵田','水域',0,3,?,1,21001,'2026-11-22 09:00:00',"
                   "'2026-11-22 11:00:00',0,'',0,0,'',0) ON CONFLICT(user_id) DO UPDATE SET "
                   "built=1,realm='金',heaven='天灵山',node_id='node-1',node_name='灵田',node_type='水域',"
                   "array_level=0,plot_count=3,plant_slots=excluded.plant_slots,planting=excluded.planting,"
                   "plant_seed_id=excluded.plant_seed_id,plant_start=excluded.plant_start,"
                   "plant_finish=excluded.plant_finish,patrol_guard=0,infiltrate_date='',"
                   "infiltrate_active_count=0,infiltrate_random_count=0,intrude_date='',intrude_count=0",
                   (uid, json.dumps(slots, ensure_ascii=False)))
        probe.seed(player,
                   "INSERT INTO map_status(user_id,realm,heaven,node_id) VALUES(?,'金','天灵山','node-1') "
                   "ON CONFLICT(user_id) DO UPDATE SET realm='金',heaven='天灵山',node_id='node-1'", (uid,))
    module.configure_dongfu_application(DongfuApplication(game, player))
    return module


def _business(probe):
    game, player = probe.context.paths.game_db, probe.context.paths.player_db
    return {
        "players": probe.rows(game, "SELECT user_id,stone,user_stamina FROM user_xiuxian ORDER BY user_id"),
        "inventory": probe.rows(game, "SELECT * FROM back ORDER BY user_id,goods_id"),
        "caves": probe.rows(player, "SELECT * FROM dongfu_status ORDER BY user_id"),
        "receipts": probe.rows(game, "SELECT * FROM dongfu_infiltrate_success_operations ORDER BY operation_id"),
        "failure_receipts": probe.rows(game, "SELECT * FROM dongfu_infiltrate_failure_operations ORDER BY operation_id"),
        "plans": probe.rows(game, "SELECT * FROM dongfu_infiltration_operations ORDER BY operation_id"),
    }


async def active_success(probe):
    module = _seed(probe)
    game, player = probe.context.paths.game_db, probe.context.paths.player_db
    reward_id = next(item_id for item_id in module.HERB_LOW
                     if module.items.get_data_by_item_id(item_id))
    reward = module.items.get_data_by_item_id(reward_id)
    now = datetime(2026, 11, 22, 12, tzinfo=timezone.utc)
    before = _business(probe)
    with (patch.object(module.runtime_clock, "now", return_value=now),
          patch.object(module.runtime_random, "random", side_effect=(0.99, 0.99)),
          patch.object(module.runtime_random, "choice", side_effect=lambda rows: rows[0]),
          patch.object(module.runtime_random, "shuffle", side_effect=lambda rows: None),
          patch.object(module.runtime_random, "randint", return_value=20),
          patch.object(module, "_get_geomancy", return_value={}),
          patch.object(module, "_roll_harvest", return_value=[(reward_id, reward["name"], reward["type"], 2)])):
        reply = await probe.command("潜入洞府", "潜入洞府 诊断对手", number=970901,
                                    allow_exception=True)
    call = probe.calls[-1]
    probe.evidence.update(reply=reply, before=before, after=_business(probe), errors=call["errors"])
    assert call["errors"] == [], call
    assert "悄然潜入" in reply and reward["name"] in reply, reply
    assert probe.rows(game, "SELECT stone FROM user_xiuxian WHERE user_id='900001'") == [(1000000,)]
    assert probe.rows(game, "SELECT user_stamina FROM user_xiuxian WHERE user_id='900001'") == [(92,)]
    assert probe.rows(game, "SELECT goods_num,bind_num FROM back WHERE user_id='900001' AND goods_id=?",
                      (reward_id,)) == [(2, 2)]
    assert probe.rows(player,
                      "SELECT infiltrate_active_count FROM dongfu_status WHERE user_id='900001'") == [(1,)]
    target = probe.rows(player,
                        "SELECT intrude_count,patrol_guard,plant_slots FROM dongfu_status WHERE user_id='900002'")[0]
    assert target[:2] == (1, 0)
    assert json.loads(target[2])[0]["plant_finish"] == "2026-11-22 11:20:00", target
    assert probe.rows(game, "SELECT infiltrate_left,intrude_left FROM dongfu_infiltrate_success_operations") == [(2, 2)]
    committed = _database_projection(probe)
    await asyncio.sleep(2)
    replay = await probe.command("潜入洞府", "潜入洞府 诊断对手", number=970901, allow_exception=True)
    assert "已经处理" in replay, replay
    assert _database_projection(probe) == committed


async def random_failure(probe):
    module = _seed(probe)
    game, player = probe.context.paths.game_db, probe.context.paths.player_db
    target_slots = json.loads(probe.rows(
        player, "SELECT plant_slots FROM dongfu_status WHERE user_id='900002'"
    )[0][0])
    target_slots[0]["plant_finish"] = "2026-11-22 13:00:00"
    probe.seed(player,
               "UPDATE dongfu_status SET plant_slots=?,plant_finish='2026-11-22 13:00:00',patrol_guard=1 "
               "WHERE user_id='900002'",
               (json.dumps(target_slots, ensure_ascii=False),))
    now = datetime(2026, 11, 22, 12, tzinfo=timezone.utc)
    with (patch.object(module.runtime_clock, "now", return_value=now),
          patch.object(module.runtime_random, "random", side_effect=(0.0, 0.99)),
          patch.object(module.runtime_random, "choice", side_effect=lambda rows: rows[0]),
          patch.object(module.runtime_random, "shuffle", side_effect=lambda rows: None),
          patch.object(module.runtime_random, "randint", side_effect=lambda low, high: 1 if low == 1 else low),
          patch.object(module, "_get_geomancy", return_value={})):
        reply = await probe.command("随机潜入洞府", "随机潜入洞府", number=970903,
                                    allow_exception=True)
        call = probe.calls[-1]
        after = _business(probe)
        probe.evidence.update(reply=reply, after=after, errors=call["errors"])
        assert call["errors"] == [], call
        assert "潜入失败" in reply and "损失灵石" in reply, reply
        assert probe.rows(game, "SELECT stone,user_stamina FROM user_xiuxian WHERE user_id='900001'") == [(950000, 92)]
        assert probe.rows(player,
                          "SELECT infiltrate_random_count FROM dongfu_status WHERE user_id='900001'") == [(1,)]
        assert probe.rows(player,
                          "SELECT intrude_count,patrol_guard,plant_slots FROM dongfu_status WHERE user_id='900002'") == [
                              (1, 0, json.dumps(target_slots, ensure_ascii=False))
                          ]
        assert after["inventory"] == [] and after["receipts"] == []
        assert probe.rows(game,
                          "SELECT infiltrate_left,intrude_left FROM dongfu_infiltrate_failure_operations") == [(2, 2)]
        committed = _database_projection(probe)
        probe.clear_cooldowns()
        replay = await probe.command("随机潜入", "随机潜入", number=970903, allow_exception=True)
        assert "已经处理" in replay, replay
        assert _database_projection(probe) == committed


async def success_write_recovery(probe, *, trigger_database, trigger_sql, reason, subject, number):
    from nonebot_plugin_xiuxian_2.core.player_reasons import reason_text

    module = _seed(probe)
    game, player = probe.context.paths.game_db, probe.context.paths.player_db
    reward_id = next(item_id for item_id in module.HERB_LOW
                     if module.items.get_data_by_item_id(item_id))
    reward = module.items.get_data_by_item_id(reward_id)
    trigger_path = game if trigger_database == "game" else player
    before = _business(probe)
    probe.seed(trigger_path, "CREATE TRIGGER infiltrate_write_fault " + trigger_sql)
    with (patch.object(module.runtime_clock, "now", return_value=datetime(2026, 11, 22, 12, tzinfo=timezone.utc)),
          patch.object(module.runtime_random, "random", side_effect=(0.99, 0.99)),
          patch.object(module.runtime_random, "choice", side_effect=lambda rows: rows[0]),
          patch.object(module.runtime_random, "shuffle", side_effect=lambda rows: None),
          patch.object(module.runtime_random, "randint", return_value=20),
          patch.object(module, "_get_geomancy", return_value={}),
          patch.object(module, "_roll_harvest",
                       return_value=[(reward_id, reward["name"], reward["type"], 2)]) as roll):
        refused = await probe.command("潜入洞府", "潜入洞府 诊断对手", number=number,
                                      allow_exception=True)
        call = probe.calls[-1]
        after_refusal = _business(probe)
        probe.evidence.update(reason=reason, reply=refused, before=before,
                              after_refusal=after_refusal, errors=call["errors"])
        assert call["errors"] == [], call
        assert reason_text(reason) in refused and subject in refused, refused
        assert "悄然潜入" not in refused, refused
        assert after_refusal["inventory"] == before["inventory"], after_refusal
        assert after_refusal["caves"] == before["caves"], after_refusal
        assert after_refusal["receipts"] == [] and after_refusal["failure_receipts"] == [], after_refusal
        expected_players = [tuple(row) for row in before["players"]]
        expected_players[0] = (expected_players[0][0], expected_players[0][1], 92)
        assert after_refusal["players"] == expected_players, after_refusal
        assert len(after_refusal["plans"]) == 1, after_refusal

        probe.seed(trigger_path, "DROP TRIGGER infiltrate_write_fault")
        probe.clear_cooldowns()
        recovered = await probe.command("潜入洞府", "潜入洞府 诊断对手", number=number,
                                        allow_exception=True)
        assert "悄然潜入" in recovered and reward["name"] in recovered, recovered
        assert roll.call_count == 1, "recovery must use the persisted random plan"
        committed = _database_projection(probe)
        probe.clear_cooldowns()
        replay = await probe.command("潜入洞府", "潜入洞府 诊断对手", number=number,
                                     allow_exception=True)
        assert "已经处理" in replay, replay
        assert _database_projection(probe) == committed


async def failure_receipt_recovery(probe):
    from nonebot_plugin_xiuxian_2.core.player_reasons import reason_text

    module = _seed(probe)
    game, player = probe.context.paths.game_db, probe.context.paths.player_db
    target_slots = json.loads(probe.rows(
        player, "SELECT plant_slots FROM dongfu_status WHERE user_id='900002'"
    )[0][0])
    target_slots[0]["plant_finish"] = "2026-11-22 13:00:00"
    probe.seed(player,
               "UPDATE dongfu_status SET plant_slots=?,plant_finish='2026-11-22 13:00:00',patrol_guard=1 "
               "WHERE user_id='900002'",
               (json.dumps(target_slots, ensure_ascii=False),))
    before = _business(probe)
    probe.seed(game, "CREATE TRIGGER infiltrate_failure_receipt_ignore BEFORE INSERT ON "
               "dongfu_infiltrate_failure_operations BEGIN SELECT RAISE(IGNORE); END")
    with (patch.object(module.runtime_clock, "now", return_value=datetime(2026, 11, 22, 12, tzinfo=timezone.utc)),
          patch.object(module.runtime_random, "random", side_effect=(0.0, 0.99)),
          patch.object(module.runtime_random, "choice", side_effect=lambda rows: rows[0]),
          patch.object(module.runtime_random, "shuffle", side_effect=lambda rows: None),
          patch.object(module.runtime_random, "randint", return_value=50000),
          patch.object(module, "_get_geomancy", return_value={})):
        refused = await probe.command("潜入洞府", "潜入洞府 诊断对手", number=970906,
                                      allow_exception=True)
        call = probe.calls[-1]
        after_refusal = _business(probe)
        probe.evidence.update(reply=refused, before=before, after_refusal=after_refusal,
                              errors=call["errors"])
        reason = "dongfu_infiltrate_failure_receipt_write_rejected"
        assert call["errors"] == [], call
        assert reason_text(reason) in refused and "操作回执" in refused, refused
        assert "被当场逼退" not in refused, refused
        assert after_refusal["inventory"] == before["inventory"], after_refusal
        assert after_refusal["caves"] == before["caves"], after_refusal
        assert after_refusal["receipts"] == [] and after_refusal["failure_receipts"] == [], after_refusal
        expected_players = [tuple(row) for row in before["players"]]
        expected_players[0] = (expected_players[0][0], expected_players[0][1], 92)
        assert after_refusal["players"] == expected_players, after_refusal
        assert len(after_refusal["plans"]) == 1, after_refusal

        probe.seed(game, "DROP TRIGGER infiltrate_failure_receipt_ignore")
        probe.clear_cooldowns()
        recovered = await probe.command("潜入洞府", "潜入洞府 诊断对手", number=970906,
                                        allow_exception=True)
        assert "潜入失败" in recovered and "损失灵石" in recovered, recovered
        assert probe.rows(game, "SELECT stone FROM user_xiuxian WHERE user_id='900001'") == [(950000,)]
        assert probe.rows(player,
                          "SELECT infiltrate_active_count FROM dongfu_status WHERE user_id='900001'") == [(1,)]
        assert probe.rows(player,
                          "SELECT intrude_count,patrol_guard FROM dongfu_status WHERE user_id='900002'") == [(1, 0)]
        committed = _database_projection(probe)
        probe.clear_cooldowns()
        replay = await probe.command("潜入洞府", "潜入洞府 诊断对手", number=970906,
                                     allow_exception=True)
        assert "已经处理" in replay, replay
        assert _database_projection(probe) == committed


async def plan_write_refusal(probe):
    from nonebot_plugin_xiuxian_2.core.player_reasons import reason_text

    module = _seed(probe)
    game = probe.context.paths.game_db
    reward_id = next(item_id for item_id in module.HERB_LOW
                     if module.items.get_data_by_item_id(item_id))
    reward = module.items.get_data_by_item_id(reward_id)
    before = _business(probe)
    probe.seed(game, "CREATE TRIGGER infiltrate_plan_ignore BEFORE INSERT ON "
               "dongfu_infiltration_operations BEGIN SELECT RAISE(IGNORE); END")
    with (patch.object(module.runtime_clock, "now", return_value=datetime(2026, 11, 22, 12, tzinfo=timezone.utc)),
          patch.object(module.runtime_random, "random", return_value=0.99),
          patch.object(module.runtime_random, "choice", side_effect=lambda rows: rows[0]),
          patch.object(module.runtime_random, "shuffle", side_effect=lambda rows: None),
          patch.object(module.runtime_random, "randint", return_value=20),
          patch.object(module, "_get_geomancy", return_value={}),
          patch.object(module, "_roll_harvest",
                       return_value=[(reward_id, reward["name"], reward["type"], 2)]) as roll):
        refused = await probe.command("潜入洞府", "潜入洞府 诊断对手", number=970907,
                                      allow_exception=True)
        call = probe.calls[-1]
        after_refusal = _business(probe)
        reason = "dongfu_infiltrate_plan_write_rejected"
        probe.evidence.update(reply=refused, before=before, after_refusal=after_refusal,
                              errors=call["errors"])
        assert call["errors"] == [], call
        assert reason_text(reason) in refused and "潜入结算计划" in refused and "8点体力" in refused, refused
        assert "悄然潜入" not in refused, refused
        assert after_refusal["inventory"] == before["inventory"], after_refusal
        assert after_refusal["caves"] == before["caves"], after_refusal
        assert after_refusal["receipts"] == [] and after_refusal["failure_receipts"] == [], after_refusal
        expected_players = [tuple(row) for row in before["players"]]
        expected_players[0] = (expected_players[0][0], expected_players[0][1], 92)
        assert after_refusal["players"] == expected_players and after_refusal["plans"] == [], after_refusal

        probe.seed(game, "DROP TRIGGER infiltrate_plan_ignore")
        probe.clear_cooldowns()
        recovered = await probe.command("潜入洞府", "潜入洞府 诊断对手", number=970907,
                                        allow_exception=True)
        assert "悄然潜入" in recovered and reward["name"] in recovered, recovered
        assert roll.call_count == 2, "a rejected plan was not persisted; the retry must prepare a new plan"
        committed = _database_projection(probe)
        probe.clear_cooldowns()
        replay = await probe.command("潜入洞府", "潜入洞府 诊断对手", number=970907,
                                     allow_exception=True)
        assert "已经处理" in replay and _database_projection(probe) == committed


async def success_stone_write_recovery(probe):
    from nonebot_plugin_xiuxian_2.core.player_reasons import reason_text

    module = _seed(probe)
    game, player = probe.context.paths.game_db, probe.context.paths.player_db
    slots = json.loads(probe.rows(
        player, "SELECT plant_slots FROM dongfu_status WHERE user_id='900002'"
    )[0][0])
    slots[0]["plant_finish"] = "2026-11-22 13:00:00"
    probe.seed(player,
               "UPDATE dongfu_status SET plant_slots=?,plant_finish='2026-11-22 13:00:00' "
               "WHERE user_id='900002'",
               (json.dumps(slots, ensure_ascii=False),))
    before = _business(probe)
    probe.seed(game, "CREATE TRIGGER infiltrate_stone_ignore BEFORE UPDATE ON user_xiuxian "
               "WHEN NEW.user_id='900001' AND NEW.stone!=OLD.stone "
               "BEGIN SELECT RAISE(IGNORE); END")

    def randint(low, high):
        return 30000 if low == 30000 else 8

    with (patch.object(module.runtime_clock, "now", return_value=datetime(2026, 11, 22, 12, tzinfo=timezone.utc)),
          patch.object(module.runtime_random, "random", side_effect=(0.99, 0.99, 0.99)),
          patch.object(module.runtime_random, "choice", side_effect=lambda rows: rows[0]),
          patch.object(module.runtime_random, "shuffle", side_effect=lambda rows: None),
          patch.object(module.runtime_random, "randint", side_effect=randint),
          patch.object(module, "_get_geomancy", return_value={})):
        refused = await probe.command("潜入洞府", "潜入洞府 诊断对手", number=970908,
                                      allow_exception=True)
        call = probe.calls[-1]
        after_refusal = _business(probe)
        reason = "dongfu_infiltrate_success_player_write_rejected"
        probe.evidence.update(reply=refused, before=before, after_refusal=after_refusal,
                              errors=call["errors"])
        assert call["errors"] == [], call
        assert reason_text(reason) in refused and "灵石" in refused, refused
        assert "悄然潜入" not in refused, refused
        assert after_refusal["inventory"] == before["inventory"], after_refusal
        assert after_refusal["caves"] == before["caves"], after_refusal
        assert after_refusal["receipts"] == [] and after_refusal["failure_receipts"] == [], after_refusal
        expected_players = [tuple(row) for row in before["players"]]
        expected_players[0] = (expected_players[0][0], 1000000, 92)
        assert after_refusal["players"] == expected_players and len(after_refusal["plans"]) == 1, after_refusal

        probe.seed(game, "DROP TRIGGER infiltrate_stone_ignore")
        probe.clear_cooldowns()
        recovered = await probe.command("潜入洞府", "潜入洞府 诊断对手", number=970908,
                                        allow_exception=True)
        assert "悄然潜入" in recovered and "灵石" in recovered, recovered
        assert probe.rows(game, "SELECT stone FROM user_xiuxian WHERE user_id='900001'") == [(1030000,)]
        committed = _database_projection(probe)
        probe.clear_cooldowns()
        replay = await probe.command("潜入洞府", "潜入洞府 诊断对手", number=970908,
                                     allow_exception=True)
        assert "已经处理" in replay and _database_projection(probe) == committed


async def failure_stone_projection_recovery(probe):
    from nonebot_plugin_xiuxian_2.core.player_reasons import reason_text

    module = _seed(probe)
    game, player = probe.context.paths.game_db, probe.context.paths.player_db
    slots = json.loads(probe.rows(
        player, "SELECT plant_slots FROM dongfu_status WHERE user_id='900002'"
    )[0][0])
    slots[0]["plant_finish"] = "2026-11-22 13:00:00"
    probe.seed(player,
               "UPDATE dongfu_status SET plant_slots=?,plant_finish='2026-11-22 13:00:00',patrol_guard=1 "
               "WHERE user_id='900002'",
               (json.dumps(slots, ensure_ascii=False),))
    before = _business(probe)
    probe.seed(game, "CREATE TRIGGER infiltrate_stone_rewrite AFTER UPDATE ON user_xiuxian "
               "WHEN NEW.user_id='900001' AND NEW.stone!=OLD.stone "
               "BEGIN UPDATE user_xiuxian SET stone=7 "
               "WHERE user_id=NEW.user_id; END")
    with (patch.object(module.runtime_clock, "now", return_value=datetime(2026, 11, 22, 12, tzinfo=timezone.utc)),
          patch.object(module.runtime_random, "random", side_effect=(0.0, 0.99)),
          patch.object(module.runtime_random, "choice", side_effect=lambda rows: rows[0]),
          patch.object(module.runtime_random, "shuffle", side_effect=lambda rows: None),
          patch.object(module.runtime_random, "randint", side_effect=lambda low, high: 50000),
          patch.object(module, "_get_geomancy", return_value={})):
        refused = await probe.command("潜入洞府", "潜入洞府 诊断对手", number=970909,
                                      allow_exception=True)
        call = probe.calls[-1]
        after_refusal = _business(probe)
        reason = "dongfu_infiltrate_failure_stone_projection_mismatch"
        probe.evidence.update(reply=refused, before=before, after_refusal=after_refusal,
                              errors=call["errors"])
        assert call["errors"] == [], call
        assert reason_text(reason) in refused and "灵石" in refused, refused
        assert "被当场逼退" not in refused, refused
        assert after_refusal["inventory"] == before["inventory"], after_refusal
        assert after_refusal["caves"] == before["caves"], after_refusal
        assert after_refusal["receipts"] == [] and after_refusal["failure_receipts"] == [], after_refusal
        expected_players = [tuple(row) for row in before["players"]]
        expected_players[0] = (expected_players[0][0], 1000000, 92)
        assert after_refusal["players"] == expected_players and len(after_refusal["plans"]) == 1, after_refusal

        probe.seed(game, "DROP TRIGGER infiltrate_stone_rewrite")
        probe.clear_cooldowns()
        recovered = await probe.command("潜入洞府", "潜入洞府 诊断对手", number=970909,
                                        allow_exception=True)
        assert "潜入失败" in recovered and "损失灵石" in recovered, recovered
        assert probe.rows(game, "SELECT stone FROM user_xiuxian WHERE user_id='900001'") == [(950000,)]
        assert probe.rows(player,
                          "SELECT infiltrate_active_count FROM dongfu_status WHERE user_id='900001'") == [(1,)]
        assert probe.rows(player,
                          "SELECT intrude_count,patrol_guard FROM dongfu_status WHERE user_id='900002'") == [(1, 0)]
        committed = _database_projection(probe)
        probe.clear_cooldowns()
        replay = await probe.command("潜入洞府", "潜入洞府 诊断对手", number=970909,
                                     allow_exception=True)
        assert "已经处理" in replay and _database_projection(probe) == committed


async def maturity_boundary(probe):
    module = _seed(probe)
    game, player = probe.context.paths.game_db, probe.context.paths.player_db
    reward_id = next(item_id for item_id in module.HERB_LOW
                     if module.items.get_data_by_item_id(item_id))
    reward = module.items.get_data_by_item_id(reward_id)
    now = datetime(2026, 11, 22, 12, tzinfo=timezone.utc)
    slots = json.loads(probe.rows(
        player, "SELECT plant_slots FROM dongfu_status WHERE user_id='900002'"
    )[0][0])
    slots[0]["plant_finish"] = "2026-11-22 12:00:00.000000"
    probe.seed(player, "UPDATE dongfu_status SET plant_slots=?,plant_finish=? WHERE user_id='900002'",
               (json.dumps(slots, ensure_ascii=False), slots[0]["plant_finish"]))
    with (patch.object(module.runtime_clock, "now", return_value=now),
          patch.object(module.runtime_random, "random", return_value=0.99),
          patch.object(module.runtime_random, "choice", side_effect=lambda rows: rows[0]),
          patch.object(module.runtime_random, "shuffle", side_effect=lambda rows: None),
          patch.object(module.runtime_random, "randint", return_value=20),
          patch.object(module, "_get_geomancy", return_value={}),
          patch.object(module, "_roll_harvest",
                       return_value=[(reward_id, reward["name"], reward["type"], 2)]) as mature_roll):
        exact_reply = await probe.command("潜入洞府", "潜入洞府 诊断对手", number=970910,
                                          allow_exception=True)
        assert "悄然潜入" in exact_reply and reward["name"] in exact_reply, exact_reply
        mature_roll.assert_called_once()
        assert probe.rows(game,
                          "SELECT goods_num,bind_num FROM back WHERE user_id='900001' AND goods_id=?",
                          (reward_id,)) == [(2, 2)]
        assert json.loads(probe.rows(player,
                         "SELECT plant_slots FROM dongfu_status WHERE user_id='900002'")[0][0])[0][
                             "plant_finish"] == "2026-11-22 12:20:00"
        exact = _database_projection(probe)

    _seed(probe)
    probe.seed(game, "DELETE FROM back WHERE user_id='900001'")
    probe.player("900001", stone=1000000, user_stamina=100)
    slots[0]["plant_finish"] = "2026-11-22 12:00:00.000001"
    probe.seed(player, "UPDATE dongfu_status SET plant_slots=?,plant_finish=?,patrol_guard=0 "
               "WHERE user_id='900002'",
               (json.dumps(slots, ensure_ascii=False), slots[0]["plant_finish"]))
    probe.clear_cooldowns()

    def randint(low, high):
        return 30000 if low == 30000 else 8

    with (patch.object(module.runtime_clock, "now", return_value=now),
          patch.object(module.runtime_random, "random", side_effect=(0.99, 0.99, 0.99)),
          patch.object(module.runtime_random, "choice", side_effect=lambda rows: rows[0]),
          patch.object(module.runtime_random, "shuffle", side_effect=lambda rows: None),
          patch.object(module.runtime_random, "randint", side_effect=randint),
          patch.object(module, "_get_geomancy", return_value={}),
          patch.object(module, "_roll_harvest") as unripe_roll):
        before_reply = await probe.command("潜入洞府", "潜入洞府 诊断对手", number=970911,
                                           allow_exception=True)
        unripe_roll.assert_not_called()
        assert "灵石" in before_reply and reward["name"] in before_reply, before_reply
        assert probe.rows(game, "SELECT stone FROM user_xiuxian WHERE user_id='900001'") == [(1030000,)]
        assert probe.rows(game, "SELECT goods_num FROM back WHERE user_id='900001' AND goods_id=?",
                          (reward_id,)) == [(2,)]
        assert json.loads(probe.rows(player,
                         "SELECT plant_slots FROM dongfu_status WHERE user_id='900002'")[0][0])[0][
                             "plant_finish"] == "2026-11-22 12:08:00"
        _database_projection(probe)
    probe.evidence.update(exact_maturity=exact, one_microsecond_before=True)


async def plan_projection_recovery(probe):
    from nonebot_plugin_xiuxian_2.core.player_reasons import reason_text

    module = _seed(probe)
    game, player = probe.context.paths.game_db, probe.context.paths.player_db
    reward_id = next(item_id for item_id in module.HERB_LOW
                     if module.items.get_data_by_item_id(item_id))
    reward = module.items.get_data_by_item_id(reward_id)
    before = _business(probe)
    probe.seed(game, "CREATE TRIGGER infiltrate_plan_rewrite AFTER INSERT ON "
               "dongfu_infiltration_operations BEGIN UPDATE dongfu_infiltration_operations "
               "SET payload='changed' WHERE operation_id=NEW.operation_id; END")
    with (patch.object(module.runtime_clock, "now", return_value=datetime(2026, 11, 22, 12, tzinfo=timezone.utc)),
          patch.object(module.runtime_random, "random", return_value=0.99),
          patch.object(module.runtime_random, "choice", side_effect=lambda rows: rows[0]),
          patch.object(module.runtime_random, "shuffle", side_effect=lambda rows: None),
          patch.object(module.runtime_random, "randint", return_value=20),
          patch.object(module, "_get_geomancy", return_value={}),
          patch.object(module, "_roll_harvest",
                       return_value=[(reward_id, reward["name"], reward["type"], 2)])):
        refused = await probe.command("潜入洞府", "潜入洞府 诊断对手", number=970912,
                                      allow_exception=True)
        call = probe.calls[-1]
        after_refusal = _business(probe)
        reason = "dongfu_infiltrate_plan_write_rejected"
        probe.evidence.update(reply=refused, before=before, after_refusal=after_refusal,
                              errors=call["errors"])
        assert call["errors"] == [], call
        assert reason_text(reason) in refused and "潜入结算计划" in refused and "8点体力" in refused, refused
        assert "悄然潜入" not in refused, refused
        assert after_refusal["inventory"] == before["inventory"]
        assert after_refusal["caves"] == before["caves"]
        assert after_refusal["receipts"] == [] and after_refusal["failure_receipts"] == []
        expected_players = [tuple(row) for row in before["players"]]
        expected_players[0] = (expected_players[0][0], expected_players[0][1], 92)
        assert after_refusal["players"] == expected_players and after_refusal["plans"] == []

        probe.seed(game, "DROP TRIGGER infiltrate_plan_rewrite")
        probe.clear_cooldowns()
        recovered = await probe.command("潜入洞府", "潜入洞府 诊断对手", number=970912,
                                        allow_exception=True)
        assert "悄然潜入" in recovered and reward["name"] in recovered, recovered
        committed = _database_projection(probe)
        probe.clear_cooldowns()
        replay = await probe.command("潜入洞府", "潜入洞府 诊断对手", number=970912,
                                     allow_exception=True)
        assert "已经处理" in replay and _database_projection(probe) == committed


async def abort_recovery(probe):
    module = _seed(probe)
    game, player = probe.context.paths.game_db, probe.context.paths.player_db
    reward_id = next(item_id for item_id in module.HERB_LOW
                     if module.items.get_data_by_item_id(item_id))
    reward = module.items.get_data_by_item_id(reward_id)
    before = _business(probe)
    probe.seed(player, "CREATE TRIGGER infiltrate_abort BEFORE UPDATE ON dongfu_status "
               "WHEN NEW.user_id='900002' BEGIN SELECT RAISE(ABORT,'infiltrate settlement refused'); END")
    with (patch.object(module.runtime_clock, "now", return_value=datetime(2026, 11, 22, 12, tzinfo=timezone.utc)),
          patch.object(module.runtime_random, "random", return_value=0.99),
          patch.object(module.runtime_random, "choice", side_effect=lambda rows: rows[0]),
          patch.object(module.runtime_random, "shuffle", side_effect=lambda rows: None),
          patch.object(module.runtime_random, "randint", return_value=20),
          patch.object(module, "_get_geomancy", return_value={}),
          patch.object(module, "_roll_harvest",
                       return_value=[(reward_id, reward["name"], reward["type"], 2)])):
        reply = await probe.command("潜入洞府", "潜入洞府 诊断对手", number=970914,
                                    allow_exception=True)
        call = probe.calls[-1]
        after_abort = _business(probe)
        errors = "\n".join(call["errors"])
        assert "定位号" in reply and "悄然潜入" not in reply, reply
        assert "Traceback" in errors and "sqlite3.IntegrityError: infiltrate settlement refused" in errors, call
        assert any("infiltrate settlement refused" in json.dumps(row, ensure_ascii=False)
                   for row in call["logs"]), call
        assert after_abort["inventory"] == before["inventory"]
        assert after_abort["caves"] == before["caves"]
        assert after_abort["receipts"] == [] and after_abort["failure_receipts"] == []
        expected_players = [tuple(row) for row in before["players"]]
        expected_players[0] = (expected_players[0][0], expected_players[0][1], 92)
        assert after_abort["players"] == expected_players and len(after_abort["plans"]) == 1

        probe.seed(player, "DROP TRIGGER infiltrate_abort")
        probe.clear_cooldowns()
        recovered = await probe.command("潜入洞府", "潜入洞府 诊断对手", number=970914,
                                        allow_exception=True)
        assert "悄然潜入" in recovered and reward["name"] in recovered, recovered
        committed = _database_projection(probe)
        probe.clear_cooldowns()
        replay = await probe.command("潜入洞府", "潜入洞府 诊断对手", number=970914,
                                     allow_exception=True)
        assert "已经处理" in replay and _database_projection(probe) == committed


def register(case):
    case("dongfu-infiltrate-active-success", "潜入洞府", "合法同地点目标主动潜入已成熟灵田",
         "双玩家地图位置相同/双方已建设洞府/目标1号灵田成熟/固定成功随机源",
         "核访问者奖励和次数、目标灵田/护府/次数与永久结算回执",
         group="dongfu-infiltrate")(active_success)

    @case("dongfu-infiltrate-success-receipt-recovery", "潜入洞府", "合法主动潜入遇永久成功回执INSERT OR IGNORE",
          "成熟目标灵田、固定奖励/时钟、隔离游戏与玩家数据库",
          "不伪成功、settlement整笔回滚、保留冻结计划，同消息恢复/replay",
          group="dongfu-infiltrate", outcome="recovery")
    async def success_receipt_recovery(probe):
        from nonebot_plugin_xiuxian_2.core.player_reasons import reason_text

        module = _seed(probe)
        game = probe.context.paths.game_db
        reward_id = next(item_id for item_id in module.HERB_LOW
                         if module.items.get_data_by_item_id(item_id))
        reward = module.items.get_data_by_item_id(reward_id)
        before = _business(probe)
        probe.seed(game, "CREATE TRIGGER infiltrate_receipt_ignore BEFORE INSERT ON "
                   "dongfu_infiltrate_success_operations BEGIN SELECT RAISE(IGNORE); END")
        with (patch.object(module.runtime_clock, "now", return_value=datetime(2026, 11, 22, 12, tzinfo=timezone.utc)),
              patch.object(module.runtime_random, "random", side_effect=(0.99, 0.99)),
              patch.object(module.runtime_random, "choice", side_effect=lambda rows: rows[0]),
              patch.object(module.runtime_random, "shuffle", side_effect=lambda rows: None),
              patch.object(module.runtime_random, "randint", return_value=20),
              patch.object(module, "_get_geomancy", return_value={}),
              patch.object(module, "_roll_harvest",
                           return_value=[(reward_id, reward["name"], reward["type"], 2)]) as roll):
            refused = await probe.command("潜入洞府", "潜入洞府 诊断对手", number=970902,
                                          allow_exception=True)
            call = probe.calls[-1]
            after_refusal = _business(probe)
            probe.evidence.update(reply=refused, before=before, after_refusal=after_refusal,
                                  errors=call["errors"])
            assert call["errors"] == [], call
            reason = "dongfu_infiltrate_success_receipt_write_rejected"
            assert reason_text(reason) in refused and "回执" in refused, refused
            assert "悄然潜入" not in refused, refused
            assert after_refusal["inventory"] == before["inventory"], after_refusal
            assert after_refusal["caves"] == before["caves"], after_refusal
            assert after_refusal["receipts"] == [], after_refusal
            expected_players = [tuple(row) for row in before["players"]]
            expected_players[0] = (expected_players[0][0], expected_players[0][1], 92)
            assert after_refusal["players"] == expected_players, after_refusal
            assert len(after_refusal["plans"]) == 1, after_refusal

            probe.seed(game, "DROP TRIGGER infiltrate_receipt_ignore")
            probe.clear_cooldowns()
            recovered = await probe.command("潜入洞府", "潜入洞府 诊断对手", number=970902,
                                            allow_exception=True)
            assert "悄然潜入" in recovered and reward["name"] in recovered, recovered
            assert roll.call_count == 1, "recovery must use the persisted random plan"
            committed = _database_projection(probe)
            probe.clear_cooldowns()
            replay = await probe.command("潜入洞府", "潜入洞府 诊断对手", number=970902,
                                         allow_exception=True)
            assert "已经处理" in replay, replay
            assert _database_projection(probe) == committed

    case("dongfu-infiltrate-random-failure", "随机潜入洞府", "随机别名进入失败结算并消耗护府",
         "可随机目标含未成熟灵田/护府1层，固定探测成功但突破失败随机源",
         "随机模式计数、灵石损失、目标被潜入次数/护府层和永久失败回执仅各写一次",
         group="dongfu-infiltrate", outcome="recovery")(random_failure)

    case("dongfu-infiltrate-failure-receipt-recovery", "潜入洞府", "失败结算回执INSERT OR IGNORE后恢复",
         "目标护府1层/灵田未成熟，固定触发阵法且被逼退", "失败回执拒写不扣石、不加次数、不消护府，同消息恢复",
         group="dongfu-infiltrate", outcome="recovery")(failure_receipt_recovery)

    case("dongfu-infiltrate-plan-write-refusal", "潜入洞府", "随机方案INSERT OR IGNORE",
         "合法成熟目标与固定随机源，隔离游戏库拒绝保存方案", "计划未落库时不进入成功/失败结算，修复后同消息重新准备并结算",
         group="dongfu-infiltrate", outcome="recovery")(plan_write_refusal)

    case("dongfu-infiltrate-success-stone-recovery", "潜入洞府", "未成熟灵田成功分支的灵石UPDATE拒绝",
         "可控UTC时钟、成功随机源、玩家库拒绝灵石加成写入", "保留潜入方案与具体原因，解除故障后同消息恢复/replay",
         group="dongfu-infiltrate", outcome="recovery")(success_stone_write_recovery)

    case("dongfu-infiltrate-failure-stone-projection-recovery", "潜入洞府", "失败分支灵石余额被AFTER触发器改写",
         "护府拦截且被逼退，隔离游戏库改写扣损后余额", "核最终账户投影并回滚，恢复后只扣一次并重放",
         group="dongfu-infiltrate", outcome="recovery")(failure_stone_projection_recovery)

    case("dongfu-infiltrate-maturity-boundary", "潜入洞府", "可控时钟跨过恰好成熟边界1微秒",
         "aware UTC业务钟与naive持久化时间，固定成功随机源", "恰好成熟走收获分支；晚1微秒未成熟走灵石/材料分支",
         group="dongfu-infiltrate")(maturity_boundary)

    case("dongfu-infiltrate-plan-projection-recovery", "潜入洞府", "计划INSERT后payload被AFTER触发器改写",
         "合法成熟目标与隔离游戏库写后改写", "计划投影不符时不结算，解除故障后同消息恢复/replay",
         group="dongfu-infiltrate", outcome="recovery")(plan_projection_recovery)

    case("dongfu-infiltrate-sqlite-abort-recovery", "潜入洞府", "双库结算遇SQLite ABORT",
         "目标状态UPDATE触发真实IntegrityError，消息经OneBot matcher投递",
         "完整traceback与定位号可见、资产事务回滚、冻结方案保留并同消息恢复",
         group="dongfu-infiltrate", outcome="recovery")(abort_recovery)

    @case("dongfu-infiltrate-target-rewrite-recovery", "潜入洞府", "目标防守UPDATE后被触发器改写",
          "隔离玩家库在目标状态UPDATE后将护府层改为2", "目标状态不一致时整笔回滚并恢复冻结计划",
          group="dongfu-infiltrate", outcome="recovery")
    async def target_rewrite_recovery(probe):
        await success_write_recovery(
            probe,
            trigger_database="player",
            trigger_sql=("AFTER UPDATE ON dongfu_status WHEN NEW.user_id='900002' AND NEW.patrol_guard=0 "
                         "BEGIN UPDATE dongfu_status SET patrol_guard=2 WHERE user_id=NEW.user_id; END"),
            reason="dongfu_infiltrate_success_target_projection_mismatch",
            subject="目标方",
            number=970904,
        )

    @case("dongfu-infiltrate-inventory-ignore-recovery", "潜入洞府", "潜入灵材INSERT OR IGNORE",
          "隔离游戏库拒绝写入来访方背包物品", "具体灵材原因、不伪成功、整笔回滚与同消息恢复",
          group="dongfu-infiltrate", outcome="recovery")
    async def inventory_ignore_recovery(probe):
        await success_write_recovery(
            probe,
            trigger_database="game",
            trigger_sql=("BEFORE INSERT ON back WHEN NEW.user_id='900001' "
                         "BEGIN SELECT RAISE(IGNORE); END"),
            reason="dongfu_infiltrate_success_inventory_write_rejected",
            subject="背包物品",
            number=970905,
        )
