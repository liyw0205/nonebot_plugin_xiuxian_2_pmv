"""Real matcher contracts for cave fertilizer consumption and recovery."""

import json
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

from tests.parameter_cases_dongfu_harvest import _database_projection


def _seed(probe, *, fertilizer=0, quantity=3, bound=2):
    from nonebot_plugin_xiuxian_2.features.dongfu.application import DongfuApplication
    from nonebot_plugin_xiuxian_2.features.dongfu.plant_slots import empty_plant_slot
    from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_dongfu as module

    player, game = probe.context.paths.player_db, probe.context.paths.game_db
    slots = [empty_plant_slot(index) for index in range(1, 4)]
    slots[0].update(seed_id=21001, seed_name=module.SEED_CONFIG[21001]["name"],
                    plant_start="2026-11-22 10:00:00", plant_finish="2026-11-22 13:00:00",
                    fertilizer=fertilizer)
    slots[1].update(slots[0], slot=2, fertilizer=0)
    probe.seed(player, "INSERT INTO dongfu_status(user_id,built,plot_count,plant_slots,planting,"
               "plant_seed_id,plant_start,plant_finish) VALUES(?,1,3,?,1,21001,?,?) "
               "ON CONFLICT(user_id) DO UPDATE SET built=1,plot_count=3,plant_slots=excluded.plant_slots,"
               "planting=1,plant_seed_id=21001,plant_start=excluded.plant_start,plant_finish=excluded.plant_finish",
               ("900001", json.dumps(slots, ensure_ascii=False), slots[0]["plant_start"], slots[0]["plant_finish"]))
    probe.grant_item(module.DONGFU_ITEM_FERTILIZER, quantity=quantity)
    probe.seed(game, "UPDATE back SET bind_num=? WHERE user_id=? AND goods_id=?",
               (bound, "900001", module.DONGFU_ITEM_FERTILIZER))
    module.configure_dongfu_application(DongfuApplication(game, player))
    return module, slots


def _business(probe):
    game, player = probe.context.paths.game_db, probe.context.paths.player_db
    return {
        "inventory": probe.rows(game, "SELECT * FROM back ORDER BY user_id,goods_id"),
        "cave": probe.rows(player, "SELECT * FROM dongfu_status ORDER BY user_id"),
        "receipt": probe.rows(game, "SELECT * FROM dongfu_fertilize_operations ORDER BY operation_id"),
    }


async def _command(probe, text, *, abort=False, **kwargs):
    from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_dongfu as module

    with patch.object(module.runtime_clock, "now", return_value=datetime(2026, 11, 22, 12, tzinfo=timezone.utc)):
        reply = await probe.command("洞府施肥", text, allow_exception=True, **kwargs)
    call = probe.calls[-1]
    for caught in call.get("caught_exceptions", []):
        assert caught["function"] == "_parse_dt", caught
        assert "ValueError: time data" in caught["traceback"] and "%Y-%m-%d %H:%M:%S.%f" in caught["traceback"], caught
    if abort:
        traces = "\n".join(call["errors"])
        assert "sqlite3.IntegrityError: fertilizer refused" in traces and "Traceback" in traces, call
    else:
        assert call["errors"] == [], call
    return reply


def register(case):
    @case("dongfu-fertilize-receipt-recovery", "洞府施肥", "合法编号1，永久回执拒写后恢复",
          "在田作物与五色灵壤充足，隔离SQLite回执INSERT IGNORE",
          "不伪成功、整笔回滚、具体回执原因、原消息恢复/replay", group="dongfu-fertilize", outcome="recovery")
    async def receipt_recovery(probe):
        from nonebot_plugin_xiuxian_2.core.player_reasons import reason_text

        module, slots = _seed(probe)
        game = probe.context.paths.game_db
        before = _business(probe)
        probe.seed(game, "CREATE TRIGGER fertilize_fault BEFORE INSERT ON dongfu_fertilize_operations "
                   "BEGIN SELECT RAISE(IGNORE); END")
        reply = await _command(probe, "洞府施肥 1", number=970501)
        after = _business(probe)
        probe.evidence.update(reply=reply, before=before, after=after)
        assert after == before, (reply, before, after)
        assert reason_text("dongfu_fertilize_receipt_write_rejected") in reply, reply
        probe.seed(game, "DROP TRIGGER fertilize_fault")
        success = await _command(probe, "洞府施肥 1", number=970501)
        assert "已对1号灵田施肥" in success, success
        committed = _database_projection(probe)
        replay = await _command(probe, "洞府施肥 1", number=970501)
        assert "已经处理" in replay, replay
        assert _database_projection(probe) == committed

    @case("dongfu-fertilize-binding", "洞府施肥", "消耗最后一份绑定五色灵壤",
          "合法作物、背包数量1且绑定数量1", "数量与绑定均归零，肥力加1且回执存在", group="dongfu-fertilize")
    async def binding(probe):
        module, slots = _seed(probe, quantity=1, bound=1)
        reply = await _command(probe, "洞府施肥 1", number=970502)
        actual = probe.rows(probe.context.paths.game_db,
                            "SELECT goods_num,bind_num FROM back WHERE user_id=? AND goods_id=?",
                            ("900001", module.DONGFU_ITEM_FERTILIZER))
        probe.evidence.update(reply=reply, inventory=actual)
        assert "已对1号灵田施肥" in reply and actual == [(0, 0)], (reply, actual)
        stored = json.loads(probe.rows(probe.context.paths.player_db,
                            "SELECT plant_slots FROM dongfu_status WHERE user_id='900001'")[0][0])
        slots[0]["fertilizer"] = 1
        assert stored == slots
        assert len(probe.rows(probe.context.paths.game_db, "SELECT * FROM dongfu_fertilize_operations")) == 1
        _database_projection(probe)

    @case("dongfu-fertilize-full-replay", "洞府施肥", "从肥力2施肥到3后重放同一合法消息",
          "五色灵壤充足，目标田肥力2", "成功回执优先于当前满肥前置，同号换田冲突且零副作用", group="dongfu-fertilize")
    async def full_replay(probe):
        module, slots = _seed(probe, fertilizer=2)
        reply = await _command(probe, "洞府施肥 1", number=970503)
        assert "当前肥力+3" in reply, reply
        committed = _database_projection(probe)
        replay = await _command(probe, "洞府施肥 1", number=970503, event_type="private")
        probe.evidence.update(first=reply, replay=replay)
        assert "已经处理" in replay, replay
        assert _database_projection(probe) == committed
        conflict = await _command(probe, "洞府施肥 2", number=970503)
        from nonebot_plugin_xiuxian_2.core.player_reasons import reason_text
        assert reason_text("operation_conflict") in conflict, conflict
        assert _database_projection(probe) == committed

        # Harvested/replanted state must not hide a permanent successful receipt.
        slots[0].update(seed_id=0, seed_name="", plant_start="", plant_finish="", fertilizer=0)
        probe.seed(probe.context.paths.player_db, "UPDATE dongfu_status SET plant_slots=? WHERE user_id='900001'",
                   (json.dumps(slots, ensure_ascii=False),))
        cleared = _database_projection(probe)
        replay = await _command(probe, "洞府施肥 1", number=970503)
        assert "已经处理" in replay, replay
        assert _database_projection(probe) == cleared

    faults = (
        ("inventory-ignore", "game", "BEFORE UPDATE ON back BEGIN SELECT RAISE(IGNORE); END", "inventory_write_rejected", "五色灵壤"),
        ("inventory-rewrite", "game", "AFTER UPDATE ON back BEGIN UPDATE back SET bind_num=99; END", "inventory_projection_mismatch", "五色灵壤"),
        ("state-ignore", "player", "BEFORE UPDATE ON dongfu_status BEGIN SELECT RAISE(IGNORE); END", "state_write_rejected", "1号灵田"),
        ("state-rewrite", "player", "AFTER UPDATE ON dongfu_status BEGIN UPDATE dongfu_status SET plant_slots='[]'; END", "state_projection_mismatch", "1号灵田"),
        ("receipt-rewrite", "game", "AFTER INSERT ON dongfu_fertilize_operations BEGIN UPDATE dongfu_fertilize_operations SET payload='changed'; END", "receipt_projection_mismatch", "施肥回执"),
        ("inventory-abort", "game", "BEFORE UPDATE ON back BEGIN SELECT RAISE(ABORT,'fertilizer refused'); END", "", ""),
        ("state-abort", "player", "BEFORE UPDATE ON dongfu_status BEGIN SELECT RAISE(ABORT,'fertilizer refused'); END", "", ""),
        ("receipt-abort", "game", "BEFORE INSERT ON dongfu_fertilize_operations BEGIN SELECT RAISE(ABORT,'fertilizer refused'); END", "", ""),
    )
    for label, database, trigger, suffix, subject in faults:
        def build(database, trigger, suffix, subject):
            async def recovery(probe):
                from nonebot_plugin_xiuxian_2.core.player_reasons import reason_text

                module, slots = _seed(probe)
                game, player = probe.context.paths.game_db, probe.context.paths.player_db
                target = game if database == "game" else player
                before = _business(probe)
                probe.seed(target, "CREATE TRIGGER fertilize_fault " + trigger)
                reply = await _command(probe, "洞府施肥 1", number=970510, abort=not suffix)
                assert _business(probe) == before, (reply, before, _business(probe))
                if suffix:
                    assert reason_text("dongfu_fertilize_" + suffix) in reply and subject in reply, reply
                    assert "已对1号灵田施肥" not in reply, reply
                else:
                    assert "定位号" in reply and "fertilizer refused" not in reply, reply
                probe.seed(target, "DROP TRIGGER fertilize_fault")
                success = await _command(probe, "洞府施肥 1", number=970510)
                assert "已对1号灵田施肥" in success and "剩余60分钟" in success, success
                slots[0]["fertilizer"] = 1
                assert json.loads(probe.rows(player, "SELECT plant_slots FROM dongfu_status WHERE user_id='900001'")[0][0]) == slots
                assert probe.rows(game, "SELECT goods_num,bind_num FROM back WHERE user_id=? AND goods_id=?",
                                  ("900001", module.DONGFU_ITEM_FERTILIZER)) == [(2, 1)]
                committed = _database_projection(probe)
                replay = await _command(probe, "洞府施肥 1", number=970510)
                assert "已经处理" in replay, replay
                assert _database_projection(probe) == committed
                probe.evidence.update(reason="dongfu_fertilize_" + suffix if suffix else "sqlite_abort",
                                      subject=subject, complete_rollback=True, recovered=True, replay_unchanged=True)
            return recovery
        case("dongfu-fertilize-recovery-" + label, "洞府施肥", "合法编号1遇" + label,
             "隔离双库实际写故障与可控UTC时钟", "具体原因、完整回滚、原消息恢复/replay及实际绑定数",
             group="dongfu-fertilize", outcome="recovery")(build(database, trigger, suffix, subject))

    @case("dongfu-fertilize-normal-parameters", "洞府施肥", "真实编号/参数/空田/满肥/无物品/玩家隔离",
          "三块灵田、两种正常在田作物、真实五色灵壤", "正常拒绝零业务写、私聊指定田成功，既有规则不重构", group="dongfu-fertilize")
    async def parameters(probe):
        module, slots = _seed(probe, fertilizer=3)
        for index, (text, expected) in enumerate((
            ("洞府施肥", "请指定灵田编号"), ("洞府施肥 two", "请指定灵田编号"),
            ("洞府施肥 1 2", "请指定灵田编号"), ("洞府施肥 0", "灵田编号只能是1到3"),
            ("洞府施肥 4", "灵田编号只能是1到3"), ("洞府施肥 3", "当前空闲"),
            ("洞府施肥 1", "肥力已满"),
        )):
            before = _business(probe)
            reply = await _command(probe, text, number=970520 + index)
            assert expected in reply, reply
            assert _business(probe) == before
        game = probe.context.paths.game_db
        probe.seed(game, "UPDATE back SET goods_num=0,bind_num=0 WHERE user_id=? AND goods_id=?",
                   ("900001", module.DONGFU_ITEM_FERTILIZER))
        before = _business(probe)
        missing = await _command(probe, "洞府施肥 2", number=970527)
        assert "你没有【五色灵壤】" in missing, missing
        assert _business(probe) == before
        other = await _command(probe, "洞府施肥 2", actor="900002", number=970528)
        assert "尚未建设洞府" in other, other
        assert _business(probe) == before
        probe.seed(game, "UPDATE back SET goods_num=1 WHERE user_id=? AND goods_id=?",
                   ("900001", module.DONGFU_ITEM_FERTILIZER))
        success = await _command(probe, "洞府施肥 2", number=970529, event_type="private")
        assert "已对2号灵田施肥，当前肥力+1" in success, success
        slots[1]["fertilizer"] = 1
        assert json.loads(probe.rows(probe.context.paths.player_db,
                         "SELECT plant_slots FROM dongfu_status WHERE user_id='900001'")[0][0]) == slots
        assert probe.rows(game, "SELECT goods_num,bind_num FROM back WHERE user_id=? AND goods_id=?",
                          ("900001", module.DONGFU_ITEM_FERTILIZER)) == [(0, 0)]
        _database_projection(probe)

    @case("dongfu-fertilize-send-recovery", "洞府施肥", "成功提交后消息发送失败再重放",
          "合法田地/库存，成功回复transport抛异常", "发送失败不宣称业务回滚；回执可重放且不再次消耗", group="dongfu-fertilize", outcome="recovery")
    async def send_recovery(probe):
        module, slots = _seed(probe)
        with patch.object(module, "handle_send", AsyncMock(side_effect=RuntimeError("isolated fertilizer delivery failure"))):
            reply = await probe.command("洞府施肥", "洞府施肥 1", number=970530, allow_exception=True)
        call = probe.calls[-1]
        assert "isolated fertilizer delivery failure" in "\n".join(call["errors"]), call
        assert "定位号" in reply and "撤销" not in reply, reply
        assert probe.rows(probe.context.paths.game_db, "SELECT goods_num,bind_num FROM back WHERE user_id=? AND goods_id=?",
                          ("900001", module.DONGFU_ITEM_FERTILIZER)) == [(2, 1)]
        assert len(probe.rows(probe.context.paths.game_db, "SELECT * FROM dongfu_fertilize_operations")) == 1
        committed = _database_projection(probe)
        replay = await _command(probe, "洞府施肥 1", number=970530)
        assert "已经处理" in replay, replay
        assert _database_projection(probe) == committed
