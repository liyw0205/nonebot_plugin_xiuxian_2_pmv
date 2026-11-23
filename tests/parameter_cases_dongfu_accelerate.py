"""Real cave acceleration commands against isolated migrated databases."""

import json
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

from tests.parameter_cases_dongfu_harvest import _database_projection


def _seed(probe, *, quantity=3, bound=2, finish="2026-11-22 14:00:00", terrain="矿脉"):
    from nonebot_plugin_xiuxian_2.features.dongfu.application import DongfuApplication
    from nonebot_plugin_xiuxian_2.features.dongfu.plant_slots import empty_plant_slot
    from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_dongfu as module

    player, game = probe.context.paths.player_db, probe.context.paths.game_db
    slots = [empty_plant_slot(index) for index in range(1, 4)]
    slots[0].update(seed_id=21001, seed_name=module.SEED_CONFIG[21001]["name"],
                    plant_start="2026-11-22 10:00:00", plant_finish=finish)
    slots[1].update(slots[0], slot=2)
    probe.seed(player, "INSERT INTO dongfu_status(user_id,built,plot_count,node_type,plant_slots,planting,"
               "plant_seed_id,plant_start,plant_finish) VALUES(?,1,3,?,?,1,21001,?,?) "
               "ON CONFLICT(user_id) DO UPDATE SET built=1,plot_count=3,node_type=excluded.node_type,"
               "plant_slots=excluded.plant_slots,planting=1,plant_seed_id=21001,"
               "plant_start=excluded.plant_start,plant_finish=excluded.plant_finish",
               ("900001", terrain, json.dumps(slots, ensure_ascii=False), slots[0]["plant_start"], finish))
    probe.grant_item(module.DONGFU_ITEM_ACCELERATE, quantity=quantity)
    probe.seed(game, "UPDATE back SET bind_num=? WHERE user_id=? AND goods_id=?",
               (bound, "900001", module.DONGFU_ITEM_ACCELERATE))
    module.configure_dongfu_application(DongfuApplication(game, player))
    return module, slots


def _business(probe):
    game, player = probe.context.paths.game_db, probe.context.paths.player_db
    return {
        "inventory": probe.rows(game, "SELECT * FROM back ORDER BY user_id,goods_id"),
        "cave": probe.rows(player, "SELECT * FROM dongfu_status ORDER BY user_id"),
        "receipt": probe.rows(game, "SELECT * FROM dongfu_accelerate_operations ORDER BY operation_id"),
    }


async def _command(probe, text, *, now=None, abort=False, **kwargs):
    from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_dongfu as module

    now = now or datetime(2026, 11, 22, 12, tzinfo=timezone.utc)
    with patch.object(module.runtime_clock, "now", return_value=now):
        reply = await probe.command("洞府催熟", text, allow_exception=True, **kwargs)
    call = probe.calls[-1]
    for caught in call.get("caught_exceptions", []):
        assert caught["function"] == "_parse_dt", caught
        assert "ValueError: time data" in caught["traceback"] and "%Y-%m-%d %H:%M:%S.%f" in caught["traceback"], caught
    if abort:
        traces = "\n".join(call["errors"])
        assert "sqlite3.IntegrityError: acceleration refused" in traces and "Traceback" in traces, call
    else:
        assert call["errors"] == [], call
    return reply


def _assert_success(probe, slots, *, slot_no=1, finish="2026-11-22 13:00:00", inventory=(2, 1)):
    slots[slot_no - 1]["plant_finish"] = finish
    game, player = probe.context.paths.game_db, probe.context.paths.player_db
    state = probe.rows(player, "SELECT plant_slots,planting,plant_seed_id,plant_start,plant_finish "
                       "FROM dongfu_status WHERE user_id='900001'")[0]
    assert json.loads(state[0]) == slots
    assert state[1:] == (1, 21001, slots[0]["plant_start"], slots[0]["plant_finish"])
    assert probe.rows(game, "SELECT goods_num,bind_num FROM back WHERE user_id='900001' AND goods_id=21005") == [inventory]
    assert probe.rows(game, "SELECT payload FROM dongfu_accelerate_operations") == [(f"900001|{slot_no}|21005",)]
    _database_projection(probe)


def register(case):
    @case("dongfu-accelerate-receipt-recovery", "洞府催熟", "合法编号1，回执INSERT IGNORE后原消息恢复",
          "在田作物/灵息露充足，隔离双库及UTC业务钟", "具体回执原因、完整回滚、实际60分钟缩短和恢复/replay",
          group="dongfu-accelerate", outcome="recovery")
    async def receipt_recovery(probe):
        from nonebot_plugin_xiuxian_2.core.player_reasons import reason_text

        module, slots = _seed(probe)
        game = probe.context.paths.game_db
        before = _business(probe)
        probe.seed(game, "CREATE TRIGGER accelerate_fault BEFORE INSERT ON dongfu_accelerate_operations "
                   "BEGIN SELECT RAISE(IGNORE); END")
        reply = await _command(probe, "洞府催熟 1", number=970601)
        probe.evidence.update(reply=reply, before=before, after=_business(probe))
        assert _business(probe) == before, (reply, before, _business(probe))
        assert reason_text("dongfu_accelerate_receipt_write_rejected") in reply and "催熟回执" in reply, reply
        probe.seed(game, "DROP TRIGGER accelerate_fault")
        success = await _command(probe, "洞府催熟 1", number=970601)
        assert "已使用【灵息露】催熟1号灵田" in success and "缩短60分钟" in success, success
        _assert_success(probe, slots)
        committed = _database_projection(probe)
        assert "已经处理" in await _command(probe, "洞府催熟 1", number=970601)
        assert _database_projection(probe) == committed

    @case("dongfu-accelerate-binding", "洞府催熟", "最后一份绑定灵息露",
          "数量1绑定1，未成熟灵田", "库存和绑定均归零，田地与旧字段一致，永久回执存在", group="dongfu-accelerate")
    async def binding(probe):
        module, slots = _seed(probe, quantity=1, bound=1)
        reply = await _command(probe, "洞府催熟 1", number=970602)
        assert "已使用【灵息露】催熟1号灵田" in reply, reply
        _assert_success(probe, slots, inventory=(0, 0))

    @case("dongfu-accelerate-mature-replay", "洞府催熟", "催熟至当前时刻后同消息重放/换田",
          "作物距成熟30分钟，原消息成功后清田", "回执优先，成熟/空田不隐藏成功；换田冲突零副作用",
          group="dongfu-accelerate")
    async def replay(probe):
        from nonebot_plugin_xiuxian_2.core.player_reasons import reason_text

        module, slots = _seed(probe, bound=0, finish="2026-11-22 12:30:00")
        success = await _command(probe, "洞府催熟 1", number=970603)
        assert "可收获" in success, success
        _assert_success(probe, slots, finish="2026-11-22 12:00:00", inventory=(2, 0))
        committed = _database_projection(probe)
        assert "已经处理" in await _command(probe, "洞府催熟 1", number=970603, event_type="private")
        assert _database_projection(probe) == committed
        conflict = await _command(probe, "洞府催熟 2", number=970603)
        assert reason_text("operation_conflict") in conflict, conflict
        assert _database_projection(probe) == committed
        slots[0].update(seed_id=0, seed_name="", plant_start="", plant_finish="", fertilizer=0)
        probe.seed(probe.context.paths.player_db, "UPDATE dongfu_status SET plant_slots=? WHERE user_id='900001'",
                   (json.dumps(slots, ensure_ascii=False),))
        cleared = _database_projection(probe)
        assert "已经处理" in await _command(probe, "洞府催熟 1", number=970603)
        assert _database_projection(probe) == cleared

    faults = (
        ("inventory-ignore", "game", "BEFORE UPDATE ON back BEGIN SELECT RAISE(IGNORE); END", "inventory_write_rejected", "灵息露"),
        ("inventory-rewrite", "game", "AFTER UPDATE ON back BEGIN UPDATE back SET bind_num=99; END", "inventory_projection_mismatch", "灵息露"),
        ("state-ignore", "player", "BEFORE UPDATE ON dongfu_status BEGIN SELECT RAISE(IGNORE); END", "state_write_rejected", "1号灵田"),
        ("state-rewrite", "player", "AFTER UPDATE ON dongfu_status BEGIN UPDATE dongfu_status SET plant_finish='lost'; END", "state_projection_mismatch", "1号灵田"),
        ("receipt-rewrite", "game", "AFTER INSERT ON dongfu_accelerate_operations BEGIN UPDATE dongfu_accelerate_operations SET payload='changed'; END", "receipt_projection_mismatch", "催熟回执"),
        ("inventory-abort", "game", "BEFORE UPDATE ON back BEGIN SELECT RAISE(ABORT,'acceleration refused'); END", "", ""),
        ("state-abort", "player", "BEFORE UPDATE ON dongfu_status BEGIN SELECT RAISE(ABORT,'acceleration refused'); END", "", ""),
        ("receipt-abort", "game", "BEFORE INSERT ON dongfu_accelerate_operations BEGIN SELECT RAISE(ABORT,'acceleration refused'); END", "", ""),
    )
    for label, database, trigger, suffix, subject in faults:
        def build(database, trigger, suffix, subject):
            async def recovery(probe):
                from nonebot_plugin_xiuxian_2.core.player_reasons import reason_text

                module, slots = _seed(probe)
                target = probe.context.paths.game_db if database == "game" else probe.context.paths.player_db
                before = _business(probe)
                probe.seed(target, "CREATE TRIGGER accelerate_fault " + trigger)
                reply = await _command(probe, "洞府催熟 1", number=970610, abort=not suffix)
                assert _business(probe) == before, (reply, before, _business(probe))
                if suffix:
                    assert reason_text("dongfu_accelerate_" + suffix) in reply and subject in reply, reply
                else:
                    assert "定位号" in reply and "acceleration refused" not in reply, reply
                assert "已使用【灵息露】催熟" not in reply, reply
                probe.seed(target, "DROP TRIGGER accelerate_fault")
                success = await _command(probe, "洞府催熟 1", number=970610)
                assert "缩短60分钟" in success and "剩余60分钟" in success, success
                _assert_success(probe, slots)
                committed = _database_projection(probe)
                assert "已经处理" in await _command(probe, "洞府催熟 1", number=970610)
                assert _database_projection(probe) == committed
                probe.evidence.update(reason="dongfu_accelerate_" + suffix if suffix else "sqlite_abort",
                                      subject=subject, complete_rollback=True, recovered=True, replay_unchanged=True)
            return recovery
        case("dongfu-accelerate-recovery-" + label, "洞府催熟", "合法编号1遇" + label,
             "真实双库写故障及UTC业务钟", "具体原因/完整回滚/原消息恢复与replay/实际投影",
             group="dongfu-accelerate", outcome="recovery")(build(database, trigger, suffix, subject))

    @case("dongfu-accelerate-normal-parameters", "洞府催熟", "真实参数/空田/成熟/无材料/玩家隔离",
          "三块田/真实灵息露，私聊水域地脉", "正常拒绝零业务写，水域缩短80分钟且旧首田字段不变",
          group="dongfu-accelerate")
    async def parameters(probe):
        module, slots = _seed(probe, terrain="水域")
        for index, (text, expected) in enumerate((
            ("洞府催熟", "请指定灵田编号"), ("洞府催熟 two", "请指定灵田编号"),
            ("洞府催熟 1 2", "请指定灵田编号"), ("洞府催熟 0", "灵田编号只能是1到3"),
            ("洞府催熟 4", "灵田编号只能是1到3"), ("洞府催熟 3", "当前空闲"),
        )):
            before = _business(probe)
            reply = await _command(probe, text, number=970620 + index)
            assert expected in reply, reply
            assert _business(probe) == before
        before = _business(probe)
        assert "已成熟" in await _command(probe, "洞府催熟 1", number=970626,
                                         now=datetime(2026, 11, 22, 14, tzinfo=timezone.utc))
        assert _business(probe) == before
        assert "尚未建设洞府" in await _command(probe, "洞府催熟 1", actor="900002", number=970627)
        assert _business(probe) == before
        game = probe.context.paths.game_db
        probe.seed(game, "UPDATE back SET goods_num=0,bind_num=0 WHERE user_id='900001' AND goods_id=21005")
        before = _business(probe)
        assert "你没有【灵息露】" in await _command(probe, "洞府催熟 2", number=970628)
        assert _business(probe) == before
        probe.seed(game, "UPDATE back SET goods_num=1 WHERE user_id='900001' AND goods_id=21005")
        success = await _command(probe, "洞府催熟 2", number=970629, event_type="private")
        assert "催熟2号灵田" in success and "缩短80分钟" in success and "剩余40分钟" in success, success
        _assert_success(probe, slots, slot_no=2, finish="2026-11-22 12:40:00", inventory=(0, 0))

    @case("dongfu-accelerate-time-boundaries", "洞府催熟", "UTC/UTC+8与微秒成熟边界",
          "可控业务钟和历史微秒时间", "同一时刻相同判断，恰好成熟拒绝，晚1微秒可催熟且不丢业务精度",
          group="dongfu-accelerate")
    async def time_boundaries(probe):
        module, slots = _seed(probe, quantity=4, bound=0, finish="2026-11-22 12:00:00.500000")
        instant = datetime(2026, 11, 22, 12, 0, 0, 500000, tzinfo=timezone.utc)
        for index, now in enumerate((instant.replace(tzinfo=None), instant,
                                     instant.astimezone(timezone(timedelta(hours=8))))):
            before = _business(probe)
            reply = await _command(probe, "洞府催熟 1", number=970640 + index, now=now)
            assert "已成熟" in reply, reply
            assert _business(probe) == before
        slots[0]["plant_finish"] = "2026-11-22 12:00:00.500001"
        probe.seed(probe.context.paths.player_db, "UPDATE dongfu_status SET plant_slots=?,plant_finish=? WHERE user_id='900001'",
                   (json.dumps(slots, ensure_ascii=False), slots[0]["plant_finish"]))
        success = await _command(probe, "洞府催熟 1", number=970643,
                                 now=instant.astimezone(timezone(timedelta(hours=8))))
        assert "催熟1号灵田" in success and "可收获" in success, success
        _assert_success(probe, slots, finish="2026-11-22 12:00:00.500000", inventory=(3, 0))

    @case("dongfu-accelerate-send-recovery", "洞府催熟", "业务成功后发送异常与原消息重放",
          "合法田地/库存，成功回复transport抛异常", "保留已提交业务，定位号不冒称撤销，replay不重复消耗",
          group="dongfu-accelerate", outcome="recovery")
    async def send_recovery(probe):
        module, slots = _seed(probe)
        with (patch.object(module.runtime_clock, "now", return_value=datetime(2026, 11, 22, 12, tzinfo=timezone.utc)),
              patch.object(module, "handle_send", AsyncMock(side_effect=RuntimeError("isolated acceleration delivery failure")))):
            reply = await probe.command("洞府催熟", "洞府催熟 1", number=970650, allow_exception=True)
        assert "isolated acceleration delivery failure" in "\n".join(probe.calls[-1]["errors"]), probe.calls[-1]
        assert "定位号" in reply and "撤销" not in reply, reply
        _assert_success(probe, slots)
        committed = _database_projection(probe)
        assert "已经处理" in await _command(probe, "洞府催熟 1", number=970650)
        assert _database_projection(probe) == committed
