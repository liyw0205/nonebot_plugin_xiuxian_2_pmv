"""Real planting commands, receipts and inventory in isolated databases."""

import json
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

from tests.parameter_cases_dongfu_harvest import _database_projection


def _seed(probe, *, quantity=3, bound=2, terrain="矿脉", array_level=0):
    from nonebot_plugin_xiuxian_2.features.dongfu.application import DongfuApplication
    from nonebot_plugin_xiuxian_2.features.dongfu.plant_slots import empty_plant_slot
    from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_dongfu as module

    game, player = probe.context.paths.game_db, probe.context.paths.player_db
    slots = [empty_plant_slot(index) for index in range(1, 4)]
    probe.seed(player, "INSERT INTO dongfu_status(user_id,built,plot_count,node_type,array_level,plant_slots,"
               "planting,plant_seed_id,plant_start,plant_finish) VALUES(?,1,3,?,?,?,0,0,'','') "
               "ON CONFLICT(user_id) DO UPDATE SET built=1,plot_count=3,node_type=excluded.node_type,"
               "array_level=excluded.array_level,plant_slots=excluded.plant_slots,planting=0,"
               "plant_seed_id=0,plant_start='',plant_finish=''",
               ("900001", terrain, array_level, json.dumps(slots, ensure_ascii=False)))
    probe.grant_item(21001, quantity=quantity)
    probe.seed(game, "UPDATE back SET bind_num=? WHERE user_id='900001' AND goods_id=21001", (bound,))
    module.configure_dongfu_application(DongfuApplication(game, player))
    return module, slots


def _business(probe):
    game, player = probe.context.paths.game_db, probe.context.paths.player_db
    return {
        "inventory": probe.rows(game, "SELECT * FROM back ORDER BY user_id,goods_id"),
        "cave": probe.rows(player, "SELECT * FROM dongfu_status ORDER BY user_id"),
        "receipt": probe.rows(game, "SELECT * FROM dongfu_plant_operations ORDER BY operation_id"),
    }


async def _command(probe, text, *, now=None, abort=False, **kwargs):
    from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_dongfu as module

    now = now or datetime(2026, 11, 22, 12, tzinfo=timezone.utc)
    with patch.object(module.runtime_clock, "now", return_value=now):
        reply = await probe.command("洞府种植", text, allow_exception=True, **kwargs)
    call = probe.calls[-1]
    for caught in call.get("caught_exceptions", []):
        assert caught["function"] == "_parse_dt", caught
        assert "ValueError: time data" in caught["traceback"] and "%Y-%m-%d %H:%M:%S.%f" in caught["traceback"], caught
    if abort:
        traces = "\n".join(call["errors"])
        assert "sqlite3.IntegrityError: planting refused" in traces and "Traceback" in traces, call
    else:
        assert call["errors"] == [], call
    return reply


def _assert_success(probe, slots, *, slot_no=1, minutes=60, inventory=(2, 1)):
    from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_dongfu as module

    slots[slot_no - 1].update(seed_id=21001, seed_name=module.SEED_CONFIG[21001]["name"],
                              plant_start="2026-11-22 12:00:00",
                              plant_finish=f"2026-11-22 {(12 + minutes // 60):02d}:{minutes % 60:02d}:00", fertilizer=0)
    game, player = probe.context.paths.game_db, probe.context.paths.player_db
    state = probe.rows(player, "SELECT plant_slots,planting,plant_seed_id,plant_start,plant_finish "
                       "FROM dongfu_status WHERE user_id='900001'")[0]
    assert json.loads(state[0]) == slots
    active = next(slot for slot in slots if slot["seed_id"])
    assert state[1:] == (1, active["seed_id"], active["plant_start"], active["plant_finish"])
    assert probe.rows(game, "SELECT goods_num,bind_num FROM back WHERE user_id='900001' AND goods_id=21001") == [inventory]
    assert probe.rows(game, "SELECT payload FROM dongfu_plant_operations") == [(f"900001|{slot_no}|21001",)]
    _database_projection(probe)


def register(case):
    @case("dongfu-plant-receipt-recovery", "洞府种植", "合法指定田1，永久回执INSERT IGNORE",
          "空田/青灵草种充足，隔离双库", "具体回执原因/完整回滚/原消息恢复和实际库存田地",
          group="dongfu-plant", outcome="recovery")
    async def receipt_recovery(probe):
        from nonebot_plugin_xiuxian_2.core.player_reasons import reason_text

        module, slots = _seed(probe)
        game = probe.context.paths.game_db
        before = _business(probe)
        probe.seed(game, "CREATE TRIGGER plant_fault BEFORE INSERT ON dongfu_plant_operations BEGIN SELECT RAISE(IGNORE); END")
        reply = await _command(probe, "洞府种植 1 青灵草种", number=970701)
        probe.evidence.update(reply=reply, before=before, after=_business(probe))
        assert _business(probe) == before, (reply, before, _business(probe))
        assert reason_text("dongfu_plant_receipt_write_rejected") in reply and "种植回执" in reply, reply
        probe.seed(game, "DROP TRIGGER plant_fault")
        success = await _command(probe, "洞府种植 1 青灵草种", number=970701)
        assert "已在1号灵田播种【青灵草种】" in success and "预计60分钟" in success, success
        _assert_success(probe, slots)
        committed = _database_projection(probe)
        assert "已经处理" in await _command(probe, "洞府种植 1 青灵草种", number=970701)
        assert _database_projection(probe) == committed

    @case("dongfu-plant-binding", "洞府种植", "最后一份绑定种子",
          "空田/青灵草种数量1绑定1", "库存绑定均归零，实际种植与回执一致", group="dongfu-plant")
    async def binding(probe):
        module, slots = _seed(probe, quantity=1, bound=1)
        reply = await _command(probe, "洞府种植 青灵草种", number=970702)
        assert "已在1号灵田播种【青灵草种】" in reply, reply
        _assert_success(probe, slots, inventory=(0, 0))

    @case("dongfu-plant-auto-replay", "洞府种植", "自动选田成功后原消息重放和改参",
          "三空田/种子无绑定，原请求未指定田号", "回执固定原目标，不因下一空田而冲突，换田/换种拒绝且零写",
          group="dongfu-plant")
    async def auto_replay(probe):
        from nonebot_plugin_xiuxian_2.core.player_reasons import reason_text

        module, slots = _seed(probe, bound=0)
        reply = await _command(probe, "洞府种植 青灵草种", number=970703)
        assert "已在1号灵田播种" in reply, reply
        _assert_success(probe, slots, inventory=(2, 0))
        committed = _database_projection(probe)
        assert "已经处理" in await _command(probe, "洞府种植 青灵草种", number=970703, event_type="private")
        assert _database_projection(probe) == committed
        for text in ("洞府种植 2 青灵草种", "洞府种植 1 玄木灵种"):
            reply = await _command(probe, text, number=970703)
            assert reason_text("operation_conflict") in reply, reply
            assert _database_projection(probe) == committed
        for slot in slots:
            slot.update(slots[0], slot=slot["slot"])
        probe.seed(probe.context.paths.player_db, "UPDATE dongfu_status SET plant_slots=? WHERE user_id='900001'",
                   (json.dumps(slots, ensure_ascii=False),))
        full = _database_projection(probe)
        assert "已经处理" in await _command(probe, "洞府种植 青灵草种", number=970703)
        assert _database_projection(probe) == full
        before_full_request = _business(probe)
        assert "灵田已全部播种" in await _command(probe, "洞府种植 青灵草种", number=970706)
        assert _business(probe) == before_full_request
        cleared = [module.empty_plant_slot(index) for index in range(1, 4)]
        probe.seed(probe.context.paths.player_db, "UPDATE dongfu_status SET plant_slots=?,planting=0,"
                   "plant_seed_id=0,plant_start='',plant_finish='' WHERE user_id='900001'",
                   (json.dumps(cleared, ensure_ascii=False),))
        empty = _database_projection(probe)
        assert "已经处理" in await _command(probe, "洞府种植 青灵草种", number=970703)
        assert _database_projection(probe) == empty
        probe.seed(probe.context.paths.game_db, "UPDATE dongfu_plant_operations SET payload='broken'")
        corrupted = _database_projection(probe)
        invalid = await _command(probe, "洞府种植 青灵草种", number=970703)
        assert reason_text("dongfu_plant_receipt_invalid") in invalid, invalid
        assert "已经处理" not in invalid and "种植回执" in invalid, invalid
        assert _database_projection(probe) == corrupted

    @case("dongfu-plant-clock-offset", "洞府种植", "同一业务时刻的UTC+8时钟",
          "空田和合法种子，业务钟20:00+08等于12:00UTC", "持久化12:00到13:00且显示剩余60分钟",
          group="dongfu-plant")
    async def clock_offset(probe):
        module, slots = _seed(probe, bound=0)
        reply = await _command(probe, "洞府种植 1 青灵草种", number=970704,
                               now=datetime(2026, 11, 22, 20, tzinfo=timezone(timedelta(hours=8))))
        probe.evidence.update(reply=reply, business=_business(probe))
        assert "剩余60分钟" in reply, reply
        _assert_success(probe, slots, inventory=(2, 0))

    faults = (
        ("inventory-ignore", "game", "BEFORE UPDATE ON back BEGIN SELECT RAISE(IGNORE); END", "inventory_write_rejected", "青灵草种"),
        ("inventory-rewrite", "game", "AFTER UPDATE ON back BEGIN UPDATE back SET bind_num=99; END", "inventory_projection_mismatch", "青灵草种"),
        ("state-ignore", "player", "BEFORE UPDATE ON dongfu_status BEGIN SELECT RAISE(IGNORE); END", "state_write_rejected", "1号灵田"),
        ("state-rewrite", "player", "AFTER UPDATE ON dongfu_status BEGIN UPDATE dongfu_status SET plant_finish='lost'; END", "state_projection_mismatch", "1号灵田"),
        ("receipt-rewrite", "game", "AFTER INSERT ON dongfu_plant_operations BEGIN UPDATE dongfu_plant_operations SET payload='changed'; END", "receipt_projection_mismatch", "种植回执"),
        ("inventory-abort", "game", "BEFORE UPDATE ON back BEGIN SELECT RAISE(ABORT,'planting refused'); END", "", ""),
        ("state-abort", "player", "BEFORE UPDATE ON dongfu_status BEGIN SELECT RAISE(ABORT,'planting refused'); END", "", ""),
        ("receipt-abort", "game", "BEFORE INSERT ON dongfu_plant_operations BEGIN SELECT RAISE(ABORT,'planting refused'); END", "", ""),
    )
    for label, database, trigger, suffix, subject in faults:
        def build(database, trigger, suffix, subject):
            async def recovery(probe):
                from nonebot_plugin_xiuxian_2.core.player_reasons import reason_text

                module, slots = _seed(probe)
                target = probe.context.paths.game_db if database == "game" else probe.context.paths.player_db
                before = _business(probe)
                probe.seed(target, "CREATE TRIGGER plant_fault " + trigger)
                reply = await _command(probe, "洞府种植 1 青灵草种", number=970710, abort=not suffix)
                assert _business(probe) == before, (reply, before, _business(probe))
                if suffix:
                    assert reason_text("dongfu_plant_" + suffix) in reply and subject in reply, reply
                else:
                    assert "定位号" in reply and "planting refused" not in reply, reply
                assert "已在1号灵田播种" not in reply, reply
                probe.seed(target, "DROP TRIGGER plant_fault")
                success = await _command(probe, "洞府种植 1 青灵草种", number=970710)
                assert "已在1号灵田播种" in success and "预计60分钟" in success, success
                _assert_success(probe, slots)
                committed = _database_projection(probe)
                assert "已经处理" in await _command(probe, "洞府种植 1 青灵草种", number=970710)
                assert _database_projection(probe) == committed
                probe.evidence.update(reason="dongfu_plant_" + suffix if suffix else "sqlite_abort", subject=subject,
                                      complete_rollback=True, recovered=True, replay_unchanged=True)
            return recovery
        case("dongfu-plant-recovery-" + label, "洞府种植", "合法指定田和种子遇" + label,
             "隔离双库真实写故障，UTC业务钟", "具体原因/完整回滚/原消息恢复/replay和实际投影",
             group="dongfu-plant", outcome="recovery")(build(database, trigger, suffix, subject))

    @case("dongfu-plant-normal-parameters", "洞府种植", "参数/占田/缺种/隔离/阵法地脉",
          "首田已有玄木灵种，水域阵法5级，青灵草种充足", "正常拒绝零业务写，私聊指定2号田按原公式46分钟且旧首田不变",
          group="dongfu-plant")
    async def parameters(probe):
        module, slots = _seed(probe, terrain="水域", array_level=5)
        slots[0].update(seed_id=21002, seed_name="玄木灵种", plant_start="2026-11-22 10:00:00",
                        plant_finish="2026-11-22 15:00:00", fertilizer=1)
        probe.seed(probe.context.paths.player_db, "UPDATE dongfu_status SET plant_slots=?,planting=1,"
                   "plant_seed_id=21002,plant_start=?,plant_finish=? WHERE user_id='900001'",
                   (json.dumps(slots, ensure_ascii=False), slots[0]["plant_start"], slots[0]["plant_finish"]))
        for index, (text, expected) in enumerate((
            ("洞府种植", "请指定种子名"), ("洞府种植 2", "请指定种子名"),
            ("洞府种植 two", "未识别种子"), ("洞府种植 2 青灵草种 多余", "未识别种子"),
            ("洞府种植 0 青灵草种", "灵田编号只能是1到3"),
            ("洞府种植 4 青灵草种", "灵田编号只能是1到3"),
            ("洞府种植 1 青灵草种", "已有种植"), ("洞府种植 2 玄木灵种", "你没有【玄木灵种】"),
        )):
            before = _business(probe)
            reply = await _command(probe, text, number=970720 + index)
            assert expected in reply, reply
            assert _business(probe) == before
        before = _business(probe)
        assert "尚未建设洞府" in await _command(probe, "洞府种植 青灵草种", actor="900002", number=970728)
        assert _business(probe) == before
        success = await _command(probe, "洞府种植 2 青灵草种", number=970729, event_type="private")
        assert "已在2号灵田播种" in success and "预计46分钟" in success and "剩余46分钟" in success, success
        _assert_success(probe, slots, slot_no=2, minutes=46)

    @case("dongfu-plant-send-recovery", "洞府种植", "成功提交后发送失败及重放",
          "空田和合法种子，transport发送异常", "提交记录保留且不冒称撤销；重放不重复扣种",
          group="dongfu-plant", outcome="recovery")
    async def send_recovery(probe):
        module, slots = _seed(probe)
        with (patch.object(module.runtime_clock, "now", return_value=datetime(2026, 11, 22, 12, tzinfo=timezone.utc)),
              patch.object(module, "handle_send", AsyncMock(side_effect=RuntimeError("isolated planting delivery failure")))):
            reply = await probe.command("洞府种植", "洞府种植 青灵草种", number=970750, allow_exception=True)
        assert "isolated planting delivery failure" in "\n".join(probe.calls[-1]["errors"]), probe.calls[-1]
        assert "定位号" in reply and "撤销" not in reply, reply
        _assert_success(probe, slots)
        committed = _database_projection(probe)
        assert "已经处理" in await _command(probe, "洞府种植 青灵草种", number=970750)
        assert _database_projection(probe) == committed

    @case("dongfu-plant-all-seeds", "洞府种植", "四种真实种子名称与各自成熟分钟",
          "已扩建四块空田，各种绑定种子一份，普通地形无阵法", "逐一扣种及绑定、种植ID/时间/旧首田/四回执精确一致",
          group="dongfu-plant")
    async def all_seeds(probe):
        module, slots = _seed(probe, quantity=1, bound=1)
        slots.append(module.empty_plant_slot(4))
        game, player = probe.context.paths.game_db, probe.context.paths.player_db
        probe.seed(player, "UPDATE dongfu_status SET plot_count=4,plant_slots=? WHERE user_id='900001'",
                   (json.dumps(slots, ensure_ascii=False),))
        expected_receipts = []
        now = datetime(2026, 11, 22, 12)
        for index, (seed_id, config) in enumerate(module.SEED_CONFIG.items(), 1):
            if seed_id != 21001:
                probe.grant_item(seed_id, quantity=1)
                probe.seed(game, "UPDATE back SET bind_num=1 WHERE user_id='900001' AND goods_id=?", (seed_id,))
            success = await _command(probe, f"洞府种植 {index} {config['name']}", number=970760 + index)
            assert f"已在{index}号灵田播种【{config['name']}】" in success, success
            assert f"预计{config['minutes']}分钟" in success, success
            slots[index - 1].update(seed_id=seed_id, seed_name=config["name"], plant_start=now.strftime("%Y-%m-%d %H:%M:%S"),
                                     plant_finish=(now + timedelta(minutes=config["minutes"])).strftime("%Y-%m-%d %H:%M:%S"))
            state = probe.rows(player, "SELECT plant_slots,planting,plant_seed_id,plant_start,plant_finish "
                               "FROM dongfu_status WHERE user_id='900001'")[0]
            assert json.loads(state[0]) == slots
            assert state[1:] == (1, 21001, slots[0]["plant_start"], slots[0]["plant_finish"])
            assert probe.rows(game, "SELECT goods_num,bind_num FROM back WHERE user_id='900001' AND goods_id=?", (seed_id,)) == [(0, 0)]
            expected_receipts.append((f"900001|{index}|{seed_id}",))
            assert probe.rows(game, "SELECT payload FROM dongfu_plant_operations ORDER BY operation_id") == expected_receipts
            _database_projection(probe)
