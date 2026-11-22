"""Isolated, registered OneBot exploration settlement cases."""

import json
import hashlib
from datetime import datetime, timedelta
from random import Random
from types import SimpleNamespace
from unittest.mock import patch

from tests.parameter_cases_map_explore_start import _command, _integrity, _setup


ACTOR = "900001"
NOW = datetime(2026, 11, 22, 14, 0)


def _prepare(probe, marker):
    from nonebot_plugin_xiuxian_2.features.map.migrations import apply_map_explore_settlement
    from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork

    module, _, _, _, node = _setup(probe)
    with DatabaseUnitOfWork(probe.context.paths.game_db) as uow:
        apply_map_explore_settlement(uow)
    probe.player(stone=10000, user_stamina=87)
    daily = {"date": "2026-11-22", "explore_count": 0, "resource_total_count": 0}
    module.map_application.save_daily_limit(ACTOR, daily)
    start_time = f"2026-11-22 13:00:{marker:02d}"
    state = {
        "running": 1, "node_type": node["type"], "node_name": node["name"],
        "start_time": start_time, "duration_min": 45, "max_duration_min": 20, "interval_min": 20,
        "settlement": "",
    }
    module.map_application.save_explore_status(ACTOR, state)
    return module, state, f"map-explore:{ACTOR}:{start_time}"


class RewardRandom(Random):
    def choices(self, population, weights=None, *, cum_weights=None, k=1):
        if "normal" in population:
            return ["normal"]
        return super().choices(population, weights, cum_weights=cum_weights, k=k)

    def random(self):
        return 0.1


def _projection(probe, operation_id):
    game, player = probe.context.paths.game_db, probe.context.paths.player_db
    status = probe.rows(player, "SELECT running,node_type,node_name,start_time,duration_min,settlement,max_duration_min,interval_min FROM map_explore_status WHERE user_id=?", (ACTOR,))
    receipt = probe.rows(game, "SELECT payload,stone,rewards FROM map_explore_settlement_operations WHERE operation_id=?", (operation_id,))
    start_time = str(status[0][3]) if status else ""
    settlement = str(status[0][5] or "") if status else ""
    if receipt and not settlement:
        stored = json.loads(receipt[0][0])[1]
        start_time, settlement = str(stored["start_time"]), str(stored["settlement"])
    snapshot_operation = (
        "map-explore-snapshot:" + hashlib.sha256(
            (ACTOR + start_time + settlement).encode("utf-8")
        ).hexdigest()[:32]
    ) if settlement and start_time else ""
    return {
        "wallet": probe.rows(game, "SELECT stone,user_stamina FROM user_xiuxian WHERE user_id=?", (ACTOR,)),
        "items": probe.rows(game, "SELECT goods_id,goods_num,bind_num FROM back WHERE user_id=? ORDER BY goods_id", (ACTOR,)),
        "daily": probe.rows(player, "SELECT date,explore_count,resource_total_count FROM map_daily_limit WHERE user_id=?", (ACTOR,)),
        "status": status,
        "receipt": receipt,
        "ledger": probe.rows(game, "SELECT status FROM operation_ledger WHERE operation_id=? AND action='map.explore_settle'", (operation_id,)),
        "snapshot_ledger": probe.rows(game, "SELECT status FROM operation_ledger WHERE operation_id=? AND action='map.explore_settlement_snapshot'", (snapshot_operation,)) if snapshot_operation else [],
    }


def register(case):
    @case("map-explore-settle-receipt-ignore", "探索结算", "正常无参真实命令",
          "探索已满20分钟，游戏库永久回执写拒绝",
          "不能伪成功；资产、次数、运行状态和回执回滚，保留已冻结奖励",
          group="map-explore-settle", outcome="recovery")
    async def receipt_ignore(probe):
        module, _, operation_id = _prepare(probe, 1)
        game = probe.context.paths.game_db
        before = _projection(probe, operation_id)
        probe.seed(game, "CREATE TRIGGER settle_fault BEFORE INSERT ON map_explore_settlement_operations BEGIN SELECT RAISE(IGNORE); END")
        with patch.object(module, "runtime_clock", SimpleNamespace(now=lambda: NOW)), patch.object(module, "runtime_random", RewardRandom(1)):
            reply = await _command(probe, "探索结算", number=974201)
        failed = _projection(probe, operation_id)
        probe.evidence.update(reply=reply, before=before, failed=failed)
        assert "探索结算回执" in reply and "探索结算完成" not in reply, reply
        assert "写入被拒" in reply, reply
        for key in ("wallet", "items", "daily", "receipt"):
            assert failed[key] == before[key], (key, before, failed)
        assert failed["ledger"] == [], failed
        assert failed["snapshot_ledger"] == [("applied",)], failed
        assert failed["status"][0][:5] == before["status"][0][:5], failed
        assert failed["status"][0][5] and failed["status"][0][6:] == before["status"][0][6:], failed
        probe.seed(game, "DROP TRIGGER settle_fault")
        next_day = NOW + timedelta(days=1)
        with patch.object(module, "runtime_clock", SimpleNamespace(now=lambda: next_day)):
            recovered = await _command(probe, "探索结算", number=974201)
        committed = _projection(probe, operation_id)
        assert "探索结算完成" in recovered, recovered
        assert "有效探索：20分钟（共1轮）" in recovered, recovered
        assert committed["receipt"] and committed["ledger"] == [("applied",)], committed
        assert committed["daily"] == [("2026-11-23", 1, 1)] and committed["status"][0][0] == 0, committed
        stored_payload = json.loads(committed["receipt"][0][0])
        assert stored_payload[2]["date"] == "2026-11-23", stored_payload
        assert sum(int(row[1]) for row in committed["items"]) == sum(
            int(amount) for _, amount in json.loads(committed["receipt"][0][2])
        )
        assert committed["wallet"][0][0] == before["wallet"][0][0] + committed["receipt"][0][1], committed
        repeated = await _command(probe, "探索结算", number=974201)
        assert "没有进行中的探索" in repeated and _projection(probe, operation_id) == committed
        _integrity(probe)

    snapshot_faults = (
        ("snapshot-ignore", "BEFORE UPDATE OF settlement ON map_explore_status WHEN NEW.settlement<>'' BEGIN SELECT RAISE(IGNORE); END",
         "map_explore_settlement_snapshot_write_rejected"),
        ("snapshot-rewrite", "AFTER UPDATE OF settlement ON map_explore_status WHEN NEW.settlement<>'' BEGIN UPDATE map_explore_status SET node_name='rewritten' WHERE user_id=NEW.user_id; END",
         "map_explore_settlement_snapshot_projection_mismatch"),
    )
    for index, (label, trigger, reason) in enumerate(snapshot_faults, start=26):
        @case(f"map-explore-settle-{label}", "探索结算", f"奖励快照写故障：{label}",
              "活动仍进行且无既有快照，状态更新触发器拒写或改写",
              "快照保存使用CAS保存点并完整回滚；恢复后同活动可结算",
              group="map-explore-settle", outcome="recovery")
        async def snapshot_recovery(probe, label=label, trigger=trigger, reason=reason,
                                   marker=index, number=974240 + index):
            from nonebot_plugin_xiuxian_2.core.player_reasons import reason_text

            module, _, operation_id = _prepare(probe, marker)
            player = probe.context.paths.player_db
            before = _projection(probe, operation_id)
            probe.seed(player, "CREATE TRIGGER settle_snapshot_fault " + trigger)
            with patch.object(module, "runtime_clock", SimpleNamespace(now=lambda: NOW)), patch.object(module, "runtime_random", RewardRandom(8)):
                reply = await _command(probe, "探索结算", number=number)
            failed = _projection(probe, operation_id)
            probe.evidence[label] = {"reply": reply, "before": before, "failed": failed}
            assert reason_text(reason) in reply and "探索结算完成" not in reply, reply
            assert failed == before, (before, failed)
            probe.seed(player, "DROP TRIGGER settle_snapshot_fault")
            with patch.object(module, "runtime_clock", SimpleNamespace(now=lambda: NOW)), patch.object(module, "runtime_random", RewardRandom(8)):
                recovered = await _command(probe, "探索结算", number=number)
            committed = _projection(probe, operation_id)
            assert "探索结算完成" in recovered and committed["receipt"] and committed["daily"][0][1:] == (1, 1), (recovered, committed)
            _integrity(probe)

    @case("map-explore-settle-snapshot-abort-recovery", "探索结算", "奖励快照写入ABORT后恢复",
          "有效活动，SQLite拒绝奖励快照更新",
          "意外异常记录原traceback/定位号，状态零变；同活动重试完成",
          group="map-explore-settle", outcome="recovery")
    async def snapshot_abort_recovery(probe):
        module, _, operation_id = _prepare(probe, 29)
        player = probe.context.paths.player_db
        before = _projection(probe, operation_id)
        probe.seed(player, "CREATE TRIGGER settle_snapshot_abort BEFORE UPDATE OF settlement ON map_explore_status WHEN NEW.settlement<>'' BEGIN SELECT RAISE(ABORT,'snapshot write blocked'); END")
        with patch.object(module, "runtime_clock", SimpleNamespace(now=lambda: NOW)), patch.object(module, "runtime_random", RewardRandom(10)):
            reply = await _command(probe, "探索结算", number=974269, allow_exception=True)
        failed = _projection(probe, operation_id)
        logs = probe.calls[-1].get("logs", [])
        probe.evidence["snapshot_abort"] = {"reply": reply, "before": before, "failed": failed, "logs": logs}
        assert "定位号" in reply and "探索结算完成" not in reply, reply
        assert any("[command] 探索结算" in row.get("message", "") and "sqlite3.IntegrityError" in row.get("exception", "") for row in logs), logs
        assert failed["wallet"] == before["wallet"] and failed["items"] == before["items"], (before, failed)
        assert failed["daily"] == before["daily"] and failed["status"] == before["status"], (before, failed)
        assert not failed["receipt"] and failed["ledger"] == [], failed
        player_snapshot_failures = probe.rows(
            probe.context.paths.game_db,
            "SELECT status FROM operation_ledger WHERE action='map.explore_settlement_snapshot' AND status='failed'",
        )
        assert player_snapshot_failures == [("failed",)], player_snapshot_failures
        probe.seed(player, "DROP TRIGGER settle_snapshot_abort")
        with patch.object(module, "runtime_clock", SimpleNamespace(now=lambda: NOW)):
            recovered = await _command(probe, "探索结算", number=974269)
        committed = _projection(probe, operation_id)
        assert "探索结算完成" in recovered and committed["receipt"] and committed["daily"][0][1:] == (1, 1), (recovered, committed)
        _integrity(probe)

    faults = (
        ("daily-ignore", "player", "BEFORE UPDATE OF explore_count ON map_daily_limit BEGIN SELECT RAISE(IGNORE); END",
         "map_explore_settlement_daily_write_rejected", "探索次数"),
        ("daily-rewrite", "player", "AFTER UPDATE OF explore_count ON map_daily_limit BEGIN UPDATE map_daily_limit SET resource_total_count=resource_total_count+1 WHERE user_id=NEW.user_id; END",
         "map_explore_settlement_daily_projection_mismatch", "探索次数"),
        ("status-ignore", "player", "BEFORE UPDATE OF running ON map_explore_status WHEN OLD.running=1 AND NEW.running=0 BEGIN SELECT RAISE(IGNORE); END",
         "map_explore_settlement_status_write_rejected", "探索状态"),
        ("status-rewrite", "player", "AFTER UPDATE OF running ON map_explore_status WHEN OLD.running=1 AND NEW.running=0 BEGIN UPDATE map_explore_status SET node_name='rewritten' WHERE user_id=NEW.user_id; END",
         "map_explore_settlement_status_projection_mismatch", "探索状态"),
        ("stone-ignore", "game", "BEFORE UPDATE OF stone ON user_xiuxian BEGIN SELECT RAISE(IGNORE); END",
         "map_explore_settlement_stone_write_rejected", "灵石钱包"),
        ("stone-rewrite", "game", "AFTER UPDATE OF stone ON user_xiuxian BEGIN UPDATE user_xiuxian SET stone=stone+1 WHERE user_id=NEW.user_id; END",
         "map_explore_settlement_stone_projection_mismatch", "灵石钱包"),
        ("inventory-ignore", "game", "BEFORE INSERT ON back BEGIN SELECT RAISE(IGNORE); END",
         "map_explore_settlement_inventory_write_rejected", "背包物品"),
        ("inventory-rewrite", "game", "AFTER INSERT ON back BEGIN UPDATE back SET goods_num=goods_num+1 WHERE user_id=NEW.user_id AND goods_id=NEW.goods_id; END",
         "map_explore_settlement_inventory_projection_mismatch", "背包物品"),
        ("receipt-rewrite", "game", "AFTER INSERT ON map_explore_settlement_operations BEGIN UPDATE map_explore_settlement_operations SET payload='rewritten' WHERE operation_id=NEW.operation_id; END",
         "map_explore_settlement_receipt_projection_mismatch", "探索结算回执"),
    )
    for index, (label, database, trigger, reason, subject) in enumerate(faults, start=2):
        @case(f"map-explore-settle-{label}", "探索结算", f"隔离SQLite写故障：{label}",
              "有效探索及完整迁移库，奖励快照由实际事件流生成",
              "具体原因/对象、全部结算写回滚，故障解除后同活动原奖励恢复",
              group="map-explore-settle", outcome="recovery")
        async def recover(probe, label=label, database=database, trigger=trigger,
                          reason=reason, subject=subject, marker=index, number=974210 + index):
            from nonebot_plugin_xiuxian_2.core.player_reasons import reason_text

            module, _, operation_id = _prepare(probe, marker)
            path = probe.context.paths.player_db if database == "player" else probe.context.paths.game_db
            before = _projection(probe, operation_id)
            probe.seed(path, "CREATE TRIGGER settle_fault " + trigger)
            if label.startswith("inventory-"):
                if label == "inventory-ignore":
                    update_trigger = "BEFORE UPDATE OF goods_num ON back BEGIN SELECT RAISE(IGNORE); END"
                else:
                    update_trigger = "AFTER UPDATE OF goods_num ON back BEGIN UPDATE back SET goods_num=goods_num+1 WHERE user_id=NEW.user_id AND goods_id=NEW.goods_id; END"
                probe.seed(path, "CREATE TRIGGER settle_fault_update " + update_trigger)
            with patch.object(module, "runtime_clock", SimpleNamespace(now=lambda: NOW)), patch.object(module, "runtime_random", RewardRandom(1)):
                reply = await _command(probe, "探索结算", number=number)
            failed = _projection(probe, operation_id)
            probe.evidence[label] = {"reply": reply, "before": before, "failed": failed}
            assert reason_text(reason) in reply and subject in reply and "探索结算完成" not in reply, reply
            for key in ("wallet", "items", "daily", "receipt"):
                assert failed[key] == before[key], (label, key, before, failed)
            assert failed["ledger"] == [], failed
            assert failed["snapshot_ledger"] == [("applied",)], failed
            assert failed["status"][0][:5] == before["status"][0][:5], failed
            assert failed["status"][0][5] and failed["status"][0][6:] == before["status"][0][6:], failed
            probe.seed(path, "DROP TRIGGER settle_fault")
            if label.startswith("inventory-"):
                probe.seed(path, "DROP TRIGGER settle_fault_update")
            with patch.object(module, "runtime_clock", SimpleNamespace(now=lambda: NOW)), patch.object(module, "runtime_random", RewardRandom(1)):
                recovered = await _command(probe, "探索结算", number=number)
            committed = _projection(probe, operation_id)
            assert "探索结算完成" in recovered and committed["ledger"] == [("applied",)], (recovered, committed)
            assert committed["receipt"] and committed["daily"][0][1:] == (1, 1), committed
            assert committed["status"][0][0] == 0, committed
            _integrity(probe)

    @case("map-explore-settle-abort-recovery", "探索结算", "回执事务ABORT后恢复",
          "有效探索及隔离迁移库，SQLite回执插入触发异常",
          "异常有traceback/定位号、事务未部分结算，解除后同活动恢复",
          group="map-explore-settle", outcome="recovery")
    async def abort_recovery(probe):
        module, _, operation_id = _prepare(probe, 20)
        game = probe.context.paths.game_db
        before = _projection(probe, operation_id)
        probe.seed(game, "CREATE TRIGGER settle_abort BEFORE INSERT ON map_explore_settlement_operations BEGIN SELECT RAISE(ABORT,'settlement receipt blocked'); END")
        with patch.object(module, "runtime_clock", SimpleNamespace(now=lambda: NOW)), patch.object(module, "runtime_random", RewardRandom(1)):
            reply = await _command(probe, "探索结算", number=974230, allow_exception=True)
        failed = _projection(probe, operation_id)
        logs = probe.calls[-1].get("logs", [])
        probe.evidence["abort"] = {"reply": reply, "before": before, "failed": failed, "logs": logs}
        assert "定位号" in reply and "结算完成" not in reply, reply
        assert any("[command] 探索结算" in row.get("message", "") and "sqlite3.IntegrityError" in row.get("exception", "") for row in logs), logs
        for key in ("wallet", "items", "daily", "receipt"):
            assert failed[key] == before[key], (key, before, failed)
        assert failed["status"][0][:5] == before["status"][0][:5] and failed["status"][0][5], failed
        assert failed["ledger"] == [("failed",)], failed
        probe.seed(game, "DROP TRIGGER settle_abort")
        with patch.object(module, "runtime_clock", SimpleNamespace(now=lambda: NOW)):
            recovered = await _command(probe, "探索结算", number=974230)
        committed = _projection(probe, operation_id)
        assert "探索结算完成" in recovered and committed["ledger"] == [("applied",)], committed
        assert committed["receipt"] and committed["status"][0][0] == 0, committed
        _integrity(probe)

    @case("map-explore-settle-boundaries", "探索结算", "时长阈值/封顶与每日次数边界",
          "真实matcher、固定业务时钟、隔离玩家数据",
          "不足一分钟零结算；一分钟保底一轮；封顶且日次数达到上限时原状态保留",
          group="map-explore-settle", outcome="success")
    async def boundaries(probe):
        module, _, short_operation = _prepare(probe, 21)
        short_start = NOW.replace(minute=NOW.minute, second=30)
        module.map_application.save_explore_status(ACTOR, {
            **module.map_application.explore_status(ACTOR),
            "start_time": short_start.strftime("%Y-%m-%d %H:%M:%S"),
            "duration_min": 45, "max_duration_min": 20,
        })
        with patch.object(module, "runtime_clock", SimpleNamespace(now=lambda: NOW)):
            short = await _command(probe, "探索结算", number=974231)
        assert "时间太短" in short and _projection(probe, short_operation)["receipt"] == [], short
        still_running = module.map_application.explore_status(ACTOR)
        assert still_running and still_running["running"] == 1

        module, _, minute_operation = _prepare(probe, 22)
        minute_start = NOW - timedelta(minutes=1)
        minute_start_text = minute_start.strftime("%Y-%m-%d %H:%M:%S")
        module.map_application.save_explore_status(ACTOR, {
            **module.map_application.explore_status(ACTOR),
            "start_time": minute_start_text,
            "duration_min": 45, "max_duration_min": 20,
        })
        minute_operation = f"map-explore:{ACTOR}:{minute_start_text}"
        with patch.object(module, "runtime_clock", SimpleNamespace(now=lambda: NOW)), patch.object(module, "runtime_random", RewardRandom(2)):
            minute = await _command(probe, "探索结算", number=974232)
        committed = _projection(probe, minute_operation)
        assert "有效探索：1分钟（共1轮）" in minute and committed["daily"][0][1:] == (1, 1), (minute, committed)
        assert committed["receipt"] and committed["status"][0][0] == 0

        module, _, capped_operation = _prepare(probe, 23)
        module.map_application.save_daily_limit(ACTOR, {
            "date": "2026-11-22", "explore_count": module.DAILY_LIMIT_CONFIG["explore"],
            "resource_total_count": 0,
        })
        before_cap = _projection(probe, capped_operation)
        with patch.object(module, "runtime_clock", SimpleNamespace(now=lambda: NOW)), patch.object(module, "runtime_random", RewardRandom(3)):
            capped = await _command(probe, "探索结算", number=974233)
        rejected = _projection(probe, capped_operation)
        assert "已达上限" in capped and not rejected["receipt"] and rejected["status"][0][0] == 1, (capped, rejected)
        assert rejected == before_cap, (before_cap, rejected)
        next_day = NOW + timedelta(days=1)
        with patch.object(module, "runtime_clock", SimpleNamespace(now=lambda: next_day)), patch.object(module, "runtime_random", RewardRandom(3)):
            recovered_cap = await _command(probe, "探索结算", number=974233)
        after_cap_recovery = _projection(probe, capped_operation)
        assert "探索结算完成" in recovered_cap and after_cap_recovery["receipt"], (recovered_cap, after_cap_recovery)
        assert after_cap_recovery["daily"] == [("2026-11-23", 1, 1)] and after_cap_recovery["status"][0][0] == 0, after_cap_recovery

        module, _, inventory_operation = _prepare(probe, 28)
        with patch.object(module, "runtime_clock", SimpleNamespace(now=lambda: NOW)), \
             patch.object(module, "runtime_random", RewardRandom(9)), \
             patch.object(module, "XiuConfig", return_value=SimpleNamespace(max_goods_num=0)):
            full = await _command(probe, "探索结算", number=974238)
        frozen = _projection(probe, inventory_operation)
        assert "背包物品（ID " in full and "背包物品已达上限" in full, full
        assert frozen["status"][0][5] and not frozen["receipt"] and not frozen["ledger"], frozen
        with patch.object(module, "runtime_clock", SimpleNamespace(now=lambda: NOW)):
            recovered = await _command(probe, "探索结算", number=974238)
        committed_inventory = _projection(probe, inventory_operation)
        assert "探索结算完成" in recovered and committed_inventory["receipt"] and committed_inventory["daily"][0][1:] == (1, 1), (recovered, committed_inventory)
        _integrity(probe)

    @case("map-explore-settle-no-active-and-isolation", "探索结算", "无活动/未注册/玩家隔离/成功后重复",
          "真实matcher与两个隔离玩家",
          "无效前置零奖励写；本人成功与另一玩家记录不串读；重复指令不重复结算",
          group="map-explore-settle", outcome="rejection")
    async def no_active_and_isolation(probe):
        module, _, operation_id = _prepare(probe, 24)
        module.map_application.save_explore_status(ACTOR, {
            "running": 0, "node_type": "", "node_name": "", "start_time": "",
            "duration_min": 0, "settlement": "", "max_duration_min": 0, "interval_min": 0,
        })
        no_active = await _command(probe, "探索结算", number=974234)
        assert "没有进行中的探索" in no_active
        assert not _projection(probe, operation_id)["receipt"]
        unregistered = await _command(probe, "探索结算", actor="900099", number=974235)
        assert "输入【我要修仙】加入" in unregistered, unregistered

        _prepare(probe, 25)
        other = "900002"
        probe.player(uid=other, stone=777)
        module.map_application.save_daily_limit(other, {"date": "2026-11-22", "explore_count": 0, "resource_total_count": 0})
        module.map_application.save_explore_status(other, {
            "running": 1, "node_type": "遗迹", "node_name": "他人节点",
            "start_time": "2026-11-22 13:00:25", "duration_min": 45, "max_duration_min": 20,
            "interval_min": 20, "settlement": "",
        })
        with patch.object(module, "runtime_clock", SimpleNamespace(now=lambda: NOW)), patch.object(module, "runtime_random", RewardRandom(4)):
            reply = await _command(probe, "探索结算", number=974236)
        own_op = f"map-explore:{ACTOR}:2026-11-22 13:00:25"
        own = _projection(probe, own_op)
        other_state = module.map_application.explore_status(other)
        assert "探索结算完成" in reply and own["receipt"] and other_state["running"] == 1, (reply, own, other_state)
        assert probe.rows(probe.context.paths.game_db, "SELECT stone FROM user_xiuxian WHERE user_id=?", (other,)) == [(777,)]
        before_repeat = _projection(probe, own_op)
        repeated = await _command(probe, "探索结算", number=974236)
        assert "没有进行中的探索" in repeated and _projection(probe, own_op) == before_repeat
        _integrity(probe)
