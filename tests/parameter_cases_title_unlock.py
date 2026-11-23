"""Unlock genuine eligible titles through live commands and isolated SQLite."""

import json
from unittest.mock import AsyncMock, patch

from tests.parameter_cases_team_leave import integrity
from tests.parameter_cases_team_view import database_projection


def eligible_player(probe, module, *, existing=True, count=30):
    probe.managers[1].update_or_write_data("900001", "statistics", "修仙签到", count)
    player = probe.context.paths.player_db
    probe.seed(player, "DELETE FROM title WHERE user_id='900001'")
    if existing:
        probe.seed(player, "INSERT INTO title(user_id,unlocked,equipped) VALUES('900001','[]','')")
    snapshot = module.get_title_condition_snapshot("900001")
    assert snapshot.statistics["修仙签到"] == count
    unlockable = module.find_unlockable_titles("900001", snapshot=snapshot, unlocked_title_ids=[])
    if count == 30:
        assert {"30001", "30002"} <= {title["id"] for title in unlockable}, unlockable
    return unlockable


def business_projection(probe):
    return {name: [table for table in tables if table[0]["name"] not in {"operation_ledger", "operation_audit", "sqlite_sequence"}]
            for name, tables in database_projection(probe).items()}


def register(case):
    @case("title-unlock-receipt-recovery", "检查称号", "真实累计签到30次，解锁回执INSERT IGNORE",
          "合法玩家、真实称号条件/资格快照及隔离双库", "回执拒写具体原因、状态/账本整笔回滚，解除故障同消息恢复/replay",
          group="title-unlock-parameters")
    async def receipt_recovery(probe):
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_title as module

        player = probe.context.paths.player_db
        titles = eligible_player(probe, module)
        before = database_projection(probe)
        operation = "title-unlock:998000:900001"
        probe.seed(player, "CREATE TRIGGER title_unlock_fault BEFORE INSERT ON title_transaction_operations "
                   "BEGIN SELECT RAISE(IGNORE); END")
        try:
            probe.clear_cooldowns()
            failed = await probe.command("检查称号", number=998000)
            rows = probe.rows(player, "SELECT unlocked,equipped FROM title WHERE user_id='900001'")
            receipts = probe.rows(player, "SELECT * FROM title_transaction_operations WHERE operation_id=?", (operation,))
            probe.evidence.update(reply=failed, state=rows, receipt=receipts, full_database_unchanged=database_projection(probe) == before)
            assert "称号解锁操作回执保存失败" in failed and "恭喜解锁" not in failed, failed
            assert database_projection(probe) == before
        finally:
            probe.seed(player, "DROP TRIGGER title_unlock_fault")
        probe.clear_cooldowns()
        recovered = await probe.command("检测称号", number=998000)
        for title in titles:
            assert title["name"] in recovered, recovered
        assert set(json.loads(probe.rows(player, "SELECT unlocked FROM title WHERE user_id='900001'")[0][0])) == {title["id"] for title in titles}
        committed = database_projection(probe)
        probe.clear_cooldowns()
        replay = await probe.command("检查称号", number=998000)
        assert "暂无新称号解锁" in replay and "恭喜解锁" not in replay, replay
        assert database_projection(probe) == committed
        integrity(probe)
        probe.evidence.update(recovered=True, replay_zero_writes=True, integrity_check="ok")

    @case("title-unlock-write-recovery", "检查称号", "新建/已有称号档案、回执/账本末步改写",
          "真实签到资格、真实handler和隔离SQLite触发器", "五类具体失败/称号subject、整事务回滚，解除故障原消息恢复",
          group="title-unlock-parameters")
    async def write_recovery(probe):
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_title as module

        player = probe.context.paths.player_db
        faults = (
            (False, "BEFORE INSERT ON title", "SELECT RAISE(IGNORE)", "称号解锁结果保存失败"),
            (True, "BEFORE UPDATE ON title", "SELECT RAISE(IGNORE)", "称号解锁结果保存失败"),
            (False, "AFTER INSERT ON title", "UPDATE title SET unlocked='[]' WHERE user_id=NEW.user_id", "称号解锁状态与请求不一致"),
            (True, "AFTER UPDATE ON title", "UPDATE title SET equipped='changed' WHERE user_id=NEW.user_id", "称号解锁状态与请求不一致"),
            (True, "AFTER INSERT ON title_transaction_operations", "UPDATE title_transaction_operations SET payload='[]' WHERE operation_id=NEW.operation_id", "称号解锁操作回执内容不一致"),
            (True, "AFTER INSERT ON title_transaction_operations", "UPDATE title SET unlocked='[]' WHERE user_id='900001'", "称号解锁状态与请求不一致"),
            (True, "AFTER INSERT ON operation_audit", "UPDATE title SET equipped='changed' WHERE user_id='900001'", "称号解锁状态与请求不一致"),
            (True, "AFTER INSERT ON operation_audit", "UPDATE title_transaction_operations SET title_id='wrong' WHERE operation_id=NEW.operation_id", "称号解锁操作回执内容不一致"),
            (True, "AFTER INSERT ON operation_audit", "UPDATE operation_ledger SET result_json='{}' WHERE operation_id=NEW.operation_id", "称号解锁账本与结算结果不一致"),
            (True, "AFTER INSERT ON operation_audit", "UPDATE operation_ledger SET request_hash='changed' WHERE operation_id=NEW.operation_id", "称号解锁账本与结算结果不一致"),
        )
        for index, (existing, timing, body, phrase) in enumerate(faults):
            titles = eligible_player(probe, module, existing=existing)
            before = database_projection(probe)
            number = 998100 + index
            probe.seed(player, f"CREATE TRIGGER title_unlock_fault {timing} BEGIN "
                       f"UPDATE title SET equipped='side-effect' WHERE user_id='900002'; {body}; END")
            try:
                probe.clear_cooldowns()
                failed = await probe.command("检查称号", number=number)
                assert phrase in failed and "30001,30002" in failed and "恭喜解锁" not in failed, failed
                assert database_projection(probe) == before
            finally:
                probe.seed(player, "DROP TRIGGER title_unlock_fault")
            probe.clear_cooldowns()
            recovered = await probe.command("检测称号", number=number)
            assert "恭喜解锁" in recovered, recovered
            for title in titles:
                assert title["name"] in recovered, recovered
            state = module.title_application.get_state("900001")
            assert set(json.loads(state["unlocked"])) == {title["id"] for title in titles}
            assert state["equipped"] == ""
            committed = database_projection(probe)
            probe.clear_cooldowns()
            assert "暂无新称号解锁" in await probe.command("检查称号", number=number)
            assert database_projection(probe) == committed
            probe.evidence[str(index)] = dict(reply=failed, rollback=True, recovered=True, repeat_zero_writes=True)
        integrity(probe)
        probe.evidence.update(integrity_check="ok")

    @case("title-unlock-abort-recovery", "检查称号", "实际解锁写点ABORT与账本/审计拒写",
          "真实资格快照、实际应用ledger和隔离数据库", "原异常/日志/定位号，业务整笔回滚且失败审计保留，原消息可恢复",
          group="title-unlock-parameters")
    async def abort_recovery(probe):
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_title as module

        player = probe.context.paths.player_db
        faults = (
            ("UPDATE ON title", "1", "ABORT", "isolated title command unlock abort"),
            ("INSERT ON title_transaction_operations", "1", "ABORT", "isolated title command unlock abort"),
            ("INSERT ON operation_audit", "NEW.status='applied'", "ABORT", "isolated title command unlock abort"),
            ("INSERT ON operation_ledger", "NEW.status='started'", "IGNORE", "operation ledger finish write rejected"),
            ("UPDATE ON operation_ledger", "NEW.status='applied'", "IGNORE", "operation ledger finish write rejected"),
            ("INSERT ON operation_audit", "NEW.status='applied'", "IGNORE", "operation audit finish write rejected"),
        )
        for index, (timing, condition, fault, phrase) in enumerate(faults):
            eligible_player(probe, module)
            before = business_projection(probe)
            number = 998200 + index
            operation = f"title-unlock:{number}:900001"
            body = "SELECT RAISE(ABORT,'isolated title command unlock abort')" if fault == "ABORT" else "SELECT RAISE(IGNORE)"
            probe.seed(player, f"CREATE TRIGGER title_unlock_fault BEFORE {timing} WHEN {condition} BEGIN {body}; END")
            try:
                probe.clear_cooldowns()
                failed = await probe.command("检查称号", number=number, allow_exception=True)
                assert "定位号：" in failed and "内部异常" in failed and "暂无新称号解锁" not in failed, failed
                assert any(phrase in row["exception"] for row in probe.calls[-1]["logs"])
                assert business_projection(probe) == before
                assert probe.rows(player, "SELECT status FROM operation_ledger WHERE operation_id=?", (operation,)) == [("failed",)]
                audit = probe.rows(player, "SELECT * FROM operation_audit WHERE operation_id=?", (operation,))
                assert len(audit) == 1 and audit[0][3] == "failed", audit
            finally:
                probe.seed(player, "DROP TRIGGER title_unlock_fault")
            probe.clear_cooldowns()
            recovered = await probe.command("检测称号", number=number)
            assert "初心不改" in recovered and "持之以恒" in recovered and "恭喜解锁" in recovered, recovered
            assert audit[0] in probe.rows(player, "SELECT * FROM operation_audit WHERE operation_id=?", (operation,))
            committed = database_projection(probe)
            probe.clear_cooldowns()
            assert "暂无新称号解锁" in await probe.command("检查称号", number=number)
            assert database_projection(probe) == committed
            probe.evidence[str(index)] = dict(reply=failed, original_cause=phrase, rollback=True, recovered=True, failure_audit_preserved=True)
        integrity(probe)
        probe.evidence.update(integrity_check="ok")

    @case("title-unlock-normal-boundaries", "检查称号", "真实签到9/10/30、别名/私聊/同消息及发送失败",
          "真实资格条件/双库/消息入口，不替换判定", "阈值与无新解锁正常，冷却拒绝零写、发送失败仍已提交，流程异常透传",
          group="title-unlock-parameters")
    async def normal_boundaries(probe):
        from nonebot.adapters.onebot.v11 import PrivateMessageEvent
        from tests import command_probe
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_title as module

        eligible_player(probe, module, count=9)
        before = database_projection(probe)
        probe.clear_cooldowns()
        empty = await probe.command("检查称号", number=998300)
        assert "当前已解锁0个称号，暂无新称号解锁" in empty, empty
        assert database_projection(probe) == before
        with patch.object(module, "_unlock_titles_from_event", side_effect=AssertionError("cooldown must precede unlock")):
            cooldown = await probe.command("检查称号", number=998399)
        assert cooldown and "检查完成" not in cooldown, cooldown
        assert database_projection(probe) == before
        probe.managers[1].update_or_write_data("900001", "statistics", "修仙签到", 10)
        probe.clear_cooldowns()
        first = await probe.command("检测称号", number=998301)
        assert "初心不改" in first and "恭喜解锁" in first and "持之以恒" not in first, first
        assert json.loads(module.title_application.get_state("900001")["unlocked"]) == ["30001"]
        probe.managers[1].update_or_write_data("900001", "statistics", "修仙签到", 30)
        make_group = command_probe.make_event

        def make_private(text, actor, number):
            data = make_group(text, actor, number).model_dump()
            data.pop("group_id")
            data.update(message_type="private", sub_type="friend")
            return PrivateMessageEvent.model_validate(data)

        probe.clear_cooldowns()
        with patch.object(command_probe, "make_event", side_effect=make_private):
            second = await probe.command("检查称号", number=998302)
        assert "持之以恒" in second and "初心不改" not in second and "恭喜解锁" in second, second
        committed = database_projection(probe)
        probe.clear_cooldowns()
        assert "当前已解锁2个称号，暂无新称号解锁" in await probe.command("检测称号", number=998302)
        assert database_projection(probe) == committed

        eligible_player(probe, module)
        probe.clear_cooldowns()
        with patch.object(module, "handle_send", AsyncMock(side_effect=RuntimeError("isolated committed title unlock reply failure"))):
            failed = await probe.command("检查称号", number=998303, allow_exception=True)
        assert "定位号：" in failed and "内部异常" in failed, failed
        assert any("isolated committed title unlock reply failure" in row["exception"] for row in probe.calls[-1]["logs"])
        assert set(json.loads(module.title_application.get_state("900001")["unlocked"])) == {"30001", "30002"}
        committed = database_projection(probe)
        probe.clear_cooldowns()
        assert "暂无新称号解锁" in await probe.command("检测称号", number=998303)
        assert database_projection(probe) == committed
        integrity(probe)
        probe.evidence.update(thresholds=[9, 10, 30], private=True, send_failure_committed=True,
                              repeat_zero_writes=True, integrity_check="ok")

    @case("title-unlock-shared-command-failures", "检查称号", "同一解锁helper的四个真实玩家入口",
          "检查称号/我的称号/我的成就/检查成就共享真实解锁事务", "已知失败不可当无新称号，保留具体reason和subject；解除故障正常解锁",
          group="title-unlock-parameters")
    async def shared_commands(probe):
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_title as module

        player = probe.context.paths.player_db
        for index, command in enumerate(("检查称号", "我的称号", "我的成就", "检查成就")):
            eligible_player(probe, module)
            before = database_projection(probe)
            number = 998400 + index
            probe.seed(player, "CREATE TRIGGER title_unlock_fault BEFORE INSERT ON title_transaction_operations BEGIN SELECT RAISE(IGNORE); END")
            try:
                probe.clear_cooldowns()
                failed = await probe.command(command, number=number)
                assert "称号解锁操作回执保存失败" in failed and "30001,30002" in failed, failed
                assert "暂无新" not in failed and "已完成" not in failed and "恭喜解锁" not in failed, failed
                assert database_projection(probe) == before
            finally:
                probe.seed(player, "DROP TRIGGER title_unlock_fault")
            probe.clear_cooldowns()
            recovered = await probe.command(command, number=number)
            assert "初心不改" in recovered and "持之以恒" in recovered, recovered
            assert set(json.loads(module.title_application.get_state("900001")["unlocked"])) == {"30001", "30002"}
            committed = database_projection(probe)
            probe.clear_cooldowns()
            assert await probe.command(command, number=number)
            assert database_projection(probe) == committed
            probe.evidence[command] = dict(failed=failed, recovered=True, repeat_zero_writes=True)
        integrity(probe)
        probe.evidence.update(integrity_check="ok")

    @case("title-unlock-conflict-reasons", "检查称号", "资格读取后称号列表变化、同消息资格增加",
          "实际应用事务拒绝，不替换条件引擎", "保留解锁列表变化/请求冲突原因，不播报暂无新称号；新消息可恢复",
          group="title-unlock-parameters")
    async def conflicts(probe):
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_title as module

        player = probe.context.paths.player_db
        eligible_player(probe, module)
        execute = module.title_application.execute

        def concurrent_change(**kwargs):
            probe.seed(player, "UPDATE title SET unlocked='[\"30004\"]' WHERE user_id='900001'")
            return execute(**kwargs)

        probe.clear_cooldowns()
        with patch.object(module.title_application, "execute", side_effect=concurrent_change):
            stale = await probe.command("检查称号", number=998500)
        assert "已解锁称号列表与操作前不一致" in stale and "30001,30002" in stale and "暂无新" not in stale, stale
        assert json.loads(module.title_application.get_state("900001")["unlocked"]) == ["30004"]
        committed = database_projection(probe)
        probe.clear_cooldowns()
        assert await probe.command("检查称号", number=998500) == stale
        assert database_projection(probe) == committed
        probe.clear_cooldowns()
        recovered = await probe.command("检测称号", number=998501)
        assert "初心不改" in recovered and "持之以恒" in recovered, recovered

        eligible_player(probe, module, count=10)
        probe.clear_cooldowns()
        assert "初心不改" in await probe.command("检查称号", number=998502)
        probe.managers[1].update_or_write_data("900001", "statistics", "修仙签到", 30)
        committed = database_projection(probe)
        probe.clear_cooldowns()
        conflict = await probe.command("检查称号", number=998502)
        assert "不同内容" in conflict and "暂无新" not in conflict, conflict
        assert database_projection(probe) == committed
        probe.clear_cooldowns()
        assert "持之以恒" in await probe.command("检测称号", number=998503)
        integrity(probe)
        probe.evidence.update(stale_reply=stale, conflict_reply=conflict, concrete_reasons=True, recovered=True, integrity_check="ok")
