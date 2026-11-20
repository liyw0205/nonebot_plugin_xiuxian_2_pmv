"""Grant one title through the real privileged matcher and isolated stores."""

import json
from contextlib import suppress

from tests.parameter_cases_team_leave import integrity
from tests.parameter_cases_team_view import database_projection
from tests.command_probe import ProbeBlocked


def register(case):
    @case("title-grant-write-recovery", "赠送称号", "名称/ID目标、状态/回执/账本拒写与改写恢复",
          "SUPERUSER真实matcher、合法道号、隔离game/player SQLite", "具体原因/称号subject，完整回滚，解除触发后原消息成功/replay",
          group="title-grant-parameters")
    async def write_recovery(probe):
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_title as module

        player = probe.context.paths.player_db
        faults = [
            (False, "BEFORE INSERT ON title", "SELECT RAISE(IGNORE)", "赠送称号状态保存失败"),
            (True, "BEFORE UPDATE ON title", "SELECT RAISE(IGNORE)", "赠送称号状态保存失败"),
            (False, "AFTER INSERT ON title", "UPDATE title SET unlocked='[]' WHERE user_id=NEW.user_id", "赠送称号状态与请求不一致"),
            (True, "AFTER UPDATE ON title", "UPDATE title SET equipped='changed' WHERE user_id=NEW.user_id", "赠送称号状态与请求不一致"),
            (True, "BEFORE INSERT ON title_transaction_operations", "SELECT RAISE(IGNORE)", "赠送称号操作回执保存失败"),
            (True, "AFTER INSERT ON title_transaction_operations", "UPDATE title_transaction_operations SET payload='[]' WHERE operation_id=NEW.operation_id", "赠送称号回执内容不一致"),
            (True, "AFTER INSERT ON title_transaction_operations", "UPDATE title SET equipped='changed' WHERE user_id='900001'", "赠送称号状态与请求不一致"),
            (True, "AFTER INSERT ON operation_audit", "UPDATE title SET equipped='changed' WHERE user_id='900001'", "赠送称号状态与请求不一致"),
            (True, "AFTER INSERT ON operation_audit", "UPDATE title_transaction_operations SET title_id='wrong' WHERE operation_id=NEW.operation_id", "赠送称号回执内容不一致"),
            (True, "AFTER INSERT ON operation_audit", "UPDATE operation_ledger SET request_hash='wrong' WHERE operation_id=NEW.operation_id", "赠送称号账本与发放结果不一致"),
        ]
        for fault_index, (existing, timing, body, phrase) in enumerate(faults):
            title_input = "初入仙途" if existing else "30101"
            number = 999600 + fault_index
            probe.seed(player, "DELETE FROM title WHERE user_id='900001'")
            if existing:
                probe.seed(player, "INSERT INTO title VALUES('900001','[]','keep')")
            probe.seed(player, "CREATE TABLE IF NOT EXISTS isolated_grant_effect(value INTEGER)")
            before = database_projection(probe)
            trigger_body = f"INSERT INTO isolated_grant_effect VALUES(1); {body}"
            probe.seed(player, f"CREATE TRIGGER isolated_grant_fault {timing} BEGIN {trigger_body}; END")
            try:
                probe.clear_cooldowns()
                failed = await probe.command("赠送称号", f"赠送称号 {title_input} 诊断道友", actor="900003", number=number)
                assert phrase in failed and "称号【初入仙途】" in failed, failed
                assert "成功给" not in failed and database_projection(probe) == before
            finally:
                probe.seed(player, "DROP TRIGGER isolated_grant_fault")
            probe.clear_cooldowns()
            recovered = await probe.command("赠送称号", f"赠送称号 {title_input} 诊断道友", actor="900003", number=number)
            assert "成功给 诊断道友 赠送称号【初入仙途】" in recovered, recovered
            state = module.title_application.get_state("900001")
            assert json.loads(state["unlocked"]) == ["30101"] and state["equipped"] == ("keep" if existing else "")
            operation = f"title-grant-30101:{number}:900001"
            assert probe.rows(player, "SELECT result_status,title_id FROM title_transaction_operations WHERE operation_id=?",
                              (operation,)) == [("applied", "30101")]
            committed = database_projection(probe)
            probe.clear_cooldowns()
            replay = await probe.command("赠送称号", f"赠送称号 {title_input} 诊断道友", actor="900003", number=number)
            assert "成功给 诊断道友" in replay and database_projection(probe) == committed
        integrity(probe)
        probe.evidence.update(faults=[fault[3] for fault in faults], target_forms=["ID", "name"],
                              new_and_existing_profile=True, same_message_recovery_and_replay=True, integrity_check="ok")

    @case("title-grant-abort-recovery", "赠送称号", "状态UPDATE ABORT保留原异常并重试",
          "真实SUPERUSER命令、合法单人道号和隔离玩家库", "有定位号/traceback且无部分写，解除故障原消息恢复永久回执",
          group="title-grant-parameters")
    async def abort_recovery(probe):
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_title as module

        player = probe.context.paths.player_db
        probe.seed(player, "DELETE FROM title WHERE user_id='900001'")
        probe.seed(player, "INSERT INTO title VALUES('900001','[]','keep')")
        before = database_projection(probe)
        probe.seed(player, "CREATE TRIGGER isolated_grant_abort BEFORE UPDATE ON title WHEN NEW.user_id='900001' "
                           "BEGIN SELECT RAISE(ABORT,'isolated real grant abort'); END")
        try:
            probe.clear_cooldowns()
            failed = await probe.command("赠送称号", "赠送称号 30101 诊断道友", actor="900003", number=999700,
                                         allow_exception=True)
            assert "定位号：" in failed and "内部异常" in failed and "成功给" not in failed, failed
            assert any("isolated real grant abort" in row["exception"] for row in probe.calls[-1]["logs"])
            current = database_projection(probe)
            for database in before:
                old_tables = {table[0]["name"]: table[1] for table in before[database]
                              if table[0]["name"] not in {"operation_ledger", "operation_audit", "sqlite_sequence"}}
                new_tables = {table[0]["name"]: table[1] for table in current[database]
                              if table[0]["name"] not in {"operation_ledger", "operation_audit", "sqlite_sequence"}}
                assert new_tables == old_tables, (database, old_tables, new_tables)
        finally:
            probe.seed(player, "DROP TRIGGER isolated_grant_abort")
        probe.clear_cooldowns()
        recovered = await probe.command("赠送称号", "赠送称号 30101 诊断道友", actor="900003", number=999700)
        assert "成功给 诊断道友" in recovered
        assert json.loads(module.title_application.get_state("900001")["unlocked"]) == ["30101"]
        assert probe.rows(player, "SELECT result_status,title_id FROM title_transaction_operations WHERE operation_id=?",
                          ("title-grant-30101:999700:900001",)) == [("applied", "30101")]
        integrity(probe)
        probe.evidence.update(original_error_logged=True, restored_same_message=True, integrity_check="ok")

    @case("title-grant-permission-contract", "赠送称号", "管理员/普通玩家、空参数、无效称号、已有目标",
          "SUPERUSER真实matcher、别名/ID和隔离数据", "权限在handler前、失败不写，已有称号明确拒绝而不伪赠送",
          group="title-grant-parameters")
    async def permission_contract(probe):
        before = database_projection(probe)
        probe.clear_cooldowns()
        with suppress(ProbeBlocked):
            await probe.command("赠送称号", "赠送称号 30101 诊断道友", actor="900001", number=999710)
        assert database_projection(probe) == before
        for index, text in enumerate(("赠送称号", "赠送称号 999999999 诊断道友")):
            probe.clear_cooldowns()
            reply = await probe.command("赠送称号", text, actor="900003", number=999711 + index)
            assert ("用法：赠送称号" if index == 0 else "称号不存在") in reply, reply
            assert database_projection(probe) == before
        probe.seed(probe.context.paths.player_db, "DELETE FROM title WHERE user_id='900001'")
        probe.seed(probe.context.paths.player_db, "INSERT INTO title VALUES('900001','[\"30101\"]','')")
        owned = {database: [table for table in tables
                            if table[0]["name"] not in {"operation_ledger", "operation_audit", "sqlite_sequence"}]
                 for database, tables in database_projection(probe).items()}
        probe.clear_cooldowns()
        reply = await probe.command("赠送称号", "赠送称号 初入仙途 诊断道友", actor="900003", number=999713)
        assert "已拥有称号【初入仙途】" in reply and "成功给" not in reply, reply
        after = {database: [table for table in tables
                            if table[0]["name"] not in {"operation_ledger", "operation_audit", "sqlite_sequence"}]
                 for database, tables in database_projection(probe).items()}
        assert after == owned
        integrity(probe)
        probe.evidence.update(nonadmin_blocked=True, invalid_and_missing_args_zero_writes=True,
                              already_owned_rejected=True, integrity_check="ok")

    @case("title-grant-self-mention", "赠送称号", "默认自己、CQ @已注册目标、@未注册目标",
          "真实SUPERUSER matcher、三个隔离玩家档案", "默认/提及目标准确、回执与称号一致，缺失目标双库零写",
          group="title-grant-parameters")
    async def target_forms(probe):
        from tests.parameter_cases_team_view import database_projection
        from tests.parameter_cases_team_leave import integrity

        game_before = database_projection(probe)[probe.context.paths.game_db.name]

        probe.clear_cooldowns()
        self_reply = await probe.command(
            "赠送称号", "赠送称号 30101", actor="900003", number=999720,
        )
        assert "成功给 诊断管理 赠送称号【初入仙途】" in self_reply, self_reply
        assert probe.rows(
            probe.context.paths.player_db,
            "SELECT unlocked,equipped FROM title WHERE user_id='900003'",
        ) == [( '["30101"]', "")]
        assert probe.rows(
            probe.context.paths.player_db,
            "SELECT result_status,title_id FROM title_transaction_operations WHERE operation_id=?",
            ("title-grant-30101:999720:900003",),
        ) == [("applied", "30101")]

        probe.clear_cooldowns()
        mention_reply = await probe.command(
            "赠送称号", "赠送称号 30102 [CQ:at,qq=900002]", actor="900003", number=999721,
        )
        assert "成功给 诊断对手 赠送称号【天选之子】" in mention_reply, mention_reply
        assert probe.rows(
            probe.context.paths.player_db,
            "SELECT unlocked,equipped FROM title WHERE user_id='900002'",
        ) == [( '["30102"]', "")]
        assert probe.rows(
            probe.context.paths.player_db,
            "SELECT result_status,title_id FROM title_transaction_operations WHERE operation_id=?",
            ("title-grant-30102:999721:900002",),
        ) == [("applied", "30102")]

        before_missing = database_projection(probe)
        probe.clear_cooldowns()
        missing_reply = await probe.command(
            "赠送称号", "赠送称号 30101 [CQ:at,qq=900099]", actor="900003", number=999722,
        )
        assert "未找到目标用户" in missing_reply and "成功给" not in missing_reply, missing_reply
        assert database_projection(probe) == before_missing
        assert database_projection(probe)[probe.context.paths.game_db.name] == game_before
        integrity(probe)
        probe.evidence.update(self_default=True, mention_target="900002", missing_mention_zero_writes=True,
                              game_store_unchanged=True, integrity_check="ok")
