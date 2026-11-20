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

    @case("title-grant-all-target-recovery", "赠送称号", "all名单中单目标回执拒写、后台汇总与原消息恢复",
          "真实SUPERUSER matcher、真实后台worker、隔离三玩家与player SQLite",
          "失败目标保留具体原因，其余目标继续；解除拒写后同消息补发且重放零写",
          group="title-grant-all-parameters")
    async def all_target_recovery(probe):
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_title as module
        from tests.parameter_cases_team_view import database_projection
        from tests.parameter_cases_team_leave import integrity

        player = probe.context.paths.player_db
        module.title_grant_target_application.repository.temp_directory = probe.context.paths.data_root
        game_before = database_projection(probe)[probe.context.paths.game_db.name]
        probe.seed(player, "INSERT INTO title VALUES('900003','[\"30101\"]','')")
        probe.seed(
            player,
            "CREATE TRIGGER isolated_title_all_receipt BEFORE INSERT ON title_transaction_operations "
            "WHEN NEW.operation_id='title-grant-30101:999730:900002' "
            "BEGIN SELECT RAISE(IGNORE); END",
        )
        try:
            probe.clear_cooldowns()
            partial = await probe.command(
                "赠送称号", "赠送称号 30101 all", actor="900003", number=999730,
            )
            assert "成功1，重复1，失败1" in partial, partial
            partial_states = probe.rows(
                player,
                "SELECT user_id,unlocked FROM title WHERE user_id IN ('900001','900002','900003') ORDER BY user_id",
            )
            partial_receipts = probe.rows(
                player,
                "SELECT operation_id,result_status,title_id FROM title_transaction_operations "
                "WHERE operation_id LIKE 'title-grant-30101:999730:%' ORDER BY operation_id",
            )
            probe.evidence.update(partial_reply=partial, partial_states=partial_states,
                                  partial_receipts=partial_receipts, failure_triggered_target="900002")
            assert "title_grant_receipt_write_rejected" in partial, partial
            assert "玩家900002" in partial and "称号30101" in partial, partial
            assert "赠送称号操作回执保存失败" in partial, partial
            assert probe.rows(
                player,
                "SELECT user_id,unlocked FROM title WHERE user_id IN ('900001','900003') ORDER BY user_id",
            ) == [("900001", '["30101"]'), ("900003", '["30101"]')]
            assert probe.rows(
                player,
                "SELECT COUNT(*) FROM title_transaction_operations WHERE operation_id LIKE 'title-grant-30101:999730:%'",
            ) == [(1,)]
            assert not probe.rows(
                player,
                "SELECT 1 FROM operation_ledger WHERE operation_id='title-grant-30101:999730:900002'",
            )
        finally:
            probe.seed(player, "DROP TRIGGER isolated_title_all_receipt")

        before_recovery = database_projection(probe)
        probe.clear_cooldowns()
        recovered = await probe.command(
            "赠送称号", "赠送称号 30101 all", actor="900003", number=999730,
        )
        assert "成功2，重复1，失败0" in recovered, recovered
        assert probe.rows(
            player,
            "SELECT user_id,unlocked FROM title WHERE user_id IN ('900001','900002','900003') ORDER BY user_id",
        ) == [(uid, '["30101"]') for uid in ("900001", "900002", "900003")]
        assert probe.rows(
            player,
            "SELECT COUNT(*) FROM title_transaction_operations WHERE operation_id LIKE 'title-grant-30101:999730:%'",
        ) == [(2,)]
        committed = database_projection(probe)
        probe.clear_cooldowns()
        replay = await probe.command(
            "赠送称号", "赠送称号 30101 all", actor="900003", number=999730,
        )
        assert "成功2，重复1，失败0" in replay and database_projection(probe) == committed
        assert committed[probe.context.paths.game_db.name] == game_before
        assert before_recovery[probe.context.paths.game_db.name] == game_before
        integrity(probe)
        probe.evidence.update(
            all_targets=3, one_target_recovered=True, known_failure_reason_visible=True,
            original_message_retry=True, replay_zero_writes=True, integrity_check="ok",
        )

    @case("title-grant-all-empty", "赠送称号", "空用户名单不启动后台任务",
          "真实SUPERUSER matcher与隔离空game SQLite名单", "明确回复空名单且两库无业务写",
          group="title-grant-all-parameters")
    async def all_empty(probe):
        from tests.parameter_cases_team_view import database_projection
        from tests.parameter_cases_team_leave import integrity
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_title as module

        game = probe.context.paths.game_db
        module.title_grant_target_application.repository.temp_directory = probe.context.paths.data_root
        probe.seed(game, "DELETE FROM user_xiuxian")
        before = database_projection(probe)
        probe.clear_cooldowns()
        reply = await probe.command("赠送称号", "赠送称号 30101 all", actor="900003", number=999731)
        assert "当前没有可赠送的用户" in reply and "已在后台开始" not in reply, reply
        assert len(probe.calls[-1]["replies"]) == 1
        assert database_projection(probe) == before
        integrity(probe)
        probe.evidence.update(empty_roster=True, backend_not_started=True,
                              game_and_player_zero_writes=True, integrity_check="ok")

    @case("title-grant-all-startup-failure", "赠送称号", "后台任务注册抛错时释放名单快照",
          "真实SUPERUSER matcher、隔离名单与任务启动故障注入", "安全定位回复/原异常日志/双库零写/快照已关闭",
          group="title-grant-all-parameters")
    async def all_startup_failure(probe):
        from unittest.mock import AsyncMock, patch
        from tests.parameter_cases_team_view import database_projection
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_title as module
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import bg_jobs

        application = module.title_grant_target_application
        application.repository.temp_directory = probe.context.paths.data_root
        original_snapshot = application.snapshot
        snapshots = []

        def tracked_snapshot():
            snapshot = original_snapshot()
            snapshots.append(snapshot)
            return snapshot

        before = database_projection(probe)
        probe.clear_cooldowns()
        with (patch.object(application, "snapshot", side_effect=tracked_snapshot),
              patch.object(bg_jobs, "spawn_admin_job",
                           new=AsyncMock(side_effect=RuntimeError("isolated task registration failure")))):
            reply = await probe.command(
                "赠送称号", "赠送称号 30101 all", actor="900003", number=999732,
                allow_exception=True,
            )
        assert "赠送称号处理提示" in reply and "定位号：" in reply, reply
        assert "isolated task registration failure" not in reply
        assert snapshots and snapshots[0]._stream.closed
        assert database_projection(probe) == before
        assert any("isolated task registration failure" in row["exception"]
                   for row in probe.calls[-1]["logs"])
        probe.evidence.update(startup_failure_safe_reply=True, exception_traceback_logged=True,
                              snapshot_closed=True, game_and_player_zero_writes=True)

    @case("title-grant-all-interrupted-recovery", "赠送称号", "处理中名单读取中断后原消息恢复剩余目标",
          "真实后台worker、隔离三玩家与首次迭代故障注入", "安全部分汇总/已处理数据保留/原消息补齐且重放零写",
          group="title-grant-all-parameters")
    async def all_interrupted_recovery(probe):
        from unittest.mock import patch
        from tests.parameter_cases_team_view import database_projection
        from tests.parameter_cases_team_leave import integrity
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_title as module

        application = module.title_grant_target_application
        application.repository.temp_directory = probe.context.paths.data_root
        original_snapshot = application.snapshot
        calls = 0

        class InterruptedSnapshot:
            def __init__(self, snapshot):
                self.snapshot = snapshot
                self.count = snapshot.count

            def iter_user_ids(self):
                users = iter(self.snapshot.iter_user_ids())
                yield next(users)
                raise OSError("isolated title grant roster interruption")

            def close(self):
                self.snapshot.close()

        def interrupt_first_snapshot():
            nonlocal calls
            snapshot = original_snapshot()
            calls += 1
            return InterruptedSnapshot(snapshot) if calls == 1 else snapshot

        game_before = database_projection(probe)[probe.context.paths.game_db.name]
        probe.clear_cooldowns()
        with patch.object(application, "snapshot", side_effect=interrupt_first_snapshot):
            interrupted = await probe.command(
                "赠送称号", "赠送称号 30101 all", actor="900003", number=999733,
                allow_exception=True,
            )
        assert "部分中断" in interrupted and "已处理1/3" in interrupted and "失败1" in interrupted, interrupted
        assert "reason=internal_error" in interrupted and "定位号：" in interrupted, interrupted
        assert "isolated title grant roster interruption" not in interrupted
        assert probe.rows(
            probe.context.paths.player_db,
            "SELECT user_id,unlocked FROM title ORDER BY user_id",
        ) == [("900001", '["30101"]')]
        assert probe.rows(probe.context.paths.player_db,
                          "SELECT COUNT(*) FROM title_transaction_operations") == [(1,)]
        assert any("isolated title grant roster interruption" in row["exception"]
                   for row in probe.calls[-1]["logs"])

        probe.clear_cooldowns()
        recovered = await probe.command(
            "赠送称号", "赠送称号 30101 all", actor="900003", number=999733,
        )
        assert "处理结束：已处理3/3" in recovered and "成功3，重复0，失败0" in recovered, recovered
        assert probe.rows(
            probe.context.paths.player_db,
            "SELECT user_id,unlocked FROM title ORDER BY user_id",
        ) == [(uid, '["30101"]') for uid in ("900001", "900002", "900003")]
        assert probe.rows(probe.context.paths.player_db,
                          "SELECT COUNT(*) FROM title_transaction_operations") == [(3,)]
        committed = database_projection(probe)
        probe.clear_cooldowns()
        replay = await probe.command(
            "赠送称号", "赠送称号 30101 all", actor="900003", number=999733,
        )
        assert "成功3，重复0，失败0" in replay and database_projection(probe) == committed
        assert committed[probe.context.paths.game_db.name] == game_before
        integrity(probe)
        probe.evidence.update(interruption_reported_safely=True, partial_target_preserved=True,
                              same_message_completed_remaining=True, replay_zero_writes=True,
                              integrity_check="ok")

    @case("title-grant-all-read-error-before-first-target", "赠送称号", "名单首个ID读取失败不可误报空名单",
          "真实后台worker、隔离三玩家与首次读取故障注入", "安全中断提示/双库零写/原消息恢复",
          group="title-grant-all-parameters")
    async def all_read_error_before_first_target(probe):
        from unittest.mock import patch
        from tests.parameter_cases_team_view import database_projection
        from tests.parameter_cases_team_leave import integrity
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_title as module

        application = module.title_grant_target_application
        application.repository.temp_directory = probe.context.paths.data_root
        original_snapshot = application.snapshot
        calls = 0

        class FailingSnapshot:
            def __init__(self, snapshot):
                self.snapshot = snapshot
                self.count = snapshot.count

            def iter_user_ids(self):
                raise OSError("isolated title roster failed before first id")
                yield ""

            def close(self):
                self.snapshot.close()

        def fail_first_read():
            nonlocal calls
            snapshot = original_snapshot()
            calls += 1
            return FailingSnapshot(snapshot) if calls == 1 else snapshot

        before = database_projection(probe)
        probe.clear_cooldowns()
        with patch.object(application, "snapshot", side_effect=fail_first_read):
            failed = await probe.command(
                "赠送称号", "赠送称号 30101 all", actor="900003", number=999735,
                allow_exception=True,
            )
        assert "部分中断" in failed and "已处理0/3" in failed and "失败1" in failed, failed
        assert "当前没有可赠送的用户" not in failed
        assert "reason=internal_error" in failed and "定位号：" in failed
        assert "isolated title roster failed before first id" not in failed
        assert database_projection(probe) == before
        assert any("isolated title roster failed before first id" in row["exception"]
                   for row in probe.calls[-1]["logs"])

        probe.clear_cooldowns()
        recovered = await probe.command(
            "赠送称号", "赠送称号 30101 all", actor="900003", number=999735,
        )
        assert "处理结束：已处理3/3" in recovered and "成功3，重复0，失败0" in recovered, recovered
        assert probe.rows(
            probe.context.paths.player_db,
            "SELECT user_id,unlocked FROM title ORDER BY user_id",
        ) == [(uid, '["30101"]') for uid in ("900001", "900002", "900003")]
        committed = database_projection(probe)
        probe.clear_cooldowns()
        replay = await probe.command(
            "赠送称号", "赠送称号 30101 all", actor="900003", number=999735,
        )
        assert "成功3，重复0，失败0" in replay and database_projection(probe) == committed
        integrity(probe)
        probe.evidence.update(first_target_read_error_not_empty_roster=True,
                              same_message_recovery=True, replay_zero_writes=True,
                              integrity_check="ok")

    @case("title-grant-all-target-abort-recovery", "赠送称号", "单目标SQLite ABORT隔离失败并恢复",
          "真实后台worker、隔离三玩家与目标title INSERT触发器", "其余目标继续/错误定位与traceback/原消息恢复",
          group="title-grant-all-parameters")
    async def all_target_abort_recovery(probe):
        from tests.parameter_cases_team_view import database_projection
        from tests.parameter_cases_team_leave import integrity
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_title as module

        player = probe.context.paths.player_db
        module.title_grant_target_application.repository.temp_directory = probe.context.paths.data_root
        game_before = database_projection(probe)[probe.context.paths.game_db.name]
        probe.seed(
            player,
            "CREATE TRIGGER isolated_title_all_abort BEFORE INSERT ON title "
            "WHEN NEW.user_id='900002' "
            "BEGIN SELECT RAISE(ABORT,'isolated all title grant abort'); END",
        )
        try:
            probe.clear_cooldowns()
            partial = await probe.command(
                "赠送称号", "赠送称号 30101 all", actor="900003", number=999734,
                allow_exception=True,
            )
            assert "已处理3/3" in partial and "成功2，重复0，失败1" in partial, partial
            assert "玩家900002 / 称号30101" in partial, partial
            assert "reason=internal_error" in partial and "定位号：" in partial, partial
            assert "isolated all title grant abort" not in partial
            assert probe.rows(
                player,
                "SELECT user_id,unlocked FROM title ORDER BY user_id",
            ) == [("900001", '["30101"]'), ("900003", '["30101"]')]
            assert probe.rows(player, "SELECT COUNT(*) FROM title_transaction_operations") == [(2,)]
            assert any("isolated all title grant abort" in row["exception"]
                       for row in probe.calls[-1]["logs"])
        finally:
            probe.seed(player, "DROP TRIGGER isolated_title_all_abort")

        probe.clear_cooldowns()
        recovered = await probe.command(
            "赠送称号", "赠送称号 30101 all", actor="900003", number=999734,
        )
        assert "处理结束：已处理3/3" in recovered and "成功3，重复0，失败0" in recovered, recovered
        assert probe.rows(
            player,
            "SELECT user_id,unlocked FROM title ORDER BY user_id",
        ) == [(uid, '["30101"]') for uid in ("900001", "900002", "900003")]
        assert probe.rows(player, "SELECT COUNT(*) FROM title_transaction_operations") == [(3,)]
        committed = database_projection(probe)
        probe.clear_cooldowns()
        replay = await probe.command(
            "赠送称号", "赠送称号 30101 all", actor="900003", number=999734,
        )
        assert "成功3，重复0，失败0" in replay and database_projection(probe) == committed
        assert committed[probe.context.paths.game_db.name] == game_before
        integrity(probe)
        probe.evidence.update(target_abort_logged=True, target_isolated=True,
                              same_message_recovery=True, replay_zero_writes=True,
                              integrity_check="ok")
