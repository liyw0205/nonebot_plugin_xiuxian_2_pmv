"""Real matcher contracts for Tianti breakthrough persistence."""
from __future__ import annotations

import json
import inspect
import sys
from unittest.mock import patch

from tests.parameter_cases_team_leave import integrity


def _handler_module(probe):
    entry = next(entry for entry in probe.entries if entry["command"] == "炼体突破")
    handler = inspect.unwrap(entry["matcher"].handlers[0].call)
    return sys.modules[handler.__module__]


def _seed_ready_profile(probe, user_id="900001", *, hp=12000, level="淬体境一重"):
    from nonebot_plugin_xiuxian_2.features.tianti_training.repository import (
        TiantiBreakthroughSqlRepository,
    )

    repository = TiantiBreakthroughSqlRepository(probe.context.paths.player_db)
    repository.ensure_profile(user_id)
    probe.seed(
        probe.context.paths.player_db,
        "UPDATE tianti_info SET tianti_level=?,tianti_hp=? WHERE user_id=?",
        (level, hp, user_id),
    )


def _projection(probe, user_id="900001"):
    player = probe.context.paths.player_db
    return {
        "profile": probe.rows(
            player,
            "SELECT tianti_level,tianti_hp FROM tianti_info WHERE user_id=?",
            (user_id,),
        ),
        "receipts": probe.rows(
            player,
            "SELECT operation_id,user_id,cultivation_rank,roll_success,old_level,"
            "new_level,hp_cost,new_hp,success FROM tianti_breakthrough_operations "
            "WHERE user_id=? ORDER BY operation_id",
            (user_id,),
        ),
        "ledger": probe.rows(
            player,
            "SELECT operation_id,status,result_json FROM operation_ledger "
            "WHERE operation_id LIKE 'tianti-break:%' ORDER BY operation_id",
        ),
    }


def _recovery_case(
    case,
    *,
    key,
    trigger_name,
    trigger_sql,
    expected_reason,
    expected_reason_text,
    expected_subject,
):
    @case(
        key,
        "炼体突破",
        "无参数突破、单个持久化写点拒绝、同消息恢复/重放",
        f"隔离 player SQLite 注入 {trigger_name}",
        f"应拒绝并完整回滚，记录 {expected_reason}/{expected_subject}，恢复后只突破一次",
        group="tianti-breakthrough-parameters",
        outcome="recovery",
    )
    async def run(probe):
        user_id = "900001"
        player = probe.context.paths.player_db
        _seed_ready_profile(probe, user_id)
        before = _projection(probe, user_id)
        number = 979_970 + len(probe.calls)
        operation_id = f"tianti-break:{number}:{user_id}"
        probe.seed(player, trigger_sql)
        module = _handler_module(probe)

        with (
            patch.object(module, "_tianti_choice_seed", return_value=0),
            patch.object(module, "get_tianti_level_index", return_value=0),
        ):
            failed = await probe.command("炼体突破", number=number, allow_exception=True)

        after_failure = _projection(probe, user_id)
        ledger = probe.rows(
            player,
            "SELECT status,result_json FROM operation_ledger WHERE operation_id=?",
            (operation_id,),
        )
        probe.evidence.update(failed=failed, before=before, after_failure=after_failure, ledger=ledger)
        assert expected_reason_text in failed and "已撤销" in failed, failed
        assert after_failure["profile"] == before["profile"], (before, after_failure, failed)
        assert after_failure["receipts"] == before["receipts"], (before, after_failure, failed)
        assert ledger and ledger[0][0] == "failed", ledger
        failure = json.loads(ledger[0][1])
        assert failure["code"] == expected_reason, failure
        assert failure["data"] == {"reason": expected_reason, "subject": expected_subject}, failure

        probe.seed(player, f"DROP TRIGGER {trigger_name}")
        probe.clear_cooldowns()
        with (
            patch.object(module, "_tianti_choice_seed", return_value=0),
            patch.object(module, "get_tianti_level_index", return_value=0),
        ):
            recovered = await probe.command("炼体突破", number=number)
        after_recovery = _projection(probe, user_id)
        assert "炼体突破成功" in recovered, recovered
        assert after_recovery["profile"] == [("淬体境二重", "11400")], after_recovery
        assert len(after_recovery["receipts"]) == 1, after_recovery

        probe.clear_cooldowns()
        with (
            patch.object(module, "_tianti_choice_seed", return_value=0),
            patch.object(module, "get_tianti_level_index", return_value=0),
        ):
            replay = await probe.command("炼体突破", number=number)
        assert "已经处理" in replay, replay
        assert _projection(probe, user_id) == after_recovery

        probe.clear_cooldowns()
        with (
            patch.object(module, "_tianti_choice_seed", return_value=0),
            patch.object(module, "get_tianti_level_index", return_value=1),
        ):
            conflict = await probe.command("炼体突破", number=number)
        assert "该操作号已用于不同内容的请求" in conflict, conflict
        assert _projection(probe, user_id) == after_recovery
        integrity(probe)
        probe.evidence.update(
            recovered=recovered,
            replay=replay,
            conflict=conflict,
            after_recovery=after_recovery,
        )

    return run


def _abort_recovery_case(case, *, key, trigger_name, statement, subject):
    @case(
        key,
        "炼体突破",
        "无参数突破、SQLite ABORT、同消息恢复",
        f"隔离 player SQLite 对 {subject} 写入注入 RAISE(ABORT)",
        "业务投影完整回滚，回复含定位号且日志保留 traceback，解除故障后同消息恢复",
        group="tianti-breakthrough-parameters",
        outcome="recovery",
    )
    async def run(probe):
        user_id = "900001"
        player = probe.context.paths.player_db
        _seed_ready_profile(probe, user_id)
        before = _projection(probe, user_id)
        number = 979_990 + len(probe.calls)
        operation_id = f"tianti-break:{number}:{user_id}"
        probe.seed(
            player,
            f"CREATE TRIGGER {trigger_name} BEFORE {statement} "
            f"BEGIN SELECT RAISE(ABORT, 'isolated tianti breakthrough {subject} abort'); END",
        )
        module = _handler_module(probe)
        with (
            patch.object(module, "_tianti_choice_seed", return_value=0),
            patch.object(module, "get_tianti_level_index", return_value=0),
        ):
            failed = await probe.command("炼体突破", number=number, allow_exception=True)
        failure_call = probe.calls[-1]
        after_failure = _projection(probe, user_id)
        ledger = probe.rows(
            player,
            "SELECT status,result_json FROM operation_ledger WHERE operation_id=?",
            (operation_id,),
        )
        log_text = json.dumps(failure_call.get("logs", []), ensure_ascii=False)
        probe.evidence.update(failed=failed, logs=log_text, before=before, after_failure=after_failure, ledger=ledger)
        assert "炼体突破处理提示：" in failed and "定位号：" in failed, failed
        assert f"isolated tianti breakthrough {subject} abort" in log_text, log_text
        tracebacks = "\n".join(failure_call.get("errors", []))
        assert "Traceback" in tracebacks and f"isolated tianti breakthrough {subject} abort" in tracebacks, tracebacks
        assert after_failure["profile"] == before["profile"], (before, after_failure)
        assert after_failure["receipts"] == before["receipts"], (before, after_failure)
        assert ledger and ledger[0][0] == "failed", ledger
        failure = json.loads(ledger[0][1])
        assert failure["code"] == "internal_error", failure

        probe.seed(player, f"DROP TRIGGER {trigger_name}")
        probe.clear_cooldowns()
        with (
            patch.object(module, "_tianti_choice_seed", return_value=0),
            patch.object(module, "get_tianti_level_index", return_value=0),
        ):
            recovered = await probe.command("炼体突破", number=number)
        after_recovery = _projection(probe, user_id)
        assert "炼体突破成功" in recovered, recovered
        assert after_recovery["profile"] == [("淬体境二重", "11400")], after_recovery
        assert len(after_recovery["receipts"]) == 1, after_recovery

        probe.clear_cooldowns()
        with (
            patch.object(module, "_tianti_choice_seed", return_value=0),
            patch.object(module, "get_tianti_level_index", return_value=0),
        ):
            replay = await probe.command("炼体突破", number=number)
        assert "已经处理" in replay, replay
        assert _projection(probe, user_id) == after_recovery
        integrity(probe)
        probe.evidence.update(recovered=recovered, replay=replay, after_recovery=after_recovery)

    return run


def _ready_roll_and_rejection_boundaries(case):
    @case(
        "tianti-breakthrough-domain-boundaries",
        "炼体突破",
        "气血门槛/修仙境界/随机失败",
        "真实 matcher，无参数；隔离 profile 分别处于门槛下、修仙境界不足和可突破状态",
        "保留既有拒绝文案；随机失败按原规则扣气血、境界不变且写入失败回执",
        group="tianti-breakthrough-parameters",
        outcome="success",
    )
    async def run(probe):
        user_id = "900001"
        player = probe.context.paths.player_db
        _seed_ready_profile(probe, user_id, hp=11999)
        module = _handler_module(probe)

        with (
            patch.object(module, "_tianti_choice_seed", return_value=0),
            patch.object(module, "get_tianti_level_index", return_value=0),
        ):
            low_hp = await probe.command("炼体突破", number=979_960)
        assert "炼体气血不足" in low_hp, low_hp
        assert _projection(probe, user_id)["profile"] == [("淬体境一重", "11999")]
        assert _projection(probe, user_id)["receipts"] == []

        probe.seed(player, "UPDATE tianti_info SET tianti_hp=? WHERE user_id=?", (12000, user_id))
        probe.clear_cooldowns()
        with (
            patch.object(module, "_tianti_choice_seed", return_value=0),
            patch.object(module, "get_tianti_level_index", return_value=100000),
        ):
            low_rank = await probe.command("炼体突破", number=979_961)
        assert "修仙境界不足" in low_rank, low_rank
        assert _projection(probe, user_id)["profile"] == [("淬体境一重", "12000")]
        assert _projection(probe, user_id)["receipts"] == []

        probe.clear_cooldowns()
        with (
            patch.object(module, "_tianti_choice_seed", return_value=1),
            patch.object(module, "get_tianti_level_index", return_value=0),
        ):
            roll_failure = await probe.command("炼体突破", number=979_962)
        state = _projection(probe, user_id)
        assert "炼体突破失败" in roll_failure, roll_failure
        assert state["profile"] == [("淬体境一重", "11400")], state
        assert len(state["receipts"]) == 1, state
        assert state["receipts"][0][6:] == (600, 11400, 0), state["receipts"]

        from nonebot_plugin_xiuxian_2.features.tianti_training.repository import (
            TiantiProfileReader,
        )

        levels = TiantiProfileReader(probe.context.paths.data).levels()
        highest = max(levels, key=lambda name: int(levels[name].get("rank", 0)))
        _seed_ready_profile(probe, user_id, hp=0, level=highest)
        before_max = _projection(probe, user_id)
        probe.clear_cooldowns()
        maximum = await probe.command("炼体突破", number=979_963)
        max_state = _projection(probe, user_id)
        assert "你的炼体已达最高境界" in maximum, maximum
        assert max_state["profile"] == [(highest, "0")], max_state
        assert max_state["receipts"] == before_max["receipts"], max_state
        integrity(probe)
        probe.evidence.update(
            low_hp=low_hp,
            low_rank=low_rank,
            roll_failure=roll_failure,
            roll_failure_state=state,
            max_level=maximum,
            max_state=max_state,
        )

    return run


def register(case):
    _recovery_case(
        case,
        key="tianti-breakthrough-profile-ignore-recovery",
        trigger_name="ignore_tianti_breakthrough_profile",
        trigger_sql=(
            "CREATE TRIGGER ignore_tianti_breakthrough_profile BEFORE UPDATE OF tianti_level "
            "ON tianti_info WHEN OLD.user_id='900001' BEGIN SELECT RAISE(IGNORE); END"
        ),
        expected_reason="tianti_breakthrough_profile_write_rejected",
        expected_reason_text="炼体突破档案保存失败",
        expected_subject="player_profile",
    )
    _recovery_case(
        case,
        key="tianti-breakthrough-receipt-ignore-recovery",
        trigger_name="ignore_tianti_breakthrough_receipt",
        trigger_sql=(
            "CREATE TRIGGER ignore_tianti_breakthrough_receipt BEFORE INSERT "
            "ON tianti_breakthrough_operations BEGIN SELECT RAISE(IGNORE); END"
        ),
        expected_reason="tianti_breakthrough_receipt_write_rejected",
        expected_reason_text="炼体突破回执保存失败",
        expected_subject="receipt",
    )
    _recovery_case(
        case,
        key="tianti-breakthrough-profile-rewrite-recovery",
        trigger_name="rewrite_tianti_breakthrough_profile",
        trigger_sql=(
            "CREATE TRIGGER rewrite_tianti_breakthrough_profile AFTER UPDATE OF tianti_level "
            "ON tianti_info WHEN NEW.user_id='900001' BEGIN UPDATE tianti_info "
            "SET tianti_hp=CAST(NEW.tianti_hp AS INTEGER)+3 WHERE user_id=NEW.user_id; END"
        ),
        expected_reason="tianti_breakthrough_profile_projection_mismatch",
        expected_reason_text="炼体档案与突破结果不一致",
        expected_subject="player_profile",
    )
    _recovery_case(
        case,
        key="tianti-breakthrough-receipt-rewrite-recovery",
        trigger_name="rewrite_tianti_breakthrough_receipt",
        trigger_sql=(
            "CREATE TRIGGER rewrite_tianti_breakthrough_receipt AFTER INSERT "
            "ON tianti_breakthrough_operations BEGIN UPDATE tianti_breakthrough_operations "
            "SET new_hp=new_hp+1 WHERE operation_id=NEW.operation_id; END"
        ),
        expected_reason="tianti_breakthrough_receipt_projection_mismatch",
        expected_reason_text="炼体突破回执与本次结果不一致",
        expected_subject="receipt",
    )
    _abort_recovery_case(
        case,
        key="tianti-breakthrough-profile-abort-recovery",
        trigger_name="abort_tianti_breakthrough_profile",
        statement="UPDATE OF tianti_level ON tianti_info",
        subject="profile",
    )
    _abort_recovery_case(
        case,
        key="tianti-breakthrough-receipt-abort-recovery",
        trigger_name="abort_tianti_breakthrough_receipt",
        statement="INSERT ON tianti_breakthrough_operations",
        subject="receipt",
    )
    _ready_roll_and_rejection_boundaries(case)
