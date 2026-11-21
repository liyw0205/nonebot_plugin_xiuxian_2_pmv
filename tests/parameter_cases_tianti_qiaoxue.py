"""Real matcher probes for Tianti qiaoxue persistence."""
from __future__ import annotations

import inspect
import json
import sys
from unittest.mock import patch

from tests.parameter_cases_team_leave import integrity


def _module(probe):
    entry = next(entry for entry in probe.entries if entry["command"] == "冲窍")
    handler = inspect.unwrap(entry["matcher"].handlers[0].call)
    return sys.modules[handler.__module__]


def _seed(probe, user_id="900001", hp=10000):
    from nonebot_plugin_xiuxian_2.features.tianti_training.repository import (
        TiantiBreakthroughSqlRepository,
    )

    player = probe.context.paths.player_db
    TiantiBreakthroughSqlRepository(player).ensure_profile(user_id)
    probe.seed(
        player,
        "UPDATE tianti_info SET tianti_level=?,tianti_hp=?,opened_qiaoxue=?,"
        "opened_qiaoxue_detail=?,qiaoxue_stage_opened=? WHERE user_id=?",
        ("淬体境一重", hp, "[]", "[]", "{}", user_id),
    )


def _projection(probe, operation_id, user_id="900001"):
    player = probe.context.paths.player_db
    return {
        "profile": probe.rows(
            player,
            "SELECT tianti_level,tianti_hp,opened_qiaoxue,opened_qiaoxue_detail "
            "FROM tianti_info WHERE user_id=?",
            (user_id,),
        ),
        "receipts": probe.rows(
            player,
            "SELECT operation_id,user_id,roll,qiaoxue_json,hp_cost,new_hp,opened_count,unlock_limit "
            "FROM tianti_qiaoxue_operations WHERE operation_id=?",
            (operation_id,),
        ),
        "ledger": probe.rows(
            player,
            "SELECT status,result_json FROM operation_ledger WHERE operation_id=?",
            (operation_id,),
        ),
    }


def _recovery_case(
    case,
    *,
    key,
    trigger_name,
    trigger_sql,
    expected_reason=None,
    expected_reason_text="",
    expected_subject="",
    abort_marker="",
):
    known_rejection = expected_reason is not None

    @case(
        key,
        "冲窍",
        "无参数冲窍、单写入点故障、同消息恢复/重放",
        f"隔离 player SQLite 注入 {trigger_name}",
        "故障时保留具体原因并整笔回滚，解除故障后原消息成功且 replay 不重复扣气血",
        group="tianti-qiaoxue-parameters",
        outcome="recovery",
    )
    async def run(probe):
        user_id = "900001"
        player = probe.context.paths.player_db
        _seed(probe, user_id)
        number = 979_980 + len(probe.calls)
        operation_id = f"tianti-qiaoxue:{number}:{user_id}"
        before = _projection(probe, operation_id, user_id)
        probe.seed(player, trigger_sql)
        module = _module(probe)

        with patch.object(module, "_tianti_choice_seed", return_value=0):
            failed = await probe.command("冲窍", number=number, allow_exception=True)

        after_failure = _projection(probe, operation_id, user_id)
        ledger = after_failure["ledger"]
        probe.evidence.update(
            failed=failed,
            before=before,
            after_failure=after_failure,
            operation_id=operation_id,
            ledger=ledger,
        )
        assert after_failure["profile"] == before["profile"], (before, after_failure, failed)
        assert after_failure["receipts"] == before["receipts"], (before, after_failure, failed)
        assert len(ledger) == 1 and ledger[0][0] == "failed", ledger
        failure = json.loads(ledger[0][1])
        if known_rejection:
            assert expected_reason_text in failed and "已撤销" in failed, failed
            assert failure["code"] == expected_reason, failure
            assert failure["data"] == {
                "reason": expected_reason,
                "subject": expected_subject,
            }, failure
        else:
            assert "定位号" in failed and "冲窍成功" not in failed, failed
            assert failure["code"] == "internal_error", failure
            assert abort_marker in str(failure.get("message", "")), failure
            assert abort_marker in json.dumps(probe.calls[-1], ensure_ascii=False)

        probe.seed(player, f"DROP TRIGGER {trigger_name}")
        probe.clear_cooldowns()
        with patch.object(module, "_tianti_choice_seed", return_value=0):
            recovered = await probe.command("冲窍", number=number)
        after_recovery = _projection(probe, operation_id, user_id)
        assert "冲窍成功！" in recovered, recovered
        assert len(after_recovery["receipts"]) == 1, after_recovery
        receipt = after_recovery["receipts"][0]
        card = json.loads(receipt[3])
        profile = after_recovery["profile"][0]
        assert int(profile[1]) == 9000, profile
        assert json.loads(profile[2]) == [card["name"]], profile
        assert int(receipt[2]) == 0 and int(receipt[4]) == 1000, receipt
        assert int(receipt[5]) == 9000 and int(receipt[6]) == 1 and int(receipt[7]) == 3, receipt

        probe.clear_cooldowns()
        with patch.object(module, "_tianti_choice_seed", return_value=0):
            replay = await probe.command("冲窍", number=number)
        assert "已经处理" in replay, replay
        assert _projection(probe, operation_id, user_id) == after_recovery

        probe.clear_cooldowns()
        with patch.object(module, "_tianti_choice_seed", return_value=1):
            conflict = await probe.command("冲窍", number=number)
        assert "该操作号已用于不同内容的请求" in conflict, conflict
        assert _projection(probe, operation_id, user_id) == after_recovery
        integrity(probe)
        probe.evidence.update(
            recovered=recovered,
            after_recovery=after_recovery,
            replay=replay,
            conflict=conflict,
        )

    return run


def _boundary_case(case):
    @case(
        "tianti-qiaoxue-hp-and-unlock-boundaries",
        "冲窍",
        "无参数冲窍的气血与累计开窍上限边界",
        "隔离 player SQLite 分别设置零气血、已达累计3窍上限",
        "沿用既有拒绝消息，边界拒绝不扣气血、不新增窍穴/回执",
        group="tianti-qiaoxue-parameters",
        outcome="success",
    )
    async def run(probe):
        user_id = "900001"
        player = probe.context.paths.player_db
        module = _module(probe)
        _seed(probe, user_id, hp=0)
        low_hp_id = "tianti-qiaoxue:979990:900001"
        low_hp_before = _projection(probe, low_hp_id, user_id)
        low_hp = await probe.command("冲窍", number=979_990)
        assert "炼体气血不足，无法冲窍" in low_hp, low_hp
        after_low_hp = _projection(probe, low_hp_id, user_id)
        assert after_low_hp["profile"] == low_hp_before["profile"]
        assert after_low_hp["receipts"] == []
        assert len(after_low_hp["ledger"]) == 1
        assert json.loads(after_low_hp["ledger"][0][1])["code"] == "hp_insufficient"

        _seed(probe, user_id, hp=10000)
        pool = module.get_qiaoxue_pool()
        opened = [item["name"] for item in pool[:3]]
        assert len(opened) == 3 and len(set(opened)) == 3, opened
        probe.seed(
            player,
            "UPDATE tianti_info SET opened_qiaoxue=?,opened_qiaoxue_detail=? WHERE user_id=?",
            (json.dumps(opened, ensure_ascii=False), "[]", user_id),
        )
        limit_id = "tianti-qiaoxue:979991:900001"
        before_limit = _projection(probe, limit_id, user_id)
        probe.clear_cooldowns()
        limit = await probe.command("冲窍", number=979_991)
        assert "最多可开 3 个窍穴" in limit and "已开启 3 个" in limit, limit
        after_limit = _projection(probe, limit_id, user_id)
        assert after_limit["profile"] == before_limit["profile"]
        assert after_limit["receipts"] == before_limit["receipts"]
        assert len(after_limit["ledger"]) == 1
        assert json.loads(after_limit["ledger"][0][1])["code"] == "limit_reached"
        integrity(probe)
        probe.evidence.update(
            low_hp=low_hp,
            low_hp_projection=low_hp_before,
            after_low_hp=after_low_hp,
            limit=limit,
            limit_projection=before_limit,
            after_limit=after_limit,
        )

    return run


def register(case):
    _recovery_case(
        case,
        key="tianti-qiaoxue-profile-ignore-recovery",
        trigger_name="ignore_tianti_qiaoxue_profile",
        trigger_sql=(
            "CREATE TRIGGER ignore_tianti_qiaoxue_profile BEFORE UPDATE OF opened_qiaoxue "
            "ON tianti_info WHEN OLD.user_id='900001' BEGIN SELECT RAISE(IGNORE); END"
        ),
        expected_reason="tianti_qiaoxue_profile_write_rejected",
        expected_reason_text="冲窍档案保存失败",
        expected_subject="player_profile",
    )
    _recovery_case(
        case,
        key="tianti-qiaoxue-receipt-ignore-recovery",
        trigger_name="ignore_tianti_qiaoxue_receipt",
        trigger_sql=(
            "CREATE TRIGGER ignore_tianti_qiaoxue_receipt BEFORE INSERT "
            "ON tianti_qiaoxue_operations BEGIN SELECT RAISE(IGNORE); END"
        ),
        expected_reason="tianti_qiaoxue_receipt_write_rejected",
        expected_reason_text="冲窍操作回执保存失败",
        expected_subject="receipt",
    )
    _recovery_case(
        case,
        key="tianti-qiaoxue-profile-rewrite-recovery",
        trigger_name="rewrite_tianti_qiaoxue_profile",
        trigger_sql=(
            "CREATE TRIGGER rewrite_tianti_qiaoxue_profile AFTER UPDATE OF opened_qiaoxue "
            "ON tianti_info WHEN NEW.user_id='900001' BEGIN UPDATE tianti_info "
            "SET tianti_hp=CAST(NEW.tianti_hp AS INTEGER)+1 WHERE user_id=NEW.user_id; END"
        ),
        expected_reason="tianti_qiaoxue_profile_projection_mismatch",
        expected_reason_text="冲窍档案与本次开窍结果不一致",
        expected_subject="player_profile",
    )
    _recovery_case(
        case,
        key="tianti-qiaoxue-receipt-rewrite-recovery",
        trigger_name="rewrite_tianti_qiaoxue_receipt",
        trigger_sql=(
            "CREATE TRIGGER rewrite_tianti_qiaoxue_receipt AFTER INSERT "
            "ON tianti_qiaoxue_operations WHEN NEW.user_id='900001' BEGIN UPDATE "
            "tianti_qiaoxue_operations SET new_hp=NEW.new_hp+1 "
            "WHERE operation_id=NEW.operation_id; END"
        ),
        expected_reason="tianti_qiaoxue_receipt_projection_mismatch",
        expected_reason_text="冲窍操作回执与本次开窍结果不一致",
        expected_subject="receipt",
    )
    for subject in ("profile", "receipt"):
        if subject == "profile":
            trigger_name = "abort_tianti_qiaoxue_profile"
            statement = "UPDATE OF opened_qiaoxue ON tianti_info"
        else:
            trigger_name = "abort_tianti_qiaoxue_receipt"
            statement = "INSERT ON tianti_qiaoxue_operations"
        marker = f"isolated tianti qiaoxue {subject} abort"
        _recovery_case(
            case,
            key=f"tianti-qiaoxue-{subject}-abort-recovery",
            trigger_name=trigger_name,
            trigger_sql=(
                f"CREATE TRIGGER {trigger_name} BEFORE {statement} "
                f"BEGIN SELECT RAISE(ABORT, '{marker}'); END"
            ),
            abort_marker=marker,
        )
    _boundary_case(case)
