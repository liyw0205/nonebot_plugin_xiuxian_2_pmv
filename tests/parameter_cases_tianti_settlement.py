"""Real matcher probes for Tianti elapsed settlement persistence."""
from __future__ import annotations

import inspect
import json
import sys
from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import patch


def _module(probe):
    entry = next(entry for entry in probe.entries if entry["command"] == "炼体结算")
    handler = inspect.unwrap(entry["matcher"].handlers[0].call)
    return sys.modules[handler.__module__]


def _seed(probe, user_id="900001", *, last_settlement="2026-11-21 10:00:00", hp=1000):
    from nonebot_plugin_xiuxian_2.features.tianti_training.repository import (
        TiantiBreakthroughSqlRepository,
    )

    player = probe.context.paths.player_db
    TiantiBreakthroughSqlRepository(player).ensure_profile(user_id)
    probe.seed(
        player,
        "UPDATE tianti_info SET tianti_level=?,tianti_hp=?,last_settle_time=?,"
        "medicine_last_time=NULL,medicine_end_time=NULL,medicine_effect=0,medicine_name='' "
        "WHERE user_id=?",
        ("淬体境一重", hp, last_settlement, user_id),
    )


def _projection(probe, operation_id, user_id="900001"):
    player = probe.context.paths.player_db
    return {
        "profile": probe.rows(
            player,
            "SELECT tianti_level,tianti_hp,last_settle_time FROM tianti_info WHERE user_id=?",
            (user_id,),
        ),
        "receipts": probe.rows(
            player,
            "SELECT operation_id,user_id,sect_level,result_status,detail_json "
            "FROM tianti_settlement_operations WHERE operation_id=?",
            (operation_id,),
        ),
        "ledger": probe.rows(
            player,
            "SELECT status,result_json FROM operation_ledger WHERE operation_id=?",
            (operation_id,),
        ),
        "audit_count": probe.rows(
            player,
            "SELECT COUNT(*) FROM operation_audit WHERE operation_id=? AND action='tianti.settle'",
            (operation_id,),
        ),
    }


def _assert_integrity(probe):
    for manager in (probe.managers[0], probe.managers[1], probe.managers[3]):
        assert manager.conn.execute("PRAGMA integrity_check").fetchone() == ("ok",)


async def _dispatch(
    probe, module, now, number, *, actor="900001", allow_exception=False
):
    probe.clear_cooldowns()
    fixed_clock = SimpleNamespace(now=lambda: now)
    with patch.object(module, "runtime_clock", fixed_clock):
        return await probe.command(
            "炼体结算", number=number, actor=actor, allow_exception=allow_exception
        )


def register(case):
    @case(
        "tianti-settlement-clock-window",
        "炼体结算",
        "冷态初始化、59秒空结算、60分钟合法结算及同消息重放",
        "隔离 player SQLite 与可控业务时钟",
        "检查实际档案、永久回执、ledger、回复和重复请求投影",
        group="tianti-settlement-parameters",
    )
    async def clock_window(probe):
        user_id = "900001"
        player = probe.context.paths.player_db
        module = _module(probe)
        start = datetime(2026, 11, 21, 10, 0, 0)
        probe.seed(player, "DELETE FROM tianti_info WHERE user_id=?", (user_id,))

        init_id = "tianti-settle:979810:900001"
        init_reply = await _dispatch(probe, module, start, 979810)
        assert "已初始化炼体计时" in init_reply, init_reply
        initialized = _projection(probe, init_id, user_id)
        assert initialized["receipts"][0][3] == "init", initialized
        assert initialized["profile"][0][2] == start.strftime("%Y-%m-%d %H:%M:%S"), initialized

        await _dispatch(probe, module, start, 979810)
        assert _projection(probe, init_id, user_id) == initialized

        empty_id = "tianti-settle:979811:900001"
        empty_reply = await _dispatch(probe, module, start + timedelta(seconds=59), 979811)
        assert "时间太短，暂无可结算炼体收益" in empty_reply, empty_reply
        empty = _projection(probe, empty_id, user_id)
        assert empty["receipts"][0][3] == "empty", empty
        assert empty["profile"][0][2] == start.strftime("%Y-%m-%d %H:%M:%S"), empty

        settle_id = "tianti-settle:979812:900001"
        settle_reply = await _dispatch(probe, module, start + timedelta(minutes=60), 979812)
        assert "本次获得炼体气血" in settle_reply, settle_reply
        settled = _projection(probe, settle_id, user_id)
        detail = json.loads(settled["receipts"][0][4])
        assert detail["status"] == "ok" and detail["mins"] == 60, detail
        assert int(settled["profile"][0][1]) == int(detail["new_hp"]), settled
        await _dispatch(probe, module, start + timedelta(minutes=60), 979812)
        assert _projection(probe, settle_id, user_id) == settled

        game = probe.managers[0]
        isolated_player = game.conn.execute(
            "UPDATE user_xiuxian SET user_name=?,level=?,exp=?,stone=?,power=?,root_level=? "
            "WHERE user_id=?",
            ("隔离炼体玩家", "江湖好手", 1000, 0, 1000, 0, "900002"),
        )
        assert isolated_player.rowcount == 1
        game.conn.commit()
        game._clear_read_cache()
        _seed(probe, "900002", last_settlement="2026-11-21 10:00:00", hp=1000)
        first_user_before = probe.rows(
            player,
            "SELECT tianti_level,tianti_hp,last_settle_time FROM tianti_info WHERE user_id='900001'",
        )
        isolated_id = "tianti-settle:979814:900002"
        isolated_reply = await _dispatch(
            probe,
            module,
            start + timedelta(hours=1),
            979814,
            actor="900002",
        )
        isolated = _projection(probe, isolated_id, "900002")
        first_user_after = probe.rows(
            player,
            "SELECT tianti_level,tianti_hp,last_settle_time FROM tianti_info WHERE user_id='900001'",
        )
        assert "本次获得炼体气血" in isolated_reply, isolated_reply
        assert isolated["receipts"][0][1] == "900002", isolated
        assert int(isolated["profile"][0][1]) == int(
            json.loads(isolated["receipts"][0][4])["new_hp"]
        ), isolated
        assert first_user_after == first_user_before, (first_user_before, first_user_after)
        _assert_integrity(probe)
        probe.evidence.update(
            initialized=initialized,
            empty=empty,
            settled=settled,
            isolated=isolated,
            replay_unchanged=True,
        )

    @case(
        "tianti-settlement-active-bonuses",
        "炼体结算",
        "60分钟结算且药浴、宗门灵脉均有效",
        "隔离档案药浴在11:00仍有效，业务时钟固定于11:00",
        "回执、档案与真实回复反映药浴/宗门/灵脉加成",
        group="tianti-settlement-parameters",
    )
    async def active_bonuses(probe):
        from nonebot_plugin_xiuxian_2.features.tianti_training.repository import (
            TiantiBreakthroughSqlRepository,
        )

        user_id = "900001"
        player = probe.context.paths.player_db
        module = _module(probe)
        now = datetime(2026, 11, 21, 11, 0, 0)
        _seed(probe, user_id, hp=1000)
        TiantiBreakthroughSqlRepository(player).ensure_profile(user_id)
        probe.seed(
            player,
            "UPDATE tianti_info SET medicine_last_time=?,medicine_end_time=?,"
            "medicine_effect=?,medicine_name=? WHERE user_id=?",
            ("2026-11-21 05:00:00", "2026-11-21 11:30:00", 1.5, "灵药浴", user_id),
        )
        operation_id = "tianti-settle:979813:900001"
        with patch.object(module, "_get_user_sect_fairyland_level", return_value=2), patch.object(
            module.tianti_settlement_application,
            "spirit_vein_multiplier",
            lambda: 1.2,
        ):
            reply = await _dispatch(probe, module, now, 979813)
        actual = _projection(probe, operation_id, user_id)
        detail = json.loads(actual["receipts"][0][4])
        assert "药浴加成：灵药浴" in reply, reply
        assert detail["bath"]["name"] == "灵药浴" and detail["bath"]["effect"] == 1.5, detail
        assert detail["sect_bonus"] == 0.1, detail
        assert abs(detail["spirit_vein_bonus"] - 0.2) < 1e-9, detail
        assert int(actual["profile"][0][1]) == int(detail["new_hp"]), actual
        _assert_integrity(probe)
        probe.evidence.update(reply=reply, detail=detail, actual=actual)

    trigger_specs = (
        (
            "profile-ignore",
            "CREATE TRIGGER ignore_tianti_settlement_profile BEFORE UPDATE OF tianti_hp "
            "ON tianti_info WHEN OLD.user_id='900001' BEGIN SELECT RAISE(IGNORE); END",
            "tianti_settlement_profile_write_rejected",
            "player_profile",
            "",
        ),
        (
            "profile-after-rewrite",
            "CREATE TRIGGER rewrite_tianti_settlement_profile AFTER UPDATE OF tianti_hp "
            "ON tianti_info WHEN NEW.user_id='900001' BEGIN UPDATE tianti_info "
            "SET tianti_hp=NEW.tianti_hp+1 WHERE user_id=NEW.user_id; END",
            "tianti_settlement_profile_projection_mismatch",
            "player_profile",
            "",
        ),
        (
            "receipt-ignore",
            "CREATE TRIGGER ignore_tianti_settlement_receipt BEFORE INSERT "
            "ON tianti_settlement_operations BEGIN SELECT RAISE(IGNORE); END",
            "tianti_settlement_receipt_write_rejected",
            "receipt",
            "",
        ),
        (
            "receipt-after-rewrite",
            "CREATE TRIGGER rewrite_tianti_settlement_receipt AFTER INSERT "
            "ON tianti_settlement_operations BEGIN UPDATE tianti_settlement_operations "
            "SET detail_json='{}' WHERE operation_id=NEW.operation_id; END",
            "tianti_settlement_receipt_projection_mismatch",
            "receipt",
            "",
        ),
        (
            "profile-after-receipt-rewrite",
            "CREATE TRIGGER rewrite_tianti_settlement_profile_after_receipt AFTER INSERT "
            "ON tianti_settlement_operations BEGIN UPDATE tianti_info "
            "SET tianti_hp=tianti_hp+1 WHERE user_id=NEW.user_id; END",
            "tianti_settlement_profile_projection_mismatch",
            "player_profile",
            "",
        ),
        (
            "profile-abort",
            "CREATE TRIGGER abort_tianti_settlement_profile BEFORE UPDATE OF tianti_hp "
            "ON tianti_info WHEN OLD.user_id='900001' "
            "BEGIN SELECT RAISE(ABORT,'tianti settlement profile aborted'); END",
            "",
            "player_profile",
            "tianti settlement profile aborted",
        ),
        (
            "receipt-abort",
            "CREATE TRIGGER abort_tianti_settlement_receipt BEFORE INSERT "
            "ON tianti_settlement_operations "
            "BEGIN SELECT RAISE(ABORT,'tianti settlement receipt aborted'); END",
            "",
            "receipt",
            "tianti settlement receipt aborted",
        ),
    )

    for index, (label, trigger_sql, reason, subject, abort_text) in enumerate(trigger_specs):
        @case(
            f"tianti-settlement-{label}-recovery",
            "炼体结算",
            f"有效60分钟结算，注入{label}写入故障后恢复同消息",
            "隔离 player SQLite 与可控业务时钟，记录故障前后实际投影",
            "拒绝时不得伪成功；具体原因/回滚或异常日志、解除故障后重试并重放",
            group="tianti-settlement-parameters",
            outcome="recovery",
        )
        async def write_fault(
            probe,
            *,
            index=index,
            label=label,
            trigger_sql=trigger_sql,
            reason=reason,
            subject=subject,
            abort_text=abort_text,
        ):
            from nonebot_plugin_xiuxian_2.core.player_reasons import reason_text

            user_id = "900001"
            number = 979820 + index
            operation_id = f"tianti-settle:{number}:{user_id}"
            player = probe.context.paths.player_db
            _seed(probe, user_id)
            before = _projection(probe, operation_id, user_id)
            trigger_name = f"tianti_settlement_fault_{index}"
            trigger_body = trigger_sql.split(" ", 3)[3]
            probe.seed(player, f"CREATE TRIGGER {trigger_name} {trigger_body}")
            module = _module(probe)
            now = datetime(2026, 11, 21, 11, 0, 0)
            reply = await _dispatch(
                probe, module, now, number, allow_exception=not bool(reason)
            )
            failed = _projection(probe, operation_id, user_id)

            assert failed["profile"] == before["profile"], (label, before, failed, reply)
            assert failed["receipts"] == [], (label, failed, reply)
            if reason:
                assert reason_text(reason) in reply, (label, reason, reply)
                subject_label = "玩家档案" if subject == "player_profile" else "操作回执"
                assert subject_label in reply, (label, subject, reply)
                assert failed["ledger"] == [], (label, failed)
            else:
                failure = probe.calls[-1]
                traces = json.dumps(failure["errors"], ensure_ascii=False)
                logs = json.dumps(failure["logs"], ensure_ascii=False)
                assert abort_text in traces and "Traceback" in traces, (label, traces, logs)
                assert not failed["ledger"] or failed["ledger"][0][0] == "failed", (label, failed)
                assert "本次获得炼体气血" not in reply, (label, reply)

            probe.seed(player, f"DROP TRIGGER {trigger_name}")
            recovered_reply = await _dispatch(probe, module, now, number)
            assert "本次获得炼体气血" in recovered_reply, (label, recovered_reply)
            recovered = _projection(probe, operation_id, user_id)
            assert len(recovered["receipts"]) == 1, (label, recovered)
            assert recovered["ledger"][0][0] == "applied", (label, recovered)
            await _dispatch(probe, module, now, number)
            replayed = _projection(probe, operation_id, user_id)
            assert replayed == recovered, (label, recovered, replayed)
            _assert_integrity(probe)
            probe.evidence.setdefault("faults", []).append(
                {
                    "label": label,
                    "reason": reason or "sqlite_abort",
                    "before": before,
                    "failed": failed,
                    "recovered": recovered,
                    "replay_unchanged": replayed == recovered,
                }
            )
