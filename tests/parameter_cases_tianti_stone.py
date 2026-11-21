"""Real matcher contracts for cross-database stone body training."""
from __future__ import annotations

import json

from tests.parameter_cases_team_leave import integrity


def _ensure_profile(probe, user_id="900001"):
    from nonebot_plugin_xiuxian_2.features.tianti_training.repository import (
        TiantiBreakthroughSqlRepository,
    )

    TiantiBreakthroughSqlRepository(probe.context.paths.player_db).ensure_profile(user_id)


def _projection(probe, user_id="900001"):
    game, player = probe.context.paths.game_db, probe.context.paths.player_db
    return {
        "stone": probe.rows(
            game,
            "SELECT stone FROM user_xiuxian WHERE user_id=?",
            (user_id,),
        ),
        "profile": probe.rows(
            player,
            "SELECT tianti_level,tianti_hp FROM tianti_info WHERE user_id=?",
            (user_id,),
        ),
        "receipts": probe.rows(
            game,
            "SELECT operation_id,user_id,requested_stone,stone_cost,hp_gain,new_hp "
            "FROM tianti_stone_training_operations WHERE user_id=? ORDER BY operation_id",
            (user_id,),
        ),
    }


def _recovery_case(
    case,
    *,
    key,
    write,
    trigger_sql,
    rejection,
    rejection_text,
    subject,
    trigger_database="game",
):
    @case(
        key,
        "灵石炼体",
        "合法金额、单一写入点故障、同消息恢复/重放/改参",
        f"隔离数据库安装 {write} 触发器拒绝或改写",
        f"具体 {rejection}、跨库回滚；解除故障后仅执行一次并可重放",
        group="tianti-stone-parameters",
        outcome="recovery",
    )
    async def run(probe):
        user_id = "900001"
        game = probe.context.paths.game_db
        probe.player(stone=100_000)
        _ensure_profile(probe, user_id)
        before = _projection(probe, user_id)
        number = 979_910 + len(probe.calls)
        operation_id = f"tianti-stone:{number}:{user_id}"
        trigger_path = probe.context.paths.player_db if trigger_database == "player" else game
        probe.seed(trigger_path, trigger_sql)

        failed = await probe.command(
            "灵石炼体", "灵石炼体 1000", number=number
        )
        after_failure = _projection(probe, user_id)
        ledger = probe.rows(
            game,
            "SELECT status,result_json FROM operation_ledger "
            "WHERE operation_id=? AND action='tianti.train'",
            (operation_id,),
        )
        probe.evidence.update(
            failed=failed,
            before=before,
            after_failure=after_failure,
            ledger=ledger,
        )
        assert rejection_text in failed and "已撤销" in failed, failed
        assert after_failure == before, (before, after_failure, failed)
        assert ledger and ledger[0][0] == "failed", ledger
        failure = json.loads(ledger[0][1])
        assert failure["code"] == rejection, failure
        assert failure["data"] == {"reason": rejection, "subject": subject}, failure

        trigger_name = trigger_sql.split("CREATE TRIGGER ", 1)[1].split()[0]
        probe.seed(trigger_path, f"DROP TRIGGER {trigger_name}")
        probe.clear_cooldowns()
        recovered = await probe.command(
            "灵石炼体", "灵石炼体 1000", number=number
        )
        after_recovery = _projection(probe, user_id)
        assert "灵石炼体完成" in recovered, recovered
        assert int(before["stone"][0][0]) - int(after_recovery["stone"][0][0]) == 1000
        assert len(after_recovery["receipts"]) == 1, after_recovery

        probe.clear_cooldowns()
        replay = await probe.command(
            "灵石炼体", "灵石炼体 1000", number=number
        )
        assert "已经处理" in replay, replay
        assert _projection(probe, user_id) == after_recovery

        probe.clear_cooldowns()
        conflict = await probe.command(
            "灵石炼体", "灵石炼体 1001", number=number
        )
        assert "该操作号已用于不同内容的请求" in conflict, conflict
        assert _projection(probe, user_id) == after_recovery
        integrity(probe)
        probe.evidence.update(
            recovered=recovered,
            after_recovery=after_recovery,
            replay=replay,
            conflict=conflict,
        )

    return run


def register(case):
    _recovery_case(
        case,
        key="tianti-stone-wallet-ignore-recovery",
        write="钱包 UPDATE IGNORE",
        trigger_sql=(
            "CREATE TRIGGER ignore_tianti_stone_wallet BEFORE UPDATE OF stone "
            "ON user_xiuxian WHEN OLD.user_id='900001' "
            "BEGIN SELECT RAISE(IGNORE); END"
        ),
        rejection="tianti_stone_wallet_write_rejected",
        rejection_text="灵石扣除保存失败",
        subject="stone_wallet",
    )
    _recovery_case(
        case,
        key="tianti-stone-wallet-rewrite-recovery",
        write="钱包 UPDATE AFTER 改写",
        trigger_sql=(
            "CREATE TRIGGER rewrite_tianti_stone_wallet AFTER UPDATE OF stone "
            "ON user_xiuxian WHEN OLD.user_id='900001' BEGIN UPDATE user_xiuxian "
            "SET stone=CAST(NEW.stone AS INTEGER)+7 WHERE user_id=NEW.user_id; END"
        ),
        rejection="tianti_stone_wallet_projection_mismatch",
        rejection_text="灵石余额与本次扣费结果不一致",
        subject="stone_wallet",
    )
    _recovery_case(
        case,
        key="tianti-stone-profile-ignore-recovery",
        write="档案 UPDATE IGNORE",
        trigger_sql=(
            "CREATE TRIGGER ignore_tianti_stone_profile BEFORE UPDATE OF tianti_hp "
            "ON tianti_info WHEN OLD.user_id='900001' "
            "BEGIN SELECT RAISE(IGNORE); END"
        ),
        rejection="tianti_stone_profile_projection_mismatch",
        rejection_text="炼体档案气血与本次结果不一致",
        subject="player_profile",
        trigger_database="player",
    )
    _recovery_case(
        case,
        key="tianti-stone-profile-rewrite-recovery",
        write="档案 UPDATE 改写",
        trigger_sql=(
            "CREATE TRIGGER rewrite_tianti_stone_profile AFTER UPDATE OF tianti_hp "
            "ON tianti_info WHEN NEW.user_id='900001' BEGIN UPDATE tianti_info "
            "SET tianti_hp=CAST(NEW.tianti_hp AS INTEGER)+7 WHERE user_id=NEW.user_id; END"
        ),
        rejection="tianti_stone_profile_projection_mismatch",
        rejection_text="炼体档案气血与本次结果不一致",
        subject="player_profile",
        trigger_database="player",
    )
    _recovery_case(
        case,
        key="tianti-stone-receipt-ignore-recovery",
        write="永久回执 INSERT IGNORE",
        trigger_sql=(
            "CREATE TRIGGER ignore_tianti_stone_receipt BEFORE INSERT "
            "ON tianti_stone_training_operations BEGIN SELECT RAISE(IGNORE); END"
        ),
        rejection="tianti_stone_receipt_write_rejected",
        rejection_text="炼体操作回执保存失败",
        subject="receipt",
    )
    _recovery_case(
        case,
        key="tianti-stone-receipt-rewrite-recovery",
        write="永久回执 AFTER 改写",
        trigger_sql=(
            "CREATE TRIGGER rewrite_tianti_stone_receipt AFTER INSERT "
            "ON tianti_stone_training_operations BEGIN UPDATE "
            "tianti_stone_training_operations SET new_hp=new_hp+1 "
            "WHERE operation_id=NEW.operation_id; END"
        ),
        rejection="tianti_stone_receipt_projection_mismatch",
        rejection_text="炼体操作回执与扣费结果不一致",
        subject="receipt",
    )
    for label, statement in (
        (
            "wallet",
            "UPDATE OF stone ON user_xiuxian",
        ),
        (
            "profile",
            "UPDATE OF tianti_hp ON tianti_info",
        ),
        (
            "receipt",
            "INSERT ON tianti_stone_training_operations",
        ),
    ):
        def build(label=label, statement=statement):
            @case(
                f"tianti-stone-{label}-abort-recovery",
                "灵石炼体",
                "合法金额、SQLite ABORT、同消息恢复",
                f"隔离 {label} 写入注入 RAISE(ABORT)",
                "玩家收到安全失败回复、双库回滚，解除故障后同消息恢复",
                group="tianti-stone-parameters",
                outcome="recovery",
            )
            async def run(probe):
                user_id = "900001"
                game, player = probe.context.paths.game_db, probe.context.paths.player_db
                probe.player(stone=100_000)
                _ensure_profile(probe, user_id)
                before = _projection(probe, user_id)
                number = 979_950 + len(probe.calls)
                operation_id = f"tianti-stone:{number}:{user_id}"
                trigger = f"abort_tianti_stone_{label}"
                trigger_path = player if label == "profile" else game
                probe.seed(
                    trigger_path,
                    f"CREATE TRIGGER {trigger} BEFORE {statement} "
                    "BEGIN SELECT RAISE(ABORT, 'isolated tianti stone "
                    + label
                    + " abort'); END",
                )

                failed = await probe.command(
                    "灵石炼体", "灵石炼体 1000", number=number,
                    allow_exception=True,
                )
                after_failure = _projection(probe, user_id)
                ledger = probe.rows(
                    game,
                    "SELECT status,result_json FROM operation_ledger "
                    "WHERE operation_id=? AND action='tianti.train'",
                    (operation_id,),
                )
                assert "定位号" in failed and "灵石炼体完成" not in failed, failed
                assert after_failure == before, (before, after_failure, failed)
                assert ledger and ledger[0][0] == "failed", ledger
                failure = json.loads(ledger[0][1])
                assert failure["code"] == "internal_error", failure
                assert f"isolated tianti stone {label} abort" in failure["message"], failure

                probe.seed(trigger_path, f"DROP TRIGGER {trigger}")
                probe.clear_cooldowns()
                recovered = await probe.command(
                    "灵石炼体", "灵石炼体 1000", number=number
                )
                after_recovery = _projection(probe, user_id)
                assert "灵石炼体完成" in recovered and len(after_recovery["receipts"]) == 1
                probe.evidence.update(
                    failed=failed,
                    before=before,
                    after_failure=after_failure,
                    failure=failure,
                    recovered=recovered,
                    after_recovery=after_recovery,
                )
                integrity(probe)

            return run

        build()

    @case(
        "tianti-stone-parameters-cap-and-cold-profile",
        "灵石炼体",
        "空/非法/Unicode/超长金额、气血上限截断和冷态档案",
        "真实角色与炼体配置，隔离 game/player SQLite",
        "坏输入零写；仅按缺口扣费；冷态角色能建档并保存回执",
        group="tianti-stone-parameters",
    )
    async def amount_cap_and_cold_profile(probe):
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_tianti as module

        user_id = "900001"
        game, player = probe.context.paths.game_db, probe.context.paths.player_db
        probe.player(stone=100_000)
        _ensure_profile(probe, user_id)
        application = module.tianti_training_application
        profile = application.read_profile(user_id)
        cap = application.profile_cap(profile)
        probe.seed(
            player,
            "UPDATE tianti_info SET tianti_hp=? WHERE user_id=?",
            (str(cap - 2), user_id),
        )
        before = _projection(probe, user_id)

        invalid = (
            ("empty", "灵石炼体 "),
            ("zero", "灵石炼体 0"),
            ("negative", "灵石炼体 -1"),
            ("decimal", "灵石炼体 1.5"),
            ("unicode", "灵石炼体 ²"),
            ("huge", "灵石炼体 " + "9" * 5000),
        )
        for index, (label, command) in enumerate(invalid):
            probe.clear_cooldowns()
            reply = await probe.command(
                "灵石炼体", command, number=979_970 + index
            )
            assert any(text in reply for text in ("用法", "至少为10")), (label, reply)
            assert _projection(probe, user_id) == before, (label, before, _projection(probe, user_id))

        probe.clear_cooldowns()
        capped = await probe.command(
            "灵石炼体", "灵石炼体 1000", number=979_980
        )
        after_capped = _projection(probe, user_id)
        assert "消耗灵石20" in capped and "获得炼体气血2" in capped, capped
        assert int(before["stone"][0][0]) - int(after_capped["stone"][0][0]) == 20
        assert int(after_capped["profile"][0][1]) == cap
        assert after_capped["receipts"][0][2:] == (1000, 20, 2, cap)

        probe.clear_cooldowns()
        at_cap = await probe.command(
            "灵石炼体", "灵石炼体 1000", number=979_981
        )
        assert "已达当前炼体境界上限" in at_cap
        assert _projection(probe, user_id) == after_capped
        integrity(probe)

        cold_user = "900002"
        probe.player(cold_user, stone=100_000)
        assert not probe.rows(
            player,
            "SELECT user_id FROM tianti_info WHERE user_id=?",
            (cold_user,),
        )
        probe.clear_cooldowns()
        cold = await probe.command(
            "灵石炼体", "灵石炼体 1000", actor=cold_user, number=979_982
        )
        assert "灵石炼体完成" in cold, cold
        assert probe.rows(
            player,
            "SELECT tianti_hp FROM tianti_info WHERE user_id=?",
            (cold_user,),
        )
        assert probe.rows(
            game,
            "SELECT requested_stone,stone_cost,hp_gain FROM tianti_stone_training_operations "
            "WHERE user_id=?",
            (cold_user,),
        ) == [(1000, 1000, 100)]
        probe.evidence.update(
            invalid_inputs=[label for label, _ in invalid],
            cap=cap,
            capped=after_capped,
            at_cap=at_cap,
            cold_profile=cold,
        )
        integrity(probe)

    @case(
        "tianti-stone-insufficient-balance",
        "灵石炼体",
        "合法金额但余额不足",
        "隔离钱包5灵石、已有炼体档案",
        "返回灵石不足的具体原因，不改余额/气血/回执",
        group="tianti-stone-parameters",
        outcome="rejection",
    )
    async def insufficient_balance(probe):
        user_id = "900001"
        probe.player(stone=5)
        _ensure_profile(probe, user_id)
        before = _projection(probe, user_id)
        reply = await probe.command(
            "灵石炼体", "灵石炼体 1000", number=979_990
        )
        assert "灵石余额不足" in reply, reply
        assert _projection(probe, user_id) == before
        integrity(probe)
