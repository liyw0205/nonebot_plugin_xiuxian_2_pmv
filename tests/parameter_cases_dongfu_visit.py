"""Bounded real-command coverage for visiting cave rewards."""

from unittest.mock import patch

from tests.parameter_cases_dongfu_harvest import _database_projection


def _seed(probe):
    from nonebot_plugin_xiuxian_2.features.dongfu.application import DongfuApplication
    from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_dongfu as module

    game, player = probe.context.paths.game_db, probe.context.paths.player_db
    probe.seed(
        player,
        "INSERT INTO dongfu_status(user_id,built,node_name,node_type) VALUES(?,1,'诊断水域','水域') "
        "ON CONFLICT(user_id) DO UPDATE SET built=1,node_name=excluded.node_name,node_type=excluded.node_type",
        ("900001",),
    )
    probe.seed(
        player,
        "INSERT INTO dongfu_status(user_id,built,node_name,node_type) VALUES(?,1,'诊断矿脉','矿脉') "
        "ON CONFLICT(user_id) DO UPDATE SET built=1,node_name=excluded.node_name,node_type=excluded.node_type",
        ("900002",),
    )
    module.configure_dongfu_application(DongfuApplication(game, player))
    return module


def _business(probe):
    game, player = probe.context.paths.game_db, probe.context.paths.player_db
    return {
        "wallets": probe.rows(
            game,
            "SELECT user_id,stone,user_stamina FROM user_xiuxian "
            "WHERE user_id IN ('900001','900002') ORDER BY user_id",
        ),
        "caves": probe.rows(
            player,
            "SELECT user_id,built,node_type FROM dongfu_status "
            "WHERE user_id IN ('900001','900002') ORDER BY user_id",
        ),
        "receipts": probe.rows(
            game,
            "SELECT operation_id,payload,gain FROM dongfu_visit_reward_operations "
            "ORDER BY operation_id",
        ),
    }


async def _command(probe, *, number, gain=12345, abort=False, event_type="group",
                   actor="900001", target="诊断对手"):
    from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_dongfu as module

    with patch.object(module.runtime_random, "randint", return_value=gain):
        reply = await probe.command(
            "拜访道友", "拜访道友" + (f" {target}" if target else ""), number=number,
            actor=actor, allow_exception=True, event_type=event_type,
        )
    call = probe.calls[-1]
    traces = "\n".join(call["errors"])
    if abort:
        assert "sqlite3.IntegrityError: visit reward refused" in traces and "Traceback" in traces, call
        assert "定位号" in reply and "visit reward refused" not in reply, reply
    else:
        assert call["errors"] == [], call
    return reply


def register(case):
    @case(
        "dongfu-visit-valid-reward-replay",
        "拜访道友",
        "真实道号目标，固定随机灵石收益",
        "双方注册并建设洞府，拜访消耗5体力，灵石收益固定为12345",
        "仅来访者按回执获得一次奖励，目标钱包不变，同消息返回原收益",
        group="dongfu-visit",
    )
    async def valid_reward_replay(probe):
        _seed(probe)
        game = probe.context.paths.game_db
        before = _business(probe)
        reply = await _command(probe, number=972201)
        probe.evidence["success_reply"] = reply
        after = _business(probe)
        probe.evidence.update(before=before, after=after)
        assert "拜访了【诊断对手】的洞府（诊断矿脉）" in reply and "获得灵石1.23万" in reply, reply
        assert after["wallets"][0] == ("900001", 1_012_345, 95), (before, after)
        assert after["wallets"][1] == ("900002", 1_000_000, 100), (before, after)
        assert after["receipts"] == [
            ("dongfu-visit:900001:972201", '["900001","900002"]', 12345)
        ]
        committed = _database_projection(probe)
        replay = await _command(probe, number=972201, gain=49999)
        probe.evidence["replay_reply"] = replay
        assert "该请求已处理" in replay and "获得灵石1.23万" in replay, replay
        assert _database_projection(probe) == committed
        probe.seed(
            probe.context.paths.player_db,
            "INSERT INTO dongfu_status(user_id,built,node_name,node_type) VALUES('900003',1,'诊断仙山','仙山')",
        )
        committed_before_conflict = _database_projection(probe)
        conflict = await _command(probe, number=972201, target="诊断管理", gain=49999)
        assert "该操作号已用于不同内容的请求" in conflict, conflict
        assert _database_projection(probe) == committed_before_conflict

    @case(
        "dongfu-visit-receipt-ignore-recovery",
        "拜访道友",
        "合法目标，永久回执INSERT IGNORE后恢复",
        "隔离game SQLite触发器拒绝dongfu_visit_reward_operations插入",
        "首次奖励事务完整回滚且保留原因，解除故障后同消息只奖励一次",
        group="dongfu-visit",
        outcome="recovery",
    )
    async def receipt_ignore_recovery(probe):
        from nonebot_plugin_xiuxian_2.core.player_reasons import reason_text

        _seed(probe)
        game = probe.context.paths.game_db
        probe.seed(
            game,
            "CREATE TRIGGER visit_receipt_ignore BEFORE INSERT ON "
            "dongfu_visit_reward_operations BEGIN SELECT RAISE(IGNORE); END",
        )
        before = _business(probe)
        reply = await _command(probe, number=972202)
        after = _business(probe)
        probe.evidence.update(failed_reply=reply, before=before, after=after)
        assert reason_text("dongfu_visit_receipt_write_rejected") in reply, reply
        assert "拜访奖励回执" in reply, reply
        assert after["wallets"][0][1] == before["wallets"][0][1]
        assert after["wallets"][0][2] == before["wallets"][0][2] - 5
        assert after["wallets"][1] == before["wallets"][1]
        assert after["caves"] == before["caves"] and after["receipts"] == before["receipts"]
        probe.seed(game, "DROP TRIGGER visit_receipt_ignore")
        recovered = await _command(probe, number=972202)
        assert "获得灵石1.23万" in recovered, recovered
        committed = _database_projection(probe)
        assert "该请求已处理" in await _command(probe, number=972202, gain=49999)
        assert _database_projection(probe) == committed

    @case("dongfu-visit-invalid-receipt", "拜访道友", "同消息已有损坏历史回执",
          "隔离game库回执payload不是合法历史格式", "明确报告坏回执且不改钱包/回执", group="dongfu-visit",
          outcome="rejection")
    async def invalid_receipt(probe):
        from nonebot_plugin_xiuxian_2.core.player_reasons import reason_text

        _seed(probe)
        game = probe.context.paths.game_db
        probe.seed(
            game,
            "INSERT INTO dongfu_visit_reward_operations(operation_id,payload,gain) VALUES(?,?,?)",
            ("dongfu-visit:900001:972234", "not-json", 12345),
        )
        before = _business(probe)
        reply = await _command(probe, number=972234)
        assert reason_text("receipt_corrupt") in reply and "拜访奖励回执" in reply, reply
        after = _business(probe)
        assert after["wallets"][0] == ("900001", before["wallets"][0][1], before["wallets"][0][2] - 5)
        assert after["wallets"][1] == before["wallets"][1]
        assert after["caves"] == before["caves"] and after["receipts"] == before["receipts"]
        _database_projection(probe)

    faults = (
        ("stone-ignore", "game", "BEFORE UPDATE ON user_xiuxian WHEN NEW.user_id='900001' AND NEW.stone!=OLD.stone BEGIN SELECT RAISE(IGNORE); END",
         "stone_write_rejected", "stone_wallet", False),
        ("stone-rewrite", "game", "AFTER UPDATE ON user_xiuxian WHEN NEW.user_id='900001' AND NEW.stone!=OLD.stone BEGIN UPDATE user_xiuxian SET stone=7 WHERE user_id='900001'; END",
         "stone_projection_mismatch", "stone_wallet", False),
        ("receipt-ignore", "game", "BEFORE INSERT ON dongfu_visit_reward_operations BEGIN SELECT RAISE(IGNORE); END",
         "receipt_write_rejected", "receipt", False),
        ("receipt-rewrite", "game", "AFTER INSERT ON dongfu_visit_reward_operations BEGIN UPDATE dongfu_visit_reward_operations SET gain=99999 WHERE operation_id=NEW.operation_id; END",
         "receipt_projection_mismatch", "receipt", False),
        ("receipt-wallet-rewrite", "game", "AFTER INSERT ON dongfu_visit_reward_operations BEGIN UPDATE user_xiuxian SET stone=7 WHERE user_id='900001'; END",
         "stone_projection_mismatch", "stone_wallet", False),
        ("stone-abort", "game", "BEFORE UPDATE ON user_xiuxian WHEN NEW.user_id='900001' AND NEW.stone!=OLD.stone BEGIN SELECT RAISE(ABORT,'visit reward refused'); END",
         "", "", True),
        ("receipt-abort", "game", "BEFORE INSERT ON dongfu_visit_reward_operations BEGIN SELECT RAISE(ABORT,'visit reward refused'); END",
         "", "", True),
    )
    for index, (label, database, trigger, suffix, subject, abort) in enumerate(faults):
        def build(label=label, database=database, trigger=trigger, suffix=suffix,
                  subject=subject, abort=abort, index=index):
            async def recovery(probe):
                from nonebot_plugin_xiuxian_2.core.player_reasons import reason_text

                _seed(probe)
                game, player = probe.context.paths.game_db, probe.context.paths.player_db
                target_database = game if database == "game" else player
                before = _business(probe)
                probe.seed(target_database, "CREATE TRIGGER visit_fault " + trigger)
                reply = await _command(probe, number=972210 + index, abort=abort)
                after = _business(probe)
                probe.evidence.update(fault=label, failed_reply=reply, before=before, after=after)
                assert after["wallets"][0] == ("900001", before["wallets"][0][1], before["wallets"][0][2] - 5), (
                    label, before, after
                )
                assert after["wallets"][1] == before["wallets"][1]
                assert after["caves"] == before["caves"] and after["receipts"] == before["receipts"]
                if abort:
                    assert "定位号" in reply and "visit reward refused" not in reply, reply
                    assert "sqlite3.IntegrityError: visit reward refused" in "\n".join(probe.calls[-1]["errors"]), probe.calls[-1]
                else:
                    assert reason_text("dongfu_visit_" + suffix) in reply, (label, reply)
                    expected_subject = "灵石钱包" if subject == "stone_wallet" else "拜访奖励回执"
                    assert expected_subject in reply, (label, reply)
                    assert "拜访了【" not in reply, reply
                probe.seed(target_database, "DROP TRIGGER visit_fault")
                recovered = await _command(probe, number=972210 + index)
                assert "拜访了【诊断对手】" in recovered and "获得灵石1.23万" in recovered, recovered
                committed = _database_projection(probe)
                assert probe.rows(game, "SELECT user_stamina,stone FROM user_xiuxian WHERE user_id='900001'") == [(95, 1_012_345)]
                replay = await _command(probe, number=972210 + index, gain=49999)
                assert "该请求已处理" in replay and "获得灵石1.23万" in replay, replay
                assert _database_projection(probe) == committed
            return recovery

        case("dongfu-visit-recovery-" + label, "拜访道友", "有效目标奖励遇" + label,
             "隔离game SQLite trigger，固定随机收益且访问冷却独立提交",
             "奖励资产/回执零部分提交、具体原因或原异常、同消息恢复/replay",
             group="dongfu-visit", outcome="recovery")(build())

    @case(
        "dongfu-visit-target-parameters",
        "拜访道友",
        "空目标/不存在/本人/未建洞府参数",
        "两个已注册角色及隔离洞府记录",
        "原参数拒绝回复保留，奖励灵石和永久回执零写，冷却体力单独断言",
        group="dongfu-visit",
    )
    async def target_parameters(probe):
        _seed(probe)
        game, player = probe.context.paths.game_db, probe.context.paths.player_db
        cases = (
            (None, "请使用：拜访道友 道号"),
            ("不存在的诊断对象", "未找到道友"),
            ("诊断道友", "你不能拜访自己"),
        )
        for index, (target, expected) in enumerate(cases):
            before = _business(probe)
            reply = await _command(probe, number=972230 + index, target=target)
            after = _business(probe)
            assert expected in reply, (target, reply)
            assert after["wallets"][0][1] == before["wallets"][0][1]
            assert after["wallets"][0][2] == before["wallets"][0][2] - 5
            assert after["wallets"][1] == before["wallets"][1]
            assert after["receipts"] == before["receipts"]

        before = _business(probe)
        unregistered = await _command(probe, number=972234, actor="900099")
        after = _business(probe)
        assert "修仙界没有道友的信息" in unregistered, unregistered
        assert after == before

        probe.seed(player, "UPDATE dongfu_status SET built=0 WHERE user_id='900002'")
        before = _business(probe)
        reply = await _command(probe, number=972233)
        after = _business(probe)
        assert "尚未建设洞府" in reply, reply
        assert after["wallets"][0][1] == before["wallets"][0][1]
        assert after["wallets"][0][2] == before["wallets"][0][2] - 5
        assert after["receipts"] == before["receipts"]
        _database_projection(probe)
