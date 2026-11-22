"""Real matcher coverage for returning to a player's cave."""

import json


def _prepare(probe, actor="900001", *, built=1, realm="仙界", heaven="九重天", node="home"):
    from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_map as module

    game, player = probe.context.paths.game_db, probe.context.paths.player_db
    module.map_application.save_status(actor, "凡界", "一重天", "outside", ["outside"])
    probe.seed(
        player,
        "INSERT INTO dongfu_status(user_id,built,realm,heaven,node_id,node_name) "
        "VALUES(?,?,?,?,?,?) ON CONFLICT(user_id) DO UPDATE SET built=excluded.built,"
        "realm=excluded.realm,heaven=excluded.heaven,node_id=excluded.node_id,"
        "node_name=excluded.node_name",
        (actor, built, realm, heaven, node, "云宫"),
    )
    return module, game, player


def _projection(probe, number, actor="900001"):
    player, game = probe.context.paths.player_db, probe.context.paths.game_db
    operation_id = f"map-home:{number}:{actor}"
    return {
        "position": probe.rows(
            player,
            "SELECT realm,heaven,node_id,visited_nodes FROM map_status WHERE user_id=?",
            (actor,),
        ),
        "receipt": probe.rows(
            player,
            "SELECT operation_id,payload,result_status,realm,heaven,node_id,node_name "
            "FROM map_home_return_operations WHERE operation_id=?",
            (operation_id,),
        ),
        "ledger": probe.rows(
            game,
            "SELECT status FROM operation_ledger "
            "WHERE operation_id=? AND action='map.return_home'",
            (operation_id,),
        ),
    }


def _assert_integrity(probe):
    from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork

    for database in (probe.context.paths.game_db, probe.context.paths.player_db):
        with DatabaseUnitOfWork(database, read_only=True) as uow:
            result = uow.query_one("PRAGMA integrity_check")
            assert result["integrity_check"] == "ok", (database, result)


def register(case):
    @case(
        "map-home-return-success-replay-isolation",
        "回府",
        "无参数回府、私聊与同消息重放，另核第二玩家隔离",
        "两个玩家各有有效洞府及不同当前位置",
        "回到对应洞府、访问节点只追加一次；原消息重放不移动第二次",
        group="map-home-return",
    )
    async def success_replay_isolation(probe):
        module, _, player = _prepare(probe)
        number = 973301
        reply = await probe.command(
            "回府", "回府", number=number, event_type="private"
        )
        committed = _projection(probe, number)
        probe.evidence.update(reply=reply, committed=committed)
        assert "你已回到洞府：仙界·九重天·云宫" in reply, reply
        assert len(committed["position"]) == 1, committed
        position = committed["position"][0]
        assert position[:3] == ("仙界", "九重天", "home"), committed
        assert json.loads(position[3]) == ["outside", "home"], committed
        assert committed["receipt"] == [(
            f"map-home:{number}:900001", '["900001"]', "applied",
            "仙界", "九重天", "home", "云宫",
        )], committed
        assert committed["ledger"] == [("applied",)], committed

        module.map_application.save_status(
            "900001", "魔界", "二重天", "away", ["outside", "home", "away"]
        )
        moved_away = _projection(probe, number)
        replay = await probe.command(
            "回府", "回府", number=number, event_type="private"
        )
        assert "你已回到洞府：仙界·九重天·云宫" in replay, replay
        assert _projection(probe, number) == moved_away, (replay, moved_away)

        module.map_application.save_status(
            "900002", "凡界", "一重天", "second-away", ["second-away"]
        )
        probe.seed(
            player,
            "INSERT INTO dongfu_status(user_id,built,realm,heaven,node_id,node_name) "
            "VALUES('900002',1,'妖界','三重天','second-home','竹庐') "
            "ON CONFLICT(user_id) DO UPDATE SET built=1,realm='妖界',heaven='三重天',"
            "node_id='second-home',node_name='竹庐'",
        )
        other_reply = await probe.command("回府", "回府", number=973302, actor="900002")
        other = _projection(probe, 973302, "900002")
        assert "你已回到洞府：妖界·三重天·竹庐" in other_reply, other_reply
        assert other["position"][0][:3] == ("妖界", "三重天", "second-home"), other
        assert _projection(probe, number) == moved_away
        _assert_integrity(probe)

    @case(
        "map-home-return-known-rejections",
        "回府",
        "未建设、洞府位置损坏、地图位置缺失",
        "隔离player库逐项建立对应状态",
        "具体拒绝并持久化原状态结果，不回复成功或改写地图位置",
        group="map-home-return",
        outcome="rejection",
    )
    async def known_rejections(probe):
        _, _, player = _prepare(probe, built=0)
        before = _projection(probe, 973303)
        missing = await probe.command("回府", "回府", number=973303)
        assert "尚未建设洞府" in missing, missing
        assert _projection(probe, 973303)["position"] == before["position"]
        assert _projection(probe, 973303)["receipt"][0][2] == "dongfu_missing"

        _prepare(probe, realm="", node="")
        invalid = await probe.command("回府", "回府", number=973304)
        assert "洞府数据异常" in invalid, invalid
        assert _projection(probe, 973304)["receipt"][0][2] == "dongfu_invalid"

        _prepare(probe)
        probe.seed(player, "DELETE FROM map_status WHERE user_id='900001'")
        position_missing = await probe.command("回府", "回府", number=973305)
        assert "地图位置数据异常" in position_missing, position_missing
        assert _projection(probe, 973305)["position"] == []
        assert _projection(probe, 973305)["receipt"][0][2] == "position_missing"
        _assert_integrity(probe)

    @case(
        "map-home-return-conflict-and-corrupt-receipt",
        "回府",
        "相同操作号冲突与畸形历史回执",
        "真实应用ledger无占用，隔离库预置另一用户或损坏回执",
        "冲突/损坏回执明确拒绝，当前位置保持不变",
        group="map-home-return",
        outcome="rejection",
    )
    async def conflict_and_corrupt_receipt(probe):
        from nonebot_plugin_xiuxian_2.core.player_reasons import reason_text

        _, _, player = _prepare(probe)
        before_position = _projection(probe, 973306)["position"]
        probe.seed(
            player,
            "INSERT INTO map_home_return_operations"
            "(operation_id,payload,result_status,realm,heaven,node_id,node_name) "
            "VALUES(?,?,?,?,?,?,?)",
            ("map-home:973306:900001", '["900002"]', "applied", "仙界", "九重天", "home", "云宫"),
        )
        conflict = await probe.command("回府", "回府", number=973306)
        assert "该事件已用于其他回府操作" in conflict, conflict
        assert _projection(probe, 973306)["position"] == before_position

        probe.seed(player, "DELETE FROM map_home_return_operations")
        probe.seed(
            player,
            "INSERT INTO map_home_return_operations"
            "(operation_id,payload,result_status,realm,heaven,node_id,node_name) "
            "VALUES(?,?,?,?,?,?,?)",
            ("map-home:973307:900001", "not-json", "applied", "仙界", "九重天", "home", "云宫"),
        )
        corrupt = await probe.command("回府", "回府", number=973307)
        assert reason_text("map_home_return_receipt_invalid") in corrupt, corrupt
        assert "回府回执" in corrupt, corrupt
        assert _projection(probe, 973307)["position"] == before_position
        _assert_integrity(probe)

    write_faults = (
        (
            "position-ignore",
            "player",
            "BEFORE UPDATE OF realm ON map_status WHEN NEW.user_id='900001' "
            "BEGIN SELECT RAISE(IGNORE); END",
            "map_home_return_position_write_rejected",
            "地图位置",
        ),
        (
            "position-rewrite",
            "player",
            "AFTER UPDATE OF realm ON map_status WHEN NEW.user_id='900001' "
            "BEGIN UPDATE map_status SET node_id='rewritten' WHERE user_id='900001'; END",
            "map_home_return_position_projection_mismatch",
            "地图位置",
        ),
        (
            "receipt-ignore",
            "player",
            "BEFORE INSERT ON map_home_return_operations "
            "BEGIN SELECT RAISE(IGNORE); END",
            "map_home_return_receipt_write_rejected",
            "回府回执",
        ),
        (
            "receipt-rewrite",
            "player",
            "AFTER INSERT ON map_home_return_operations "
            "BEGIN UPDATE map_home_return_operations SET node_name='rewritten' "
            "WHERE operation_id=NEW.operation_id; END",
            "map_home_return_receipt_projection_mismatch",
            "回府回执",
        ),
    )
    for index, (label, database, trigger, reason, subject_label) in enumerate(write_faults):
        @case(
            "map-home-return-recovery-" + label,
            "回府",
            "实际写路径故障：" + label,
            "有效洞府与隔离SQLite位置/回执写入故障",
            "具体reason/subject、业务投影与ledger回滚；解除故障后原消息恢复/replay",
            group="map-home-return",
            outcome="recovery",
        )
        async def recover_write(
            probe,
            label=label,
            database=database,
            trigger=trigger,
            reason=reason,
            subject_label=subject_label,
            number=973310 + index,
        ):
            from nonebot_plugin_xiuxian_2.core.player_reasons import reason_text

            module, game, player = _prepare(probe)
            target = game if database == "game" else player
            before = _projection(probe, number)
            probe.seed(target, "CREATE TRIGGER home_fault " + trigger)
            reply = await probe.command("回府", "回府", number=number)
            after = _projection(probe, number)
            probe.evidence[label] = {"before": before, "after": after, "reply": reply}
            assert reason_text(reason) in reply and subject_label in reply, reply
            assert after == before, (before, after, reply)
            assert probe.calls[-1].get("errors", []) == [], probe.calls[-1]

            probe.seed(target, "DROP TRIGGER home_fault")
            recovered = await probe.command("回府", "回府", number=number)
            committed = _projection(probe, number)
            assert "你已回到洞府：仙界·九重天·云宫" in recovered, recovered
            assert committed["position"][0][:3] == ("仙界", "九重天", "home"), committed
            assert committed["receipt"][0][2] == "applied", committed
            assert committed["ledger"] == [("applied",)], committed

            module.map_application.save_status(
                "900001", "魔界", "二重天", "away", ["outside", "home", "away"]
            )
            moved_away = _projection(probe, number)
            replay = await probe.command("回府", "回府", number=number)
            assert "你已回到洞府：仙界·九重天·云宫" in replay, replay
            assert _projection(probe, number) == moved_away
            _assert_integrity(probe)

    abort_faults = (
        (
            "position-abort",
            "BEFORE UPDATE OF realm ON map_status WHEN NEW.user_id='900001'",
        ),
        (
            "receipt-abort",
            "BEFORE INSERT ON map_home_return_operations",
        ),
    )
    for index, (label, trigger) in enumerate(abort_faults):
        @case(
            "map-home-return-recovery-" + label,
            "回府",
            "SQLite ABORT：" + label,
            "有效洞府与隔离SQLite具体写点ABORT",
            "原traceback/定位号、整笔回滚；修复trigger后原消息可恢复",
            group="map-home-return",
            outcome="recovery",
        )
        async def recover_abort(probe, label=label, trigger=trigger, number=973320 + index):
            _, game, player = _prepare(probe)
            before = _projection(probe, number)
            probe.seed(
                player,
                "CREATE TRIGGER home_abort " + trigger
                + " BEGIN SELECT RAISE(ABORT,'home refused'); END",
            )
            reply = await probe.command(
                "回府", "回府", number=number, allow_exception=True
            )
            call = probe.calls[-1]
            errors = "\n".join(call.get("errors", []))
            after = _projection(probe, number)
            probe.evidence[label] = {
                "before": before,
                "after": after,
                "reply": reply,
                "errors": call.get("errors", []),
                "logs": call.get("logs", []),
            }
            assert "定位号" in reply and "home refused" not in reply, reply
            assert "Traceback" in errors and "sqlite3.IntegrityError: home refused" in errors, call
            assert after["position"] == before["position"] and after["receipt"] == before["receipt"], after
            assert after["ledger"] == [("failed",)], after

            probe.seed(player, "DROP TRIGGER home_abort")
            recovered = await probe.command("回府", "回府", number=number)
            committed = _projection(probe, number)
            assert "你已回到洞府：仙界·九重天·云宫" in recovered, recovered
            assert committed["position"][0][:3] == ("仙界", "九重天", "home"), committed
            assert committed["receipt"][0][2] == "applied", committed
            assert committed["ledger"] == [("applied",)], committed
            _assert_integrity(probe)
