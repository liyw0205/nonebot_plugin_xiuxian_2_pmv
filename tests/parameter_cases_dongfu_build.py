"""Real command coverage for building a cave from the map feature."""


def _prepare(probe, *, stone=150_000_000):
    from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_map as module

    data = module._load_map_data()
    realm, heaven, node = next(
        (realm, heaven, node)
        for realm, realm_data in data.items()
        if realm != "meta"
        for heaven, nodes in realm_data["heavens"].items()
        for node in nodes
        if node.get("type", "") not in module.FORBIDDEN_DONGFU_TYPES
    )
    probe.player(stone=stone)
    module.map_application.save_status("900001", realm, heaven, node["id"], [node["id"]])
    probe.seed(
        probe.context.paths.player_db,
        "INSERT INTO dongfu_status(user_id,built) VALUES('900001',0) "
        "ON CONFLICT(user_id) DO UPDATE SET built=0",
    )
    return module, {"realm": realm, "heaven": heaven, "node": node}


def _operation_id(number):
    return f"map-dongfu-build:900001:{number}"


def _projection(probe, number=973001):
    game, player = probe.context.paths.game_db, probe.context.paths.player_db
    operation_id = _operation_id(number)
    return {
        "wallet": probe.rows(game, "SELECT stone FROM user_xiuxian WHERE user_id='900001'"),
        "position": probe.rows(player, "SELECT realm,heaven,node_id FROM map_status WHERE user_id='900001'"),
        "dongfu": probe.rows(
            player,
            "SELECT user_id,built,realm,heaven,node_id,node_name,node_type "
            "FROM dongfu_status WHERE user_id='900001'",
        ),
        "receipt": probe.rows(
            game,
            "SELECT operation_id,payload,stone FROM map_dongfu_build_operations "
            "WHERE operation_id=?",
            (operation_id,),
        ),
        "ledger": probe.rows(
            game,
            "SELECT status FROM operation_ledger WHERE operation_id=? AND action='map.build_dongfu'",
            (operation_id,),
        ),
    }


def _business(probe, number):
    projection = _projection(probe, number)
    return {key: value for key, value in projection.items() if key != "ledger"}


def register(case):
    @case("dongfu-build-receipt-ignore", "建设洞府", "无参数建设，永久回执被SQLite IGNORE拒绝",
          "真实可建设地图节点、灵石充足、隔离双库", "明确回执拒绝、双库投影完整回滚且原消息可恢复",
          group="dongfu-build", outcome="recovery")
    async def receipt_ignore(probe):
        from nonebot_plugin_xiuxian_2.core.player_reasons import reason_text

        module, location = _prepare(probe)
        game = probe.context.paths.game_db
        probe.seed(
            game,
            "CREATE TRIGGER build_receipt_ignore BEFORE INSERT ON map_dongfu_build_operations "
            "BEGIN SELECT RAISE(IGNORE); END",
        )
        before = _projection(probe)
        reply = await probe.command("建设洞府", "建设洞府", number=973001, allow_exception=True)
        after = _projection(probe)
        probe.evidence.update(location=location, before=before, after=after, reply=reply,
                              handler_errors=probe.calls[-1].get("errors", []))
        assert reason_text("map_dongfu_build_receipt_write_rejected") in reply and "建设回执" in reply, reply
        assert probe.calls[-1].get("errors", []) == [], probe.calls[-1]
        assert after == before, (before, after, reply)
        probe.seed(game, "DROP TRIGGER build_receipt_ignore")
        recovered = await probe.command("建设洞府", "建设洞府", number=973001)
        assert "洞府建设成功" in recovered, recovered
        committed = _projection(probe)
        assert committed["wallet"] == [(50_000_000,)], committed
        assert committed["dongfu"][0][1:4] == (1, location["realm"], location["heaven"]), committed
        assert len(committed["receipt"]) == 1, committed
        assert _projection(probe) == committed

    @case("dongfu-build-success-replay", "建设洞府", "无参数，在可建设节点建设",
          "隔离地图位置有效、灵石恰好高于建设费用、洞府未建", "扣费与完整洞府位置/永久回执一致，重复不再次扣款",
          group="dongfu-build")
    async def success_replay(probe):
        _, location = _prepare(probe, stone=100_000_000)
        reply = await probe.command("建设洞府", "建设洞府", number=973002)
        after = _projection(probe, 973002)
        probe.evidence.update(location=location, after=after, reply=reply)
        assert "洞府建设成功" in reply, reply
        assert after["wallet"] == [(0,)], after
        assert after["dongfu"] == [(
            "900001", 1, location["realm"], location["heaven"], location["node"]["id"],
            location["node"]["name"], location["node"].get("type", ""),
        )], after
        assert len(after["receipt"]) == len(after["ledger"]) == 1, after
        assert after["ledger"] == [("applied",)], after
        duplicate_reply = await probe.command("建设洞府", "建设洞府", number=973002)
        assert "已建立洞府" in duplicate_reply, duplicate_reply
        assert _projection(probe, 973002) == after

    @case("dongfu-build-insufficient-stone", "建设洞府", "无参数，灵石比费用少1",
          "真实可建设节点、隔离钱包为99,999,999", "准确拒绝且钱包/洞府/回执零写",
          group="dongfu-build", outcome="rejection")
    async def insufficient_stone(probe):
        _, location = _prepare(probe, stone=99_999_999)
        before = _projection(probe, 973003)
        reply = await probe.command("建设洞府", "建设洞府", number=973003)
        assert "需要1亿灵石" in reply and "灵石不足" in reply, reply
        assert _projection(probe, 973003) == before
        probe.evidence.update(location=location, before=before, reply=reply, no_business_writes=True)

    @case("dongfu-build-forbidden-node", "建设洞府", "无参数，当前位置为禁建类型节点",
          "真实地图配置中的坊市/渡口/交通等禁建节点", "拒绝建设且钱包/洞府/回执零写",
          group="dongfu-build", outcome="rejection")
    async def forbidden_node(probe):
        module, location = _prepare(probe)
        data = module._load_map_data()
        forbidden = next(
            (realm, heaven, node)
            for realm, realm_data in data.items()
            if realm != "meta"
            for heaven, nodes in realm_data["heavens"].items()
            for node in nodes
            if node.get("type", "") in module.FORBIDDEN_DONGFU_TYPES
        )
        realm, heaven, node = forbidden
        module.map_application.save_status("900001", realm, heaven, node["id"], [node["id"]])
        before = _projection(probe, 973004)
        reply = await probe.command("建设洞府", "建设洞府", number=973004)
        assert f"当前节点【{node['name']}】" in reply and "不可建设洞府" in reply, reply
        assert _projection(probe, 973004) == before
        probe.evidence.update(forbidden={"realm": realm, "heaven": heaven, "node": node}, reply=reply)

    @case("dongfu-build-position-changed", "建设洞府", "无参数，构造位置读取后玩家换位",
          "真实可建设节点与隔离地图状态，应用调用前更新位置", "具体位置变化拒绝，资产零写，原位置恢复后同消息可建设",
          group="dongfu-build", outcome="recovery")
    async def position_changed(probe):
        from unittest.mock import patch

        from nonebot_plugin_xiuxian_2.core.player_reasons import reason_text

        module, location = _prepare(probe)
        data = module._load_map_data()
        other_realm, other_heaven, other_node = next(
            (realm, heaven, node)
            for realm, realm_data in data.items()
            if realm != "meta"
            for heaven, nodes in realm_data["heavens"].items()
            for node in nodes
            if node.get("type", "") not in module.FORBIDDEN_DONGFU_TYPES
            and (realm, heaven, node["id"])
            != (location["realm"], location["heaven"], location["node"]["id"])
        )
        event_number = 973005
        before = _projection(probe, event_number)
        original_build = module.map_application.build_dongfu

        def move_before_write(**kwargs):
            module.map_application.save_status(
                "900001", other_realm, other_heaven, other_node["id"], [other_node["id"]]
            )
            return original_build(**kwargs)

        with patch.object(module.map_application, "build_dongfu", side_effect=move_before_write):
            reply = await probe.command("建设洞府", "建设洞府", number=event_number)
        changed = _projection(probe, event_number)
        assert reason_text("map_dongfu_build_position_changed") in reply and "地图位置" in reply, reply
        assert changed["wallet"] == before["wallet"], changed
        assert changed["dongfu"] == before["dongfu"] and changed["receipt"] == before["receipt"], changed
        assert changed["position"] == [(other_realm, other_heaven, other_node["id"])], changed
        assert changed["ledger"] == [], changed

        module.map_application.save_status(
            "900001", location["realm"], location["heaven"], location["node"]["id"],
            [location["node"]["id"]],
        )
        recovered = await probe.command("建设洞府", "建设洞府", number=event_number)
        assert "洞府建设成功" in recovered, recovered
        committed = _projection(probe, event_number)
        assert committed["wallet"] == [(50_000_000,)] and len(committed["receipt"]) == 1, committed

    write_faults = (
        ("wallet-ignore", "game",
         "BEFORE UPDATE ON user_xiuxian WHEN NEW.user_id='900001' BEGIN SELECT RAISE(IGNORE); END",
         "map_dongfu_build_stone_write_rejected", "stone_wallet", "灵石钱包"),
        ("wallet-rewrite", "game",
         "AFTER UPDATE ON user_xiuxian WHEN NEW.user_id='900001' BEGIN "
         "UPDATE user_xiuxian SET stone=1 WHERE user_id='900001'; END",
         "map_dongfu_build_stone_projection_mismatch", "stone_wallet", "灵石钱包"),
        ("state-ignore", "player",
         "BEFORE UPDATE OF built ON dongfu_status BEGIN SELECT RAISE(IGNORE); END",
         "map_dongfu_build_state_write_rejected", "dongfu_status", "洞府状态"),
        ("state-rewrite", "player",
         "AFTER UPDATE OF built ON dongfu_status BEGIN "
         "UPDATE dongfu_status SET node_name='被改写' WHERE user_id='900001'; END",
         "map_dongfu_build_state_projection_mismatch", "dongfu_status", "洞府状态"),
        ("receipt-ignore", "game",
         "BEFORE INSERT ON map_dongfu_build_operations BEGIN SELECT RAISE(IGNORE); END",
         "map_dongfu_build_receipt_write_rejected", "receipt", "建设回执"),
        ("receipt-rewrite", "game",
         "AFTER INSERT ON map_dongfu_build_operations BEGIN "
         "UPDATE map_dongfu_build_operations SET payload='changed' "
         "WHERE operation_id=NEW.operation_id; END",
         "map_dongfu_build_receipt_projection_mismatch", "receipt", "建设回执"),
    )
    for index, (label, database, trigger, reason, subject, subject_label) in enumerate(write_faults):
        @case("dongfu-build-recovery-" + label, "建设洞府", "写路径" + label,
              "真实无参数 matcher、隔离双库写点触发IGNORE或AFTER改写",
              "具体reason/subject、整笔回滚、同消息解除故障后恢复",
              group="dongfu-build", outcome="recovery")
        async def recover_write(probe, database=database, trigger=trigger, reason=reason,
                                subject=subject, subject_label=subject_label,
                                event_number=973010 + index, label=label):
            from nonebot_plugin_xiuxian_2.core.player_reasons import reason_text

            _, location = _prepare(probe)
            game, player = probe.context.paths.game_db, probe.context.paths.player_db
            target = game if database == "game" else player
            probe.seed(target, "CREATE TRIGGER build_fault " + trigger)
            before = _business(probe, event_number)
            reply = await probe.command("建设洞府", "建设洞府", number=event_number)
            after = _business(probe, event_number)
            probe.evidence[label] = {
                "location": location, "before": before, "after": after, "reply": reply,
            }
            assert reason_text(reason) in reply and subject_label in reply, reply
            assert after == before, (label, before, after)
            assert probe.calls[-1].get("errors", []) == [], probe.calls[-1]
            assert _projection(probe, event_number)["ledger"] == []
            probe.seed(target, "DROP TRIGGER build_fault")
            recovered = await probe.command("建设洞府", "建设洞府", number=event_number)
            assert "洞府建设成功" in recovered, recovered
            committed = _projection(probe, event_number)
            assert committed["wallet"] == [(50_000_000,)], committed
            assert len(committed["receipt"]) == 1 and committed["ledger"] == [("applied",)], committed
            assert _projection(probe, event_number) == committed

    abort_faults = (
        ("wallet-abort", "game",
         "BEFORE UPDATE ON user_xiuxian WHEN NEW.user_id='900001'"),
        ("state-abort", "player", "BEFORE UPDATE OF built ON dongfu_status"),
        ("receipt-abort", "game", "BEFORE INSERT ON map_dongfu_build_operations"),
    )
    for index, (label, database, trigger) in enumerate(abort_faults):
        @case("dongfu-build-recovery-" + label, "建设洞府", "SQLite ABORT：" + label,
              "真实无参数 matcher、具体建设写点ABORT",
              "原traceback/定位号、整笔双库回滚，原消息在修复后恢复",
              group="dongfu-build", outcome="recovery")
        async def recover_abort(probe, database=database, trigger=trigger,
                                event_number=973020 + index, label=label):
            game, player = probe.context.paths.game_db, probe.context.paths.player_db
            target = game if database == "game" else player
            _prepare(probe)
            probe.seed(
                target,
                "CREATE TRIGGER build_abort " + trigger
                + " BEGIN SELECT RAISE(ABORT,'build refused'); END",
            )
            before = _business(probe, event_number)
            reply = await probe.command("建设洞府", "建设洞府", number=event_number, allow_exception=True)
            call = probe.calls[-1]
            after = _business(probe, event_number)
            probe.evidence[label] = {
                "before": before, "after": after, "reply": reply,
                "handler_errors": call.get("errors", []), "logs": call.get("logs", []),
            }
            assert "定位号" in reply and "build refused" not in reply, reply
            assert after == before, (label, before, after)
            assert _projection(probe, event_number)["ledger"] == [("failed",)]
            probe.seed(target, "DROP TRIGGER build_abort")
            recovered = await probe.command("建设洞府", "建设洞府", number=event_number)
            assert "洞府建设成功" in recovered, recovered
            committed = _projection(probe, event_number)
            assert committed["wallet"] == [(50_000_000,)], committed
            assert len(committed["receipt"]) == 1 and committed["ledger"] == [("applied",)], committed
