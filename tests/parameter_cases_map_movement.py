"""Map movement through registered OneBot commands and isolated SQLite."""

import json


def _setup(probe):
    from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_map as module

    data = module._load_map_data()
    realm, heaven, source, target = next(
        (realm, heaven, left, right)
        for realm in module._all_realms(data)
        for heaven in module._heaven_names(data, realm)
        for left, right in zip(module._nodes(data, realm, heaven), module._nodes(data, realm, heaven)[1:])
        if left["type"] not in module.TRAVEL_NODE_TYPES
        and right["type"] in module.TRAVEL_NODE_TYPES
        and module._find_node_by_name(data, right["name"]) == (realm, heaven, right)
        and len(module._heaven_names(data, realm)) > 1
    )
    module.map_application.save_status("900001", realm, heaven, source["id"], [source["id"]])
    return module, data, realm, heaven, source, target


def _travel_location(data):
    return next(
        (realm, heaven, node)
        for realm in data
        if realm != "meta"
        for heaven, nodes in data[realm]["heavens"].items()
        for node in nodes
        if node["type"] in {"交通", "渡口", "驿站"}
    )


def _projection(probe, number, actor="900001"):
    game, player = probe.context.paths.game_db, probe.context.paths.player_db
    operation = f"map-move:{number}:{actor}"
    return {
        "stamina": probe.rows(game, "SELECT user_stamina FROM user_xiuxian WHERE user_id=?", (actor,)),
        "position": probe.rows(player, "SELECT realm,heaven,node_id,visited_nodes FROM map_status WHERE user_id=?", (actor,)),
        "receipt": probe.rows(game, "SELECT payload,stamina FROM map_movement_operations WHERE operation_id=?", (operation,)),
        "ledger": probe.rows(game, "SELECT status FROM operation_ledger WHERE operation_id=? AND action='map.move'", (operation,)),
    }


def _applied(probe, number, before, destination, cost):
    result = _projection(probe, number)
    remaining = before["stamina"][0][0] - cost
    assert result["stamina"] == [(remaining,)], result
    realm, heaven, node, visited = result["position"][0]
    assert (realm, heaven, node) == destination, result
    expected_visited = json.loads(before["position"][0][3])
    if node not in expected_visited:
        expected_visited.append(node)
    assert json.loads(visited) == expected_visited, result
    payload, receipt_stamina = result["receipt"][0]
    assert json.loads(payload) == [
        "900001",
        dict(zip(("realm", "heaven", "node_id"), before["position"][0][:3])),
        dict(zip(("realm", "heaven", "node_id"), destination)),
        before["stamina"][0][0],
        cost,
    ], result
    assert receipt_stamina == remaining and result["ledger"] == [("applied",)], result
    return result


def _integrity(probe):
    from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork

    for path in (probe.context.paths.game_db, probe.context.paths.player_db):
        with DatabaseUnitOfWork(path, read_only=True) as uow:
            assert uow.query_one("PRAGMA integrity_check")["integrity_check"] == "ok"


def register(case):
    @case("map-movement-parameter-costs", "前往", "节点名、天名、界名的群聊/私聊用法",
          "读取实际地图顺序、交通节点和move_cost配置", "三种移动均按配置扣体力、更新完整位置/访问历史/回执",
          group="map-movement")
    async def configured_costs(probe):
        module, data, realm, heaven, source, node_target = _setup(probe)
        costs = data["meta"]["move_cost"]
        node_cost = int(costs["cross_node"])
        probe.player(user_stamina=node_cost)
        number = 973801
        before = _projection(probe, number)
        reply = await probe.command("前往", f"前往 {node_target['name']}", number=number)
        assert "移动成功" in reply and node_target["name"] in reply, reply
        _applied(probe, number, before, (realm, heaven, node_target["id"]), node_cost)

        travel_realm, travel_heaven, travel_node = _travel_location(data)
        other_heaven = next(
            name for name in module._heaven_names(data, travel_realm) if name != travel_heaven
        )
        heaven_target = module._nodes(data, travel_realm, other_heaven)[0]
        module.map_application.save_status(
            "900001", travel_realm, travel_heaven, travel_node["id"], [travel_node["id"]]
        )
        heaven_cost = int(costs["cross_heaven"])
        probe.player(user_stamina=heaven_cost)
        number = 973802
        before = _projection(probe, number)
        reply = await probe.command("前往", f"前往 {other_heaven}", number=number, event_type="private")
        assert "跨天成功" in reply, reply
        _applied(probe, number, before, (travel_realm, other_heaven, heaven_target["id"]), heaven_cost)

        destination_realm = next(name for name in module._all_realms(data) if name != travel_realm)
        first_heaven = module._get_realm_heaven_order(data, destination_realm)[0]
        realm_target = module._nodes(data, destination_realm, first_heaven)[0]
        module.map_application.save_status(
            "900001", travel_realm, travel_heaven, travel_node["id"], [travel_node["id"]]
        )
        realm_cost = int(costs["cross_realm"])
        probe.player(user_stamina=realm_cost)
        number = 973803
        before = _projection(probe, number)
        reply = await probe.command("前往", f"前往 {destination_realm}", number=number)
        assert "跨界成功" in reply, reply
        _applied(probe, number, before, (destination_realm, first_heaven, realm_target["id"]), realm_cost)
        _integrity(probe)

    @case("map-movement-replay-and-conflict", "前往", "私聊同节点重复及同消息改参",
          "相邻节点与恰好足够体力", "重复不二次扣费；同操作号异参保留冲突且不写",
          group="map-movement")
    async def replay_and_conflict(probe):
        module, data, realm, heaven, source, target = _setup(probe)
        cost = int(data["meta"]["move_cost"]["cross_node"])
        probe.player(user_stamina=cost * 2)
        number = 973804
        before = _projection(probe, number)
        other = _projection(probe, number, "900002")
        text = f"前往 {target['name']}"
        first = await probe.command("前往", text, number=number, event_type="private")
        assert "移动成功" in first, first
        committed = _applied(probe, number, before, (realm, heaven, target["id"]), cost)
        repeated = await probe.command("前往", text, number=number, event_type="private")
        assert "你已在该节点" in repeated, repeated
        assert _projection(probe, number) == committed
        conflict = await probe.command(
            "前往", f"前往 {source['name']}", number=number,
            event_type="private", allow_exception=True,
        )
        assert "已用于不同内容的请求" in conflict, conflict
        assert "前往" not in conflict or "栖霞妖岭" not in conflict, conflict
        assert _projection(probe, number) == committed
        assert _projection(probe, number, "900002") == other
        _integrity(probe)

    @case("map-movement-invalid-and-insufficient", "前往", "空参数、未知地点、体力不足、非交通点跨天",
          "有效隔离地图位置及地图配置", "参数和边界拒绝具体，所有业务投影不变", group="map-movement",
          outcome="rejection")
    async def invalid_and_insufficient(probe):
        module, data, realm, heaven, source, target = _setup(probe)
        probe.player(user_stamina=0)
        before = _projection(probe, 973805)
        assert "请使用：前往" in await probe.command("前往", "前往", number=973805)
        assert "未找到地点" in await probe.command("前往", "前往 无此地点", number=973805)
        assert "移动体力不足" in await probe.command("前往", f"前往 {target['name']}", number=973805)
        assert _projection(probe, 973805) == before

        other_heaven = next(name for name in module._heaven_names(data, realm) if name != heaven)
        blocked = await probe.command("前往", f"前往 {other_heaven}", number=973806)
        assert "跨天需通过" in blocked, blocked
        assert _projection(probe, 973806) == _projection(probe, 973807)
        _integrity(probe)

    faults = (
        ("position-ignore", "player", "BEFORE UPDATE OF realm ON map_status BEGIN SELECT RAISE(IGNORE); END",
         "map_move_position_write_rejected", "地图位置"),
        ("position-rewrite", "player", "AFTER UPDATE OF node_id ON map_status BEGIN UPDATE map_status SET node_id='rewritten' WHERE user_id=NEW.user_id; END",
         "map_move_position_projection_mismatch", "地图位置"),
        ("stamina-ignore", "game", "BEFORE UPDATE OF user_stamina ON user_xiuxian BEGIN SELECT RAISE(IGNORE); END",
         "map_move_stamina_write_rejected", "移动体力"),
        ("stamina-rewrite", "game", "AFTER UPDATE OF user_stamina ON user_xiuxian BEGIN UPDATE user_xiuxian SET user_stamina=NEW.user_stamina+1 WHERE user_id=NEW.user_id; END",
         "map_move_stamina_projection_mismatch", "移动体力"),
        ("receipt-ignore", "game", "BEFORE INSERT ON map_movement_operations BEGIN SELECT RAISE(IGNORE); END",
         "map_move_receipt_write_rejected", "移动回执"),
        ("receipt-rewrite", "game", "AFTER INSERT ON map_movement_operations BEGIN UPDATE map_movement_operations SET payload='rewritten' WHERE operation_id=NEW.operation_id; END",
         "map_move_receipt_projection_mismatch", "移动回执"),
        ("stamina-fractional", "game", "AFTER UPDATE OF user_stamina ON user_xiuxian BEGIN UPDATE user_xiuxian SET user_stamina=NEW.user_stamina+0.5 WHERE user_id=NEW.user_id; END",
         "map_move_stamina_projection_mismatch", "移动体力"),
        ("receipt-fractional", "game", "AFTER INSERT ON map_movement_operations BEGIN UPDATE map_movement_operations SET stamina=NEW.stamina+0.5 WHERE operation_id=NEW.operation_id; END",
         "map_move_receipt_projection_mismatch", "移动回执"),
    )
    for index, (label, database, trigger, reason, subject) in enumerate(faults):
        @case("map-movement-recovery-" + label, "前往", "实际SQLite写路径故障：" + label,
              "合法相邻节点，隔离game/player SQLite", "具体原因/subject、完整回滚；解除故障后原消息恢复",
              group="map-movement", outcome="recovery")
        async def recover(probe, label=label, database=database, trigger=trigger,
                          reason=reason, subject=subject, number=973810 + index):
            from nonebot_plugin_xiuxian_2.core.player_reasons import reason_text

            _, data, realm, heaven, _, target = _setup(probe)
            cost = int(data["meta"]["move_cost"]["cross_node"])
            probe.player(user_stamina=cost)
            path = probe.context.paths.player_db if database == "player" else probe.context.paths.game_db
            operation = f"map-move:{number}:900001"
            probe.seed(path, "CREATE TRIGGER move_fault " + trigger)
            before = _projection(probe, number)
            reply = await probe.command("前往", f"前往 {target['name']}", number=number)
            after = _projection(probe, number)
            probe.evidence.update(fault=label, before=before, after=after, reply=reply)
            assert reason_text(reason) in reply and subject in reply, reply
            assert {key: value for key, value in after.items() if key != "ledger"} == {
                key: value for key, value in before.items() if key != "ledger"
            }, (before, after, reply)
            assert after["ledger"] == [], after
            probe.seed(path, "DROP TRIGGER move_fault")
            recovered = await probe.command("前往", f"前往 {target['name']}", number=number)
            assert "移动成功" in recovered, recovered
            _applied(probe, number, before, (realm, heaven, target["id"]), cost)
            _integrity(probe)

    aborts = (
        ("position-abort", "player", "BEFORE UPDATE OF realm ON map_status BEGIN SELECT RAISE(ABORT,'movement position blocked'); END"),
        ("stamina-abort", "game", "BEFORE UPDATE OF user_stamina ON user_xiuxian BEGIN SELECT RAISE(ABORT,'movement stamina blocked'); END"),
        ("receipt-abort", "game", "BEFORE INSERT ON map_movement_operations BEGIN SELECT RAISE(ABORT,'movement receipt blocked'); END"),
    )
    for index, (label, database, trigger) in enumerate(aborts):
        @case("map-movement-abort-recovery-" + label, "前往", "SQLite ABORT原异常：" + label,
              "合法相邻节点，隔离SQLite", "原异常可观察，移动事务回滚，同操作号解除故障后恢复",
              group="map-movement", outcome="recovery")
        async def abort_recover(probe, label=label, database=database, trigger=trigger,
                                number=973820 + index):
            _, data, realm, heaven, _, target = _setup(probe)
            cost = int(data["meta"]["move_cost"]["cross_node"])
            probe.player(user_stamina=cost)
            path = probe.context.paths.player_db if database == "player" else probe.context.paths.game_db
            probe.seed(path, "CREATE TRIGGER move_abort " + trigger)
            before = _projection(probe, number)
            reply = await probe.command(
                "前往", f"前往 {target['name']}", number=number, allow_exception=True
            )
            after = _projection(probe, number)
            probe.evidence.update(abort=label, before=before, after=after, reply=reply,
                                  errors=probe.calls[-1].get("errors", []), logs=probe.calls[-1].get("logs", []))
            assert "处理提示" in reply and "定位号" in reply, reply
            errors = probe.calls[-1].get("errors", [])
            assert len(errors) == 1 and "sqlite3.IntegrityError" in errors[0], probe.calls[-1]
            assert any("[command] 地图移动" in row.get("message", "") for row in probe.calls[-1].get("logs", [])), probe.calls[-1]
            assert {key: value for key, value in after.items() if key != "ledger"} == {
                key: value for key, value in before.items() if key != "ledger"
            }, (before, after)
            assert after["ledger"] == [("failed",)], after
            probe.seed(path, "DROP TRIGGER move_abort")
            recovered = await probe.command("前往", f"前往 {target['name']}", number=number)
            assert "移动成功" in recovered, recovered
            _applied(probe, number, before, (realm, heaven, target["id"]), cost)
            _integrity(probe)
