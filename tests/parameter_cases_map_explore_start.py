"""Real matcher coverage for starting map exploration."""

import json


def _setup(probe, actor="900001", *, initialize=True):
    from nonebot_plugin_xiuxian_2.features.map.migrations import apply_map_explore_start
    from nonebot_plugin_xiuxian_2.plugin import apply_platform_schema
    from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
    from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_map as module

    probe.clear_cooldowns()
    game, player = probe.context.paths.game_db, probe.context.paths.player_db
    with DatabaseUnitOfWork(game) as uow:
        apply_platform_schema(uow)
        apply_map_explore_start(uow)
    data = module._load_map_data()
    realm, heaven, node = next(
        (realm, heaven, node)
        for realm in module._all_realms(data)
        for heaven in module._heaven_names(data, realm)
        for node in module._nodes(data, realm, heaven)
        if node["type"] in module.EXPLORE_NODE_TYPES
    )
    module.map_application.save_status(actor, realm, heaven, node["id"], [node["id"]])
    if initialize:
        module.map_application.save_explore_status(actor, _idle_status())
        module.map_application.save_daily_limit(actor, {
            "date": module._today_str(), "gather_count": 0, "combat_count": 0,
            "explore_count": 0, "resource_total_count": 0,
        })
        module.map_application.set_cooldown(actor, "explore_start_cd_until", None)
    return module, data, realm, heaven, node


async def _command(probe, *args, **kwargs):
    probe.clear_cooldowns()
    return await probe.command(*args, **kwargs)


def _idle_status():
    return {
        "running": 0, "node_type": "", "node_name": "", "start_time": "",
        "duration_min": 0, "settlement": "", "max_duration_min": 0, "interval_min": 0,
    }


def _operation(number, actor="900001"):
    return f"map-explore-start:{actor}:{number}"


def _projection(probe, number, actor="900001"):
    game, player, operation = probe.context.paths.game_db, probe.context.paths.player_db, _operation(number, actor)
    return {
        "stamina": probe.rows(game, "SELECT user_stamina FROM user_xiuxian WHERE user_id=?", (actor,)),
        "position": probe.rows(player, "SELECT realm,heaven,node_id,visited_nodes FROM map_status WHERE user_id=?", (actor,)),
        "status": probe.rows(player, "SELECT running,node_type,node_name,start_time,duration_min,settlement,max_duration_min,interval_min FROM map_explore_status WHERE user_id=?", (actor,)),
        "daily": probe.rows(player, "SELECT date,explore_count,resource_total_count FROM map_daily_limit WHERE user_id=?", (actor,)),
        "cooldown": probe.rows(player, "SELECT explore_start_cd_until FROM map_cooldown WHERE user_id=?", (actor,)),
        "receipt": probe.rows(game, "SELECT payload,stamina FROM map_explore_start_operations WHERE operation_id=?", (operation,)),
        "ledger": probe.rows(game, "SELECT status FROM operation_ledger WHERE operation_id=? AND action='map.explore_start'", (operation,)),
    }


def _integrity(probe):
    from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork

    for path in (probe.context.paths.game_db, probe.context.paths.player_db):
        with DatabaseUnitOfWork(path, read_only=True) as uow:
            assert uow.query_one("PRAGMA integrity_check")["integrity_check"] == "ok"


def register(case):
    @case("map-explore-start-parameter-contract", "开始探索", "默认、合法、下限、超长、别名及重复消息",
          "实际探索节点，冻结时钟，隔离game/player SQLite", "遵循既有时长/上限合同；成功四写一致，重复不二次消费",
          group="map-explore-start")
    async def parameter_contract(probe):
        from datetime import datetime
        from types import SimpleNamespace
        from unittest.mock import patch

        module, _, realm, heaven, node = _setup(probe)
        conf = module.EXPLORE_CONFIG[node["type"]]
        probe.player(user_stamina=100)
        fixed = datetime(2026, 11, 22, 12, 0, 0)
        with patch.object(module, "runtime_clock", SimpleNamespace(now=lambda: fixed)):
            for number, command, text, expected in (
                (974101, "开始探索", "开始探索", conf["duration_min"]),
                (974102, "开始探索", "开始探索 1", 5),
                (974103, "节点探索", "节点探索 45", 45),
                (974104, "开始探索", "开始探索 " + str(conf["max_duration_min"]), conf["max_duration_min"]),
                (974109, "开始探索", "开始探索 999999999999", conf["max_duration_min"]),
                (974108, "开始探索", "开始探索 " + "9" * 5000, conf["max_duration_min"]),
                (974105, "开始探索", "开始探索 abc", conf["duration_min"]),
            ):
                module.map_application.save_explore_status("900001", _idle_status())
                module.map_application.set_cooldown("900001", "explore_start_cd_until", None)
                number_before = _projection(probe, number)
                reply = await _command(probe, command, text, number=number)
                after = _projection(probe, number)
                probe.evidence.setdefault("parameters", []).append({"input": text, "reply": reply, "before": number_before, "after": after})
                assert "已开始" in reply and f"计划时长：{expected}分钟" in reply, reply
                if number in {974109, 974108}:
                    assert text.split()[-1] not in reply, reply
                assert after["stamina"] == [(number_before["stamina"][0][0] - conf["stamina_cost"],)], after
                state = after["status"][0]
                assert state[:3] == (1, node["type"], node["name"]), after
                assert state[4:8] == (expected, "", conf["max_duration_min"], conf["base_interval_min"]), after
                assert state[3] == fixed.strftime("%Y-%m-%d %H:%M:%S"), after
                assert after["daily"][0][1:] == (0, 0), after
                assert after["cooldown"] == [("2026-11-22 12:00:30",)], after
                assert len(after["receipt"]) == 1 and after["ledger"] == [("applied",)], after
                module.map_application.save_explore_status("900001", _idle_status())
                module.map_application.set_cooldown("900001", "explore_start_cd_until", None)

        number = 974106
        reply = await _command(probe, "开始探索", "开始探索 20", number=number)
        committed = _projection(probe, number)
        repeated = await _command(probe, "开始探索", "开始探索 20", number=number)
        assert "请稍候" in repeated and "已有进行中的探索" not in repeated, repeated
        assert _projection(probe, number) == committed
        assert committed["status"][0][4] == 20 and committed["ledger"] == [("applied",)], committed

        conflict_number = 974107
        module.map_application.save_explore_status("900001", _idle_status())
        module.map_application.set_cooldown("900001", "explore_start_cd_until", None)
        assert "已开始" in await _command(probe, "开始探索", "开始探索 20", number=conflict_number)
        module.map_application.save_explore_status("900001", _idle_status())
        module.map_application.set_cooldown("900001", "explore_start_cd_until", None)
        before_conflict = _projection(probe, conflict_number)
        from nonebot_plugin_xiuxian_2.core.player_reasons import reason_text

        conflict = await _command(
            probe, "开始探索", "开始探索 30", number=conflict_number, allow_exception=True,
        )
        assert reason_text("operation_conflict") in conflict, conflict
        assert _projection(probe, conflict_number) == before_conflict
        assert module.map_application.explore_status("900002") is None
        assert (realm, heaven, node["id"]) == tuple(committed["position"][0][:3])
        _integrity(probe)

    @case("map-explore-start-rejections", "开始探索", "未注册、空白/无效参数、节点、体力、冷却、每日上限及进行中前置",
          "合法真实matcher输入与各自隔离状态快照", "具体正常拒绝且不扣体力/创建回执/变更探索状态",
          group="map-explore-start", outcome="rejection")
    async def rejection_contract(probe):
        from datetime import datetime, timedelta
        from types import SimpleNamespace
        from unittest.mock import patch

        module, data, realm, heaven, node = _setup(probe)
        probe.player(user_stamina=100)
        assert "输入【我要修仙】加入" in await _command(probe, "开始探索", actor="900099")
        assert "已开始" in await _command(probe, "开始探索", "开始探索 bad", number=974110)
        module.map_application.save_explore_status("900001", _idle_status())
        module.map_application.set_cooldown("900001", "explore_start_cd_until", None)
        probe.player(user_stamina=0)
        assert "体力不足" in await _command(probe, "开始探索", "开始探索", number=974111)
        assert _projection(probe, 974111)["receipt"] == []

        module.map_application.save_explore_status("900001", _idle_status())
        module.map_application.set_cooldown("900001", "explore_start_cd_until", None)
        probe.player(user_stamina=100)
        other = next((r, h, n) for r in module._all_realms(data) for h in module._heaven_names(data, r)
                     for n in module._nodes(data, r, h) if n["type"] not in module.EXPLORE_NODE_TYPES)
        module.map_application.save_status("900001", other[0], other[1], other[2]["id"], [other[2]["id"]])
        reply = await _command(probe, "开始探索", "开始探索", number=974112)
        assert "不支持长时间探索" in reply, reply
        assert _projection(probe, 974112)["receipt"] == []
        module.map_application.save_status("900001", realm, heaven, node["id"], [node["id"]])

        fixed = datetime(2026, 11, 22, 12, 0, 0)
        with patch.object(module, "runtime_clock", SimpleNamespace(now=lambda: fixed)):
            module.map_application.set_cooldown("900001", "explore_start_cd_until", (fixed + timedelta(seconds=90)).strftime("%Y-%m-%d %H:%M:%S"))
            assert "请稍候" in await _command(probe, "开始探索", "开始探索", number=974113)
            module.map_application.set_cooldown("900001", "explore_start_cd_until", None)
            module.map_application.save_daily_limit("900001", {"date": "2026-11-22", "explore_count": module.DAILY_LIMIT_CONFIG["explore"], "resource_total_count": 0})
            assert "已达上限" in await _command(probe, "开始探索", "开始探索", number=974114)
            module.map_application.save_daily_limit("900001", {"date": "2026-11-22", "explore_count": 0, "resource_total_count": 0})
            module.map_application.save_explore_status("900001", {**_idle_status(), "running": 1, "node_type": node["type"], "node_name": node["name"], "start_time": "2026-11-22 11:00:00", "duration_min": 20, "max_duration_min": 120, "interval_min": 20})
            assert "已有进行中的探索" in await _command(probe, "开始探索", "开始探索", number=974115)

        for number in (974112, 974113, 974114, 974115):
            after = _projection(probe, number)
            assert after["receipt"] == [] and after["ledger"] == [], after

        probe.seed(probe.context.paths.player_db, "DELETE FROM map_explore_status WHERE user_id='900001'")
        probe.player(user_stamina=0)
        missing_status_reply = await _command(probe, "开始探索", "开始探索", number=974116)
        initialized = _projection(probe, 974116)
        probe.evidence["missing_status_normalization"] = {"reply": missing_status_reply, "projection": initialized}
        assert "体力不足" in missing_status_reply and initialized["status"] == [
            (0, "", "", "", 0, "", 0, 0)
        ], initialized
        assert initialized["stamina"] == [(0,)] and not initialized["receipt"] and not initialized["ledger"], initialized
        _integrity(probe)

    write_faults = (
        ("stamina-ignore", "game", "BEFORE UPDATE OF user_stamina ON user_xiuxian BEGIN SELECT RAISE(IGNORE); END", "map_explore_start_stamina_write_rejected", "探索体力"),
        ("status-ignore", "player", "BEFORE UPDATE OF running ON map_explore_status WHEN NEW.running=1 BEGIN SELECT RAISE(IGNORE); END", "map_explore_start_status_write_rejected", "探索状态"),
        ("status-rewrite", "player", "AFTER UPDATE OF running ON map_explore_status WHEN NEW.running=1 BEGIN UPDATE map_explore_status SET node_name='rewritten' WHERE user_id=NEW.user_id; END", "map_explore_start_status_projection_mismatch", "探索状态"),
        ("cooldown-ignore", "player", "BEFORE UPDATE OF explore_start_cd_until ON map_cooldown BEGIN SELECT RAISE(IGNORE); END", "map_explore_start_cooldown_write_rejected", "探索冷却"),
        ("cooldown-rewrite", "player", "AFTER UPDATE OF explore_start_cd_until ON map_cooldown BEGIN UPDATE map_cooldown SET explore_start_cd_until='rewritten' WHERE user_id=NEW.user_id; END", "map_explore_start_cooldown_projection_mismatch", "探索冷却"),
        ("receipt-ignore", "game", "BEFORE INSERT ON map_explore_start_operations BEGIN SELECT RAISE(IGNORE); END", "map_explore_start_receipt_write_rejected", "探索永久回执"),
        ("receipt-rewrite", "game", "AFTER INSERT ON map_explore_start_operations BEGIN UPDATE map_explore_start_operations SET payload='rewritten' WHERE operation_id=NEW.operation_id; END", "map_explore_start_receipt_projection_mismatch", "探索永久回执"),
    )
    for index, (label, database, trigger, reason, subject) in enumerate(write_faults):
        @case("map-explore-start-recovery-" + label, "开始探索", "隔离SQLite写故障：" + label,
              "已注册玩家、有效探索节点、双库真实迁移", "具体原因/subject、双库回滚、解除故障后同消息恢复",
              group="map-explore-start", outcome="recovery")
        async def recover(probe, label=label, database=database, trigger=trigger, reason=reason,
                          subject=subject, number=974120 + index):
            from nonebot_plugin_xiuxian_2.core.player_reasons import reason_text

            module, _, _, _, node = _setup(probe)
            cost = module.EXPLORE_CONFIG[node["type"]]["stamina_cost"]
            probe.player(user_stamina=100)
            path = probe.context.paths.player_db if database == "player" else probe.context.paths.game_db
            probe.seed(path, "CREATE TRIGGER explore_fault " + trigger)
            before = _projection(probe, number)
            reply = await _command(probe, "开始探索", f"开始探索 20", number=number)
            after = _projection(probe, number)
            probe.evidence.update(**{label: {"before": before, "after": after, "reply": reply}})
            assert reason_text(reason) in reply and subject in reply, reply
            for key in ("stamina", "position", "status", "daily", "cooldown", "receipt"):
                assert after[key] == before[key], (label, key, before, after)
            assert after["ledger"] == [], after
            probe.seed(path, "DROP TRIGGER explore_fault")
            recovered = await _command(probe, "开始探索", "开始探索 20", number=number)
            committed = _projection(probe, number)
            assert "已开始" in recovered, recovered
            assert committed["stamina"] == [(100 - cost,)] and committed["status"][0][0] == 1, committed
            assert len(committed["receipt"]) == 1 and committed["ledger"] == [("applied",)], committed
            replay = await _command(probe, "开始探索", "开始探索 20", number=number)
            assert "请稍候" in replay and _projection(probe, number) == committed, replay
            _integrity(probe)

    abort_faults = (
        ("stamina", "game", "BEFORE UPDATE OF user_stamina ON user_xiuxian BEGIN SELECT RAISE(ABORT,'explore stamina blocked'); END"),
        ("status", "player", "BEFORE UPDATE OF running ON map_explore_status WHEN NEW.running=1 BEGIN SELECT RAISE(ABORT,'explore status blocked'); END"),
        ("cooldown", "player", "BEFORE INSERT ON map_cooldown BEGIN SELECT RAISE(ABORT,'explore cooldown blocked'); END"),
        ("receipt", "game", "BEFORE INSERT ON map_explore_start_operations BEGIN SELECT RAISE(ABORT,'explore receipt blocked'); END"),
    )
    for index, (label, database, trigger) in enumerate(abort_faults):
        @case("map-explore-start-abort-recovery-" + label, "开始探索", "SQLite ABORT：" + label,
              "有效参数与隔离双库", "原异常保留、事务回滚、故障解除后同消息恢复",
              group="map-explore-start", outcome="recovery")
        async def abort_recover(probe, label=label, database=database, trigger=trigger,
                                number=974140 + index):
            from types import SimpleNamespace
            from unittest.mock import patch

            module, _, _, _, _ = _setup(probe)
            probe.player(user_stamina=100)
            path = probe.context.paths.player_db if database == "player" else probe.context.paths.game_db
            probe.seed(path, "CREATE TRIGGER explore_abort " + trigger)
            before = _projection(probe, number)
            class FrozenClock:
                current = module.runtime_clock.now().replace(tzinfo=None)

                def now(self):
                    return self.current

            with patch.object(module, "runtime_clock", FrozenClock()):
                reply = await _command(probe, "开始探索", "开始探索 20", number=number, allow_exception=True)
                failed = _projection(probe, number)
                logs = probe.calls[-1].get("logs", [])
                probe.evidence.update(**{label + "_abort": {"before": before, "failed": failed, "reply": reply, "logs": logs}})
                assert "定位号" in reply, probe.calls[-1]
                assert any("[command] 发起探索" in row.get("message", "") and "sqlite3.IntegrityError" in row.get("exception", "") for row in logs), probe.calls[-1]
                for key in ("stamina", "position", "status", "daily", "cooldown", "receipt"):
                    assert failed[key] == before[key], (label, key, before, failed)
                assert failed["ledger"] == [("failed",)], failed
                probe.seed(path, "DROP TRIGGER explore_abort")
                recovered = await _command(probe, "开始探索", "开始探索 20", number=number)
            failed = _projection(probe, number)
            committed = _projection(probe, number)
            assert "已开始" in recovered and committed["ledger"] == [("applied",)], (recovered, committed)
            assert len(committed["receipt"]) == 1 and committed["status"][0][0] == 1, committed
            _integrity(probe)
