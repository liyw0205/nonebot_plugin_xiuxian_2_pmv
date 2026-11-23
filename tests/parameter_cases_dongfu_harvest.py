"""Real registered-command coverage for mature cave harvests."""

import json
from datetime import datetime
from unittest.mock import patch


def _database_projection(probe):
    from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork

    result = {}
    for database in (
        probe.context.paths.game_db,
        probe.context.paths.player_db,
        probe.context.paths.impart_db,
    ):
        with DatabaseUnitOfWork(database, read_only=True) as uow:
            tables = uow.query_all(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name NOT LIKE 'sqlite_%' ORDER BY name"
            )
            result[database.name] = {
                row["name"]: [
                    tuple(record.values())
                    for record in uow.query_all(
                        'SELECT * FROM "' + str(row["name"]).replace('"', '""') + '"'
                    )
                ]
                for row in tables
            }
            assert uow.query_one("PRAGMA integrity_check")["integrity_check"] == "ok"
    return result


def _seed(probe):
    from nonebot_plugin_xiuxian_2.compatibility.game_event_effects import LegacyGameEventEffects
    from nonebot_plugin_xiuxian_2.features.dongfu.application import DongfuApplication
    from nonebot_plugin_xiuxian_2.features.dongfu.plant_slots import empty_plant_slot
    from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_dongfu as module

    game, player = probe.context.paths.game_db, probe.context.paths.player_db
    now = datetime(2026, 11, 21, 12)
    slots = [empty_plant_slot(index) for index in range(1, 4)]
    for index, finish in ((0, "2026-11-21 12:00:00"), (1, "2026-11-21 13:00:00")):
        slots[index].update(seed_id=21001, seed_name=module.SEED_CONFIG[21001]["name"],
                            plant_start="2026-11-21 10:00:00", plant_finish=finish)
    probe.seed(player, "INSERT INTO dongfu_status(user_id,built,plot_count,plant_slots,planting,"
               "plant_seed_id,plant_start,plant_finish,harvest_settlement) VALUES(?,1,3,?,1,21001,?,?, '') "
               "ON CONFLICT(user_id) DO UPDATE SET built=1,plot_count=3,plant_slots=excluded.plant_slots,"
               "planting=1,plant_seed_id=21001,plant_start=excluded.plant_start,"
               "plant_finish=excluded.plant_finish,harvest_settlement=''",
               ("900001", json.dumps(slots, ensure_ascii=False), slots[0]["plant_start"], slots[0]["plant_finish"]))
    item_id = next(item_id for item_id in module.HERB_LOW if module.items.get_data_by_item_id(item_id))
    item = module.items.get_data_by_item_id(item_id)
    probe.grant_item(item_id, quantity=10)
    effects = LegacyGameEventEffects(game, player, clock=probe.context.clock)
    module.configure_dongfu_application(DongfuApplication(game, player, game_event_effects=effects))
    return module, now, slots, item_id, item, effects


def _business(probe):
    game, player = probe.context.paths.game_db, probe.context.paths.player_db
    return {
        "inventory": probe.rows(game, "SELECT * FROM back ORDER BY user_id,goods_id"),
        "cave": probe.rows(player, "SELECT * FROM dongfu_status ORDER BY user_id"),
        "receipt": probe.rows(game, "SELECT * FROM dongfu_harvest_operations ORDER BY operation_id"),
        "outbox": probe.rows(game, "SELECT * FROM domain_outbox ORDER BY event_id"),
    }


async def _command(probe, name, text, *, expect_abort=False, **kwargs):
    reply = await probe.command(name, text, allow_exception=True, **kwargs)
    call = probe.calls[-1]
    # The legacy parser tries microseconds before the supported whole-second format.
    # Its first ValueError is a normal format fallback, not a swallowed business error.
    for caught in call.get("caught_exceptions", []):
        assert caught["function"] == "_parse_dt", caught
        assert "ValueError: time data" in caught["traceback"] and "%Y-%m-%d %H:%M:%S.%f" in caught["traceback"], caught
    if expect_abort:
        traces = "\n".join(call["errors"])
        assert "sqlite3.IntegrityError: harvest refused" in traces and "Traceback" in traces, call
    else:
        assert call["errors"] == [], call
    return reply


def register(case):
    @case("dongfu-harvest-selected-boundaries", "洞府收获", "指定田、别名/私聊、空/未成熟/非法/越界参数",
          "1号恰好成熟，2号未成熟，3号空田", "只收指定田；正常拒绝不改业务数据",
          group="dongfu-harvest")
    async def selected_boundaries(probe):
        module, now, slots, item_id, item, _ = _seed(probe)
        for index, (text, expected) in enumerate((
            ("洞府收获 2", "还需60分钟"), ("洞府收获 3", "当前空闲"),
            ("洞府收获 0", "灵田编号只能是1到3"), ("洞府收获 4", "灵田编号只能是1到3"),
            ("洞府收获 two", "请使用"), ("洞府收获 1 2", "请使用"),
        )):
            before = _business(probe)
            with patch.object(module, "_now", return_value=now):
                reply = await _command(probe, "洞府收获", text, number=970410 + index)
            assert expected in reply, reply
            assert _business(probe) == before
        with (patch.object(module, "_now", return_value=now),
              patch.object(module, "_roll_harvest", return_value=[(item_id, item["name"], item["type"], 1)])):
            reply = await _command(probe, "收获洞府", "收获洞府 1", number=970416, event_type="private")
        assert "共收获1块灵田" in reply, reply
        state = json.loads(probe.rows(probe.context.paths.player_db,
                           "SELECT plant_slots FROM dongfu_status WHERE user_id='900001'")[0][0])
        assert [row["seed_id"] for row in state] == [0, 21001, 0], state
        assert probe.rows(probe.context.paths.game_db, "SELECT goods_num,bind_num FROM back "
                          "WHERE user_id='900001' AND goods_id=?", (item_id,)) == [(11, 1)]
        _database_projection(probe)

    fault_cases = (
        ("state", "player", "AFTER UPDATE ON dongfu_status WHEN NEW.harvest_settlement='' "
         "BEGIN UPDATE dongfu_status SET planting=0; END", "dongfu_harvest_state_projection_mismatch"),
        ("inventory", "game", "BEFORE UPDATE ON back BEGIN SELECT RAISE(IGNORE); END",
         "dongfu_harvest_inventory_write_rejected"),
        ("receipt", "game", "BEFORE INSERT ON dongfu_harvest_operations BEGIN SELECT RAISE(IGNORE); END",
         "dongfu_harvest_receipt_write_rejected"),
        ("outbox", "game", "BEFORE INSERT ON domain_outbox BEGIN SELECT RAISE(IGNORE); END",
         "dongfu_harvest_outbox_write_rejected"),
        ("abort", "game", "BEFORE INSERT ON dongfu_harvest_operations BEGIN SELECT RAISE(ABORT,'harvest refused'); END", None),
    )
    for label, database, trigger, reason in fault_cases:
        def build(database, trigger, reason):
            async def recovery(probe):
                from nonebot_plugin_xiuxian_2.core.player_reasons import reason_text
                module, now, slots, item_id, item, _ = _seed(probe)
                target = probe.context.paths.player_db if database == "player" else probe.context.paths.game_db
                before = _business(probe)
                probe.seed(target, "CREATE TRIGGER harvest_fault " + trigger)
                drop = [(item_id, item["name"], item["type"], 1)]
                with (patch.object(module, "_now", return_value=now),
                      patch.object(module, "_roll_harvest", return_value=drop) as roll):
                    reply = await _command(probe, "洞府收获", "洞府收获 1", number=970420, expect_abort=reason is None)
                    after = _business(probe)
                    assert after["inventory"] == before["inventory"] and not after["receipt"] and not after["outbox"], after
                    plan_raw = probe.rows(probe.context.paths.player_db,
                              "SELECT harvest_settlement,plant_slots FROM dongfu_status WHERE user_id='900001'")[0]
                    plan = json.loads(plan_raw[0])
                    assert plan["operation_id"] == "dongfu-harvest:900001:970420", plan
                    assert json.loads(plan_raw[1]) == slots
                    if reason:
                        assert reason_text(reason) in reply and "完成，共收获" not in reply, reply
                    else:
                        assert "定位号" in reply, reply
                        logs = probe.calls[-1]["logs"]
                        assert any("harvest refused" in json.dumps(row, ensure_ascii=False) for row in logs), logs
                    probe.seed(target, "DROP TRIGGER harvest_fault")
                    success = await _command(probe, "洞府收获", "洞府收获 1", number=970420)
                    assert "共收获1块灵田" in success, success
                    assert roll.call_count == 1, "retry must use the frozen reward plan"
                    committed = _database_projection(probe)
                    replay = await _command(probe, "洞府收获", "洞府收获 1", number=970420)
                    assert "已经处理" in replay, replay
                    assert _database_projection(probe) == committed
                probe.evidence.update(reason=reason or "sqlite_abort", plan=plan, recovery=True, replay_unchanged=True)
            return recovery
        case("dongfu-harvest-recovery-" + label, "洞府收获", "合法指定田遇" + label + "写故障",
             "隔离SQLite与固定奖励/时钟", "失败整笔回滚、冻结方案保留，同消息恢复/replay",
             group="dongfu-harvest", outcome="recovery")(build(database, trigger, reason))

    @case("dongfu-harvest-capacity-recovery", "洞府收获", "背包超限拒绝后释放容量重试原消息",
          "成熟田且奖励后超过物品上限", "保留随机方案、无部分领取、恰好到上限成功",
          group="dongfu-harvest", outcome="recovery")
    async def capacity_recovery(probe):
        module, now, slots, item_id, item, _ = _seed(probe)
        config = module.XiuConfig()
        probe.seed(probe.context.paths.game_db, "UPDATE back SET goods_num=? WHERE user_id='900001' AND goods_id=?",
                   (config.max_goods_num, item_id))
        drop = [(item_id, item["name"], item["type"], 1)]
        with (patch.object(module, "_now", return_value=now),
              patch.object(module, "_roll_harvest", return_value=drop) as roll):
            refused = await _command(probe, "洞府收获", "洞府收获 1", number=970430)
            assert "背包物品已达上限" in refused, refused
            assert not probe.rows(probe.context.paths.game_db, "SELECT * FROM dongfu_harvest_operations")
            probe.seed(probe.context.paths.game_db, "UPDATE back SET goods_num=? WHERE user_id='900001' AND goods_id=?",
                       (config.max_goods_num - 1, item_id))
            reply = await _command(probe, "洞府收获", "洞府收获 1", number=970430)
            assert "共收获1块灵田" in reply, reply
            assert roll.call_count == 1
        assert probe.rows(probe.context.paths.game_db, "SELECT goods_num FROM back WHERE user_id='900001' AND goods_id=?",
                          (item_id,)) == [(config.max_goods_num,)]
        _database_projection(probe)

    @case("dongfu-harvest-actual-roll-fertilizer", "洞府收获", "真实随机产出链、施肥和地脉加成",
          "已成熟1号田施肥2次，地脉额外产出必触发", "原随机规则产生4件绑定药材，任务/统计实际落库",
          group="dongfu-harvest")
    async def actual_roll(probe):
        module, now, slots, item_id, item, _ = _seed(probe)
        slots[0]["fertilizer"] = 2
        probe.seed(probe.context.paths.player_db, "UPDATE dongfu_status SET plant_slots=? WHERE user_id='900001'",
                   (json.dumps(slots, ensure_ascii=False),))

        def choose(pool):
            if pool and isinstance(pool[0], int):
                return next(value for value in pool if module.items.get_data_by_item_id(value))
            return pool[0]

        with (patch.object(module, "_now", return_value=now),
              patch.object(module, "_get_geomancy", return_value={"harvest_bonus": 1.0}),
              patch.object(module.runtime_random, "randint", return_value=1),
              patch.object(module.runtime_random, "random", return_value=0),
              patch.object(module.runtime_random, "choice", side_effect=choose)):
            reply = await _command(probe, "洞府收获", "洞府收获 1", number=970440)
        assert f"{item['name']} x4" in reply, reply
        assert probe.rows(probe.context.paths.game_db, "SELECT goods_num,bind_num FROM back "
                          "WHERE user_id='900001' AND goods_id=?", (item_id,)) == [(14, 4)]
        task = probe.rows(probe.context.paths.player_db, "SELECT daily_progress FROM xiuxian_tasks WHERE user_id='900001'")
        assert len(task) == 1 and json.loads(task[0][0])["daily_dongfu_harvest"] == 1, task
        assert probe.rows(probe.context.paths.player_db, 'SELECT "洞府收获" FROM statistics WHERE user_id=\'900001\'') == [(1,)]
        _database_projection(probe)

    @case("dongfu-harvest-effects-recovery", "洞府收获", "业务成功后统计源故障，再由同消息重放补偿",
          "有效成熟田，统计库视图使真实SELECT失败", "物品只发一次、outbox pending保留、源恢复后sent且统计/任务只记一次",
          group="dongfu-harvest", outcome="recovery")
    async def effects_recovery(probe):
        module, now, slots, item_id, item, _ = _seed(probe)
        player, game = probe.context.paths.player_db, probe.context.paths.game_db
        probe.seed(player, "ALTER TABLE statistics RENAME TO statistics_source")
        probe.seed(player, "CREATE VIEW statistics AS SELECT * FROM missing_harvest_statistics")
        with (patch.object(module, "_now", return_value=now),
              patch.object(module, "_roll_harvest", return_value=[(item_id, item["name"], item["type"], 1)]) as roll):
            first = await _command(probe, "洞府收获", "洞府收获 1", number=970450)
            assert "共收获1块灵田" in first and "统计与任务进度正在补偿" in first, first
            state_sql = "SELECT plant_slots,harvest_settlement FROM dongfu_status WHERE user_id='900001'"
            committed_state = probe.rows(player, state_sql)
            inventory = probe.rows(game, "SELECT goods_num,bind_num FROM back WHERE user_id='900001' AND goods_id=?", (item_id,))
            event_id = "dongfu.harvest.effects:dongfu-harvest:900001:970450"
            assert probe.rows(game, "SELECT status,attempts FROM domain_outbox WHERE event_id=?", (event_id,)) == [("pending", 1)]
            probe.seed(player, "DROP VIEW statistics")
            probe.seed(player, "ALTER TABLE statistics_source RENAME TO statistics")
            replay = await _command(probe, "洞府收获", "洞府收获 1", number=970450)
            assert "已经处理" in replay and "正在补偿" not in replay, replay
            assert probe.rows(game, "SELECT status,attempts FROM domain_outbox WHERE event_id=?", (event_id,)) == [("sent", 1)]
            assert probe.rows(player, 'SELECT "洞府收获" FROM statistics WHERE user_id=\'900001\'') == [(1,)]
            assert probe.rows(player, state_sql) == committed_state
            assert probe.rows(game, "SELECT goods_num,bind_num FROM back WHERE user_id='900001' AND goods_id=?", (item_id,)) == inventory
            committed = _database_projection(probe)
            await _command(probe, "洞府收获", "洞府收获 1", number=970450)
            assert _database_projection(probe) == committed and roll.call_count == 1
        probe.evidence.update(event_id=event_id, pending_retained=True, recovered=True, inventory=inventory)

    @case(
        "dongfu-harvest-mature-replay",
        "洞府收获",
        "无参数收取全部成熟灵田；重复相同消息号",
        "本人已建洞府，1/3号田成熟、2号田未成熟",
        "只领取成熟田奖励、保留未成熟田，并通过永久回执重放",
        group="dongfu-harvest",
    )
    async def mature_harvest_and_replay(probe):
        from nonebot_plugin_xiuxian_2.compatibility.game_event_effects import (
            LegacyGameEventEffects,
        )
        from nonebot_plugin_xiuxian_2.features.dongfu.application import DongfuApplication
        from nonebot_plugin_xiuxian_2.features.dongfu.plant_slots import empty_plant_slot
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_dongfu as module

        game = probe.context.paths.game_db
        player = probe.context.paths.player_db
        now = datetime(2026, 11, 21, 12, 0, 0)
        slots = [empty_plant_slot(index) for index in range(1, 4)]
        for index, start, finish in (
            (0, "2026-11-21 09:00:00.000000", "2026-11-21 10:00:00.000000"),
            (1, "2026-11-21 11:00:00.000000", "2026-11-21 13:00:00.000000"),
            (2, "2026-11-21 08:00:00.000000", "2026-11-21 09:00:00.000000"),
        ):
            slots[index].update(
                {
                    "seed_id": 21001,
                    "seed_name": module.SEED_CONFIG[21001]["name"],
                    "plant_start": start,
                    "plant_finish": finish,
                }
            )

        probe.seed(
            player,
            "INSERT INTO dongfu_status(user_id,built,node_type,array_level,plot_count,"
            "planting,plant_seed_id,plant_start,plant_finish,harvest_settlement,plant_slots) "
            "VALUES(?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(user_id) DO UPDATE SET "
            "built=excluded.built,node_type=excluded.node_type,array_level=excluded.array_level,"
            "plot_count=excluded.plot_count,planting=excluded.planting,"
            "plant_seed_id=excluded.plant_seed_id,plant_start=excluded.plant_start,"
            "plant_finish=excluded.plant_finish,harvest_settlement=excluded.harvest_settlement,"
            "plant_slots=excluded.plant_slots",
            (
                "900001", 1, "", 0, 3, 1, 21001, slots[0]["plant_start"],
                slots[0]["plant_finish"], "", json.dumps(slots, ensure_ascii=False),
            ),
        )
        item_id = next(
            item_id for item_id in module.HERB_LOW
            if module.items.get_data_by_item_id(item_id)
        )
        item = module.items.get_data_by_item_id(item_id)
        probe.grant_item(item_id, quantity=10)
        effects = LegacyGameEventEffects(game, player, clock=probe.context.clock)
        module.configure_dongfu_application(
            DongfuApplication(game, player, game_event_effects=effects)
        )

        fixed_drop = [(item_id, item["name"], item.get("type", "药材"), 1)]
        with (
            patch.object(module, "_now", return_value=now),
            patch.object(module, "_roll_harvest", return_value=fixed_drop),
        ):
            first = await probe.command("洞府收获", "洞府收获", number=970401)
            after_first = _database_projection(probe)
            replay = await probe.command("洞府收获", "洞府收获", number=970401)

        state = probe.rows(
            player,
            "SELECT plant_slots,planting,harvest_settlement FROM dongfu_status "
            "WHERE user_id='900001'",
        )[0]
        persisted_slots = json.loads(state[0])
        assert "共收获2块灵田" in first and item["name"] in first, first
        assert [slot["seed_id"] for slot in persisted_slots] == [0, 21001, 0], persisted_slots
        assert int(state[1]) == 1 and state[2] == "", state

        inventory = probe.rows(
            game,
            "SELECT goods_num,bind_num FROM back WHERE user_id='900001' AND goods_id=?",
            (item_id,),
        )
        assert inventory == [(12, 2)], inventory
        operation_id = "dongfu-harvest:900001:970401"
        receipt = probe.rows(
            game,
            "SELECT payload,rewards FROM dongfu_harvest_operations WHERE operation_id=?",
            (operation_id,),
        )
        assert len(receipt) == 1 and json.loads(receipt[0][1]) == [[item_id, 2]], receipt
        event_id = "dongfu.harvest.effects:" + operation_id
        outbox = probe.rows(
            game,
            "SELECT status,payload_json FROM domain_outbox WHERE event_id=?",
            (event_id,),
        )
        assert len(outbox) == 1, outbox
        statistics_columns = [
            row[1] for row in probe.rows(player, "PRAGMA table_info(statistics)")
        ]
        stats = probe.rows(
            player,
            'SELECT "洞府收获" FROM statistics WHERE user_id=\'900001\'',
        )
        stats_events = probe.rows(
            player,
            "SELECT event_id,event_key,user_id,increment FROM game_event_statistics_events "
            "WHERE event_id=?",
            (event_id,),
        )
        probe.evidence.update(
            outbox_status=outbox[0][0],
            outbox_payload=json.loads(outbox[0][1]),
            statistics_columns=statistics_columns,
            stats=stats,
            stats_events=stats_events,
            effects_repository=getattr(
                getattr(effects, "statistics", None), "database", None
            ),
        )
        assert stats == [(2,)], stats
        assert len(stats_events) == 1 and stats_events[0][1:] == (
            "洞府收获", "900001", 2
        ), stats_events
        assert outbox[0][0] == "sent", outbox
        assert "统计与任务进度正在补偿" not in first, first
        assert "该收获请求已经处理" in replay, replay
        assert _database_projection(probe) == after_first
        probe.evidence.update(
            item_id=item_id,
            status_after_first=(state[1], state[2]),
            inventory=inventory,
            receipt=receipt,
            outbox_status=outbox[0][0],
            harvest_stat=stats,
            replay=replay,
            replay_unchanged=True,
        )
