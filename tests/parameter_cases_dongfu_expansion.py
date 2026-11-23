"""Real matcher coverage for cave expansion and its multi-database writes."""

import json

from tests.parameter_cases_dongfu_harvest import _database_projection


def _seed(probe, *, count=3, stone=200_000_000, deeds=10, bound=0):
    from nonebot_plugin_xiuxian_2.features.dongfu.application import DongfuApplication
    from nonebot_plugin_xiuxian_2.features.dongfu.plant_slots import canonical_plant_slots, empty_plant_slot
    from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_dongfu as module

    game, player = probe.context.paths.game_db, probe.context.paths.player_db
    slots = [empty_plant_slot(index) for index in range(1, count + 1)]
    probe.seed(
        player,
        "INSERT INTO dongfu_status(user_id,built,node_name,node_type,plot_count,plant_slots,"
        "planting,plant_seed_id,plant_start,plant_finish) VALUES('900001',1,'诊断洞府','水域',?,?,0,0,'','') "
        "ON CONFLICT(user_id) DO UPDATE SET built=1,node_name='诊断洞府',node_type='水域',"
        "plot_count=excluded.plot_count,plant_slots=excluded.plant_slots,planting=0,"
        "plant_seed_id=0,plant_start='',plant_finish=''",
        (count, canonical_plant_slots(slots)),
    )
    probe.player(stone=stone)
    probe.seed(game, "DELETE FROM back WHERE user_id='900001' AND goods_id=?", (module.DONGFU_ITEM_DEED,))
    if deeds:
        probe.grant_item(module.DONGFU_ITEM_DEED, quantity=deeds)
        probe.seed(game, "UPDATE back SET bind_num=? WHERE user_id='900001' AND goods_id=?",
                   (bound, module.DONGFU_ITEM_DEED))
    module.configure_dongfu_application(DongfuApplication(game, player))
    return module


def _business(probe):
    game, player = probe.context.paths.game_db, probe.context.paths.player_db
    return {
        "wallet": probe.rows(game, "SELECT stone FROM user_xiuxian WHERE user_id='900001'"),
        "deed": probe.rows(game, "SELECT goods_num,bind_num FROM back WHERE user_id='900001' AND goods_id=21008"),
        "cave": probe.rows(player, "SELECT plot_count,plant_slots,planting,plant_seed_id,plant_start,plant_finish "
                                    "FROM dongfu_status WHERE user_id='900001'"),
        "receipt": probe.rows(game, "SELECT operation_id,user_id,previous_count,current_count,deed_cost,stone_cost "
                                    "FROM dongfu_expansion_operations ORDER BY operation_id"),
    }


async def _command(probe, text, *, number, actor="900001", expect_abort=False):
    reply = await probe.command("洞府扩建", text, number=number, actor=actor, allow_exception=True)
    call = probe.calls[-1]
    traces = "\n".join(call.get("errors", []))
    if expect_abort:
        assert "sqlite3.IntegrityError: expansion refused" in traces and "Traceback" in traces, call
        assert "定位号" in reply and "expansion refused" not in reply, reply
    return reply


def register(case):
    @case("dongfu-expansion-quantity-replay", "洞府扩建", "默认一级与显式扩建数量、同消息重放/改参",
          "隔离双库洞府3块田、灵石充足、地契充足", "数量按逐级费用一次事务扩建，回执重放不重复扣费且参数冲突明确拒绝",
          group="dongfu-expansion")
    async def quantity_replay(probe):
        module = _seed(probe)
        game, player = probe.context.paths.game_db, probe.context.paths.player_db
        before = _business(probe)
        reply = await _command(probe, "洞府扩建 2", number=972601)
        after = _business(probe)
        probe.evidence.update(before=before, after=after, reply=reply)
        assert "扩建成功" in reply and "5块" in reply, reply
        assert after["wallet"] == [(140_000_000,)], after
        assert after["deed"] == [(7, 0)], after
        assert json.loads(after["cave"][0][1])[-1]["slot"] == 5, after
        assert after["receipt"] == [("dongfu:972601:expand:900001", "900001", 3, 5, 3, 60_000_000)], after
        committed = _database_projection(probe)
        replay = await _command(probe, "洞府扩建 2", number=972601)
        assert "已经处理" in replay, replay
        assert _database_projection(probe) == committed
        conflict = await _command(probe, "洞府扩建 1", number=972601)
        assert "操作号" in conflict and "不同内容" in conflict, conflict
        assert _database_projection(probe) == committed

    @case("dongfu-expansion-invalid-quantity", "洞府扩建", "零/负数/非数字/多余参数/超长参数",
          "3块田且地契灵石充足", "无效参数明确提示用法且两库零写", group="dongfu-expansion", outcome="rejection")
    async def invalid_quantity(probe):
        _seed(probe)
        before = _database_projection(probe)
        for index, text in enumerate(("洞府扩建 0", "洞府扩建 -1", "洞府扩建 abc",
                                      "洞府扩建 1 2", "洞府扩建 " + "9" * 5000)):
            reply = await _command(probe, text, number=972602 + index)
            probe.evidence[text] = reply
            assert "请使用" in reply, reply
            assert _database_projection(probe) == before

    @case("dongfu-expansion-resource-boundaries", "洞府扩建", "灵石/地契不足与数量超过余量",
          "隔离库分别只有不足资源或仅剩一块可扩", "准确拒绝且钱包/地契/洞府/回执零写",
          group="dongfu-expansion", outcome="rejection")
    async def resource_boundaries(probe):
        _seed(probe, stone=59_999_999)
        before = _business(probe)
        reply = await _command(probe, "洞府扩建 2", number=972608)
        assert "需要" in reply and "灵石" in reply, reply
        assert _business(probe) == before

        probe.player(stone=200_000_000)
        probe.seed(probe.context.paths.game_db, "UPDATE back SET goods_num=2,bind_num=0 WHERE user_id='900001' AND goods_id=21008")
        before = _business(probe)
        reply = await _command(probe, "洞府扩建 2", number=972609)
        assert "洞府地契" in reply and "x3" in reply, reply
        assert _business(probe) == before

        _seed(probe, count=5, stone=200_000_000, deeds=10)
        before = _business(probe)
        reply = await _command(probe, "洞府扩建 2", number=972611)
        assert "上限" in reply and "6块" in reply, reply
        assert _business(probe) == before

    @case("dongfu-expansion-bound-deed", "洞府扩建", "消耗带绑定数的最后一份洞府地契",
          "洞府3块田，地契总数2且绑定数1", "按绑定优先扣1份，数量与绑定数同步且实际回执存在",
          group="dongfu-expansion")
    async def bound_deed(probe):
        _seed(probe, deeds=2, bound=1)
        reply = await _command(probe, "洞府扩建", number=972603)
        after = _business(probe)
        assert "4块" in reply, reply
        assert after["deed"] == [(1, 0)], after
        assert after["receipt"] == [("dongfu:972603:expand:900001", "900001", 3, 4, 1, 20_000_000)], after

    @case("dongfu-expansion-max-replay", "洞府扩建", "已成功扩建到上限后的原消息重放",
          "当前6块田且已有该event的5到6回执", "命中原回执并回复已处理，不被满级前置截断", group="dongfu-expansion")
    async def max_replay(probe):
        _seed(probe, count=6, stone=140_000_000, deeds=7)
        game, player = probe.context.paths.game_db, probe.context.paths.player_db
        probe.seed(game, "INSERT INTO dongfu_expansion_operations(operation_id,user_id,previous_count,current_count,"
                   "deed_cost,stone_cost) VALUES(?,?,?,?,?,?)",
                   ("dongfu:972604:expand:900001", "900001", 5, 6, 3, 60_000_000))
        before = _database_projection(probe)
        reply = await _command(probe, "洞府扩建", number=972604)
        assert "已经处理" in reply, reply
        assert _database_projection(probe) == before

    @case("dongfu-expansion-invalid-receipt", "洞府扩建", "历史回执字段损坏后重放",
          "隔离库中同消息号存在非整数灵田数回执", "明确拒绝损坏回执且钱包/地契/洞府/回执零写",
          group="dongfu-expansion", outcome="rejection")
    async def invalid_receipt(probe):
        from nonebot_plugin_xiuxian_2.core.player_reasons import reason_text

        _seed(probe)
        game = probe.context.paths.game_db
        probe.seed(game, "INSERT INTO dongfu_expansion_operations(operation_id,user_id,previous_count,"
                   "current_count,deed_cost,stone_cost) VALUES(?,?,?,?,?,?)",
                   ("dongfu:972630:expand:900001", "900001", 3, "damaged", 1, 20_000_000))
        before = _database_projection(probe)
        reply = await _command(probe, "洞府扩建", number=972630)
        assert reason_text("dongfu_expansion_receipt_invalid") in reply and "扩建回执" in reply, reply
        assert _database_projection(probe) == before

    @case("dongfu-expansion-player-isolation", "洞府扩建", "私聊第二玩家显式扩建1块",
          "两个玩家分别有3块田、资源与洞府状态", "仅发起玩家的钱包/地契/田数/回执变化",
          group="dongfu-expansion")
    async def player_isolation(probe):
        module = _seed(probe)
        game, player = probe.context.paths.game_db, probe.context.paths.player_db
        from nonebot_plugin_xiuxian_2.features.dongfu.plant_slots import canonical_plant_slots, empty_plant_slot

        slots = canonical_plant_slots([empty_plant_slot(index) for index in range(1, 4)])
        probe.seed(player, "INSERT INTO dongfu_status(user_id,built,node_name,node_type,plot_count,plant_slots) "
                   "VALUES('900002',1,'第二洞府','水域',3,?) ON CONFLICT(user_id) DO UPDATE SET "
                   "built=1,node_name='第二洞府',node_type='水域',plot_count=3,plant_slots=excluded.plant_slots",
                   (slots,))
        probe.player(uid="900002", stone=100_000_000)
        probe.grant_item(module.DONGFU_ITEM_DEED, quantity=4, uid="900002")
        before = {
            "wallets": probe.rows(game, "SELECT user_id,stone FROM user_xiuxian WHERE user_id IN ('900001','900002') ORDER BY user_id"),
            "deeds": probe.rows(game, "SELECT user_id,goods_num,bind_num FROM back WHERE goods_id=21008 ORDER BY user_id"),
        }
        reply = await _command(probe, "洞府扩建 1", number=972605, actor="900002")
        assert "从3块提升至4块" in reply, reply
        assert probe.rows(game, "SELECT user_id,stone FROM user_xiuxian WHERE user_id IN ('900001','900002') ORDER BY user_id") == [
            ("900001", before["wallets"][0][1]), ("900002", before["wallets"][1][1] - 20_000_000)
        ]
        assert probe.rows(game, "SELECT operation_id,user_id,previous_count,current_count,deed_cost,stone_cost "
                               "FROM dongfu_expansion_operations") == [
            ("dongfu:972605:expand:900002", "900002", 3, 4, 1, 20_000_000)
        ]
        assert probe.rows(player, "SELECT user_id,plot_count FROM dongfu_status WHERE user_id IN ('900001','900002') ORDER BY user_id") == [
            ("900001", 3), ("900002", 4)
        ]
        _database_projection(probe)

    write_faults = (
        ("deed-ignore", "game", "CREATE TRIGGER x BEFORE UPDATE ON back WHEN NEW.user_id='900001' AND NEW.goods_id=21008 BEGIN SELECT RAISE(IGNORE); END",
         "dongfu_expansion_deed_write_rejected", "地契"),
        ("deed-rewrite", "game", "CREATE TRIGGER x AFTER UPDATE ON back WHEN NEW.user_id='900001' AND NEW.goods_id=21008 BEGIN UPDATE back SET goods_num=77 WHERE user_id='900001' AND goods_id=21008; END",
         "dongfu_expansion_deed_projection_mismatch", "地契"),
        ("stone-ignore", "game", "CREATE TRIGGER x BEFORE UPDATE ON user_xiuxian WHEN NEW.user_id='900001' BEGIN SELECT RAISE(IGNORE); END",
         "dongfu_expansion_stone_write_rejected", "灵石钱包"),
        ("stone-rewrite", "game", "CREATE TRIGGER x AFTER UPDATE ON user_xiuxian WHEN NEW.user_id='900001' BEGIN UPDATE user_xiuxian SET stone=1 WHERE user_id='900001'; END",
         "dongfu_expansion_stone_projection_mismatch", "灵石钱包"),
        ("cave-ignore", "player", "CREATE TRIGGER x BEFORE UPDATE ON dongfu_status WHEN NEW.user_id='900001' BEGIN SELECT RAISE(IGNORE); END",
         "dongfu_expansion_state_write_rejected", "洞府状态"),
        ("cave-rewrite", "player", "CREATE TRIGGER x AFTER UPDATE ON dongfu_status WHEN NEW.user_id='900001' BEGIN UPDATE dongfu_status SET plot_count=6 WHERE user_id='900001'; END",
         "dongfu_expansion_state_projection_mismatch", "洞府状态"),
        ("receipt-ignore", "game", "CREATE TRIGGER x BEFORE INSERT ON dongfu_expansion_operations BEGIN SELECT RAISE(IGNORE); END",
         "dongfu_expansion_receipt_write_rejected", "扩建回执"),
        ("receipt-rewrite", "game", "CREATE TRIGGER x AFTER INSERT ON dongfu_expansion_operations BEGIN UPDATE dongfu_expansion_operations SET current_count=99 WHERE operation_id=NEW.operation_id; END",
         "dongfu_expansion_receipt_projection_mismatch", "扩建回执"),
    )
    for label, database, trigger_sql, reason, subject_label in write_faults:
        @case("dongfu-expansion-write-" + label, "洞府扩建", "写路径" + label,
              "真实命令入口隔离注入具体SQLite写故障", "具体reason/subject回复、game/player整笔回滚且原消息可恢复",
              group="dongfu-expansion", outcome="recovery")
        async def write_fault(probe, database=database, trigger_sql=trigger_sql, reason=reason,
                              subject_label=subject_label, label=label):
            from nonebot_plugin_xiuxian_2.core.player_reasons import reason_text

            module = _seed(probe)
            game, player = probe.context.paths.game_db, probe.context.paths.player_db
            target = game if database == "game" else player
            probe.seed(target, trigger_sql)
            before = _business(probe)
            reply = await _command(probe, "洞府扩建 2", number=972610)
            after = _business(probe)
            probe.evidence[label] = {"before": before, "after": after, "reply": reply}
            assert reason_text(reason) in reply and subject_label in reply, reply
            assert probe.calls[-1]["errors"] == [], probe.calls[-1]
            assert after == before, (label, before, after)
            probe.seed(target, "DROP TRIGGER x")
            recovered = await _command(probe, "洞府扩建 2", number=972610)
            assert "5块" in recovered, recovered
            assert _business(probe)["receipt"] == [
                ("dongfu:972610:expand:900001", "900001", 3, 5, 3, 60_000_000)
            ]
            _database_projection(probe)

    for label, database, table, trigger_body in (
        ("deed", "game", "back", "BEFORE UPDATE ON back WHEN NEW.user_id='900001' AND NEW.goods_id=21008"),
        ("stone", "game", "user_xiuxian", "BEFORE UPDATE ON user_xiuxian WHEN NEW.user_id='900001'"),
        ("cave", "player", "dongfu_status", "BEFORE UPDATE ON dongfu_status WHEN NEW.user_id='900001'"),
        ("receipt", "game", "dongfu_expansion_operations", "BEFORE INSERT ON dongfu_expansion_operations"),
    ):
        @case("dongfu-expansion-abort-" + label, "洞府扩建", "SQLite ABORT：" + label,
              "真实扩建写点抛出IntegrityError", "原traceback/定位号保留，跨库写全部回滚",
              group="dongfu-expansion", outcome="recovery")
        async def abort_write(probe, database=database, table=table, trigger_body=trigger_body):
            _seed(probe)
            game, player = probe.context.paths.game_db, probe.context.paths.player_db
            target = game if database == "game" else player
            probe.seed(target, f"CREATE TRIGGER x {trigger_body} BEGIN SELECT RAISE(ABORT,'expansion refused'); END")
            before = _business(probe)
            await _command(probe, "洞府扩建 2", number=972620, expect_abort=True)
            assert _business(probe) == before, (table, before, _business(probe))
            probe.seed(target, "DROP TRIGGER x")
            reply = await _command(probe, "洞府扩建 2", number=972620)
            assert "5块" in reply, reply
            _database_projection(probe)
