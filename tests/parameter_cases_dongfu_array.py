"""Bounded real-command coverage for cave array upgrades."""

from tests.parameter_cases_dongfu_harvest import _database_projection


def _seed(probe, *, level=4, node_type="矿脉", stone=100_000_000, item_count=4, bound_count=4):
    from nonebot_plugin_xiuxian_2.features.dongfu.application import DongfuApplication
    from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_dongfu as module

    game, player = probe.context.paths.game_db, probe.context.paths.player_db
    probe.seed(
        player,
        "INSERT INTO dongfu_status(user_id,built,node_type,array_level) "
        "VALUES('900001',1,?,?) ON CONFLICT(user_id) DO UPDATE SET "
        "built=1,node_type=excluded.node_type,array_level=excluded.array_level",
        (node_type, level),
    )
    probe.player(stone=stone)
    probe.seed(game, "DELETE FROM back WHERE user_id='900001' AND goods_id=?",
               (module.DONGFU_ITEM_ARRAY_STONE,))
    if item_count:
        probe.grant_item(module.DONGFU_ITEM_ARRAY_STONE, quantity=item_count)
        probe.seed(
            game,
            "UPDATE back SET bind_num=? WHERE user_id='900001' AND goods_id=?",
            (bound_count, module.DONGFU_ITEM_ARRAY_STONE),
        )
    module.configure_dongfu_application(DongfuApplication(game, player))
    return module


def _business(probe):
    game, player = probe.context.paths.game_db, probe.context.paths.player_db
    return {
        "wallet": probe.rows(game, "SELECT stone FROM user_xiuxian WHERE user_id='900001'"),
        "inventory": probe.rows(game, "SELECT * FROM back WHERE user_id='900001' ORDER BY goods_id"),
        "cave": probe.rows(player, "SELECT * FROM dongfu_status ORDER BY user_id"),
        "receipt": probe.rows(game, "SELECT * FROM dongfu_array_upgrade_operations ORDER BY operation_id"),
    }


async def _command(probe, *, number, actor="900001", command="洞府布阵", event_type="group",
                   expect_abort=False):
    reply = await probe.command(command, command, actor=actor, number=number,
                                event_type=event_type, allow_exception=True)
    errors = "\n".join(probe.calls[-1]["errors"])
    if expect_abort:
        assert "sqlite3.IntegrityError: array upgrade refused" in errors and "Traceback" in errors, probe.calls[-1]
        assert "定位号" in reply and "array upgrade refused" not in reply, reply
    else:
        assert probe.calls[-1]["errors"] == [], probe.calls[-1]
    return reply


def register(case):
    @case(
        "dongfu-array-valid-bound-upgrade",
        "洞府布阵",
        "无参数阵法升级",
        "真实注册玩家，矿脉4级阵法，100000000灵石及4份全绑定玄铁阵石",
        "按矿脉折扣升级至5级并实际扣除13500000灵石、1份阵石，绑定数随总数保持一致",
        group="dongfu-array",
    )
    async def valid_bound_upgrade(probe):
        module = _seed(probe)
        game, player = probe.context.paths.game_db, probe.context.paths.player_db
        before = _business(probe)
        assert probe.rows(game, "SELECT goods_num,bind_num FROM back WHERE user_id='900001' AND goods_id=21007") == [
            (4, 4)
        ]

        reply = await probe.command("洞府布阵", "洞府布阵", number=972001)

        after = _business(probe)
        probe.evidence.update(before=before, after=after, reply=reply)
        assert "洞府布阵成功" in reply and "当前阵法等级：5" in reply, reply
        assert probe.rows(game, "SELECT stone FROM user_xiuxian WHERE user_id='900001'") == [
            (86_500_000,)
        ]
        assert probe.rows(game, "SELECT goods_num,bind_num FROM back WHERE user_id='900001' AND goods_id=21007") == [
            (3, 3)
        ]
        assert probe.rows(player, "SELECT array_level FROM dongfu_status WHERE user_id='900001'") == [(5,)]
        assert probe.rows(
            game,
            "SELECT payload,level FROM dongfu_array_upgrade_operations "
            "WHERE operation_id='dongfu-array:900001:972001'",
        ) == [("900001|4|5|13500000|21007|1", 5)]
        committed = _database_projection(probe)
        replay = await _command(probe, number=972001)
        probe.evidence["replay"] = replay
        assert "已经处理" in replay, replay
        assert _database_projection(probe) == committed

    faults = (
        ("player-ignore", "game", "BEFORE UPDATE ON user_xiuxian WHEN NEW.user_id='900001' BEGIN SELECT RAISE(IGNORE); END",
         "dongfu_array_player_write_rejected", "stone_wallet"),
        ("player-rewrite", "game", "AFTER UPDATE ON user_xiuxian WHEN NEW.user_id='900001' BEGIN UPDATE user_xiuxian SET stone=1 WHERE user_id='900001'; END",
         "dongfu_array_player_projection_mismatch", "stone_wallet"),
        ("inventory-ignore", "game", "BEFORE UPDATE ON back WHEN NEW.user_id='900001' BEGIN SELECT RAISE(IGNORE); END",
         "dongfu_array_inventory_write_rejected", "item:21007"),
        ("inventory-rewrite", "game", "AFTER UPDATE ON back WHEN NEW.user_id='900001' BEGIN UPDATE back SET goods_num=99 WHERE user_id='900001' AND goods_id=21007; END",
         "dongfu_array_inventory_projection_mismatch", "item:21007"),
        ("state-ignore", "player", "BEFORE UPDATE ON dongfu_status WHEN NEW.user_id='900001' BEGIN SELECT RAISE(IGNORE); END",
         "dongfu_array_state_write_rejected", "array"),
        ("state-rewrite", "player", "AFTER UPDATE ON dongfu_status WHEN NEW.user_id='900001' BEGIN UPDATE dongfu_status SET array_level=9 WHERE user_id='900001'; END",
         "dongfu_array_state_projection_mismatch", "array"),
        ("receipt-ignore", "game", "BEFORE INSERT ON dongfu_array_upgrade_operations BEGIN SELECT RAISE(IGNORE); END",
         "dongfu_array_receipt_write_rejected", "receipt"),
        ("receipt-rewrite", "game", "AFTER INSERT ON dongfu_array_upgrade_operations BEGIN UPDATE dongfu_array_upgrade_operations SET payload='changed' WHERE operation_id=NEW.operation_id; END",
         "dongfu_array_receipt_projection_mismatch", "receipt"),
    )
    for index, (label, database, trigger, reason, subject) in enumerate(faults):
        def build(label=label, database=database, trigger=trigger, reason=reason, subject=subject, index=index):
            async def recovery(probe):
                from nonebot_plugin_xiuxian_2.core.player_reasons import reason_text

                module = _seed(probe)
                subject_label = {
                    "stone_wallet": "灵石钱包",
                    "array": "洞府阵法",
                    "receipt": "操作回执",
                }.get(subject, "玄铁阵石")
                game, player = probe.context.paths.game_db, probe.context.paths.player_db
                target = game if database == "game" else player
                before = _business(probe)
                probe.seed(target, "CREATE TRIGGER array_fault " + trigger)
                reply = await _command(probe, number=972100 + index)
                probe.evidence.update(fault=label, reply=reply, before=before, after=_business(probe))
                assert _business(probe) == before, (label, before, _business(probe), reply)
                assert reason_text(reason) in reply and subject_label in reply, (
                    label, reason, subject_label, reply
                )
                if subject.startswith("item:"):
                    assert "21007" in reply, reply
                assert "洞府布阵成功" not in reply, reply
                probe.seed(target, "DROP TRIGGER array_fault")
                success = await _command(probe, number=972100 + index)
                assert "洞府布阵成功" in success and "当前阵法等级：5" in success, success
                committed = _database_projection(probe)
                replay = await _command(probe, number=972100 + index)
                assert "已经处理" in replay, replay
                assert _database_projection(probe) == committed
            return recovery

        case("dongfu-array-recovery-" + label, "洞府布阵", "真实阵法升级遇" + label,
             "隔离game/player触发器，固定矿脉4级阵法与真实无参数matcher",
             "具体reason/subject、双库整笔回滚、同消息恢复及重放零重复扣费",
             group="dongfu-array", outcome="recovery")(
                 build())

    @case("dongfu-array-abort-recovery", "洞府布阵", "阵法升级各写点SQLite ABORT",
          "隔离双库依次拒绝灵石、阵石、阵法、回执写入",
          "定位号和原始traceback、整笔回滚、解除故障后同消息恢复",
          group="dongfu-array", outcome="recovery")
    async def abort_recovery(probe):
        game, player = probe.context.paths.game_db, probe.context.paths.player_db
        faults = (
            (game, "BEFORE UPDATE ON user_xiuxian WHEN NEW.user_id='900001'"),
            (game, "BEFORE UPDATE ON back WHEN NEW.user_id='900001'"),
            (player, "BEFORE UPDATE ON dongfu_status WHEN NEW.user_id='900001'"),
            (game, "BEFORE INSERT ON dongfu_array_upgrade_operations"),
        )
        for index, (target, event) in enumerate(faults):
            _seed(probe)
            before = _business(probe)
            probe.seed(target, "CREATE TRIGGER array_fault " + event +
                       " BEGIN SELECT RAISE(ABORT,'array upgrade refused'); END")
            reply = await _command(probe, number=972130 + index, expect_abort=True)
            assert _business(probe) == before, (event, reply, before, _business(probe))
            probe.seed(target, "DROP TRIGGER array_fault")
            success = await _command(probe, number=972130 + index)
            assert "洞府布阵成功" in success, success

    @case("dongfu-array-cost-and-rejections", "洞府布阵", "等级/地脉费用、灵石和阵石不足、满级",
          "使用隔离玩家依次设置阵法等级/地脉和准确资源余额",
          "实际费用与阵石数量遵循既有公式；不足/满级拒绝全库零写",
          group="dongfu-array")
    async def costs_and_rejections(probe):
        game, player = probe.context.paths.game_db, probe.context.paths.player_db

        _seed(probe, level=3, node_type="矿脉", item_count=0, bound_count=0)
        before_stone = probe.rows(game, "SELECT stone FROM user_xiuxian WHERE user_id='900001'")[0][0]
        reply = await _command(probe, number=972140)
        assert "洞府布阵成功" in reply, reply
        assert probe.rows(game, "SELECT stone FROM user_xiuxian WHERE user_id='900001'") == [
            (before_stone - 10_800_000,)
        ]
        assert probe.rows(player, "SELECT array_level FROM dongfu_status WHERE user_id='900001'") == [(4,)]
        assert probe.rows(game, "SELECT COUNT(*) FROM dongfu_array_upgrade_operations WHERE operation_id='dongfu-array:900001:972140'") == [(1,)]

        _seed(probe, level=4, node_type="水域", item_count=2, bound_count=2)
        before = _business(probe)
        reply = await _command(probe, number=972141)
        assert "洞府布阵成功" in reply, reply
        assert probe.rows(game, "SELECT stone FROM user_xiuxian WHERE user_id='900001'") == [(85_000_000,)]
        assert probe.rows(game, "SELECT goods_num,bind_num FROM back WHERE user_id='900001' AND goods_id=21007") == [(0, 0)]
        assert probe.rows(player, "SELECT array_level FROM dongfu_status WHERE user_id='900001'") == [(5,)]

        _seed(probe, level=4, node_type="矿脉", stone=1_000_000)
        before = _business(probe)
        insufficient_stone = await _command(probe, number=972142)
        assert "需要" in insufficient_stone and _business(probe) == before, insufficient_stone

        _seed(probe, level=4, node_type="矿脉", item_count=0, bound_count=0)
        before = _business(probe)
        insufficient_item = await _command(probe, number=972143)
        assert "玄铁阵石" in insufficient_item and _business(probe) == before, insufficient_item

        _seed(probe, level=10)
        before = _business(probe)
        maximum = await _command(probe, number=972144)
        assert "阵法已达满级" in maximum and _business(probe) == before, maximum

        _seed(probe, level=9, node_type="水域", item_count=7, bound_count=7)
        reply = await _command(probe, number=972145)
        assert "当前阵法等级：10" in reply and "玄铁阵石x7" in reply, reply
        committed = _database_projection(probe)
        replay = await _command(probe, number=972145)
        assert "已经处理" in replay and "当前阵法等级：10" in replay, replay
        assert _database_projection(probe) == committed

    @case("dongfu-array-invalid-receipt", "洞府布阵", "历史升级回执payload损坏",
          "真实操作号存在但payload与level不一致", "返回具体回执原因且不覆盖或追加业务写入",
          group="dongfu-array", outcome="rejection")
    async def invalid_receipt(probe):
        from nonebot_plugin_xiuxian_2.core.player_reasons import reason_text

        _seed(probe)
        game = probe.context.paths.game_db
        operation_id = "dongfu-array:900001:972160"
        probe.seed(
            game,
            "INSERT INTO dongfu_array_upgrade_operations(operation_id,payload,level) VALUES(?,?,?)",
            (operation_id, "corrupt", 5),
        )
        before = _business(probe)
        reply = await _command(probe, number=972160)
        assert reason_text("dongfu_array_receipt_invalid") in reply and "操作回执" in reply, reply
        assert _business(probe) == before
        _database_projection(probe)

    @case("dongfu-array-private-player-isolation", "洞府布阵", "私聊第二玩家升级",
          "两个注册玩家均有矿脉4级阵法、灵石和阵石",
          "只更新发起玩家的灵石、绑定材料、阵法等级与回执",
          group="dongfu-array")
    async def private_player_isolation(probe):
        game, player = probe.context.paths.game_db, probe.context.paths.player_db
        _seed(probe)
        probe.seed(player,
                   "INSERT INTO dongfu_status(user_id,built,node_type,array_level) VALUES('900002',1,'矿脉',4) "
                   "ON CONFLICT(user_id) DO UPDATE SET built=1,node_type='矿脉',array_level=4")
        probe.player(uid="900002", stone=100_000_000)
        probe.grant_item(21007, quantity=4, uid="900002")
        probe.seed(game, "UPDATE back SET bind_num=4 WHERE user_id='900002' AND goods_id=21007")
        before = probe.rows(game, "SELECT user_id,stone FROM user_xiuxian WHERE user_id IN ('900001','900002') ORDER BY user_id")
        reply = await _command(probe, number=972150, actor="900002", event_type="private")
        assert "洞府布阵成功" in reply and "当前阵法等级：5" in reply, reply
        after = probe.rows(game, "SELECT user_id,stone FROM user_xiuxian WHERE user_id IN ('900001','900002') ORDER BY user_id")
        assert after == [("900001", before[0][1]), ("900002", before[1][1] - 13_500_000)], (before, after)
        assert probe.rows(game, "SELECT user_id,goods_num,bind_num FROM back WHERE goods_id=21007 ORDER BY user_id") == [
            ("900001", 4, 4), ("900002", 3, 3)
        ]
        assert probe.rows(player, "SELECT user_id,array_level FROM dongfu_status ORDER BY user_id") == [
            ("900001", 4), ("900002", 5)
        ]
        assert probe.rows(game, "SELECT operation_id FROM dongfu_array_upgrade_operations") == [
            ("dongfu-array:900002:972150",)
        ]
        _database_projection(probe)
