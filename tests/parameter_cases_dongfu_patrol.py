"""Bounded real-command reproduction for cave patrol settlement."""

from datetime import datetime, timezone
from unittest.mock import patch

from tests.parameter_cases_dongfu_harvest import _database_projection


def _seed(probe):
    from nonebot_plugin_xiuxian_2.features.dongfu.application import DongfuApplication
    from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_dongfu as module

    game, player = probe.context.paths.game_db, probe.context.paths.player_db
    probe.seed(player, "INSERT INTO dongfu_status(user_id,built,node_type,patrol_date,patrol_count,patrol_guard) "
               "VALUES('900001',1,'水域','2026-11-22',0,0) ON CONFLICT(user_id) DO UPDATE SET "
               "built=1,node_type='水域',patrol_date='2026-11-22',patrol_count=0,patrol_guard=0")
    module.configure_dongfu_application(DongfuApplication(game, player))
    return module


def _business(probe):
    game, player = probe.context.paths.game_db, probe.context.paths.player_db
    return {
        "players": probe.rows(game, "SELECT * FROM user_xiuxian ORDER BY user_id"),
        "inventory": probe.rows(game, "SELECT * FROM back ORDER BY user_id,goods_id"),
        "cave": probe.rows(player, "SELECT * FROM dongfu_status ORDER BY user_id"),
        "receipt": probe.rows(game, "SELECT * FROM dongfu_patrol_operations ORDER BY operation_id"),
    }


async def _command(probe, *, number, now=None, text="洞府巡山", command="洞府巡山", abort=False,
                   reward_roll=0, stone_roll=100000, **kwargs):
    from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_dongfu as module

    with (patch.object(module.runtime_clock, "now", return_value=now or datetime(2026, 11, 22, 12, tzinfo=timezone.utc)),
          patch.object(module.runtime_random, "random", return_value=reward_roll),
          patch.object(module.runtime_random, "randint", return_value=stone_roll)):
        reply = await probe.command(command, text, number=number, allow_exception=True, **kwargs)
    if abort:
        errors = "\n".join(probe.calls[-1]["errors"])
        assert "sqlite3.IntegrityError: patrol refused" in errors and "Traceback" in errors, probe.calls[-1]
    else:
        assert probe.calls[-1]["errors"] == [], probe.calls[-1]
    return reply


def register(case):
    @case("dongfu-patrol-normal-settlement", "洞府巡山", "合法无参数巡山，固定掉落灵息露",
          "水域/体力100/灵石1000000/当日0次，固定随机100000与掉落命中",
          "扣8体力、地脉灵石120000、绑定材料1、次数护府各1和永久回执", group="dongfu-patrol")
    async def normal(probe):
        module = _seed(probe)
        game, player = probe.context.paths.game_db, probe.context.paths.player_db
        before = probe.rows(game, "SELECT user_stamina,stone FROM user_xiuxian WHERE user_id='900001'")[0]
        reply = await _command(probe, number=970801)
        assert "洞府巡山完成" in reply and "灵息露x1" in reply and "今日巡山：1/3" in reply, reply
        after = probe.rows(game, "SELECT user_stamina,stone FROM user_xiuxian WHERE user_id='900001'")[0]
        assert (int(after[0]), int(after[1])) == (int(before[0]) - 8, int(before[1]) + 120000)
        assert probe.rows(game, "SELECT goods_num,bind_num FROM back WHERE user_id='900001' AND goods_id=21005") == [(1, 1)]
        assert probe.rows(player, "SELECT patrol_date,patrol_count,patrol_guard FROM dongfu_status WHERE user_id='900001'") == [("2026-11-22", 1, 1)]
        assert probe.rows(game, "SELECT patrol_count,patrol_guard FROM dongfu_patrol_operations") == [(1, 1)]
        committed = _database_projection(probe)
        assert "已经处理" in await _command(probe, number=970801)
        assert _database_projection(probe) == committed

    @case("dongfu-patrol-cross-day-replay", "洞府巡山", "同一无参数巡山消息跨日重放",
          "23:59UTC成功，00:00UTC重放原消息", "按成功回执重放原日结算，不因时钟变化误报改参且零写",
          group="dongfu-patrol")
    async def cross_day_replay(probe):
        module = _seed(probe)
        success = await _command(probe, number=970803, now=datetime(2026, 11, 22, 23, 59, tzinfo=timezone.utc))
        assert "今日巡山：1/3" in success, success
        committed = _database_projection(probe)
        reply = await _command(probe, number=970803, now=datetime(2026, 11, 23, tzinfo=timezone.utc))
        probe.evidence.update(first=success, replay=reply)
        assert "已经处理" in reply, reply
        assert _database_projection(probe) == committed

    faults = (
        ("player-ignore", "game", "BEFORE UPDATE ON user_xiuxian BEGIN SELECT RAISE(IGNORE); END", "player_write_rejected", "玩家体力与灵石", False),
        ("player-rewrite", "game", "AFTER UPDATE ON user_xiuxian BEGIN UPDATE user_xiuxian SET user_stamina=99; END", "player_projection_mismatch", "玩家体力与灵石", False),
        ("state-ignore", "player", "BEFORE UPDATE ON dongfu_status BEGIN SELECT RAISE(IGNORE); END", "state_write_rejected", "巡山记录", False),
        ("state-rewrite", "player", "AFTER UPDATE ON dongfu_status BEGIN UPDATE dongfu_status SET patrol_guard=0; END", "state_projection_mismatch", "巡山记录", False),
        ("inventory-insert-ignore", "game", "BEFORE INSERT ON back BEGIN SELECT RAISE(IGNORE); END", "inventory_write_rejected", "灵息露", False),
        ("inventory-update-ignore", "game", "BEFORE UPDATE ON back BEGIN SELECT RAISE(IGNORE); END", "inventory_write_rejected", "灵息露", True),
        ("inventory-rewrite", "game", "AFTER UPDATE ON back BEGIN UPDATE back SET bind_num=99; END", "inventory_projection_mismatch", "灵息露", True),
        ("receipt-ignore", "game", "BEFORE INSERT ON dongfu_patrol_operations BEGIN SELECT RAISE(IGNORE); END", "receipt_write_rejected", "巡山回执", False),
        ("receipt-rewrite", "game", "AFTER INSERT ON dongfu_patrol_operations BEGIN UPDATE dongfu_patrol_operations SET payload='changed'; END", "receipt_projection_mismatch", "巡山回执", False),
    )
    for index, (label, database, trigger, suffix, subject_label, existing_item) in enumerate(faults):
        def build(database, trigger, suffix, subject_label, existing_item, index):
            async def recovery(probe):
                from nonebot_plugin_xiuxian_2.core.player_reasons import reason_text

                module = _seed(probe)
                game, player = probe.context.paths.game_db, probe.context.paths.player_db
                if existing_item:
                    probe.grant_item(21005, quantity=1)
                elif label == "inventory-insert-ignore":
                    pass
                target = game if database == "game" else player
                before = _business(probe)
                if label == "inventory-insert-ignore":
                    probe.seed(game, "CREATE TRIGGER patrol_fault " + trigger)
                    operation = 970810 + index
                    reply = await _command(probe, number=operation)
                    assert _business(probe) == before
                    assert reason_text("dongfu_patrol_" + suffix) in reply and subject_label in reply, reply
                    probe.seed(game, "DROP TRIGGER patrol_fault")
                    success = await _command(probe, number=operation)
                    assert "洞府巡山完成" in success
                    committed = _database_projection(probe)
                    assert "已经处理" in await _command(probe, number=operation)
                    assert _database_projection(probe) == committed
                    return
                probe.seed(target, "CREATE TRIGGER patrol_fault " + trigger)
                reply = await _command(probe, number=970810 + index, abort=False)
                assert _business(probe) == before, (reply, before, _business(probe))
                assert reason_text("dongfu_patrol_" + suffix) in reply and subject_label in reply, reply
                assert "洞府巡山完成" not in reply, reply
                probe.seed(target, "DROP TRIGGER patrol_fault")
                success = await _command(probe, number=970810 + index)
                assert "洞府巡山完成" in success, success
                committed = _database_projection(probe)
                assert "已经处理" in await _command(probe, number=970810 + index)
                assert _database_projection(probe) == committed
            return recovery
        case("dongfu-patrol-recovery-" + label, "洞府巡山", "合法水域巡山遇" + label,
             "隔离双库写故障及固定时钟/随机", "具体reason/对象、整笔回滚、原消息恢复/replay",
             group="dongfu-patrol", outcome="recovery")(
                 build(database, trigger, suffix, subject_label, existing_item, index))

    @case("dongfu-patrol-abort-recovery", "洞府巡山", "玩家/巡山状态/材料/回执SQLite ABORT",
          "每次独立隔离触发器及原始sqlite3.IntegrityError", "定位号和完整traceback、整笔回滚、原消息恢复",
          group="dongfu-patrol", outcome="recovery")
    async def abort_recovery(probe):
        module = _seed(probe)
        game, player = probe.context.paths.game_db, probe.context.paths.player_db
        faults = (
            (game, "BEFORE UPDATE ON user_xiuxian"), (player, "BEFORE UPDATE ON dongfu_status"),
            (game, "BEFORE INSERT ON back"), (game, "BEFORE INSERT ON dongfu_patrol_operations"),
        )
        for index, (target, operation) in enumerate(faults):
            probe.player(user_stamina=100, stone=1000000)
            probe.seed(player, "UPDATE dongfu_status SET patrol_date='2026-11-22',patrol_count=0,patrol_guard=0 WHERE user_id='900001'")
            probe.seed(game, "DELETE FROM back WHERE user_id='900001' AND goods_id=21005")
            before = _business(probe)
            probe.seed(target, "CREATE TRIGGER patrol_fault " + operation + " BEGIN SELECT RAISE(ABORT,'patrol refused'); END")
            reply = await _command(probe, number=970850 + index, abort=True)
            assert _business(probe) == before, (operation, reply, before, _business(probe))
            assert "定位号" in reply and "patrol refused" not in reply, reply
            probe.seed(target, "DROP TRIGGER patrol_fault")
            success = await _command(probe, number=970850 + index)
            assert "洞府巡山完成" in success, success

    @case("dongfu-patrol-parameters", "巡山护府", "别名/体力/日上限/材料容量/新日重置",
          "水域，随机奖励与灵石固定，分步调整隔离玩家/洞府状态", "精确正常拒绝零业务写、别名结算和每日/护府边界",
          group="dongfu-patrol")
    async def parameters(probe):
        module = _seed(probe)
        game, player = probe.context.paths.game_db, probe.context.paths.player_db
        probe.seed(game, "UPDATE user_xiuxian SET user_stamina=7 WHERE user_id='900001'")
        low = _business(probe)
        reply = await _command(probe, number=970880)
        assert "体力不足" in reply and _business(probe) == low, reply
        probe.seed(game, "UPDATE user_xiuxian SET user_stamina=100 WHERE user_id='900001'")
        probe.seed(player, "UPDATE dongfu_status SET patrol_count=3,patrol_guard=3 WHERE user_id='900001'")
        full = _business(probe)
        limited = await _command(probe, number=970881)
        assert "已达上限（3次）" in limited and _business(probe) == full, limited
        probe.seed(player, "UPDATE dongfu_status SET patrol_date='2026-11-21',patrol_count=3,patrol_guard=3 WHERE user_id='900001'")
        assert probe.rows(player, "SELECT patrol_date,patrol_count,patrol_guard FROM dongfu_status WHERE user_id='900001'") == [
            ("2026-11-21", 3, 3)
        ]
        stone_before = probe.rows(game, "SELECT stone FROM user_xiuxian WHERE user_id='900001'")[0][0]
        reset = await _command(probe, number=970882, command="巡山护府", reward_roll=1)
        assert "今日巡山：1/3" in reset and "灵息露x1" not in reset, reset
        assert probe.rows(game, "SELECT stone,user_stamina FROM user_xiuxian WHERE user_id='900001'") == [
            (stone_before + 120000, 92)
        ]
        assert probe.rows(player, "SELECT patrol_date,patrol_count,patrol_guard FROM dongfu_status WHERE user_id='900001'") == [
            ("2026-11-22", 1, 1)
        ]
        assert probe.rows(game, "SELECT payload,patrol_count,patrol_guard FROM dongfu_patrol_operations WHERE operation_id='dongfu-patrol:900001:970882'") == [
            ('["900001","2026-11-22"]', 1, 1)
        ]
        assert probe.rows(game, "SELECT * FROM back WHERE user_id='900001'") == []
        assert probe.rows(game, "SELECT user_stamina,stone FROM user_xiuxian WHERE user_id='900002'") == [(100, 1000000)]
        second = await _command(probe, number=970883, reward_roll=1)
        assert "巡山护府：2层" in second and "今日巡山：2/3" in second, second
        third = await _command(probe, number=970884, reward_roll=1)
        assert "巡山护府：3层" in third and "今日巡山：3/3" in third, third
        assert probe.rows(player, "SELECT patrol_count,patrol_guard FROM dongfu_status WHERE user_id='900001'") == [(3, 3)]
        before_limit = _business(probe)
        fourth = await _command(probe, number=970885, reward_roll=1)
        assert "已达上限（3次）" in fourth and _business(probe) == before_limit, fourth
        assert probe.rows(game, "SELECT COUNT(*) FROM dongfu_patrol_operations WHERE operation_id IN "
                          "('dongfu-patrol:900001:970882','dongfu-patrol:900001:970883','dongfu-patrol:900001:970884')") == [(3,)]
        _database_projection(probe)

    @case("dongfu-patrol-capacity", "洞府巡山", "奖励材料达到背包堆叠上限",
          "水域/灵息露达到XiuConfig.max_goods_num/固定掉落命中", "具体容量拒绝且体力、灵石、巡山、背包与回执均零写",
          group="dongfu-patrol", outcome="rejection")
    async def inventory_capacity(probe):
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_config import XiuConfig

        _seed(probe)
        game, player = probe.context.paths.game_db, probe.context.paths.player_db
        maximum = XiuConfig().max_goods_num
        probe.grant_item(21005, quantity=maximum)
        before = _business(probe)
        reply = await _command(probe, number=970886, reward_roll=0)
        assert "背包空间不足" in reply, reply
        assert _business(probe) == before
        assert probe.rows(game, "SELECT goods_num FROM back WHERE user_id='900001' AND goods_id=21005") == [(maximum,)]
        assert probe.rows(game, "SELECT COUNT(*) FROM dongfu_patrol_operations") == [(0,)]
        _database_projection(probe)

    @case("dongfu-patrol-private-isolation", "巡山护府", "私聊别名由第二玩家巡山",
          "第二玩家单独建设水域洞府/不掉材料", "别名与私聊入口成功，只改变其game/player记录与回执",
          group="dongfu-patrol")
    async def private_player_isolation(probe):
        _seed(probe)
        game, player = probe.context.paths.game_db, probe.context.paths.player_db
        probe.seed(player, "INSERT INTO dongfu_status(user_id,built,node_type,patrol_date,patrol_count,patrol_guard) "
                   "VALUES('900002',1,'水域','2026-11-22',0,0) ON CONFLICT(user_id) DO UPDATE SET "
                   "built=1,node_type='水域',patrol_date='2026-11-22',patrol_count=0,patrol_guard=0")
        before_first = probe.rows(game, "SELECT user_stamina,stone FROM user_xiuxian WHERE user_id='900001'")
        reply = await _command(probe, number=970887, actor="900002", command="巡山护府",
                               event_type="private", reward_roll=1)
        assert "巡山护府：1层" in reply and "今日巡山：1/3" in reply and "灵息露x1" not in reply, reply
        assert probe.rows(game, "SELECT user_stamina,stone FROM user_xiuxian WHERE user_id='900001'") == before_first
        assert probe.rows(game, "SELECT user_stamina,stone FROM user_xiuxian WHERE user_id='900002'") == [(92, 1120000)]
        assert probe.rows(game, "SELECT COUNT(*) FROM back WHERE user_id IN ('900001','900002') AND goods_id=21005") == [(0,)]
        assert probe.rows(player, "SELECT patrol_count,patrol_guard FROM dongfu_status WHERE user_id='900001'") == [(0, 0)]
        assert probe.rows(player, "SELECT patrol_count,patrol_guard FROM dongfu_status WHERE user_id='900002'") == [(1, 1)]
        assert probe.rows(game, "SELECT operation_id,payload,patrol_count,patrol_guard FROM dongfu_patrol_operations") == [
            ("dongfu-patrol:900002:970887", '["900002","2026-11-22"]', 1, 1)
        ]
        _database_projection(probe)

    @case("dongfu-patrol-receipt-recovery", "洞府巡山", "合法巡山，永久回执INSERT IGNORE",
          "水域/体力灵石充足/固定奖励，隔离双库回执拒写",
          "不伪成功、完整回滚、具体回执原因、故障解除原消息恢复/replay", group="dongfu-patrol", outcome="recovery")
    async def receipt_recovery(probe):
        from nonebot_plugin_xiuxian_2.core.player_reasons import reason_text

        module = _seed(probe)
        game = probe.context.paths.game_db
        before = _business(probe)
        probe.seed(game, "CREATE TRIGGER patrol_fault BEFORE INSERT ON dongfu_patrol_operations BEGIN SELECT RAISE(IGNORE); END")
        reply = await _command(probe, number=970802)
        probe.evidence.update(reply=reply, before=before, after=_business(probe))
        assert _business(probe) == before, (reply, before, _business(probe))
        assert reason_text("dongfu_patrol_receipt_write_rejected") in reply and "巡山回执" in reply, reply
        probe.seed(game, "DROP TRIGGER patrol_fault")
        success = await _command(probe, number=970802)
        assert "洞府巡山完成" in success and "今日巡山：1/3" in success, success
        committed = _database_projection(probe)
        assert "已经处理" in await _command(probe, number=970802)
        assert _database_projection(probe) == committed
