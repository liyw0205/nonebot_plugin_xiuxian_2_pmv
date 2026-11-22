"""Real matcher coverage for the personal cave-harvest history."""

import json
import sqlite3
from datetime import datetime
from unittest.mock import patch

from tests.parameter_cases_dongfu_status_view import _snapshot


class FixedClock:
    def now(self):
        return datetime(2026, 11, 22, 12)


def _seed(probe):
    from nonebot_plugin_xiuxian_2.features.dongfu.harvest_history import (
        DongfuHarvestHistoryApplication,
    )
    from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_dongfu as module

    game = probe.context.paths.game_db
    module.dongfu_harvest_history_application = DongfuHarvestHistoryApplication(
        game, clock=FixedClock()
    )
    with sqlite3.connect(game) as connection:
        connection.execute("DELETE FROM dongfu_harvest_operations")
    rows = (
        ("history-current", "900001", "2026-11-22 12:00:00",
         [[1001, "青灵草", "药材", 2], [1002, "灵石袋", "道具", 1]], ["2"]),
        ("history-boundary", "900001", "2026-11-15 12:00:00",
         [[1003, "青玉果", "果实", 3]], []),
        ("history-old", "900001", "2026-11-15 11:59:59",
         [[1004, "过期灵木", "材料", 99]], []),
        ("history-other", "900002", "2026-11-22 11:00:00",
         [[1005, "他人灵药", "药材", 50]], []),
        ("history-future", "900001", "2026-11-23 00:00:00",
         [[1001, "青灵草", "药材", 1]], []),
    )
    for operation_id, actor, created_at, items, failed in rows:
        payload = json.dumps(
            [actor, [1, 2], [1], items, failed], ensure_ascii=False
        )
        rewards = json.dumps([[item[0], item[3]] for item in items])
        probe.seed(
            game,
            "INSERT INTO dongfu_harvest_operations(operation_id,payload,rewards,created_at) "
            "VALUES(?,?,?,?)",
            (operation_id, payload, rewards, created_at),
        )
    return module, game


def register(case):
    @case(
        "dongfu-harvest-history-parameters-window-isolation",
        "我的洞府收获记录",
        "真实投递三个别名并查询确定的历史/边界数据",
        "隔离game SQLite含本玩家窗口边界/内外、其他玩家和未来记录",
        "汇总本玩家下界内记录、最近顺序和奖励数量，不泄漏其他玩家且全库只读",
        group="dongfu-harvest-history",
    )
    async def history_window_isolation(probe):
        module, _ = _seed(probe)
        before = _snapshot(probe)
        commands = (
            "我的洞府收获记录",
            "洞府收获记录",
            "灵田收获记录",
        )
        replies = []
        for index, command in enumerate(commands):
            reply = await probe.command(
                command, command, number=974300 + index, event_type="private",
                timeout=30,
            )
            assert "近7天洞府收获记录" in reply, reply
            assert "收获：3 次（共 7 件，3 种物品）" in reply, reply
            assert "青灵草×1" in reply and "青灵草×2、灵石袋×1（1 块未成熟）" in reply, reply
            assert "青玉果×3" in reply, reply
            assert "过期灵木" not in reply and "他人灵药" not in reply, reply
            replies.append(reply)
        after = _snapshot(probe)
        assert replies[0] == replies[1] == replies[2], replies
        assert after == before, "harvest history aliases wrote to a database"

        empty = await probe.command(
            commands[0], commands[0], number=974304, actor="900002",
            event_type="private", timeout=30,
        )
        assert "收获：1 次（共 50 件，1 种物品）" in empty, empty
        assert "他人灵药×50" in empty and "青灵草" not in empty, empty
        assert _snapshot(probe) == before, "empty history read wrote to a database"
        probe.evidence.update(before=before, after=after, replies=replies, second_player=empty)

    @case(
        "dongfu-harvest-history-read-denied-recovery",
        "我的洞府收获记录",
        "真实历史SELECT经SQLite authorizer拒绝后解除并用同一消息恢复",
        "本玩家有有效收获记录，读取payload列被SQLITE_DENY",
        "数据库读取故障不得伪装成无记录；保留traceback/定位号且故障解除后恢复",
        group="dongfu-harvest-history",
        outcome="rejection",
    )
    async def denied_history_read_recovers(probe):
        from nonebot_plugin_xiuxian_2.features.dongfu import harvest_history

        _seed(probe)
        game = probe.context.paths.game_db
        before = _snapshot(probe)
        original_uow = harvest_history.DatabaseUnitOfWork

        class DenyHistoryReadUnitOfWork:
            def __init__(self, *args, **kwargs):
                self.delegate = original_uow(*args, **kwargs)

            def __enter__(self):
                active = self.delegate.__enter__()

                def authorize(action, table, column, database, trigger):
                    if (
                        action == sqlite3.SQLITE_READ
                        and table == "dongfu_harvest_operations"
                        and column == "payload"
                    ):
                        return sqlite3.SQLITE_DENY
                    return sqlite3.SQLITE_OK

                active.connection.set_authorizer(authorize)
                return active

            def __exit__(self, exc_type, exc, traceback):
                return self.delegate.__exit__(exc_type, exc, traceback)

        with patch.object(
            harvest_history, "DatabaseUnitOfWork", DenyHistoryReadUnitOfWork
        ):
            reply = await probe.command(
                "我的洞府收获记录", "我的洞府收获记录", number=974310,
                event_type="private", allow_exception=True, timeout=30,
            )
        failed_call = probe.calls[-1]
        after_failure = _snapshot(probe)
        recovered = await probe.command(
            "我的洞府收获记录", "我的洞府收获记录", number=974310,
            event_type="private", timeout=30,
        )
        after_recovery = _snapshot(probe)
        errors = "\n".join(failed_call.get("errors", []))
        probe.evidence.update(
            before=before, after_failure=after_failure,
            after_recovery=after_recovery, reply=reply, recovered=recovered,
            errors=failed_call.get("errors", []),
            caught_exceptions=failed_call.get("caught_exceptions", []),
        )
        assert "记录处理提示" in reply and "定位号" in reply, reply
        assert "近7天没有洞府收获记录" not in reply, reply
        assert "access to dongfu_harvest_operations.payload is prohibited" in errors, failed_call
        assert "收获：3 次（共 7 件，3 种物品）" in recovered, recovered
        assert after_failure == after_recovery == before, "history reads wrote to data"
