"""Real matcher coverage for the read-only cave status view."""

import sqlite3
from datetime import datetime, timedelta, timezone
from unittest.mock import patch


def _seed(probe):
    from nonebot_plugin_xiuxian_2.features.dongfu.application import DongfuApplication
    from nonebot_plugin_xiuxian_2.features.dongfu.plant_slots import (
        canonical_plant_slots,
        empty_plant_slot,
    )
    from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_dongfu as module

    module.configure_dongfu_application(
        DongfuApplication(probe.context.paths.game_db, probe.context.paths.player_db)
    )

    slots = [empty_plant_slot(index) for index in range(1, 4)]
    slots[0].update(
        seed_id=21001,
        seed_name=module.SEED_CONFIG[21001]["name"],
        plant_start="2026-11-22 10:00:00",
        plant_finish="2026-11-22 12:00:00",
        fertilizer=1,
    )
    slots[1].update(
        seed_id=21002,
        seed_name=module.SEED_CONFIG[21002]["name"],
        plant_start="2026-11-22 11:00:00",
        plant_finish="2026-11-22 13:00:00",
    )
    probe.seed(
        probe.context.paths.player_db,
        "INSERT INTO dongfu_status(user_id,built,realm,heaven,node_id,node_name,node_type,"
        "array_level,planting,plant_seed_id,plant_start,plant_finish,plant_slots,plot_count,"
        "intrude_date,intrude_count,infiltrate_date,infiltrate_active_count,"
        "infiltrate_random_count,patrol_date,patrol_count,patrol_guard) "
        "VALUES('900001',1,'仙界','九重天','node-home','云宫','水域',3,1,21001,"
        "'2026-11-22 10:00:00','2026-11-22 12:00:00',?,3,'2000-01-01',3,"
        "'2000-01-01',2,1,'2000-01-01',3,2) "
        "ON CONFLICT(user_id) DO UPDATE SET built=excluded.built,realm=excluded.realm,"
        "heaven=excluded.heaven,node_id=excluded.node_id,node_name=excluded.node_name,"
        "node_type=excluded.node_type,array_level=excluded.array_level,"
        "planting=excluded.planting,plant_seed_id=excluded.plant_seed_id,"
        "plant_start=excluded.plant_start,plant_finish=excluded.plant_finish,"
        "plant_slots=excluded.plant_slots,plot_count=excluded.plot_count,"
        "intrude_date=excluded.intrude_date,intrude_count=excluded.intrude_count,"
        "infiltrate_date=excluded.infiltrate_date,"
        "infiltrate_active_count=excluded.infiltrate_active_count,"
        "infiltrate_random_count=excluded.infiltrate_random_count,"
        "patrol_date=excluded.patrol_date,patrol_count=excluded.patrol_count,"
        "patrol_guard=excluded.patrol_guard",
        (canonical_plant_slots(slots),),
    )
    return module


def _snapshot(probe):
    from tests.parameter_cases_dongfu_harvest import _database_projection

    return _database_projection(probe)


async def _command(
    probe, module, *, number, now=datetime(2026, 11, 22, 12), expect_error=False, **options
):
    with patch.object(module.runtime_clock, "now", return_value=now):
        reply = await probe.command(
            "我的洞府", "我的洞府", number=number, timeout=30, allow_exception=True, **options
        )
    call = probe.calls[-1]
    if not expect_error:
        assert not call.get("errors"), call["errors"]
    for caught in call.get("caught_exceptions", []):
        assert caught["function"] == "_parse_dt", caught
        assert (
            "ValueError: time data" in caught["traceback"]
            and "%Y-%m-%d %H:%M:%S.%f" in caught["traceback"]
        ), caught
    return reply


def register(case):
    @case(
        "dongfu-status-view-projection",
        "我的洞府",
        "查看有效洞府、地脉/灵田/成熟状态与跨日剩余次数",
        "隔离玩家水域洞府、两块在种灵田与旧日期的次数状态",
        "真实展示地脉和时钟边界的派生状态，查询前后数据库完全不变",
        group="dongfu-status-view",
    )
    async def status_projection(probe):
        module = _seed(probe)
        before = _snapshot(probe)
        at_finish = datetime(2026, 11, 22, 20, tzinfo=timezone(timedelta(hours=8)))
        before_finish = await _command(
            probe, module, number=973400, now=at_finish - timedelta(microseconds=1)
        )
        reply = await _command(
            probe, module, number=973401,
            now=at_finish, event_type="private",
        )
        other = await _command(
            probe, module, number=973403, actor="900002", event_type="private",
        )
        after = _snapshot(probe)
        probe.evidence.update(before=before, after=after, reply=reply,
                              before_finish=before_finish, other=other)
        assert "2/3 已播种，0块可收获" in before_finish, before_finish
        assert "（可收获" not in before_finish, before_finish
        assert "我的洞府" in reply and "仙界·九重天·云宫" in reply, reply
        assert "水润灵脉" in reply and "成熟时间-8%" in reply, reply
        assert "2/3 已播种，1块可收获" in reply, reply
        assert "剩余60分钟" in reply, reply
        assert "今日巡山0/3" in reply and "今日可发起潜入：主动3/3，随机3/3" in reply, reply
        assert "今日剩余可被潜入次数：3" in reply, reply
        assert "你尚未建设洞府" in other and "云宫" not in other, other
        assert after == before, "status display wrote to the database"

    @case(
        "dongfu-status-read-denied-not-missing",
        "我的洞府",
        "对真实状态SELECT施加SQLite authorizer拒绝",
        "有效洞府，隔离player SQLite在读取built列时返回SQLITE_DENY",
        "必须报告状态读取失败且保留SQL异常，不得伪装成未建设洞府",
        group="dongfu-status-view",
        outcome="rejection",
    )
    async def status_read_denied(probe):
        from nonebot_plugin_xiuxian_2.features.dongfu import status_repository

        module = _seed(probe)
        player = probe.context.paths.player_db
        before = _snapshot(probe)
        original_uow = status_repository.DatabaseUnitOfWork

        class DenyCaveReadUnitOfWork:
            def __init__(self, *args, **kwargs):
                self.delegate = original_uow(*args, **kwargs)

            def __enter__(self):
                active = self.delegate.__enter__()

                def authorize(action, arg1, arg2, database, trigger):
                    if (
                        action == sqlite3.SQLITE_READ
                        and arg1 == "dongfu_status"
                        and arg2 == "built"
                    ):
                        return sqlite3.SQLITE_DENY
                    return sqlite3.SQLITE_OK

                active.connection.set_authorizer(authorize)
                return active

            def __exit__(self, exc_type, exc, traceback):
                return self.delegate.__exit__(exc_type, exc, traceback)

        with patch.object(
            status_repository, "DatabaseUnitOfWork", DenyCaveReadUnitOfWork
        ):
            reply = await _command(probe, module, number=973402, expect_error=True)
        failed_call = probe.calls[-1]
        after_failure = _snapshot(probe)
        recovered = await _command(probe, module, number=973402)
        after = _snapshot(probe)
        probe.evidence.update(
            before=before,
            after_failure=after_failure,
            after=after,
            reply=reply,
            recovered=recovered,
            player_db=str(player),
            handler_errors=failed_call.get("errors", []),
            caught_exceptions=failed_call.get("caught_exceptions", []),
        )
        assert "尚未建设洞府" not in reply, reply
        assert "我的洞府处理提示" in reply and "定位号" in reply, reply
        errors = "\n".join(failed_call.get("errors", []))
        assert "access to dongfu_status.built is prohibited" in errors, failed_call
        assert "仙界·九重天·云宫" in recovered and "1块可收获" in recovered, recovered
        assert after_failure == after == before, "failed and recovered status reads must be read-only"
