"""Run the nearby-player display through its registered OneBot matcher."""

import logging
import sqlite3
import traceback
from unittest.mock import patch

from tests.parameter_cases_map_help import _database_projection
from tests.parameter_cases_map_position import _seed_status


def _plain(text):
    return text.replace("\r", "\n").replace("[CQ:at,qq=900001]", "").strip()


def _seed_group(probe, count=0):
    from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_map as module

    data = module._load_map_data()
    realm = module._all_realms(data)[0]
    heaven = module._heaven_names(data, realm)[0]
    nodes = module._nodes(data, realm, heaven)
    _seed_status(probe, realm=realm, heaven=heaven, node_id=nodes[0]["id"])
    game = probe.managers[0]
    for index in range(count):
        user_id = f"9901{index:02d}"
        _seed_status(probe, realm=realm, heaven=heaven, node_id=nodes[0]["id"], actor=user_id)
        game.conn.execute(
            "INSERT INTO user_xiuxian(user_id,user_name,level,power) VALUES(?,?,?,?)",
            (user_id, f"候选{index:02d}", f"境界{index:02d}", index + 10),
        )
    outside_id = "990199"
    if len(nodes) > 1:
        _seed_status(probe, realm=realm, heaven=heaven, node_id=nodes[1]["id"], actor=outside_id)
        game.conn.execute(
            "INSERT INTO user_xiuxian(user_id,user_name,level,power) VALUES(?,?,?,?)",
            (outside_id, "异节点道友", "异界", 999),
        )
    game.conn.commit()
    game._clear_read_cache()
    return realm, heaven, nodes


def _business_projection(probe):
    snapshot = _database_projection(probe)
    return {key: snapshot[key] for key in ("game_db", "player_db", "trade_db", "impart_db")}


def register(case):
    @case(
        "map-nearby-display-sampling-readonly",
        "附近道友",
        "无参数、重复查询、12名同节点候选与异节点玩家",
        "固定抽样源并检查实际 map_status/profile 数据",
        "排除本人和异节点、最多展示10名、顺序稳定且业务库零写",
        group="map-nearby-display",
    )
    async def sampling_and_readonly(probe):
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_map as module

        _seed_group(probe, count=12)
        before = _business_projection(probe)
        with (
            patch.object(module.runtime_random, "randint", side_effect=lambda _low, high: high),
            patch.object(module.runtime_random, "sample", side_effect=lambda items, _count: list(reversed(items))),
        ):
            first = _plain(await probe.command("附近道友", "附近道友", number=975700, timeout=30))
            second = _plain(await probe.command(
                "附近道友", "附近道友", number=975701, event_type="private", timeout=30,
            ))
        names = [f"候选{index:02d}" for index in reversed(range(10))]
        assert first == second, (first, second)
        assert first.startswith("【附近道友】"), first
        assert [line.split("（", 1)[0][2:] for line in first.splitlines()[1:]] == names, first
        assert "异节点道友" not in first and "900001" not in first, first
        assert _business_projection(probe) == before
        probe.evidence.update(displayed_names=names, candidate_count=12, excluded_other_node=True,
                              repeated_query_readonly=True)

    @case(
        "map-nearby-display-empty-and-unregistered",
        "附近道友",
        "没有同节点候选与未注册玩家",
        "固定本人的规范位置",
        "无候选使用既有空态回复；未注册提示入门且不初始化地图状态",
        group="map-nearby-display",
        outcome="rejection",
    )
    async def empty_and_unregistered(probe):
        _seed_group(probe)
        before = _business_projection(probe)
        empty = _plain(await probe.command("附近道友", "附近道友", number=975710, timeout=30))
        unregistered = _plain(await probe.command(
            "附近道友", "附近道友", actor="900099", number=975711, timeout=30,
        ))
        assert empty == "附近暂无其他道友。", empty
        assert "我要修仙" in unregistered, unregistered
        assert probe.rows(probe.context.paths.player_db,
                          "SELECT 1 FROM map_status WHERE user_id='900099'") == []
        assert _business_projection(probe) == before

    @case(
        "map-nearby-display-read-recovery",
        "附近道友",
        "合法查询时拒绝读取同节点档案",
        "对隔离附库 user_xiuxian 安装 SQLITE_DENY",
        "不伪报附近为空；明确查询失败、保留原异常，解除后原消息可恢复",
        group="map-nearby-display",
        outcome="recovery",
    )
    async def read_failure_recovery(probe):
        from nonebot_plugin_xiuxian_2.core.player_reasons import reason_text
        from nonebot_plugin_xiuxian_2.features.map import nearby_display_repository
        from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork

        _seed_group(probe, count=1)
        database = probe.context.paths.player_db.resolve()
        before = _business_projection(probe)
        state = {"deny": True, "table": "map_status"}

        class AuthorizerUnitOfWork(DatabaseUnitOfWork):
            def __enter__(self):
                super().__enter__()
                if self.database.resolve() == database and state["deny"]:
                    self.connection.set_authorizer(
                        lambda action, table, *_: sqlite3.SQLITE_DENY
                        if action == sqlite3.SQLITE_READ and table == state["table"]
                        else sqlite3.SQLITE_OK
                    )
                return self

        class ErrorCapture(logging.Handler):
            def __init__(self):
                super().__init__(logging.ERROR)
                self.errors = []

            def emit(self, record):
                if record.exc_info:
                    self.errors.append("".join(traceback.format_exception(*record.exc_info)))

        capture = ErrorCapture()
        service_logger = logging.getLogger("nonebot_plugin_xiuxian_2.features.map.application")
        previous_level = service_logger.level
        service_logger.addHandler(capture)
        service_logger.setLevel(logging.ERROR)
        try:
            with patch.object(nearby_display_repository, "DatabaseUnitOfWork", AuthorizerUnitOfWork):
                failures = []
                for index, (table, reason, subject) in enumerate((
                    ("map_status", "map_nearby_position_read_failed", "本人地图位置"),
                    ("user_xiuxian", "map_nearby_display_read_failed", "附近道友列表"),
                )):
                    state.update(deny=True, table=table)
                    number = 975720 + index
                    failed = _plain(await probe.command(
                        "附近道友", "附近道友", number=number, timeout=30, allow_exception=True,
                    ))
                    failures.append((failed, reason, subject))
                    state["deny"] = False
                    recovered = _plain(await probe.command(
                        "附近道友", "附近道友", number=number, timeout=30,
                    ))
                    assert "候选00" in recovered, (table, recovered)
        finally:
            service_logger.removeHandler(capture)
            service_logger.setLevel(previous_level)

        errors = list(capture.errors)
        assert all(
            subject in reply and reason_text(reason) in reply and "附近暂无" not in reply
            for reply, reason, subject in failures
        ), failures
        assert any("map_status.realm" in error and "prohibited" in error for error in errors), errors
        assert any("user_xiuxian" in error and "prohibited" in error for error in errors), errors
        assert _business_projection(probe) == before
        probe.evidence.update(read_failure_replies=[reply for reply, _, _ in failures], tracebacks=errors,
                              business_databases_unchanged=True, recovery_after_each_failure=True)
