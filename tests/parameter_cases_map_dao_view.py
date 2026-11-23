"""Dispatch dao record queries through the registered OneBot matcher."""

import logging
import sqlite3
import traceback
from unittest.mock import patch

from tests.parameter_cases_map_help import _database_projection
from tests.parameter_cases_map_position import _seed_status


def _setup(probe):
    from nonebot_plugin_xiuxian_2.features.combat_settlement.migrations import apply_dao_battle_record
    from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
    from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_map as module

    database = probe.context.paths.player_db
    with DatabaseUnitOfWork(database) as uow:
        apply_dao_battle_record(uow)
        uow.executemany(
            "INSERT INTO dao_record(user_id,total,win,lose) VALUES(?,?,?,?)",
            [("900001", 3, 2, 1), ("900002", 5, 1, 4)],
        )
    data = module._load_map_data()
    realm = module._all_realms(data)[0]
    heaven = module._heaven_names(data, realm)[0]
    nodes = module._nodes(data, realm, heaven)
    for actor, node in zip(("900001", "900002", "900003"), (nodes[0], nodes[0], nodes[1])):
        _seed_status(probe, realm=realm, heaven=heaven, node_id=node["id"], actor=actor)
    return database.resolve()


async def _invoke(probe, name="", **kwargs):
    return await probe.command("论道查看", "论道查看" + (" " + name if name else ""), timeout=30, **kwargs)


def _assert_readonly(probe, before):
    after = _database_projection(probe)
    for key in ("game_db", "player_db", "trade_db", "impart_db"):
        assert before[key] == after[key], key


async def _assert_read_recovery(probe, repository, table, target, reason, number, *, logger_name=None):
    from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork

    database = _setup(probe)
    before = _database_projection(probe)

    class AuthorizerUnitOfWork(DatabaseUnitOfWork):
        def __enter__(self):
            super().__enter__()
            if self.database.resolve() == database:
                self.connection.set_authorizer(
                    lambda action, read_table, *_: sqlite3.SQLITE_DENY
                    if action == sqlite3.SQLITE_READ and read_table == table else sqlite3.SQLITE_OK
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
    service_logger = logging.getLogger(logger_name or repository.__name__)
    service_logger.addHandler(capture)
    previous_level = service_logger.level
    service_logger.setLevel(logging.ERROR)
    try:
        with patch.object(repository, "DatabaseUnitOfWork", AuthorizerUnitOfWork):
            failed = await _invoke(probe, target, number=number, allow_exception=True)
    finally:
        service_logger.removeHandler(capture)
        service_logger.setLevel(previous_level)
    errors = capture.errors
    assert reason in failed and "附近未找到" not in failed and "【论道战绩】" not in failed, failed
    assert any(table + "." in error and "prohibited" in error for error in errors), errors
    recovered = await _invoke(probe, target, number=number)
    assert "【论道战绩】" in recovered and ("总场次：5" if target else "总场次：3") in recovered, recovered
    _assert_readonly(probe, before)


def register(case):
    @case("map-dao-view-records", "论道查看", "无参数、空白及合法道号",
          "同位置及异节点玩家拥有独立战绩", "本人及同节点战绩正确，异节点与不存在目标不可见",
          group="map-dao-view")
    async def records(probe):
        _setup(probe)
        before = _database_projection(probe)
        for name, event_type in (("", "group"), ("　", "private"), ("诊断道友", "group")):
            reply = await _invoke(probe, name, event_type=event_type)
            assert all(text in reply for text in ("【论道战绩】诊断道友", "总场次：3", "胜场：2", "负场：1", "胜率：66.7%")), reply
        for _ in range(2):
            reply = await _invoke(probe, "诊断对手", number=975810)
            assert all(text in reply for text in ("【论道战绩】诊断对手", "总场次：5", "胜场：1", "负场：4", "胜率：20.0%")), reply
        for name in ("诊断管理", "不存在的道友"):
            reply = await _invoke(probe, name)
            assert f"附近未找到道友【{name}】" in reply and "【论道战绩】" not in reply, reply
        empty = await _invoke(probe, actor="900003")
        assert all(text in empty for text in ("【论道战绩】诊断管理", "总场次：0", "胜率：0.0%")), empty
        unregistered = await _invoke(probe, actor="900099")
        assert "我要修仙" in unregistered and "【论道战绩】" not in unregistered, unregistered
        _assert_readonly(probe, before)

    @case("map-dao-view-target-read-recovery", "论道查看", "合法道号",
          "拒绝读取同节点目标", "资料读取失败不伪报未找到，解除后同消息可查询",
          group="map-dao-view", outcome="recovery")
    async def target_read_recovery(probe):
        from nonebot_plugin_xiuxian_2.features.map import repository

        await _assert_read_recovery(probe, repository, "user_xiuxian", "诊断对手", "附近道友资料读取失败", 975811)

    @case("map-dao-view-record-read-recovery", "论道查看", "本人查询",
          "拒绝读取战绩表", "战绩读取失败有明确回复，解除后同消息可查询",
          group="map-dao-view", outcome="recovery")
    async def record_read_recovery(probe):
        from nonebot_plugin_xiuxian_2.features.combat_settlement import repository
        from nonebot_plugin_xiuxian_2.features.combat_settlement import application

        await _assert_read_recovery(
            probe, repository, "dao_record", "", "论道战绩读取失败", 975812,
            logger_name=application.__name__,
        )
