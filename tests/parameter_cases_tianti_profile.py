"""Real matcher probes for the read-only 我的炼体 status view."""
from __future__ import annotations

import json
import inspect
import sqlite3
import sys
from datetime import datetime
from unittest.mock import patch

from tests.parameter_cases_team_leave import integrity
from tests.parameter_cases_team_view import database_projection


def register(case):
    @case(
        "tianti-profile-read",
        "我的炼体",
        "本人/别名/冷态/读取失败",
        "隔离 player SQLite 中写入有效、过期及缺失档案",
        "真实只读状态映射；拒绝档案读取时给出失败回复并保留错误日志",
        group="tianti-profile-parameters",
        outcome="rejection",
    )
    async def read_tianti_profile(probe):
        from nonebot_plugin_xiuxian_2.features.tianti_training.repository import (
            TiantiProfileReader,
        )
        from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.utils import number_to

        player_database = probe.context.paths.player_db
        defaults = TiantiProfileReader(probe.context.paths.data).default_data()
        fields = tuple(defaults)

        def seed_profile(user_id, hp, bath_end, bath_name):
            data = dict(defaults)
            data.update(
                tianti_hp=str(hp),
                medicine_last_time="2026-11-21 11:00:00",
                medicine_end_time=bath_end,
                medicine_effect="1.25",
                medicine_name=bath_name,
                opened_qiaoxue=json.dumps([], ensure_ascii=False),
                opened_qiaoxue_detail=json.dumps([], ensure_ascii=False),
                qiaoxue_stage_opened=json.dumps({}, ensure_ascii=False),
            )
            columns = ("user_id", *fields)
            placeholders = ",".join("?" for _ in columns)
            probe.seed(
                player_database,
                f"INSERT OR REPLACE INTO tianti_info ({','.join(columns)}) "
                f"VALUES ({placeholders})",
                (user_id, *(data[field] for field in fields)),
            )

        seed_profile("900001", 12345, "2026-11-21 13:00:00", "有效药浴")
        seed_profile("900002", 23456, "2026-11-21 11:59:00", "过期药浴")
        assert probe.rows(
            player_database,
            "SELECT tianti_hp,medicine_end_time FROM tianti_info "
            "WHERE user_id IN ('900001','900002') ORDER BY user_id",
        ) == [("12345", "2026-11-21 13:00:00"), ("23456", "2026-11-21 11:59:00")]

        class FrozenClock:
            @staticmethod
            def now():
                return datetime(2026, 11, 21, 12, 0)

        matcher_entry = next(
            entry for entry in probe.entries if entry["command"] == "我的炼体"
        )
        handler = inspect.unwrap(matcher_entry["matcher"].handlers[0].call)
        module = sys.modules[handler.__module__]
        before = database_projection(probe)
        with patch.object(module, "runtime_clock", FrozenClock()):
            primary = await probe.command("我的炼体", number=979801)
            assert "【我的炼体】" in primary and "炼体气血：" + number_to(12345) in primary, primary
            assert f"境界：{defaults['tianti_level']}" in primary, primary
            assert "药浴：有效药浴" in primary and "剩余 60 分钟" in primary, primary
            assert "已开窍：0/108" in primary, primary
            assert database_projection(probe) == before

            probe.clear_cooldowns()
            repeated = await probe.command("我的炼体", number=979802)
            assert repeated == primary, (primary, repeated)
            assert database_projection(probe) == before

            probe.clear_cooldowns()
            private_alias = await probe.command(
                "炼体状态", "炼体状态", actor="900002", event_type="private", number=979803
            )
            assert "炼体气血：" + number_to(23456) in private_alias, private_alias
            assert "上次药浴已结束（过期药浴" in private_alias, private_alias
            assert database_projection(probe) == before

            probe.clear_cooldowns()
            cold = await probe.command("我的炼体", actor="900003", number=979804)
            assert "炼体气血：0" in cold, cold
            assert probe.rows(
                player_database,
                "SELECT user_id FROM tianti_info WHERE user_id='900003'",
            ) == []
            assert database_projection(probe) == before

            probe.clear_cooldowns()
            unregistered = await probe.command(
                "我的炼体", actor="900099", number=979805
            )
            assert "修仙界没有道友的信息" in unregistered, unregistered
            assert database_projection(probe) == before

            original_enter = DatabaseUnitOfWork.__enter__

            def enter_with_denied_profile_read(uow):
                result = original_enter(uow)
                if uow.database.resolve() == player_database.resolve() and uow.read_only:
                    def deny_profile_read(action, table, _column, _database, _source):
                        if action == sqlite3.SQLITE_READ and table == "tianti_info":
                            return sqlite3.SQLITE_DENY
                        return sqlite3.SQLITE_OK

                    uow.connection.set_authorizer(deny_profile_read)
                return result

            probe.clear_cooldowns()
            with patch.object(DatabaseUnitOfWork, "__enter__", enter_with_denied_profile_read):
                failed_read = await probe.command(
                    "我的炼体", number=979806, allow_exception=True
                )
            failure_call = probe.calls[-1]
            assert "我的炼体处理提示：" in failed_read and "定位号：" in failed_read, failed_read
            assert any(
                "sqlite3.DatabaseError: access to tianti_info.user_id is prohibited" in error
                for error in failure_call["errors"]
            ), failure_call
            assert any(
                "DatabaseError('access to tianti_info.user_id is prohibited')" in log["exception"]
                for log in failure_call["logs"]
            ), failure_call
            assert database_projection(probe) == before

        integrity(probe)
        probe.evidence.update(
            active_profile={"user_id": "900001", "hp": 12345, "bath_remaining_minutes": 60},
            expired_profile={"user_id": "900002", "hp": 23456},
            alias="炼体状态",
            private=True,
            repeat_equal=True,
            cold_profile_not_created=True,
            unregistered_rejected=True,
            full_database_zero_writes=True,
            sqlite_authorizer_read_failure_reply=failed_read,
            read_failure_traceback="not authorized",
            integrity_check="ok",
        )
