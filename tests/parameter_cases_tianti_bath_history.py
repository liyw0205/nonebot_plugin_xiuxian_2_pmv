"""Real matcher probes for the read-only 我的药浴记录 view."""
from __future__ import annotations

import inspect
import json
import sqlite3
import sys
from datetime import datetime, timedelta
from unittest.mock import patch

from tests.parameter_cases_team_leave import integrity
from tests.parameter_cases_team_view import database_projection


def register(case):
    @case(
        "tianti-bath-history-data",
        "我的药浴记录",
        "本人记录/别名/冷态/隔离",
        "隔离 game SQLite 中写入真实结构的药浴回执",
        "汇总本人近七天数据并保持只读，冷态与未注册各自正确",
        group="tianti-bath-history",
    )
    async def bath_history_data(probe):
        game_database = probe.context.paths.game_db

        def insert(operation_id, user_id, name, amount, effect, created_at):
            plan = [{"item_id": 1, "name": name, "amount": amount}]
            result = {
                "consumed": plan,
                "effect": effect,
                "bath_name": f"{name}药浴",
                "end_time": "2026-11-21 18:00:00",
                "settlement": {},
            }
            probe.seed(
                game_database,
                "INSERT INTO tianti_medicine_bath_operations "
                "(operation_id,user_id,request_json,result_json,created_at) VALUES(?,?,?,?,?)",
                (
                    operation_id,
                    user_id,
                    json.dumps({"plan": plan, "effect": effect}, ensure_ascii=False),
                    json.dumps(result, ensure_ascii=False),
                    created_at,
                ),
            )

        now = datetime(2026, 11, 21, 12, 0, 0)
        total_units = 0
        for index in range(12):
            amount = index + 1
            total_units += amount
            insert(
                f"bath-own-{index}",
                "900001",
                "恒心草" if index % 2 == 0 else "天青花",
                amount,
                1.2 + index / 20,
                (now - timedelta(hours=index + 1)).strftime("%Y-%m-%d %H:%M:%S"),
            )
        insert(
            "bath-own-future",
            "900001",
            "夜交藤",
            30,
            2.0,
            (now + timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S"),
        )
        total_units += 30
        insert(
            "bath-own-old",
            "900001",
            "过期药材",
            90,
            1.9,
            (now - timedelta(days=8)).strftime("%Y-%m-%d %H:%M:%S"),
        )
        insert(
            "bath-other",
            "900002",
            "他人药材",
            99,
            2.0,
            (now - timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S"),
        )
        insert(
            "bath-second-player",
            "900002",
            "玉髓芝",
            2,
            1.6,
            (now - timedelta(hours=2)).strftime("%Y-%m-%d %H:%M:%S"),
        )

        matcher_entry = next(
            entry for entry in probe.entries if entry["command"] == "我的药浴记录"
        )
        handler = inspect.unwrap(matcher_entry["matcher"].handlers[0].call)
        module = sys.modules[handler.__module__]

        class FrozenClock:
            @staticmethod
            def now():
                return now

        from nonebot_plugin_xiuxian_2.features.tianti_training.bath_history import (
            TiantiBathHistoryApplication,
        )

        application = TiantiBathHistoryApplication(game_database, clock=FrozenClock())
        before = database_projection(probe)
        with patch.object(module, "tianti_bath_history_application", application):
            primary = await probe.command("我的药浴记录", number=979901)
            assert "开启次数：13 次" in primary, primary
            assert f"累计消耗药材：{total_units} 份（3 种）" in primary, primary
            assert "最高结算效果：200%" in primary, primary
            assert "夜交藤药浴" in primary and "夜交藤×30" in primary, primary
            assert primary.count("- ") == 10, primary
            assert "过期药材" not in primary and "他人药材" not in primary, primary
            assert database_projection(probe) == before

            probe.clear_cooldowns()
            repeated = await probe.command("我的药浴记录", number=979901)
            assert repeated == primary, (primary, repeated)
            assert database_projection(probe) == before

            probe.clear_cooldowns()
            alias = await probe.command(
                "药浴记录", "药浴记录", actor="900002", event_type="private", number=979902
            )
            assert "开启次数：2 次" in alias and "累计消耗药材：101 份（2 种）" in alias, alias
            assert "他人药材" in alias and "玉髓芝药浴" in alias, alias
            assert database_projection(probe) == before

            probe.clear_cooldowns()
            cold = await probe.command("我的药浴记录", actor="900003", number=979903)
            assert cold.split("\r", 1)[-1] == "近7天没有药浴记录。", cold
            assert database_projection(probe) == before

            probe.clear_cooldowns()
            unregistered = await probe.command(
                "我的药浴记录", actor="900099", number=979904
            )
            assert "修仙界没有道友的信息" in unregistered, unregistered
            assert database_projection(probe) == before

        integrity(probe)
        probe.evidence.update(
            own_records_in_window=13,
            old_and_other_records_excluded=True,
            future_timestamp_included=True,
            latest_ten_ordered=True,
            repeated_zero_writes=True,
            private_alias_player_isolated=True,
            cold_empty=True,
            unregistered_rejected=True,
            all_database_projections_unchanged=True,
            integrity_check="ok",
        )

    @case(
        "tianti-bath-history-read-failure",
        "我的药浴记录",
        "有效回执存在时拒绝 SQLite 读取",
        "隔离 game SQLite 中已有有效药浴回执",
        "拒绝读取不得伪装为空历史，回复定位号且日志保留原始数据库错误",
        group="tianti-bath-history",
        outcome="rejection",
    )
    async def bath_history_read_failure(probe):
        game_database = probe.context.paths.game_db
        probe.seed(
            game_database,
            "INSERT INTO tianti_medicine_bath_operations "
            "(operation_id,user_id,request_json,result_json,created_at) VALUES(?,?,?,?,?)",
            (
                "bath-authorizer-denied",
                "900001",
                json.dumps({"plan": [{"name": "恒心草", "amount": 5}], "effect": 1.5}),
                json.dumps({
                    "consumed": [{"name": "恒心草", "amount": 5}],
                    "effect": 1.5,
                    "bath_name": "有效药浴",
                    "end_time": "2026-11-21 18:00:00",
                }, ensure_ascii=False),
                "2026-11-21 11:00:00",
            ),
        )
        before = database_projection(probe)
        from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork

        original_enter = DatabaseUnitOfWork.__enter__

        def enter_with_denied_receipt_read(uow):
            result = original_enter(uow)
            if uow.database.resolve() == game_database.resolve() and uow.read_only:
                def deny_receipt_read(action, table, _column, _database, _source):
                    if action == sqlite3.SQLITE_READ and table == "tianti_medicine_bath_operations":
                        return sqlite3.SQLITE_DENY
                    return sqlite3.SQLITE_OK

                uow.connection.set_authorizer(deny_receipt_read)
            return result

        probe.clear_cooldowns()
        with patch.object(DatabaseUnitOfWork, "__enter__", enter_with_denied_receipt_read):
            text = await probe.command(
                "我的药浴记录", number=979905, allow_exception=True
            )
        failure_call = probe.calls[-1]
        assert "我的药浴记录处理提示：" in text and "定位号：" in text, text
        assert "近7天没有药浴记录" not in text, text
        assert any(
            "sqlite3.DatabaseError: access to tianti_medicine_bath_operations.operation_id is prohibited"
            in error
            for error in failure_call["errors"]
        ), failure_call
        assert any(
            "DatabaseError('access to tianti_medicine_bath_operations.operation_id is prohibited')"
            in log["exception"]
            for log in failure_call["logs"]
        ), failure_call
        assert database_projection(probe) == before
        integrity(probe)
        probe.evidence.update(
            valid_record_exists=True,
            failure_reply_with_reference=text,
            original_database_error_logged=True,
            no_false_empty_history=True,
            full_database_zero_writes=True,
            integrity_check="ok",
        )
