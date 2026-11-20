"""Probe the read-only player statistics command through its live handler."""
import sqlite3

from tests.parameter_cases_team_leave import integrity
from tests.parameter_cases_team_view import database_projection


def register(case):
    @case(
        "statistics-view-empty",
        "修仙数据",
        "没有统计行",
        "隔离 player SQLite 中当前玩家无 statistics 记录",
        "返回空态且不新建/改写统计数据",
        group="statistics-view-parameters",
    )
    async def empty_statistics_remain_empty(probe):
        player_database = probe.context.paths.player_db
        assert probe.rows(
            player_database,
            "SELECT user_id FROM statistics WHERE user_id=?",
            ("900001",),
        ) == []
        before = database_projection(probe)

        reply = await probe.command("修仙数据", number=999881)

        assert "暂无统计数据" in reply, reply
        assert database_projection(probe) == before
        integrity(probe)
        probe.evidence.update(
            empty_state=True, full_database_zero_writes=True, integrity_check="ok"
        )

    @case(
        "statistics-view-values-and-isolation",
        "修仙数据",
        "排序/格式/本人/别名/重复只读查询",
        "隔离 player SQLite 中两名玩家有不同统计值",
        "展示本人的排序与格式化数值，别名重复查询不写库",
        group="statistics-view-parameters",
    )
    async def statistics_values_are_sorted_formatted_and_read_only(probe):
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.utils import number_to

        player = probe.managers[1]
        player.update_or_write_data(
            "900001", "statistics", "修仙签到", 17000, data_type="INTEGER"
        )
        player.update_or_write_data(
            "900001", "statistics", "闭关次数", 5, data_type="INTEGER"
        )
        player.update_or_write_data(
            "900002", "statistics", "修仙签到", 90000, data_type="INTEGER"
        )
        player.update_or_write_data(
            "900002", "statistics", "闭关次数", 23, data_type="INTEGER"
        )
        before = database_projection(probe)
        own_values = probe.rows(
            probe.context.paths.player_db,
            'SELECT "修仙签到","闭关次数" FROM statistics WHERE user_id=?',
            ("900001",),
        )
        other_values = probe.rows(
            probe.context.paths.player_db,
            'SELECT "修仙签到","闭关次数" FROM statistics WHERE user_id=?',
            ("900002",),
        )
        assert own_values == [(17000, 5)] and other_values == [(90000, 23)]

        primary = await probe.command("修仙数据", number=999882)
        expected = [
            f"- {key}：{number_to(value)}"
            for key, value in sorted((("修仙签到", 17000), ("闭关次数", 5)))
        ]
        assert "'name': '统计数据'" in primary, primary
        assert all(line in primary for line in expected), (expected, primary)
        assert primary.index(expected[0]) < primary.index(expected[1]), primary
        other_values = [
            f"- {key}：{number_to(value)}"
            for key, value in (("修仙签到", 90000), ("闭关次数", 23))
        ]
        assert all(line not in primary for line in other_values), primary
        assert database_projection(probe) == before

        aliases = ("统计数据", "我的数据", "查数据", "数据记录", "统计信息")
        for index, alias in enumerate(aliases, start=1):
            alias_reply = await probe.command(alias, number=999882 + index)
            assert alias_reply == primary, (alias, primary)
            assert database_projection(probe) == before
        integrity(probe)
        probe.evidence.update(
            own_values=own_values,
            other_values=other_values,
            sorted_fields=expected,
            aliases_match=aliases,
            repeat_zero_writes=True,
            full_database_zero_writes=True,
            integrity_check="ok",
        )

    @case(
        "statistics-view-read-error",
        "修仙数据",
        "已有统计数据的 SQLite 读取失败",
        "隔离 player SQLite 中存在实际统计行",
        "读取错误不得伪装成暂无统计数据或改写源数据",
        group="statistics-view-parameters",
        outcome="rejection",
    )
    async def read_error_is_not_empty(probe):
        player = probe.managers[1]
        player.update_or_write_data(
            "900001", "statistics", "修仙签到", 17, data_type="INTEGER"
        )
        before = database_projection(probe)
        before_value = probe.rows(
            probe.context.paths.player_db,
            'SELECT "修仙签到" FROM statistics WHERE user_id=?',
            ("900001",),
        )
        assert before_value == [(17,)], before_value

        def deny_statistics_read(action, table, _column, _database, _source):
            if action == sqlite3.SQLITE_READ and table == "statistics":
                return sqlite3.SQLITE_DENY
            return sqlite3.SQLITE_OK

        player.conn._raw.set_authorizer(deny_statistics_read)
        try:
            reply = await probe.command(
                "修仙数据", number=999880, timeout=30, allow_exception=True
            )
        finally:
            player.conn._raw.set_authorizer(None)

        assert "统计数据读取失败" in reply and "暂无统计数据" not in reply, reply
        assert database_projection(probe) == before
        assert probe.rows(
            probe.context.paths.player_db,
            'SELECT "修仙签到" FROM statistics WHERE user_id=?',
            ("900001",),
        ) == before_value
        integrity(probe)
        probe.evidence.update(
            actual_source_value=17,
            read_rejection="sqlite_authorizer_denied_statistics",
            false_empty_reply=False,
            full_database_zero_writes=True,
            integrity_check="ok",
        )
