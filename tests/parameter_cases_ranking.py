"""Real read-only ranking command probes against isolated migrated data."""
from __future__ import annotations

import sqlite3


def database_projection(probe):
    result = {}
    for database in (
        probe.context.paths.game_db,
        probe.context.paths.player_db,
        probe.context.paths.impart_db,
    ):
        with sqlite3.connect(database) as connection:
            tables = connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name NOT LIKE 'sqlite_%' ORDER BY name"
            ).fetchall()
            rows = []
            for (table,) in tables:
                escaped = table.replace('"', '""')
                columns = [
                    row[1]
                    for row in connection.execute(f'PRAGMA table_info("{escaped}")')
                ]
                selected = ",".join(f'"{column.replace(chr(34), chr(34) * 2)}"' for column in columns)
                values = connection.execute(
                    f'SELECT {selected} FROM "{escaped}"'
                ).fetchall() if columns else []
                rows.append((table, tuple(sorted(values, key=repr))))
            result[database.name] = tuple(rows)
    return result


def integrity(probe):
    for manager in (probe.managers[0], probe.managers[1], probe.managers[3]):
        assert manager.conn.execute("PRAGMA integrity_check").fetchone() == ("ok",)


def register(case):
    @case(
        "ranking-five-categories-top50-empty-readonly",
        "排行榜",
        "五类榜单、已注册别名、top 50、空榜、隔离数据与重复读取",
        "隔离 game SQLite 建立 55 名可排序玩家与 55 个宗门",
        "五类均按实际数据显示前 50 名；空榜仅显示标题；查询不改三库",
        group="ranking-parameters",
    )
    async def five_rankings_are_read_only(probe):
        game = probe.context.paths.game_db
        manager = probe.managers[0]
        connection = manager.conn
        for index in range(55):
            suffix = f"{index:02d}"
            user_id = f"rank-{suffix}"
            connection.execute(
                "INSERT INTO user_xiuxian(user_id,user_name,level,exp,stone,power,root_level) "
                "VALUES(?,?,?,?,?,?,?)",
                (
                    user_id,
                    f"榜单玩家{suffix}",
                    "江湖好手",
                    100_000 + index,
                    2_000_000 + index,
                    300_000 + index,
                    index,
                ),
            )
            connection.execute(
                "INSERT INTO sects(sect_id,sect_name,sect_owner,sect_scale) "
                "VALUES(?,?,?,?)",
                (880_000 + index, f"榜单宗门{suffix}", user_id, 400_000 + index),
            )
        connection.commit()
        manager._clear_read_cache()
        before = database_projection(probe)
        cases = (
            ("排行榜", "排行榜", "位面境界排行榜", "榜单玩家54", "榜单玩家05", "榜单玩家04"),
            ("修仙排行榜", "修仙排行榜", "位面境界排行榜", "榜单玩家54", "榜单玩家05", "榜单玩家04"),
            ("境界排行榜", "境界排行榜", "位面境界排行榜", "榜单玩家54", "榜单玩家05", "榜单玩家04"),
            ("灵石排行榜", "灵石排行榜", "位面灵石排行榜", "榜单玩家54", "榜单玩家05", "榜单玩家04"),
            ("战力排行榜", "战力排行榜", "位面战力排行榜", "榜单玩家54", "榜单玩家05", "榜单玩家04"),
            ("宗门排行榜", "宗门排行榜", "位面宗门建设排行榜", "榜单宗门54", "榜单宗门05", "榜单宗门04"),
            ("轮回排行榜", "轮回排行榜", "轮回排行榜", "榜单玩家54", "榜单玩家05", "榜单玩家04"),
        )
        replies = {}
        for index, (command, text, heading, first, fiftieth, omitted) in enumerate(cases):
            reply = await probe.command(command, text, number=979_000 + index)
            assert heading in reply, (command, reply)
            assert reply.count("位：") == 50, (command, reply.count("位："), reply)
            assert first in reply and fiftieth in reply and omitted not in reply, (command, reply)
            assert database_projection(probe) == before, command
            assert await probe.command(command, text, number=979_100 + index) == reply
            assert database_projection(probe) == before, command
            replies[command] = reply

        assert replies["排行榜"] == replies["修仙排行榜"] == replies["境界排行榜"]

        connection.execute("UPDATE user_xiuxian SET user_name=NULL")
        connection.execute("UPDATE sects SET sect_owner=NULL")
        connection.commit()
        manager._clear_read_cache()
        empty_before = database_projection(probe)
        empty_cases = cases[:1] + cases[3:]
        for index, (command, text, heading, *_rest) in enumerate(empty_cases):
            reply = await probe.command(command, text, number=979_200 + index)
            assert heading in reply and "第1位：" not in reply, (command, reply)
            assert database_projection(probe) == empty_before, command

        integrity(probe)
        probe.evidence.update(
            categories=5,
            named_rows_per_category=55,
            entries_returned_per_category=50,
            aliases=("修仙排行榜", "灵石排行榜", "战力排行榜", "宗门排行榜", "轮回排行榜"),
            empty_rankings=True,
            duplicate_queries=True,
            full_game_player_impart_zero_writes=True,
            integrity_check="ok",
        )
