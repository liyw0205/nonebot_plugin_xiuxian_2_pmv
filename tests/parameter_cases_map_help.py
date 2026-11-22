"""Real matcher coverage for the map help catalog."""

import html
import re


def _database_projection(probe):
    from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork

    result = {}
    for spec in probe.context.database.specs():
        with DatabaseUnitOfWork(spec.path, read_only=True) as uow:
            tables = uow.query_all(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name NOT LIKE 'sqlite_%' ORDER BY name"
            )
            result[spec.key] = {
                row["name"]: [
                    tuple(record.values())
                    for record in sorted(
                        uow.query_all(
                            'SELECT * FROM "' + str(row["name"]).replace('"', '""') + '"'
                        ),
                        key=lambda record: repr(tuple(record.values())),
                    )
                ]
                for row in tables
            }
            assert uow.query_one("PRAGMA integrity_check")["integrity_check"] == "ok"
    return result


USAGES = (
    "地图 [界名/天名]",
    "前往 节点名/天名/界名",
    "论道切磋 [道号]",
    "论道查看 [道号]",
    "购买种子 种子名 数量",
    "开始探索 [分钟]",
    "节点探索 [分钟]",
)


def register(case):
    @case(
        "map-help-registered-usage-readonly",
        "地图帮助",
        "群聊、私聊与重复查询",
        "运行期地图 matcher 与五库隔离数据库快照",
        "展示实际注册的命令/别名及参数，业务库不写入且仅记录入出站消息",
        group="map-help",
    )
    async def help_catalog(probe):
        from nonebot_plugin_xiuxian_2.features.map.manifest import FEATURE
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_map

        runtime_commands = {
            name
            for entry in probe.entries
            if entry["module"] == xiuxian_map.__name__
            for name in (entry["command"], *entry["aliases"])
        }
        manifest_commands = {
            name
            for command in FEATURE.commands
            for name in (command.name, *command.aliases)
        }
        assert runtime_commands == manifest_commands, {
            "runtime_only": sorted(runtime_commands - manifest_commands),
            "manifest_only": sorted(manifest_commands - runtime_commands),
        }

        before = _database_projection(probe)
        replies = []
        for index, event_type in enumerate(("group", "private", "group")):
            reply = await probe.command(
                "地图帮助",
                number=974600 + index,
                event_type=event_type,
                timeout=30,
            )
            replies.append(re.sub(r"^\[CQ:at,qq=\d+\]", "", html.unescape(reply)).replace("\r", ""))

        after = _database_projection(probe)
        assert replies.count(replies[0]) == len(replies), "help varies by chat or repeat"
        business_databases_unchanged = all(
            before[key] == after[key]
            for key in ("game_db", "player_db", "trade_db", "impart_db")
        )
        message_tables = after["message_db"]
        message_delta = {
            table: len(message_tables.get(table, ())) - len(before["message_db"].get(table, ()))
            for table in set(before["message_db"]) | set(message_tables)
            if before["message_db"].get(table) != message_tables.get(table)
        }
        expected_message_delta = {"messages": 6, "user_nicknames": 1}
        catalog = replies[0]
        missing = [
            name for name in sorted(runtime_commands | {"洞府帮助"})
            if not re.search(rf"(?<!\w){re.escape(name)}(?!\w)", catalog)
        ]
        missing_usage = [form for form in USAGES if form not in catalog]
        probe.evidence.update(
            catalog=catalog,
            missing=missing,
            missing_usage=missing_usage,
            business_databases_unchanged=business_databases_unchanged,
            message_delta=message_delta,
        )
        assert business_databases_unchanged, "map help queries changed gameplay databases"
        assert message_delta == expected_message_delta, {
            "unexpected message database writes": message_delta,
        }
        assert not missing and not missing_usage, {"commands": missing, "usage": missing_usage}
