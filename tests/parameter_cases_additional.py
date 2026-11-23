"""Additional bounded semantic probes for ordinary non-asset commands."""

import json
from unittest.mock import patch

from tests.parameter_cases_assets import balances, backpack


def database_projection(probe):
    from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork

    state = {}
    for database in (
        probe.context.paths.game_db,
        probe.context.paths.player_db,
        probe.context.paths.impart_db,
    ):
        with DatabaseUnitOfWork(database, read_only=True) as uow:
            tables = uow.query_all(
                "SELECT name,sql FROM sqlite_master "
                "WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
            )
            state[database.name] = [
                (
                    row["name"],
                    row["sql"],
                    uow.query_all(
                        'SELECT * FROM "' + row["name"].replace('"', '""') + '"'
                    ),
                )
                for row in tables
            ]
    return state


def integrity_check(probe):
    from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork

    for database in (
        probe.context.paths.game_db,
        probe.context.paths.player_db,
        probe.context.paths.impart_db,
    ):
        with DatabaseUnitOfWork(database, read_only=True) as uow:
            assert uow.query_one("PRAGMA integrity_check") == {"integrity_check": "ok"}


def register(case):
    for command, query, marker in (
        ("排行榜", "战力排行榜", "战力"),
        ("通天塔商店", "通天塔商店 1", "通天塔商店"),
        ("历练商店", "历练商店 1", "历练商店"),
    ):
        def build(command, query, marker):
            async def run(probe):
                before, stock = balances(probe), backpack(probe)
                text = await probe.command(command, query)
                assert marker in text and balances(probe) == before and backpack(probe) == stock, text
                probe.evidence.update(query=query, readonly=True)

            return run

        case(
            "additional-read-" + command,
            command,
            "真实查询参数",
            "注册玩家、真实配置和隔离迁移",
            "读取实际视图并保持资产不变",
            group="additional-player-parameters",
        )(build(command, query, marker))

    @case(
        "additional-read-我的体窍",
        "我的体窍",
        "合法天罡分组参数",
        "注册玩家和真实 108 穴配置，隔离迁移已完成",
        "列出天罡穴位和实际进度，不改变资产",
        group="additional-player-parameters",
    )
    async def read_qiaoxue(probe):
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_tianti as module
        from nonebot_plugin_xiuxian_2.features.tianti_training.repository import (
            TiantiProfileReader,
        )

        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_tianti import (
            tianti_data as module_data,
        )
        module_data._TIANTI_QIAOXUE_CACHE = None
        qiaoxue = module.get_qiaoxue_pool()
        first_tiangang = next(item for item in qiaoxue if item.get("group") == "天罡")
        second_tiangang = next(
            item for item in qiaoxue
            if item.get("group") == "天罡" and item["name"] != first_tiangang["name"]
        )
        first_disha = next(item for item in qiaoxue if item.get("group") == "地煞")
        profile = TiantiProfileReader(probe.context.paths.data).default_data()
        profile_sql = (
            "INSERT OR REPLACE INTO tianti_info "
            "(user_id,tianti_level,tianti_hp,opened_qiaoxue,"
            "opened_qiaoxue_detail,qiaoxue_stage_opened) VALUES(?,?,?,?,?,?)"
        )
        for user_id, opened in (
            ("900001", [first_tiangang["name"]]),
            ("900002", [first_disha["name"]]),
        ):
            probe.seed(
                probe.context.paths.player_db,
                profile_sql,
                (
                    user_id,
                    profile["tianti_level"],
                    "1000",
                    json.dumps(opened, ensure_ascii=False),
                    "[]",
                    "{}",
                ),
            )

        async def read(query=None, *, actor="900001", event_type="group"):
            before = database_projection(probe)
            text = await probe.command(
                "我的体窍", query or "我的体窍", actor=actor, event_type=event_type
            )
            assert database_projection(probe) == before, query or "我的体窍"
            probe.clear_cooldowns()
            return text

        overview = await read()
        assert "已开窍：1/108" in overview, overview
        assert "天罡：1/36" in overview and "地煞：0/72" in overview, overview

        tiangang = await read("我的体窍 天罡")
        assert "【天罡窍穴】" in tiangang and first_tiangang["name"] in tiangang, tiangang
        assert "✅" in tiangang and "进度：1/36" in tiangang, tiangang
        assert second_tiangang["name"] in tiangang and "⬜" in tiangang, tiangang

        disha = await read("我的体窍 地煞")
        assert "【地煞窍穴】" in disha and first_disha["name"] in disha, disha
        assert "⬜" in disha and "进度：0/72" in disha, disha

        opened = await read("我的体窍 " + first_tiangang["name"])
        assert "状态：已开启" in opened and "效果：" in opened, opened
        unopened = await read("我的体窍 " + second_tiangang["name"])
        assert "状态：未开启" in unopened and "效果：" in unopened, unopened
        unknown = await read("我的体窍 不存在的窍穴")
        assert "没有这个窍穴" in unknown, unknown

        other_opened = await read(
            "我的体窍 " + first_disha["name"], actor="900002", event_type="private"
        )
        other_unopened = await read(
            "我的体窍 " + first_tiangang["name"], actor="900002"
        )
        other_disha = await read("我的体窍 地煞", actor="900002")
        assert "状态：已开启" in other_opened, other_opened
        assert "状态：未开启" in other_unopened, other_unopened
        assert "✅" in other_disha and "进度：1/72" in other_disha, other_disha
        unregistered = await read("我的体窍", actor="900099")
        assert "修仙界没有道友的信息" in unregistered, unregistered
        empty_profile = await read("我的体窍", actor="900003")
        assert "已开窍：0/108" in empty_profile, empty_profile
        assert "天罡：0/36" in empty_profile and "地煞：0/72" in empty_profile, empty_profile

        integrity_check(probe)
        probe.evidence.update(
            overview=True,
            groups={"天罡": "1/36", "地煞": "0/72"},
            opened_and_unopened=True,
            player_isolation=True,
            private=True,
            unknown=True,
            unregistered=True,
            database_zero_writes=True,
            integrity_check="ok",
        )

    def qiaoxue_config_error_case(mode):
        async def run(probe):
            from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_tianti import tianti_data

            config_dir = probe.context.paths.data / "diagnostic-qiaoxue-config" / mode
            config_dir.mkdir(parents=True, exist_ok=True)
            config_name = "炼体窍穴.config" if mode == "unreadable" else "炼体窍穴.json"
            config_path = config_dir / config_name
            if mode == "invalid-json":
                original = b'{"\xe7\xaa\x8d\xe7\xa9\xb4": ['
                config_path.write_bytes(original)
                reason = "qiaoxue_config_invalid"
            elif mode == "invalid-shape":
                original = (
                    b'{"\xe7\xaa\x8d\xe7\xa9\xb4":[{"name":"bad",'
                    b'"group":[],"effect_type":"base_per_min_ratio",'
                    b'"effect_value":0.1}]}'
                )
                config_path.write_bytes(original)
                reason = "qiaoxue_config_invalid"
            elif mode == "unreadable":
                config_path.mkdir()
                original = None
                reason = "qiaoxue_config_unreadable"
            else:
                original = None
                config_path.unlink(missing_ok=True)
                reason = "qiaoxue_config_missing"

            before = database_projection(probe)
            with (
                patch.object(tianti_data, "TIANTI_QIAOXUE_FILE", config_path),
                patch.object(tianti_data, "_TIANTI_QIAOXUE_CACHE", None),
            ):
                text = await probe.command("我的体窍", "我的体窍 天罡")
            same_source = (
                config_path.exists() and config_path.read_bytes() == original
                if original is not None
                else (
                    config_path.is_dir() and not any(config_path.iterdir())
                    if mode == "unreadable"
                    else not config_path.exists()
                )
            )
            backups = list(config_dir.glob(config_name + ".invalid.*.bak"))
            probe.evidence.update(
                mode=mode,
                reply=text,
                source_preserved=same_source,
                backup_count=len(backups),
                zero_database_writes=database_projection(probe) == before,
            )
            assert reason in text and "未执行查询" in text, text
            assert same_source and not backups, (config_path, same_source, backups)
            assert database_projection(probe) == before
            integrity_check(probe)
            probe.clear_cooldowns()

        return run

    for mode in ("invalid-json", "invalid-shape", "unreadable", "missing"):
        case(
            "additional-read-我的体窍-config-" + mode,
            "我的体窍",
            "隔离配置异常",
            "真实 matcher，隔离窍穴配置文件与三库快照",
            "保留配置源，不显示伪空列表，返回具体配置原因",
            group="additional-player-parameters",
        )(qiaoxue_config_error_case(mode))

    @case(
        "additional-read-修仙日志",
        "修仙日志",
        "yymmdd 日期与页码",
        "注册玩家并在隔离日志目录写入一条真实日志",
        "按日期和页码读取日志且资产不变",
        group="additional-player-parameters",
    )
    async def read_logs(probe):
        from datetime import datetime
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.utils import log_message

        before, stock = balances(probe), backpack(probe)
        date = datetime.now().strftime("%y%m%d")
        log_message("900001", "参数路径诊断日志")
        query = f"修仙日志 {date} 1"
        text = await probe.command("修仙日志", query)
        assert "修仙日志" in text and "参数路径诊断日志" in text, text
        assert balances(probe) == before and backpack(probe) == stock
        probe.evidence.update(query=query, readonly=True, date_format="yymmdd")

    for command, query, markers in (
        ("拜师", "不存在的诊断对象", ("未找到", "不存在", "对象")),
        ("绑定道侣", "不存在的诊断对象", ("未找到", "不存在", "道侣")),
        ("师徒传功", "不存在的诊断对象", ("未找到", "不存在", "对象")),
        ("拜访道友", "不存在的诊断对象", ("未找到", "不存在", "洞府")),
        ("洞府种植", "青灵草种", ("尚未", "未识别", "种子")),
        ("洞府收获", "1", ("尚未", "灵田", "收获")),
        ("洞府施肥", "1", ("尚未", "灵田", "肥料")),
        ("洞府催熟", "1", ("尚未", "灵田", "催熟")),
        ("通天塔兑换", "999999 1", ("不存在", "物品ID", "商品", "兑换", "积分")),
        ("历练兑换", "999999 1", ("不存在", "商品", "兑换", "历练")),
        ("合成", "不存在的合成物 1", ("未找到", "合成")),
        ("领取任务奖励", "每日", ("没有", "未完成", "领取", "任务")),
        ("虚神界对决", "不存在的对手", ("不存在", "对手", "无法", "修士")),
        ("炼丹", "不存在的丹方 1", ("不存在", "配方", "丹方", "材料", "背包空空")),
    ):
        def build(command, query, markers):
            async def run(probe):
                before, stock = balances(probe), backpack(probe)
                text = await probe.command(command, command + " " + query)
                assert balances(probe) == before and backpack(probe) == stock, text
                assert any(marker in text for marker in markers), text
                probe.evidence.update(query=query, assets_unchanged=True)

            return run

        case(
            "additional-reject-" + command,
            command,
            "具体对象/编号参数",
            "注册玩家和隔离迁移，使用不存在或不满足前置的真实语法",
            "具体业务拒绝且资产不变",
            group="additional-player-parameters",
            outcome="rejection",
        )(build(command, query, markers))
