"""Registered map browser queries through isolated SQLite and message transport."""

import json
import sqlite3
from unittest.mock import patch

from tests.parameter_cases_map_help import _database_projection
from tests.parameter_cases_map_position import _position, _seed_status


def _plain(text):
    return text.replace("\r", "").replace("[CQ:at,qq=900001]", "")


def register(case):
    @case("map-info-catalog-readonly", "地图", "实际界名/天名/旧天别名", "已注册玩家且保存规范位置",
          "展示配置节点与当前标记；查询期间不改变业务数据", group="map-info")
    async def catalog(probe):
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_map as module

        data = module._load_map_data()
        realm = module._all_realms(data)[0]
        heaven = module._heaven_names(data, realm)[0]
        node = module._nodes(data, realm, heaven)[0]
        _seed_status(probe, realm=realm, heaven=heaven, node_id=node["id"])
        before = _database_projection(probe)

        for target_realm in module._all_realms(data):
            text = _plain(await probe.command("地图", "地图 " + target_realm, timeout=30))
            expected = module._get_realm_heaven_order(data, target_realm)
            assert f"【地图 - {target_realm}】" in text and "—— 天层列表 ——" in text, text
            assert all(name in text for name in expected), text
            marked = [name for name in expected if f"📍 {name}" in text]
            assert marked == ([heaven] if target_realm == realm else []), (target_realm, marked, text)

        for target_realm in module._all_realms(data):
            for target_heaven in module._heaven_names(data, target_realm):
                text = _plain(await probe.command(
                    "地图", "地图 " + target_heaven, timeout=30,
                    event_type="private" if target_heaven == heaven else "group",
                ))
                nodes = module._nodes(data, target_realm, target_heaven)
                assert f"【地图 - {target_realm}·{target_heaven}】" in text, text
                assert all(f"{row['name']}（{row['type']}）" in text for row in nodes), text
                marked = [row["id"] for row in nodes if f"📍 {row['name']}（{row['type']}）" in text]
                expected = [node["id"]] if target_realm == realm and target_heaven == heaven else []
                assert marked == expected, (target_realm, target_heaven, marked, text)

        aliases = data["meta"]["legacy_heaven_aliases"]
        alias_realm, (alias, canonical) = next(
            (name, next(iter(entries.items()))) for name, entries in aliases.items() if entries
        )
        text = _plain(await probe.command("地图", "地图 " + alias, timeout=30))
        assert f"【地图 - {alias_realm}·{canonical}】" in text, text
        after = _database_projection(probe)
        for key in ("game_db", "player_db", "trade_db", "impart_db"):
            assert before[key] == after[key], key
        probe.evidence.update(realms=len(module._all_realms(data)), heavens=sum(
            len(module._heaven_names(data, realm)) for realm in module._all_realms(data)
        ), alias=alias, business_databases_unchanged=True)

    @case("map-info-default-view", "地图", "无参数", "已注册玩家有规范地图位置",
          "展示当前天全节点和当前位置，不写入业务库", group="map-info")
    async def default_view(probe):
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_map as module

        data = module._load_map_data()
        realm = module._all_realms(data)[0]
        heaven = module._heaven_names(data, realm)[0]
        node = module._nodes(data, realm, heaven)[0]
        _seed_status(probe, realm=realm, heaven=heaven, node_id=node["id"])
        before = _database_projection(probe)
        text = _plain(await probe.command("地图", "地图", timeout=30, event_type="private"))
        assert f"当前位置：{realm}·{heaven}·{node['name']}（{node['type']}）" in text, text
        assert all(row["name"] in text for row in module._nodes(data, realm, heaven)), text
        assert _position(probe) == [(realm, heaven, node["id"], json.dumps([node["id"]], ensure_ascii=False))]
        after = _database_projection(probe)
        for key in ("game_db", "player_db", "trade_db", "impart_db"):
            assert before[key] == after[key], key

    @case("map-info-invalid-query-no-write", "地图", "未识别的界/天名", "注册玩家缺少 map_status",
          "拒绝未知查询且不初始化或写入地图位置", group="map-info", outcome="rejection")
    async def invalid_query(probe):
        before = _database_projection(probe)
        text = _plain(await probe.command("地图", "地图 不存在的界名", timeout=30))
        assert "未识别参数【不存在的界名】" in text, text
        assert _position(probe) == []
        after = _database_projection(probe)
        for key in ("game_db", "player_db", "trade_db", "impart_db"):
            assert before[key] == after[key], key

    @case("map-info-read-recovery", "地图", "有效界查询时拒绝读取 map_status", "隔离 UOW 对 map_status.realm 安装 SQLITE_DENY",
          "真实读取异常给出可诊断回复且不伪装成功，解除后同消息可查", group="map-info", outcome="recovery")
    async def read_recovery(probe):
        from nonebot_plugin_xiuxian_2.features.map import repository
        from nonebot_plugin_xiuxian_2.infrastructure.database.uow import DatabaseUnitOfWork
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_map as module

        data = module._load_map_data()
        realm = module._all_realms(data)[0]
        heaven = module._heaven_names(data, realm)[0]
        node = module._nodes(data, realm, heaven)[0]
        _seed_status(probe, realm=realm, heaven=heaven, node_id=node["id"])
        database = probe.context.paths.player_db.resolve()
        state = {"deny": True}

        class AuthorizerUnitOfWork(DatabaseUnitOfWork):
            def __enter__(self):
                super().__enter__()
                if self.database.resolve() == database and state["deny"]:
                    self.connection.set_authorizer(
                        lambda action, table, column, *_: sqlite3.SQLITE_DENY
                        if action == sqlite3.SQLITE_READ and table == "map_status"
                        else sqlite3.SQLITE_OK
                    )
                return self

        with patch.object(repository, "DatabaseUnitOfWork", AuthorizerUnitOfWork):
            failed = await probe.command("地图", "地图 " + realm, number=975080, timeout=30,
                                         allow_exception=True)
            call = probe.calls[-1]
            state["deny"] = False
            recovered = _plain(await probe.command("地图", "地图 " + realm, number=975080, timeout=30))
        assert "地图处理提示" in failed and "定位号" in failed and "【地图 -" not in failed, failed
        assert len(call["errors"]) == 1 and "map_status.realm" in call["errors"][0], call
        assert f"【地图 - {realm}】" in recovered and heaven in recovered, recovered
        assert _position(probe) == [(realm, heaven, node["id"], json.dumps([node["id"]], ensure_ascii=False))]
