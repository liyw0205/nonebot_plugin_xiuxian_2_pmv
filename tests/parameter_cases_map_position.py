"""Registered `我的位置` reads and status initialization through isolated SQLite."""

import json
import sqlite3
from unittest.mock import patch

from tests.parameter_cases_map_help import _database_projection


def _seed_status(probe, *, realm, heaven, node_id, visited=None, actor="900001"):
    from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_map as module

    module.map_application.save_status(actor, realm, heaven, node_id, visited or [node_id])


def _position(probe, actor="900001"):
    return probe.rows(
        probe.context.paths.player_db,
        "SELECT realm,heaven,node_id,visited_nodes FROM map_status WHERE user_id=?",
        (actor,),
    )


def _choice(*, realm, heaven, node):
    remaining = iter((realm, heaven, node))

    def choose(options):
        selected = next(remaining)
        assert selected in options, (selected, options)
        return selected

    return choose


def register(case):
    @case("map-position-canonical-display", "我的位置", "群聊/私聊读取已保存位置",
          "玩家有随包地图中的规范位置", "精确展示实际节点；game/player/trade/impart业务数据不变",
          group="map-position")
    async def canonical_display(probe):
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_map as module

        data = module._load_map_data()
        realm = module._all_realms(data)[0]
        heaven = module._heaven_names(data, realm)[0]
        node = module._nodes(data, realm, heaven)[0]
        _seed_status(probe, realm=realm, heaven=heaven, node_id=node["id"], visited=[node["id"]])
        before = _database_projection(probe)
        replies = []
        for number, event_type in ((975001, "group"), (975002, "private")):
            replies.append(await probe.command(
                "我的位置", "我的位置", number=number, event_type=event_type, timeout=30
            ))
        assert all(f"{realm}·{heaven}·{node['name']}（{node['type']}）" in reply for reply in replies), replies
        after = _database_projection(probe)
        for key in ("game_db", "player_db", "trade_db", "impart_db"):
            assert after[key] == before[key], key

    @case("map-position-unregistered-no-write", "我的位置", "未注册玩家查询",
          "actor没有user_xiuxian或map_status", "明确注册前置回复且隔离业务库零写",
          group="map-position", outcome="rejection")
    async def unregistered(probe):
        before = _database_projection(probe)
        reply = await probe.command("我的位置", "我的位置", actor="900099", number=975003, timeout=30)
        assert "修仙界没有道友的信息，请输入【我要修仙】加入！" in reply, reply
        assert _position(probe, "900099") == []
        after = _database_projection(probe)
        for key in ("game_db", "player_db", "trade_db", "impart_db"):
            assert after[key] == before[key], key

    @case("map-position-initialize-and-normalize", "我的位置", "缺失状态初始化与旧天堂别名规范化",
          "随机源固定，第二步使用地图配置中的青石天旧别名", "初始化/规范化后真实map_status与回复一致",
          group="map-position")
    async def initialize_and_normalize(probe):
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_map as module

        data = module._load_map_data()
        realm = module._all_realms(data)[0]
        heaven = module._heaven_names(data, realm)[0]
        node = module._nodes(data, realm, heaven)[0]
        with patch.object(module.runtime_random, "choice", side_effect=_choice(
            realm=realm, heaven=heaven, node=node
        )):
            reply = await probe.command("我的位置", "我的位置", number=975004, timeout=30)
        assert f"{realm}·{heaven}·{node['name']}（{node['type']}）" in reply, reply
        assert _position(probe) == [(realm, heaven, node["id"], json.dumps([node["id"]], ensure_ascii=False))]

        aliases = data["meta"]["legacy_heaven_aliases"]
        alias_realm, (legacy_heaven, canonical_heaven) = next(
            (name, next(iter(entries.items()))) for name, entries in aliases.items() if entries
        )
        canonical_node = module._nodes(data, alias_realm, canonical_heaven)[0]
        visited = [canonical_node["id"], "historic-node"]
        probe.seed(probe.context.paths.player_db,
                   "UPDATE map_status SET realm=?,heaven=?,node_id=?,visited_nodes=? WHERE user_id='900001'",
                   (alias_realm, legacy_heaven, canonical_node["id"], json.dumps(visited, ensure_ascii=False)))
        reply = await probe.command("我的位置", "我的位置", number=975005, event_type="private", timeout=30)
        assert f"{alias_realm}·{canonical_heaven}·{canonical_node['name']}（{canonical_node['type']}）" in reply, reply
        assert _position(probe) == [(alias_realm, canonical_heaven, canonical_node["id"],
                                     json.dumps(visited, ensure_ascii=False))]

    @case("map-position-write-recovery", "我的位置", "初始化状态INSERT拒绝/改写/ABORT后原消息恢复",
          "玩家已注册且没有map_status，触发器只作用于隔离player SQLite",
          "不伪报已保存位置；具体拒绝/定位号、无半写并在故障解除后恢复",
          group="map-position", outcome="recovery")
    async def write_recovery(probe):
        from nonebot_plugin_xiuxian_2.core.player_reasons import reason_text
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_map as module

        data = module._load_map_data()
        realm = module._all_realms(data)[0]
        heaven = module._heaven_names(data, realm)[0]
        node = module._nodes(data, realm, heaven)[0]
        before = _database_projection(probe)
        database = probe.context.paths.player_db
        faults = (
            ("ignore", "BEFORE INSERT ON map_status BEGIN SELECT RAISE(IGNORE); END",
             "map_status_write_rejected"),
            ("rewrite", "AFTER INSERT ON map_status BEGIN UPDATE map_status SET node_id='rewritten' WHERE user_id=NEW.user_id; END",
             "map_status_projection_mismatch"),
        )
        for index, (label, trigger, reason) in enumerate(faults):
            probe.seed(database, "DELETE FROM map_status WHERE user_id='900001'")
            probe.seed(database, "CREATE TRIGGER map_position_fault " + trigger)
            with patch.object(module.runtime_random, "choice", side_effect=_choice(
                realm=realm, heaven=heaven, node=node
            )):
                reply = await probe.command("我的位置", "我的位置", number=975010 + index, timeout=30,
                                            allow_exception=True)
            actual = _position(probe)
            assert "当前位置" not in reply and actual == [], (reply, actual)
            assert reason_text(reason) and reason_text(reason) in reply and "地图位置" in reply, reply
            probe.seed(database, "DROP TRIGGER map_position_fault")
            with patch.object(module.runtime_random, "choice", side_effect=_choice(
                realm=realm, heaven=heaven, node=node
            )):
                recovered = await probe.command("我的位置", "我的位置", number=975010 + index, timeout=30)
            assert f"{realm}·{heaven}·{node['name']}（{node['type']}）" in recovered, recovered
            assert _position(probe) == [(realm, heaven, node["id"],
                                         json.dumps([node["id"]], ensure_ascii=False))]
            probe.seed(database, "DELETE FROM map_status WHERE user_id='900001'")
        assert _database_projection(probe)["player_db"] == before["player_db"]

        probe.seed(database, "CREATE TRIGGER map_position_abort BEFORE INSERT ON map_status "
                   "BEGIN SELECT RAISE(ABORT,'map position blocked'); END")
        with patch.object(module.runtime_random, "choice", side_effect=_choice(
            realm=realm, heaven=heaven, node=node
        )):
            reply = await probe.command("我的位置", "我的位置", number=975012, timeout=30, allow_exception=True)
        assert "定位号" in reply and "map position blocked" not in reply, reply
        assert _position(probe) == []
        probe.seed(database, "DROP TRIGGER map_position_abort")
        with patch.object(module.runtime_random, "choice", side_effect=_choice(
            realm=realm, heaven=heaven, node=node
        )):
            recovered = await probe.command("我的位置", "我的位置", number=975012, timeout=30)
        assert f"{realm}·{heaven}·{node['name']}（{node['type']}）" in recovered, recovered
        assert _position(probe) == [(realm, heaven, node["id"], json.dumps([node["id"]], ensure_ascii=False))]

    @case("map-position-repair-recovery", "我的位置", "旧天别名和失效节点同时规范化时末步被拒绝",
          "旧天及节点均失效，触发器只在写入最终节点时中止",
          "领域拒绝或原异常可诊断，完整旧位置回滚，原事件可重试",
          group="map-position", outcome="recovery")
    async def repair_recovery(probe):
        from nonebot_plugin_xiuxian_2.core.player_reasons import reason_text
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_map as module

        data = module._load_map_data()
        realm, (legacy, heaven) = next((realm, next(iter(aliases.items())))
                                     for realm, aliases in data["meta"]["legacy_heaven_aliases"].items() if aliases)
        node = module._nodes(data, realm, heaven)[0]
        database = probe.context.paths.player_db
        old_position = (realm, legacy, "retired-node", '["retired-node"]')
        expected_position = (realm, heaven, node["id"], old_position[3])
        faults = (
            ("BEFORE UPDATE ON map_status WHEN NEW.node_id <> OLD.node_id "
             "BEGIN SELECT RAISE(ABORT,'map position repair blocked'); END", None),
            ("BEFORE UPDATE ON map_status WHEN NEW.node_id <> OLD.node_id "
             "BEGIN SELECT RAISE(IGNORE); END", "map_status_write_rejected"),
            ("AFTER UPDATE ON map_status WHEN NEW.node_id <> OLD.node_id "
             "BEGIN UPDATE map_status SET visited_nodes='[]' WHERE user_id=NEW.user_id; END",
             "map_status_projection_mismatch"),
        )
        for number, (trigger, reason) in enumerate(faults, 975030):
            probe.seed(database, "INSERT OR REPLACE INTO map_status(user_id,realm,heaven,node_id,visited_nodes) "
                       "VALUES('900001',?,?,?,?)", old_position)
            before = _database_projection(probe)
            probe.seed(database, "CREATE TRIGGER map_position_repair_fault " + trigger)
            reply = await probe.command("我的位置", "我的位置", number=number, timeout=30,
                                        allow_exception=True)
            actual = _position(probe)
            assert actual == [old_position] and "当前位置" not in reply, (reply, actual)
            if reason:
                assert reason_text(reason) and reason_text(reason) in reply and "地图位置" in reply, reply
            else:
                assert "定位号" in reply and "map position repair blocked" not in reply, reply
                assert len(probe.calls[-1]["errors"]) == 1
                assert "sqlite3.IntegrityError: map position repair blocked" in probe.calls[-1]["errors"][0]
            probe.seed(database, "DROP TRIGGER map_position_repair_fault")
            after = _database_projection(probe)
            for key in ("game_db", "player_db", "trade_db", "impart_db"):
                assert before[key] == after[key], key
            recovered = await probe.command("我的位置", "我的位置", number=number, timeout=30)
            assert f"{realm}·{heaven}·{node['name']}（{node['type']}）" in recovered, recovered
            assert _position(probe) == [expected_position]

    @case("map-position-read-recovery", "我的位置", "player SQLite拒绝读取map_status后原消息恢复",
          "只在对应隔离UOW连接安装SQLITE_DENY authorizer", "读取错误不伪报未注册/位置；记录原原因并恢复",
          group="map-position", outcome="recovery")
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
            failed = await probe.command("我的位置", "我的位置", number=975020, timeout=30,
                                         allow_exception=True)
            call = probe.calls[-1]
            state["deny"] = False
            recovered = await probe.command("我的位置", "我的位置", number=975020, timeout=30)
        assert "处理提示" in failed and "定位号" in failed and "当前位置" not in failed, failed
        assert len(call["errors"]) == 1 and "map_status" in call["errors"][0], call
        assert f"{realm}·{heaven}·{node['name']}（{node['type']}）" in recovered, recovered
        assert _position(probe) == [(realm, heaven, node["id"], json.dumps([node["id"]], ensure_ascii=False))]

    @case("map-position-missing-schema", "我的位置", "未完成启动迁移时查询缺失位置",
          "隔离 player SQLite 不含 map_status", "明确提示迁移未完成，命令不建表，迁移后原消息恢复",
          group="map-position", outcome="recovery")
    async def missing_schema(probe):
        from nonebot_plugin_xiuxian_2.core.player_reasons import reason_text
        from nonebot_plugin_xiuxian_2.features.map.migrations import apply_map_player_state_schema
        from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork

        database = probe.context.paths.player_db
        probe.seed(database, "DROP TABLE map_status")
        before = _database_projection(probe)
        reply = await probe.command("我的位置", "我的位置", number=975040, timeout=30,
                                    allow_exception=True)
        text = reason_text("map_status_schema_missing")
        assert text and text in reply and "地图位置" in reply, reply
        after = _database_projection(probe)
        for key in ("game_db", "player_db", "trade_db", "impart_db"):
            assert before[key] == after[key], key
        with DatabaseUnitOfWork(database) as uow:
            apply_map_player_state_schema(uow)
        recovered = await probe.command("我的位置", "我的位置", number=975040, timeout=30)
        assert "当前位置" in recovered and len(_position(probe)) == 1, recovered
