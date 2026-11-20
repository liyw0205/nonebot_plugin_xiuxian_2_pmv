"""Read-only contracts for the live tower shop command."""
from __future__ import annotations

import json
import sqlite3


_DATABASES = ("game_db", "player_db", "impart_db")


def _projection(probe):
    result = {}
    for name in _DATABASES:
        database = getattr(probe.context.paths, name)
        with sqlite3.connect(database) as connection:
            tables = connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name NOT LIKE 'sqlite_%' ORDER BY name"
            ).fetchall()
            projected = []
            for (table,) in tables:
                escaped = table.replace('"', '""')
                columns = [
                    row[1]
                    for row in connection.execute(f'PRAGMA table_info("{escaped}")')
                ]
                selected = ",".join(f'"{column.replace(chr(34), chr(34) * 2)}"' for column in columns)
                rows = connection.execute(
                    f'SELECT {selected} FROM "{escaped}"'
                ).fetchall() if columns else []
                projected.append((table, tuple(sorted(rows, key=repr))))
            result[name] = tuple(projected)
    return result


def _integrity_check(probe):
    for manager in (probe.managers[0], probe.managers[1], probe.managers[3]):
        assert manager.conn.execute("PRAGMA integrity_check").fetchone() == ("ok",)


def _remove_tower_state(probe):
    player = probe.context.paths.player_db
    probe.seed(player, "DELETE FROM tower_state_operations WHERE user_id=?", ("900001",))
    probe.seed(player, "DELETE FROM tower WHERE user_id=?", ("900001",))


def _seed_tower_state(probe, weekly_purchases):
    player = probe.context.paths.player_db
    _remove_tower_state(probe)
    probe.seed(
        player,
        "INSERT INTO tower(user_id,current_floor,max_floor,score,weekly_purchases) "
        "VALUES(?,?,?,?,?)",
        ("900001", 7, 12, 4321, weekly_purchases),
    )


def register(case):
    @case(
        "tower-shop-cold-read-only",
        "通天塔商店",
        "冷态玩家只读查询",
        "隔离 player SQLite 中不存在该玩家的 tower 行",
        "返回默认积分和限购视图，三库全表投影不变",
        group="tower-shop-parameters",
    )
    async def cold_query_is_read_only(probe):
        _remove_tower_state(probe)
        before = _projection(probe)
        text = await probe.command("通天塔商店", "通天塔商店 1", number=978_001)
        after = _projection(probe)
        probe.evidence.update(before=before, after=after, reply=text)
        assert before == after, "冷态只读查询写入了塔状态或其他业务数据"
        assert "道友目前拥有的通天塔积分：0点" in text, text
        _integrity_check(probe)

    @case(
        "tower-shop-expired-week-read-only",
        "通天塔商店",
        "过期周状态只读查询",
        "隔离 tower 行保存旧 ISO 周限购数据",
        "展示当周零已购视图，但不回写旧周数据或写重置回执",
        group="tower-shop-parameters",
    )
    async def expired_week_query_is_read_only(probe):
        _seed_tower_state(
            probe,
            json.dumps({"_last_reset": "2020-12-31", "1999": 4}, separators=(",", ":")),
        )
        before = _projection(probe)
        text = await probe.command("通天塔商店", "通天塔商店 1", number=978_002)
        after = _projection(probe)
        probe.evidence.update(before=before, after=after, reply=text)
        assert before == after, "过期周状态查询回写了限购或重置回执"
        assert "道友目前拥有的通天塔积分：4321点" in text, text
        _integrity_check(probe)

    @case(
        "tower-shop-invalid-week-read-only",
        "通天塔商店",
        "坏周 JSON 只读查询",
        "隔离 tower 行的 weekly_purchases 是非法 JSON",
        "按空限购视图展示，不修写原数据、不新增状态回执",
        group="tower-shop-parameters",
    )
    async def invalid_week_query_is_read_only(probe):
        _seed_tower_state(probe, "not-json")
        before = _projection(probe)
        text = await probe.command("通天塔商店", "通天塔商店 1", number=978_003)
        after = _projection(probe)
        probe.evidence.update(before=before, after=after, reply=text)
        assert before == after, "坏周 JSON 查询改写了持久化状态或写入回执"
        assert "道友目前拥有的通天塔积分：4321点" in text, text
        _integrity_check(probe)

    @case(
        "tower-shop-catalog-and-read-only-contract",
        "通天塔商店",
        "全部商品、筛选、分页、积分与周限购快照",
        "27 个真实配置商品及实际 Item catalog，玩家状态为本周有效值",
        "商品字段与定义一致，关键词/边界分页正确，重复查询三库零写",
        group="tower-shop-parameters",
    )
    async def catalog_and_page_queries_are_read_only(probe):
        from nonebot_plugin_xiuxian_2.infrastructure.clock import SystemClock
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_tower.tower_data import tower_data
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.item_json import Items

        definitions = tower_data.config["商店商品"]
        item_manager = Items()
        assert len(definitions) == 27, len(definitions)
        weekly = {"_last_reset": SystemClock().now().date().isoformat()}
        ordered = list(definitions.items())
        weekly[str(ordered[0][0])] = 3
        weekly[str(ordered[1][0])] = int(ordered[1][1]["weekly_limit"])
        _seed_tower_state(probe, json.dumps(weekly, ensure_ascii=False, sort_keys=True))

        before = _projection(probe)
        pages = []
        for page in (1, 2, 3):
            text = await probe.command(
                "通天塔商店", f"通天塔商店 {page}", number=978_010 + page
            )
            assert f"【通天塔商店】第{page}/3页" in text, text
            pages.append(text)
            assert _projection(probe) == before, f"页码 {page} 查询产生写入"

        for index, (item_id, definition) in enumerate(ordered):
            item = item_manager.get_data_by_item_id(item_id)
            assert item, f"配置商品 {item_id} 不存在于 Item catalog"
            text = pages[index // 10]
            purchased = int(weekly.get(str(item_id), 0))
            remaining = max(int(definition["weekly_limit"]) - purchased, 0)
            expected_limit = (
                f"每周限购：已售罄（{definition['weekly_limit']}/{definition['weekly_limit']}个）"
                if remaining == 0
                else f"每周限购：{remaining}/{definition['weekly_limit']}个"
            )
            for expected in (
                f"编号：{item_id}",
                f"名称：{item['name']}",
                f"描述：{item.get('desc', '暂无描述')}",
                f"价格：{definition['cost']}积分",
                expected_limit,
            ):
                assert expected in text, (item_id, expected, text)

        clamped = await probe.command(
            "通天塔商店", "通天塔商店 99", number=978_014
        )
        assert clamped == pages[2], clamped
        filtered = await probe.command(
            "通天塔商店", "通天塔商店 秘境加速券 1", number=978_015
        )
        assert "秘境加速券" in filtered and "第1/1页" in filtered, filtered
        assert "渡厄丹" not in filtered, filtered
        empty = await probe.command(
            "通天塔商店", "通天塔商店 不存在的诊断商品", number=978_016
        )
        assert "不存在的诊断商品" in empty and "通天塔商店" in empty, empty
        for query, number in (
            ("通天塔商店 1", 978_020),
            ("通天塔商店 2", 978_021),
            ("通天塔商店 3", 978_022),
        ):
            await probe.command("通天塔商店", query, number=number)
            assert _projection(probe) == before, query
        assert "道友目前拥有的通天塔积分：4321点" in pages[0], pages[0]
        _integrity_check(probe)
        probe.evidence.update(
            item_count=len(definitions),
            page_counts=[10, 10, 7],
            score=4321,
            filter_keyword="秘境加速券",
            empty_keyword=True,
            overflow_page_clamped=True,
            repeated_queries_zero_writes=True,
            full_game_player_impart_zero_writes=True,
            integrity_check="ok",
        )
