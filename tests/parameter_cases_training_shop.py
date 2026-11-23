"""Read-only contracts for the live training shop command."""
from __future__ import annotations

import json


DATABASES = ("game_db", "player_db", "impart_db")


def projection(probe):
    from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork

    state = {}
    for key in DATABASES:
        database = getattr(probe.context.paths, key)
        with DatabaseUnitOfWork(database, read_only=True) as uow:
            catalog = uow.query_all(
                "SELECT type,name,tbl_name,sql FROM sqlite_master "
                "WHERE name NOT LIKE 'sqlite_%' ORDER BY type,name"
            )
            projected = []
            for entry in catalog:
                row = dict(entry)
                if row["type"] != "table":
                    projected.append((row["type"], row["name"], row["tbl_name"], row["sql"], ()))
                    continue
                escaped = row["name"].replace('"', '""')
                columns = [
                    str(column["name"])
                    for column in uow.query_all(f'PRAGMA table_info("{escaped}")')
                ]
                selected = ",".join(f'"{name.replace(chr(34), chr(34) * 2)}"' for name in columns)
                rows = uow.query_all(f'SELECT {selected} FROM "{escaped}"') if columns else []
                values = tuple(sorted((tuple(item.values()) for item in rows), key=repr))
                projected.append((row["type"], row["name"], row["tbl_name"], row["sql"], values))
            state[key] = tuple(projected)
    return state


def integrity_check(probe):
    from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork

    for key in DATABASES:
        database = getattr(probe.context.paths, key)
        with DatabaseUnitOfWork(database, read_only=True) as uow:
            assert uow.query_one("PRAGMA integrity_check") == {"integrity_check": "ok"}


def clear_player_state(probe, user_id="900001"):
    player = probe.context.paths.player_db
    probe.seed(player, "DELETE FROM training_state_operations WHERE user_id=?", (user_id,))
    probe.seed(player, "DELETE FROM training WHERE user_id=?", (user_id,))


def seed_training(probe, user_id, *, points, weekly_purchases):
    clear_player_state(probe, user_id)
    probe.seed(
        probe.context.paths.player_db,
        "INSERT INTO training(user_id,progress,last_time,points,completed,max_progress,last_event,weekly_purchases) "
        "VALUES(?,?,?,?,?,?,?,?)",
        (user_id, 0, None, points, 0, 0, "", weekly_purchases),
    )


def register(case):
    @case(
        "training-shop-cold-read-only",
        "历练商店",
        "冷态注册玩家只读查询",
        "隔离 player SQLite 中没有 training 状态行",
        "显示零积分与商品，不创建状态或初始化回执",
        group="training-shop-parameters",
    )
    async def cold_read(probe):
        clear_player_state(probe)
        before = projection(probe)
        reply = await probe.command("历练商店", "历练商店 1", number=979_001)
        after = projection(probe)
        probe.evidence.update(before=before, after=after, reply=reply)
        assert before == after, "冷态商店查询创建了历练状态或初始化回执"
        assert "道友目前拥有的历练成就点：0点" in reply, reply
        assert "【历练商店】第1/3页" in reply, reply
        integrity_check(probe)

    @case(
        "training-shop-expired-week-read-only",
        "历练商店",
        "过期周限购状态只读查询",
        "隔离 training 行保存旧 ISO 周及已购商品",
        "展示当前积分和本周零已购视图，不回写周状态或回执",
        group="training-shop-parameters",
    )
    async def expired_week_read(probe):
        seed_training(
            probe,
            "900001",
            points=4321,
            weekly_purchases=json.dumps(
                {"_last_reset": "2020-12-31", "1999": 4}, separators=(",", ":")
            ),
        )
        before = projection(probe)
        reply = await probe.command("历练商店", "历练商店 1", number=979_002)
        after = projection(probe)
        probe.evidence.update(before=before, after=after, reply=reply)
        assert before == after, "过期周限购查询回写了周数据或状态回执"
        assert "道友目前拥有的历练成就点：4321点" in reply, reply
        assert "每周限购：10/10个" in reply, reply
        integrity_check(probe)

    @case(
        "training-shop-invalid-week-read-only",
        "历练商店",
        "非法周 JSON 只读查询",
        "隔离 training 行的 weekly_purchases 为非法 JSON",
        "展示空限购快照，不修写源 JSON、不新增回执",
        group="training-shop-parameters",
    )
    async def invalid_week_read(probe):
        seed_training(probe, "900001", points=8765, weekly_purchases="not-json")
        before = projection(probe)
        reply = await probe.command("历练商店", "历练商店 1", number=979_003)
        after = projection(probe)
        probe.evidence.update(before=before, after=after, reply=reply)
        assert before == after, "非法周 JSON 查询改写了持久化状态或新增回执"
        assert "道友目前拥有的历练成就点：8765点" in reply, reply
        assert "每周限购：10/10个" in reply, reply
        integrity_check(probe)

    @case(
        "training-shop-catalogue-filter-pages-read-only",
        "历练商店",
        "全部商品、关键词、分页、本人积分与限购",
        "实际商品和 Item 定义，玩家保存当前周已购数据",
        "商品编号/名称/描述/价格/限购一致，重复/筛选/翻页全库零写",
        group="training-shop-parameters",
    )
    async def catalogue_filter_pages(probe):
        from nonebot_plugin_xiuxian_2.features.base.shop_limit import shop_limit_line
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_training as module

        today = module.runtime_clock.now().date().isoformat()
        config = module.training_data.config["商店商品"]
        items = module._items()
        ordered = sorted(config.items(), key=lambda row: int(row[0]))
        assert len(ordered) == 26, len(ordered)
        for item_id, _item in ordered:
            assert items.get_data_by_item_id(item_id) is not None, item_id

        seed_training(
            probe,
            "900001",
            points=4321,
            weekly_purchases=json.dumps(
                {"_last_reset": today, "1999": 2}, separators=(",", ":")
            ),
        )
        seed_training(
            probe,
            "900002",
            points=765,
            weekly_purchases=json.dumps({"_last_reset": today}, separators=(",", ":")),
        )
        before = projection(probe)

        pages = []
        for page in (1, 2, 3):
            reply = await probe.command(
                "历练商店", f"历练商店 {page}", number=979_010 + page
            )
            assert f"【历练商店】第{page}/3页" in reply, reply
            assert "道友目前拥有的历练成就点：4321点" in reply, reply
            page_rows = ordered[(page - 1) * 10 : page * 10]
            for item_id, item in page_rows:
                data = items.get_data_by_item_id(item_id)
                purchased = 2 if item_id == "1999" else 0
                for text in (
                    f"编号：{item_id}",
                    f"名称：{data['name']}",
                    f"描述：{data.get('desc', '暂无描述')}",
                    f"价格：{item['cost']}成就点",
                    shop_limit_line(item["weekly_limit"], purchased),
                ):
                    assert text in reply, (item_id, text, reply)
            next_page = min(page + 1, 3)
            assert f"历练商店 {next_page}" in reply, reply
            pages.append(reply)
            assert projection(probe) == before, "分页查询改写了业务数据库"
            assert await probe.command(
                "历练商店", f"历练商店 {page}", number=979_020 + page
            ) == reply
            assert projection(probe) == before, "重复查询产生了业务写入"

        filtered = await probe.command(
            "历练商店", "历练商店 灵宠", number=979_031
        )
        pet_rows = [
            (item_id, item)
            for item_id, item in ordered
            if "灵宠" in item["name"]
            or "灵宠" in str(items.get_data_by_item_id(item_id).get("desc", ""))
        ]
        assert len(pet_rows) == 5
        assert "【历练商店】第1/1页" in filtered, filtered
        for item_id, item in pet_rows:
            assert f"编号：{item_id}" in filtered and item["name"] in filtered, filtered
        assert projection(probe) == before

        last_page = await probe.command(
            "历练商店", "历练商店 99", number=979_032
        )
        assert "【历练商店】第3/3页" in last_page, last_page
        assert all(f"编号：{item_id}" in last_page for item_id, _ in ordered[-6:])
        assert projection(probe) == before

        from nonebot_plugin_xiuxian_2.features.base.keyword_page import empty_notice

        empty = await probe.command(
            "历练商店", "历练商店 不存在的历练物品", number=979_033
        )
        assert empty_notice("不存在的历练物品", "历练商店") in empty, empty
        assert projection(probe) == before

        other = await probe.command(
            "历练商店", "历练商店 1", actor="900002", number=979_034
        )
        assert "道友目前拥有的历练成就点：765点" in other, other
        assert projection(probe) == before
        integrity_check(probe)
        probe.evidence.update(
            products=len(ordered),
            pages=3,
            filtered=len(pet_rows),
            last_page_clamped=True,
            no_match=True,
            player_isolation=True,
            repeat_full_database_zero_writes=True,
            integrity_check="ok",
        )
