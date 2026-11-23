"""Real map, seed, fusion and alchemy parameter paths."""

import json
import re

from tests.parameter_cases_assets import balances, backpack


def register(case):
    @case("map-info-real-query", "地图", "随包真实界名", "真实地图配置和已注册玩家",
          "查询实际界面节点，不修改位置或资产", group="map-fusion-parameters")
    async def map_info(probe):
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_map as module
        data = module._load_map_data()
        realm = data["meta"]["realms"][0]
        before = balances(probe)
        text = await probe.command("地图", "地图 " + realm)
        assert realm in text and balances(probe) == before, text
        probe.evidence.update(realm=realm, readonly=True)

    @case("map-go-real-node", "前往", "当前天另一真实节点名", "真实地图状态和足够体力",
          "按实际节点名移动，位置和体力持久化，重复事件不重复扣体力", group="map-fusion-parameters")
    async def map_go(probe):
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_map as module
        data = module._load_map_data()
        module._get_player_map_status("900001", data)
        status = module.map_application.map_status("900001")
        nodes = module._nodes(data, status["realm"], status["heaven"])
        target = next(node for node in nodes if node["id"] != status["node_id"])
        probe.player(user_stamina=100)
        text = await probe.command("前往", "前往 " + target["name"], number=970001)
        moved = module.map_application.map_status("900001")
        assert moved["node_id"] == target["id"] and moved["realm"] == status["realm"], text
        saved = probe.rows(probe.context.paths.game_db, "SELECT user_stamina FROM user_xiuxian WHERE user_id='900001'")
        assert saved[0][0] < 100
        await probe.command("前往", "前往 " + target["name"], number=970001)
        assert probe.rows(probe.context.paths.game_db, "SELECT user_stamina FROM user_xiuxian WHERE user_id='900001'") == saved
        probe.evidence.update(target=target["name"], before=status, after=moved, replay_unchanged=True)

    @case("map-seed-purchase-valid", "购买种子", "真实种子名 数量2", "真实种子商店节点、100000000灵石、空库存",
          "精确购买2份种子，扣真实价格并写库存，重放不重复", group="map-seed-purchase")
    async def seed_buy(probe):
        from nonebot_plugin_xiuxian_2.features.map.migrations import apply_map_seed_purchase
        from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
        from nonebot_plugin_xiuxian_2.infrastructure.database.ledger import OperationLedger
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_map as module
        data = module._load_map_data()
        for realm in data["meta"]["realms"]:
            for heaven in module._heaven_names(data, realm):
                node = next((candidate for candidate in module._nodes(data, realm, heaven) if module._is_seed_shop_node(candidate["type"])), None)
                if node:
                    node = {**node, "realm": realm, "heaven": heaven}
                    break
            if node:
                break
        module.map_application.save_status("900001", node["realm"], node["heaven"], node["id"], [node["id"]])
        probe.player(stone=100_000_000)
        with DatabaseUnitOfWork(probe.context.paths.game_db) as uow:
            apply_map_seed_purchase(uow)
            OperationLedger().ensure_schema(uow)
        seed = module.SEED_CONFIG[21001]
        before = balances(probe)
        text = await probe.command("购买种子", f"购买种子 {seed['name']} 2", number=970002)
        assert probe.managers[0].goods_num("900001", 21001) == 2, text
        after = balances(probe)
        assert before[0][1] - after[0][1] == 2 * seed["price"]
        saved = backpack(probe)
        await probe.command("购买种子", f"购买种子 {seed['name']} 2", number=970002)
        assert backpack(probe) == saved and balances(probe) == after
        changed = await probe.command(
            "购买种子", f"购买种子 {seed['name']} 3", number=970002, allow_exception=True
        )
        assert "不同内容" in changed and backpack(probe) == saved and balances(probe) == after, changed
        database = probe.context.paths.game_db
        legacy_operation = "map-seed-purchase:900001:970004"
        probe.seed(
            database,
            "INSERT INTO map_seed_purchase_operations(operation_id,payload,quantity,cost,stone,inventory) "
            "VALUES(?,?,?,?,?,?)",
            (legacy_operation, json.dumps(["900001", 21001, seed["name"], 2, seed["price"], 100_000_000, 1000]),
             2, 2 * seed["price"], 99_000_000, 2),
        )
        legacy_replay = await probe.command(
            "购买种子", f"购买种子 {seed['name']} 2", number=970004, timeout=30
        )
        assert "购买成功" in legacy_replay and backpack(probe) == saved and balances(probe) == after
        for index, (item_id, configured) in enumerate(module.SEED_CONFIG.items()):
            if item_id == 21001:
                continue
            purchase_number = 970010 + index
            before_stone = probe.rows(
                probe.context.paths.game_db,
                "SELECT stone FROM user_xiuxian WHERE user_id='900001'",
            )[0][0]
            text = await probe.command(
                "购买种子",
                f"购买种子 {configured['name']} 1",
                number=purchase_number,
                timeout=30,
            )
            assert f"购买成功：{configured['name']} x1" in text
            assert module.number_to(configured["price"]) in text
            saved_stone = probe.rows(
                probe.context.paths.game_db,
                "SELECT stone FROM user_xiuxian WHERE user_id='900001'",
            )
            saved_item = probe.rows(
                probe.context.paths.game_db,
                "SELECT goods_num,bind_num FROM back WHERE user_id='900001' AND goods_id=?",
                (item_id,),
            )
            assert saved_stone == [(before_stone - configured["price"],)]
            assert saved_item == [(1, 1)]
            replay = await probe.command(
                "购买种子",
                f"购买种子 {configured['name']} 1",
                number=purchase_number,
                timeout=30,
            )
            assert f"购买成功：{configured['name']} x1" in replay
            assert probe.rows(
                probe.context.paths.game_db,
                "SELECT stone FROM user_xiuxian WHERE user_id='900001'",
            ) == saved_stone
            assert probe.rows(
                probe.context.paths.game_db,
                "SELECT goods_num,bind_num FROM back WHERE user_id='900001' AND goods_id=?",
                (item_id,),
            ) == saved_item
        module.map_application.save_status("900002", node["realm"], node["heaven"], node["id"], [node["id"]])
        probe.player(uid="900002", stone=10_000_000)
        other_reply = await probe.command(
            "购买种子", f"购买种子 {seed['name']} 1", actor="900002", number=970003, event_type="private"
        )
        assert "购买成功" in other_reply
        assert probe.rows(probe.context.paths.game_db, "SELECT stone FROM user_xiuxian WHERE user_id='900002'") == [(9_500_000,)]
        assert probe.rows(probe.context.paths.game_db, "SELECT goods_num,bind_num FROM back WHERE user_id='900002' AND goods_id=21001") == [(1, 1)]
        assert probe.rows(probe.context.paths.game_db, "SELECT goods_num,bind_num FROM back WHERE user_id='900001' AND goods_id=21001") == [(2, 2)]
        probe.seed(database, "UPDATE back SET bind_num=NULL WHERE user_id='900002' AND goods_id=21001")
        old_backpack_reply = await probe.command(
            "购买种子", f"购买种子 {seed['name']} 1", actor="900002", number=970005, event_type="private"
        )
        assert "购买成功" in old_backpack_reply, old_backpack_reply
        assert probe.rows(database, "SELECT goods_num,bind_num FROM back WHERE user_id='900002' AND goods_id=21001") == [(2, 1)]
        assert probe.rows(database, "SELECT stone FROM user_xiuxian WHERE user_id='900002'") == [(9_000_000,)]
        probe.evidence.update(seed_id=21001, quantity=2, replay_unchanged=True,
                              other_player_isolated=True, legacy_null_binding=True)

    @case("map-seed-extra-reject", "购买种子", "真实种子名 数量 多余参数", "真实种子商店节点和充足灵石",
          "格式拒绝，资产与库存不变", group="map-seed-purchase", outcome="rejection")
    async def seed_extra(probe):
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_map as module
        data = module._load_map_data()
        for realm in data["meta"]["realms"]:
            for heaven in module._heaven_names(data, realm):
                node = next((candidate for candidate in module._nodes(data, realm, heaven) if module._is_seed_shop_node(candidate["type"])), None)
                if node:
                    node = {**node, "realm": realm, "heaven": heaven}
                    break
            if node:
                break
        module.map_application.save_status("900001", node["realm"], node["heaven"], node["id"], [node["id"]])
        before, stock = balances(probe), backpack(probe)
        text = await probe.command("购买种子", "购买种子 青灵草种 1 extra")
        assert before == balances(probe) and stock == backpack(probe), text
        assert "格式" in text, text
        probe.evidence.update(assets_unchanged=True, inventory_unchanged=True)

    @case("map-seed-purchase-input-and-business-boundaries", "购买种子", "非法/未知/非商店/余额/背包上限",
          "隔离真实节点、钱包、背包和迁移表",
          "拒绝路径返回具体状态且不改钱包、库存或购买回执",
          group="map-seed-purchase", outcome="rejection")
    async def seed_purchase_boundaries(probe):
        from nonebot_plugin_xiuxian_2.features.map.migrations import apply_map_seed_purchase
        from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
        from nonebot_plugin_xiuxian_2.infrastructure.database.ledger import OperationLedger
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_map as module

        game = probe.context.paths.game_db
        data = module._load_map_data()
        nodes = [
            {**candidate, "realm": realm, "heaven": heaven}
            for realm in data["meta"]["realms"]
            for heaven in module._heaven_names(data, realm)
            for candidate in module._nodes(data, realm, heaven)
        ]
        shop = next(node for node in nodes if module._is_seed_shop_node(node["type"]))
        other = next(node for node in nodes if not module._is_seed_shop_node(node["type"]))
        seed = module.SEED_CONFIG[21001]
        with DatabaseUnitOfWork(game) as uow:
            apply_map_seed_purchase(uow)
            OperationLedger().ensure_schema(uow)
        module.map_application.save_status("900001", shop["realm"], shop["heaven"], shop["id"], [shop["id"]])

        checks = (
            ("购买种子 青灵草种 0", "数量需大于0"),
            ("购买种子 青灵草种 -1", "数量需大于0"),
            ("购买种子 青灵草种 nope", "数量必须是整数"),
            ("购买种子 青灵草种 1 extra", "购买格式"),
            ("购买种子 不存在的种子 1", "未找到种子"),
        )
        for index, (command, expected) in enumerate(checks):
            before = (
                probe.rows(game, "SELECT stone FROM user_xiuxian WHERE user_id='900001'"),
                probe.rows(game, "SELECT goods_num,bind_num FROM back WHERE user_id='900001' ORDER BY goods_id"),
                probe.rows(game, "SELECT * FROM map_seed_purchase_operations ORDER BY operation_id"),
            )
            reply = await probe.command("购买种子", command, number=970050 + index, timeout=30)
            assert expected in reply, (command, reply)
            after = (
                probe.rows(game, "SELECT stone FROM user_xiuxian WHERE user_id='900001'"),
                probe.rows(game, "SELECT goods_num,bind_num FROM back WHERE user_id='900001' ORDER BY goods_id"),
                probe.rows(game, "SELECT * FROM map_seed_purchase_operations ORDER BY operation_id"),
            )
            assert after == before, (command, reply, before, after)

        conflict_operation = "map-seed-purchase:900001:970059"
        probe.seed(
            game,
            "INSERT INTO map_seed_purchase_operations(operation_id,payload,quantity,cost,stone,inventory) "
            "VALUES(?,?,?,?,?,?)",
            (conflict_operation, json.dumps(["900001", 21001, seed["name"], 1, seed["price"]]),
             1, seed["price"], 500_000, 1),
        )
        before_conflict = (
            probe.rows(game, "SELECT stone FROM user_xiuxian WHERE user_id='900001'"),
            probe.rows(game, "SELECT goods_num,bind_num FROM back WHERE user_id='900001' ORDER BY goods_id"),
        )
        conflict_reply = await probe.command(
            "购买种子", f"购买种子 {seed['name']} 2", number=970059, timeout=30
        )
        assert "不同内容" in conflict_reply, conflict_reply
        assert before_conflict == (
            probe.rows(game, "SELECT stone FROM user_xiuxian WHERE user_id='900001'"),
            probe.rows(game, "SELECT goods_num,bind_num FROM back WHERE user_id='900001' ORDER BY goods_id"),
        )

        corrupt_operation = "map-seed-purchase:900001:970060"
        probe.seed(
            game,
            "INSERT INTO map_seed_purchase_operations(operation_id,payload,quantity,cost,stone,inventory) "
            "VALUES(?,?,?,?,?,?)",
            (corrupt_operation, "{broken", 1, seed["price"], 500_000, 1),
        )
        before_corrupt = (
            probe.rows(game, "SELECT stone FROM user_xiuxian WHERE user_id='900001'"),
            probe.rows(game, "SELECT goods_num,bind_num FROM back WHERE user_id='900001' ORDER BY goods_id"),
        )
        from nonebot_plugin_xiuxian_2.core.player_reasons import reason_text
        corrupt_reply = await probe.command(
            "购买种子", f"购买种子 {seed['name']} 1", number=970060, timeout=30
        )
        assert reason_text("map_seed_purchase_receipt_invalid") in corrupt_reply, corrupt_reply
        assert before_corrupt == (
            probe.rows(game, "SELECT stone FROM user_xiuxian WHERE user_id='900001'"),
            probe.rows(game, "SELECT goods_num,bind_num FROM back WHERE user_id='900001' ORDER BY goods_id"),
        )

        module.map_application.save_status("900001", other["realm"], other["heaven"], other["id"], [other["id"]])
        reply = await probe.command("购买种子", f"购买种子 {seed['name']} 1", number=970061, timeout=30)
        assert "无种子商店" in reply
        module.map_application.save_status("900001", shop["realm"], shop["heaven"], shop["id"], [shop["id"]])

        probe.player(stone=1)
        before_stone = probe.rows(game, "SELECT stone FROM user_xiuxian WHERE user_id='900001'")
        before_receipts = probe.rows(game, "SELECT * FROM map_seed_purchase_operations ORDER BY operation_id")
        reply = await probe.command("购买种子", f"购买种子 {seed['name']} 1", number=970062, timeout=30)
        assert "灵石不足" in reply
        assert probe.rows(game, "SELECT stone FROM user_xiuxian WHERE user_id='900001'") == before_stone
        assert probe.rows(game, "SELECT * FROM map_seed_purchase_operations ORDER BY operation_id") == before_receipts

        probe.player(stone=100_000_000)
        definition = module.Items().get_data_by_item_id(21001)
        probe.managers[0].send_back("900001", 21001, definition["name"], definition["type"], 1000)
        before_full = (
            probe.rows(game, "SELECT stone FROM user_xiuxian WHERE user_id='900001'"),
            probe.rows(game, "SELECT goods_num,bind_num FROM back WHERE user_id='900001' AND goods_id=21001"),
            probe.rows(game, "SELECT * FROM map_seed_purchase_operations ORDER BY operation_id"),
        )
        reply = await probe.command("购买种子", f"购买种子 {seed['name']} 1", number=970063, timeout=30)
        assert "背包物品已达上限" in reply
        after_full = (
            probe.rows(game, "SELECT stone FROM user_xiuxian WHERE user_id='900001'"),
            probe.rows(game, "SELECT goods_num,bind_num FROM back WHERE user_id='900001' AND goods_id=21001"),
            probe.rows(game, "SELECT * FROM map_seed_purchase_operations ORDER BY operation_id"),
        )
        assert after_full == before_full

        probe.seed(game, "UPDATE back SET goods_num=1,bind_num=-1 WHERE user_id='900001' AND goods_id=21001")
        reply = await probe.command(
            "购买种子", f"购买种子 {seed['name']} 1", number=970064, timeout=30, allow_exception=True
        )
        assert reason_text("map_seed_purchase_inventory_invalid") in reply, reply
        assert "背包物品（ID 21001）" in reply, reply
        assert probe.rows(game, "SELECT stone FROM user_xiuxian WHERE user_id='900001'") == before_full[0]
        assert probe.rows(game, "SELECT goods_num,bind_num FROM back WHERE user_id='900001' AND goods_id=21001") == [(1, -1)]
        assert probe.rows(game, "SELECT * FROM map_seed_purchase_operations ORDER BY operation_id") == before_full[2]
        probe.evidence["boundary_branches"] = len(checks) + 6

    @case("map-seed-shop-readonly", "种子商店", "群聊/私聊重复只读展示",
          "真实种子配置、商店节点及有效地图状态",
          "展示每个配置种子的实际价格，业务数据库投影不变",
          group="map-seed-purchase")
    async def seed_shop_readonly(probe):
        from tests.parameter_cases_map_help import _database_projection
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_map as module

        data = module._load_map_data()
        node = next(
            {**candidate, "realm": realm, "heaven": heaven}
            for realm in data["meta"]["realms"]
            for heaven in module._heaven_names(data, realm)
            for candidate in module._nodes(data, realm, heaven)
            if module._is_seed_shop_node(candidate["type"])
        )
        module.map_application.save_status("900001", node["realm"], node["heaven"], node["id"], [node["id"]])
        before = _database_projection(probe)
        replies = [
            await probe.command("种子商店", "种子商店", number=970060 + index,
                                event_type=event_type, timeout=30)
            for index, event_type in enumerate(("group", "private", "group"))
        ]
        replies = [re.sub(r"^\[CQ:at,qq=\d+\]", "", reply).replace("\r", "") for reply in replies]
        after = _database_projection(probe)
        for key in ("game_db", "player_db", "trade_db", "impart_db"):
            assert after[key] == before[key], key
        for item_id, seed in module.SEED_CONFIG.items():
            assert seed["name"] in replies[0] and module.number_to(seed["price"]) in replies[0], (item_id, seed, replies[0])
        assert replies[0] == replies[1] == replies[2]
        probe.evidence.update(seed_count=len(module.SEED_CONFIG), business_databases_readonly=True)

    @case("map-seed-purchase-write-recovery", "购买种子", "真实种子名 数量2",
          "种子商店、隔离迁移库和逐写点IGNORE/AFTER改写/ABORT触发器",
          "每个写点拒绝均不伪成功或留下半结算；同消息解除故障后成功一次",
          group="map-seed-purchase", outcome="recovery")
    async def seed_purchase_write_recovery(probe):
        from nonebot_plugin_xiuxian_2.core.player_reasons import reason_text
        from nonebot_plugin_xiuxian_2.features.map.migrations import apply_map_seed_purchase
        from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
        from nonebot_plugin_xiuxian_2.infrastructure.database.ledger import OperationLedger
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_map as module

        game = probe.context.paths.game_db
        data = module._load_map_data()
        node = next(
            {**candidate, "realm": realm, "heaven": heaven}
            for realm in data["meta"]["realms"]
            for heaven in module._heaven_names(data, realm)
            for candidate in module._nodes(data, realm, heaven)
            if module._is_seed_shop_node(candidate["type"])
        )
        seed = module.SEED_CONFIG[21001]
        module.map_application.save_status("900001", node["realm"], node["heaven"], node["id"], [node["id"]])
        probe.player(stone=100_000_000)
        with DatabaseUnitOfWork(game) as uow:
            apply_map_seed_purchase(uow)
            OperationLedger().ensure_schema(uow)

        faults = (
            ("stone", "BEFORE UPDATE OF stone ON user_xiuxian WHEN NEW.user_id='900001' "
             "BEGIN SELECT RAISE(IGNORE); END", "map_seed_purchase_stone_write_rejected"),
            ("stone", "AFTER UPDATE OF stone ON user_xiuxian WHEN NEW.user_id='900001' "
             "BEGIN UPDATE user_xiuxian SET stone=NEW.stone+1 WHERE user_id=NEW.user_id; END",
             "map_seed_purchase_stone_projection_mismatch"),
            ("stone", "BEFORE UPDATE OF stone ON user_xiuxian WHEN NEW.user_id='900001' "
             "BEGIN SELECT RAISE(ABORT,'seed stone blocked'); END", None),
            ("inventory", "BEFORE UPDATE OF goods_num ON back WHEN NEW.user_id='900001' AND NEW.goods_id=21001 "
             "BEGIN SELECT RAISE(IGNORE); END", "map_seed_purchase_inventory_write_rejected"),
            ("inventory", "AFTER UPDATE OF goods_num ON back WHEN NEW.user_id='900001' AND NEW.goods_id=21001 "
             "BEGIN UPDATE back SET goods_num=NEW.goods_num+1 WHERE user_id=NEW.user_id AND goods_id=NEW.goods_id; END",
             "map_seed_purchase_inventory_projection_mismatch"),
            ("inventory", "BEFORE UPDATE OF goods_num ON back WHEN NEW.user_id='900001' AND NEW.goods_id=21001 "
             "BEGIN SELECT RAISE(ABORT,'seed inventory blocked'); END", None),
            ("receipt", "BEFORE INSERT ON map_seed_purchase_operations BEGIN SELECT RAISE(IGNORE); END",
             "map_seed_purchase_receipt_write_rejected"),
            ("receipt", "AFTER INSERT ON map_seed_purchase_operations BEGIN UPDATE map_seed_purchase_operations "
             "SET cost=NEW.cost+1 WHERE operation_id=NEW.operation_id; END",
             "map_seed_purchase_receipt_projection_mismatch"),
            ("receipt", "BEFORE INSERT ON map_seed_purchase_operations "
             "BEGIN SELECT RAISE(ABORT,'seed receipt blocked'); END", None),
        )
        evidence = []
        for index, (subject, trigger, reason) in enumerate(faults):
            number = 970030 + index
            before_stone = probe.rows(game, "SELECT stone FROM user_xiuxian WHERE user_id='900001'")
            before_inventory = probe.rows(game, "SELECT goods_num,bind_num FROM back WHERE user_id='900001' AND goods_id=21001")
            before_inventory_values = before_inventory[0] if before_inventory else (0, 0)
            before_receipt = probe.rows(game, "SELECT operation_id,payload,quantity,cost,stone,inventory FROM map_seed_purchase_operations ORDER BY operation_id")
            trigger_name = f"map_seed_fault_{index}"
            probe.seed(game, f"CREATE TRIGGER {trigger_name} {trigger}")
            reply = await probe.command(
                "购买种子", f"购买种子 {seed['name']} 2", number=number,
                timeout=30, allow_exception=reason is None,
            )
            assert "购买成功" not in reply, (subject, trigger, reply)
            assert probe.rows(game, "SELECT stone FROM user_xiuxian WHERE user_id='900001'") == before_stone
            assert probe.rows(game, "SELECT goods_num,bind_num FROM back WHERE user_id='900001' AND goods_id=21001") == before_inventory
            assert probe.rows(game, "SELECT operation_id,payload,quantity,cost,stone,inventory FROM map_seed_purchase_operations ORDER BY operation_id") == before_receipt
            if reason:
                assert reason_text(reason) in reply, (reason, reply)
                if subject == "stone":
                    assert "灵石钱包" in reply, reply
                elif subject == "inventory":
                    assert "背包物品" in reply, reply
                else:
                    assert "购买回执" in reply, reply
                assert probe.rows(game, "SELECT status FROM operation_ledger WHERE operation_id=? AND action='map.purchase_seed'", (f"map-seed-purchase:900001:{number}",)) == []
            else:
                assert "定位号" in reply and "blocked" not in reply, reply
                assert probe.rows(game, "SELECT status FROM operation_ledger WHERE operation_id=? AND action='map.purchase_seed'", (f"map-seed-purchase:900001:{number}",)) == [("failed",)]

            probe.seed(game, f"DROP TRIGGER {trigger_name}")
            recovered = await probe.command("购买种子", f"购买种子 {seed['name']} 2", number=number, timeout=30)
            assert "购买成功" in recovered, (subject, recovered)
            current = probe.rows(game, "SELECT stone FROM user_xiuxian WHERE user_id='900001'")
            assert current == [(before_stone[0][0] - 2 * seed["price"],)]
            inventory = probe.rows(game, "SELECT goods_num,bind_num FROM back WHERE user_id='900001' AND goods_id=21001")
            assert inventory == [(before_inventory_values[0] + 2, before_inventory_values[1] + 2)]
            receipt = probe.rows(game, "SELECT quantity,cost,stone,inventory FROM map_seed_purchase_operations WHERE operation_id=?", (f"map-seed-purchase:900001:{number}",))
            assert receipt == [(2, 2 * seed["price"], current[0][0], inventory[0][0])]
            evidence.append({"subject": subject, "expected_reason": reason, "recovered": True})
            with DatabaseUnitOfWork(game, read_only=True) as uow:
                assert uow.query_one("PRAGMA integrity_check")["integrity_check"] == "ok"

        probe.evidence["write_failures"] = evidence

    @case("fusion-real-item-count", "强行合成", "真实可合成物品名 数量", "真实融合配置、合法境界与材料库存",
          "走真实融合应用，材料/灵石按数量结算并持久化，重放不重复", group="map-fusion-parameters")
    async def fusion(probe):
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_fusion as module
        candidate = None
        for item_id in range(1, 20000):
            item = module._items().get_data_by_item_id(item_id)
            if item and item.get("fusion", {}).get("need_item"):
                candidate = (item_id, item)
                break
        assert candidate, "fixture has no fusion recipe"
        item_id, item = candidate
        fusion = item["fusion"]
        probe.player(level=fusion.get("need_rank", "江湖好手"), stone=max(10**9, int(fusion.get("need_stone", 0)) * 2), exp=10**30)
        for material_id, amount in fusion["need_item"].items():
            probe.grant_item(int(material_id), int(amount) * 2)
        before = backpack(probe)
        text = await probe.command("强行合成", f"强行合成 {item['name']} 1", number=970003)
        after = backpack(probe)
        assert after != before and item["name"] in text, text
        saved = backpack(probe)
        await probe.command("强行合成", f"强行合成 {item['name']} 1", number=970003)
        assert backpack(probe) == saved
        probe.evidence.update(item_id=item_id, item_name=item["name"], replay_unchanged=True)

    @case("fusion-invalid-quantity", "强行合成", "真实物品名 零/非数字/多余参数", "真实可合成物品和材料库存",
          "严格拒绝数量格式，不消费材料", group="map-fusion-parameters", outcome="rejection")
    async def fusion_invalid(probe):
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_fusion as module
        item = next(module._items().get_data_by_item_id(i) for i in range(1, 20000) if module._items().get_data_by_item_id(i) and module._items().get_data_by_item_id(i).get("fusion", {}).get("need_item"))
        before = backpack(probe)
        text = await probe.command("强行合成", f"强行合成 {item['name']} 0")
        assert backpack(probe) == before and "正整数" in text, text
        probe.evidence.update(assets_unchanged=True)

    @case("fusion-receipt-ignore-recovery", "强行合成", "天罪", "真实固定成功配方及所需材料",
          "业务回执拒写时完整回滚并明确失败，解除故障后同消息恢复且不重复结算",
          group="fusion-write-recovery", outcome="recovery")
    async def fusion_receipt_ignore_recovery(probe):
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_fusion as module
        from nonebot_plugin_xiuxian_2.core.player_reasons import reason_text
        from nonebot_plugin_xiuxian_2.features.fusion.migrations import apply_fusion_operations
        from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork

        game = probe.context.paths.game_db
        with DatabaseUnitOfWork(game) as uow:
            apply_fusion_operations(uow)
        item_id, item = module._items().get_data_by_item_name("天罪")
        recipe = item["fusion"]
        assert int(item_id) == 7084 and int(recipe["need_stone"]) == 0
        probe.player(level=recipe["need_rank"], exp=0, stone=0)
        for material_id, amount in recipe["need_item"].items():
            probe.grant_item(int(material_id), int(amount))
        before = (
            balances(probe),
            backpack(probe),
            probe.rows(game, "SELECT * FROM fusion_operations ORDER BY operation_id"),
        )
        probe.seed(
            game,
            "CREATE TRIGGER reject_fusion_receipt BEFORE INSERT ON fusion_operations "
            "BEGIN SELECT RAISE(IGNORE); END",
        )
        text = await probe.command(
            "强行合成", "强行合成 天罪 1", number=971001, allow_exception=True,
        )
        after = (
            balances(probe),
            backpack(probe),
            probe.rows(game, "SELECT * FROM fusion_operations ORDER BY operation_id"),
        )
        probe.evidence.update(reply=text, business_before=before, business_after=after,
                              operation_id="force-fusion:971001:900001")
        assert after == before, (text, before, after)
        assert reason_text("fusion_receipt_write_rejected") in text, text
        ledger = probe.rows(
            game,
            "SELECT status FROM operation_ledger WHERE operation_id=? AND action='fusion.apply'",
            ("force-fusion:971001:900001",),
        )
        assert ledger == [("started",)], ledger

        probe.seed(game, "DROP TRIGGER reject_fusion_receipt")
        recovered = await probe.command("强行合成", "强行合成 天罪 1", number=971001)
        saved = (
            balances(probe),
            backpack(probe),
            probe.rows(game, "SELECT * FROM fusion_operations ORDER BY operation_id"),
        )
        replay = await probe.command("强行合成", "强行合成 天罪 1", number=971001)
        assert "成功合成" in recovered and "成功合成" in replay, (recovered, replay)
        assert saved == (
            balances(probe),
            backpack(probe),
            probe.rows(game, "SELECT * FROM fusion_operations ORDER BY operation_id"),
        )
        assert probe.rows(game,
                          "SELECT successful,protected FROM fusion_operations WHERE operation_id=?",
                          ("force-fusion:971001:900001",)) == [(1, 0)]
        assert probe.rows(
            game,
            "SELECT status FROM operation_ledger WHERE operation_id=? AND action='fusion.apply'",
            ("force-fusion:971001:900001",),
        ) == [("applied",)]
        with DatabaseUnitOfWork(game, read_only=True) as uow:
            assert uow.query_one("PRAGMA integrity_check")["integrity_check"] == "ok"
        probe.evidence.update(recovered=True, replay_unchanged=True)
