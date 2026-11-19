"""Focused semantic coverage for ordinary parameterized commands not in core groups."""

import json

from tests.parameter_cases_assets import balances, backpack


def assert_dungeon_purchase_integrity(database):
    from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork

    with DatabaseUnitOfWork(database, read_only=True) as uow:
        assert uow.query_one("PRAGMA integrity_check") == {"integrity_check": "ok"}


def seed_accessory_document(probe, item_ids, *, equipped_slots=()):
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.item_json import Items

    catalog = Items()
    instances = []
    for index, item_id in enumerate(item_ids, start=1):
        item = catalog.get_data_by_item_id(item_id)
        assert item and item.get("type") == "饰品", (item_id, item)
        instances.append(
            {
                "uid": f"fixture-accessory-{index:02d}",
                "item_id": item_id,
                "name": item["name"],
                "part": item["part"],
                "set_type": item["set_type"],
                "quality": 1,
                "affixes": [],
                "locked_affixes": [],
                "wash_count": 0,
            }
        )
    equipped = {slot: None for slot in ("手镯", "戒指", "手环", "项链")}
    bag = instances[:]
    for slot, instance in zip(equipped_slots, instances):
        equipped[slot] = instance
        bag.remove(instance)
    probe.managers[1].save_doc(
        "900001",
        "player_accessory",
        {"equipped": equipped, "bag": bag},
        fields=["equipped", "bag"],
        dirty_check=False,
    )
    return instances


def accessory_projection(probe):
    return probe.rows(
        probe.context.paths.player_db,
        "SELECT equipped,bag FROM player_accessory WHERE user_id='900001'",
    )


async def check_accessory_equipment_writes(probe, *, equip):
    player = probe.context.paths.player_db
    command = "装备饰品" if equip else "卸下饰品"
    subject = "fixture-accessory-02" if equip else "手镯"
    request = f"{command} {subject}"
    success = "已装备：" if equip else "已卸下："
    unchanged_assets = balances(probe), backpack(probe)
    probe.seed(player, "CREATE TABLE isolated_accessory_effect (effect TEXT)")
    faults = (
        ("BEFORE", "SELECT RAISE(IGNORE)", "player_document_write_rejected"),
        ("AFTER", "UPDATE player_accessory SET bag='[]' WHERE user_id=NEW.user_id",
         "player_document_projection_mismatch"),
        ("BEFORE", "SELECT RAISE(ABORT,'isolated accessory write abort')", None),
    )
    for index, (timing, fault, reason) in enumerate(faults, start=970300):
        instances = seed_accessory_document(
            probe, [19101, 19001, 19002], equipped_slots=("手镯",),
        )
        before = accessory_projection(probe)
        probe.seed(
            player,
            f"CREATE TRIGGER reject_accessory_write {timing} UPDATE OF equipped ON player_accessory "
            f"BEGIN INSERT INTO isolated_accessory_effect VALUES('trigger'); {fault}; END",
        )
        text = await probe.command(command, request, number=index, allow_exception=reason is None)
        assert accessory_projection(probe) == before, text
        assert not probe.rows(player, "SELECT effect FROM isolated_accessory_effect"), text
        assert success not in text, text
        if reason is None:
            errors = "\n".join(probe.calls[-1]["errors"])
            assert "sqlite3.IntegrityError: isolated accessory write abort" in errors, errors
        else:
            assert reason in text and subject in text, text

        probe.seed(player, "DROP TRIGGER reject_accessory_write")
        recovered = await probe.command(command, request, number=index)
        assert success in recovered, recovered
        equipped_value, bag_value = accessory_projection(probe)[0]
        expected_equipped = {"手镯": instances[1] if equip else None, "戒指": None, "手环": None, "项链": None}
        expected_bag = [instances[2], instances[0]] if equip else [instances[1], instances[2], instances[0]]
        assert json.loads(equipped_value) == expected_equipped
        assert json.loads(bag_value) == expected_bag
        after = accessory_projection(probe)
        replay = await probe.command(command, request, number=index)
        assert ("未找到该饰品UID" if equip else "手镯未装备饰品") in replay, replay
        assert accessory_projection(probe) == after

    assert (balances(probe), backpack(probe)) == unchanged_assets
    assert_dungeon_purchase_integrity(player)
    probe.evidence.update(ignore=True, rewrite=True, abort=True, recovered=True, replay_unchanged=True)


async def check_accessory_affix_writes(probe, *, unlock):
    player, game = probe.context.paths.player_db, probe.context.paths.game_db
    command, action = ("饰品解锁", "unlock") if unlock else ("饰品锁定", "lock")
    index_token = "1" if unlock else "2"
    request = f"{command} fixture-accessory-01 {index_token}"
    success = "已解锁：" if unlock else "已锁定："
    phrases = {
        "accessory_affix_state_write_rejected": "饰品词条状态保存失败",
        "accessory_affix_state_projection_mismatch": "饰品词条状态与请求不一致",
        "accessory_affix_receipt_write_rejected": "饰品词条操作回执保存失败",
        "accessory_affix_receipt_projection_mismatch": "饰品词条操作回执不一致",
    }
    faults = (
        (player, "BEFORE UPDATE OF equipped ON player_accessory",
         "SELECT RAISE(IGNORE)", "accessory_affix_state_write_rejected"),
        (player, "AFTER UPDATE OF equipped ON player_accessory",
         "UPDATE player_accessory SET bag='[]' WHERE user_id=NEW.user_id",
         "accessory_affix_state_projection_mismatch"),
        (game, "BEFORE INSERT ON accessory_transaction_operations",
         "SELECT RAISE(IGNORE)", "accessory_affix_receipt_write_rejected"),
        (game, "AFTER INSERT ON accessory_transaction_operations",
         "UPDATE accessory_transaction_operations SET result_json='{}' WHERE operation_id=NEW.operation_id",
         "accessory_affix_receipt_projection_mismatch"),
        (game, "BEFORE INSERT ON accessory_transaction_operations",
         "SELECT RAISE(ABORT,'isolated accessory affix abort')", None),
    )
    unchanged_assets = balances(probe), backpack(probe)
    for database in (game, player):
        probe.seed(database, "CREATE TABLE isolated_affix_effect (effect INTEGER)")
    receipt_sql = (
        "SELECT operation_id,action,payload,result_json "
        "FROM accessory_transaction_operations ORDER BY operation_id"
    )
    for number, (database, event, fault, reason) in enumerate(faults, start=970400):
        instances = seed_accessory_document(probe, [19001])
        accessory = instances[0]
        accessory["affixes"] = [
            {"type": "攻击", "value": 0.1},
            {"type": "速度", "value": 20},
        ]
        if unlock:
            accessory["locked_affixes"] = [0]
        bag = json.loads(accessory_projection(probe)[0][1])
        bag[0] = accessory
        probe.seed(player, "UPDATE player_accessory SET bag=? WHERE user_id='900001'",
                   (json.dumps(bag, ensure_ascii=False),))
        before = accessory_projection(probe)
        prior_receipts = probe.rows(game, receipt_sql)
        probe.seed(
            database,
            f"CREATE TRIGGER reject_affix {event} "
            f"BEGIN INSERT INTO isolated_affix_effect VALUES(1); {fault}; END",
        )
        text = await probe.command(command, request, number=number, allow_exception=reason is None)
        failed_state = accessory_projection(probe)
        failed_receipts = probe.rows(game, receipt_sql)
        probe.evidence["last_failure"] = {
            "fault": fault,
            "state_unchanged": failed_state == before,
            "receipt_delta": len(failed_receipts) - len(prior_receipts),
        }
        if reason is None:
            errors = "\n".join(probe.calls[-1]["errors"])
            assert "sqlite3.IntegrityError: isolated accessory affix abort" in errors, errors
        else:
            assert phrases[reason] in text and "fixture-accessory-01" in text, text
        assert success not in text and failed_state == before, (text, before, failed_state)
        assert failed_receipts == prior_receipts and not probe.rows(database, "SELECT effect FROM isolated_affix_effect")
        probe.seed(database, "DROP TRIGGER reject_affix")
        recovered = await probe.command(command, request, number=number)
        assert success in recovered, recovered
        stored = json.loads(accessory_projection(probe)[0][1])[0]
        assert stored.get("locked_affixes", []) == ([1] if not unlock else []), stored
        receipt = probe.rows(game, receipt_sql)
        [(operation_id, saved_action, payload, result)] = [row for row in receipt if row not in prior_receipts]
        assert operation_id == f"accessory:{number}:{action}:900001:fixture-accessory-01"
        assert saved_action == action and json.loads(payload)["locked_indexes"] == stored.get("locked_affixes", [])
        assert json.loads(result)["accessory"] == stored
        after = accessory_projection(probe)
        replay = await probe.command(command, request, number=number)
        assert success in replay and accessory_projection(probe) == after
        assert probe.rows(game, receipt_sql) == receipt
    assert (balances(probe), backpack(probe)) == unchanged_assets
    assert_dungeon_purchase_integrity(game)
    assert_dungeon_purchase_integrity(player)
    probe.evidence.update(state_ignore=True, state_rewrite=True, receipt_ignore=True, receipt_rewrite=True,
                          abort=True, recovered=True, replay_unchanged=True)


async def check_accessory_wash_writes(probe):
    player, game = probe.context.paths.player_db, probe.context.paths.game_db
    command = "饰品洗练"
    uid = "fixture-accessory-01"
    request = f"{command} {uid}"
    phrases = {
        "accessory_wash_stone_write_rejected": "洗练石扣除未能保存",
        "accessory_wash_stone_projection_mismatch": "洗练石数量与扣除请求不一致",
        "accessory_wash_state_write_rejected": "饰品洗练状态保存失败",
        "accessory_wash_state_projection_mismatch": "饰品洗练状态与请求不一致",
        "accessory_wash_receipt_write_rejected": "饰品洗练操作回执保存失败",
        "accessory_wash_receipt_projection_mismatch": "饰品洗练操作回执不一致",
    }
    faults = (
        (game, "BEFORE UPDATE OF goods_num ON back", "SELECT RAISE(IGNORE)",
         "accessory_wash_stone_write_rejected"),
        (game, "AFTER UPDATE OF goods_num ON back",
         "UPDATE back SET goods_num=0 WHERE user_id=NEW.user_id AND goods_id=NEW.goods_id",
         "accessory_wash_stone_projection_mismatch"),
        (player, "BEFORE UPDATE OF equipped ON player_accessory", "SELECT RAISE(IGNORE)",
         "accessory_wash_state_write_rejected"),
        (player, "AFTER UPDATE OF equipped ON player_accessory",
         "UPDATE player_accessory SET bag='[]' WHERE user_id=NEW.user_id",
         "accessory_wash_state_projection_mismatch"),
        (game, "BEFORE INSERT ON accessory_transaction_operations", "SELECT RAISE(IGNORE)",
         "accessory_wash_receipt_write_rejected"),
        (game, "AFTER INSERT ON accessory_transaction_operations",
         "UPDATE accessory_transaction_operations SET result_json='{}' WHERE operation_id=NEW.operation_id",
         "accessory_wash_receipt_projection_mismatch"),
        (game, "BEFORE INSERT ON accessory_transaction_operations",
         "SELECT RAISE(ABORT,'isolated accessory wash abort')", None),
    )
    probe.grant_item(20023, 20)
    probe.seed(game, "CREATE TABLE isolated_wash_effect (effect INTEGER)")
    probe.seed(player, "CREATE TABLE isolated_wash_effect (effect INTEGER)")
    before_assets = balances(probe), backpack(probe)
    receipt_sql = "SELECT operation_id,action,payload,result_json FROM accessory_transaction_operations ORDER BY operation_id"
    baseline_stones = probe.rows(
        game,
        "SELECT goods_num,bind_num FROM back WHERE user_id='900001' AND goods_id=20023",
    )[0]

    for number, (database, event, fault, reason) in enumerate(faults, start=970500):
        accessory = seed_accessory_document(probe, [19001])[0]
        accessory["affixes"] = [
            {"type": "攻击", "value": 0.1},
            {"type": "速度", "value": 20},
        ]
        bag = [accessory]
        probe.seed(player, "UPDATE player_accessory SET bag=? WHERE user_id='900001'",
                   (json.dumps(bag, ensure_ascii=False),))
        before_state = accessory_projection(probe)
        stone_sql = "SELECT goods_num,bind_num FROM back WHERE user_id='900001' AND goods_id=20023"
        before_stones = probe.rows(game, stone_sql)
        before_receipts = probe.rows(game, receipt_sql)
        probe.seed(database, f"CREATE TRIGGER reject_wash {event} "
                   f"BEGIN INSERT INTO isolated_wash_effect VALUES(1); {fault}; END")
        text = await probe.command(command, request, number=number, allow_exception=reason is None)
        if reason is None:
            errors = "\n".join(probe.calls[-1]["errors"])
            assert "sqlite3.IntegrityError: isolated accessory wash abort" in errors, errors
        else:
            assert phrases[reason] in text and uid in text, text
        assert "洗练完成：" not in text, text
        assert accessory_projection(probe) == before_state
        assert probe.rows(game, stone_sql) == before_stones
        assert probe.rows(game, receipt_sql) == before_receipts
        assert not probe.rows(database, "SELECT effect FROM isolated_wash_effect")

        probe.seed(database, "DROP TRIGGER reject_wash")
        recovered = await probe.command(command, request, number=number)
        assert "洗练完成：" in recovered, recovered
        after_state = accessory_projection(probe)
        after_stones = probe.rows(game, stone_sql)
        after_receipts = probe.rows(game, receipt_sql)
        assert int(after_stones[0][0]) == int(before_stones[0][0]) - 1
        updated = json.loads(after_state[0][1])[0]
        assert updated["wash_count"] == 1 and len(updated["affixes"]) == 2
        receipt = next(row for row in after_receipts if row not in before_receipts)
        assert receipt[0] == f"accessory:{number}:wash:900001:{uid}"
        assert json.loads(receipt[2])["expected_stones"] == int(before_stones[0][0])
        assert json.loads(receipt[3])["accessory"] == updated
        replay = await probe.command(command, request, number=number)
        assert "洗练完成：" in replay
        assert accessory_projection(probe) == after_state
        assert probe.rows(game, stone_sql) == after_stones
        assert probe.rows(game, receipt_sql) == after_receipts
        probe.seed(
            game,
            "UPDATE back SET goods_num=?,bind_num=? WHERE user_id='900001' AND goods_id=20023",
            baseline_stones,
        )
    assert (balances(probe), backpack(probe)) == before_assets, (
        before_assets,
        (balances(probe), backpack(probe)),
    )
    assert_dungeon_purchase_integrity(game)
    assert_dungeon_purchase_integrity(player)
    probe.evidence.update(stone_write=True, stone_projection=True, state_write=True,
                          state_projection=True, receipt_write=True, receipt_projection=True,
                          abort=True, recovery=True, replay_unchanged=True)


async def check_accessory_decompose_writes(probe, *, batch=False):
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_back.accessory import ACCESSORY_DECOMPOSE_GAIN

    player, game = probe.context.paths.player_db, probe.context.paths.game_db
    stone_sql = (
        "SELECT goods_name,goods_type,goods_num,bind_num FROM back "
        "WHERE user_id='900001' AND goods_id=20023"
    )
    receipt_sql = (
        "SELECT operation_id,action,payload,result_json "
        "FROM accessory_transaction_operations ORDER BY operation_id"
    )
    phrases = {
        "accessory_decompose_stone_write_rejected": "分解所得洗练石未能保存",
        "accessory_decompose_stone_projection_mismatch": "分解所得洗练石与应得数量不一致",
        "accessory_decompose_state_write_rejected": "饰品分解状态保存失败",
        "accessory_decompose_state_projection_mismatch": "饰品分解状态与请求不一致",
        "accessory_decompose_receipt_write_rejected": "饰品分解操作回执保存失败",
        "accessory_decompose_receipt_projection_mismatch": "饰品分解操作回执不一致",
    }
    faults = (
        (game, False, "BEFORE INSERT ON back", "SELECT RAISE(IGNORE)",
         "accessory_decompose_stone_write_rejected"),
        (game, False, "AFTER INSERT ON back",
         "UPDATE back SET bind_num=0 WHERE user_id=NEW.user_id AND goods_id=NEW.goods_id",
         "accessory_decompose_stone_projection_mismatch"),
        (game, True, "BEFORE UPDATE OF goods_num ON back", "SELECT RAISE(IGNORE)",
         "accessory_decompose_stone_write_rejected"),
        (game, True, "AFTER UPDATE OF goods_num ON back",
         "UPDATE back SET goods_num=0 WHERE user_id=NEW.user_id AND goods_id=NEW.goods_id",
         "accessory_decompose_stone_projection_mismatch"),
        (player, True, "BEFORE UPDATE OF equipped ON player_accessory", "SELECT RAISE(IGNORE)",
         "accessory_decompose_state_write_rejected"),
        (player, True, "AFTER UPDATE OF equipped ON player_accessory",
         "UPDATE player_accessory SET bag='[]' WHERE user_id=NEW.user_id",
         "accessory_decompose_state_projection_mismatch"),
        (game, True, "BEFORE INSERT ON accessory_transaction_operations", "SELECT RAISE(IGNORE)",
         "accessory_decompose_receipt_write_rejected"),
        (game, True, "AFTER INSERT ON accessory_transaction_operations",
         "UPDATE accessory_transaction_operations SET result_json='{}' WHERE operation_id=NEW.operation_id",
         "accessory_decompose_receipt_projection_mismatch"),
        (game, True, "AFTER INSERT ON accessory_transaction_operations",
         "UPDATE back SET bind_num=0 WHERE user_id='900001' AND goods_id=20023",
         "accessory_decompose_stone_projection_mismatch"),
        (player, True, "AFTER UPDATE OF equipped ON player_accessory",
         "UPDATE player_accessory SET equipped='{}' WHERE user_id=NEW.user_id",
         "accessory_decompose_state_projection_mismatch"),
        (game, True, "BEFORE INSERT ON accessory_transaction_operations",
         "SELECT RAISE(ABORT,'isolated accessory decompose abort')", None),
    )
    for database in (game, player):
        probe.seed(database, "CREATE TABLE isolated_decompose_effect (effect INTEGER)")
    initial_balances = balances(probe)
    initial_backpack = [row for row in backpack(probe) if int(row[1]) != 20023]
    for number, (database, existing, event, fault, reason) in enumerate(faults, start=970600):
        if batch and reason:
            reason = reason.replace("accessory_decompose", "accessory_batch_decompose")
        instances = seed_accessory_document(
            probe, [19001, 19002, 19003, *([19101] if batch else [])], equipped_slots=("手镯",),
        )
        target = instances[1]
        target_uid = ",".join(item["uid"] for item in instances[1:3]) if batch else target["uid"]
        command = "快速分解饰品" if batch else "饰品分解"
        command_text = "快速分解饰品 烈阳 1" if batch else f"饰品分解 {target_uid}"
        probe.seed(game, "DELETE FROM back WHERE user_id='900001' AND goods_id=20023")
        if existing:
            probe.seed(game, "INSERT INTO back(user_id,goods_id,goods_name,goods_type,goods_num,bind_num) "
                       "VALUES('900001',20023,'洗练石','特殊道具',5,2)")
        before_state = accessory_projection(probe)
        before_stones = probe.rows(game, stone_sql)
        before_receipts = probe.rows(game, receipt_sql)
        probe.seed(database, f"CREATE TRIGGER reject_decompose {event} "
                   f"BEGIN INSERT INTO isolated_decompose_effect VALUES(1); {fault}; END")
        probe.clear_cooldowns()
        text = await probe.command(command, command_text, number=number, allow_exception=reason is None)
        failed_state = accessory_projection(probe)
        failed_stones = probe.rows(game, stone_sql)
        failed_receipts = probe.rows(game, receipt_sql)
        probe.evidence["last_failure"] = {
            "event": event, "reason": reason, "reply": text,
            "state_unchanged": failed_state == before_state,
            "stones_before": before_stones, "stones_after": failed_stones,
            "receipt_delta": len(failed_receipts) - len(before_receipts),
        }
        if reason is None:
            errors = "\n".join(probe.calls[-1]["errors"])
            assert "sqlite3.IntegrityError: isolated accessory decompose abort" in errors, errors
        else:
            phrase = phrases[reason.replace("accessory_batch_decompose", "accessory_decompose")]
            if batch:
                phrase = phrase.replace("饰品分解", "饰品快速分解")
            assert phrase in text and target_uid in text, text
        success_marker = "快速分解完成：" if batch else "已分解："
        assert success_marker not in text, text
        assert failed_state == before_state and failed_stones == before_stones
        assert failed_receipts == before_receipts
        assert not probe.rows(database, "SELECT effect FROM isolated_decompose_effect")

        probe.seed(database, "DROP TRIGGER reject_decompose")
        probe.clear_cooldowns()
        recovered = await probe.command(command, command_text, number=number)
        assert success_marker in recovered, recovered
        after_state = accessory_projection(probe)
        after_stones = probe.rows(game, stone_sql)
        after_receipts = probe.rows(game, receipt_sql)
        gain = ACCESSORY_DECOMPOSE_GAIN[target["quality"]] * (2 if batch else 1)
        assert json.loads(after_state[0][0]) == json.loads(before_state[0][0])
        assert json.loads(after_state[0][1]) == [instances[3] if batch else instances[2]]
        assert after_stones == [("洗练石", "特殊道具", (5 if existing else 0) + gain,
                                 (2 if existing else 0) + gain)]
        [receipt] = [row for row in after_receipts if row not in before_receipts]
        if batch:
            assert receipt[0] == f"accessory:{number}:batch-decompose:900001:烈阳:1"
            assert json.loads(receipt[2])["selected_uids"] == target_uid.split(",")
            assert json.loads(receipt[3])["affected"] == 2
        else:
            assert receipt[0] == f"accessory:{number}:decompose:900001:{target_uid}"
            assert json.loads(receipt[2])["expected_accessory"] == target
            assert json.loads(receipt[3])["accessory"] == target
        probe.clear_cooldowns()
        replay = await probe.command(command, command_text, number=number)
        assert success_marker in replay, replay
        assert accessory_projection(probe) == after_state
        assert probe.rows(game, stone_sql) == after_stones
        assert probe.rows(game, receipt_sql) == after_receipts
    assert balances(probe) == initial_balances
    assert [row for row in backpack(probe) if int(row[1]) != 20023] == initial_backpack
    assert_dungeon_purchase_integrity(game)
    assert_dungeon_purchase_integrity(player)
    probe.evidence.update(stone_insert=True, stone_update=True, state_write=True, receipt_write=True,
                          projection_rewrites=True, abort=True, recovered=True, replay_unchanged=True)


def register(case):
    @case(
        "accessory-bag-page-real",
        "饰品背包",
        "页码1",
        "注册玩家和启动迁移",
        "按指定页读取饰品背包，不改变资产",
        group="remaining-player-parameters",
    )
    async def accessory_bag_page(probe):
        item_ids = [
            *range(19001, 19005),
            *range(19101, 19105),
            *range(19201, 19205),
            *range(19301, 19305),
            19401,
        ]
        instances = seed_accessory_document(probe, item_ids)
        before = accessory_projection(probe)
        page_one = await probe.command("饰品背包", "饰品背包 1", number=970200)
        page_two = await probe.command("饰品背包", "饰品背包 2", number=970201)
        filtered = await probe.command("饰品背包", "饰品背包 烈阳", number=970202)
        clamped = await probe.command("饰品背包", "饰品背包 99", number=970203)
        after = accessory_projection(probe)
        assert "第 1/2 页" in page_one, page_one
        assert "第 2/2 页" in page_two and "烈阳" not in page_two, page_two
        assert "第 1/1 页" in filtered and "烈阳手镯" in filtered and "玄渊" not in filtered, filtered
        assert "第 2/2 页" in clamped, clamped
        assert after == before and len(instances) == 17
        probe.evidence.update(
            actual_rows=len(instances),
            page_one=True,
            page_two=True,
            keyword_filter=True,
            page_clamp=True,
            read_only=after == before,
        )

    @case(
        "accessory-collection-real-set",
        "饰品图鉴",
        "真实套装名",
        "随包饰品套装配置",
        "按真实套装筛选图鉴，不改变资产",
        group="remaining-player-parameters",
    )
    async def accessory_collection(probe):
        instances = seed_accessory_document(
            probe,
            [19001, 19002, 19003, 19004],
            equipped_slots=("手镯",),
        )
        before = accessory_projection(probe)
        text = await probe.command("饰品图鉴", "饰品图鉴 烈阳")
        after = accessory_projection(probe)
        assert "【烈阳套装图鉴】" in text and "已装备1/4件" in text, text
        assert "一阶：已集齐" in text and "烈阳手镯" not in text, text
        assert after == before and len(instances) == 4
        probe.evidence.update(set_name="烈阳", actual_rows=len(instances), equipped=1, readonly=after == before)

    @case(
        "accessory-equip-write-rejection",
        "装备饰品",
        "有效背包UID",
        "隔离player_accessory已有装备、待替换饰品和写入故障",
        "拒写完整回滚并可重试替换装备，重放不改变状态",
        group="remaining-player-parameters",
        outcome="rejection",
    )
    async def accessory_equip_write_rejection(probe):
        await check_accessory_equipment_writes(probe, equip=True)

    @case(
        "accessory-unequip-write-rejection",
        "卸下饰品",
        "有效已装备部位",
        "隔离player_accessory已有装备、背包数据和写入故障",
        "拒写完整回滚并可重试卸下装备，重放不改变状态",
        group="remaining-player-parameters",
        outcome="rejection",
    )
    async def accessory_unequip_write_rejection(probe):
        await check_accessory_equipment_writes(probe, equip=False)

    @case(
        "accessory-affix-lock-write-recovery",
        "饰品锁定",
        "真实UID与可锁定词条序号",
        "合法饰品、实际词条、player SQLite和永久操作回执",
        "拒写/改写保留具体原因、整笔回滚，ABORT原异常并可恢复重放",
        group="remaining-player-parameters",
        outcome="rejection",
    )
    async def accessory_affix_lock_write_recovery(probe):
        await check_accessory_affix_writes(probe, unlock=False)

    @case(
        "accessory-affix-unlock-write-recovery",
        "饰品解锁",
        "真实UID与当前锁定词条序号",
        "合法饰品、既有锁定词条、player SQLite和永久操作回执",
        "拒写/改写保留具体原因、整笔回滚，ABORT原异常并可恢复重放",
        group="remaining-player-parameters",
        outcome="rejection",
    )
    async def accessory_affix_unlock_write_recovery(probe):
        await check_accessory_affix_writes(probe, unlock=True)

    @case(
        "accessory-wash-write-recovery",
        "饰品洗练",
        "真实饰品UID，使用洗练石",
        "合法饰品实际词条、洗练石库存、player/game隔离SQLite和永久回执",
        "石料、饰品状态、回执拒写/改写均保留原因且整笔回滚，ABORT原样并可恢复重放",
        group="remaining-player-parameters",
        outcome="rejection",
    )
    async def accessory_wash_write_recovery(probe):
        await check_accessory_wash_writes(probe)

    @case(
        "accessory-decompose-write-recovery",
        "饰品分解",
        "真实背包饰品UID",
        "合法背包与已装备饰品，洗练石有/无库存，隔离game/player SQLite",
        "石料、饰品状态、回执拒写/改写均具体拒绝且整笔回滚，ABORT原样并可恢复重放",
        group="remaining-player-parameters",
        outcome="rejection",
    )
    async def accessory_decompose_write_recovery(probe):
        await check_accessory_decompose_writes(probe)

    @case(
        "accessory-batch-decompose-write-recovery",
        "快速分解饰品",
        "烈阳 1",
        "两件匹配饰品、一件不匹配饰品、一件已装备饰品和隔离game/player SQLite",
        "批量返还/状态/回执拒写改写整笔回滚，具体原因、恢复、replay与ABORT",
        group="remaining-player-parameters",
        outcome="rejection",
    )
    async def accessory_batch_decompose_write_recovery(probe):
        await check_accessory_decompose_writes(probe, batch=True)

    @case(
        "accessory-batch-decompose-stable-replay",
        "快速分解饰品",
        "烈阳 1，同消息重放时背包已新增匹配饰品",
        "合法匹配饰品，第一次成功后新增另一合法UID",
        "重放历史批量结果，不把新到饰品再次分解或新增第二笔回执",
        group="remaining-player-parameters",
    )
    async def accessory_batch_decompose_stable_replay(probe):
        player, game = probe.context.paths.player_db, probe.context.paths.game_db
        seed_accessory_document(probe, [19002])
        first = await probe.command("快速分解饰品", "快速分解饰品 烈阳 1", number=970630)
        assert "快速分解完成：1件" in first, first
        [new_item] = seed_accessory_document(probe, [19002])
        new_item["uid"] = "new-arrival-accessory"
        probe.seed(player, "UPDATE player_accessory SET bag=? WHERE user_id='900001'",
                   (json.dumps([new_item], ensure_ascii=False),))
        receipt_sql = "SELECT operation_id,payload,result_json FROM accessory_transaction_operations ORDER BY operation_id"
        before = accessory_projection(probe), backpack(probe), probe.rows(game, receipt_sql)
        probe.clear_cooldowns()
        replay = await probe.command("快速分解饰品", "快速分解饰品 烈阳 1", number=970630)
        after = accessory_projection(probe), backpack(probe), probe.rows(game, receipt_sql)
        probe.evidence.update(reply=replay, state_unchanged=after[0] == before[0],
                              stones_unchanged=after[1] == before[1], receipt_delta=len(after[2]) - len(before[2]))
        assert after == before and "快速分解完成：1件" in replay, probe.evidence

    @case(
        "accessory-preset-save-write-recovery",
        "饰品预设",
        "1 / 预设饰品 1",
        "一件实际已装备饰品、三个预设栏和 game/player 隔离 SQLite",
        "拒写整笔回滚并保留具体原因，恢复后同消息保存且 replay 不重复写",
        group="remaining-player-parameters",
        outcome="rejection",
    )
    async def accessory_preset_save_write_recovery(probe):
        player, game = probe.context.paths.player_db, probe.context.paths.game_db
        assets = balances(probe), backpack(probe)
        instances = seed_accessory_document(probe, [19001], equipped_slots=("手镯",))
        empty_preset = {slot: None for slot in ("手镯", "戒指", "手环", "项链")}
        probe.seed(player, "UPDATE player_accessory SET preset_1=? WHERE user_id='900001'",
                   (json.dumps(empty_preset, ensure_ascii=False),))
        receipt_sql = (
            "SELECT operation_id,action,payload,result_json FROM accessory_transaction_operations "
            "WHERE action='save_preset' ORDER BY operation_id"
        )
        state_sql = "SELECT equipped,bag,preset_1 FROM player_accessory WHERE user_id='900001'"
        faults = (
            (player, "BEFORE UPDATE OF preset_1 ON player_accessory", "SELECT RAISE(IGNORE)",
             "accessory_preset_state_write_rejected", "保存失败", "饰品预设"),
            (player, "AFTER UPDATE OF preset_1 ON player_accessory",
             "UPDATE player_accessory SET preset_1='{}' WHERE user_id=NEW.user_id",
             "accessory_preset_state_projection_mismatch", "预设状态与保存请求不一致", "饰品预设"),
            (game, "BEFORE INSERT ON accessory_transaction_operations", "SELECT RAISE(IGNORE)",
             "accessory_preset_receipt_write_rejected", "回执保存失败", "预设饰品"),
            (game, "AFTER INSERT ON accessory_transaction_operations",
             "UPDATE accessory_transaction_operations SET result_json='{}' WHERE operation_id=NEW.operation_id",
             "accessory_preset_receipt_projection_mismatch", "预设操作回执内容不一致", "饰品预设"),
            (game, "BEFORE INSERT ON accessory_transaction_operations",
             "SELECT RAISE(ABORT,'isolated preset abort')", None, None, "饰品预设"),
        )
        for number, (database, event, fault, reason, phrase, command) in enumerate(faults, start=970640):
            before_state = probe.rows(player, state_sql)
            before_receipts = probe.rows(game, receipt_sql)
            probe.seed(database,
                       f"CREATE TRIGGER reject_accessory_preset {event} BEGIN {fault}; END")
            failed = await probe.command(command, f"{command} 1", number=number, allow_exception=reason is None)
            after_state = probe.rows(player, state_sql)
            after_receipts = probe.rows(game, receipt_sql)
            probe.evidence[reason] = {
                "reply": failed,
                "state_unchanged": after_state == before_state,
                "receipt_unchanged": after_receipts == before_receipts,
            }
            if reason is None:
                errors = "\n".join(probe.calls[-1]["errors"])
                assert "sqlite3.IntegrityError: isolated preset abort" in errors, errors
            else:
                assert phrase in failed and "饰品预设1" in failed, (reason, failed)
            assert "已保存当前装备" not in failed, failed
            assert after_state == before_state and after_receipts == before_receipts
            probe.seed(database, "DROP TRIGGER reject_accessory_preset")
            probe.clear_cooldowns()
            recovered = await probe.command(command, f"{command} 1", number=number)
            after_state = probe.rows(player, state_sql)
            after_receipts = probe.rows(game, receipt_sql)
            assert "已保存当前装备到饰品预设1" in recovered, recovered
            assert json.loads(after_state[0][0])["手镯"] == instances[0]
            assert json.loads(after_state[0][2]) == {
                "手镯": instances[0]["uid"], "戒指": None, "手环": None, "项链": None,
            }
            assert len(after_receipts) == len(before_receipts) + 1
            [receipt] = [row for row in after_receipts if row not in before_receipts]
            assert receipt[0] == f"accessory:{number}:save_preset:900001:1"
            assert json.loads(receipt[2])["preset_idx"] == 1
            assert json.loads(receipt[3])["details"]["preset"] == json.loads(after_state[0][2])
            replay_before = after_state, after_receipts
            probe.clear_cooldowns()
            replay = await probe.command(command, f"{command} 1", number=number)
            assert "已保存当前装备到饰品预设1" in replay, replay
            assert (probe.rows(player, state_sql), probe.rows(game, receipt_sql)) == replay_before
            probe.seed(player, "UPDATE player_accessory SET preset_1=? WHERE user_id='900001'",
                       (json.dumps(empty_preset, ensure_ascii=False),))
            assert probe.rows(player, state_sql) == before_state
        for database in (game, player):
            assert_dungeon_purchase_integrity(database)
        assert (balances(probe), backpack(probe)) == assets
        probe.evidence.update(
            equipped_uid=instances[0]["uid"],
            state_rejected=True,
            receipt_rejected=True,
            recovered=True,
            replay_unchanged=True,
        )

    @case(
        "accessory-preset-parameters-readonly",
        "饰品预设",
        "查看、1/2/3、别名、非法参数",
        "两件实际已装备饰品和 game/player 隔离 SQLite",
        "查询只读，合法序号覆盖并持久化，非法参数明确拒绝且零写",
        group="remaining-player-parameters",
    )
    async def accessory_preset_parameters_readonly(probe):
        player, game = probe.context.paths.player_db, probe.context.paths.game_db
        instances = seed_accessory_document(probe, [19001, 19002], equipped_slots=("手镯", "戒指"))
        empty = {slot: None for slot in ("手镯", "戒指", "手环", "项链")}
        old = dict(empty, 手镯="old-accessory")
        valid = dict(empty, 手镯=instances[0]["uid"], 戒指=instances[1]["uid"])
        probe.seed(player, "UPDATE player_accessory SET preset_1=?,preset_2=? WHERE user_id='900001'",
                   (json.dumps(old, ensure_ascii=False), json.dumps(valid, ensure_ascii=False)))
        state_sql = (
            "SELECT equipped,bag,preset_1,preset_2,preset_3 FROM player_accessory "
            "WHERE user_id='900001'"
        )
        receipt_sql = (
            "SELECT operation_id,action,payload,result_json FROM accessory_transaction_operations "
            "WHERE action='save_preset' ORDER BY operation_id"
        )
        before_state, before_receipts = probe.rows(player, state_sql), probe.rows(game, receipt_sql)
        view = await probe.command("饰品预设", "饰品预设")
        alias_view = await probe.command("预设饰品", "预设饰品")
        assert "【预设1】" in view and "【预设3】" in alias_view, (view, alias_view)
        assert all(instance["name"] in reply for instance in instances for reply in (view, alias_view)), (
            view, alias_view,
        )
        assert (probe.rows(player, state_sql), probe.rows(game, receipt_sql)) == (before_state, before_receipts)

        for index, command in ((1, "预设饰品"), (2, "饰品预设"), (3, "饰品预设")):
            probe.clear_cooldowns()
            reply = await probe.command(command, f"{command} {index}", number=970660 + index)
            assert f"已保存当前装备到饰品预设{index}" in reply, reply
            if index == 1:
                assert "本次已覆盖" in reply, reply
            saved = probe.rows(player, state_sql)[0]
            assert json.loads(saved[index + 1]) == {
                "手镯": instances[0]["uid"], "戒指": instances[1]["uid"],
                "手环": None, "项链": None,
            }
        saved_state, saved_receipts = probe.rows(player, state_sql), probe.rows(game, receipt_sql)
        assert len(saved_receipts) == 3
        for index, invalid in enumerate(("0", "4", "abc", "1 2"), start=1):
            probe.clear_cooldowns()
            reply = await probe.command("饰品预设", f"饰品预设 {invalid}", number=970670 + index)
            assert "用法：饰品预设 1/2/3" in reply, reply
            assert (probe.rows(player, state_sql), probe.rows(game, receipt_sql)) == (
                saved_state, saved_receipts,
            )
        for database in (game, player):
            assert_dungeon_purchase_integrity(database)
        probe.evidence.update(
            equipped=[instance["uid"] for instance in instances],
            query_readonly=True,
            alias=True,
            saved_indices=[1, 2, 3],
            invalid_parameters=4,
            receipts=len(saved_receipts),
        )

    @case(
        "accessory-decompose-legal-boundaries",
        "饰品分解",
        "真实背包/已装备饰品UID",
        "合法饰品实例和达到/恰好未达上限的洗练石库存",
        "已装备与容量拒绝零写，恰好达到上限成功返还绑定石料",
        group="remaining-player-parameters",
    )
    async def accessory_decompose_boundaries(probe):
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_config import XiuConfig

        game = probe.context.paths.game_db
        instances = seed_accessory_document(probe, [19001, 19002], equipped_slots=("手镯",))
        maximum = int(XiuConfig().max_goods_num)
        probe.seed(game, "INSERT INTO back(user_id,goods_id,goods_name,goods_type,goods_num,bind_num) "
                   "VALUES('900001',20023,'洗练石','特殊道具',?,0) ON CONFLICT(user_id,goods_id) "
                   "DO UPDATE SET goods_num=excluded.goods_num,bind_num=0", (maximum,))
        before = accessory_projection(probe), backpack(probe), balances(probe)
        receipt_sql = "SELECT * FROM accessory_transaction_operations ORDER BY operation_id"
        receipts = probe.rows(game, receipt_sql)
        probe.clear_cooldowns()
        equipped = await probe.command("饰品分解", f"饰品分解 {instances[0]['uid']}", number=970620)
        assert "已装备饰品请先卸下" in equipped, equipped
        probe.clear_cooldowns()
        full = await probe.command("饰品分解", f"饰品分解 {instances[1]['uid']}", number=970621)
        assert "洗练石已达背包上限" in full, full
        assert (accessory_projection(probe), backpack(probe), balances(probe)) == before
        assert probe.rows(game, receipt_sql) == receipts
        probe.seed(game, "UPDATE back SET goods_num=? WHERE user_id='900001' AND goods_id=20023", (maximum - 1,))
        probe.clear_cooldowns()
        success = await probe.command("饰品分解", f"饰品分解 {instances[1]['uid']}", number=970621)
        assert "已分解：" in success and "获得洗练石：1个" in success, success
        assert probe.rows(game, "SELECT goods_num,bind_num FROM back WHERE user_id='900001' AND goods_id=20023") == [(maximum, 1)]
        assert json.loads(accessory_projection(probe)[0][1]) == []
        assert json.loads(accessory_projection(probe)[0][0])["手镯"] == instances[0]
        assert len(probe.rows(game, receipt_sql)) == len(receipts) + 1
        assert_dungeon_purchase_integrity(game)
        assert_dungeon_purchase_integrity(probe.context.paths.player_db)
        probe.evidence.update(equipped_rejection=True, capacity_rejection=True, exact_capacity=True,
                              rejection_unchanged=True, recovered=True)

    for command, query, marker in (
        ("查看饰品", "acc_missing_parameter", "未找到"),
        ("装备饰品", "acc_missing_parameter", "未找到"),
        ("饰品锁定", "acc_missing_parameter 1", "未找到"),
        ("饰品解锁", "acc_missing_parameter 1", "未找到"),
        ("饰品洗练", "acc_missing_parameter", "未找到"),
        ("饰品分解", "acc_missing_parameter", "未在"),
    ):
        def build(command, query, marker):
            async def run(probe):
                before, stock = balances(probe), backpack(probe)
                text = await probe.command(command, command + " " + query)
                after_stock = backpack(probe)
                after = balances(probe)
                assert marker in text and after == before and after_stock == stock, text
                probe.evidence.update(
                    invalid_uid=query.split()[0],
                    assets_unchanged=True,
                )

            return run

        case(
            "accessory-invalid-" + command,
            command,
            "不存在饰品 UID",
            "真实饰品入口和隔离玩家",
            "具体 UID 拒绝且资产不变",
            group="remaining-player-parameters",
            outcome="rejection",
        )(build(command, query, marker))

    @case(
        "partner-protection-status",
        "双修保护",
        "状态",
        "注册玩家，关系服务已迁移",
        "读取并返回双修保护状态，不改变资产",
        group="remaining-player-parameters",
    )
    async def partner_protection_status(probe):
        before, stock = balances(probe), backpack(probe)
        text = await probe.command("双修保护", "双修保护 状态")
        assert "双修保护状态" in text and balances(probe) == before and backpack(probe) == stock, text
        probe.evidence.update(status_read=True, readonly=True)

    @case(
        "partner-protection-enable",
        "双修保护",
        "开启",
        "注册玩家，关系服务已迁移",
        "持久化开启双修保护并返回成功回执",
        group="remaining-player-parameters",
    )
    async def partner_protection_enable(probe):
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_buff import partner

        text = await probe.command("双修保护", "双修保护 开启", number=970001)
        assert "开启" in text and partner.load_player_user("900001") == "on", text
        probe.evidence.update(status="on", persisted=True)

    @case(
        "partner-protection-recovery",
        "双修保护",
        "拒绝/关闭 + 写入拒绝恢复",
        "真实注册入口、保护状态已有行、隔离 player SQLite",
        "状态或永久回执拒写保留具体原因，整笔回滚后同消息恢复",
        group="partner-protection-recovery",
    )
    async def partner_protection_recovery(probe):
        player = probe.context.paths.player_db
        probe.seed(
            player,
            "CREATE TRIGGER reject_partner_protection_insert "
            "BEFORE INSERT ON status "
            "BEGIN SELECT RAISE(IGNORE); END",
        )
        initial_failed = await probe.command("双修保护", "双修保护 开启", number=970010)
        assert "双修保护状态保存失败" in initial_failed, initial_failed
        assert probe.rows(player, "SELECT user_id FROM status WHERE user_id='900001'") == []
        assert not probe.rows(
            player,
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name='partner_protection_operations'",
        )
        probe.seed(player, "DROP TRIGGER reject_partner_protection_insert")
        initial_recovered = await probe.command("双修保护", "双修保护 开启", number=970010)
        assert "已开启" in initial_recovered
        assert probe.rows(player, "SELECT two_exp_protect FROM status WHERE user_id='900001'") == [("on",)]
        probe.seed(
            player,
            "CREATE TRIGGER reject_partner_protection_update "
            "BEFORE UPDATE OF two_exp_protect ON status "
            "BEGIN SELECT RAISE(IGNORE); END",
        )
        failed = await probe.command("双修保护", "双修保护 拒绝", number=970011)
        assert "双修保护状态保存失败" in failed, failed
        assert probe.rows(player, "SELECT two_exp_protect FROM status WHERE user_id='900001'") == [("on",)]
        assert len(probe.rows(player, "SELECT operation_id FROM partner_protection_operations")) == 1
        probe.seed(player, "DROP TRIGGER reject_partner_protection_update")
        recovered = await probe.command("双修保护", "双修保护 拒绝", number=970011)
        assert "已设置为拒绝" in recovered
        assert probe.rows(player, "SELECT two_exp_protect FROM status WHERE user_id='900001'") == [("refusal",)]

        probe.seed(
            player,
            "CREATE TRIGGER reject_partner_protection_receipt "
            "BEFORE INSERT ON partner_protection_operations "
            "BEGIN SELECT RAISE(IGNORE); END",
        )
        receipt_failed = await probe.command("双修保护", "双修保护 关闭", number=970012)
        assert "双修保护操作回执保存失败" in receipt_failed, receipt_failed
        assert probe.rows(player, "SELECT two_exp_protect FROM status WHERE user_id='900001'") == [("refusal",)]
        probe.seed(player, "DROP TRIGGER reject_partner_protection_receipt")
        receipt_recovered = await probe.command("双修保护", "双修保护 关闭", number=970012)
        assert "已关闭" in receipt_recovered
        assert probe.rows(player, "SELECT two_exp_protect FROM status WHERE user_id='900001'") == [("off",)]
        receipts = probe.rows(player, "SELECT operation_id FROM partner_protection_operations ORDER BY rowid")
        assert [row[0] for row in receipts] == ["relation:cultivation-protection:970010:900001", "relation:cultivation-protection:970011:900001", "relation:cultivation-protection:970012:900001"], receipts
        probe.evidence.update(status_rejection="partner_protection_state_write_rejected", receipt_rejection="partner_protection_receipt_write_rejected", recovered=True)

    @case(
        "partner-invite-reject-state-recovery",
        "双修 / 拒绝双修",
        "真实道号 + 拒绝指令，拒写后同指令恢复",
        "双方隔离角色，受邀方双修保护开启，玩家 SQLite 注入拒写 trigger",
        "实际 matcher 创建邀请并执行拒绝；失败原因可见，状态不变，解除故障后同消息成功",
        group="partner-invite-parameters",
        outcome="rejection",
    )
    async def partner_invite_reject_recovery(probe):
        from unittest.mock import AsyncMock, patch
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_buff import partner

        player = probe.context.paths.player_db
        probe.player("900002", exp=300)
        for uid in ("900001", "900002"):
            probe.impart(uid)
        probe.seed(
            player,
            "INSERT INTO status(user_id,two_exp_protect) VALUES('900002','on') "
            "ON CONFLICT(user_id) DO UPDATE SET two_exp_protect='on'",
        )
        with patch.object(partner, "expire_invite", new=AsyncMock()):
            created = await probe.command("双修", "双修 诊断对手 2", number=970020)
        assert "已向" in created, created
        invite = probe.rows(player, "SELECT invite_id,status FROM partner_cultivation_invites")
        assert len(invite) == 1 and invite[0][1] == "pending", invite
        probe.seed(
            player,
            "CREATE TRIGGER reject_partner_invite_reject "
            "BEFORE UPDATE OF status ON partner_cultivation_invites "
            "WHEN NEW.status='rejected' BEGIN SELECT RAISE(IGNORE); END",
        )
        failed = await probe.command("拒绝双修", actor="900002", number=970021, allow_exception=True)
        assert "双修邀请处理状态保存失败" in failed, failed
        assert "拒绝了" not in failed and probe.rows(
            player, "SELECT status FROM partner_cultivation_invites"
        ) == [("pending",)]
        probe.seed(player, "DROP TRIGGER reject_partner_invite_reject")
        recovered = await probe.command("拒绝双修", actor="900002", number=970021)
        assert "拒绝了" in recovered, recovered
        assert probe.rows(player, "SELECT status FROM partner_cultivation_invites") == [("rejected",)]
        probe.evidence.update(reason="partner_invite_resolve_write_rejected", state_rolled_back=True, recovered=True)

    @case(
        "partner-invite-accept-state-recovery",
        "双修 / 同意双修",
        "真实道号 + 同意指令，结算拒写后同指令恢复",
        "双方隔离角色，受邀方双修保护开启，玩家 SQLite 注入拒写 trigger",
        "邀请状态拒写时角色修为/次数/统计/永久回执整笔回滚，解除故障后同消息结算",
        group="partner-invite-parameters",
        outcome="rejection",
    )
    async def partner_invite_accept_recovery(probe):
        from unittest.mock import AsyncMock, patch
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_buff import partner

        game, player = probe.context.paths.game_db, probe.context.paths.player_db
        probe.player(exp=500)
        probe.player("900002", exp=300)
        for uid in ("900001", "900002"):
            probe.impart(uid)
        probe.seed(
            player,
            "INSERT INTO status(user_id,two_exp_protect) VALUES('900002','on') "
            "ON CONFLICT(user_id) DO UPDATE SET two_exp_protect='on'",
        )
        with patch.object(partner, "expire_invite", new=AsyncMock()):
            created = await probe.command("双修", "双修 诊断对手 2", number=970030)
        assert "已向" in created, created
        probe.clear_cooldowns()
        exp_before = probe.rows(game, "SELECT user_id,exp FROM user_xiuxian WHERE user_id IN ('900001','900002') ORDER BY user_id")
        usage_before = probe.rows(player, "SELECT user_id,used_count FROM partner_two_exp_usage WHERE user_id IN ('900001','900002') ORDER BY user_id")
        stats_before = probe.rows(player, 'SELECT user_id,"双修次数" FROM statistics WHERE user_id IN (\'900001\',\'900002\') ORDER BY user_id')
        probe.seed(
            player,
            "CREATE TRIGGER reject_partner_invite_accept "
            "BEFORE UPDATE OF status ON partner_cultivation_invites "
            "WHEN NEW.status='accepted' BEGIN SELECT RAISE(IGNORE); END",
        )
        failed = await probe.command("同意双修", actor="900002", number=970031, allow_exception=True)
        assert "双修邀请状态保存失败" in failed, failed
        assert any("partner_cultivation_invite_write_rejected" in row["exception"] for row in probe.calls[-1]["logs"])
        assert probe.rows(game, "SELECT user_id,exp FROM user_xiuxian WHERE user_id IN ('900001','900002') ORDER BY user_id") == exp_before
        assert probe.rows(player, "SELECT user_id,used_count FROM partner_two_exp_usage WHERE user_id IN ('900001','900002') ORDER BY user_id") == usage_before
        assert probe.rows(player, 'SELECT user_id,"双修次数" FROM statistics WHERE user_id IN (\'900001\',\'900002\') ORDER BY user_id') == stats_before
        assert probe.rows(player, "SELECT status FROM partner_cultivation_invites") == [("pending",)]
        assert probe.rows(game, "SELECT operation_id FROM partner_cultivation_operations") == []
        probe.seed(player, "DROP TRIGGER reject_partner_invite_accept")
        recovered = await probe.command("同意双修", actor="900002", number=970031)
        assert "获得修为" in recovered, recovered
        assert probe.rows(player, "SELECT status FROM partner_cultivation_invites") == [("accepted",)]
        assert probe.rows(game, "SELECT COUNT(*) FROM partner_cultivation_operations") == [(1,)]
        probe.evidence.update(reason="partner_cultivation_invite_write_rejected", state_rolled_back=True, recovered=True)

    @case(
        "dungeon-team-create-replay",
        "创建队伍",
        "自定义队伍名",
        "注册玩家、真实组队迁移和群聊上下文",
        "创建队伍写入真实队伍状态，重复事件只返回原结果",
        group="remaining-player-parameters",
    )
    async def dungeon_team_create(probe):
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_dungeon as module

        text = await probe.command("创建队伍", "创建队伍 参数队伍", number=970002)
        team_id = module.dungeon_team_application.team_id_for_user("900001")
        assert team_id and "队伍" in text, text
        snapshot = module.dungeon_team_application.snapshot(team_id)
        await probe.command("创建队伍", "创建队伍 参数队伍", number=970002)
        assert module.dungeon_team_application.snapshot(team_id) == snapshot
        probe.evidence.update(team_id=team_id, replay_unchanged=True)

    @case(
        "arena-shop-page-real",
        "竞技场商店",
        "页码1",
        "竞技场配置和注册玩家",
        "读取竞技场商店指定页，不改变资产",
        group="remaining-player-parameters",
    )
    async def arena_shop_page(probe):
        before, stock = balances(probe), backpack(probe)
        text = await probe.command("竞技场商店", "竞技场商店 1")
        assert "竞技场商店" in text and balances(probe) == before and backpack(probe) == stock, text
        probe.evidence.update(page=1, readonly=True)

    @case(
        "fusion-catalog-page-real",
        "查看可合成物品",
        "页码1",
        "随包合成配置",
        "读取真实可合成物品目录指定页，不改变资产",
        group="remaining-player-parameters",
    )
    async def fusion_catalog_page(probe):
        before, stock = balances(probe), backpack(probe)
        text = await probe.command("查看可合成物品", "查看可合成物品 1")
        assert text and balances(probe) == before and backpack(probe) == stock, text
        probe.evidence.update(page=1, readonly=True)

    @case(
        "dungeon-purchase-invalid-reject",
        "副本兑换",
        "不存在商品ID 数量1",
        "副本商店入口和注册玩家",
        "具体商品不存在拒绝且资产不变",
        group="remaining-player-parameters",
        outcome="rejection",
    )
    async def dungeon_purchase_invalid(probe):
        before, stock = balances(probe), backpack(probe)
        text = await probe.command("副本兑换", "副本兑换 999999 1")
        assert before == balances(probe) and stock == backpack(probe), text
        assert any(token in text for token in ("不存在", "商品", "无效", "兑换")), text
        probe.evidence.update(invalid_item_id=999999, assets_unchanged=True)

    @case(
        "dungeon-purchase-receipt-ignore-recovery",
        "副本兑换",
        "合法商品 1999，数量 2",
        "注册玩家、真实副本商店商品和隔离 game SQLite",
        "回执拒写返回具体失败；灵石/背包零写，解除故障后同消息恢复且重放不二次兑换",
        group="dungeon-purchase-parameters",
        outcome="rejection",
    )
    async def dungeon_purchase_receipt_ignore_recovery(probe):
        import json

        database = probe.context.paths.game_db
        operation_id = "dungeon-purchase:970100:900001"
        probe.player(stone=1_000_000)
        stone_sql = "SELECT stone FROM user_xiuxian WHERE user_id='900001'"
        inventory_sql = "SELECT goods_num,bind_num FROM back WHERE user_id='900001' AND goods_id=1999"
        before = (probe.rows(database, stone_sql), probe.rows(database, inventory_sql))
        probe.seed(
            database,
            "CREATE TRIGGER reject_dungeon_purchase_receipt BEFORE INSERT ON dungeon_purchase_operations "
            "WHEN NEW.operation_id='dungeon-purchase:970100:900001' "
            "BEGIN SELECT RAISE(IGNORE); END",
        )
        failed = await probe.command("副本兑换", "副本兑换 1999 2", number=970100)
        after_rejection = (probe.rows(database, stone_sql), probe.rows(database, inventory_sql))
        receipt_rows = probe.rows(database, "SELECT operation_id FROM dungeon_purchase_operations")
        probe.evidence.update(
            red_reply=failed,
            before=before,
            after_rejection=after_rejection,
            receipt_rows=receipt_rows,
        )
        assert "回执未能完整保存" in failed, failed
        assert after_rejection == before
        assert receipt_rows == []
        assert_dungeon_purchase_integrity(database)
        ledger = probe.rows(
            database,
            "SELECT status,result_json FROM operation_ledger WHERE operation_id=? AND action='dungeon.purchase'",
            (operation_id,),
        )
        assert len(ledger) == 1 and ledger[0][0] == "failed", ledger
        failure = json.loads(ledger[0][1])
        assert failure["code"] == "dungeon_purchase_receipt_write_rejected", failure
        assert failure["data"]["subject"] == "actor", failure
        probe.seed(database, "DROP TRIGGER reject_dungeon_purchase_receipt")

        recovered = await probe.command("副本兑换", "副本兑换 1999 2", number=970100)
        assert "成功兑换" in recovered, recovered
        after = (probe.rows(database, stone_sql), probe.rows(database, inventory_sql))
        assert after[0] == [(800_000,)] and after[1] == [(2, 2)], after
        receipt = probe.rows(
            database,
            "SELECT result_status,quantity,cost,stone,inventory,response "
            "FROM dungeon_purchase_operations WHERE operation_id=?",
            (operation_id,),
        )
        assert receipt == [("applied", 2, 200_000, 800_000, 2, "成功兑换渡厄丹×2，消耗200000灵石。")], receipt
        replay = await probe.command("副本兑换", "副本兑换 1999 2", number=970100)
        assert "成功兑换" in replay and (probe.rows(database, stone_sql), probe.rows(database, inventory_sql)) == after
        assert_dungeon_purchase_integrity(database)
        probe.evidence.update(
            reason="dungeon_purchase_receipt_write_rejected",
            subject="actor",
            rolled_back=True,
            recovered=True,
            replay_unchanged=True,
        )

    async def dungeon_purchase_write_rejection(
        probe,
        *,
        number,
        trigger_name,
        trigger_sql,
        reason,
        expected_subject,
        expected_reply,
        seed_inventory=False,
    ):
        import json

        database = probe.context.paths.game_db
        operation_id = f"dungeon-purchase:{number}:900001"
        probe.player(stone=1_000_000)
        if seed_inventory:
            probe.grant_item(1999, 1)
        stone_sql = "SELECT stone FROM user_xiuxian WHERE user_id='900001'"
        inventory_sql = "SELECT goods_num,bind_num FROM back WHERE user_id='900001' AND goods_id=1999"
        receipt_sql = "SELECT operation_id FROM dungeon_purchase_operations WHERE operation_id=?"
        before = (probe.rows(database, stone_sql), probe.rows(database, inventory_sql))
        probe.seed(database, trigger_sql)
        failed = await probe.command("副本兑换", "副本兑换 1999 2", number=number)
        after_rejection = (probe.rows(database, stone_sql), probe.rows(database, inventory_sql))
        receipts = probe.rows(database, receipt_sql, (operation_id,))
        assert expected_reply in failed, failed
        assert after_rejection == before, (before, after_rejection)
        assert receipts == [], receipts
        assert_dungeon_purchase_integrity(database)
        ledger = probe.rows(
            database,
            "SELECT status,result_json FROM operation_ledger WHERE operation_id=? AND action='dungeon.purchase'",
            (operation_id,),
        )
        assert len(ledger) == 1 and ledger[0][0] == "failed", ledger
        failure = json.loads(ledger[0][1])
        assert failure["code"] == reason, failure
        assert failure["data"]["reason"] == reason and failure["data"]["subject"] == expected_subject, failure
        probe.seed(database, f"DROP TRIGGER {trigger_name}")

        recovered = await probe.command("副本兑换", "副本兑换 1999 2", number=number)
        after = (probe.rows(database, stone_sql), probe.rows(database, inventory_sql))
        assert "成功兑换" in recovered, recovered
        assert after[0] == [(800_000,)], after
        initial_goods, initial_bound = before[1][0] if before[1] else (0, 0)
        assert after[1] == [(initial_goods + 2, initial_bound + 2)], after
        receipt = probe.rows(
            database,
            "SELECT result_status,quantity,cost,stone,inventory,response "
            "FROM dungeon_purchase_operations WHERE operation_id=?",
            (operation_id,),
        )
        expected_inventory = initial_goods + 2
        assert receipt == [
            ("applied", 2, 200_000, 800_000, expected_inventory,
             f"成功兑换渡厄丹×2，消耗200000灵石。")
        ], receipt
        replay = await probe.command("副本兑换", "副本兑换 1999 2", number=number)
        assert "成功兑换" in replay
        assert (probe.rows(database, stone_sql), probe.rows(database, inventory_sql)) == after
        assert_dungeon_purchase_integrity(database)
        probe.evidence.update(
            reason=reason,
            subject=failure["data"]["subject"],
            before=before,
            after_rejection=after_rejection,
            rolled_back=True,
            recovered=True,
            replay_unchanged=True,
        )

    @case(
        "dungeon-purchase-stone-ignore-recovery",
        "副本兑换",
        "合法商品 1999，数量 2",
        "注册玩家、真实副本商店商品和隔离 game SQLite",
        "灵石拒写返回具体失败；灵石/背包零写，解除故障后同消息恢复且重放不二次兑换",
        group="dungeon-purchase-parameters",
        outcome="rejection",
    )
    async def dungeon_purchase_stone_ignore_recovery(probe):
        await dungeon_purchase_write_rejection(
            probe,
            number=970101,
            trigger_name="reject_dungeon_purchase_stone",
            trigger_sql=(
                "CREATE TRIGGER reject_dungeon_purchase_stone BEFORE UPDATE OF stone ON user_xiuxian "
                "WHEN NEW.user_id='900001' BEGIN SELECT RAISE(IGNORE); END"
            ),
            reason="dungeon_purchase_stone_write_rejected",
            expected_subject="actor",
            expected_reply="扣除灵石未能保存",
        )

    @case(
        "dungeon-purchase-inventory-ignore-recovery",
        "副本兑换",
        "合法商品 1999，数量 2",
        "注册玩家、已有一件同商品和隔离 game SQLite",
        "背包拒写返回具体失败；灵石/原库存零写，解除故障后同消息恢复且重放不二次兑换",
        group="dungeon-purchase-parameters",
        outcome="rejection",
    )
    async def dungeon_purchase_inventory_ignore_recovery(probe):
        await dungeon_purchase_write_rejection(
            probe,
            number=970102,
            trigger_name="reject_dungeon_purchase_inventory",
            trigger_sql=(
                "CREATE TRIGGER reject_dungeon_purchase_inventory BEFORE INSERT ON back "
                "WHEN NEW.user_id='900001' AND NEW.goods_id=1999 BEGIN SELECT RAISE(IGNORE); END"
            ),
            reason="dungeon_purchase_inventory_write_rejected",
            expected_subject="inventory",
            expected_reply="物品入库未能保存",
            seed_inventory=True,
        )
