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
