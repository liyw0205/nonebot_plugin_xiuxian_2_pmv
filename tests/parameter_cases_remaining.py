"""Focused semantic coverage for ordinary parameterized commands not in core groups."""

from tests.parameter_cases_assets import balances, backpack


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
        before, stock = balances(probe), backpack(probe)
        text = await probe.command("饰品背包", "饰品背包 1")
        assert "饰品背包" in text and balances(probe) == before and backpack(probe) == stock, text
        probe.evidence.update(page=1, readonly=True)

    @case(
        "accessory-collection-real-set",
        "饰品图鉴",
        "真实套装名",
        "随包饰品套装配置",
        "按真实套装筛选图鉴，不改变资产",
        group="remaining-player-parameters",
    )
    async def accessory_collection(probe):
        before, stock = balances(probe), backpack(probe)
        text = await probe.command("饰品图鉴", "饰品图鉴 烈阳")
        assert "套装图鉴" in text and balances(probe) == before and backpack(probe) == stock, text
        probe.evidence.update(set_name="烈阳", readonly=True)

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
