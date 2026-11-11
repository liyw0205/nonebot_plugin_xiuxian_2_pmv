"""Virtual administrators traverse real asset commands on fictional players only."""
from tests.parameter_cases_assets import balances, backpack
from unittest.mock import patch


def register(case):
    for form, target in (("name", "诊断对手"), ("mention", "[CQ:at,qq=900002]"), ("self", "")):
        def build(target):
            async def run(probe):
                uid = "900002" if target else "900003"
                sql = "SELECT stone FROM user_xiuxian WHERE user_id=?"
                before = probe.rows(probe.context.paths.game_db, sql, (uid,))
                text = await probe.command("神秘力量", f"神秘力量 123 {target}", actor="900003", number=990001)
                assert probe.rows(probe.context.paths.game_db, sql, (uid,)) == [(before[0][0] + 123,)], text
                saved = balances(probe)
                await probe.command("神秘力量", f"神秘力量 123 {target}", actor="900003", number=990001)
                assert saved == balances(probe)
                probe.evidence.update(target=uid, delta=123, replay_unchanged=True)
            return run
        case("admin-stone-" + form, "神秘力量", "数量 实际" + form, "虚构超级用户权限、存在目标",
             "只给正确目标加123灵石、真实回执且重放不重复", group="admin-parameters")(build(target))

    @case("admin-impart-name-quantity", "传承力量", "数量 真实道号", "真实传承档案、虚构超级用户",
          "传承余额增加123、消息成功，重复事件不再增加", group="admin-parameters")
    async def impart(probe):
        probe.impart("900002", stone_num=50)
        sql = "SELECT stone_num FROM xiuxian_impart WHERE user_id='900002'"
        text = await probe.command("传承力量", "传承力量 123 诊断对手", actor="900003", number=990002)
        assert probe.rows(probe.context.paths.impart_db, sql) == [(173,)], text
        await probe.command("传承力量", "传承力量 123 诊断对手", actor="900003", number=990002)
        assert probe.rows(probe.context.paths.impart_db, sql) == [(173,)]
        probe.evidence.update(before=50, after=173, replay_unchanged=True)

    for form, target in (("name", "诊断对手"), ("mention", "[CQ:at,qq=900002]")):
        def build(target):
            async def run(probe):
                before = probe.rows(probe.context.paths.game_db, "SELECT exp FROM user_xiuxian WHERE user_id='900002'")[0][0]
                text = await probe.command("修为调整", f"修为调整 {target} 123", actor="900003", number=990003)
                assert probe.rows(probe.context.paths.game_db, "SELECT exp FROM user_xiuxian WHERE user_id='900002'") == [(before + 123,)], text
                saved = balances(probe)
                await probe.command("修为调整", f"修为调整 {target} 123", actor="900003", number=990003)
                assert saved == balances(probe)
                probe.evidence.update(delta=123, replay_unchanged=True)
            return run
        case("admin-exp-" + form, "修为调整", "目标" + form + " 修为123", "虚构超级用户、合法目标",
             "目标修为加123且重放不重复，战力按领域规则更新", group="admin-parameters")(build(target))

    @case("admin-create-item-target-count", "创造力量", "真实物品名 数量2 真实道号", "虚构超级用户与目标，背包空余",
          "只给目标2件真实物品、回执持久化且重放不重复", group="admin-parameters")
    async def create(probe):
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_admin as m
        name = m.items.get_data_by_item_id(7001)["name"]
        text = await probe.command("创造力量", f"创造力量 {name} 2 诊断对手", actor="900003", number=990004)
        assert probe.managers[0].goods_num("900002", 7001) == 2, text
        saved = backpack(probe)
        await probe.command("创造力量", f"创造力量 {name} 2 诊断对手", actor="900003", number=990004)
        assert saved == backpack(probe)
        probe.evidence.update(target="900002", item_id=7001, quantity=2, replay_unchanged=True)

    for command, query, field, expected in (
        ("轮回力量", "8 诊断对手", "root_type", "永恒道果"),
        ("造化力量", "练气境初期 诊断对手", "level", "练气境初期"),
        ("易名", "诊断对手 参数新名", "user_name", "参数新名"),
    ):
        def build(command, query, field, expected):
            async def run(probe):
                text = await probe.command(command, command + " " + query, actor="900003")
                assert probe.rows(probe.context.paths.game_db, f"SELECT {field} FROM user_xiuxian WHERE user_id='900002'") == [(expected,)], text
                probe.evidence.update(target="900002", field=field, value=expected)
            return run
        case("admin-change-" + field, command, "真实配置值 道号", "虚构超级用户、合法已注册目标",
             "实际目标字段持久化为指定值", group="admin-parameters")(build(command, query, field, expected))

    for label, args in (("zero", "0 诊断对手"), ("bad", "abc 诊断对手"),
                        ("extra", "123 诊断对手 extra"), ("unknown", "123 不存在的目标")):
        def build(args):
            async def run(probe):
                before = balances(probe)
                text = await probe.command("神秘力量", "神秘力量 " + args, actor="900003")
                assert before == balances(probe), text
                probe.evidence.update(assets_unchanged=True)
            return run
        case("admin-stone-reject-" + label, "神秘力量", label, "虚构管理权限与真实目标",
             "具体参数/目标拒绝，不调整资产", group="admin-parameters", outcome="rejection")(build(args))

    for kind in ("stone", "item"):
        def build(kind):
            async def run(probe):
                from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_admin as m
                from nonebot_plugin_xiuxian_2.features.admin_asset.application import AdminAssetApplication
                from nonebot_plugin_xiuxian_2.core.errors import OperationConflictError
                app = AdminAssetApplication(probe.context.paths.game_db)
                uid = "900002"
                if kind == "stone":
                    current = probe.rows(probe.context.paths.game_db, "SELECT stone FROM user_xiuxian WHERE user_id=?", (uid,))[0][0]
                    old = dict(operator_id="900003", user_id=uid, expected_stone=current, requested_delta=123, target_name="诊断对手")
                    action, function = "admin.stone_adjust", "adjust_stone"
                    command, text = "神秘力量", "神秘力量 123 诊断对手"
                else:
                    definition = m.items.get_data_by_item_id(7001)
                    old = dict(operator_id="900003", user_id=uid, item_id=7001, item_name=definition["name"], item_type=definition["type"],
                               quantity=2, expected_quantity=0, max_goods_num=999999, target_name="诊断对手")
                    action, function = "admin.item_grant", "grant_item"
                    command, text = "创造力量", f"创造力量 {definition['name']} 2 诊断对手"
                # Leave an old-format started ledger after the actual business
                # commit to reproduce a pre-result process interruption.
                await probe.command(command, text, actor="900003", number=990008)
                rows = probe.rows(probe.context.paths.game_db, "SELECT operation_id FROM operation_ledger WHERE action=?", (action,))
                assert len(rows) == 1, rows
                operation_id = rows[0][0]
                from nonebot_plugin_xiuxian_2.infrastructure.database.ledger import request_hash
                probe.seed(probe.context.paths.game_db, "UPDATE operation_ledger SET request_hash=?,status='started',result_json=NULL WHERE operation_id=? AND action=?",
                           (request_hash(old), operation_id, action))
                before, bag = balances(probe), backpack(probe)
                with patch.object(m, "admin_asset_application", app):
                    await probe.command(command, text, actor="900003", number=990008)
                assert balances(probe) == before and backpack(probe) == bag
                changed = dict(old, operation_id=operation_id)
                changed["requested_delta" if kind == "stone" else "quantity"] = 124 if kind == "stone" else 3
                try:
                    getattr(app, function)(**changed)
                except OperationConflictError:
                    pass
                else:
                    raise AssertionError("altered request accepted against committed business receipt")
                assert balances(probe) == before and backpack(probe) == bag
                assert probe.rows(probe.context.paths.game_db, "SELECT status FROM operation_ledger WHERE operation_id=? AND action=?", (operation_id, action)) == [("applied",)]
                probe.evidence.update(legacy_hash=True, interrupted_result_recovered=True, altered_request_rejected=True, assets_unchanged=True)
            return run
        case("admin-legacy-recovery-" + kind, "神秘力量" if kind == "stone" else "创造力量", "真实合法目标及数量，同一消息重放",
             "真实业务回执已提交、旧快照hash的started回执、重建应用", "恢复原结果不重复发放，篡改数量拒绝", group="admin-recovery")(build(kind))
