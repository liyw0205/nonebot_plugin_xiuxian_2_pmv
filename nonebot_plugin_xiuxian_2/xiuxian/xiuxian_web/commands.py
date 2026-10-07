from .core import (
    ADMIN_COMMANDS,
    DATABASE,
    IMPART_DB,
    PLAYER_DB,
    ROOTS,
    WEB_CONFIG,
    app,
    convert_rank,
    execute_sql,
    get_user_by_name,
    items,
    jsondata,
    jsonify,
    redirect,
    render_template,
    request,
    session,
    url_for,
    uuid,
)

from ...features.admin_asset.application import AdminAssetApplication
from .config import get_root_rate


admin_asset_application = AdminAssetApplication(DATABASE)


def _failure(message):
    return jsonify({"success": False, "error": str(message)})


def _success(message):
    return jsonify({"success": True, "message": str(message)})


def _operation_id(action: str, target: str = "all") -> str:
    payload = request.get_json(silent=True) or {}
    request_id = payload.get("request_id")
    token = str(request_id) if request_id else uuid.uuid4().hex
    return f"web-admin:{action}:{session.get('admin_id', 'unknown')}:{target}:{token}"


def _owner_status(result) -> str:
    data = getattr(result, "data", None)
    if isinstance(data, dict) and data.get("status"):
        return str(data["status"])
    return str(getattr(result, "status", "unknown"))


def _run_batch(step):
    while True:
        result = step()
        if (
            getattr(result, "status", None) != "applied"
            or int(getattr(result, "completed", 0) or 0)
            >= int(getattr(result, "total", 0) or 0)
        ):
            return result


def _user_or_error(username: str):
    user = get_user_by_name(username)
    if not user:
        return None, _failure(f"用户 {username} 不存在")
    return user, None


def _item_quantity(user_id: str, item_id: int) -> int:
    rows = execute_sql(
        DATABASE,
        "SELECT goods_num FROM back WHERE user_id = %s AND goods_id = %s",
        (str(user_id), int(item_id)),
    )
    return int(rows[0].get("goods_num") or 0) if rows else 0


def _user_ids():
    rows = execute_sql(DATABASE, "SELECT user_id FROM user_xiuxian", ())
    return [str(row["user_id"]) for row in rows if row.get("user_id") is not None]


def _accessory_tools():
    from ..xiuxian_back.accessory_helpers import ACCESSORY_BAG_LIMIT, create_accessory_instance

    return ACCESSORY_BAG_LIMIT, create_accessory_instance


@app.route('/commands')
def commands():
    if 'admin_id' not in session:
        return redirect(url_for('login'))
    return render_template('commands.html', commands=ADMIN_COMMANDS)


@app.route('/execute_command', methods=['POST'])
def execute_command():
    if 'admin_id' not in session:
        return _failure("未登录")

    data = request.get_json() or {}
    command_name = data.get('command_name')
    request_id = data.get("request_id")
    if request_id is not None and (
        not isinstance(request_id, str)
        or not request_id
        or len(request_id) > 96
        or not request_id.isascii()
        or not all(char.isalnum() or char in "-_" for char in request_id)
    ):
        return _failure("请求编号格式错误")

    if not command_name:
        return _failure("未指定命令")

    try:
        operator_id = str(session.get("admin_id") or "unknown")
        if command_name == "gm_command":
            target = data.get('target')
            username = data.get('username')
            amount = int(data.get('amount', 0))
            if target == "指定用户" and username:
                user_info, error = _user_or_error(username)
                if error:
                    return error
                if amount == 0:
                    return _success(f"成功向 {username} 增加 0 灵石")
                outcome = admin_asset_application.adjust_stone(
                    operation_id=_operation_id("stone", str(user_info["user_id"])),
                    operator_id=operator_id,
                    user_id=str(user_info["user_id"]),
                    expected_stone=int(user_info.get("stone") or 0),
                    requested_delta=amount,
                    target_name=username,
                )
                if _owner_status(outcome) not in {"adjusted", "duplicate"}:
                    return _failure(f"灵石调整未完成：{_owner_status(outcome)}")
                applied_delta = int(outcome.data.get("applied_delta", amount))
                if applied_delta == 0:
                    return _success(
                        f"成功将 {username} 的灵石调整为 {int(outcome.data.get('final_stone', 0))}"
                    )
                return _success(
                    f"成功向 {username} {'增加' if applied_delta > 0 else '减少'} {abs(applied_delta)} 灵石"
                )

            if amount == 0:
                return _success("全服发放 0 灵石成功")
            operation_id = admin_asset_application.find_running_stone_batch(
                operator_id=operator_id, requested_delta=amount
            ) or _operation_id("stone-all")
            result = _run_batch(
                lambda: admin_asset_application.adjust_stone_batch(
                    operation_id=operation_id,
                    operator_id=operator_id,
                    requested_delta=amount,
                )
            )
            if result.status not in {"applied", "duplicate"}:
                return _failure(f"全服灵石调整未完成：{result.status}")
            return _success(
                f"全服{'发放' if amount >= 0 else '扣除'} {abs(amount)} 灵石成功，"
                f"影响 {result.affected_users} 名用户"
            )

        if command_name == "adjust_exp_command":
            target = data.get('target')
            username = data.get('username')
            amount = int(data.get('amount', 0))
            if target == "指定用户" and username:
                user_info, error = _user_or_error(username)
                if error:
                    return error
                if amount == 0:
                    return _success(f"成功从 {username} 减少 0 修为")
                outcome = admin_asset_application.adjust_exp(
                    operation_id=_operation_id("exp", str(user_info["user_id"])),
                    operator_id=operator_id,
                    user_id=str(user_info["user_id"]),
                    expected_exp=int(user_info.get("exp") or 0),
                    requested_delta=amount,
                    target_name=username,
                )
                if _owner_status(outcome) not in {"adjusted", "duplicate"}:
                    return _failure(f"修为调整未完成：{_owner_status(outcome)}")
                applied_delta = int(outcome.data.get("applied_delta", amount))
                if applied_delta == 0:
                    return _success(
                        f"成功将 {username} 的修为调整为 {int(outcome.data.get('final_exp', 0))}"
                    )
                return _success(
                    f"成功向 {username} {'增加' if applied_delta > 0 else '减少'} {abs(applied_delta)} 修为"
                )

            if amount == 0:
                return _success("全服增加 0 修为成功")
            outcome = admin_asset_application.adjust_exp_all(
                operation_id=_operation_id("exp-all"),
                operator_id=operator_id,
                requested_delta=amount,
            )
            if outcome.status not in {"adjusted", "duplicate", "no_targets"}:
                return _failure(f"全服修为调整未完成：{outcome.status}")
            return _success(
                f"全服{'增加' if amount >= 0 else '减少'} {abs(amount)} 修为成功"
            )

        if command_name == "gmm_command":
            username = data.get('username')
            root_type = str(data.get('root_type') or "")
            if not username:
                return _failure("请指定用户名")
            user_info, error = _user_or_error(username)
            if error:
                return error

            root_names = {
                "1": "全属性灵根", "2": "融合万物灵根", "3": "月灵根", "4": "言灵灵根",
                "5": "金灵根", "6": "轮回千次不灭，只为臻至巅峰", "7": "轮回万次不灭，只为超越巅峰",
                "8": "轮回无尽不灭，只为触及永恒之境", "9": f"轮回命主·{username}",
            }
            root_name = root_names.get(root_type, "未知灵根")
            root_type_name = ROOTS.get(root_type, "混沌灵根")
            levels = jsondata.level_data()
            spend = float(levels[user_info["level"]]["spend"])
            expected = (
                user_info.get("root"), user_info.get("root_type"), user_info.get("root_level"),
                user_info.get("level"), user_info.get("exp"), user_info.get("power"),
                user_info.get("user_name"),
            )
            outcome = admin_asset_application.change_root(
                operation_id=_operation_id("root", str(user_info["user_id"])),
                operator_id=operator_id,
                user_id=str(user_info["user_id"]),
                expected_snapshot=expected,
                root_id=int(root_type),
                level_spend=spend,
                new_root_rate=float(get_root_rate(root_type_name, user_info["user_id"])),
                target_name=username,
            )
            if _owner_status(outcome) not in {"applied", "duplicate"}:
                return _failure(f"灵根修改未完成：{_owner_status(outcome)}")
            return _success(f"成功将 {username} 的灵根修改为 {root_name}")

        if command_name == "zaohua_xiuxian":
            username = data.get('username')
            level = data.get('level')
            if not username:
                return _failure("请指定用户名")
            user_info, error = _user_or_error(username)
            if error:
                return error
            levels = convert_rank('江湖好手')[1]
            if level not in levels:
                return _failure(f"无效的境界: {level}")
            level_data = jsondata.level_data()
            if not level_data or level not in level_data:
                return _failure(f"无法获取境界 {level} 的数据")
            config = level_data[level]
            expected = (
                user_info.get("level"), user_info.get("exp"), user_info.get("hp"),
                user_info.get("mp"), user_info.get("atk"), user_info.get("power"),
                user_info.get("root_type"), user_info.get("root_level"),
            )
            outcome = admin_asset_application.change_level(
                operation_id=_operation_id("level", str(user_info["user_id"])),
                operator_id=operator_id,
                user_id=str(user_info["user_id"]),
                expected_snapshot=expected,
                new_level=level,
                new_exp=int(config["power"]),
                level_spend=float(config["spend"]),
                root_rate=float(get_root_rate(user_info["root_type"], user_info["user_id"])),
                target_name=username,
            )
            if _owner_status(outcome) not in {"applied", "duplicate"}:
                return _failure(f"境界修改未完成：{_owner_status(outcome)}")
            return _success(f"成功将 {username} 的境界修改为 {level}")

        if command_name in {"cz", "hmll"}:
            grant = command_name == "cz"
            target = data.get('target')
            username = data.get('username')
            item_input = data.get('item')
            amount = int(data.get('amount', 1))
            if not item_input:
                return _failure("请指定物品")
            if amount <= 0:
                return _failure("数量必须大于0")
            quality = max(1, min(5, int(data.get('quality', 1)))) if grant else 1
            goods_id, item_data = items.get_data_by_item_name(str(item_input))
            if not goods_id or not item_data:
                return _failure(f"物品 {item_input} 不存在")
            goods_id = int(goods_id)
            goods_name = item_data['name']
            goods_type = item_data.get('type', '未知类型')
            is_accessory = item_data.get("item_type") == "饰品"
            accessory_limit, accessory_factory = _accessory_tools() if is_accessory else (0, None)

            if target == "指定用户" and username:
                user_info, error = _user_or_error(username)
                if error:
                    return error
                user_id = str(user_info['user_id'])
                operation_kind = (
                    ("accessory-grant" if grant else "accessory-destroy")
                    if is_accessory
                    else ("item-grant" if grant else "item-destroy")
                )
                operation_id = _operation_id(operation_kind, user_id)
                if is_accessory:
                    outcome = admin_asset_application.adjust_accessory(
                        operation_id=operation_id,
                        operator_id=operator_id,
                        user_id=user_id,
                        action="grant" if grant else "destroy",
                        item_id=goods_id,
                        item_name=goods_name,
                        quantity=amount,
                        target_name=username,
                        player_database=PLAYER_DB,
                        quality=quality if grant else None,
                        max_accessories=accessory_limit if grant else None,
                        create_accessory=(
                            lambda: accessory_factory(goods_id, quality)
                        ) if grant else None,
                    )
                    if not outcome.ok:
                        action_error = "饰品发放失败" if grant else "饰品扣除失败"
                        return _failure(f"{action_error}: {outcome.status}")
                    affected = int(outcome.data["affected_quantity"])
                    if grant:
                        return _success(
                            f"成功向 {username} 发放【{goods_name}】饰品 x{affected}（{quality}阶）"
                        )
                    message = f"成功从 {username} 扣除【{goods_name}】饰品 x{affected}"
                    if affected < amount:
                        message += "（数量不足，已按实际可扣执行）"
                    return _success(message)
                expected_quantity = _item_quantity(user_id, goods_id)
                if not grant:
                    outcome = admin_asset_application.destroy_item(
                        operation_id=operation_id,
                        operator_id=operator_id,
                        user_id=user_id,
                        item_id=goods_id,
                        item_name=goods_name,
                        item_type=goods_type,
                        quantity=amount,
                        expected_quantity=expected_quantity,
                        target_name=username,
                    )
                    if not outcome.ok:
                        return _failure(f"物品扣除失败: {_owner_status(outcome)}")
                    removed = int(outcome.data["removed_quantity"])
                    message = f"成功从 {username} 扣除 {goods_name} x{removed}"
                    if removed < amount:
                        message += "（数量不足，已按实际可扣执行）"
                    return _success(message)
                outcome = admin_asset_application.grant_item(
                    operation_id=operation_id,
                    operator_id=operator_id,
                    user_id=user_id,
                    item_id=goods_id,
                    item_name=goods_name,
                    item_type=goods_type,
                    quantity=amount,
                    expected_quantity=expected_quantity,
                    max_goods_num=int(WEB_CONFIG.max_goods_num),
                    target_name=username,
                )
                if not outcome.ok:
                    return _failure(f"物品发放失败: {_owner_status(outcome)}")
                return _success(f"成功向 {username} 发放 {goods_name} x{amount}")

            if is_accessory:
                user_ids = _user_ids()
                operation_id = admin_asset_application.find_running_accessory_batch(
                    player_database=PLAYER_DB,
                    action="grant" if grant else "destroy",
                    operator_id=operator_id,
                    item_id=goods_id,
                    item_name=goods_name,
                    quality=quality if grant else 0,
                    quantity=amount,
                    max_accessories=accessory_limit if grant else 0,
                )
                if not user_ids and operation_id is None:
                    if grant:
                        return _success(
                            f"全服发放【{goods_name}】饰品 x{amount}（{quality}阶）成功，影响 0 名用户"
                        )
                    return _success(
                        f"全服扣除【{goods_name}】完成，影响 0 名用户，累计扣除 0 件（仅背包，已装备未扣除）"
                    )
                operation_id = operation_id or _operation_id(
                    "accessory-grant-all" if grant else "accessory-destroy-all",
                    str(goods_id),
                )
                if grant:
                    result = _run_batch(
                        lambda: admin_asset_application.grant_accessory_batch(
                            operation_id, operator_id, user_ids, goods_id, goods_name,
                            quality, amount, accessory_limit,
                            lambda _user_id: accessory_factory(goods_id, quality),
                            player_database=PLAYER_DB,
                        )
                    )
                else:
                    result = _run_batch(
                        lambda: admin_asset_application.destroy_accessory_batch(
                            operation_id, operator_id, user_ids, goods_id, goods_name,
                            amount, player_database=PLAYER_DB,
                        )
                    )
                if result.status == "no_targets":
                    if grant:
                        return _success(
                            f"全服发放【{goods_name}】饰品 x{amount}（{quality}阶）成功，影响 0 名用户"
                        )
                    return _success(
                        f"全服扣除【{goods_name}】完成，影响 0 名用户，累计扣除 0 件（仅背包，已装备未扣除）"
                    )
                if result.status not in {"applied", "duplicate"}:
                    return _failure(f"全服饰品操作未完成：{result.status}")
                if grant:
                    return _success(
                        f"全服发放【{goods_name}】饰品 x{amount}（{quality}阶）成功，影响 {result.affected_users} 名用户"
                    )
                return _success(
                    f"全服扣除【{goods_name}】完成，影响 {result.affected_users} 名用户，"
                    f"累计扣除 {result.affected_quantity} 件（仅背包，已装备未扣除）"
                )

            operation_id = admin_asset_application.find_running_item_batch(
                action="grant" if grant else "destroy",
                operator_id=operator_id,
                item_id=goods_id,
                item_name=goods_name,
                item_type=goods_type,
                quantity=amount,
                max_goods_num=int(WEB_CONFIG.max_goods_num) if grant else 0,
            ) or _operation_id("item-grant-all" if grant else "item-destroy-all", str(goods_id))
            result = _run_batch(
                lambda: admin_asset_application.adjust_item_batch(
                    action="grant" if grant else "destroy",
                    operation_id=operation_id,
                    operator_id=operator_id,
                    item_id=goods_id,
                    item_name=goods_name,
                    item_type=goods_type,
                    quantity=amount,
                    max_goods_num=int(WEB_CONFIG.max_goods_num) if grant else 0,
                )
            )
            if result.status == "no_targets":
                if grant:
                    return _success(f"全服发放 {goods_name} x{amount} 成功，影响 0 名用户")
                return _success(
                    f"全服扣除 {goods_name} 完成，影响 0 名用户，累计扣除 0 个"
                )
            if result.status not in {"applied", "duplicate"}:
                return _failure(f"全服物品操作未完成：{result.status}")
            if grant:
                return _success(
                    f"全服发放 {goods_name} x{amount} 成功，影响 {result.affected_users} 名用户"
                )
            return _success(
                f"全服扣除 {goods_name} 完成，影响 {result.affected_users} 名用户，累计扣除 {result.removed} 个"
            )

        if command_name == "ccll_command":
            target = data.get('target')
            username = data.get('username')
            amount = int(data.get('amount', 0))
            if target == "指定用户" and username:
                user_info, error = _user_or_error(username)
                if error:
                    return error
                if amount == 0:
                    return _success(f"成功向 {username} 增加 0 思恋结晶")
                snapshot = admin_asset_application.snapshot_impart_stone(
                    str(user_info["user_id"]), impart_database=IMPART_DB
                )
                if snapshot.status != "ok":
                    return _failure(f"传承石调整未完成：{snapshot.status}")
                outcome = admin_asset_application.adjust_impart_stone(
                    operation_id=_operation_id("impart-stone", str(user_info["user_id"])),
                    operator_id=operator_id,
                    user_id=str(user_info["user_id"]),
                    expected_stone=snapshot.stone,
                    requested_delta=amount,
                    target_name=username,
                    impart_database=IMPART_DB,
                )
                if not outcome.ok:
                    return _failure(f"传承石调整未完成：{outcome.status}")
                applied_delta = int(outcome.data.get("applied_delta", amount))
                if applied_delta == 0:
                    return _success(
                        f"成功将 {username} 的思恋结晶调整为 {int(outcome.data.get('final_stone', 0))}"
                    )
                return _success(
                    f"成功向 {username} {'增加' if applied_delta > 0 else '减少'} {abs(applied_delta)} 思恋结晶"
                )

            if amount == 0:
                return _success(
                    "全服发放 0 思恋结晶成功，影响 0 名用户"
                )
            operation_id = admin_asset_application.find_running_impart_stone_batch(
                operator_id=operator_id,
                requested_delta=amount,
                impart_database=IMPART_DB,
            ) or _operation_id("impart-stone-all")
            result = _run_batch(
                lambda: admin_asset_application.adjust_impart_stone_batch(
                    operation_id=operation_id,
                    operator_id=operator_id,
                    requested_delta=amount,
                    impart_database=IMPART_DB,
                )
            )
            if result.status == "no_targets":
                return _success(
                    f"全服{'发放' if amount >= 0 else '扣除'} {abs(amount)} 思恋结晶成功，影响 0 名用户"
                )
            if result.status not in {"applied", "duplicate"}:
                return _failure(f"全服传承石调整未完成：{result.status}")
            return _success(
                f"全服{'发放' if amount >= 0 else '扣除'} {abs(amount)} 思恋结晶成功，"
                f"影响 {result.affected_users} 名用户"
            )

        return _failure(f"未知命令: {command_name}")
    except ValueError as exc:
        return _failure(f"参数格式错误: {exc}")
    except Exception as exc:
        return _failure(f"执行错误: {exc}")
