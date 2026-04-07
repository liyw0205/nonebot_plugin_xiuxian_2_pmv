try:
    import ujson as json
except ImportError:
    import json

import random
import traceback
from pathlib import Path
from nonebot import on_command
from nonebot.params import CommandArg

from ..adapter_compat import Bot, Message, GroupMessageEvent, PrivateMessageEvent
from ..xiuxian_utils.lay_out import assign_bot, Cooldown
from ..xiuxian_utils.utils import check_user, handle_send, number_to
from ..xiuxian_utils.xiuxian2_handle import XiuxianDateManage, PlayerDataManager

sql_message = XiuxianDateManage()
player_data_manager = PlayerDataManager()

MAP_FILE = Path() / "data" / "xiuxian" / "地图.json"
MAP_TABLE = "map_status"
DONGFU_TABLE = "dongfu_status"

# ===== 洞府建设配置 =====
DONGFU_COST = 100000000
FORBIDDEN_DONGFU_TYPES = {"坊市", "渡口", "驿站", "交通", "关隘", "情报", "宫殿", "试炼"}

# ===== 节点功能 =====
SEED_SHOP_TYPES = {"坊市", "城池", "驿站"}

# ===== 命令 =====
map_help = on_command("地图帮助", priority=8, block=True)
map_info = on_command("地图", priority=8, block=True)
my_pos = on_command("我的位置", priority=8, block=True)
map_go = on_command("前往", priority=8, block=True)

build_dongfu = on_command("建设洞府", priority=8, block=True)
go_home = on_command("回府", priority=8, block=True)

nearby_users_cmd = on_command("附近道友", priority=8, block=True)
dao_qc = on_command("论道切磋", priority=8, block=True)
dao_view = on_command("论道查看", priority=8, block=True)

seed_shop = on_command("种子商店", priority=8, block=True)
buy_seed = on_command("购买种子", priority=8, block=True)

SEED_CONFIG = {
    21001: {"name": "青灵草种", "price": 500000, "pool": "herb_mid", "minutes": 180},
    21003: {"name": "星砂神种", "price": 15000000, "pool": "god_low", "minutes": 360},
    21004: {"name": "混元神种", "price": 80000000, "pool": "god_high", "minutes": 720},
}


def _load_map_data():
    if not MAP_FILE.exists():
        raise FileNotFoundError(f"未找到地图文件：{MAP_FILE}")
    try:
        with open(MAP_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        raise RuntimeError(
            f"地图JSON解析失败：{MAP_FILE}\n{e}\n{traceback.format_exc()}"
        )


def _all_realms(map_data):
    return [k for k in map_data.keys() if k != "meta"]


def _heaven_names(map_data, realm: str):
    return list(map_data[realm]["heavens"].keys())


def _nodes(map_data, realm: str, heaven_name: str):
    return map_data[realm]["heavens"].get(heaven_name, [])


def _find_node_by_id(map_data, realm: str, heaven_name: str, node_id: str):
    for n in _nodes(map_data, realm, heaven_name):
        if n["id"] == node_id:
            return n
    return None


def _find_node_by_name(map_data, node_name: str):
    for realm in _all_realms(map_data):
        for heaven_name, node_list in map_data[realm]["heavens"].items():
            for n in node_list:
                if n["name"] == node_name:
                    return realm, heaven_name, n
    return None


def _get_realm_heaven_order(map_data, realm: str):
    order_cfg = map_data.get("meta", {}).get("heaven_order", {})
    order = order_cfg.get(realm)
    if order and isinstance(order, list):
        return order
    return _heaven_names(map_data, realm)


def _get_player_map_status(user_id: str, map_data: dict):
    data = player_data_manager.get_fields(str(user_id), MAP_TABLE)
    if data and data.get("realm") and data.get("heaven") and data.get("node_id"):
        return data
    return _init_player_map_status(user_id, map_data)


def _init_player_map_status(user_id: str, map_data: dict):
    realm = random.choice(_all_realms(map_data))
    heavens = _heaven_names(map_data, realm)
    heaven = random.choice(heavens)
    node = random.choice(_nodes(map_data, realm, heaven))

    init_data = {
        "realm": realm,
        "heaven": heaven,
        "node_id": node["id"],
        "visited_nodes": [node["id"]],
    }
    for k, v in init_data.items():
        player_data_manager.update_or_write_data(str(user_id), MAP_TABLE, k, v)
    return init_data


def _parse_map_query(map_data, text: str):
    """
    解析地图查询参数：
    - 返回 ("realm", realm_name) 或 ("heaven", (realm_name, heaven_name)) 或 (None, None)
    """
    q = (text or "").strip()
    if not q:
        return None, None

    # 先判定是否是界名
    for realm in _all_realms(map_data):
        if q == realm:
            return "realm", realm

    # 再判定是否是天名
    for realm in _all_realms(map_data):
        heavens = _heaven_names(map_data, realm)
        if q in heavens:
            return "heaven", (realm, q)

    return None, None


def _calc_move_cost(meta, map_data, cur_realm, cur_heaven, cur_node_id, tar_realm, tar_heaven, tar_node_id):
    cost_cfg = meta.get("move_cost", {})
    cross_realm = int(cost_cfg.get("cross_realm", 300))
    cross_heaven = int(cost_cfg.get("cross_heaven", 50))
    cross_node = int(cost_cfg.get("cross_node", 5))

    if cur_realm == tar_realm and cur_heaven == tar_heaven and cur_node_id == tar_node_id:
        return 0, "你已经在该节点。"

    # 跨界：仅交通节点可发起
    if cur_realm != tar_realm:
        cur_node = _find_node_by_id(map_data, cur_realm, cur_heaven, cur_node_id)
        if not cur_node or cur_node.get("type") != "交通":
            return -1, "只有在交通类节点才可前往其他界。"
        return cross_realm, ""

    # 同界跨天：允许去任意天，但若不允许跳层，则一次只能相邻天
    if cur_heaven != tar_heaven:
        allow_jump = bool(meta.get("rules", {}).get("allow_cross_level_jump", False))
        if not allow_jump:
            order = _get_realm_heaven_order(map_data, cur_realm)
            if cur_heaven in order and tar_heaven in order:
                if abs(order.index(cur_heaven) - order.index(tar_heaven)) > 1:
                    return -1, "不可跨越多个天层，请逐层前往。"
        return cross_heaven, ""

    # 同天跨节点：随意前往
    if cur_node_id != tar_node_id:
        return cross_node, ""

    return -1, "无效移动。"


def _get_all_in_same_node(realm, heaven, node_id):
    all_user_ids = sql_message.get_all_user_id() or []
    res = []
    for uid in all_user_ids:
        st = player_data_manager.get_fields(str(uid), MAP_TABLE)
        if not st:
            continue
        if st.get("realm") == realm and st.get("heaven") == heaven and st.get("node_id") == node_id:
            ui = sql_message.get_user_info_with_id(uid)
            if ui:
                res.append(ui)
    return res


def _is_seed_shop_node(node_type: str):
    return node_type in SEED_SHOP_TYPES


@map_help.handle(parameterless=[Cooldown(cd_time=1.4)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    bot, _ = await assign_bot(bot=bot, event=event)
    msg = (
        "【地图帮助】\n"
        "1. 地图 / 我的位置\n"
        "2. 前往 节点名（例：前往 九幽殿）\n"
        "3. 建设洞府 / 回府\n"
        "4. 附近道友 / 论道切磋 / 论道查看\n"
        "5. 种子商店 / 购买种子 种子名 数量"
    )
    await handle_send(bot, event, msg)


@map_info.handle(parameterless=[Cooldown(cd_time=1.4)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    bot, _ = await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return

    uid = str(user_info["user_id"])
    map_data = _load_map_data()
    st = _get_player_map_status(uid, map_data)

    query = args.extract_plain_text().strip()

    # 无参数：显示当前天节点列表
    if not query:
        realm = st["realm"]
        heaven = st["heaven"]
        cur_node_id = st["node_id"]
        cur_node = _find_node_by_id(map_data, realm, heaven, cur_node_id)

        lines = [
            "【地图信息】",
            f"当前位置：{realm}·{heaven}·{cur_node['name']}（{cur_node['type']}）" if cur_node else f"当前位置：{realm}·{heaven}",
            "—— 当前天可前往节点 ——"
        ]
        for n in _nodes(map_data, realm, heaven):
            mark = "📍" if n["id"] == cur_node_id else "▫"
            lines.append(f"{mark} {n['name']}（{n['type']}）")
        await handle_send(bot, event, "\n".join(lines))
        return

    kind, parsed = _parse_map_query(map_data, query)
    if kind is None:
        await handle_send(bot, event, f"未识别参数【{query}】，可输入界名或天名。")
        return

    # 查询“界” -> 显示天列表
    if kind == "realm":
        realm = parsed
        heavens = _get_realm_heaven_order(map_data, realm)
        lines = [f"【地图 - {realm}】", "—— 天层列表 ——"]
        for h in heavens:
            if st["realm"] == realm and st["heaven"] == h:
                lines.append(f"📍 {h}")
            else:
                lines.append(f"▫ {h}")
        await handle_send(bot, event, "\n".join(lines))
        return

    # 查询“天” -> 显示该天节点
    if kind == "heaven":
        realm, heaven = parsed
        lines = [f"【地图 - {realm}·{heaven}】", "—— 节点列表 ——"]
        for n in _nodes(map_data, realm, heaven):
            mark = "📍" if (st["realm"] == realm and st["heaven"] == heaven and st["node_id"] == n["id"]) else "▫"
            lines.append(f"{mark} {n['name']}（{n['type']}）")
        await handle_send(bot, event, "\n".join(lines))
        return


@my_pos.handle(parameterless=[Cooldown(cd_time=1.4)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    bot, _ = await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return

    uid = str(user_info["user_id"])
    map_data = _load_map_data()
    st = _get_player_map_status(uid, map_data)
    node = _find_node_by_id(map_data, st["realm"], st["heaven"], st["node_id"])
    m = f"当前位置：{st['realm']}·{st['heaven']}·{node['name']}（{node['type']}）" if node else f"当前位置：{st['realm']}·{st['heaven']}"
    await handle_send(bot, event, m)


@map_go.handle(parameterless=[Cooldown(cd_time=1.4)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    bot, _ = await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return

    uid = str(user_info["user_id"])
    stamina = int(user_info.get("user_stamina", 0))
    target_name = args.extract_plain_text().strip()
    if not target_name:
        await handle_send(bot, event, "请使用：前往 节点名")
        return

    map_data = _load_map_data()
    st = _get_player_map_status(uid, map_data)
    found = _find_node_by_name(map_data, target_name)
    if not found:
        await handle_send(bot, event, f"未找到节点【{target_name}】")
        return

    tar_realm, tar_heaven, tar_node = found
    cost, err = _calc_move_cost(
        map_data["meta"],
        map_data,
        st["realm"], st["heaven"], st["node_id"],
        tar_realm, tar_heaven, tar_node["id"]
    )
    if cost < 0:
        await handle_send(bot, event, err)
        return
    if cost == 0:
        await handle_send(bot, event, err or "你已在该节点。")
        return
    if stamina < cost:
        await handle_send(bot, event, f"体力不足，需{cost}，当前{stamina}")
        return

    sql_message.update_user_stamina(uid, cost, 2)
    player_data_manager.update_or_write_data(uid, MAP_TABLE, "realm", tar_realm)
    player_data_manager.update_or_write_data(uid, MAP_TABLE, "heaven", tar_heaven)
    player_data_manager.update_or_write_data(uid, MAP_TABLE, "node_id", tar_node["id"])

    visited = player_data_manager.get_field_data(uid, MAP_TABLE, "visited_nodes") or []
    if tar_node["id"] not in visited:
        visited.append(tar_node["id"])
    player_data_manager.update_or_write_data(uid, MAP_TABLE, "visited_nodes", visited)

    await handle_send(
        bot, event,
        f"你已前往：{tar_realm}·{tar_heaven}·{tar_node['name']}（{tar_node['type']}）\n"
        f"消耗体力：{cost}，剩余体力：{number_to(stamina - cost)}"
    )


@build_dongfu.handle(parameterless=[Cooldown(cd_time=1.4)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    bot, _ = await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return

    user_id = str(user_info["user_id"])
    map_data = _load_map_data()
    status = _get_player_map_status(user_id, map_data)

    dongfu_data = player_data_manager.get_fields(user_id, DONGFU_TABLE) or {}
    if int(dongfu_data.get("built", 0)) == 1:
        await handle_send(bot, event, f"你已建立洞府：{dongfu_data.get('node_name', '未知节点')}，无需重复建设。")
        return

    realm = status["realm"]
    heaven = status["heaven"]
    node_id = status["node_id"]

    node = _find_node_by_id(map_data, realm, heaven, node_id)
    if not node:
        await handle_send(bot, event, "当前位置异常，无法建设洞府。")
        return

    node_type = node.get("type", "")
    if node_type in FORBIDDEN_DONGFU_TYPES:
        buildable_names = []
        for n in _nodes(map_data, realm, heaven):
            if n.get("type", "") not in FORBIDDEN_DONGFU_TYPES:
                buildable_names.append(n["name"])

        if buildable_names:
            await handle_send(
                bot, event,
                f"当前节点【{node['name']}】（{node_type}）不可建设洞府。\n"
                f"当前天可建设节点：{'、'.join(buildable_names)}"
            )
        else:
            await handle_send(
                bot, event,
                f"当前节点【{node['name']}】（{node_type}）不可建设洞府。\n"
                f"当前天暂无可建设节点，请前往其他天或其他界。"
            )
        return

    if int(user_info.get("stone", 0)) < DONGFU_COST:
        await handle_send(bot, event, f"建设洞府需要{number_to(DONGFU_COST)}灵石，你当前灵石不足。")
        return

    sql_message.update_ls(user_id, DONGFU_COST, 2)

    save_data = {
        "built": 1,
        "realm": realm,
        "heaven": heaven,
        "node_id": node["id"],
        "node_name": node["name"]
    }
    for k, v in save_data.items():
        player_data_manager.update_or_write_data(user_id, DONGFU_TABLE, k, v)

    await handle_send(
        bot, event,
        f"洞府建设成功！\n"
        f"位置：{realm}·{heaven}·{node['name']}\n"
        f"消耗灵石：{number_to(DONGFU_COST)}"
    )


@go_home.handle(parameterless=[Cooldown(cd_time=1.4)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    bot, _ = await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return

    user_id = str(user_info["user_id"])
    dongfu_data = player_data_manager.get_fields(user_id, DONGFU_TABLE) or {}

    if int(dongfu_data.get("built", 0)) != 1:
        await handle_send(bot, event, "你尚未建设洞府，请先使用【建设洞府】。")
        return

    home_realm = dongfu_data.get("realm", "")
    home_heaven = dongfu_data.get("heaven", "")
    home_node_id = dongfu_data.get("node_id", "")
    home_node_name = dongfu_data.get("node_name", "未知节点")

    if not home_realm or not home_heaven or not home_node_id:
        await handle_send(bot, event, "洞府数据异常，请联系管理员处理。")
        return

    player_data_manager.update_or_write_data(user_id, MAP_TABLE, "realm", home_realm)
    player_data_manager.update_or_write_data(user_id, MAP_TABLE, "heaven", home_heaven)
    player_data_manager.update_or_write_data(user_id, MAP_TABLE, "node_id", home_node_id)

    visited = player_data_manager.get_field_data(user_id, MAP_TABLE, "visited_nodes") or []
    if home_node_id not in visited:
        visited.append(home_node_id)
    player_data_manager.update_or_write_data(user_id, MAP_TABLE, "visited_nodes", visited)

    await handle_send(bot, event, f"你已回到洞府：{home_realm}·{home_heaven}·{home_node_name}")


@nearby_users_cmd.handle(parameterless=[Cooldown(cd_time=1.4)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    bot, _ = await assign_bot(bot=bot, event=event)
    is_user, user_info, m = check_user(event)
    if not is_user:
        await handle_send(bot, event, m, md_type="我要修仙")
        return

    uid = str(user_info["user_id"])
    map_data = _load_map_data()
    st = _get_player_map_status(uid, map_data)

    users = _get_all_in_same_node(st["realm"], st["heaven"], st["node_id"])
    users = [u for u in users if str(u["user_id"]) != uid]

    if not users:
        await handle_send(bot, event, "附近暂无其他道友。")
        return

    if len(users) > 10:
        users = random.sample(users, 10)

    lines = ["【附近道友】"]
    for u in users:
        lines.append(f"- {u['user_name']}（{u['level']}）")
    await handle_send(bot, event, "\n".join(lines))


def _add_dao_record(user_id, win: bool):
    total = player_data_manager.get_field_data(str(user_id), "dao_record", "total") or 0
    win_n = player_data_manager.get_field_data(str(user_id), "dao_record", "win") or 0
    lose_n = player_data_manager.get_field_data(str(user_id), "dao_record", "lose") or 0
    total += 1
    if win:
        win_n += 1
    else:
        lose_n += 1
    player_data_manager.update_or_write_data(str(user_id), "dao_record", "total", total)
    player_data_manager.update_or_write_data(str(user_id), "dao_record", "win", win_n)
    player_data_manager.update_or_write_data(str(user_id), "dao_record", "lose", lose_n)


@dao_qc.handle(parameterless=[Cooldown(cd_time=20, stamina_cost=1)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    bot, _ = await assign_bot(bot=bot, event=event)
    is_user, user_info, m = check_user(event)
    if not is_user:
        await handle_send(bot, event, m, md_type="我要修仙")
        return

    uid = str(user_info["user_id"])
    target_name = args.extract_plain_text().strip()

    map_data = _load_map_data()
    st = _get_player_map_status(uid, map_data)
    nearby = _get_all_in_same_node(st["realm"], st["heaven"], st["node_id"])
    nearby = [u for u in nearby if str(u["user_id"]) != uid]
    if not nearby:
        await handle_send(bot, event, "附近无可切磋道友。")
        return

    if target_name:
        target = next((u for u in nearby if u["user_name"] == target_name), None)
        if not target:
            await handle_send(bot, event, f"附近未找到道友【{target_name}】")
            return
    else:
        target = random.choice(nearby)

    my_power = int(user_info.get("power", 0))
    ta_power = int(target.get("power", 0))
    my_win_rate = 0.5 if (my_power + ta_power) == 0 else my_power / (my_power + ta_power)
    my_win = random.random() < my_win_rate

    _add_dao_record(uid, my_win)
    _add_dao_record(target["user_id"], not my_win)

    winner = user_info["user_name"] if my_win else target["user_name"]
    await handle_send(bot, event, f"你与【{target['user_name']}】论道切磋一番，胜者：{winner}")


@dao_view.handle(parameterless=[Cooldown(cd_time=1.4)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    bot, _ = await assign_bot(bot=bot, event=event)
    is_user, user_info, m = check_user(event)
    if not is_user:
        await handle_send(bot, event, m, md_type="我要修仙")
        return

    uid = str(user_info["user_id"])
    target_name = args.extract_plain_text().strip()

    target_id = uid
    show_name = user_info["user_name"]

    if target_name:
        map_data = _load_map_data()
        st = _get_player_map_status(uid, map_data)
        nearby = _get_all_in_same_node(st["realm"], st["heaven"], st["node_id"])
        target = next((u for u in nearby if u["user_name"] == target_name), None)
        if not target:
            await handle_send(bot, event, f"附近未找到道友【{target_name}】")
            return
        target_id = str(target["user_id"])
        show_name = target["user_name"]

    total = player_data_manager.get_field_data(target_id, "dao_record", "total") or 0
    win_n = player_data_manager.get_field_data(target_id, "dao_record", "win") or 0
    lose_n = player_data_manager.get_field_data(target_id, "dao_record", "lose") or 0
    rate = (win_n / total * 100) if total else 0.0
    await handle_send(bot, event, f"【论道战绩】{show_name}\n总场次：{total}\n胜场：{win_n}\n负场：{lose_n}\n胜率：{rate:.1f}%")


@seed_shop.handle(parameterless=[Cooldown(cd_time=1.4)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    bot, _ = await assign_bot(bot=bot, event=event)
    is_user, user_info, m = check_user(event)
    if not is_user:
        await handle_send(bot, event, m, md_type="我要修仙")
        return

    uid = str(user_info["user_id"])
    map_data = _load_map_data()
    st = _get_player_map_status(uid, map_data)
    node = _find_node_by_id(map_data, st["realm"], st["heaven"], st["node_id"])
    if not node:
        await handle_send(bot, event, "当前位置异常。")
        return

    if not _is_seed_shop_node(node["type"]):
        await handle_send(bot, event, f"当前节点【{node['name']}】无种子商店。")
        return

    lines = [f"【种子商店 - {node['name']}】", "可购买："]
    for sid, conf in SEED_CONFIG.items():
        lines.append(f"- {conf['name']}：{number_to(conf['price'])}灵石")
    lines.append("购买格式：购买种子 种子名 数量")
    await handle_send(bot, event, "\n".join(lines))


@buy_seed.handle(parameterless=[Cooldown(cd_time=1.4)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    bot, _ = await assign_bot(bot=bot, event=event)
    is_user, user_info, m = check_user(event)
    if not is_user:
        await handle_send(bot, event, m, md_type="我要修仙")
        return

    uid = str(user_info["user_id"])
    text = args.extract_plain_text().strip()
    parts = text.split()
    if len(parts) < 2:
        await handle_send(bot, event, "购买格式：购买种子 种子名 数量")
        return

    seed_name = parts[0]
    try:
        num = int(parts[1])
    except Exception:
        await handle_send(bot, event, "数量必须是整数。")
        return
    if num <= 0:
        await handle_send(bot, event, "数量需大于0。")
        return

    map_data = _load_map_data()
    st = _get_player_map_status(uid, map_data)
    node = _find_node_by_id(map_data, st["realm"], st["heaven"], st["node_id"])
    if not node:
        await handle_send(bot, event, "当前位置异常。")
        return
    if not _is_seed_shop_node(node["type"]):
        await handle_send(bot, event, f"当前节点【{node['name']}】无种子商店。")
        return

    seed_id = None
    for sid, conf in SEED_CONFIG.items():
        if conf["name"] == seed_name:
            seed_id = sid
            break
    if seed_id is None:
        await handle_send(bot, event, f"未找到种子【{seed_name}】")
        return

    cost = SEED_CONFIG[seed_id]["price"] * num
    if int(user_info.get("stone", 0)) < cost:
        await handle_send(bot, event, f"灵石不足，需{number_to(cost)}。")
        return

    sql_message.update_ls(uid, cost, 2)
    sql_message.send_back(uid, seed_id, SEED_CONFIG[seed_id]["name"], "特殊物品", num, 1)

    await handle_send(bot, event, f"购买成功：{SEED_CONFIG[seed_id]['name']} x{num}，花费{number_to(cost)}灵石。")