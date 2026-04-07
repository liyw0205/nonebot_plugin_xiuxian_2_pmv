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
from ..xiuxian_utils.item_json import Items

sql_message = XiuxianDateManage()
player_data_manager = PlayerDataManager()
items = Items()

MAP_FILE = Path() / "data" / "xiuxian" / "地图.json"
MAP_TABLE = "map_status"
DONGFU_TABLE = "dongfu_status"

# ===== 洞府建设配置 =====
DONGFU_COST = 100000000
FORBIDDEN_DONGFU_TYPES = {"坊市", "渡口", "驿站", "交通", "关隘", "情报", "宫殿", "试炼"}

# ===== 移动节点类型 =====
TRAVEL_NODE_TYPES = {"交通", "渡口", "驿站"}

# ===== 节点功能 =====
SEED_SHOP_TYPES = {"坊市", "城池", "驿站"}

# ===== 节点生产配置 =====
NODE_ACTION_CONFIG = {
    "水域": {"cmd": "钓鱼", "cost": 5,  "pool_key": "fish", "desc": "垂钓灵鱼"},
    "矿脉": {"cmd": "挖矿", "cost": 5,  "pool_key": "ore",  "desc": "开采灵矿"},
    "灵林": {"cmd": "采集", "cost": 3,  "pool_key": "herb", "desc": "采集灵草"},
    "仙山": {"cmd": "采集", "cost": 4,  "pool_key": "herb", "desc": "探寻天材"},
}

# 📦 奖励物品池配置
ACTION_ITEM_POOLS = {
    "fish": [40001, 40002, 40003, 40004, 40005],
    "ore":  [40006, 40007, 40008, 40009, 40010],
    "herb": [3001, 3002, 3003, 3004, 3005],
}

# ===== 种子配置 =====
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
    q = (text or "").strip()
    if not q:
        return None, None
    for realm in _all_realms(map_data):
        if q == realm:
            return "realm", realm
    for realm in _all_realms(map_data):
        heavens = _heaven_names(map_data, realm)
        if q in heavens:
            return "heaven", (realm, q)
    return None, None


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


def _save_map_status(uid: str, realm: str, heaven: str, node_id: str):
    """统一保存玩家地图状态及访问记录"""
    player_data_manager.update_or_write_data(uid, MAP_TABLE, "realm", realm)
    player_data_manager.update_or_write_data(uid, MAP_TABLE, "heaven", heaven)
    player_data_manager.update_or_write_data(uid, MAP_TABLE, "node_id", node_id)
    
    visited = player_data_manager.get_field_data(uid, MAP_TABLE, "visited_nodes") or []
    if node_id not in visited:
        visited.append(node_id)
        player_data_manager.update_or_write_data(uid, MAP_TABLE, "visited_nodes", visited)


# ============================================================
# 🆕 便捷函数
# ============================================================
def get_player_current_node(user_id: str) -> dict | None:
    map_data = _load_map_data()
    status = _get_player_map_status(str(user_id), map_data)
    return _find_node_by_id(map_data, status["realm"], status["heaven"], status["node_id"])


def get_current_node_name(user_id: str) -> str | None:
    node = get_player_current_node(user_id)
    return node["name"] if node else None


# ============================================================
# 🆕 节点生产逻辑核心处理器
# ============================================================
async def _process_node_action(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, action_type: str):
    ok, user_info, m = check_user(event)
    if not ok:
        await handle_send(bot, event, m, md_type="我要修仙")
        return

    uid = str(user_info["user_id"])
    node = get_player_current_node(uid)
    if not node:
        await handle_send(bot, event, "当前位置节点数据异常，请尝试重新【前往】其他节点。")
        return

    config = NODE_ACTION_CONFIG.get(node["type"])
    if not config or config["cmd"] != action_type:
        await handle_send(bot, event, f"当前节点【{node['name']}】(类型:{node['type']}) 无法进行【{action_type}】。")
        return

    stamina = int(user_info.get("user_stamina", 0))
    if stamina < config["cost"]:
        await handle_send(bot, event, f"体力不足！{config['desc']}需消耗 {config['cost']} 体力，当前仅剩 {stamina}。")
        return

    sql_message.update_user_stamina(uid, config["cost"], 2)

    pool_ids = ACTION_ITEM_POOLS.get(config["pool_key"], [])
    if not pool_ids:
        await handle_send(bot, event, f"⚠️ 系统配置缺失 {config['pool_key']} 物品池，请联系管理员补全。")
        return

    reward_count = random.randint(1, 2)
    rewards = []
    for _ in range(reward_count):
        item_id = random.choice(pool_ids)
        item_data = items.get_data_by_item_id(str(item_id))
        if item_data:
            sql_message.send_back(uid, item_id, item_data["name"], item_data.get("type", "材料"), 1, 0)
            rewards.append(item_data["name"])

    if rewards:
        msg = f"✅ 在【{node['name']}】成功{config['cmd']}！\n"
        msg += f"🔋 消耗体力：{config['cost']}\n"
        msg += f"🎁 获得物品：{'、'.join(rewards)}"
    else:
        msg = f"💨 在【{node['name']}】{config['cmd']}一番，似乎什么也没得到...\n"
        msg += f"🔋 消耗体力：{config['cost']}"
    await handle_send(bot, event, msg)


# ============================================================
# 🟢 命令注册
# ============================================================
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

fishing_cmd = on_command("钓鱼", priority=8, block=True)
mining_cmd = on_command("挖矿", priority=8, block=True)
gathering_cmd = on_command("采集", priority=8, block=True)


# ============================================================
# 🟡 命令处理器
# ============================================================
@map_help.handle(parameterless=[Cooldown(cd_time=1.4)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    bot, _ = await assign_bot(bot=bot, event=event)
    msg = (
        "🗺️【地图系统帮助】\n"
        "1️⃣ 导航查询：地图 / 我的位置 / 前往 节点名\n"
        "2️⃣ 洞府系统：建设洞府 / 回府\n"
        "3️⃣ 社交互动：附近道友 / 论道切磋 / 论道查看\n"
        "4️⃣ 节点生产：🎣钓鱼 / ⛏️挖矿 / 🌿采集\n"
        "💡 提示：移动消耗体力。跨界/跨天需至【交通/渡口/驿站】节点。"
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
    if not query:
        cur_node = _find_node_by_id(map_data, st["realm"], st["heaven"], st["node_id"])
        lines = [
            "【地图信息】",
            f"当前位置：{st['realm']}·{st['heaven']}·{cur_node['name']}（{cur_node['type']}）" if cur_node else f"当前位置：{st['realm']}·{st['heaven']}",
            "—— 当前天可前往节点 ——"
        ]
        for n in _nodes(map_data, st["realm"], st["heaven"]):
            mark = "📍" if n["id"] == st["node_id"] else "▫"
            lines.append(f"{mark} {n['name']}（{n['type']}）")
        await handle_send(bot, event, "\n".join(lines))
        return

    kind, parsed = _parse_map_query(map_data, query)
    if kind is None:
        await handle_send(bot, event, f"未识别参数【{query}】，可输入界名或天名。")
        return

    if kind == "realm":
        heavens = _get_realm_heaven_order(map_data, parsed)
        lines = [f"【地图 - {parsed}】", "—— 天层列表 ——"]
        for h in heavens:
            mark = "📍" if st["heaven"] == h else "▫"
            lines.append(f"{mark} {h}")
        await handle_send(bot, event, "\n".join(lines))
        return

    if kind == "heaven":
        r, h = parsed
        lines = [f"【地图 - {r}·{h}】", "—— 节点列表 ——"]
        for n in _nodes(map_data, r, h):
            mark = "📍" if st["realm"] == r and st["heaven"] == h and st["node_id"] == n["id"] else "▫"
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


# 🚶 核心路由：前往命令
@map_go.handle(parameterless=[Cooldown(cd_time=1.4)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    bot, _ = await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return

    uid = str(user_info["user_id"])
    target_text = args.extract_plain_text().strip()
    if not target_text:
        await handle_send(bot, event, "请使用：前往 [节点名/天名/界名]")
        return

    map_data = _load_map_data()
    st = _get_player_map_status(uid, map_data)
    meta = map_data.get("meta", {})
    cost_cfg = meta.get("move_cost", {})

    stamina = int(user_info.get("user_stamina", 0))
    cur_node = _find_node_by_id(map_data, st["realm"], st["heaven"], st["node_id"])
    if not cur_node:
        await handle_send(bot, event, "当前位置节点数据异常。")
        return

    # 1. 智能识别目标类型 (界 > 天 > 节点)
    t_type, t_data = None, None
    if target_text in _all_realms(map_data):
        t_type, t_data = "realm", target_text
    else:
        found_heaven = False
        for r in _all_realms(map_data):
            if target_text in _heaven_names(map_data, r):
                t_type, t_data = "heaven", (r, target_text)
                found_heaven = True
                break
        if not found_heaven:
            found_node = _find_node_by_name(map_data, target_text)
            if found_node:
                t_type, t_data = "node", found_node
            else:
                await handle_send(bot, event, f"未找到地点【{target_text}】，请检查名称是否正确。")
                return

    # ================= 跨界逻辑 =================
    if t_type == "realm":
        tar_realm = t_data
        if st["realm"] == tar_realm:
            await handle_send(bot, event, "你已在此界。")
            return
        if cur_node["type"] not in TRAVEL_NODE_TYPES:
            travel_nodes = [n["name"] for n in _nodes(map_data, st["realm"], st["heaven"]) if n["type"] in TRAVEL_NODE_TYPES]
            tip = f"跨界需通过【交通/渡口/驿站】节点。\n请先前往本天内的：{'、'.join(travel_nodes)}" if travel_nodes else "本天暂无交通节点，无法跨界。"
            await handle_send(bot, event, tip)
            return

        first_heaven = _get_realm_heaven_order(map_data, tar_realm)[0]
        tar_node = _nodes(map_data, tar_realm, first_heaven)[0]
        cost = int(cost_cfg.get("cross_realm", 300))
        if stamina < cost:
            await handle_send(bot, event, f"跨界体力不足！需{cost}，当前{stamina}。")
            return
            
        _save_map_status(uid, tar_realm, first_heaven, tar_node["id"])
        sql_message.update_user_stamina(uid, cost, 2)
        await handle_send(bot, event, f"🚀 跨界成功！\n已抵达 {tar_realm}·{first_heaven}·{tar_node['name']}\n消耗体力：{cost}，剩余：{number_to(stamina - cost)}")
        return

    # ================= 跨天逻辑 ✅ 支持任意层跳跃，体力固定 =================
    if t_type == "heaven":
        tar_realm, tar_heaven = t_data
        if st["realm"] != tar_realm:
            await handle_send(bot, event, f"跨天需在界内进行，请先前往【{tar_realm}】。")
            return
        if st["heaven"] == tar_heaven:
            await handle_send(bot, event, "你已在此天。")
            return
        if cur_node["type"] not in TRAVEL_NODE_TYPES:
            travel_nodes = [n["name"] for n in _nodes(map_data, st["realm"], st["heaven"]) if n["type"] in TRAVEL_NODE_TYPES]
            tip = f"跨天需通过【交通/渡口/驿站】节点。\n请先前往本天内的：{'、'.join(travel_nodes)}" if travel_nodes else "本天暂无交通节点，无法跨天。"
            await handle_send(bot, event, tip)
            return

        # ✅ 允许直接跳跃至任意天，落点为目标的第一个节点
        tar_node = _nodes(map_data, tar_realm, tar_heaven)[0]
        cost = int(cost_cfg.get("cross_heaven", 50))  # ✅ 固定消耗，不按层数累加
        if stamina < cost:
            await handle_send(bot, event, f"跨天体力不足！需{cost}，当前{stamina}。")
            return
            
        _save_map_status(uid, tar_realm, tar_heaven, tar_node["id"])
        sql_message.update_user_stamina(uid, cost, 2)
        await handle_send(bot, event, f"☁️ 跨天成功！\n已抵达 {tar_realm}·{tar_heaven}·{tar_node['name']}\n消耗体力：{cost}，剩余：{number_to(stamina - cost)}")
        return

    # ================= 节点移动逻辑 (同天) ✅ 支持多站直达，体力按步数累加 =================
    if t_type == "node":
        tar_realm, tar_heaven, tar_node = t_data
        if tar_realm != st["realm"]:
            await handle_send(bot, event, f"该节点位于【{tar_realm}】，请先前往该界。")
            return
        if tar_heaven != st["heaven"]:
            await handle_send(bot, event, f"该节点位于【{tar_heaven}】，请先前往该天。")
            return

        nodes_list = _nodes(map_data, tar_realm, tar_heaven)
        all_ids = [n["id"] for n in nodes_list]
        idx_cur = all_ids.index(cur_node["id"])
        idx_tar = all_ids.index(tar_node["id"])
        steps = abs(idx_tar - idx_cur)

        if steps == 0:
            await handle_send(bot, event, "你已在该节点。")
            return

        # ✅ 同天直达，体力 = 步数 × 单步消耗
        cost = steps * int(cost_cfg.get("cross_node", 5))
        if stamina < cost:
            await handle_send(bot, event, f"移动体力不足！需{cost}，当前{stamina}。")
            return

        _save_map_status(uid, tar_realm, tar_heaven, tar_node["id"])
        sql_message.update_user_stamina(uid, cost, 2)
        await handle_send(bot, event, f"👣 移动成功！\n已前往【{tar_node['name']}】\n跨越 {steps} 个节点，消耗体力：{cost}，剩余：{number_to(stamina - cost)}")
        return


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
            await handle_send(bot, event, f"当前节点【{node['name']}】（{node_type}）不可建设洞府。\n当前天可建设节点：{'、'.join(buildable_names)}")
        else:
            await handle_send(bot, event, f"当前节点【{node['name']}】（{node_type}）不可建设洞府。\n当前天暂无可建设节点，请前往其他天或其他界。")
        return

    if int(user_info.get("stone", 0)) < DONGFU_COST:
        await handle_send(bot, event, f"建设洞府需要{number_to(DONGFU_COST)}灵石，你当前灵石不足。")
        return

    sql_message.update_ls(user_id, DONGFU_COST, 2)
    save_data = {"built": 1, "realm": realm, "heaven": heaven, "node_id": node["id"], "node_name": node["name"]}
    for k, v in save_data.items():
        player_data_manager.update_or_write_data(user_id, DONGFU_TABLE, k, v)

    await handle_send(bot, event, f"洞府建设成功！\n位置：{realm}·{heaven}·{node['name']}\n消耗灵石：{number_to(DONGFU_COST)}")


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

    if not all(dongfu_data.get(k) for k in ("realm", "heaven", "node_id")):
        await handle_send(bot, event, "洞府数据异常，请联系管理员处理。")
        return

    _save_map_status(user_id, dongfu_data["realm"], dongfu_data["heaven"], dongfu_data["node_id"])
    await handle_send(bot, event, f"你已回到洞府：{dongfu_data['realm']}·{dongfu_data['heaven']}·{dongfu_data['node_name']}")


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

    # 获取同节点所有用户
    users = _get_all_in_same_node(st["realm"], st["heaven"], st["node_id"])
    
    # ✅ 仅排除当前生效的自身ID。化身/伪装视为独立实体，若在同一节点则正常显示
    filtered_users = [u for u in users if str(u.get("user_id")) != uid]
    
    # 去重（防数据异常导致重复显示）
    seen_ids = set()
    unique_filtered = []
    for u in filtered_users:
        u_id = str(u.get("user_id"))
        if u_id not in seen_ids:
            seen_ids.add(u_id)
            unique_filtered.append(u)

    if not unique_filtered:
        await handle_send(bot, event, "附近暂无其他道友。")
        return

    # 最多随机展示10人，避免刷屏
    if len(unique_filtered) > 10:
        unique_filtered = random.sample(unique_filtered, 10)

    lines = ["【附近道友】"] + [f"- {u['user_name']}（{u['level']}）" for u in unique_filtered]
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
    nearby = [u for u in _get_all_in_same_node(st["realm"], st["heaven"], st["node_id"]) if str(u["user_id"]) != uid]
    if not nearby:
        await handle_send(bot, event, "附近无可切磋道友。")
        return

    target = next((u for u in nearby if u["user_name"] == target_name), None) if target_name else random.choice(nearby)
    if not target:
        await handle_send(bot, event, f"附近未找到道友【{target_name}】")
        return

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


@fishing_cmd.handle(parameterless=[Cooldown(cd_time=2.0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    await _process_node_action(bot, event, "钓鱼")

@mining_cmd.handle(parameterless=[Cooldown(cd_time=2.0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    await _process_node_action(bot, event, "挖矿")

@gathering_cmd.handle(parameterless=[Cooldown(cd_time=2.0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    await _process_node_action(bot, event, "采集")