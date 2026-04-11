try:
    import ujson as json
except ImportError:
    import json

import random
import asyncio
from datetime import datetime, timedelta
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

# ============================================================
# 基础掉落池（你可继续扩）
# ============================================================
ACTION_ITEM_POOLS = {
    # 钓鱼：偏水系/寒系/湿地药材
    "fish": [
        3053, 3067, 3092, 3087, 3100, 3027, 3032, 3065, 3068, 3040,
        3004, 3048, 3051, 3091, 3108
    ],

    # 挖矿：偏矿脉/岩系/根茎类药材
    "ore": [
        3034, 3105, 3106, 3017, 3085, 3025, 3028, 3033, 3101, 3073,
        3005, 3013, 3050, 3098, 3104
    ],

    # 采集：草木综合池（数量更大）
    "herb": [
        3001, 3002, 3003, 3004, 3005, 3006, 3009, 3010, 3018, 3021,
        3022, 3024, 3037, 3038, 3041, 3042, 3045, 3046, 3049, 3054,
        3057, 3058, 3061, 3062, 3069, 3070, 3077, 3078, 3081, 3082,
        3089, 3090, 3093, 3094
    ],

    # ===== 功能池 =====
    "wash_stone": [20023],
    "token_common": [20001, 20007, 20012, 20020, 20021],
    "token_mid": [20005, 20010, 20014, 20015, 20017],
    "token_high": [20002, 20003, 20006, 20013],

    # ===== 神物池 =====
    "god_low": [15000, 15001, 15002, 15003, 15004, 15005, 15006, 15007, 15008, 15009],
    "god_high": [15010, 15011, 15012, 15013, 15014, 15015],

    # ===== 饰品礼包池 =====
    "acc_pack_low": [18121, 18122, 18123, 18131, 18132, 18133],
    "acc_pack_high": [18134, 18159, 18160, 18161, 18162, 18163, 18164, 18165, 18166],
}

# ============================================================
# 交互式采集配置（仅钓鱼/挖矿/采集）
# ============================================================
INTERACTIVE_ACTION_CONFIG = {
    "钓鱼": {
        "start_msg": "你抛下鱼钩，静候水波……",
        "wait_min": 10,
        "wait_max": 30,
        "trigger_msg": "🎣 鱼上钩了！请在20秒内发送【收杆】",
        "resolve_cmd": "收杆",
        "resolve_timeout": 20,
        "cooldown_sec": 35,
        "success_rate": 0.78,
    },
    "挖矿": {
        "start_msg": "你举镐探脉，细听地鸣……",
        "wait_min": 8,
        "wait_max": 22,
        "trigger_msg": "⛏️ 矿脉显形！请在20秒内发送【落镐】",
        "resolve_cmd": "落镐",
        "resolve_timeout": 20,
        "cooldown_sec": 30,
        "success_rate": 0.82,
    },
    "采集": {
        "start_msg": "你凝神寻药，分辨灵机……",
        "wait_min": 6,
        "wait_max": 18,
        "trigger_msg": "🌿 灵草现形！请在20秒内发送【采收】",
        "resolve_cmd": "采收",
        "resolve_timeout": 20,
        "cooldown_sec": 25,
        "success_rate": 0.85,
    },
}

# 玩家交互状态（采集）
INTERACTIVE_ACTION_STATE = {}

# ============================================================
# 对战玩法配置（试炼/险地）
# ============================================================
COMBAT_NODE_TYPES = {"试炼", "险地"}

COMBAT_CONFIG = {
    "试炼": {
        "stamina_cost": 8,
        "cooldown_sec": 45,
        "base_win_rate": 0.72,
        "reward_plan": [
            ("token_common", 1, 2, 1.0),
            ("wash_stone", 1, 2, 0.65),
            ("acc_pack_low", 1, 1, 0.10),
        ],
        "fail_msg": "试炼失利，你负伤而退。",
    },
    "险地": {
        "stamina_cost": 12,
        "cooldown_sec": 70,
        "base_win_rate": 0.55,
        "reward_plan": [
            ("token_mid", 1, 2, 0.95),
            ("wash_stone", 1, 3, 0.8),
            ("god_low", 1, 1, 0.18),
            ("acc_pack_high", 1, 1, 0.05),
        ],
        "fail_msg": "险地凶险万分，你仓促脱身。",
    },
}

# 玩家战斗冷却缓存
COMBAT_CD_STATE = {}

# ============================================================
# 长时间探索玩法（遗迹/情报/宫殿）
# - 发起一次，按时间结算
# - 支持上限（超过按上限算）
# ============================================================
EXPLORE_NODE_TYPES = {"遗迹", "情报", "宫殿"}

EXPLORE_CONFIG = {
    "遗迹": {
        "stamina_cost": 6,
        "duration_min": 20,      # 建议时长
        "max_duration_min": 120, # 结算上限
        "base_interval_min": 20, # 每20分钟一轮结算
        "reward_plan": [
            ("wash_stone", 1, 2, 0.9),
            ("token_mid", 1, 2, 0.7),
            ("acc_pack_low", 1, 1, 0.15),
        ],
    },
    "情报": {
        "stamina_cost": 5,
        "duration_min": 15,
        "max_duration_min": 90,
        "base_interval_min": 15,
        "reward_plan": [
            ("token_common", 1, 2, 0.95),
            ("token_mid", 1, 1, 0.45),
            ("wash_stone", 1, 1, 0.35),
        ],
    },
    "宫殿": {
        "stamina_cost": 10,
        "duration_min": 30,
        "max_duration_min": 180,
        "base_interval_min": 30,
        "reward_plan": [
            ("token_high", 1, 2, 0.8),
            ("god_low", 1, 1, 0.28),
            ("acc_pack_high", 1, 1, 0.16),
            ("wash_stone", 2, 4, 0.75),
        ],
    },
}

EXPLORE_TABLE = "map_explore_status"

# ============================================================
# 种子配置
# ============================================================
SEED_CONFIG = {
    21001: {"name": "青灵草种", "price": 500000, "pool": "herb_mid", "minutes": 180},
    21003: {"name": "星砂神种", "price": 15000000, "pool": "god_low", "minutes": 360},
    21004: {"name": "混元神种", "price": 80000000, "pool": "god_high", "minutes": 720},
}

# ============================================================
# 工具函数
# ============================================================
def _load_map_data():
    if not MAP_FILE.exists():
        raise FileNotFoundError(f"未找到地图文件：{MAP_FILE}")
    with open(MAP_FILE, "r", encoding="utf-8") as f:
        return json.load(f)

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
    player_data_manager.update_or_write_data(uid, MAP_TABLE, "realm", realm)
    player_data_manager.update_or_write_data(uid, MAP_TABLE, "heaven", heaven)
    player_data_manager.update_or_write_data(uid, MAP_TABLE, "node_id", node_id)

    visited = player_data_manager.get_field_data(uid, MAP_TABLE, "visited_nodes") or []
    if node_id not in visited:
        visited.append(node_id)
        player_data_manager.update_or_write_data(uid, MAP_TABLE, "visited_nodes", visited)

def get_player_current_node(user_id: str) -> dict | None:
    map_data = _load_map_data()
    status = _get_player_map_status(str(user_id), map_data)
    return _find_node_by_id(map_data, status["realm"], status["heaven"], status["node_id"])

def get_current_node_name(user_id: str) -> str | None:
    node = get_player_current_node(user_id)
    return node["name"] if node else None

def _grant_rewards(user_id: str, reward_plan):
    """
    reward_plan: [(pool_key, min_count, max_count, chance), ...]
    """
    rewards = []
    for pool_key, cmin, cmax, chance in reward_plan:
        if random.random() > chance:
            continue
        pool_ids = ACTION_ITEM_POOLS.get(pool_key, [])
        if not pool_ids:
            continue
        cnt = random.randint(cmin, cmax)
        for _ in range(cnt):
            gid = random.choice(pool_ids)
            info = items.get_data_by_item_id(str(gid))
            if not info:
                continue
            gname = info["name"]
            gtype = info.get("type", "材料")
            # 灵石单独处理（目前池里没放灵石，预留）
            if gname == "灵石":
                sql_message.update_ls(user_id, 1, 1)
                rewards.append("灵石x1")
            else:
                sql_message.send_back(user_id, gid, gname, gtype, 1, 0)
                rewards.append(f"{gname}x1")
    return rewards

# ============================================================
# 交互采集逻辑
# ============================================================
NODE_ACTION_CONFIG = {
    "水域": {"cmd": "钓鱼", "cost": 5, "pool_key": "fish", "desc": "垂钓灵鱼"},
    "矿脉": {"cmd": "挖矿", "cost": 5, "pool_key": "ore",  "desc": "开采灵矿"},
    "灵林": {"cmd": "采集", "cost": 3, "pool_key": "herb", "desc": "采集灵草"},
    "仙山": {"cmd": "采集", "cost": 4, "pool_key": "herb", "desc": "探寻天材"},
}

async def _process_node_action(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, action_type: str):
    ok, user_info, m = check_user(event)
    if not ok:
        await handle_send(bot, event, m, md_type="我要修仙")
        return

    uid = str(user_info["user_id"])
    now = datetime.now()

    # 冷却检查/过期清理
    st = INTERACTIVE_ACTION_STATE.get(uid)
    if st:
        cd_until = st.get("cooldown_until")
        if cd_until and now < cd_until:
            sec = int((cd_until - now).total_seconds())
            await handle_send(bot, event, f"你刚忙完，先歇会儿吧（冷却剩余 {sec}s）")
            return
        if st.get("expire_ts") and now > st["expire_ts"]:
            INTERACTIVE_ACTION_STATE.pop(uid, None)

    node = get_player_current_node(uid)
    if not node:
        await handle_send(bot, event, "当前位置节点数据异常，请尝试重新【前往】其他节点。")
        return

    config = NODE_ACTION_CONFIG.get(node["type"])
    if not config or config["cmd"] != action_type:
        await handle_send(bot, event, f"当前节点【{node['name']}】(类型:{node['type']}) 无法进行【{action_type}】。")
        return

    ia = INTERACTIVE_ACTION_CONFIG[action_type]

    old = INTERACTIVE_ACTION_STATE.get(uid)
    if old and old.get("cooldown_until") and now < old["cooldown_until"]:
        sec = int((old["cooldown_until"] - now).total_seconds())
        await handle_send(bot, event, f"你还在冷却中（{sec}s）")
        return

    stamina = int(user_info.get("user_stamina", 0))
    if stamina < config["cost"]:
        await handle_send(bot, event, f"体力不足！{config['desc']}需消耗 {config['cost']} 体力，当前仅剩 {stamina}。")
        return

    # 发起时扣体力
    sql_message.update_user_stamina(uid, config["cost"], 2)

    wait_sec = random.randint(ia["wait_min"], ia["wait_max"])
    ready_ts = now + timedelta(seconds=wait_sec)
    expire_ts = ready_ts + timedelta(seconds=ia["resolve_timeout"])
    cooldown_until = now + timedelta(seconds=ia["cooldown_sec"])

    INTERACTIVE_ACTION_STATE[uid] = {
        "action": action_type,
        "node_name": node["name"],
        "node_type": node["type"],
        "pool_key": config["pool_key"],
        "ready": False,
        "start_ts": now,
        "ready_ts": ready_ts,
        "expire_ts": expire_ts,
        "cooldown_until": cooldown_until,
        "cost": config["cost"],
    }

    await handle_send(bot, event, f"{ia['start_msg']}\n预计等待 {wait_sec}s...")

    async def _ready_notice():
        await asyncio.sleep(wait_sec)

        cur = INTERACTIVE_ACTION_STATE.get(uid)
        if not cur:
            return
        if cur.get("action") != action_type:
            return

        cur["ready"] = True
        INTERACTIVE_ACTION_STATE[uid] = cur
        await handle_send(bot, event, f"{ia['trigger_msg']}（地点：{node['name']}）")

        await asyncio.sleep(ia["resolve_timeout"])
        cur2 = INTERACTIVE_ACTION_STATE.get(uid)
        if not cur2:
            return
        if cur2.get("action") == action_type and cur2.get("ready") is True:
            INTERACTIVE_ACTION_STATE.pop(uid, None)
            await handle_send(bot, event, f"❌ 时机已过，{action_type}失败（{node['name']}）")

    asyncio.create_task(_ready_notice())

async def _resolve_interactive_action(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, resolve_cmd: str):
    ok, user_info, m = check_user(event)
    if not ok:
        await handle_send(bot, event, m, md_type="我要修仙")
        return

    uid = str(user_info["user_id"])
    now = datetime.now()
    st = INTERACTIVE_ACTION_STATE.get(uid)

    if not st:
        await handle_send(bot, event, "你当前没有进行中的采集行为。")
        return

    action = st.get("action")
    ia = INTERACTIVE_ACTION_CONFIG.get(action)
    if not ia:
        INTERACTIVE_ACTION_STATE.pop(uid, None)
        await handle_send(bot, event, "状态异常，已重置。")
        return

    if ia["resolve_cmd"] != resolve_cmd:
        await handle_send(bot, event, f"当前应使用【{ia['resolve_cmd']}】而不是【{resolve_cmd}】")
        return

    if not st.get("ready"):
        sec = max(1, int((st["ready_ts"] - now).total_seconds()))
        await handle_send(bot, event, f"还没到时机，再等等（约 {sec}s）")
        return

    if st.get("expire_ts") and now > st["expire_ts"]:
        INTERACTIVE_ACTION_STATE.pop(uid, None)
        await handle_send(bot, event, f"❌ 时机已过，{action}失败。")
        return

    if random.random() > ia["success_rate"]:
        INTERACTIVE_ACTION_STATE.pop(uid, None)
        await handle_send(bot, event, f"💨 你动作慢了半拍，{action}失败（目标跑了）")
        return

    # 成功掉落
    pool_key = st["pool_key"]
    reward_plan = [(pool_key, 1, 2, 1.0)]
    # 附加掉落，避免同质化
    if action == "钓鱼":
        reward_plan += [("token_common", 1, 1, 0.25), ("wash_stone", 1, 1, 0.10)]
    elif action == "挖矿":
        reward_plan += [("wash_stone", 1, 2, 0.20), ("token_common", 1, 1, 0.15)]
    elif action == "采集":
        reward_plan += [("token_common", 1, 1, 0.18), ("god_low", 1, 1, 0.06)]

    rewards = _grant_rewards(uid, reward_plan)
    INTERACTIVE_ACTION_STATE.pop(uid, None)

    if rewards:
        await handle_send(bot, event, f"✅ {action}成功！\n地点：{st['node_name']}\n获得：{'、'.join(rewards)}")
    else:
        await handle_send(bot, event, f"✅ {action}完成，但这次没有收获。")

# ============================================================
# 对战节点玩法
# ============================================================
async def _process_node_combat(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    ok, user_info, m = check_user(event)
    if not ok:
        await handle_send(bot, event, m, md_type="我要修仙")
        return

    uid = str(user_info["user_id"])
    now = datetime.now()
    node = get_player_current_node(uid)
    if not node:
        await handle_send(bot, event, "当前位置异常，请先前往有效节点。")
        return

    ntype = node["type"]
    if ntype not in COMBAT_NODE_TYPES:
        await handle_send(bot, event, f"当前节点【{node['name']}】不是对战节点。")
        return

    conf = COMBAT_CONFIG[ntype]

    # 冷却
    cd = COMBAT_CD_STATE.get(uid)
    if cd and now < cd:
        sec = int((cd - now).total_seconds())
        await handle_send(bot, event, f"你刚经历一战，尚需冷却 {sec}s")
        return

    stamina = int(user_info.get("user_stamina", 0))
    if stamina < conf["stamina_cost"]:
        await handle_send(bot, event, f"体力不足！需要 {conf['stamina_cost']}，当前 {stamina}")
        return

    # 扣体力
    sql_message.update_user_stamina(uid, conf["stamina_cost"], 2)
    COMBAT_CD_STATE[uid] = now + timedelta(seconds=conf["cooldown_sec"])

    # 简易胜率：按战力微调
    my_power = int(user_info.get("power", 0))
    adjust = min(0.12, max(-0.12, (my_power - 5_000_000) / 100_000_000))
    win_rate = max(0.2, min(0.9, conf["base_win_rate"] + adjust))

    if random.random() > win_rate:
        await handle_send(bot, event, f"⚔️ {conf['fail_msg']}\n地点：{node['name']}")
        return

    rewards = _grant_rewards(uid, conf["reward_plan"])
    if not rewards:
        await handle_send(bot, event, f"⚔️ 你在【{node['name']}】获胜，但未获得战利品。")
    else:
        await handle_send(bot, event, f"⚔️ 你在【{node['name']}】战胜强敌！\n战利品：{'、'.join(rewards)}")

# ============================================================
# 长时间探索玩法
# ============================================================
def _get_explore_status(uid: str):
    d = player_data_manager.get_fields(str(uid), EXPLORE_TABLE)
    if not d:
        d = {
            "running": 0,
            "node_type": "",
            "node_name": "",
            "start_time": "",
            "duration_min": 0,
            "max_duration_min": 0,
            "interval_min": 0,
            "reward_plan": [],
        }
        for k, v in d.items():
            player_data_manager.update_or_write_data(str(uid), EXPLORE_TABLE, k, v)
    return d

def _save_explore_status(uid: str, d: dict):
    for k, v in d.items():
        player_data_manager.update_or_write_data(str(uid), EXPLORE_TABLE, k, v)

async def _start_explore(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, duration_arg: int | None):
    ok, user_info, m = check_user(event)
    if not ok:
        await handle_send(bot, event, m, md_type="我要修仙")
        return

    uid = str(user_info["user_id"])
    node = get_player_current_node(uid)
    if not node:
        await handle_send(bot, event, "当前位置异常，请先前往有效节点。")
        return

    ntype = node["type"]
    if ntype not in EXPLORE_NODE_TYPES:
        await handle_send(bot, event, f"当前节点【{node['name']}】不支持长时间探索。")
        return

    st = _get_explore_status(uid)
    if int(st.get("running", 0)) == 1:
        await handle_send(bot, event, f"你已有进行中的探索，请先【探索结算】。")
        return

    conf = EXPLORE_CONFIG[ntype]
    need_stamina = conf["stamina_cost"]
    stamina = int(user_info.get("user_stamina", 0))
    if stamina < need_stamina:
        await handle_send(bot, event, f"体力不足！发起探索需 {need_stamina}，当前 {stamina}")
        return

    # 持续时间
    if duration_arg is None:
        duration_min = conf["duration_min"]
    else:
        duration_min = max(5, duration_arg)

    max_duration = conf["max_duration_min"]
    interval_min = conf["base_interval_min"]

    # 发起扣体力（一次性）
    sql_message.update_user_stamina(uid, need_stamina, 2)

    new_st = {
        "running": 1,
        "node_type": ntype,
        "node_name": node["name"],
        "start_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "duration_min": duration_min,
        "max_duration_min": max_duration,
        "interval_min": interval_min,
        "reward_plan": conf["reward_plan"],
    }
    _save_explore_status(uid, new_st)
    await handle_send(
        bot, event,
        f"🧭 已开始【{node['name']}】探索\n"
        f"计划时长：{duration_min}分钟（上限按{max_duration}分钟结算）\n"
        f"可稍后发送【探索结算】领取收益。"
    )

async def _settle_explore(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    ok, user_info, m = check_user(event)
    if not ok:
        await handle_send(bot, event, m, md_type="我要修仙")
        return

    uid = str(user_info["user_id"])
    st = _get_explore_status(uid)
    if int(st.get("running", 0)) != 1:
        await handle_send(bot, event, "当前没有进行中的探索。")
        return

    start_time = st.get("start_time", "")
    dt = None
    try:
        dt = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
    except Exception:
        pass
    if not dt:
        # 异常保护
        st["running"] = 0
        _save_explore_status(uid, st)
        await handle_send(bot, event, "探索状态异常，已重置。")
        return

    now = datetime.now()
    elapsed_min = max(0, int((now - dt).total_seconds() // 60))

    # 实际结算分钟 = min(已过分钟, 计划时长, 上限)
    duration_min = int(st.get("duration_min", 0))
    max_duration = int(st.get("max_duration_min", 0))
    settle_min = min(elapsed_min, duration_min if duration_min > 0 else elapsed_min, max_duration if max_duration > 0 else elapsed_min)

    if settle_min <= 0:
        await handle_send(bot, event, "探索时间太短，暂时无可结算收益。")
        return

    interval = max(1, int(st.get("interval_min", 20)))
    rounds = max(1, settle_min // interval)

    reward_plan = st.get("reward_plan", [])
    if not isinstance(reward_plan, list):
        reward_plan = []

    all_rewards = []
    for _ in range(rounds):
        all_rewards.extend(_grant_rewards(uid, reward_plan))

    # 重置状态
    st["running"] = 0
    st["node_type"] = ""
    st["node_name"] = ""
    st["start_time"] = ""
    st["duration_min"] = 0
    st["max_duration_min"] = 0
    st["interval_min"] = 0
    st["reward_plan"] = []
    _save_explore_status(uid, st)

    node_name = st.get("node_name", "未知地点")
    if not all_rewards:
        await handle_send(bot, event, f"🧭 探索结算完成（有效{settle_min}分钟），本次未获得奖励。")
    else:
        await handle_send(
            bot, event,
            f"🧭 探索结算完成\n有效探索：{settle_min}分钟（共{rounds}轮）\n获得：{'、'.join(all_rewards[:80])}"
        )

# ============================================================
# 命令注册
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

# 采集类
fishing_cmd = on_command("钓鱼", priority=8, block=True)
mining_cmd = on_command("挖矿", priority=8, block=True)
gathering_cmd = on_command("采集", priority=8, block=True)
fish_pull_cmd = on_command("收杆", priority=8, block=True)
mine_hit_cmd = on_command("落镐", priority=8, block=True)
gather_pick_cmd = on_command("采收", priority=8, block=True)

# 对战类
node_combat_cmd = on_command("节点战斗", aliases={"试炼挑战", "险地挑战"}, priority=8, block=True)

# 探索类
start_explore_cmd = on_command("开始探索", aliases={"节点探索"}, priority=8, block=True)
settle_explore_cmd = on_command("探索结算", priority=8, block=True)

# ============================================================
# 命令处理器
# ============================================================
@map_help.handle(parameterless=[Cooldown(cd_time=1.4)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    bot, _ = await assign_bot(bot=bot, event=event)
    msg = (
        "🗺️【地图系统帮助】\n"
        "1️⃣ 导航查询：地图 / 我的位置 / 前往 节点名\n"
        "2️⃣ 洞府系统：建设洞府 / 回府\n"
        "3️⃣ 社交互动：附近道友 / 论道切磋 / 论道查看\n"
        "4️⃣ 交互采集：钓鱼→收杆 / 挖矿→落镐 / 采集→采收\n"
        "5️⃣ 对战玩法：节点战斗（试炼/险地节点）\n"
        "6️⃣ 长时探索：开始探索 [分钟] / 探索结算（遗迹/情报/宫殿）\n"
        "💡 跨界跨天需在交通/渡口/驿站。"
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

    # 识别目标
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

    # 跨界
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

    # 跨天
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

        tar_node = _nodes(map_data, tar_realm, tar_heaven)[0]
        cost = int(cost_cfg.get("cross_heaven", 50))
        if stamina < cost:
            await handle_send(bot, event,cost}，当前{stamina}。")
            return

        _save_map_status(uid, tar_realm, tar_heaven, tar_node["id"])
        sql_message.update_user_stamina(uid, cost, 2)
        await handle_send(bot, event, f"☁️ 跨天成功！\n已抵达 {tar_realm}·{tar_heaven}·{tar_node['name']}\n消耗体力：{cost}，剩余：{number_to(stamina - cost)}")
        return

    # 同天节点移动
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

    users = _get_all_in_same_node(st["realm"], st["heaven"], st["node_id"])
    filtered_users = [u for u in users if str(u.get("user_id")) != uid]

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


# =========================
# 采集入口（交互式）
# =========================
@fishing_cmd.handle(parameterless=[Cooldown(cd_time=2.0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    await _process_node_action(bot, event, "钓鱼")


@mining_cmd.handle(parameterless=[Cooldown(cd_time=2.0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    await _process_node_action(bot, event, "挖矿")


@gathering_cmd.handle(parameterless=[Cooldown(cd_time=2.0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    await _process_node_action(bot, event, "采集")


@fish_pull_cmd.handle(parameterless=[Cooldown(cd_time=0.6)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    await _resolve_interactive_action(bot, event, "收杆")


@mine_hit_cmd.handle(parameterless=[Cooldown(cd_time=0.6)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    await _resolve_interactive_action(bot, event, "落镐")


@gather_pick_cmd.handle(parameterless=[Cooldown(cd_time=0.6)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    await _resolve_interactive_action(bot, event, "采收")


# =========================
# 对战节点（试炼/险地）
# =========================
@node_combat_cmd.handle(parameterless=[Cooldown(cd_time=1.2)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    bot, _ = await assign_bot(bot=bot, event=event)
    await _process_node_combat(bot, event)


# =========================
# 长时间探索（遗迹/情报/宫殿）
# =========================
@start_explore_cmd.handle(parameterless=[Cooldown(cd_time=1.2)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    bot, _ = await assign_bot(bot=bot, event=event)
    txt = args.extract_plain_text().strip()
    duration_arg = None
    if txt:
        try:
            duration_arg = int(txt)
        except Exception:
            duration_arg = None
    await _start_explore(bot, event, duration_arg)


@settle_explore_cmd.handle(parameterless=[Cooldown(cd_time=1.2)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    bot, _ = await assign_bot(bot=bot, event=event)
    await _settle_explore(bot, event)