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
from ..xiuxian_utils.utils import check_user, handle_send, number_to, send_msg_handler
from ..xiuxian_utils.xiuxian2_handle import XiuxianDateManage, PlayerDataManager
from ..xiuxian_utils.item_json import Items
from ..xiuxian_utils.player_fight import Boss_fight
from ..xiuxian_config import base_rank

sql_message = XiuxianDateManage()
player_data_manager = PlayerDataManager()
items = Items()

MAP_FILE = Path() / "data" / "xiuxian" / "地图.json"
MAP_TABLE = "map_status"
MAP_MISSION_TABLE = "map_mission"
DONGFU_TABLE = "dongfu_status"
EXPLORE_TABLE = "map_explore_status"

# 持久化限制表
MAP_LIMIT_TABLE = "map_daily_limit"
MAP_CD_TABLE = "map_cooldown"

# =========================================
# 洞府配置
# =========================================
DONGFU_COST = 100000000
FORBIDDEN_DONGFU_TYPES = {"坊市", "渡口", "驿站", "交通", "关隘", "情报", "宫殿", "试炼"}

# =========================================
# 地图移动
# =========================================
TRAVEL_NODE_TYPES = {"交通", "渡口", "驿站"}
SEED_SHOP_TYPES = {"坊市", "城池", "驿站"}

# =========================================
# 每日限制配置
# =========================================
DAILY_LIMIT_CONFIG = {
    "gather": 30,
    "combat": 15,
    "explore": 5,
}

# 收益衰减配置（按当日总资源行为次数）
REWARD_DECAY_STEPS = [
    (20, 1.00),
    (30, 0.70),
    (999999, 0.35),
]

MAP_MISSION_CONFIG = {
    "gather": {
        "name": "采集委托",
        "count_key": "gather_count",
        "targets": [3, 5, 8],
        "desc": lambda n: f"今日完成采集 {n} 次",
    },
    "combat": {
        "name": "战斗委托",
        "count_key": "combat_count",
        "targets": [1, 2, 3],
        "desc": lambda n: f"今日完成节点战斗 {n} 次",
    },
    "explore": {
        "name": "探索委托",
        "count_key": "explore_count",
        "targets": [1, 2, 3],
        "desc": lambda n: f"今日发起探索 {n} 次",
    },
}

# =========================================
# 种子商店配置
# =========================================
SEED_CONFIG = {
    21001: {"name": "青灵草种", "price": 500000, "pool": "herb_mid", "minutes": 180},
    21002: {"name": "玄木灵种", "price": 3000000, "pool": "herb_mid", "minutes": 240},
    21003: {"name": "星砂神种", "price": 15000000, "pool": "god_low", "minutes": 360},
    21004: {"name": "混元神种", "price": 80000000, "pool": "god_low", "minutes": 720},
}

# =========================================
# 地图掉落池
# =========================================
ACTION_ITEM_POOLS = {
    # 水域掉落：偏灵草、果实、灵液感
    "fish": [
        3001, 3002, 3004, 3006,
        3037, 3038, 3040, 3042,
        3053, 3074, 3078
    ],

    # 矿脉掉落：偏根茎、矿质、硬质材料感
    "ore": [
        3005, 3007, 3013, 3016,
        3073, 3086, 3093, 3105,
        3106, 3026, 3034
    ],

    # 低阶采药
    "herb_low": [
        3001, 3002, 3003, 3004, 3005, 3006,
        3037, 3038, 3039, 3040, 3041, 3042,
        3073, 3074, 3075, 3076, 3077, 3078
    ],

    # 中阶采药
    "herb_mid": [
        3009, 3010, 3011, 3012, 3013, 3014, 3015, 3016,
        3045, 3046, 3047, 3048, 3049, 3050, 3051, 3052,
        3081, 3082, 3083, 3084, 3085, 3086, 3087, 3088
    ],

    # 灵石奖励
    "stone_low": ["LS_150000", "LS_180000", "LS_1100000"],
    "stone_mid": ["LS_1500000", "LS_2200000", "LS_3000000"],
    "stone_high": ["LS_5000000", "LS_8000000"],

    # 额外资源
    "wash_stone_low": [20023],
    "token_common": [20001, 20012, 20014],
    "token_rare": [20005, 20007, 20013, 20018],

    # 神物碎片/高阶资源池
    "god_frag": [15000, 15001, 15002, 15003, 15004],

    # 饰品礼包池
    "acc_pack_low": [
        18121, 18131, 18132, 18133, 18134,
        18135, 18136, 18159, 18160,
        18163, 18164
    ],
}

# =========================================
# 技能/装备掉落配置
# =========================================
SKILL_EQUIP_TYPES = ["功法", "神通", "辅修功法", "法器", "防具", "身法", "瞳术"]

MAP_EXTRA_DROP_RATE = {
    "gather": 0.06,
    "combat_trial": 0.12,
    "combat_risk": 0.18,
    "explore_normal": 0.08,
    "explore_rare": 0.22,
}

# =========================================
# 交互采集玩法
# =========================================
INTERACTIVE_ACTION_CONFIG = {
    "钓鱼": {
        "start_msg": "你抛下鱼钩，静候水波……",
        "wait_min": 10,
        "wait_max": 25,
        "trigger_msg": "🎣 鱼上钩了！请在20秒内发送【收杆】",
        "resolve_cmd": "收杆",
        "resolve_timeout": 20,
        "cooldown_sec": 25,
        "success_rate": 0.78,
    },
    "挖矿": {
        "start_msg": "你举镐探脉，细听地鸣……",
        "wait_min": 8,
        "wait_max": 20,
        "trigger_msg": "⛏️ 矿脉显形！请在20秒内发送【落镐】",
        "resolve_cmd": "落镐",
        "resolve_timeout": 20,
        "cooldown_sec": 25,
        "success_rate": 0.80,
    },
    "采集": {
        "start_msg": "你凝神寻药，分辨灵机……",
        "wait_min": 6,
        "wait_max": 18,
        "trigger_msg": "🌿 灵草现形！请在20秒内发送【采收】",
        "resolve_cmd": "采收",
        "resolve_timeout": 20,
        "cooldown_sec": 22,
        "success_rate": 0.84,
    },
}

NODE_ACTION_CONFIG = {
    "水域": {"cmd": "钓鱼", "cost": 4, "pool_key": "fish", "desc": "垂钓灵鱼"},
    "矿脉": {"cmd": "挖矿", "cost": 5, "pool_key": "ore", "desc": "开采灵矿"},
    "灵林": {"cmd": "采集", "cost": 3, "pool_key": "herb_low", "desc": "采集灵草"},
    "仙山": {"cmd": "采集", "cost": 4, "pool_key": "herb_mid", "desc": "探寻灵材"},
}

INTERACTIVE_ACTION_STATE = {}

# =========================================
# 战斗节点玩法
# =========================================
COMBAT_NODE_TYPES = {"试炼", "险地"}

COMBAT_CONFIG = {
    "试炼": {
        "stamina_cost": 8,
        "cooldown_sec": 30,
        "reward_plan_win": [
            ("stone_mid", 1, 2, 1.0),
            ("wash_stone_low", 1, 1, 0.35),
            ("token_common", 1, 1, 0.18),
        ],
        "reward_plan_big_win": [
            ("stone_high", 1, 1, 1.0),
            ("wash_stone_low", 1, 2, 0.65),
            ("token_common", 1, 1, 0.35),
            ("acc_pack_low", 1, 1, 0.15),
        ],
        "fail_msg": "试炼失利，你负伤而退。",
    },
    "险地": {
        "stamina_cost": 12,
        "cooldown_sec": 45,
        "reward_plan_win": [
            ("stone_high", 1, 2, 1.0),
            ("wash_stone_low", 1, 2, 0.55),
            ("token_common", 1, 1, 0.28),
            ("token_rare", 1, 1, 0.08),
        ],
        "reward_plan_big_win": [
            ("stone_high", 2, 2, 1.0),
            ("wash_stone_low", 2, 3, 0.75),
            ("token_common", 1, 2, 0.40),
            ("token_rare", 1, 1, 0.16),
            ("god_frag", 1, 1, 0.04),
            ("acc_pack_low", 1, 1, 0.18),
        ],
        "fail_msg": "险地凶险万分，你仓促脱身。",
    },
}

COMBAT_CD_STATE = {}

# =========================================
# 长时间探索：事件流
# =========================================
EXPLORE_NODE_TYPES = {"遗迹", "情报", "宫殿"}

EXPLORE_CONFIG = {
    "遗迹": {
        "stamina_cost": 6,
        "duration_min": 20,
        "max_duration_min": 120,
        "base_interval_min": 20,
        "event_weights": {
            "normal": 55,
            "good": 22,
            "battle": 13,
            "empty": 7,
            "rare": 3,
        }
    },
    "情报": {
        "stamina_cost": 5,
        "duration_min": 15,
        "max_duration_min": 90,
        "base_interval_min": 15,
        "event_weights": {
            "normal": 58,
            "good": 20,
            "battle": 8,
            "empty": 11,
            "rare": 3,
        }
    },
    "宫殿": {
        "stamina_cost": 10,
        "duration_min": 30,
        "max_duration_min": 180,
        "base_interval_min": 30,
        "event_weights": {
            "normal": 45,
            "good": 22,
            "battle": 18,
            "empty": 8,
            "rare": 7,
        }
    },
}

# =========================================
# 命令注册
# =========================================
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
fish_pull_cmd = on_command("收杆", priority=8, block=True)
mine_hit_cmd = on_command("落镐", priority=8, block=True)
gather_pick_cmd = on_command("采收", priority=8, block=True)

node_combat_cmd = on_command("节点战斗", aliases={"试炼挑战", "险地挑战"}, priority=8, block=True)

start_explore_cmd = on_command("开始探索", aliases={"节点探索"}, priority=8, block=True)
settle_explore_cmd = on_command("探索结算", priority=8, block=True)

map_mission_cmd = on_command("地图委托", priority=8, block=True)
map_mission_accept_cmd = on_command("接取委托", priority=8, block=True)
map_mission_claim_cmd = on_command("委托完成", priority=8, block=True)

# =========================================
# 限制/冷却工具
# =========================================
def _today_str():
    return datetime.now().strftime("%Y-%m-%d")


def _parse_dt(s: str | None):
    if not s:
        return None
    if isinstance(s, datetime):
        return s
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f"):
        try:
            return datetime.strptime(str(s), fmt)
        except Exception:
            continue
    return None


def _get_daily_limit(uid: str):
    d = player_data_manager.get_fields(uid, MAP_LIMIT_TABLE) or {}
    today = _today_str()
    if d.get("date") != today:
        d = {
            "date": today,
            "gather_count": 0,
            "combat_count": 0,
            "explore_count": 0,
            "resource_total_count": 0,
        }
        for k, v in d.items():
            player_data_manager.update_or_write_data(uid, MAP_LIMIT_TABLE, k, v)
    return d


def _save_daily_limit(uid: str, d: dict):
    for k, v in d.items():
        player_data_manager.update_or_write_data(uid, MAP_LIMIT_TABLE, k, v)


def _inc_daily_count(uid: str, key: str, n: int = 1):
    d = _get_daily_limit(uid)
    d[key] = int(d.get(key, 0)) + n
    if key in ("gather_count", "combat_count", "explore_count"):
        d["resource_total_count"] = int(d.get("resource_total_count", 0)) + n
    _save_daily_limit(uid, d)
    return d


def _check_daily_cap(uid: str, key: str, cap: int):
    d = _get_daily_limit(uid)
    cur = int(d.get(key, 0))
    return cur < cap, cur, cap


def _get_reward_decay(uid: str):
    d = _get_daily_limit(uid)
    n = int(d.get("resource_total_count", 0))
    for threshold, ratio in REWARD_DECAY_STEPS:
        if n <= threshold:
            return ratio
    return 0.3


def _get_cd(uid: str, cd_key: str):
    s = player_data_manager.get_field_data(uid, MAP_CD_TABLE, cd_key)
    return _parse_dt(s)


def _set_cd(uid: str, cd_key: str, seconds: int):
    t = datetime.now() + timedelta(seconds=seconds)
    player_data_manager.update_or_write_data(uid, MAP_CD_TABLE, cd_key, t.strftime("%Y-%m-%d %H:%M:%S"))
    return t


def _default_map_mission():
    return {
        "date": _today_str(),
        "mission_type": "",
        "target": 0,
        "claimed": 0,
    }


def _get_map_mission(uid: str):
    d = player_data_manager.get_fields(str(uid), MAP_MISSION_TABLE) or {}
    default = _default_map_mission()

    # 跨天重置
    if d.get("date") != _today_str():
        d = default.copy()
        for k, v in d.items():
            player_data_manager.update_or_write_data(str(uid), MAP_MISSION_TABLE, k, v)
        return d

    for k, v in default.items():
        if k not in d or d.get(k) is None:
            d[k] = v
            player_data_manager.update_or_write_data(str(uid), MAP_MISSION_TABLE, k, v)
    return d


def _save_map_mission(uid: str, d: dict):
    for k, v in d.items():
        player_data_manager.update_or_write_data(str(uid), MAP_MISSION_TABLE, k, v)


def _roll_new_map_mission(uid: str):
    mission_type = random.choice(list(MAP_MISSION_CONFIG.keys()))
    conf = MAP_MISSION_CONFIG[mission_type]
    target = random.choice(conf["targets"])

    d = {
        "date": _today_str(),
        "mission_type": mission_type,
        "target": target,
        "claimed": 0,
    }
    _save_map_mission(uid, d)
    return d


def _get_map_mission_progress(uid: str, mission_data: dict):
    mission_type = mission_data.get("mission_type")
    if not mission_type or mission_type not in MAP_MISSION_CONFIG:
        return 0

    daily = _get_daily_limit(uid)
    count_key = MAP_MISSION_CONFIG[mission_type]["count_key"]
    return int(daily.get(count_key, 0))


def _get_mission_desc(mission_data: dict):
    mission_type = mission_data.get("mission_type")
    target = int(mission_data.get("target", 0))
    if not mission_type or mission_type not in MAP_MISSION_CONFIG:
        return "暂无委托"
    return MAP_MISSION_CONFIG[mission_type]["desc"](target)


def _grant_map_mission_reward(uid: str, user_info: dict):
    rewards = []

    # 基础灵石奖励：从 stone_high 抽一个
    stone_pool = ACTION_ITEM_POOLS.get("stone_high", [])
    if stone_pool:
        stone_pick = random.choice(stone_pool)
        if isinstance(stone_pick, str) and stone_pick.startswith("LS_"):
            stone_num = int(stone_pick.split("_")[1])
            sql_message.update_ls(uid, stone_num, 1)
            rewards.append(f"灵石x{number_to(stone_num)}")

    # 额外奖励池：先随机选池
    extra_pool_key = random.choice(["acc_pack_low", "god_frag", "token_rare"])
    extra_pool = ACTION_ITEM_POOLS.get(extra_pool_key, [])
    if extra_pool:
        gid = random.choice(extra_pool)
        info = items.get_data_by_item_id(str(gid))
        if info:
            sql_message.send_back(uid, gid, info["name"], info.get("type", "材料"), 1, 1)
            rewards.append(f"{info['name']}x1")

    return rewards
# =========================================
# 地图工具
# =========================================
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
    heaven = random.choice(_heaven_names(map_data, realm))
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


def _save_map_status(uid: str, realm: str, heaven: str, node_id: str):
    player_data_manager.update_or_write_data(uid, MAP_TABLE, "realm", realm)
    player_data_manager.update_or_write_data(uid, MAP_TABLE, "heaven", heaven)
    player_data_manager.update_or_write_data(uid, MAP_TABLE, "node_id", node_id)

    visited = player_data_manager.get_field_data(uid, MAP_TABLE, "visited_nodes") or []
    if node_id not in visited:
        visited.append(node_id)
        player_data_manager.update_or_write_data(uid, MAP_TABLE, "visited_nodes", visited)


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


def get_player_current_node(user_id: str) -> dict | None:
    map_data = _load_map_data()
    status = _get_player_map_status(str(user_id), map_data)
    return _find_node_by_id(map_data, status["realm"], status["heaven"], status["node_id"])


def get_current_node_name(user_id: str) -> str | None:
    node = get_player_current_node(user_id)
    return node["name"] if node else None

# =========================================
# 奖励工具
# =========================================
def _grant_rewards(user_id: str, reward_plan, decay_ratio: float = 1.0):
    rewards = []
    for pool_key, cmin, cmax, chance in reward_plan:
        if random.random() > chance:
            continue
        pool_ids = ACTION_ITEM_POOLS.get(pool_key, [])
        if not pool_ids:
            continue
        cnt = random.randint(cmin, cmax)
        cnt = max(1, int(round(cnt * decay_ratio)))

        for _ in range(cnt):
            gid = random.choice(pool_ids)

            if isinstance(gid, str) and gid.startswith("LS_"):
                ls_num = int(gid.split("_")[1])
                ls_num = max(1, int(round(ls_num * decay_ratio)))
                sql_message.update_ls(user_id, ls_num, 1)
                rewards.append(f"灵石x{number_to(ls_num)}")
                continue

            info = items.get_data_by_item_id(str(gid))
            if not info:
                continue
            gname = info["name"]
            gtype = info.get("type", "材料")
            sql_message.send_back(user_id, gid, gname, gtype, 1, 1)
            rewards.append(f"{gname}x1")
    return rewards


def _grant_skill_equip_drop(user_info: dict, drop_rate: float = 0.1):
    """
    随机掉落技能/装备
    """
    if random.random() > drop_rate:
        return None

    user_id = str(user_info["user_id"])
    user_level = user_info.get("level", "江湖好手")

    item_type = random.choice(SKILL_EQUIP_TYPES)

    if item_type in ["法器", "防具", "辅修功法", "身法", "瞳术"]:
        zx_rank = base_rank(user_level, 16)
    else:
        zx_rank = base_rank(user_level, 5)

    item_id_list = items.get_random_id_list_by_rank_and_item_type(zx_rank, item_type)
    if not item_id_list:
        return None

    item_id = random.choice(item_id_list)
    item_info = items.get_data_by_item_id(item_id)
    if not item_info:
        return None

    sql_message.send_back(
        user_id,
        item_id,
        item_info["name"],
        item_info.get("type", item_type),
        1,
        1
    )

    return f"{item_info.get('level', '未知品级')}:{item_info['name']}x1"


def _merge_reward_text(rewards: list[str]) -> str:
    if not rewards:
        return "无"
    return "、".join(rewards[:80])

# =========================================
# 洞府
# =========================================
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
            await handle_send(bot, event, f"当前节点【{node['name']}】（{node_type}）不可建设洞府。")
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

# =========================================
# 地图基础
# =========================================
@map_help.handle(parameterless=[Cooldown(cd_time=1.4)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    bot, _ = await assign_bot(bot=bot, event=event)
    msg = (
        "🗺️【地图系统帮助】\n"
        "1. 地图 / 我的位置 / 前往 节点名\n"
        "2. 建设洞府 / 回府\n"
        "3. 附近道友 / 论道切磋 / 论道查看\n"
        "4. 资源玩法：钓鱼、挖矿、采集（含每日上限）\n"
        "5. 战斗玩法：节点战斗（试炼/险地，含每日上限）\n"
        "6. 长时玩法：开始探索 [分钟] / 探索结算（含每日上限）\n"
        "7. 商店玩法：种子商店 / 购买种子\n"
        "8. 洞府互动：我的洞府 / 洞府种植 / 洞府布阵 / 潜入洞府\n"
        "9. 地图委托：地图委托 / 接取委托 / 委托完成\n"
        "注：地图收益存在日内衰减机制"
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
                await handle_send(bot, event, f"未找到地点【{target_text}】")
                return

    if t_type == "realm":
        tar_realm = t_data
        if st["realm"] == tar_realm:
            await handle_send(bot, event, "你已在此界。")
            return
        if cur_node["type"] not in TRAVEL_NODE_TYPES:
            travel_nodes = [n["name"] for n in _nodes(map_data, st["realm"], st["heaven"]) if n["type"] in TRAVEL_NODE_TYPES]
            tip = f"跨界需通过【交通/渡口/驿站】节点。\n请先前往：{'、'.join(travel_nodes)}" if travel_nodes else "本天暂无交通节点，无法跨界。"
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
        await handle_send(bot, event, f"🚀 跨界成功！\n已抵达 {tar_realm}·{first_heaven}·{tar_node['name']}\n消耗体力：{cost}")
        return

    if t_type == "heaven":
        tar_realm, tar_heaven = t_data
        if st["realm"] != tar_realm:
            await handle_send(bot, event, f"请先前往【{tar_realm}】。")
            return
        if st["heaven"] == tar_heaven:
            await handle_send(bot, event, "你已在此天。")
            return
        if cur_node["type"] not in TRAVEL_NODE_TYPES:
            travel_nodes = [n["name"] for n in _nodes(map_data, st["realm"], st["heaven"]) if n["type"] in TRAVEL_NODE_TYPES]
            tip = f"跨天需通过【交通/渡口/驿站】节点。\n请先前往：{'、'.join(travel_nodes)}" if travel_nodes else "本天暂无交通节点，无法跨天。"
            await handle_send(bot, event, tip)
            return

        tar_node = _nodes(map_data, tar_realm, tar_heaven)[0]
        cost = int(cost_cfg.get("cross_heaven", 50))
        if stamina < cost:
            await handle_send(bot, event, f"跨天体力不足！需{cost}，当前{stamina}。")
            return

        _save_map_status(uid, tar_realm, tar_heaven, tar_node["id"])
        sql_message.update_user_stamina(uid, cost, 2)
        await handle_send(bot, event, f"☁️ 跨天成功！\n已抵达 {tar_realm}·{tar_heaven}·{tar_node['name']}\n消耗体力：{cost}")
        return

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
        await handle_send(bot, event, f"👣 移动成功！\n已前往【{tar_node['name']}】\n跨越 {steps} 个节点，消耗体力：{cost}")
        return


# =========================================
# 社交
# =========================================
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


# =========================================
# 种子商店
# =========================================
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


# =========================================
# 采集逻辑（已加每日次数+持久化CD）
# =========================================
async def _process_node_action(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, action_type: str):
    ok, user_info, m = check_user(event)
    if not ok:
        await handle_send(bot, event, m, md_type="我要修仙")
        return

    uid = str(user_info["user_id"])
    now = datetime.now()

    can_use, cur_cnt, cap = _check_daily_cap(uid, "gather_count", DAILY_LIMIT_CONFIG["gather"])
    if not can_use:
        await handle_send(bot, event, f"今日采集次数已达上限（{cap}次），请明日再来。")
        return

    gather_cd = _get_cd(uid, "gather_cd_until")
    if gather_cd and now < gather_cd:
        sec = int((gather_cd - now).total_seconds())
        await handle_send(bot, event, f"你刚忙完，先歇会儿吧（冷却剩余 {sec}s）")
        return

    st = INTERACTIVE_ACTION_STATE.get(uid)
    if st:
        if st.get("expire_ts") and now > st["expire_ts"]:
            INTERACTIVE_ACTION_STATE.pop(uid, None)
        else:
            await handle_send(bot, event, "你已有进行中的采集动作，请先完成。")
            return

    node = get_player_current_node(uid)
    if not node:
        await handle_send(bot, event, "当前位置节点数据异常。")
        return

    config = NODE_ACTION_CONFIG.get(node["type"])
    if not config or config["cmd"] != action_type:
        await handle_send(bot, event, f"当前节点【{node['name']}】(类型:{node['type']}) 无法进行【{action_type}】。")
        return

    ia = INTERACTIVE_ACTION_CONFIG[action_type]
    stamina = int(user_info.get("user_stamina", 0))
    if stamina < config["cost"]:
        await handle_send(bot, event, f"体力不足！{config['desc']}需消耗 {config['cost']} 体力，当前仅剩 {stamina}。")
        return

    sql_message.update_user_stamina(uid, config["cost"], 2)

    wait_sec = random.randint(ia["wait_min"], ia["wait_max"])
    ready_ts = now + timedelta(seconds=wait_sec)
    expire_ts = ready_ts + timedelta(seconds=ia["resolve_timeout"])

    INTERACTIVE_ACTION_STATE[uid] = {
        "action": action_type,
        "node_name": node["name"],
        "node_type": node["type"],
        "pool_key": config["pool_key"],
        "ready": False,
        "start_ts": now,
        "ready_ts": ready_ts,
        "expire_ts": expire_ts,
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
            _set_cd(uid, "gather_cd_until", ia["cooldown_sec"])
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
        _set_cd(uid, "gather_cd_until", ia["cooldown_sec"])
        await handle_send(bot, event, f"❌ 时机已过，{action}失败。")
        return

    if random.random() > ia["success_rate"]:
        INTERACTIVE_ACTION_STATE.pop(uid, None)
        _set_cd(uid, "gather_cd_until", ia["cooldown_sec"])
        await handle_send(bot, event, f"💨 你动作慢了半拍，{action}失败（目标跑了）")
        return

    _inc_daily_count(uid, "gather_count", 1)

    decay = _get_reward_decay(uid)

    roll = random.random()
    rewards = []
    extra_msg = ""

    if roll < 0.10:
        rewards = _grant_rewards(uid, [("stone_low", 1, 1, 1.0)], decay_ratio=decay)
        extra_msg = "你惊动了附近的异兽，只来得及捡走些散落资源。"
    elif roll < 0.30:
        rewards = _grant_rewards(uid, [
            (st["pool_key"], 2, 3, 1.0),
            ("stone_low", 1, 2, 1.0),
            ("wash_stone_low", 1, 1, 0.15),
        ], decay_ratio=decay)
        extra_msg = "运气极佳，收获颇丰！"
    else:
        rewards = _grant_rewards(uid, [
            (st["pool_key"], 1, 2, 1.0),
            ("stone_low", 1, 1, 0.55),
        ], decay_ratio=decay)

    extra = _grant_skill_equip_drop(user_info, MAP_EXTRA_DROP_RATE["gather"])
    if extra:
        rewards.append(extra)

    INTERACTIVE_ACTION_STATE.pop(uid, None)
    _set_cd(uid, "gather_cd_until", ia["cooldown_sec"])

    decay_tip = f"\n当前收益系数：{int(decay * 100)}%" if decay < 1 else ""
    if rewards:
        tip = f"\n{extra_msg}" if extra_msg else ""
        await handle_send(bot, event, f"✅ {action}成功！\n地点：{st['node_name']}\n获得：{_merge_reward_text(rewards)}{tip}{decay_tip}")
    else:
        await handle_send(bot, event, f"✅ {action}完成，但这次没有收获。{decay_tip}")


# =========================================
# 战斗节点：真实战斗（已加每日次数+持久化CD）
# =========================================
def _build_map_enemy(user_info: dict, node_type: str, node_name: str):
    user_exp = int(user_info.get("exp", 1000))
    user_power = int(user_info.get("power", 1000))

    if node_type == "试炼":
        factor = random.uniform(0.75, 1.00)
        names = ["守关石傀", "幻影剑修", "试炼战灵", "铜甲傀儡"]
    else:
        factor = random.uniform(0.95, 1.20)
        names = ["瘴骨妖", "赤瞳凶魇", "裂甲魔猿", "险地邪修"]

    exp_base = max(500, int(user_exp * factor))
    atk = max(200, int(exp_base / 10))
    hp = max(1000, int(exp_base / 2))
    mp = max(500, int(exp_base))
    user_level = user_info.get("level", "江湖好手")
    if len(user_level) == 5:
        level = user_level[:3]
    elif len(user_level) == 4:
        level = "感气境"
    elif len(user_level) == 2:
        level = "永恒境"
    else:
        level = "感气境"

    return {
        "name": f"{random.choice(names)}·{node_name}",
        "jj": level,
        "气血": hp,
        "总血量": hp,
        "真元": mp,
        "攻击": atk,
        "stone": max(10000, int(user_power * 0.01)),
    }


async def _process_node_combat(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    ok, user_info, m = check_user(event)
    if not ok:
        await handle_send(bot, event, m, md_type="我要修仙")
        return

    uid = str(user_info["user_id"])

    can_use, cur_cnt, cap = _check_daily_cap(uid, "combat_count", DAILY_LIMIT_CONFIG["combat"])
    if not can_use:
        await handle_send(bot, event, f"今日节点战斗次数已达上限（{cap}次），请明日再来。")
        return

    node = get_player_current_node(uid)
    if not node:
        await handle_send(bot, event, "当前位置异常，请先前往有效节点。")
        return

    ntype = node["type"]
    if ntype not in COMBAT_NODE_TYPES:
        await handle_send(bot, event, f"当前节点【{node['name']}】不是对战节点。")
        return

    now = datetime.now()
    cd = _get_cd(uid, "combat_cd_until")
    if cd and now < cd:
        sec = int((cd - now).total_seconds())
        await handle_send(bot, event, f"你刚经历一战，尚需冷却 {sec}s")
        return

    conf = COMBAT_CONFIG[ntype]
    stamina = int(user_info.get("user_stamina", 0))
    if stamina < conf["stamina_cost"]:
        await handle_send(bot, event, f"体力不足！需要 {conf['stamina_cost']}，当前 {stamina}")
        return

    sql_message.update_user_stamina(uid, conf["stamina_cost"], 2)
    _set_cd(uid, "combat_cd_until", conf["cooldown_sec"])

    enemy = _build_map_enemy(user_info, ntype, node["name"])
    result, victor, bossinfo_new = await Boss_fight(uid, enemy, bot_id=bot.self_id)
    await send_msg_handler(bot, event, result)

    _inc_daily_count(uid, "combat_count", 1)

    if victor != "群友赢了":
        await handle_send(bot, event, f"⚔️ {conf['fail_msg']}\n地点：{node['name']}")
        return

    big_win = False
    remain_hp = None
    try:
        remain_hp = bossinfo_new.get("群友", {}).get("剩余气血")
    except Exception:
        remain_hp = None

    if remain_hp is not None:
        try:
            big_win = int(remain_hp) > int(user_info.get("hp", 1)) * 0.5
        except Exception:
            big_win = False
    else:
        big_win = random.random() < 0.25

    decay = _get_reward_decay(uid)
    plan = conf["reward_plan_big_win"] if big_win else conf["reward_plan_win"]
    rewards = _grant_rewards(uid, plan, decay_ratio=decay)

    drop_rate = MAP_EXTRA_DROP_RATE["combat_trial"] if ntype == "试炼" else MAP_EXTRA_DROP_RATE["combat_risk"]
    extra = _grant_skill_equip_drop(user_info, drop_rate)
    if extra:
        rewards.append(extra)

    title = "大胜而归" if big_win else "战而胜之"
    decay_tip = f"\n当前收益系数：{int(decay * 100)}%" if decay < 1 else ""
    await handle_send(bot, event, f"⚔️ 你在【{node['name']}】{title}！\n战利品：{_merge_reward_text(rewards)}{decay_tip}")


# =========================================
# 探索状态
# =========================================
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
        }
        for k, v in d.items():
            player_data_manager.update_or_write_data(str(uid), EXPLORE_TABLE, k, v)
    return d


def _save_explore_status(uid: str, d: dict):
    for k, v in d.items():
        player_data_manager.update_or_write_data(str(uid), EXPLORE_TABLE, k, v)


# =========================================
# 探索事件流
# =========================================
def _pick_explore_event(ntype: str):
    conf = EXPLORE_CONFIG[ntype]
    ew = conf["event_weights"]
    keys = list(ew.keys())
    vals = list(ew.values())
    return random.choices(keys, weights=vals, k=1)[0]


def _resolve_explore_event(uid: str, user_info: dict, node_type: str, node_name: str):
    event_type = _pick_explore_event(node_type)

    if event_type == "empty":
        text_pool = [
            f"你在【{node_name}】搜索许久，却只看到风过残痕。",
            f"你循迹探查【{node_name}】，最终一无所获。",
            f"这一次在【{node_name}】的探索，未能找到有价值的线索。"
        ]
        return random.choice(text_pool), []

    if event_type == "normal":
        rewards = _grant_rewards(uid, [
            ("stone_low", 1, 2, 1.0),
            ("herb_low", 1, 1, 0.55),
            ("wash_stone_low", 1, 1, 0.12),
        ], decay_ratio=_get_reward_decay(uid))
        extra = _grant_skill_equip_drop(user_info, MAP_EXTRA_DROP_RATE["explore_normal"])
        if extra:
            rewards.append(extra)
        return f"你在【{node_name}】有所收获。", rewards

    if event_type == "good":
        rewards = _grant_rewards(uid, [
            ("stone_mid", 1, 2, 1.0),
            ("herb_mid", 1, 2, 0.65),
            ("token_common", 1, 1, 0.18),
            ("wash_stone_low", 1, 1, 0.25),
        ], decay_ratio=_get_reward_decay(uid))
        extra = _grant_skill_equip_drop(user_info, MAP_EXTRA_DROP_RATE["explore_normal"])
        if extra:
            rewards.append(extra)
        return f"你在【{node_name}】发现了一处隐秘资源点。", rewards

    if event_type == "rare":
        rewards = _grant_rewards(uid, [
            ("stone_high", 1, 1, 1.0),
            ("token_rare", 1, 1, 0.20),
            ("acc_pack_low", 1, 1, 0.14),
            ("god_frag", 1, 1, 0.02),
        ], decay_ratio=_get_reward_decay(uid))
        extra = _grant_skill_equip_drop(user_info, MAP_EXTRA_DROP_RATE["explore_rare"])
        if extra:
            rewards.append(extra)
        return f"你在【{node_name}】触发了一场罕见机缘！", rewards

    rewards = _grant_rewards(uid, [
        ("stone_mid", 1, 2, 1.0),
        ("wash_stone_low", 1, 2, 0.45),
        ("token_common", 1, 1, 0.25),
    ], decay_ratio=_get_reward_decay(uid))
    extra = _grant_skill_equip_drop(user_info, MAP_EXTRA_DROP_RATE["explore_normal"])
    if extra:
        rewards.append(extra)
    return f"你在【{node_name}】遭遇阻击，鏖战后夺得战利品。", rewards


async def _start_explore(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, duration_arg: int | None):
    ok, user_info, m = check_user(event)
    if not ok:
        await handle_send(bot, event, m, md_type="我要修仙")
        return

    uid = str(user_info["user_id"])

    can_use, cur_cnt, cap = _check_daily_cap(uid, "explore_count", DAILY_LIMIT_CONFIG["explore"])
    if not can_use:
        await handle_send(bot, event, f"今日探索发起次数已达上限（{cap}次），请明日再来。")
        return

    now = datetime.now()
    cd = _get_cd(uid, "explore_start_cd_until")
    if cd and now < cd:
        sec = int((cd - now).total_seconds())
        await handle_send(bot, event, f"你刚开始过一次探索，请稍候（{sec}s）")
        return

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
        await handle_send(bot, event, "你已有进行中的探索，请先【探索结算】。")
        return

    conf = EXPLORE_CONFIG[ntype]
    need_stamina = conf["stamina_cost"]
    stamina = int(user_info.get("user_stamina", 0))
    if stamina < need_stamina:
        await handle_send(bot, event, f"体力不足！发起探索需 {need_stamina}，当前 {stamina}")
        return

    if duration_arg is None:
        duration_min = conf["duration_min"]
    else:
        duration_min = max(5, duration_arg)

    sql_message.update_user_stamina(uid, need_stamina, 2)
    _inc_daily_count(uid, "explore_count", 1)
    _set_cd(uid, "explore_start_cd_until", 60)

    new_st = {
        "running": 1,
        "node_type": ntype,
        "node_name": node["name"],
        "start_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "duration_min": duration_min,
        "max_duration_min": conf["max_duration_min"],
        "interval_min": conf["base_interval_min"],
    }
    _save_explore_status(uid, new_st)

    await handle_send(
        bot, event,
        f"🧭 已开始【{node['name']}】探索\n"
        f"计划时长：{duration_min}分钟（上限按{conf['max_duration_min']}分钟结算）\n"
        f"探索期间将以事件流形式结算，稍后发送【探索结算】领取收益。"
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
    try:
        dt = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
    except Exception:
        st["running"] = 0
        _save_explore_status(uid, st)
        await handle_send(bot, event, "探索状态异常，已重置。")
        return

    now = datetime.now()
    elapsed_min = max(0, int((now - dt).total_seconds() // 60))
    duration_min = int(st.get("duration_min", 0))
    max_duration = int(st.get("max_duration_min", 0))
    interval = max(1, int(st.get("interval_min", 20)))

    settle_min = min(elapsed_min, duration_min if duration_min > 0 else elapsed_min, max_duration if max_duration > 0 else elapsed_min)
    if settle_min <= 0:
        await handle_send(bot, event, "探索时间太短，暂时无可结算收益。")
        return

    rounds = max(1, settle_min // interval)
    node_type = st.get("node_type", "")
    node_name = st.get("node_name", "未知地点")

    event_lines = []
    all_rewards = []

    for i in range(rounds):
        line, rewards = _resolve_explore_event(uid, user_info, node_type, node_name)
        event_lines.append(f"{i + 1}. {line}")
        all_rewards.extend(rewards)

    clear_st = {
        "running": 0,
        "node_type": "",
        "node_name": "",
        "start_time": "",
        "duration_min": 0,
        "max_duration_min": 0,
        "interval_min": 0,
    }
    _save_explore_status(uid, clear_st)

    decay = _get_reward_decay(uid)
    decay_tip = f"\n当前收益系数：{int(decay * 100)}%" if decay < 1 else ""

    msg = (
        f"🧭 探索结算完成\n"
        f"地点：{node_name}\n"
        f"有效探索：{settle_min}分钟（共{rounds}轮）\n"
        f"—— 事件记录 ——\n" + "\n".join(event_lines[:20]) +
        f"\n—— 总收益 ——\n{_merge_reward_text(all_rewards)}{decay_tip}"
    )
    await handle_send(bot, event, msg)


# =========================================
# 命令入口
# =========================================
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


@node_combat_cmd.handle(parameterless=[Cooldown(cd_time=1.2)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    bot, _ = await assign_bot(bot=bot, event=event)
    await _process_node_combat(bot, event)


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

@map_mission_cmd.handle(parameterless=[Cooldown(cd_time=1.2)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    bot, _ = await assign_bot(bot=bot, event=event)
    ok, user_info, m = check_user(event)
    if not ok:
        await handle_send(bot, event, m, md_type="我要修仙")
        return

    uid = str(user_info["user_id"])
    mission = _get_map_mission(uid)

    if not mission.get("mission_type"):
        await handle_send(
            bot, event,
            "今日尚未接取地图委托。\n发送【接取委托】获取今日任务。"
        )
        return

    desc = _get_mission_desc(mission)
    progress = _get_map_mission_progress(uid, mission)
    target = int(mission.get("target", 0))
    claimed = int(mission.get("claimed", 0))

    status = "已领取" if claimed else ("可完成" if progress >= target else "进行中")

    msg = (
        f"【地图委托】\n"
        f"任务：{desc}\n"
        f"进度：{progress}/{target}\n"
        f"状态：{status}\n"
        f"奖励：高额灵石 + 额外随机宝物"
    )
    await handle_send(bot, event, msg)

@map_mission_accept_cmd.handle(parameterless=[Cooldown(cd_time=1.2)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    bot, _ = await assign_bot(bot=bot, event=event)
    ok, user_info, m = check_user(event)
    if not ok:
        await handle_send(bot, event, m, md_type="我要修仙")
        return

    uid = str(user_info["user_id"])
    mission = _get_map_mission(uid)

    if not mission.get("mission_type"):
        mission = _roll_new_map_mission(uid)

    desc = _get_mission_desc(mission)
    progress = _get_map_mission_progress(uid, mission)
    target = int(mission.get("target", 0))

    msg = (
        f"已接取今日地图委托！\n"
        f"任务：{desc}\n"
        f"当前进度：{progress}/{target}\n"
        f"完成后发送【委托完成】领取奖励。"
    )
    await handle_send(bot, event, msg)

@map_mission_claim_cmd.handle(parameterless=[Cooldown(cd_time=1.2)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    bot, _ = await assign_bot(bot=bot, event=event)
    ok, user_info, m = check_user(event)
    if not ok:
        await handle_send(bot, event, m, md_type="我要修仙")
        return

    uid = str(user_info["user_id"])
    mission = _get_map_mission(uid)

    if not mission.get("mission_type"):
        await handle_send(bot, event, "你今日尚未接取地图委托，请先发送【接取委托】。")
        return

    if int(mission.get("claimed", 0)) == 1:
        await handle_send(bot, event, "今日地图委托奖励已领取。")
        return

    progress = _get_map_mission_progress(uid, mission)
    target = int(mission.get("target", 0))

    if progress < target:
        await handle_send(
            bot, event,
            f"委托尚未完成。\n当前进度：{progress}/{target}"
        )
        return

    rewards = _grant_map_mission_reward(uid, user_info)
    mission["claimed"] = 1
    _save_map_mission(uid, mission)

    msg = (
        f"✅ 地图委托完成！\n"
        f"任务：{_get_mission_desc(mission)}\n"
        f"获得奖励：{_merge_reward_text(rewards)}"
    )
    await handle_send(bot, event, msg)
