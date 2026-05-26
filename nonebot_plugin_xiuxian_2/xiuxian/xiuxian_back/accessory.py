import random
import time
from urllib.parse import quote

from ..on_compat import on_command
from nonebot.params import CommandArg

from ..adapter_compat import (
    Bot,
    Message,
    GroupMessageEvent,
    PrivateMessageEvent,
    MessageSegment,
)
from ..xiuxian_config import XiuConfig
from ..xiuxian_utils.item_json import Items
from ..xiuxian_utils.utils import check_user, handle_send, send_msg_handler, send_help_message
from ..xiuxian_utils.xiuxian2_handle import PlayerDataManager, XiuxianDateManage, calc_accessory_effects
from ..xiuxian_utils.lay_out import Cooldown

items = Items()
sql_message = XiuxianDateManage()
player_data_manager = PlayerDataManager()

my_accessory = on_command("我的饰品", priority=10, block=True)
accessory_bag = on_command("饰品背包", priority=10, block=True)
equip_accessory = on_command("装备饰品", priority=10, block=True)
unequip_accessory = on_command("卸下饰品", priority=10, block=True)
wash_accessory = on_command("饰品洗练", priority=10, block=True)
decompose_accessory = on_command("饰品分解", priority=10, block=True)
quick_decompose_accessory = on_command("快速分解饰品", aliases={"饰品快速分解"}, priority=10, block=True)
accessory_help = on_command("饰品帮助", aliases={"饰品系统帮助"}, priority=10, block=True)
accessory_basic_help = on_command("饰品基础帮助", aliases={"饰品查看帮助", "饰品装备帮助"}, priority=10, block=True)
accessory_growth_help = on_command("饰品成长帮助", aliases={"饰品洗练帮助", "饰品升阶帮助"}, priority=10, block=True)
accessory_manage_help = on_command("饰品整理帮助", aliases={"饰品分解帮助", "饰品预设帮助", "饰品管理帮助"}, priority=10, block=True)
check_accessory = on_command("查看饰品", priority=10, block=True)
upgrade_accessory = on_command("饰品升阶", aliases={"升阶饰品"}, priority=10, block=True)
accessory_preset = on_command("饰品预设", aliases={"预设饰品"}, priority=10, block=True)
quick_equip_accessory = on_command("快速装备饰品", priority=10, block=True)


def _paginate_sections(sections, page: int, per_page: int = 15):
    total_items = sum(len(rows) for _, rows in sections)
    total_pages = max(1, (total_items + per_page - 1) // per_page)
    page = max(1, min(page, total_pages))

    start = (page - 1) * per_page
    end = start + per_page

    out = []
    cursor = 0
    for title, rows in sections:
        next_cursor = cursor + len(rows)
        if next_cursor <= start:
            cursor = next_cursor
            continue
        if cursor >= end:
            break

        local_start = max(0, start - cursor)
        local_end = min(len(rows), end - cursor)
        part = rows[local_start:local_end]
        if part:
            out.append((title, part))
        cursor = next_cursor

    return out, page, total_pages


TABLE = "player_accessory"

AFFIX_TYPES = ["气血", "抗暴", "防御", "会心", "会心伤害", "攻击"]

# 品阶1-5洗练区间
WASH_RANGE = {
    1: {"气血": (0.02, 0.05), "抗暴": (0.01, 0.03), "防御": (0.01, 0.03), "会心": (0.01, 0.03), "会心伤害": (0.02, 0.05), "攻击": (0.02, 0.05)},
    2: {"气血": (0.04, 0.08), "抗暴": (0.02, 0.05), "防御": (0.02, 0.05), "会心": (0.02, 0.05), "会心伤害": (0.04, 0.08), "攻击": (0.04, 0.08)},
    3: {"气血": (0.06, 0.12), "抗暴": (0.03, 0.07), "防御": (0.03, 0.07), "会心": (0.03, 0.07), "会心伤害": (0.06, 0.12), "攻击": (0.06, 0.12)},
    4: {"气血": (0.08, 0.16), "抗暴": (0.04, 0.10), "防御": (0.04, 0.10), "会心": (0.04, 0.10), "会心伤害": (0.08, 0.16), "攻击": (0.08, 0.16)},
    5: {"气血": (0.10, 0.20), "抗暴": (0.05, 0.12), "防御": (0.05, 0.12), "会心": (0.05, 0.12), "会心伤害": (0.10, 0.20), "攻击": (0.10, 0.20)},
}

SLOTS = ["手镯", "戒指", "手环", "项链"]

# 你的饰品词条中文 -> 统一属性键
AFFIX_KEY_MAP = {
    "气血": "hp_pct",              # 最大生命百分比
    "抗暴": "crit_resist",         # 抗暴
    "防御": "dmg_reduction",       # 伤害减免
    "会心": "crit_rate",           # 会心率
    "会心伤害": "crit_damage",     # 会心伤害
    "攻击": "atk_pct",             # 攻击百分比
}

# 套装效果（2件 / 4件）
SET_BONUS = {
    "烈阳": {
        2: {"type": "attack", "value": 0.08},
        4: {"type": "true_damage", "value": 0.06},
    },
    "玄渊": {
        2: {"type": "shield", "value": 0.12},
        4: {"type": "reflect", "value": 0.12},
    },
    "天衡": {
        2: {"type": "armor_pen", "value": 0.08},
        4: {"type": "dmg_reduction", "value": 0.10},
    },
    "星痕": {
        2: {"type": "crit_rate", "value": 0.06},
        4: {"type": "dodge", "value": 12},
    },
    "龙魄": {
        2: {"type": "attack", "value": 0.06},
        4: {"type": "shield_break", "value": 0.10},
    },
}

def quality_to_cn(q: int) -> str:
    return {
        1: "一阶",
        2: "二阶",
        3: "三阶",
        4: "四阶",
        5: "五阶",
    }.get(int(q), f"{q}阶")

SET_TYPE_CN = {
    "attack": "攻击提升",
    "true_damage": "附加真伤",
    "shield": "开场护盾",
    "reflect": "反伤",
    "armor_pen": "护甲穿透",
    "dmg_reduction": "伤害减免",
    "crit_rate": "会心率",
    "dodge": "闪避",
    "shield_break": "护盾穿透",
}

SET_VALUE_POINT_TYPES = {"dodge"}

ACCESSORY_SETS = ["烈阳", "玄渊", "天衡", "星痕", "龙魄"]
ACCESSORY_PARTS = ["手镯", "戒指", "手环", "项链"]
QUALITY_RANGE = [1, 2, 3, 4, 5]

WASH_STONE_ID = 20023
WASH_STONE_NAME = "洗练石"

WASH_STONE_COST = {
    1: 1,
    2: 2,
    3: 4,
    4: 8,
    5: 12
}

ACCESSORY_DECOMPOSE_GAIN = {
    1: 1,
    2: 3,
    3: 8,
    4: 20,
    5: 50
}


def _default_accessory_doc():
    return {
        "equipped": {"手镯": None, "戒指": None, "手环": None, "项链": None},
        "bag": []
    }

def _normalize_accessory_doc(doc: dict):
    if not isinstance(doc, dict):
        doc = _default_accessory_doc()

    eq = doc.get("equipped")
    if not isinstance(eq, dict):
        eq = {"手镯": None, "戒指": None, "手环": None, "项链": None}
    for s in SLOTS:
        if s not in eq:
            eq[s] = None

    bag = doc.get("bag")
    if not isinstance(bag, list):
        bag = []

    doc["equipped"] = eq
    doc["bag"] = bag
    return doc

def _get_data(user_id: str):
    doc = player_data_manager.get_doc(
        user_id=user_id,
        table_name=TABLE,
        fields=["equipped", "bag"],
        default_factory=_default_accessory_doc
    )
    return _normalize_accessory_doc(doc)

def _save_data(user_id: str, data: dict):
    data = _normalize_accessory_doc(data)
    player_data_manager.save_doc(
        user_id=user_id,
        table_name=TABLE,
        data=data,
        fields=["equipped", "bag"],
        dirty_check=True
    )

def roll_affixes(quality: int, count: int = 2):
    count = max(1, min(4, count))
    pool = random.sample(AFFIX_TYPES, count)
    out = []
    for t in pool:
        lo, hi = WASH_RANGE[quality][t]
        out.append({"type": t, "value": round(random.uniform(lo, hi), 4)})
    return out

def roll_affixes_with_pity(quality: int, count: int = 2, pity_reached: bool = False):
    count = max(1, min(4, count))
    pool = random.sample(AFFIX_TYPES, count)
    out = []
    for t in pool:
        lo, hi = WASH_RANGE[quality][t]
        v = hi if pity_reached else round(random.uniform(lo, hi), 4)
        out.append({"type": t, "value": v})
    return out

def create_accessory_instance(item_id: int, quality: int = 1):
    item = items.get_data_by_item_id(item_id)
    uid = f"acc_{int(time.time())}_{random.randint(1,9999)}"
    return {
        "uid": uid,
        "item_id": item_id,
        "name": item["name"],
        "part": item["part"],
        "set_type": item["set_type"],
        "quality": quality,
        "affixes": roll_affixes(quality, 2),
        "wash_count": 0
    }

def add_accessory_to_bag(user_id: str, item_id: int, quality: int = 1):
    data = _get_data(user_id)
    ins = create_accessory_instance(item_id, quality)
    data["bag"].append(ins)
    _save_data(user_id, data)
    return ins

def _find_accessory_in_bag(data: dict, uid: str):
    bag = data.get("bag", [])
    for i, x in enumerate(bag):
        if str(x.get("uid", "")) == str(uid):
            return i, x
    return -1, None

def _parse_quality_arg(q_text: str):
    q_text = str(q_text).strip()
    mapping = {
        "1": 1, "2": 2, "3": 3, "4": 4, "5": 5,
        "一阶": 1, "二阶": 2, "三阶": 3, "四阶": 4, "五阶": 5,
        "q1": 1, "q2": 2, "q3": 3, "q4": 4, "q5": 5,
        "Q1": 1, "Q2": 2, "Q3": 3, "Q4": 4, "Q5": 5,
    }
    return mapping.get(q_text, None)

def _match_accessory_type(acc: dict, t: str):
    t = str(t).strip()
    if t == "全部":
        return True
    if t in ["烈阳", "玄渊", "天衡", "星痕", "龙魄"]:
        return acc.get("set_type") == t
    if t in ["手镯", "戒指", "手环", "项链"]:
        return acc.get("part") == t
    return False

def _find_accessory_anywhere(data: dict, uid: str):
    for i, x in enumerate(data.get("bag", [])):
        if str(x.get("uid", "")) == str(uid):
            return "bag", i, x
    for s in SLOTS:
        it = data.get("equipped", {}).get(s)
        if it and str(it.get("uid", "")) == str(uid):
            return "equipped", s, it
    return None, None, None

def _default_accessory_preset():
    return {"手镯": None, "戒指": None, "手环": None, "项链": None}

def _get_accessory_preset(user_id: str, preset_idx: int):
    field = f"preset_{preset_idx}"
    raw = player_data_manager.get_field_data(str(user_id), TABLE, field)

    if not isinstance(raw, dict):
        raw = _default_accessory_preset()

    for s in SLOTS:
        if s not in raw:
            raw[s] = None

    return raw

def _save_accessory_preset(user_id: str, preset_idx: int, preset_data: dict):
    field = f"preset_{preset_idx}"
    normalized = _default_accessory_preset()
    if isinstance(preset_data, dict):
        for s in SLOTS:
            normalized[s] = preset_data.get(s)
    player_data_manager.update_or_write_data(str(user_id), TABLE, field, normalized, data_type="TEXT")

def _accessory_uid_exists(data: dict, uid: str):
    if not uid:
        return False

    # 查背包
    for x in data.get("bag", []):
        if str(x.get("uid", "")) == str(uid):
            return True

    # 查已装备
    for s in SLOTS:
        it = data.get("equipped", {}).get(s)
        if it and str(it.get("uid", "")) == str(uid):
            return True

    return False

def _clean_accessory_preset(user_id: str, preset_idx: int):
    data = _get_data(str(user_id))
    preset = _get_accessory_preset(str(user_id), preset_idx)

    changed = False
    result = _default_accessory_preset()

    for s in SLOTS:
        uid = preset.get(s)
        if uid and _accessory_uid_exists(data, uid):
            result[s] = uid
        else:
            if uid is not None:
                changed = True
            result[s] = None

    if changed:
        _save_accessory_preset(str(user_id), preset_idx, result)

    return result

def _get_accessory_by_uid(data: dict, uid: str):
    if not uid:
        return None

    for x in data.get("bag", []):
        if str(x.get("uid", "")) == str(uid):
            return x

    for s in SLOTS:
        it = data.get("equipped", {}).get(s)
        if it and str(it.get("uid", "")) == str(uid):
            return it

    return None

def _format_accessory_preset(user_id: str, preset_idx: int):
    data = _get_data(str(user_id))
    preset = _clean_accessory_preset(str(user_id), preset_idx)

    lines = [f"【预设{preset_idx}】"]
    empty = True

    for s in SLOTS:
        uid = preset.get(s)
        if not uid:
            lines.append(f"{s}：未记录")
            continue

        acc = _get_accessory_by_uid(data, uid)
        if not acc:
            lines.append(f"{s}：未记录")
            continue

        empty = False
        lines.append(
            f"{s}：{acc.get('name', '未知饰品')}[{quality_to_cn(acc.get('quality', 1))}] "
            f"({acc.get('set_type', '未知')}·UID:{uid})"
        )

    if empty:
        lines.append("（当前预设为空）")

    return "\n".join(lines)

def _get_upgrade_cost(cur_quality: int) -> int:
    if cur_quality <= 1:
        return 1
    return cur_quality - 1

def _is_same_accessory_for_upgrade(main_acc: dict, material_acc: dict) -> bool:
    if not main_acc or not material_acc:
        return False
    return (
        int(main_acc.get("item_id", 0)) == int(material_acc.get("item_id", -1))
        and str(main_acc.get("part", "")) == str(material_acc.get("part", ""))
        and str(main_acc.get("set_type", "")) == str(material_acc.get("set_type", ""))
        and int(main_acc.get("quality", 1)) == int(material_acc.get("quality", 0))
    )

def _build_accessory_sections_for_md(user_id: str):
    data = _get_data(str(user_id))
    if not data:
        return []

    bag = data.get("bag", [])
    equipped = data.get("equipped", {})

    set_order = ["烈阳", "玄渊", "天衡", "星痕", "龙魄", "其他"]
    buckets = {k: [] for k in set_order}

    equipped_rows = []
    for s in SLOTS:
        it = equipped.get(s)
        if not it:
            continue
        row = {
            "name": it.get("name", "未知饰品"),
            "count": 1,
            "bind": 0,
            "goods_type": "饰品",
            "uid": it.get("uid", ""),
            "quality": int(it.get("quality", 1)),
            "part": it.get("part", s),
            "set_type": it.get("set_type", "其他"),
            "is_equipped": True
        }
        equipped_rows.append(row)

    bag_rows = []
    for x in bag:
        row = {
            "name": x.get("name", "未知饰品"),
            "count": 1,
            "bind": 0,
            "goods_type": "饰品",
            "uid": x.get("uid", ""),
            "quality": int(x.get("quality", 1)),
            "part": x.get("part", ""),
            "set_type": x.get("set_type", "其他"),
            "is_equipped": False
        }
        bag_rows.append(row)

    all_rows = equipped_rows + bag_rows

    for row in all_rows:
        st = row.get("set_type", "其他")
        if st not in buckets:
            st = "其他"
        buckets[st].append(row)

    sections = []
    for st in set_order:
        rows = buckets.get(st, [])
        if not rows:
            continue

        rows = sorted(
            rows,
            key=lambda r: (
                0 if r.get("is_equipped") else 1,
                -r.get("quality", 1),
                r.get("part", ""),
                r.get("name", "")
            )
        )
        sections.append((f"{st}套装", rows))

    return sections

def _build_accessory_md_text(
    title: str,
    sections: list[tuple[str, list[dict]]],
    current_page: int,
    total_pages: int,
    next_cmd: str = ""
) -> str:
    lines = [f"☆------{title}------☆", ""]

    for sec_title, rows in sections:
        if not rows:
            continue

        lines.append(f"【{sec_title}】")
        lines.append("")

        for row in rows:
            name = row.get("name", "未知饰品")
            uid = row.get("uid", "")
            q = int(row.get("quality", 1))
            part = row.get("part", "")
            set_type = row.get("set_type", "未知")

            view_cmd = quote(f"查看饰品 {uid}", safe="")
            view_md = f"[{name}](mqqapi://aio/inlinecmd?command={view_cmd}&enter=false&reply=false)"

            equip_cmd = quote(f"装备饰品 {uid}", safe="")
            wash_cmd = quote(f"饰品洗练 {uid}", safe="")
            decompose_cmd = quote(f"饰品分解 {uid}", safe="")
            op_md = (
                f"[装备](mqqapi://aio/inlinecmd?command={equip_cmd}&enter=false&reply=false) "
                f"[洗练](mqqapi://aio/inlinecmd?command={wash_cmd}&enter=false&reply=false) "
                f"[分解](mqqapi://aio/inlinecmd?command={decompose_cmd}&enter=false&reply=false)"
            )

            eq_flag = "【已装备】" if row.get("is_equipped") else ""
            lines.append(
                f"> - {eq_flag}{view_md} | {part} | {set_type} | {quality_to_cn(q)} | UID:{uid} | {op_md}"
            )
            lines.append("\r")

    lines.append("")
    lines.append(f"第 {current_page}/{total_pages} 页")
    if current_page < total_pages and next_cmd:
        next_q = quote(next_cmd, safe="")
        lines.append(f"[下一页](mqqapi://aio/inlinecmd?command={next_q}&enter=false&reply=false)")

    return "\r".join(lines)

# ========== 命令 ==========
@accessory_help.handle(parameterless=[Cooldown(cd_time=3)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    msg = """
【饰品系统帮助】

你可以通过以下命令了解更多：

1.  **基础操作**：发送【饰品基础帮助】
    > 查看、背包、详情、装备、卸下

2.  **成长强化**：发送【饰品成长帮助】
    > 洗练、升阶、材料规则

3.  **整理预设**：发送【饰品整理帮助】
    > 分解、快速分解、预设、快速装备

4.  **查看信息**：发送【我的饰品】或【饰品背包】
""".strip()

    await send_help_message(
        bot, event, msg,
        k1="基础帮助", v1="饰品基础帮助",
        k2="成长帮助", v2="饰品成长帮助",
        k3="整理帮助", v3="饰品整理帮助",
        k4="饰品背包", v4="饰品背包"
    )


@accessory_basic_help.handle(parameterless=[Cooldown(cd_time=3)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    msg = """
【饰品基础帮助】

1）查看已装备饰品：
   发送：我的饰品

2）查看饰品背包：
   发送：饰品背包 [页码]
   例如：饰品背包 2

3）查看单件饰品详情：
   发送：查看饰品 饰品UID
   例如：查看饰品 acc_1730000000000_1234

4）装备饰品：
   发送：装备饰品 饰品UID
   例如：装备饰品 acc_1730000000000_1234

5）卸下饰品：
   发送：卸下饰品 部位
   可用部位：手镯 / 戒指 / 手环 / 项链
""".strip()

    await send_help_message(
        bot, event, msg,
        k1="我的饰品", v1="我的饰品",
        k2="饰品背包", v2="饰品背包",
        k3="查看饰品", v3="查看饰品",
        k4="主帮助", v4="饰品帮助"
    )


@accessory_growth_help.handle(parameterless=[Cooldown(cd_time=3)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    msg = """
【饰品成长帮助】

1）洗练饰品：
   发送：饰品洗练 饰品UID
   - 消耗【洗练石】随品阶增加
   - 每件饰品独立洗练次数
   - 150次保底：词条值固定上限，仅词条类型变化

2）饰品升阶：
   发送：饰品升阶 部位 材料UID1 [材料UID2 ...]
   例如：饰品升阶 项链 UID1 UID2
   规则：
   - 主饰品固定为该部位“当前已装备”的饰品
   - 该部位未装备时不可升阶
   - 材料必须在背包中，且与主饰品同阶同款
   - 升阶消耗：
     1→2：1件材料
     2→3：1件材料
     3→4：2件材料
     4→5：3件材料
   - 最高五阶，五阶不可继续升阶
   - 升阶后：保留当前词条，仅重置洗练次数（wash_count=0）
""".strip()

    await send_help_message(
        bot, event, msg,
        k1="洗练", v1="饰品洗练",
        k2="升阶示例", v2="饰品升阶 项链 UID1 UID2",
        k3="饰品背包", v3="饰品背包",
        k4="主帮助", v4="饰品帮助"
    )


@accessory_manage_help.handle(parameterless=[Cooldown(cd_time=3)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    msg = """
【饰品整理帮助】

1）单件分解：
   发送：饰品分解 饰品UID
   说明：已装备饰品不能直接分解，请先卸下

2）快速分解：
   发送：快速分解饰品 类型 品阶
   类型：全部 / 烈阳 / 玄渊 / 天衡 / 星痕 / 龙魄 / 手镯 / 戒指 / 手环 / 项链
   品阶：全部 / 1~5 / 一阶~五阶
   安全规则：
   - 当“类型=全部”或“品阶=全部”时，自动忽略4/5阶

3）饰品预设：
   发送：饰品预设 1/2/3
   - 保存当前已装备饰品到对应预设位
   - 若原有记录存在，则自动覆盖
   - 直接发送【饰品预设】可查看所有预设

4）快速装备饰品：
   发送：快速装备饰品 1/2/3
   - 一键装备对应预设中的饰品
   - 若预设中某个UID已不存在，会自动清理该失效记录
""".strip()

    await send_help_message(
        bot, event, msg,
        k1="分解", v1="饰品分解",
        k2="快速分解", v2="快速分解饰品 全部 全部",
        k3="预设", v3="饰品预设",
        k4="主帮助", v4="饰品帮助"
    )

@my_accessory.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return

    user_id = str(user_info["user_id"])
    data = _get_data(user_id)
    eq = data["equipped"]

    lines = ["☆------我的饰品------☆"]

    # 已装备饰品
    for s in SLOTS:
        it = eq.get(s)
        if not it:
            lines.append(f"{s}：未装备")
        else:
            lines.append(
                f"{s}：{it['name']}[{quality_to_cn(it.get('quality', 1))}]"
                f"（{it.get('set_type', '未知')}·{it.get('part', s)}）"
            )

    # 汇总饰品总加成
    effect = calc_accessory_effects(user_id)

    bonus_lines = []
    if effect.get("hp_pct", 0):
        bonus_lines.append(f"气血 +{effect['hp_pct'] * 100:.2f}%")
    if effect.get("atk_pct", 0):
        bonus_lines.append(f"攻击 +{effect['atk_pct'] * 100:.2f}%")
    if effect.get("crit_rate", 0):
        bonus_lines.append(f"会心 +{effect['crit_rate'] * 100:.2f}%")
    if effect.get("crit_damage", 0):
        bonus_lines.append(f"会心伤害 +{effect['crit_damage'] * 100:.2f}%")
    if effect.get("dmg_reduction", 0):
        bonus_lines.append(f"减伤 +{effect['dmg_reduction'] * 100:.2f}%")
    if effect.get("crit_resist", 0):
        bonus_lines.append(f"抗暴 +{effect['crit_resist'] * 100:.2f}%")

    lines.append("")
    lines.append("【饰品总加成】")
    if bonus_lines:
        lines.extend([f" - {x}" for x in bonus_lines])
    else:
        lines.append(" - 暂无加成")

    # 套装件数
    set_count = effect.get("set_count", {})
    lines.append("")
    lines.append("【套装件数】")
    if set_count:
        for set_name, cnt in set_count.items():
            lines.append(f" - {set_name}：{cnt}件")
    else:
        lines.append(" - 暂无套装")

    # 套装效果
    type_name_map = {
        "attack": "攻击提升",
        "true_damage": "附加真伤",
        "shield": "开场护盾",
        "reflect": "反伤",
        "armor_pen": "护甲穿透",
        "dmg_reduction": "伤害减免",
        "crit_rate": "会心率",
        "dodge": "闪避",
        "shield_break": "护盾穿透",
    }

    set_bonus = effect.get("set_bonus", [])
    lines.append("")
    lines.append("【已激活套装效果】")
    if set_bonus:
        for sb in set_bonus:
            set_name = sb.get("set", "未知")
            pieces = sb.get("pieces", 0)
            sb_type = sb.get("type", "")
            sb_value = float(sb.get("value", 0))
            show_name = type_name_map.get(sb_type, sb_type)

            if sb_type == "dodge":
                lines.append(f" - {set_name}{pieces}件：{show_name} +{sb_value:.0f}点")
            else:
                lines.append(f" - {set_name}{pieces}件：{show_name} +{sb_value * 100:.2f}%")
    else:
        lines.append(" - 暂无激活效果")

    await handle_send(
        bot, event, "\n".join(lines),
        md_type="背包",
        k1="饰品背包", v1="饰品背包",
        k2="查看饰品", v2="查看饰品",
        k3="饰品帮助", v3="饰品帮助"
    )

@accessory_bag.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return

    user_id = str(user_info["user_id"])

    try:
        current_page = int(args.extract_plain_text().strip())
    except ValueError:
        current_page = 1

    data = _get_data(user_id)
    bag = data.get("bag", [])
    if not bag:
        await handle_send(bot, event, "饰品背包为空")
        return

    if XiuConfig().markdown_status:
        sections = _build_accessory_sections_for_md(user_id)
        if not sections:
            await handle_send(bot, event, "饰品背包为空")
            return

        page_sections, current_page, total_pages = _paginate_sections(
            sections, current_page, per_page=15
        )

        md_text = _build_accessory_md_text(
            title=f"{user_info.get('user_name', '道友')}的饰品背包",
            sections=page_sections,
            current_page=current_page,
            total_pages=total_pages,
            next_cmd=f"饰品背包 {current_page + 1}"
        )

        try:
            await bot.send(event=event, message=MessageSegment.markdown(bot, md_text))
        except Exception:
            await handle_send(bot, event, md_text)
        return

    sections = _build_accessory_sections_for_md(user_id)
    flat_rows = []
    for sec_title, rows in sections:
        for r in rows:
            flat_rows.append((sec_title, r))

    per_page = 15
    total_pages = (len(flat_rows) + per_page - 1) // per_page
    current_page = max(1, min(current_page, total_pages))

    start = (current_page - 1) * per_page
    end = start + per_page
    page_flat = flat_rows[start:end]

    title = [f"☆------{user_info.get('user_name', '道友')}的饰品背包------☆"]
    lines = []
    last_sec = None
    for sec_title, r in page_flat:
        if sec_title != last_sec:
            lines.append(f"\n【{sec_title}】")
            last_sec = sec_title

        lines.append(
            f"{r.get('name')} | {r.get('part')} | {r.get('set_type')} | {quality_to_cn(r.get('quality', 1))} | UID:{r.get('uid')}"
        )

    lines.append(f"\n第 {current_page}/{total_pages} 页")
    if current_page < total_pages:
        lines.append(f"输入 饰品背包 {current_page + 1} 查看下一页")
    lines.append("可用命令：装备饰品 UID / 饰品洗练 UID / 饰品分解 UID")
    page = ["翻页", f"饰品背包 {current_page + 1}", "装备", "装备饰品", "洗练", "饰品洗练", f"{current_page}/{total_pages}"]
    await send_msg_handler(bot, event, '饰品背包', bot.self_id, lines, title=title, page=page)

@check_accessory.handle(parameterless=[Cooldown(cd_time=1.2)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return

    uid = args.extract_plain_text().strip()
    if not uid:
        await handle_send(bot, event, "用法：查看饰品 饰品UID\n例如：查看饰品 acc_1730000000000_1234")
        return

    user_id = str(user_info["user_id"])
    data = _get_data(user_id)

    target = None
    where = "背包"

    for x in data.get("bag", []):
        if str(x.get("uid", "")) == uid:
            target = x
            where = "背包"
            break

    if not target:
        for s in SLOTS:
            it = data.get("equipped", {}).get(s)
            if it and str(it.get("uid", "")) == uid:
                target = it
                where = f"已装备（{s}）"
                break

    if not target:
        await handle_send(bot, event, "未找到该饰品UID，请检查是否输入正确。")
        return

    item_id = int(target.get("item_id", 0))
    item_info = items.get_data_by_item_id(item_id) or {}

    name = target.get("name", item_info.get("name", "未知饰品"))
    part = target.get("part", item_info.get("part", "未知部位"))
    set_type = target.get("set_type", item_info.get("set_type", "未知套装"))
    quality = int(target.get("quality", 1))
    desc = item_info.get("desc", "暂无介绍")

    affixes = target.get("affixes", [])
    if not affixes:
        affix_lines = ["- 无词条"]
    else:
        affix_lines = []
        for af in affixes:
            t = af.get("type", "未知")
            v = float(af.get("value", 0))
            affix_lines.append(f"- {t}：+{round(v * 100, 2)}%")

    set_lines = []
    sb = SET_BONUS.get(set_type, {})
    if 2 in sb:
        t = sb[2].get("type")
        v = float(sb[2].get("value", 0))
        t_cn = SET_TYPE_CN.get(t, t)
        if t in SET_VALUE_POINT_TYPES:
            set_lines.append(f"2件：{t_cn} +{round(v, 2)}点")
        else:
            set_lines.append(f"2件：{t_cn} +{round(v * 100, 2)}%")
    if 4 in sb:
        t = sb[4].get("type")
        v = float(sb[4].get("value", 0))
        t_cn = SET_TYPE_CN.get(t, t)
        if t in SET_VALUE_POINT_TYPES:
            set_lines.append(f"4件：{t_cn} +{round(v, 2)}点")
        else:
            set_lines.append(f"4件：{t_cn} +{round(v * 100, 2)}%")
    if not set_lines:
        set_lines = ["暂无套装效果配置"]

    msg = (
        f"☆------饰品详情------☆\n"
        f"名称：{name}\n"
        f"UID：{uid}\n"
        f"品阶：{quality_to_cn(quality)}\n"
        f"部位：{part}\n"
        f"套装：{set_type}\n"
        f"状态：{where}\n"
        f"介绍：{desc}\n\n"
        f"【当前词条】\n" + "\n".join(affix_lines) + "\n\n"
        f"【套装效果】\n" + "\n".join(set_lines)
    )

    await handle_send(
        bot, event, msg,
        md_type="背包",
        k1="饰品背包", v1="饰品背包",
        k2="我的饰品", v2="我的饰品",
        k3="饰品帮助", v3="饰品帮助"
    )

@equip_accessory.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return

    uid = args.extract_plain_text().strip()
    if not uid:
        await handle_send(bot, event, "用法：装备饰品 饰品UID")
        return

    user_id = str(user_info["user_id"])
    result = {"ok": False, "msg": "未找到该饰品UID"}

    def _mut(doc):
        nonlocal result
        doc = _normalize_accessory_doc(doc)
        bag = doc["bag"]

        hit_idx = -1
        hit = None
        for i, x in enumerate(bag):
            if str(x.get("uid", "")) == uid:
                hit_idx = i
                hit = x
                break
        if hit_idx < 0:
            return False

        part = hit.get("part")
        if part not in SLOTS:
            result["msg"] = "饰品部位异常，无法装备"
            return False

        old = doc["equipped"].get(part)
        if old:
            bag.append(old)

        doc["equipped"][part] = hit
        del bag[hit_idx]

        result["ok"] = True
        result["msg"] = f"已装备：{hit.get('name', '未知饰品')} 到 {part}"
        return True

    player_data_manager.patch_doc(
        user_id=user_id,
        table_name=TABLE,
        fields=["equipped", "bag"],
        mutator=_mut,
        default_factory=_default_accessory_doc
    )

    await handle_send(bot, event, result["msg"])

@unequip_accessory.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return

    part = args.extract_plain_text().strip()
    if part not in SLOTS:
        await handle_send(bot, event, "用法：卸下饰品 手镯/戒指/手环/项链")
        return

    user_id = str(user_info["user_id"])
    result = {"ok": False, "msg": f"{part}未装备饰品"}

    def _mut(doc):
        nonlocal result
        doc = _normalize_accessory_doc(doc)
        cur = doc["equipped"].get(part)
        if not cur:
            return False
        doc["bag"].append(cur)
        doc["equipped"][part] = None
        result["ok"] = True
        result["msg"] = f"已卸下：{cur.get('name', '未知饰品')}"
        return True

    player_data_manager.patch_doc(
        user_id=user_id,
        table_name=TABLE,
        fields=["equipped", "bag"],
        mutator=_mut,
        default_factory=_default_accessory_doc
    )

    await handle_send(bot, event, result["msg"])

@wash_accessory.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return

    uid = args.extract_plain_text().strip()
    if not uid:
        await handle_send(bot, event, "用法：饰品洗练 饰品UID")
        return

    user_id = str(user_info["user_id"])

    data = _get_data(user_id)
    where, key, target = _find_accessory_anywhere(data, uid)
    if not target:
        await handle_send(bot, event, "未找到该饰品UID")
        return

    q = max(1, min(5, int(target.get("quality", 1))))
    need = WASH_STONE_COST.get(q, 1)
    have = sql_message.goods_num(user_id, WASH_STONE_ID)

    if have < need:
        await handle_send(
            bot, event,
            f"洗练失败：{WASH_STONE_NAME}不足（需要{need}个，当前{have}个）",
            md_type="背包", k1="背包", v1="我的背包", k2="饰品", v2="饰品背包"
        )
        return

    sql_message.update_back_j(user_id, WASH_STONE_ID, num=need)

    result = {"ok": False, "msg": "洗练失败：未找到饰品"}

    def _mut(doc):
        nonlocal result
        doc = _normalize_accessory_doc(doc)

        w, k, t = _find_accessory_anywhere(doc, uid)
        if not t:
            result["msg"] = "洗练失败：饰品不存在（可能刚被操作）"
            return False

        q2 = max(1, min(5, int(t.get("quality", 1))))
        old_cnt = len(t.get("affixes", [])) if isinstance(t.get("affixes", []), list) else 2
        old_cnt = max(1, min(4, old_cnt))

        wash_count = int(t.get("wash_count", 0)) + 1
        t["wash_count"] = wash_count

        pity_reached = wash_count >= 150
        t["affixes"] = roll_affixes_with_pity(q2, old_cnt, pity_reached=pity_reached)

        if w == "bag":
            doc["bag"][k] = t
        else:
            doc["equipped"][k] = t

        tip = "（已触发150次保底：词条数值固定上限）" if pity_reached else ""
        result["ok"] = True
        result["msg"] = (
            f"洗练完成：{t.get('name','未知饰品')}（{quality_to_cn(q2)}）\n"
            f"消耗{WASH_STONE_NAME}：{need}个\n"
            f"当前洗练次数：{wash_count}/150 {tip}"
        )
        return True

    player_data_manager.patch_doc(
        user_id=user_id,
        table_name=TABLE,
        fields=["equipped", "bag"],
        mutator=_mut,
        default_factory=_default_accessory_doc
    )

    await handle_send(
        bot, event, result["msg"],
        md_type="背包", k1="饰品", v1="饰品背包", k2="查看", v2="我的饰品"
    )

@decompose_accessory.handle(parameterless=[Cooldown(cd_time=1.2)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return

    uid = args.extract_plain_text().strip()
    if not uid:
        await handle_send(bot, event, "用法：饰品分解 饰品UID")
        return

    user_id = str(user_info["user_id"])
    result = {"ok": False, "gain": 0, "name": "未知饰品", "q": 1, "msg": ""}

    def _mut(doc):
        nonlocal result
        doc = _normalize_accessory_doc(doc)
        idx, target = _find_accessory_in_bag(doc, uid)
        if idx < 0 or not target:
            result["msg"] = "分解失败：未在饰品背包中找到该UID（已装备饰品请先卸下）"
            return False

        q = max(1, min(5, int(target.get("quality", 1))))
        gain = ACCESSORY_DECOMPOSE_GAIN.get(q, 1)

        result["ok"] = True
        result["gain"] = gain
        result["name"] = target.get("name", "未知饰品")
        result["q"] = q

        del doc["bag"][idx]
        return True

    player_data_manager.patch_doc(
        user_id=user_id,
        table_name=TABLE,
        fields=["equipped", "bag"],
        mutator=_mut,
        default_factory=_default_accessory_doc
    )

    if not result["ok"]:
        await handle_send(bot, event, result["msg"] or "分解失败")
        return

    sql_message.send_back(user_id, WASH_STONE_ID, WASH_STONE_NAME, "特殊道具", result["gain"], 1)
    await handle_send(
        bot, event,
        f"已分解：{result['name']}（{quality_to_cn(result['q'])}）\n获得{WASH_STONE_NAME}：{result['gain']}个",
        md_type="背包", k1="饰品", v1="饰品背包", k2="背包", v2="我的背包"
    )

@quick_decompose_accessory.handle(parameterless=[Cooldown(cd_time=2)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return

    parts = args.extract_plain_text().split()
    if len(parts) < 2:
        await handle_send(
            bot, event,
            "用法：快速分解饰品 类型 品阶\n类型：全部/烈阳/玄渊/天衡/星痕/龙魄/手镯/戒指/手环/项链\n品阶：全部/1~5/一阶~五阶"
        )
        return

    t = parts[0].strip()
    q_text = parts[1].strip()

    if q_text == "全部":
        q_filter = None
    else:
        q_filter = _parse_quality_arg(q_text)
        if q_filter is None:
            await handle_send(bot, event, "品阶参数错误，请使用：全部/1~5/一阶~五阶")
            return

    valid_types = ["全部", "烈阳", "玄渊", "天衡", "星痕", "龙魄", "手镯", "戒指", "手环", "项链"]
    if t not in valid_types:
        await handle_send(bot, event, f"类型参数错误：{t}\n可用类型：{'/'.join(valid_types)}")
        return

    user_id = str(user_info["user_id"])
    data = _get_data(user_id)
    bag = data.get("bag", [])

    if not bag:
        await handle_send(bot, event, "饰品背包为空，无可分解饰品")
        return

    keep = []
    hit = []
    total_gain = 0

    safe_mode = (t == "全部" or q_text == "全部")

    for acc in bag:
        ok_type = _match_accessory_type(acc, t)
        q = max(1, min(5, int(acc.get("quality", 1))))
        ok_quality = (q_filter is None or q == q_filter)

        if safe_mode and q >= 4:
            keep.append(acc)
            continue

        if ok_type and ok_quality:
            hit.append(acc)
            total_gain += ACCESSORY_DECOMPOSE_GAIN.get(q, 1)
        else:
            keep.append(acc)

    if not hit:
        await handle_send(bot, event, "未找到符合条件的饰品")
        return

    data["bag"] = keep
    _save_data(user_id, data)

    sql_message.send_back(user_id, WASH_STONE_ID, WASH_STONE_NAME, "特殊道具", total_gain, 1)

    await handle_send(
        bot, event,
        f"快速分解完成：{len(hit)}件\n筛选：{t} / {q_text}\n获得{WASH_STONE_NAME}：{total_gain}个",
        md_type="背包", k1="饰品", v1="饰品背包", k2="背包", v2="我的背包"
    )

@upgrade_accessory.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return

    parts = args.extract_plain_text().split()
    if len(parts) < 2:
        await handle_send(
            bot, event,
            "用法：饰品升阶 部位 材料UID1 [材料UID2 ...]\n例如：饰品升阶 项链 UID1 UID2",
            md_type="背包", k1="饰品", v1="饰品背包", k2="帮助", v2="饰品帮助"
        )
        return

    part = parts[0]
    material_uids = parts[1:]

    if part not in SLOTS:
        await handle_send(bot, event, f"部位错误，可用：{'/'.join(SLOTS)}")
        return

    user_id = str(user_info["user_id"])
    data = _get_data(user_id)

    # 主饰品=该部位已装备饰品
    main_acc = data.get("equipped", {}).get(part)
    if not main_acc:
        await handle_send(bot, event, f"{part}当前未装备饰品，无法升阶")
        return

    main_q = int(main_acc.get("quality", 1))
    if main_q >= 5:
        await handle_send(bot, event, "该饰品已达最高五阶，无法继续升阶")
        return

    need_cnt = _get_upgrade_cost(main_q)

    # 材料UID去重，避免重复传同一个
    material_uids = list(dict.fromkeys(material_uids))

    if len(material_uids) < need_cnt:
        await handle_send(bot, event, f"材料数量不足：当前升阶需 {need_cnt} 个材料UID")
        return

    bag = data.get("bag", [])
    uid_to_idx = {}
    for i, x in enumerate(bag):
        uid_to_idx[str(x.get("uid", ""))] = i

    consume_idx = []
    for mu in material_uids:
        idx = uid_to_idx.get(str(mu))
        if idx is None:
            await handle_send(bot, event, f"材料UID无效或不在背包中：{mu}")
            return

        mat = bag[idx]
        # 材料必须同阶同款（同 item_id/part/set_type/quality）
        if not _is_same_accessory_for_upgrade(main_acc, mat):
            await handle_send(bot, event, f"材料不匹配：{mu} 不是同阶同款饰品")
            return

        consume_idx.append(idx)

    # 只取需要数量
    consume_idx = consume_idx[:need_cnt]

    # 删除材料（倒序删）
    for i in sorted(set(consume_idx), reverse=True):
        del bag[i]

    # 升阶：仅提升品阶 + 重置洗练次数；保留原词条
    old_q = main_q
    new_q = old_q + 1
    main_acc["quality"] = new_q
    main_acc["wash_count"] = 0

    data["equipped"][part] = main_acc
    data["bag"] = bag
    _save_data(user_id, data)

    await handle_send(
        bot, event,
        f"升阶成功：{main_acc.get('name', '未知饰品')} {quality_to_cn(old_q)} → {quality_to_cn(new_q)}\n"
        f"消耗材料：{need_cnt}件同阶同款饰品\n"
        f"当前词条已保留，仅重置洗练次数",
        md_type="背包",
        k1="我的饰品", v1="我的饰品",
        k2="饰品背包", v2="饰品背包",
        k3="查看饰品", v3=f"查看饰品 {main_acc.get('uid', '')}"
    )

@accessory_preset.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return

    user_id = str(user_info["user_id"])
    arg = args.extract_plain_text().strip()

    # 不带参数：显示全部预设
    if not arg:
        msg = "\n\n".join([
            "☆------饰品预设------☆",
            _format_accessory_preset(user_id, 1),
            _format_accessory_preset(user_id, 2),
            _format_accessory_preset(user_id, 3),
        ])
        await handle_send(
            bot, event, msg,
            md_type="背包",
            k1="保存1", v1="饰品预设 1",
            k2="保存2", v2="饰品预设 2",
            k3="快速装备", v3="快速装备饰品 1"
        )
        return

    if arg not in {"1", "2", "3"}:
        await handle_send(bot, event, "用法：饰品预设 1/2/3\n或直接发送【饰品预设】查看当前预设")
        return

    preset_idx = int(arg)
    old_preset = _clean_accessory_preset(user_id, preset_idx)

    data = _get_data(user_id)
    eq = data.get("equipped", {})

    new_preset = _default_accessory_preset()
    for s in SLOTS:
        it = eq.get(s)
        new_preset[s] = it.get("uid") if it else None

    had_old = any(old_preset.get(s) for s in SLOTS)
    _save_accessory_preset(user_id, preset_idx, new_preset)

    msg_lines = [f"已保存当前装备到饰品预设{preset_idx}。"]
    if had_old:
        msg_lines.append("检测到原有记录，本次已覆盖。")
    msg_lines.append("")
    msg_lines.append(_format_accessory_preset(user_id, preset_idx))

    await handle_send(
        bot, event, "\n".join(msg_lines),
        md_type="背包",
        k1="查看预设", v1="饰品预设",
        k2="快速装备", v2=f"快速装备饰品 {preset_idx}",
        k3="我的饰品", v3="我的饰品"
    )

@quick_equip_accessory.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return

    user_id = str(user_info["user_id"])
    arg = args.extract_plain_text().strip()

    if arg not in {"1", "2", "3"}:
        await handle_send(bot, event, "用法：快速装备饰品 1/2/3")
        return

    preset_idx = int(arg)
    preset = _clean_accessory_preset(user_id, preset_idx)

    if not any(preset.get(s) for s in SLOTS):
        await handle_send(bot, event, f"饰品预设{preset_idx}为空，无法快速装备。")
        return

    result = {
        "equipped": [],
        "skipped": [],
        "missing": []
    }

    def _mut(doc):
        doc = _normalize_accessory_doc(doc)

        for s in SLOTS:
            uid = preset.get(s)
            if not uid:
                continue

            # 已在该部位装备
            current_eq = doc.get("equipped", {}).get(s)
            if current_eq and str(current_eq.get("uid", "")) == str(uid):
                result["skipped"].append(f"{s}已是目标饰品")
                continue

            # 在背包中找
            hit_idx = -1
            hit = None
            for i, x in enumerate(doc.get("bag", [])):
                if str(x.get("uid", "")) == str(uid):
                    hit_idx = i
                    hit = x
                    break

            # 不在背包，再看看是不是装备在别的地方（理论上正常不会错部位，但做兼容）
            if not hit:
                for slot_name in SLOTS:
                    it = doc.get("equipped", {}).get(slot_name)
                    if it and str(it.get("uid", "")) == str(uid):
                        hit = it
                        doc["equipped"][slot_name] = None
                        break

            if not hit:
                result["missing"].append(f"{s}预设饰品已不存在")
                continue

            # 部位校验
            if hit.get("part") != s:
                result["skipped"].append(f"{hit.get('name', '未知饰品')}部位不匹配，跳过")
                continue

            # 当前部位旧装备回背包
            old = doc["equipped"].get(s)
            if old:
                doc["bag"].append(old)

            # 如果命中对象来自背包则删掉
            if hit_idx >= 0:
                del doc["bag"][hit_idx]

            doc["equipped"][s] = hit
            result["equipped"].append(f"{s}→{hit.get('name', '未知饰品')}")

        return True

    player_data_manager.patch_doc(
        user_id=user_id,
        table_name=TABLE,
        fields=["equipped", "bag"],
        mutator=_mut,
        default_factory=_default_accessory_doc
    )

    msg_lines = [f"☆------快速装备饰品{preset_idx}------☆"]

    if result["equipped"]:
        msg_lines.append("【成功装备】")
        msg_lines.extend([f" - {x}" for x in result["equipped"]])

    if result["skipped"]:
        msg_lines.append("【跳过】")
        msg_lines.extend([f" - {x}" for x in result["skipped"]])

    if result["missing"]:
        msg_lines.append("【失效记录已清理】")
        msg_lines.extend([f" - {x}" for x in result["missing"]])

    await handle_send(
        bot, event, "\n".join(msg_lines),
        md_type="背包",
        k1="我的饰品", v1="我的饰品",
        k2="饰品预设", v2="饰品预设",
        k3="饰品背包", v3="饰品背包"
    )
