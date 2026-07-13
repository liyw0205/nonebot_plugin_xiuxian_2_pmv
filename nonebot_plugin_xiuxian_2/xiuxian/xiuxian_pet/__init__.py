import copy
import re
import time
from urllib.parse import quote

from nonebot.params import CommandArg

from ..adapter_compat import Bot, GroupMessageEvent, Message, MessageSegment, PrivateMessageEvent
from ..on_compat import on_command
from ..messaging.delivery import delivery_service
from ..xiuxian_config import XiuConfig
from ..xiuxian_utils.game_events import safe_record_game_event
from ..xiuxian_utils.item_json import Items
from ..xiuxian_utils.lay_out import Cooldown
from ..xiuxian_utils.pet_system import (
    EGG_COST,
    EGG_PITY_RARITY_WEIGHTS,
    EGG_PITY_THRESHOLD,
    PET_BAG_LIMIT,
    PET_TRAVEL_MAX_HOURS,
    PET_TRAVEL_MIN_HOURS,
    PET_RELEASE_REFUND_ITEM_ID,
    QIMING_STONE_ID,
    build_pet_detail,
    calc_feed_exp,
    calc_pet_release_refund,
    can_add_pets,
    create_pet_instance,
    prepare_pet_travel_completion,
    exp_to_next_star,
    feed_active_pet,
    format_pet_travel_time,
    format_stars,
    fuse_pet,
    get_pet_travel_scenes,
    get_pet_travel_status,
    get_pet_doc,
    get_pet_bag_rows,
    get_pet_total_count,
    get_releasable_pets_by_keyword,
    get_pet_travel_scene_key,
    grant_pet,
    grant_pet_egg_pity_rewards,
    PET_TRAVEL_ITEM_POOLS,
    roll_egg_pity_rarity,
    roll_pet_template_by_rarity,
    _put_pet_into_doc,
    remove_pets_by_keyword,
    remove_pet,
    replace_pet_skill,
    requires_fusion_for_next_star,
    reroll_pet_skill,
    set_active_pet,
    validate_pet_feed_item,
)
from ..xiuxian_utils.utils import (
    check_user,
    handle_send,
    number_to,
    send_help_message,
    send_msg_handler,
    update_statistics_value,
)
from ..xiuxian_utils.xiuxian2_handle import XiuxianDateManage
from ...paths import get_paths
from .travel_claim_service import PetTravelClaimService
from .feed_service import PetFeedService
from .skill_replace_service import PetSkillReplaceService
from .travel_start_service import PetTravelStartService
from .hatch_service import PetHatchService
from .release_service import PetReleaseService

items = Items()
sql_message = XiuxianDateManage()
pet_travel_claim_service = PetTravelClaimService(get_paths().game_db, get_paths().player_db)
pet_feed_service = PetFeedService(get_paths().game_db, get_paths().player_db)
pet_skill_replace_service = PetSkillReplaceService(get_paths().player_db)
pet_travel_start_service = PetTravelStartService(get_paths().player_db)
pet_hatch_service = PetHatchService(get_paths().game_db, get_paths().player_db)
pet_release_service = PetReleaseService(get_paths().game_db, get_paths().player_db)

pet_help = on_command("宠物帮助", aliases={"宠物系统帮助"}, priority=10, block=True)
pet_intro_help = on_command("宠物入门帮助", aliases={"宠物获取帮助", "宠物查看帮助"}, priority=10, block=True)
pet_growth_help = on_command("宠物成长帮助", aliases={"宠物养成帮助", "宠物技能帮助"}, priority=10, block=True)
pet_bag_help = on_command("宠物背包帮助", aliases={"宠物放生帮助"}, priority=10, block=True)
pet_travel_help = on_command("宠物游历帮助", aliases={"宠物派遣帮助"}, priority=10, block=True)
pet_info = on_command("我的宠物", aliases={"宠物信息"}, priority=10, block=True)
pet_bag = on_command("宠物背包", aliases={"灵宠背包"}, priority=10, block=True)
pet_egg = on_command("砸蛋", aliases={"砸宠物蛋"}, priority=10, block=True)
pet_release = on_command("放生宠物", aliases={"宠物放生"}, priority=10, block=True)
pet_release_batch = on_command("一键放生", aliases={"批量放生", "一键放生宠物"}, priority=10, block=True)
pet_feed = on_command("宠物喂食", priority=10, block=True)
pet_fusion = on_command("宠物融合", priority=10, block=True)
pet_check = on_command("查看宠物", priority=10, block=True)
pet_set_active = on_command("出战宠物", aliases={"携带宠物", "宠物出战"}, priority=10, block=True)
pet_reroll_skill = on_command("宠物启明", aliases={"重置宠物技能", "宠物洗技能"}, priority=10, block=True)
pet_replace_skill = on_command("替换宠物技能", aliases={"确认替换宠物技能", "宠物技能替换"}, priority=10, block=True)
pet_keep_skill = on_command("保留宠物技能", aliases={"放弃替换宠物技能", "取消替换宠物技能"}, priority=10, block=True)
pet_travel = on_command("宠物游历", aliases={"灵宠游历", "派遣宠物", "宠物派遣"}, priority=10, block=True)
pet_travel_status = on_command("宠物游历状态", aliases={"灵宠游历状态", "游历状态"}, priority=10, block=True)
pet_travel_claim = on_command("领取宠物游历", aliases={"宠物游历领取", "领取游历奖励", "宠物游历结算"}, priority=10, block=True)

PET_SKILL_REPLACE_CACHE = {}
PET_SKILL_REPLACE_EXPIRE = 300


def _split_args(text: str):
    text = str(text or "").strip()
    if not text:
        return []
    return [x for x in re.split(r"[\s,，、]+", text) if x]


def _parse_feed_args(text: str):
    text = str(text or "").strip()
    if not text:
        return "", 0

    parts = text.split()
    count = 1

    if len(parts) >= 2 and parts[-1].isdigit():
        count = int(parts[-1])
        item_name = "".join(parts[:-1]).strip()
    else:
        match = re.match(r"^(.+?)(?:[xX*×])?(\d+)$", text)
        if match:
            item_name = match.group(1).strip()
            count = int(match.group(2))
        else:
            item_name = text.strip()

    return item_name, max(1, count)


def _parse_egg_count(text: str):
    text = str(text or "").strip()
    if not text:
        return 1, ""

    parts = _split_args(text)
    if len(parts) != 1:
        return 0, "用法：砸蛋 [数量]，数量范围1-10。"

    try:
        count = int(parts[0])
    except ValueError:
        return 0, "砸蛋数量必须是数字，范围1-10。"

    if count < 1 or count > 10:
        return 0, "砸蛋数量范围为1-10。"

    return count, ""


def _summarize_pet_rarities(pets: list[dict]):
    counts = {}
    for pet in pets:
        rarity = pet.get("rarity", "常见")
        counts[rarity] = counts.get(rarity, 0) + 1
    return "、".join(f"{rarity}x{count}" for rarity, count in counts.items()) or "无"


def _format_egg_pity_status(pity_count: int, no_mythic_count: int):
    return (
        f"保底进度：{pity_count}/{EGG_PITY_THRESHOLD}；"
        f"神话兜底：{no_mythic_count}/9（满9后下次保底必出神话）"
    )


def _format_egg_pity_weight_text():
    return "/".join(str(EGG_PITY_RARITY_WEIGHTS.get(rarity, 0)) for rarity in ("卓越", "传说", "神话"))


def _format_egg_pity_rewards(rewards: list[dict]):
    if not rewards:
        return []

    lines = ["保底触发："]
    for index, reward in enumerate(rewards, 1):
        pet = reward.get("pet", {}) or {}
        location = reward.get("location", "bag")
        location_msg = "出战" if location == "active" else "背包"
        forced_msg = "，九连未出神话强制" if reward.get("forced_mythic") else ""
        lines.append(
            f"{index}. {pet.get('form_name', pet.get('name', '未知宠物'))}"
            f"（{pet.get('rarity')}·{pet.get('race')}·{pet.get('type')}{forced_msg}，"
            f"UID:{pet.get('uid')}，{location_msg}）"
        )
    return lines


def _parse_travel_args(text: str):
    parts = _split_args(text)
    scene = parts[0] if parts else ""
    duration = 4

    if len(parts) == 1 and parts[0].isdigit():
        scene = ""
        duration = int(parts[0])
    elif len(parts) >= 2:
        try:
            duration = int(parts[1])
        except ValueError:
            return scene, 0, "游历时长必须是数字，单位为小时。"

    if duration < PET_TRAVEL_MIN_HOURS or duration > PET_TRAVEL_MAX_HOURS:
        return scene, 0, f"游历时长范围为{PET_TRAVEL_MIN_HOURS}-{PET_TRAVEL_MAX_HOURS}小时。"

    return scene, duration, ""


def _format_travel_status(travel: dict | None):
    if not travel:
        return "当前没有宠物正在游历。"

    remaining = max(0, int(travel.get("end_at", 0)) - int(time.time()))
    state = "可领取" if remaining <= 0 else f"剩余{format_pet_travel_time(remaining)}"
    return (
        "【宠物游历状态】\n"
        f"宠物：{travel.get('pet_name', '未知宠物')}（{travel.get('pet_rarity', '常见')}·{format_stars(travel.get('pet_stars', 1))}）\n"
        f"地点：{travel.get('scene_name', '灵草谷')}\n"
        f"时长：{travel.get('duration_hours', 1)}小时\n"
        f"状态：{state}"
    )


def _format_travel_scenes():
    lines = []
    for scene in get_pet_travel_scenes().values():
        lines.append(f"- {scene['name']}：{scene['desc']}")
    return "\n".join(lines)


def _grant_pet_travel_rewards(user_id: str, result: dict):
    lines = []
    stone = int(result.get("stone", 0) or 0)
    exp = int(result.get("exp", 0) or 0)

    if stone > 0:
        sql_message.update_ls(user_id, stone, 1)
        update_statistics_value(user_id, "灵石获取", increment=stone)
        lines.append(f"灵石：{number_to(stone)}")

    if exp > 0:
        sql_message.update_exp(user_id, exp)
        sql_message.update_power2(user_id)
        update_statistics_value(user_id, "宠物游历修为获取", increment=exp)
        lines.append(f"修为：{number_to(exp)}")

    for reward in result.get("items", []) or []:
        item_id = int(reward.get("id", 0) or 0)
        amount = int(reward.get("amount", 0) or 0)
        if item_id <= 0 or amount <= 0:
            continue
        item_info = items.get_data_by_item_id(item_id)
        if not item_info:
            continue
        sql_message.send_back(
            user_id,
            item_id,
            item_info.get("name", f"未知物品{item_id}"),
            item_info.get("type", "道具"),
            amount,
            1,
        )
        lines.append(f"{item_info.get('name', f'未知物品{item_id}')} x{amount}")

    update_statistics_value(user_id, "宠物游历次数")
    update_statistics_value(user_id, "宠物游历时长", increment=int(result.get("travel", {}).get("duration_hours", 0) or 0))
    safe_record_game_event(
        user_id,
        "pet_travel_claim",
        1,
        {
            "source": "pet",
            "action": "travel_claim",
            "stone_delta": stone,
            "exp_delta": exp,
            "item_delta": [
                {
                    "id": reward.get("id"),
                    "amount": reward.get("amount", 0),
                }
                for reward in result.get("items", []) or []
            ],
            "detail": {
                "duration_hours": int(result.get("travel", {}).get("duration_hours", 0) or 0),
                "scene": result.get("travel", {}).get("scene"),
                "pet_uid": result.get("travel", {}).get("pet_uid"),
            },
        },
    )
    return lines


def _grant_pet_release_refund(user_id: str, pets: list[dict]):
    refund_item = items.get_data_by_item_id(PET_RELEASE_REFUND_ITEM_ID) or {}
    refund_name = refund_item.get("name", "一阶天地灵髓")
    refund_type = refund_item.get("type", "特殊道具")
    total_refund_count = 0
    total_exp = 0
    total_refund_base_exp = 0
    refund_exp = 800

    for pet in pets:
        refund_count, pet_total_exp, pet_refund_base_exp, refund_exp = calc_pet_release_refund(pet, refund_item)
        total_refund_count += refund_count
        total_exp += pet_total_exp
        total_refund_base_exp += pet_refund_base_exp

    if total_refund_count > 0:
        sql_message.send_back(
            user_id,
            PET_RELEASE_REFUND_ITEM_ID,
            refund_name,
            refund_type,
            total_refund_count,
            1,
        )
        if len(pets) == 1:
            return (
                f"\n返还：{refund_name} x{total_refund_count}"
                f"（累计经验{total_exp}，按80%计{total_refund_base_exp}经验折算，{refund_exp}经验/个，余数不返）"
            )
        return (
            f"\n返还：{refund_name} x{total_refund_count}"
            f"（合计累计经验{total_exp}，逐只按80%折算，合计折算经验{total_refund_base_exp}，"
            f"{refund_exp}经验/个，余数不返）"
        )

    if len(pets) == 1:
        return f"\n返还：无（累计经验{total_exp}，按80%计{total_refund_base_exp}经验，不足{refund_exp}）"
    return (
        f"\n返还：无（合计累计经验{total_exp}，逐只按80%折算，"
        f"合计折算经验{total_refund_base_exp}，均不足{refund_exp}）"
    )


def _format_pet_release_refund(pets: list[dict], refund_item: dict, total_refund_count: int):
    refund_name = refund_item.get("name", "一阶天地灵髓")
    total_exp = 0
    total_refund_base_exp = 0
    refund_exp = 800
    for pet in pets:
        _, pet_total_exp, pet_refund_base_exp, refund_exp = calc_pet_release_refund(pet, refund_item)
        total_exp += pet_total_exp
        total_refund_base_exp += pet_refund_base_exp
    if total_refund_count > 0:
        return (
            f"\n返还：{refund_name} x{total_refund_count}"
            f"（合计累计经验{total_exp}，逐只按80%折算，合计折算经验{total_refund_base_exp}，"
            f"{refund_exp}经验/个，余数不返）"
        )
    return (
        f"\n返还：无（合计累计经验{total_exp}，逐只按80%折算，"
        f"合计折算经验{total_refund_base_exp}，均不足{refund_exp}）"
    )


def _resolve_item_name(item_name: str):
    item_id, item_info = items.get_data_by_item_name(item_name)
    if item_info:
        return item_id, item_info

    if item_name.endswith("材料"):
        item_id, item_info = items.get_data_by_item_name(item_name[:-2])
        if item_info:
            return item_id, item_info

    return None, None


def _format_skill_brief(skill: dict):
    if not isinstance(skill, dict):
        return "未知技能"
    name = skill.get("name", "未知技能")
    scope = skill.get("scope", "通用")
    category = skill.get("category", "基础")
    desc = skill.get("desc", "")
    if desc:
        return f"{name}（{scope}·{category}，{desc}）"
    return f"{name}（{scope}·{category}）"


def _cache_skill_offer(user_id: str, pet: dict, offer: dict | None):
    if not pet or not offer or not isinstance(offer.get("skill"), dict):
        return ""

    skill = dict(offer["skill"])
    PET_SKILL_REPLACE_CACHE[str(user_id)] = {
        "uid": str(pet.get("uid", "")),
        "skill": skill,
        "expire_at": time.time() + PET_SKILL_REPLACE_EXPIRE,
    }
    return (
        f"升★领悟候选技能：{_format_skill_brief(skill)}\n"
        f"当前技能：{_format_skill_brief(pet.get('skill', {}))}\n"
        "发送【替换宠物技能】替换，或发送【保留宠物技能】放弃。"
    )


def _pop_skill_offer(user_id: str):
    data = PET_SKILL_REPLACE_CACHE.get(str(user_id))
    if not data:
        return None
    if float(data.get("expire_at", 0)) < time.time():
        PET_SKILL_REPLACE_CACHE.pop(str(user_id), None)
        return None
    return data


def _format_my_pet(data: dict):
    lines = ["【我的宠物】"]
    active = data.get("active")

    if active:
        lines.append("【出战主宠】")
        lines.append(build_pet_detail(active))
    else:
        lines.append("当前未设置出战宠物。")
        if data.get("bag"):
            lines.append("可发送【宠物背包】查看背包宠物并设置出战。")

    return "\n".join(lines)


def _format_pet_bag(data: dict, page: int = 1, per_page: int = 15):
    rows = get_pet_bag_rows(data)
    if not rows:
        return "宠物背包为空。"

    total_pages = max(1, (len(rows) + per_page - 1) // per_page)
    page = max(1, min(int(page), total_pages))
    start = (page - 1) * per_page
    end = start + per_page

    lines = ["【宠物背包】", f"容量：{get_pet_total_count(data)}/{PET_BAG_LIMIT}"]
    for pet in rows[start:end]:
        active_flag = "【出战中】" if pet.get("is_active") else ""
        lines.append(
            f"- {active_flag}{pet.get('form_name', pet.get('name', '未知宠物'))}"
            f" | {pet.get('rarity', '常见')}"
            f" | {pet.get('type', '攻击')}"
            f" | {format_stars(pet.get('stars', 1))}"
            f" | UID:{pet.get('uid', '')}"
        )

    lines.append(f"\n第 {page}/{total_pages} 页")
    if page < total_pages:
        lines.append(f"输入 宠物背包 {page + 1} 查看下一页")
    lines.append("可用命令：查看宠物 UID / 出战宠物 UID / 放生宠物 UID / 一键放生 稀有度或宠物名称")

    return "\n".join(lines)


def _build_pet_bag_md_text(
    title: str,
    data: dict,
    current_page: int,
    per_page: int = 15,
) -> tuple[str, int, int]:
    rows = get_pet_bag_rows(data)
    total_pages = max(1, (len(rows) + per_page - 1) // per_page)
    current_page = max(1, min(int(current_page), total_pages))
    start = (current_page - 1) * per_page
    end = start + per_page

    lines = [f"【{title}】", "", f"容量：{get_pet_total_count(data)}/{PET_BAG_LIMIT}", ""]

    for pet in rows[start:end]:
        name = pet.get("form_name", pet.get("name", "未知宠物"))
        rarity = pet.get("rarity", "常见")
        pet_type = pet.get("type", "攻击")
        stars = format_stars(pet.get("stars", 1))
        uid = str(pet.get("uid", ""))
        active_flag = "【出战中】" if pet.get("is_active") else ""

        view_cmd = quote(f"查看宠物 {uid}", safe="")
        active_cmd = quote(f"出战宠物 {uid}", safe="")
        release_cmd = quote(f"放生宠物 {uid}", safe="")

        name_md = f"[{name}](mqqapi://aio/inlinecmd?command={view_cmd}&enter=false&reply=false)"
        op_md = (
            f"[出战](mqqapi://aio/inlinecmd?command={active_cmd}&enter=false&reply=false) "
            f"[放生](mqqapi://aio/inlinecmd?command={release_cmd}&enter=false&reply=false)"
        )
        lines.append(f"> - {active_flag}{name_md} | {rarity} | {pet_type} | {stars} | UID:{uid} | {op_md}")
        lines.append("\r")

    lines.append("")
    lines.append(f"第 {current_page}/{total_pages} 页")
    if current_page < total_pages:
        next_cmd = quote(f"宠物背包 {current_page + 1}", safe="")
        lines.append(f"[下一页](mqqapi://aio/inlinecmd?command={next_cmd}&enter=false&reply=false)")

    return "\r".join(lines), current_page, total_pages


def _build_pet_bag_plain_text(
    title: str,
    data: dict,
    current_page: int,
    per_page: int = 15,
) -> str:
    rows = get_pet_bag_rows(data)
    total_pages = max(1, (len(rows) + per_page - 1) // per_page)
    current_page = max(1, min(int(current_page), total_pages))
    start = (current_page - 1) * per_page
    end = start + per_page

    lines = [
        f"【{title}】",
        f"容量：{get_pet_total_count(data)}/{PET_BAG_LIMIT}",
        "",
    ]

    for pet in rows[start:end]:
        active_flag = "【出战中】" if pet.get("is_active") else ""
        lines.append(
            f"- {active_flag}{pet.get('form_name', pet.get('name', '未知宠物'))}"
            f" | {pet.get('rarity', '常见')}"
            f" | {pet.get('type', '攻击')}"
            f" | {format_stars(pet.get('stars', 1))}"
            f" | UID:{pet.get('uid', '')}"
        )

    lines.extend([
        "",
        f"第 {current_page}/{total_pages} 页",
    ])
    if current_page < total_pages:
        lines.append(f"输入 宠物背包 {current_page + 1} 查看下一页")
    lines.append("可用命令：查看宠物 UID / 出战宠物 UID / 放生宠物 UID / 一键放生 稀有度或宠物名称")

    return "\n".join(lines)


@pet_help.handle(parameterless=[Cooldown(cd_time=3)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    msg = f"""
**宠物系统帮助**

---

1.  **入门获取**：宠物入门帮助
    > 砸蛋、宠物蛋、查看宠物、切换出战

2.  **成长技能**：宠物成长帮助
    > 喂食、融合突破、启明、技能替换

3.  **背包放生**：宠物背包帮助
    > 宠物背包、放生、一键放生、经验返还

4.  **游历派遣**：宠物游历帮助
    > 派遣出战宠物外出，返回后领取材料、灵髓或宠物资源

5.  **查看信息**：我的宠物 / 宠物背包

""".strip()

    await send_help_message(
        bot,
        event,
        msg,
        k1="入门帮助",
        v1="宠物入门帮助",
        k2="成长帮助",
        v2="宠物成长帮助",
        k3="背包帮助",
        v3="宠物背包帮助",
        k4="游历帮助",
        v4="宠物游历帮助",
    )


@pet_intro_help.handle(parameterless=[Cooldown(cd_time=3)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    msg = f"""
**宠物入门帮助**

---

**砸蛋**
   发送：砸蛋 [数量]
   数量范围：1-10
   消耗：{number_to(EGG_COST)}灵石
   规则：没有出战宠物时自动出战，否则进入宠物背包。
   保底：累计砸蛋{EGG_PITY_THRESHOLD}次自动额外抽取1只卓越/传说/神话宠物（{_format_egg_pity_weight_text()}），连续9次保底未出神话则第10次保底必出神话。
   宠物蛋：发送【道具使用 宠物蛋名 [数量]】可孵化指定稀有度宠物。

**查看宠物**
   发送：我的宠物（查看当前出战宠物）
   发送：宠物背包 [页码]（查看所有宠物，出战宠物排在最前，其余按品阶从高到低排序）
   发送：查看宠物 宠物UID
   每只宠物初始获得1个基础通用技能，技能受宠物稀有度、形态、品阶影响。
   满足条件的专属技能不会直接生效，只会在启明或整★领悟时概率随机出现。
   技能效果包含直伤、多段、持续伤害、控制、破盾、增益、护盾、净化、反伤等。

**切换出战**
   发送：出战宠物 宠物UID
""".strip()

    await send_help_message(
        bot,
        event,
        msg,
        k1="砸蛋",
        v1="砸蛋",
        k2="我的宠物",
        v2="我的宠物",
        k3="宠物背包",
        v3="宠物背包",
        k4="主帮助",
        v4="宠物帮助",
    )


@pet_growth_help.handle(parameterless=[Cooldown(cd_time=3)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    msg = """
**宠物成长帮助**

---

**喂食升级**
   发送：宠物喂食 材料名 [数量]
   例如：宠物喂食 恒心草 10
   可喂食：药材 / 一至五阶天地灵髓
   药材经验：按一品至九品递增，品阶越高经验越多。
   规则：☆主要通过喂食经验提升；当前品阶达到四个☆时需先喂满经验，再使用宠物融合突破为★。
   每五个☆折算为一个★，天地灵髓只能喂食对应★级及以下宠物。

**宠物启明**
   发送：宠物启明 [宠物UID]
   消耗：启明石 x1
   作用：为指定宠物重新随机获得1个技能，不填UID时默认当前出战宠物。

**融合突破**
   发送：宠物融合 本体UID [本体UID...] [破阶UID]
   例如：宠物融合 UID1 UID2 UID3
   规则：
   - 默认以当前出战宠物作为主宠
   - 只在当前品阶达到四个☆时使用；普通☆通过喂食经验提升
   - 四☆突破前需要先通过喂食补满当前经验
   - 材料均通过UID区分，同名宠物不会冲突
   - 本体必须在宠物背包中
   - 本体必须与主宠同名
   - UID不需要固定顺序，系统会自动判断本体和破阶宠，多填的会自动忽略
   - 主宠品阶越高，消耗本体越多
   - 传说破入★★★/★★★★时额外消耗1只满★普通宠物
   - 神话破入★★★★/★★★★★时额外消耗1只满★卓越宠物
   - 每提升到整★时会随机领悟候选技能，可选择是否替换；满足条件时专属技能有概率进入候选

**技能替换**
   发送：替换宠物技能
   发送：保留宠物技能
   规则：只在启明或突破产生候选技能后有效。
""".strip()

    await send_help_message(
        bot,
        event,
        msg,
        k1="喂食",
        v1="宠物喂食",
        k2="融合",
        v2="宠物融合",
        k3="启明",
        v3="宠物启明",
        k4="主帮助",
        v4="宠物帮助",
    )


@pet_bag_help.handle(parameterless=[Cooldown(cd_time=3)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    msg = """
**宠物背包帮助**

---

**宠物背包**
   发送：宠物背包 [页码]
   作用：查看所有宠物，出战宠物排在最前，其余按品阶从高到低排序。

**查看详情**
   发送：查看宠物 宠物UID

**放生**
   发送：放生宠物 宠物UID
   不填UID时默认放生当前出战宠物
   按累计宠物经验的80%返还一阶天地灵髓，不足1个时不返还

**一键放生**
   发送：一键放生 稀有度/宠物名称
   例如：一键放生 常见
   例如：一键放生 山灵犬
   规则：只放生背包宠物，出战宠物会跳过
""".strip()

    await send_help_message(
        bot,
        event,
        msg,
        k1="宠物背包",
        v1="宠物背包",
        k2="查看宠物",
        v2="查看宠物",
        k3="一键放生",
        v3="一键放生 常见",
        k4="主帮助",
        v4="宠物帮助",
    )


@pet_travel_help.handle(parameterless=[Cooldown(cd_time=3)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    msg = f"""
**宠物游历帮助**

---

**开始游历**
   发送：宠物游历 [地点] [小时]
   例如：宠物游历 灵草谷 4
   时长范围：{PET_TRAVEL_MIN_HOURS}-{PET_TRAVEL_MAX_HOURS}小时
   不填地点默认灵草谷，不填小时默认4小时。
   当前仅派遣出战宠物，同一时间只能有1只宠物游历。

**查看和领取**
   发送：宠物游历状态
   发送：领取宠物游历

**游历地点**
{_format_travel_scenes()}

**收益规则**
   游历时长越长，奖励次数越多。
   宠物稀有度、品阶和形态会提高材料收益。
   有低概率带回宠物资源，宠物蛋概率参考地图奖励池。
   宠物游历不影响出战和战斗。
""".strip()

    await send_help_message(
        bot,
        event,
        msg,
        k1="开始游历",
        v1="宠物游历 灵草谷 4",
        k2="游历状态",
        v2="宠物游历状态",
        k3="领取",
        v3="领取宠物游历",
        k4="主帮助",
        v4="宠物帮助",
    )


@pet_info.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return

    data = get_pet_doc(str(user_info["user_id"]))
    await handle_send(
        bot,
        event,
        _format_my_pet(data),
        md_type="背包",
        k1="砸蛋",
        v1="砸蛋",
        k2="宠物背包",
        v2="宠物背包",
        k3="帮助",
        v3="宠物帮助",
    )


@pet_travel.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return

    scene, duration, error = _parse_travel_args(args.extract_plain_text())
    if error:
        await handle_send(bot, event, error)
        return

    user_id = str(user_info["user_id"])
    data = get_pet_doc(user_id)
    pet = data.get("active")
    scene_key = get_pet_travel_scene_key(scene)
    if data.get("travel") or not pet or not scene_key:
        result_msg = "已有宠物正在游历，请先领取游历收获。" if data.get("travel") else "当前没有可派遣的出战宠物。"
        await handle_send(
            bot,
            event,
            result_msg,
            md_type="背包",
            k1="游历状态",
            v1="宠物游历状态",
            k2="宠物",
            v2="我的宠物",
            k3="帮助",
            v3="宠物游历帮助",
        )
        return

    now = int(time.time())
    travel = {
        "pet_uid": str(pet.get("uid", "")),
        "pet_name": str(pet.get("form_name", pet.get("name", "未知宠物"))),
        "pet_rarity": str(pet.get("rarity", "常见")),
        "pet_stars": int(pet.get("stars", 1)),
        "scene": scene_key,
        "scene_name": PET_TRAVEL_ITEM_POOLS[scene_key]["name"],
        "start_at": now,
        "end_at": now + duration * 3600,
        "duration_hours": duration,
    }
    event_id = str(getattr(event, "message_id", "") or getattr(event, "id", "") or "").strip()
    started = pet_travel_start_service.start(
        f"pet-travel-start:{event_id or time.time_ns()}:{user_id}", user_id, pet["uid"], data.get("travel"), travel
    )
    if not started.succeeded:
        await handle_send(bot, event, "宠物游历状态已变化，请重新查询后再试。")
        return
    result_msg = "派遣成功。"

    await handle_send(
        bot,
        event,
        (
            f"{result_msg}\n"
            f"宠物：{pet.get('form_name', pet.get('name', '未知宠物'))}（{pet.get('rarity', '常见')}·{format_stars(pet.get('stars', 1))}）\n"
            f"地点：{travel.get('scene_name')}\n"
            f"时长：{travel.get('duration_hours')}小时\n"
            f"预计返回：{format_pet_travel_time(int(travel.get('end_at', 0)) - int(time.time()))}后\n"
            "返回后发送【领取宠物游历】领取收获。"
        ),
        md_type="背包",
        k1="状态",
        v1="宠物游历状态",
        k2="领取",
        v2="领取宠物游历",
        k3="帮助",
        v3="宠物游历帮助",
    )


@pet_travel_status.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return

    travel = get_pet_travel_status(str(user_info["user_id"]))
    await handle_send(
        bot,
        event,
        _format_travel_status(travel),
        md_type="背包",
        k1="领取",
        v1="领取宠物游历",
        k2="开始游历",
        v2="宠物游历",
        k3="帮助",
        v3="宠物游历帮助",
    )


@pet_travel_claim.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return

    user_id = str(user_info["user_id"])
    ok, result_msg, result = prepare_pet_travel_completion(user_id)
    if not ok:
        await handle_send(
            bot,
            event,
            result_msg,
            md_type="背包",
            k1="状态",
            v1="宠物游历状态",
            k2="帮助",
            v2="宠物游历帮助",
        )
        return

    reward_items = []
    for reward in result.get("items", []) or []:
        item_info = items.get_data_by_item_id(int(reward.get("id", 0) or 0))
        if item_info and int(reward.get("amount", 0) or 0) > 0:
            reward_items.append({
                "id": int(reward["id"]),
                "name": item_info.get("name", f"未知物品{reward['id']}"),
                "type": item_info.get("type", "道具"),
                "amount": int(reward["amount"]),
            })
    event_id = str(getattr(event, "message_id", "") or getattr(event, "id", "") or "").strip()
    travel = result.get("travel", {}) or {}
    travel_id = f"{travel.get('pet_uid', '')}:{travel.get('start_at', '')}:{travel.get('end_at', '')}"
    operation_id = f"pet-travel-claim:{event_id or travel_id}:{user_id}"
    claim_result = pet_travel_claim_service.claim(
        operation_id,
        user_id,
        travel,
        int(result.get("stone", 0) or 0),
        int(result.get("exp", 0) or 0),
        reward_items,
        XiuConfig().max_goods_num,
    )
    if claim_result.status == "inventory_full":
        await handle_send(bot, event, "背包物品已达上限，宠物游历奖励尚未领取。")
        return
    if claim_result.status == "state_changed":
        await handle_send(bot, event, "宠物游历状态已变化，请重新查询游历状态。")
        return
    if claim_result.status == "pet_missing":
        await handle_send(bot, event, "游历宠物状态已变化，奖励尚未领取。")
        return
    if claim_result.status == "user_missing":
        await handle_send(bot, event, "未找到道友数据，宠物游历奖励领取失败。")
        return
    reward_result = dict(result)
    reward_result["items"] = reward_items
    reward_lines = []
    if claim_result.stone > 0:
        reward_lines.append(f"灵石：{number_to(claim_result.stone)}")
    if claim_result.exp > 0:
        reward_lines.append(f"修为：{number_to(claim_result.exp)}")
    reward_lines.extend(f"{reward['name']} x{reward['amount']}" for reward in reward_items)
    update_statistics_value(user_id, "宠物游历次数")
    update_statistics_value(user_id, "宠物游历时长", increment=int(travel.get("duration_hours", 0) or 0))
    pet = result.get("pet", {}) or {}
    travel = result.get("travel", {}) or {}
    lines = [
        "宠物游历归来。",
        f"宠物：{pet.get('form_name', pet.get('name', travel.get('pet_name', '未知宠物')))}",
        f"地点：{travel.get('scene_name', '灵草谷')}",
        result.get("story", ""),
        "收获：",
    ]
    lines.extend(f"- {line}" for line in reward_lines)
    if not reward_lines:
        lines.append("- 无")

    await handle_send(
        bot,
        event,
        "\n".join(lines),
        md_type="背包",
        k1="再次游历",
        v1="宠物游历",
        k2="宠物",
        v2="我的宠物",
        k3="帮助",
        v3="宠物游历帮助",
    )


@pet_bag.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return

    try:
        current_page = int(args.extract_plain_text().strip())
    except ValueError:
        current_page = 1

    data = get_pet_doc(str(user_info["user_id"]))
    if not get_pet_bag_rows(data):
        await handle_send(bot, event, "宠物背包为空。")
        return

    if XiuConfig().markdown_status:
        md_text, _, _ = _build_pet_bag_md_text(
            title=f"{user_info.get('user_name', '道友')}的宠物背包",
            data=data,
            current_page=current_page,
        )
        fallback_text = _build_pet_bag_plain_text(
            title=f"{user_info.get('user_name', '道友')}的宠物背包",
            data=data,
            current_page=current_page,
        )
        try:
            await delivery_service.reply(bot, event, MessageSegment.markdown(bot, md_text))
        except Exception:
            await handle_send(bot, event, fallback_text)
        return

    await handle_send(
        bot,
        event,
        _format_pet_bag(data, current_page),
        md_type="背包",
        k1="我的宠物",
        v1="我的宠物",
        k2="出战",
        v2="出战宠物",
        k3="帮助",
        v3="宠物帮助",
    )


@pet_check.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return

    token = args.extract_plain_text().strip()
    if not token:
        await handle_send(bot, event, "用法：查看宠物 宠物UID")
        return

    data = get_pet_doc(str(user_info["user_id"]))
    target = None
    active = data.get("active")
    if active and str(active.get("uid")) == token:
        target = active
    if not target:
        for pet in data.get("bag", []):
            if str(pet.get("uid")) == token:
                target = pet
                break

    if not target:
        await handle_send(bot, event, "未找到该宠物UID。")
        return

    await handle_send(bot, event, "☆------宠物详情------☆\n" + build_pet_detail(target))


@pet_egg.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return

    user_id = str(user_info["user_id"])
    count, count_error = _parse_egg_count(args.extract_plain_text())
    if count_error:
        await handle_send(bot, event, count_error)
        return

    total_cost = EGG_COST * count
    if int(user_info.get("stone", 0)) < total_cost:
        await handle_send(
            bot,
            event,
            f"砸蛋{count}次需要{number_to(total_cost)}灵石，道友当前灵石不足。",
            md_type="背包",
            k1="灵石",
            v1="灵石",
            k2="帮助",
            v2="宠物帮助",
        )
        return

    data = get_pet_doc(user_id)
    pity_pet_count = (int(data.get("egg_pity_count", 0) or 0) + count) // EGG_PITY_THRESHOLD
    needed_capacity = count + pity_pet_count
    ok, owned, remaining = can_add_pets(user_id, needed_capacity)
    if not ok:
        await handle_send(
            bot,
            event,
            (
                f"宠物背包容量不足，无法砸蛋{count}次。\n"
                f"当前容量：{owned}/{PET_BAG_LIMIT}，剩余{remaining}格；本次最多需要{needed_capacity}格"
                f"（含可能触发的保底宠物{pity_pet_count}只）。\n"
                "请先放生或整理宠物。"
            ),
            md_type="背包",
            k1="宠物背包",
            v1="宠物背包",
            k2="放生",
            v2="一键放生",
        )
        return

    working = copy.deepcopy(data)
    original_uids = {str(p.get("uid")) for p in ([working.get("active")] + working.get("bag", [])) if p}
    pets = [_put_pet_into_doc(working, create_pet_instance()) for _ in range(count)]
    pity_rewards = []
    pity_count = int(working.get("egg_pity_count", 0)) + count
    no_mythic_count = int(working.get("egg_pity_no_mythic_count", 0))
    while pity_count >= EGG_PITY_THRESHOLD:
        pity_count -= EGG_PITY_THRESHOLD
        rarity, forced = roll_egg_pity_rarity(no_mythic_count)
        pet, location = _put_pet_into_doc(working, create_pet_instance(roll_pet_template_by_rarity(rarity)))
        no_mythic_count = 0 if rarity == "神话" else no_mythic_count + 1
        pity_rewards.append({"pet": pet, "location": location, "rarity": rarity, "forced_mythic": forced})
    new_rows = [(p, str(p.get("uid")) == str((working.get("active") or {}).get("uid", ""))) for p in ([working.get("active")] + working.get("bag", [])) if p and str(p.get("uid")) not in original_uids]
    expected_meta = [str((data.get("active") or {}).get("uid", "")), int(data.get("egg_pity_count", 0)), int(data.get("egg_pity_no_mythic_count", 0)), data.get("travel")]
    updated_meta = [str((working.get("active") or {}).get("uid", "")), pity_count, no_mythic_count]
    event_id = str(getattr(event, "message_id", "") or getattr(event, "id", "") or "").strip()
    hatched = pet_hatch_service.hatch(f"pet-hatch:{event_id or time.time_ns()}:{user_id}", user_id, int(user_info.get("stone", 0)), total_cost, expected_meta, new_rows, updated_meta, PET_BAG_LIMIT)
    if not hatched.succeeded:
        await handle_send(bot, event, "砸蛋状态已变化，请重新查询灵石和宠物背包后再试。")
        return
    success_cost = total_cost
    error_msg = ""
    pity_error = ""

    if len(pets) == 1 and not error_msg:
        pet, location = pets[0]
        location_msg = "已自动出战" if location == "active" else "已放入宠物背包，可发送【出战宠物 UID】切换出战"
        msg = (
            f"砸蛋成功！消耗{number_to(success_cost)}灵石。\n"
            f"获得宠物：{pet.get('form_name', pet.get('name', '未知宠物'))}\n"
            f"稀有度：{pet.get('rarity')} | 种族：{pet.get('race')} | 类型：{pet.get('type')}\n"
            f"初始技能：{pet.get('skill', {}).get('name', '未知技能')}\n"
            f"UID：{pet.get('uid')}\n"
            f"{location_msg}"
        )
        extra_lines = _format_egg_pity_rewards(pity_rewards)
        if pity_error:
            extra_lines.append(f"保底结算失败：{pity_error}")
        else:
            extra_lines.append(_format_egg_pity_status(pity_count, no_mythic_count))
        msg += "\n" + "\n".join(extra_lines)
    else:
        pet_list = [pet for pet, _ in pets]
        lines = [
            f"砸蛋完成：成功{len(pets)}/{count}次，消耗{number_to(success_cost)}灵石。",
            f"稀有度统计：{_summarize_pet_rarities(pet_list)}",
        ]
        if error_msg:
            lines.append(f"失败原因：{error_msg}")
        for index, (pet, location) in enumerate(pets, 1):
            location_msg = "出战" if location == "active" else "背包"
            lines.append(
                f"{index}. {pet.get('form_name', pet.get('name', '未知宠物'))}"
                f"（{pet.get('rarity')}·{pet.get('race')}·{pet.get('type')}，"
                f"UID:{pet.get('uid')}，{location_msg}）"
            )
        lines.extend(_format_egg_pity_rewards(pity_rewards))
        if pity_error:
            lines.append(f"保底结算失败：{pity_error}")
        else:
            lines.append(_format_egg_pity_status(pity_count, no_mythic_count))
        msg = "\n".join(lines)

    if count > 1:
        page = ["宠物", "我的宠物", "宠物背包", "宠物背包", "帮助", "宠物帮助"]
        await send_msg_handler(bot, event, "砸蛋", bot.self_id, [msg], title="【砸蛋结果】\n", page=page)
        return

    await handle_send(
        bot,
        event,
        msg,
        md_type="背包",
        k1="宠物",
        v1="我的宠物",
        k2="宠物背包",
        v2="宠物背包",
        k3="帮助",
        v3="宠物帮助",
    )


@pet_set_active.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return

    uid = args.extract_plain_text().strip()
    if not uid:
        await handle_send(bot, event, "用法：出战宠物 宠物UID")
        return

    _, result_msg, _ = set_active_pet(str(user_info["user_id"]), uid)
    await handle_send(
        bot,
        event,
        result_msg,
        md_type="背包",
        k1="宠物",
        v1="我的宠物",
        k2="帮助",
        v2="宠物帮助",
    )


@pet_release.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return

    token = args.extract_plain_text().strip()
    user_id = str(user_info["user_id"])
    pet = remove_pet(user_id, token or None)
    if not pet:
        await handle_send(bot, event, "未找到可放生的宠物。")
        return

    refund_msg = _grant_pet_release_refund(user_id, [pet])

    await handle_send(
        bot,
        event,
        f"已放生：{pet.get('form_name', pet.get('name', '未知宠物'))}（UID:{pet.get('uid')}）。{refund_msg}",
        md_type="背包",
        k1="砸蛋",
        v1="砸蛋",
        k2="宠物",
        v2="我的宠物",
    )


@pet_release_batch.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return

    keyword = args.extract_plain_text().strip()
    if not keyword:
        await handle_send(bot, event, "用法：一键放生 稀有度/宠物名称\n例如：一键放生 常见")
        return

    user_id = str(user_info["user_id"])
    pets, skipped_active = get_releasable_pets_by_keyword(user_id, keyword)
    if not pets:
        msg = f"未找到可一键放生的宠物：{keyword}。"
        if skipped_active:
            msg += "\n匹配到当前出战宠物，已跳过；如需放生出战宠物，请使用【放生宠物】。"
        await handle_send(bot, event, msg)
        return

    refund_item = items.get_data_by_item_id(PET_RELEASE_REFUND_ITEM_ID) or {}
    refund_count = sum(calc_pet_release_refund(pet, refund_item)[0] for pet in pets)
    event_id = str(getattr(event, "message_id", "") or getattr(event, "id", "") or "").strip()
    snapshot_id = ":".join(sorted(str(pet.get("uid", "")) for pet in pets))
    release_result = pet_release_service.release_batch(
        f"pet-release-batch:{event_id or snapshot_id}:{user_id}",
        user_id,
        [{"uid": pet.get("uid"), "total_exp": pet.get("total_exp", 0), "is_active": 0} for pet in pets],
        PET_RELEASE_REFUND_ITEM_ID,
        refund_item.get("name", "一阶天地灵髓"),
        refund_item.get("type", "特殊道具"),
        refund_count,
        XiuConfig().max_goods_num,
    )
    if release_result.status == "inventory_full":
        await handle_send(bot, event, "背包物品已达上限，宠物未放生。")
        return
    if not release_result.succeeded:
        await handle_send(bot, event, "宠物状态已变化，请重新查看宠物背包后再试。")
        return
    refund_msg = _format_pet_release_refund(pets, refund_item, release_result.refund)
    lines = [
        f"已一键放生：{keyword}，共{len(pets)}只。",
        f"稀有度统计：{_summarize_pet_rarities(pets)}",
    ]
    if skipped_active:
        lines.append("已跳过当前出战宠物；如需放生出战宠物，请使用【放生宠物】。")
    for index, pet in enumerate(pets[:10], 1):
        lines.append(
            f"{index}. {pet.get('form_name', pet.get('name', '未知宠物'))}"
            f"（{pet.get('rarity', '常见')}，UID:{pet.get('uid')}）"
        )
    if len(pets) > 10:
        lines.append(f"...其余{len(pets) - 10}只已放生")
    lines.append(refund_msg.strip())

    await handle_send(
        bot,
        event,
        "\n".join(lines),
        md_type="背包",
        k1="宠物背包",
        v1="宠物背包",
        k2="砸蛋",
        v2="砸蛋",
    )


@pet_feed.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return

    user_id = str(user_info["user_id"])
    data = get_pet_doc(user_id)
    if not data.get("active"):
        await handle_send(bot, event, "道友还没有宠物，可先发送【砸蛋】。")
        return
    active_pet = data.get("active")
    active_stars = int(active_pet.get("stars", 1))
    active_max_stars = int(active_pet.get("max_stars", 5))
    if active_stars >= active_max_stars:
        await handle_send(bot, event, f"{active_pet.get('form_name')}已达到当前稀有度上限，无法继续喂食。")
        return
    active_need = exp_to_next_star(active_stars)
    if requires_fusion_for_next_star(active_stars) and int(active_pet.get("exp", 0)) >= active_need:
        await handle_send(
            bot,
            event,
            f"{active_pet.get('form_name')}四☆突破经验已满（{active_need} / {active_need}），请使用【宠物融合 本体UID】突破。",
        )
        return

    item_name, count = _parse_feed_args(args.extract_plain_text())
    if not item_name:
        await handle_send(bot, event, "用法：宠物喂食 材料名 [数量]\n例如：宠物喂食 恒心草 10")
        return

    item_id, item_info = _resolve_item_name(item_name)
    if not item_info:
        await handle_send(bot, event, f"未找到材料：{item_name}")
        return

    ok, reason = validate_pet_feed_item(data.get("active"), item_info)
    if not ok:
        await handle_send(bot, event, reason)
        return

    have = sql_message.goods_num(user_id, item_id)
    if have < count:
        await handle_send(bot, event, f"材料不足：当前仅有{have}个{item_info.get('name', item_name)}。")
        return

    feed_exp = calc_feed_exp(item_info, count)
    if feed_exp <= 0:
        await handle_send(bot, event, "该材料无法提供宠物经验。")
        return

    old_stars = int(active_pet.get("stars", 1))
    old_exp = int(active_pet.get("exp", 0))
    old_total_exp = int(active_pet.get("total_exp", old_exp))
    new_stars, new_exp = old_stars, old_exp + feed_exp
    while new_stars < active_max_stars:
        need = exp_to_next_star(new_stars)
        if new_exp < need:
            break
        if requires_fusion_for_next_star(new_stars):
            new_exp = need
            break
        new_exp -= need
        new_stars += 1
    event_id = str(getattr(event, "message_id", "") or getattr(event, "id", "") or time.time_ns())
    result = pet_feed_service.feed(
        f"pet-feed:{event_id}:{user_id}", user_id, active_pet.get("uid"), item_id, count,
        (old_stars, old_exp, old_total_exp), (new_stars, new_exp, old_total_exp + feed_exp),
    )
    if not result.succeeded:
        messages = {"item_missing": "材料不足，请重新检查背包。", "state_changed": "宠物或背包状态已变化，请重试。"}
        await handle_send(bot, event, messages.get(result.status, "喂食结算失败，请重试。"))
        return
    pet = get_pet_doc(user_id).get("active")
    upgraded = new_stars - old_stars
    form_changes, skill_offers = [], []

    lines = [
        f"喂食成功：消耗{item_info.get('name', item_name)} x{count}",
        f"获得宠物经验：{feed_exp}",
        f"当前宠物：{pet.get('form_name', pet.get('name', '未知宠物'))}",
        f"当前品阶：{format_stars(pet.get('stars', 1))}",
    ]
    next_need = exp_to_next_star(pet.get("stars", 1)) if pet.get("stars", 1) < pet.get("max_stars", 5) else 0
    if next_need:
        current_exp = min(int(pet.get("exp", 0)), next_need)
        exp_label = "四☆突破经验" if requires_fusion_for_next_star(pet.get("stars", 1)) else "当前经验"
        lines.append(f"{exp_label}：{current_exp} / {next_need}")
        if requires_fusion_for_next_star(pet.get("stars", 1)) and current_exp >= next_need:
            lines.append("四☆突破经验已满，请使用【宠物融合 本体UID】突破。")
    if upgraded > 0:
        lines.append(f"提升品阶：+{upgraded}☆")
    if form_changes:
        lines.append("形态进化：" + "、".join(form_changes))
    has_skill_offer = False
    if skill_offers:
        skill_msg = _cache_skill_offer(user_id, pet, skill_offers[-1])
        if skill_msg:
            lines.append(skill_msg)
            has_skill_offer = True

    if has_skill_offer:
        await handle_send(
            bot,
            event,
            "\n".join(lines),
            md_type="背包",
            k1="替换技能",
            v1="替换宠物技能",
            k2="保留技能",
            v2="保留宠物技能",
            k3="宠物",
            v3="我的宠物",
        )
    else:
        await handle_send(
            bot,
            event,
            "\n".join(lines),
            md_type="背包",
            k1="宠物",
            v1="我的宠物",
            k2="宠物背包",
            v2="宠物背包",
            k3="帮助",
            v3="宠物帮助",
        )


@pet_fusion.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return

    tokens = _split_args(args.extract_plain_text())
    if len(tokens) < 1:
        await handle_send(bot, event, "用法：宠物融合 本体UID [本体UID...] [破阶UID]\n默认以当前出战宠物作为主宠，UID顺序随意；仅用于当前品阶达到四个☆时突破为★。")
        return

    ok, result_msg, pet, skill_offer = fuse_pet(str(user_info["user_id"]), tokens)
    has_skill_offer = False
    if ok and pet and skill_offer:
        skill_msg = _cache_skill_offer(str(user_info["user_id"]), pet, skill_offer)
        if skill_msg:
            result_msg = f"{result_msg}\n{skill_msg}"
            has_skill_offer = True
    if has_skill_offer:
        await handle_send(
            bot,
            event,
            result_msg,
            md_type="背包",
            k1="替换技能",
            v1="替换宠物技能",
            k2="保留技能",
            v2="保留宠物技能",
            k3="宠物",
            v3="我的宠物",
        )
    else:
        await handle_send(
            bot,
            event,
            result_msg,
            md_type="背包",
            k1="宠物",
            v1="我的宠物",
            k2="帮助",
            v2="宠物帮助",
        )


@pet_reroll_skill.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return

    user_id = str(user_info["user_id"])
    token = args.extract_plain_text().strip()
    have = sql_message.goods_num(user_id, QIMING_STONE_ID)
    if have <= 0:
        await handle_send(bot, event, "背包中没有启明石，无法重置宠物技能。")
        return

    pet, new_skill = reroll_pet_skill(user_id, token or None)
    if not pet:
        await handle_send(bot, event, "未找到可启明的宠物，请检查宠物UID。")
        return

    sql_message.update_back_j(user_id, QIMING_STONE_ID, num=1)
    PET_SKILL_REPLACE_CACHE.pop(user_id, None)
    await handle_send(
        bot,
        event,
        (
            f"启明成功：消耗启明石 x1\n"
            f"宠物：{pet.get('form_name', pet.get('name', '未知宠物'))}（UID:{pet.get('uid')}）\n"
            f"新技能：{_format_skill_brief(new_skill)}"
        ),
        md_type="背包",
        k1="宠物",
        v1="我的宠物",
        k2="宠物背包",
        v2="宠物背包",
    )


@pet_replace_skill.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return

    user_id = str(user_info["user_id"])
    pending = _pop_skill_offer(user_id)
    if not pending:
        await handle_send(bot, event, "当前没有待替换的宠物技能。")
        return

    data = get_pet_doc(user_id)
    _, _, current_pet = next(((where, key, pet) for where, key, pet in [("active", None, data.get("active"))] + [("bag", i, pet) for i, pet in enumerate(data.get("bag", []))] if pet and str(pet.get("uid")) == str(pending["uid"])), (None, None, None))
    new_skill_id = str(pending["skill"].get("skill_id", ""))
    old_skill_id = str((current_pet or {}).get("skill", {}).get("skill_id", ""))
    event_id = str(getattr(event, "message_id", "") or getattr(event, "id", "") or time.time_ns())
    result = pet_skill_replace_service.replace(f"pet-skill-replace:{event_id}:{user_id}", user_id, pending["uid"], old_skill_id, new_skill_id)
    PET_SKILL_REPLACE_CACHE.pop(user_id, None)
    if not result.succeeded:
        await handle_send(bot, event, "替换失败：未找到对应宠物，或技能类型不匹配。")
        return
    pet = replace_pet_skill(user_id, pending["uid"], pending["skill"])

    await handle_send(
        bot,
        event,
        (
            f"宠物技能已替换。\n"
            f"宠物：{pet.get('form_name', pet.get('name', '未知宠物'))}（UID:{pet.get('uid')}）\n"
            f"当前技能：{_format_skill_brief(pet.get('skill', {}))}"
        ),
        md_type="背包",
        k1="宠物",
        v1="我的宠物",
        k2="宠物背包",
        v2="宠物背包",
    )


@pet_keep_skill.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return

    user_id = str(user_info["user_id"])
    pending = _pop_skill_offer(user_id)
    if not pending:
        await handle_send(bot, event, "当前没有待处理的宠物技能。")
        return

    PET_SKILL_REPLACE_CACHE.pop(user_id, None)
    await handle_send(bot, event, "已保留当前宠物技能。", md_type="背包", k1="宠物", v1="我的宠物")
