import re
import time

from nonebot.params import CommandArg

from ..adapter_compat import Bot, GroupMessageEvent, Message, PrivateMessageEvent
from ..on_compat import on_command
from ..xiuxian_utils.item_json import Items
from ..xiuxian_utils.lay_out import Cooldown
from ..xiuxian_utils.pet_system import (
    EGG_COST,
    QIMING_STONE_ID,
    build_pet_detail,
    calc_feed_exp,
    feed_active_pet,
    format_stars,
    fuse_pet,
    get_pet_doc,
    grant_pet,
    remove_pet,
    replace_pet_skill,
    reroll_pet_skill,
    set_active_pet,
    validate_pet_feed_item,
)
from ..xiuxian_utils.utils import check_user, handle_send, number_to, send_help_message
from ..xiuxian_utils.xiuxian2_handle import XiuxianDateManage

items = Items()
sql_message = XiuxianDateManage()

pet_help = on_command("宠物帮助", aliases={"宠物系统帮助"}, priority=10, block=True)
pet_info = on_command("我的宠物", aliases={"宠物信息"}, priority=10, block=True)
pet_bag = on_command("宠物背包", aliases={"灵宠背包"}, priority=10, block=True)
pet_egg = on_command("砸蛋", aliases={"砸宠物蛋"}, priority=10, block=True)
pet_release = on_command("放生宠物", aliases={"宠物放生"}, priority=10, block=True)
pet_feed = on_command("宠物喂食", priority=10, block=True)
pet_fusion = on_command("宠物融合", priority=10, block=True)
pet_check = on_command("查看宠物", priority=10, block=True)
pet_set_active = on_command("出战宠物", aliases={"携带宠物", "宠物出战"}, priority=10, block=True)
pet_reroll_skill = on_command("宠物启明", aliases={"重置宠物技能", "宠物洗技能"}, priority=10, block=True)
pet_replace_skill = on_command("替换宠物技能", aliases={"确认替换宠物技能", "宠物技能替换"}, priority=10, block=True)
pet_keep_skill = on_command("保留宠物技能", aliases={"放弃替换宠物技能", "取消替换宠物技能"}, priority=10, block=True)

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
    lines = ["☆------我的宠物------☆"]
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
    bag = data.get("bag", [])
    if not bag:
        return "宠物背包为空。"

    total_pages = max(1, (len(bag) + per_page - 1) // per_page)
    page = max(1, min(int(page), total_pages))
    start = (page - 1) * per_page
    end = start + per_page

    lines = ["☆------宠物背包------☆"]
    for pet in bag[start:end]:
        lines.append(
            f"- {pet.get('form_name', pet.get('name', '未知宠物'))}"
            f" | {pet.get('rarity', '常见')}"
            f" | {pet.get('type', '攻击')}"
            f" | {format_stars(pet.get('stars', 1))}"
            f" | UID:{pet.get('uid', '')}"
        )

    lines.append(f"\n第 {page}/{total_pages} 页")
    if page < total_pages:
        lines.append(f"输入 宠物背包 {page + 1} 查看下一页")
    lines.append("可用命令：查看宠物 UID / 出战宠物 UID / 放生宠物 UID")

    return "\n".join(lines)


@pet_help.handle(parameterless=[Cooldown(cd_time=3)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    msg = f"""
【宠物系统帮助】

1）砸蛋：
   发送：砸蛋
   消耗：{number_to(EGG_COST)}灵石
   规则：没有出战宠物时自动出战，否则进入宠物背包。
   宠物蛋：发送【道具使用 宠物蛋名 [数量]】可孵化指定稀有度宠物。

2）查看宠物：
   发送：我的宠物（查看当前出战宠物）
   发送：宠物背包 [页码]（查看未出战宠物）
   发送：查看宠物 宠物UID
   每只宠物初始获得1个基础通用技能，技能受宠物稀有度、形态、品阶影响。
   技能效果包含直伤、多段、持续伤害、控制、破盾、增益、护盾、净化、反伤等。

3）切换出战：
   发送：出战宠物 宠物UID

4）喂食升级：
   发送：宠物喂食 材料名 [数量]
   例如：宠物喂食 恒心草 10
   可喂食：药材 / 一至五阶天地灵髓
   规则：1★=5☆，天地灵髓只能喂食对应★级及以下宠物。

5）宠物启明：
   发送：宠物启明 [宠物UID]
   消耗：启明石 x1
   作用：为指定宠物重新随机获得1个基础技能，不填UID时默认当前出战宠物。

6）融合进阶：
   发送：宠物融合 主宠UID,本体UID1,本体UID2
   规则：
   - 主宠和本体均通过UID区分，同名宠物不会冲突
   - 本体必须在宠物背包中
   - 本体必须与主宠同名
   - 主宠品阶越高，消耗本体越多
   - 每提升到整★时会随机领悟候选技能，可选择是否替换

7）放生：
   发送：放生宠物 宠物UID
   不填UID时默认放生当前出战宠物
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
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return

    user_id = str(user_info["user_id"])
    if int(user_info.get("stone", 0)) < EGG_COST:
        await handle_send(
            bot,
            event,
            f"砸蛋需要{number_to(EGG_COST)}灵石，道友当前灵石不足。",
            md_type="背包",
            k1="灵石",
            v1="灵石",
            k2="帮助",
            v2="宠物帮助",
        )
        return

    try:
        pet, location = grant_pet(user_id)
    except Exception as e:
        await handle_send(bot, event, f"砸蛋失败：{e}")
        return

    if not pet:
        await handle_send(bot, event, "砸蛋失败：未能生成宠物。")
        return

    sql_message.update_ls(user_id, EGG_COST, 2)

    location_msg = "已自动出战" if location == "active" else "已放入宠物背包，可发送【出战宠物 UID】切换出战"
    msg = (
        f"砸蛋成功！消耗{number_to(EGG_COST)}灵石。\n"
        f"获得宠物：{pet.get('form_name', pet.get('name', '未知宠物'))}\n"
        f"稀有度：{pet.get('rarity')} | 种族：{pet.get('race')} | 类型：{pet.get('type')}\n"
        f"初始技能：{pet.get('skill', {}).get('name', '未知技能')}\n"
        f"UID：{pet.get('uid')}\n"
        f"{location_msg}"
    )
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
    pet = remove_pet(str(user_info["user_id"]), token or None)
    if not pet:
        await handle_send(bot, event, "未找到可放生的宠物。")
        return

    await handle_send(
        bot,
        event,
        f"已放生：{pet.get('form_name', pet.get('name', '未知宠物'))}（UID:{pet.get('uid')}）。",
        md_type="背包",
        k1="砸蛋",
        v1="砸蛋",
        k2="宠物",
        v2="我的宠物",
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

    pet, upgraded, form_changes, skill_offers = feed_active_pet(user_id, feed_exp)
    if not pet:
        await handle_send(bot, event, "喂食失败：未找到出战宠物。")
        return

    if upgraded <= 0 and pet.get("stars", 1) >= pet.get("max_stars", 5):
        await handle_send(bot, event, f"{pet.get('form_name')}已达到当前稀有度上限，无法继续喂食。")
        return

    sql_message.update_back_j(user_id, item_id, num=count)

    lines = [
        f"喂食成功：消耗{item_info.get('name', item_name)} x{count}",
        f"获得宠物经验：{feed_exp}",
        f"当前宠物：{pet.get('form_name', pet.get('name', '未知宠物'))}",
        f"当前品阶：{format_stars(pet.get('stars', 1))}",
    ]
    if upgraded > 0:
        lines.append(f"提升品阶：+{upgraded}☆")
    if form_changes:
        lines.append("形态进化：" + "、".join(form_changes))
    if skill_offers:
        skill_msg = _cache_skill_offer(user_id, pet, skill_offers[-1])
        if skill_msg:
            lines.append(skill_msg)

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
    if len(tokens) < 2:
        await handle_send(bot, event, "用法：宠物融合 主宠UID,本体UID1,本体UID2")
        return

    ok, result_msg, pet, skill_offer = fuse_pet(str(user_info["user_id"]), tokens[0], tokens[1:])
    if ok and pet and skill_offer:
        skill_msg = _cache_skill_offer(str(user_info["user_id"]), pet, skill_offer)
        if skill_msg:
            result_msg = f"{result_msg}\n{skill_msg}"
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

    pet = replace_pet_skill(user_id, pending["uid"], pending["skill"])
    PET_SKILL_REPLACE_CACHE.pop(user_id, None)
    if not pet:
        await handle_send(bot, event, "替换失败：未找到对应宠物，或技能类型不匹配。")
        return

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
