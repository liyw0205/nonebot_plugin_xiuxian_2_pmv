from copy import deepcopy
import time

from ..on_compat import on_command
from nonebot.params import CommandArg

from ...paths import get_paths
from ..adapter_compat import (
    Bot,
    Message,
    GroupMessageEvent,
    PrivateMessageEvent,
    MessageSegment,
)
from ..messaging.delivery import delivery_service
from ..xiuxian_config import XiuConfig
from ..xiuxian_utils.item_json import Items
from ..xiuxian_utils.utils import check_user, handle_send, send_msg_handler, send_help_message
from ..xiuxian_utils.xiuxian2_handle import PlayerDataManager, XiuxianDateManage, calc_accessory_effects
from ..xiuxian_utils.lay_out import Cooldown
from .accessory_helpers import *
from .accessory_transaction_service import AccessoryTransactionService

items = Items()
sql_message = XiuxianDateManage()
player_data_manager = PlayerDataManager()
accessory_transaction_service = AccessoryTransactionService(
    get_paths().game_db, get_paths().player_db
)


def _accessory_operation_id(event, action, user_id, target):
    event_id = str(
        getattr(event, "message_id", "") or getattr(event, "id", "") or ""
    ).strip()
    if event_id:
        return f"accessory:{event_id}:{action}:{user_id}:{target}"
    return f"accessory:{action}:{user_id}:{target}:{time.time_ns()}"

my_accessory = on_command("我的饰品", priority=10, block=True)
accessory_bag = on_command("饰品背包", priority=10, block=True)
accessory_collection = on_command(
    "饰品图鉴",
    aliases={
        "饰品套装", "饰品收集",
        "烈阳套", "烈阳套装", "烈阳图鉴",
        "玄渊套", "玄渊套装", "玄渊图鉴",
        "天衡套", "天衡套装", "天衡图鉴",
        "星痕套", "星痕套装", "星痕图鉴",
        "龙魄套", "龙魄套装", "龙魄图鉴",
        "踏风套", "踏风套装", "踏风图鉴",
    },
    priority=10,
    block=True,
)
equip_accessory = on_command("装备饰品", priority=10, block=True)
unequip_accessory = on_command("卸下饰品", priority=10, block=True)
wash_accessory = on_command("饰品洗练", priority=10, block=True)
lock_accessory_affix = on_command("饰品锁定", aliases={"锁定饰品词条", "饰品词条锁定"}, priority=10, block=True)
unlock_accessory_affix = on_command("饰品解锁", aliases={"解锁饰品词条", "饰品词条解锁"}, priority=10, block=True)
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




# ========== 命令 ==========
@accessory_help.handle(parameterless=[Cooldown(cd_time=3)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    msg = """
**饰品系统帮助**

---

1.  **基础操作**：饰品基础帮助
    > 查看、背包、详情、装备、卸下

2.  **成长强化**：饰品成长帮助
    > 洗练、升阶、材料规则

3.  **整理预设**：饰品整理帮助
    > 分解、快速分解、预设、快速装备

4.  **查看信息**：我的饰品 / 饰品背包
    > 套装图鉴、收集进度、已激活效果
""".strip()

    await send_help_message(
        bot, event, msg,
        k1="基础帮助", v1="饰品基础帮助",
        k2="成长帮助", v2="饰品成长帮助",
        k3="饰品图鉴", v3="饰品图鉴",
        k4="饰品背包", v4="饰品背包"
    )


@accessory_basic_help.handle(parameterless=[Cooldown(cd_time=3)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    msg = """
**饰品基础帮助**

---

**查看已装备饰品**
   发送：我的饰品

**查看饰品背包**
   发送：饰品背包 [页码]
   例如：饰品背包 2

**查看单件饰品详情**
   发送：查看饰品 饰品UID
   例如：查看饰品 acc_1730000000000_1234

**查看套装图鉴**
   发送：饰品图鉴 [套装名]
   例如：饰品图鉴 烈阳

**装备饰品**
   发送：装备饰品 饰品UID
   例如：装备饰品 acc_1730000000000_1234

**卸下饰品**
   发送：卸下饰品 部位
   可用部位：手镯 / 戒指 / 手环 / 项链
""".strip()

    await send_help_message(
        bot, event, msg,
        k1="我的饰品", v1="我的饰品",
        k2="饰品背包", v2="饰品背包",
        k3="饰品图鉴", v3="饰品图鉴",
        k4="主帮助", v4="饰品帮助"
    )


@accessory_growth_help.handle(parameterless=[Cooldown(cd_time=3)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    msg = """
**饰品成长帮助**

---

**洗练饰品**
   发送：饰品洗练 饰品UID
   - 消耗【洗练石】随品阶增加
   - 一至三阶饰品2条词条，四至五阶饰品3条词条
   - 可锁定词条后洗练，锁定1条消耗翻2倍，锁定2条消耗翻3倍
   - 不能锁定全部词条，至少保留1条参与洗练
   - 每件饰品独立洗练次数
   - 150次保底：词条值固定上限，仅词条类型变化

**锁定 / 解锁词条**
   发送：饰品锁定 饰品UID 词条序号
   例如：饰品锁定 acc_1730000000000_1234 1 2
   发送：饰品解锁 饰品UID 词条序号
   发送：饰品解锁 饰品UID 全部

**饰品升阶**
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
   - 升到四阶后补至3条词条；升阶会重置洗练次数（wash_count=0）
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
**饰品整理帮助**

---

**单件分解**
   命令：饰品分解 饰品UID
   限制：已装备饰品不能直接分解，请先卸下

**快速分解**
   命令：快速分解饰品 类型 品阶
   类型：全部 / 烈阳 / 玄渊 / 天衡 / 星痕 / 龙魄 / 手镯 / 戒指 / 手环 / 项链
   品阶：全部 / 1~5 / 一阶~五阶
   安全规则：
   - 当“类型=全部”或“品阶=全部”时，自动忽略4/5阶

**饰品预设**
   发送：饰品预设 1/2/3
   - 保存当前已装备饰品到对应预设位
   - 若原有记录存在，则自动覆盖
   - 直接发送【饰品预设】可查看所有预设

**快速装备饰品**
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

    lines = ["【我的饰品】"]

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
    if effect.get("speed", 0):
        bonus_lines.append(f"速度 +{effect['speed']:.0f}点")

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
        "speed_pct": "速度提升",
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
    capacity_text = f"容量：{get_accessory_total_count(data)}/{ACCESSORY_BAG_LIMIT}"

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
            next_cmd=f"饰品背包 {current_page + 1}",
            capacity_text=capacity_text,
        )
        fallback_text = _build_accessory_plain_text(
            title=f"{user_info.get('user_name', '道友')}的饰品背包",
            sections=page_sections,
            current_page=current_page,
            total_pages=total_pages,
            next_cmd=f"饰品背包 {current_page + 1}",
            capacity_text=capacity_text,
        )

        try:
            await delivery_service.reply(bot, event, MessageSegment.markdown(bot, md_text))
        except Exception:
            await handle_send(bot, event, fallback_text)
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

    title = [f"【{user_info.get('user_name', '道友')}的饰品背包】"]
    lines = [capacity_text]
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


@accessory_collection.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return

    user_id = str(user_info["user_id"])
    arg = args.extract_plain_text().strip()
    set_filter = _resolve_collection_set_filter(arg)
    if not arg:
        event_text = _extract_plain_text_from_event(event)
        direct_filter = _resolve_collection_set_filter(event_text)
        if direct_filter:
            arg = event_text
            set_filter = direct_filter
    if arg and set_filter is None and arg not in {"全部", "总览"}:
        await handle_send(
            bot,
            event,
            f"未识别的套装：{arg}\n可用：{'/'.join(ACCESSORY_SETS)}",
            **_accessory_collection_buttons(None),
        )
        return

    collection = _build_accessory_collection(user_id)
    summary = _summarize_accessory_collection(collection)
    details = _build_accessory_set_details(user_id)
    total_owned = sum(info["owned_total"] for info in details.values())

    if set_filter:
        detail = details[set_filter]
        equipped_total = int(detail.get("equipped_total", 0))
        active_lines = _format_active_set_bonus_lines(set_filter, equipped_total)

        lines = [f"【{set_filter}套装图鉴】"]
        lines.append("套装效果：")
        lines.extend([f" - {line}" for line in _format_set_bonus_lines(set_filter)])
        lines.append("")
        lines.append("当前激活：")
        lines.append(f" - 已装备{equipped_total}/4件：{_format_slots(detail.get('equipped_slots', []), '暂无')}")
        if active_lines:
            lines.extend([f" - 已激活{line}" for line in active_lines])
        else:
            lines.append(" - 暂无激活效果")
        lines.append(f" - {_next_set_bonus_hint(set_filter, detail)}")
        lines.append("")
        lines.append("持有整理：")
        lines.append(f" - 已装备件：{_format_owned_slot_counts(detail.get('equipped_by_slot', {}))}")
        lines.append(f" - 背包件：{_format_owned_slot_counts(detail.get('bag_by_slot', {}))}")
        lines.append(f" - 重复件：{int(detail.get('duplicate_total', 0))}件")
        lines.append(f" - 缺失部位：{_format_slots(detail.get('missing_slots', []))}")
        lines.append("")
        lines.append("收集进度：")
        for quality in QUALITY_RANGE:
            slot_map = collection[set_filter][quality]
            slot_text = []
            for slot in SLOTS:
                owned = slot_map.get(slot, [])
                slot_text.append(f"{slot}{_format_collection_slot_detail(owned)}")
            completed = all(slot_map.get(slot) for slot in SLOTS)
            state = "已集齐" if completed else "未集齐"
            lines.append(f"{quality_to_cn(quality)}：{state} | {'、'.join(slot_text)}")
        lines.append("")
        if detail.get("owned_total", 0) <= 0:
            lines.append("当前还没有该套装饰品，可先查看饰品背包或继续获取饰品。")
        else:
            lines.append("可执行操作：饰品背包、我的饰品、快速装备饰品")
    else:
        total_complete = sum(len(info["complete_qualities"]) for info in summary.values())
        total_sets = len(ACCESSORY_SETS) * len(QUALITY_RANGE)
        lines = [
            "【饰品套装图鉴】",
            f"完整套装：{total_complete}/{total_sets}",
            f"当前持有：{total_owned}件",
            "",
            "套装总览：",
        ]
        if total_owned <= 0:
            lines.append("暂无饰品记录，所有套装部位均未收集。")
        for set_name in ACCESSORY_SETS:
            info = summary[set_name]
            complete_text = (
                "、".join(quality_to_cn(q) for q in info["complete_qualities"])
                if info["complete_qualities"]
                else "暂无完整阶数"
            )
            best_quality = info["best_quality"]
            best_text = quality_to_cn(best_quality) if best_quality else "未集齐"
            progress_text = _format_set_progress(details[set_name])
            lines.append(
                f"{set_name}：{progress_text}，完整阶数：{complete_text}，最高完整：{best_text}"
            )
        lines.append("")
        lines.append("发送【饰品图鉴 套装名】查看详细部位，例如：饰品图鉴 烈阳")

    await handle_send(
        bot,
        event,
        "\n".join(lines),
        **_accessory_collection_buttons(set_filter),
    )

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
    wash_count = max(0, int(target.get("wash_count", 0) or 0))
    desc = item_info.get("desc", "暂无介绍")

    affixes = target.get("affixes", [])
    affix_count = len(affixes) if isinstance(affixes, list) else 0
    locked_indexes = _normalize_locked_affixes(target, affix_count)
    if not affixes:
        affix_lines = ["- 无词条"]
    else:
        affix_lines = []
        for idx, af in enumerate(affixes):
            t = af.get("type", "未知")
            lock_tag = "（已锁定）" if idx in locked_indexes else ""
            affix_lines.append(f"- {idx + 1}. {t}：{_format_affix_value(af)}{lock_tag}")

    next_wash_need = _wash_stone_need(quality, len(locked_indexes))
    target_affix_count = _target_affix_count_for_quality(quality)

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
        f"【饰品详情】\n"
        f"名称：{name}\n"
        f"UID：{uid}\n"
        f"品阶：{quality_to_cn(quality)}\n"
        f"部位：{part}\n"
        f"套装：{set_type}\n"
        f"状态：{where}\n"
        f"当前洗练次数：{wash_count}/150\n"
        f"词条槽位：{affix_count}/{target_affix_count}\n"
        f"锁定词条：{_format_locked_positions(locked_indexes)}\n"
        f"下次洗练消耗：{WASH_STONE_NAME}x{next_wash_need}\n"
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

@lock_accessory_affix.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return

    parts = args.extract_plain_text().split()
    if len(parts) < 2:
        await handle_send(bot, event, "用法：饰品锁定 饰品UID 词条序号\n例如：饰品锁定 acc_1730000000000_1234 1 2")
        return

    uid = parts[0].strip()
    index_tokens = parts[1:]
    user_id = str(user_info["user_id"])
    operation_id = _accessory_operation_id(event, "lock", user_id, uid)
    replay = accessory_transaction_service.replay(operation_id, "lock")
    if replay is not None and replay.accessory is not None:
        target = replay.accessory
        q = max(1, min(5, int(target.get("quality", 1))))
        locked = _normalize_locked_affixes(target)
        result_msg = (
            f"已锁定：{target.get('name', '未知饰品')}\n"
            f"锁定词条：{_format_locked_positions(locked)}\n"
            f"下次洗练消耗：{WASH_STONE_NAME}x{_wash_stone_need(q, len(locked))}"
        )
    else:
        data = _get_data(user_id)
        _, _, target = _find_accessory_anywhere(data, uid)
        if not target:
            result_msg = "锁定失败：未找到饰品"
        else:
            affixes = target.get("affixes", [])
            affix_count = len(affixes) if isinstance(affixes, list) else 0
            if affix_count <= 0:
                result_msg = "锁定失败：该饰品没有可锁定词条"
            else:
                indexes, err = _parse_affix_indexes(index_tokens, affix_count)
                if err:
                    result_msg = f"锁定失败：{err}"
                else:
                    q = max(1, min(5, int(target.get("quality", 1))))
                    target_count = _target_affix_count_for_quality(q)
                    current_locked = _normalize_locked_affixes(target, affix_count)
                    new_locked = sorted(set(current_locked + indexes))
                    if len(new_locked) >= target_count:
                        result_msg = f"锁定失败：{quality_to_cn(q)}最多锁定{target_count - 1}条，至少保留1条参与洗练"
                    else:
                        result = accessory_transaction_service.set_affix_locks(
                            operation_id, "lock", user_id, uid,
                            deepcopy(target), new_locked,
                        )
                        if not result.succeeded or result.accessory is None:
                            result_msg = "锁定失败：饰品状态已变化，请重新查看后再试"
                        else:
                            updated = result.accessory
                            result_msg = (
                                f"已锁定：{updated.get('name', '未知饰品')}\n"
                                f"锁定词条：{_format_locked_positions(new_locked)}\n"
                                f"下次洗练消耗：{WASH_STONE_NAME}x{_wash_stone_need(q, len(new_locked))}"
                            )

    await handle_send(
        bot, event, result_msg,
        md_type="背包", k1="查看", v1=f"查看饰品 {uid}", k2="洗练", v2=f"饰品洗练 {uid}"
    )

@unlock_accessory_affix.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return

    parts = args.extract_plain_text().split()
    if len(parts) < 2:
        await handle_send(bot, event, "用法：饰品解锁 饰品UID 词条序号/全部\n例如：饰品解锁 acc_1730000000000_1234 1\n或：饰品解锁 acc_1730000000000_1234 全部")
        return

    uid = parts[0].strip()
    index_tokens = parts[1:]
    unlock_all = any(str(token).strip() in {"全部", "全解", "all", "ALL"} for token in index_tokens)
    user_id = str(user_info["user_id"])
    operation_id = _accessory_operation_id(event, "unlock", user_id, uid)
    replay = accessory_transaction_service.replay(operation_id, "unlock")
    if replay is not None and replay.accessory is not None:
        target = replay.accessory
        q = max(1, min(5, int(target.get("quality", 1))))
        locked = _normalize_locked_affixes(target)
        result_msg = (
            f"已解锁：{target.get('name', '未知饰品')}\n"
            f"锁定词条：{_format_locked_positions(locked)}\n"
            f"下次洗练消耗：{WASH_STONE_NAME}x{_wash_stone_need(q, len(locked))}"
        )
    else:
        data = _get_data(user_id)
        _, _, target = _find_accessory_anywhere(data, uid)
        if not target:
            result_msg = "解锁失败：未找到饰品"
        else:
            affixes = target.get("affixes", [])
            affix_count = len(affixes) if isinstance(affixes, list) else 0
            current_locked = _normalize_locked_affixes(target, affix_count)
            if not current_locked:
                result_msg = "该饰品当前没有锁定词条"
            else:
                if unlock_all:
                    new_locked = []
                    err = ""
                else:
                    indexes, err = _parse_affix_indexes(index_tokens, affix_count)
                    remove_set = set(indexes or [])
                    new_locked = [idx for idx in current_locked if idx not in remove_set]
                if err:
                    result_msg = f"解锁失败：{err}"
                else:
                    result = accessory_transaction_service.set_affix_locks(
                        operation_id, "unlock", user_id, uid,
                        deepcopy(target), new_locked,
                    )
                    if not result.succeeded or result.accessory is None:
                        result_msg = "解锁失败：饰品状态已变化，请重新查看后再试"
                    else:
                        updated = result.accessory
                        q = max(1, min(5, int(updated.get("quality", 1))))
                        result_msg = (
                            f"已解锁：{updated.get('name', '未知饰品')}\n"
                            f"锁定词条：{_format_locked_positions(new_locked)}\n"
                            f"下次洗练消耗：{WASH_STONE_NAME}x{_wash_stone_need(q, len(new_locked))}"
                        )

    await handle_send(
        bot, event, result_msg,
        md_type="背包", k1="查看", v1=f"查看饰品 {uid}", k2="洗练", v2=f"饰品洗练 {uid}"
    )

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
    operation_id = _accessory_operation_id(event, "wash", user_id, uid)
    replay = accessory_transaction_service.replay(operation_id, "wash")
    if replay is not None and replay.accessory is not None:
        updated = replay.accessory
        q2 = max(1, min(5, int(updated.get("quality", 1))))
        locked = _normalize_locked_affixes(updated)
        wash_count = int(updated.get("wash_count", 0))
        need = abs(replay.stone_delta)
        tip = "（已触发150次保底：词条数值固定上限）" if wash_count >= 150 else ""
        await handle_send(
            bot,
            event,
            f"洗练完成：{updated.get('name','未知饰品')}（{quality_to_cn(q2)}）\n"
            f"消耗{WASH_STONE_NAME}：{need}个\n"
            f"锁定词条：{_format_locked_positions(locked)}\n"
            f"当前洗练次数：{wash_count}/150 {tip}",
            md_type="背包", k1="饰品", v1="饰品背包", k2="查看", v2="我的饰品"
        )
        return

    data = _get_data(user_id)
    _, _, target = _find_accessory_anywhere(data, uid)
    if not target:
        await handle_send(bot, event, "未找到该饰品UID")
        return

    q = max(1, min(5, int(target.get("quality", 1))))
    affixes = target.get("affixes", [])
    affix_count = len(affixes) if isinstance(affixes, list) else 0
    locked_indexes = _normalize_locked_affixes(target, affix_count)
    target_count = _target_affix_count_for_quality(q)
    if len(locked_indexes) >= target_count:
        await handle_send(bot, event, f"洗练失败：{quality_to_cn(q)}最多锁定{target_count - 1}条，至少保留1条参与洗练")
        return

    need = _wash_stone_need(q, len(locked_indexes))
    have = sql_message.goods_num(user_id, WASH_STONE_ID)

    if have < need:
        await handle_send(
            bot, event,
            f"洗练失败：{WASH_STONE_NAME}不足（需要{need}个，当前{have}个）",
            md_type="背包", k1="背包", v1="我的背包", k2="饰品", v2="饰品背包"
        )
        return

    expected_accessory = deepcopy(target)

    def _reroll(t):
        q2 = max(1, min(5, int(t.get("quality", 1))))
        old_affixes = t.get("affixes", [])
        if not isinstance(old_affixes, list):
            old_affixes = []
        locked = _normalize_locked_affixes(t, len(old_affixes))
        target_cnt = _target_affix_count_for_quality(q2)
        if len(locked) >= target_cnt:
            raise ValueError("all affixes are locked")

        wash_count = int(t.get("wash_count", 0)) + 1
        t["wash_count"] = wash_count

        pity_reached = wash_count >= 150
        t["affixes"] = _reroll_affixes_preserving_locked(
            q2,
            old_affixes,
            locked,
            pity_reached=pity_reached
        )
        _set_locked_affixes(t, _normalize_locked_affixes(t, len(t["affixes"])))
        return t

    result = accessory_transaction_service.wash(
        operation_id,
        user_id,
        uid,
        expected_accessory,
        have,
        WASH_STONE_ID,
        need,
        _reroll,
    )

    if result.status == "item_insufficient":
        text = f"洗练失败：{WASH_STONE_NAME}不足"
    elif result.status in {"accessory_missing", "state_changed"}:
        text = "洗练失败：饰品或材料状态已变化，请重新查看后再试"
    elif not result.succeeded or result.accessory is None:
        text = "洗练失败"
    else:
        updated = result.accessory
        q2 = max(1, min(5, int(updated.get("quality", 1))))
        locked = _normalize_locked_affixes(updated)
        wash_count = int(updated.get("wash_count", 0))
        tip = "（已触发150次保底：词条数值固定上限）" if wash_count >= 150 else ""
        text = (
            f"洗练完成：{updated.get('name','未知饰品')}（{quality_to_cn(q2)}）\n"
            f"消耗{WASH_STONE_NAME}：{need}个\n"
            f"锁定词条：{_format_locked_positions(locked)}\n"
            f"当前洗练次数：{wash_count}/150 {tip}"
        )

    await handle_send(
        bot, event, text,
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
    operation_id = _accessory_operation_id(event, "decompose", user_id, uid)
    replay = accessory_transaction_service.replay(operation_id, "decompose")
    if replay is not None and replay.accessory is not None:
        decomposed = replay.accessory
        q = max(1, min(5, int(decomposed.get("quality", 1))))
        await handle_send(
            bot, event,
            f"已分解：{decomposed.get('name', '未知饰品')}（{quality_to_cn(q)}）\n获得{WASH_STONE_NAME}：{replay.stone_delta}个",
            md_type="背包", k1="饰品", v1="饰品背包", k2="背包", v2="我的背包"
        )
        return
    data = _get_data(user_id)
    _, target = _find_accessory_in_bag(data, uid)
    if not target:
        await handle_send(bot, event, "分解失败：未在饰品背包中找到该UID（已装备饰品请先卸下）")
        return
    q = max(1, min(5, int(target.get("quality", 1))))
    gain = ACCESSORY_DECOMPOSE_GAIN.get(q, 1)
    result = accessory_transaction_service.decompose(
        operation_id,
        user_id,
        uid,
        deepcopy(target),
        WASH_STONE_ID,
        WASH_STONE_NAME,
        gain,
        int(XiuConfig().max_goods_num),
    )

    if result.status == "inventory_full":
        await handle_send(bot, event, f"分解失败：{WASH_STONE_NAME}已达背包上限")
        return
    if not result.succeeded or result.accessory is None:
        await handle_send(bot, event, "分解失败：饰品状态已变化，请重新查看后再试")
        return

    decomposed = result.accessory
    await handle_send(
        bot, event,
        f"已分解：{decomposed.get('name', '未知饰品')}（{quality_to_cn(q)}）\n获得{WASH_STONE_NAME}：{gain}个",
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

    selected_uids = [str(acc.get("uid", "")) for acc in hit]
    target_key = f"{t}:{q_text}:{','.join(selected_uids)}"
    result = accessory_transaction_service.batch_decompose(
        _accessory_operation_id(event, "batch-decompose", user_id, target_key),
        user_id,
        deepcopy(bag),
        selected_uids,
        WASH_STONE_ID,
        WASH_STONE_NAME,
        total_gain,
        int(XiuConfig().max_goods_num),
    )
    if result.status == "inventory_full":
        await handle_send(bot, event, f"快速分解失败：{WASH_STONE_NAME}已达背包上限")
        return
    if not result.succeeded:
        await handle_send(bot, event, "快速分解失败：饰品状态已变化，请重新查看后再试")
        return

    await handle_send(
        bot, event,
        f"快速分解完成：{result.affected}件\n筛选：{t} / {q_text}\n获得{WASH_STONE_NAME}：{result.stone_delta}个",
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
    material_uids = list(dict.fromkeys(parts[1:]))

    if part not in SLOTS:
        await handle_send(bot, event, f"部位错误，可用：{'/'.join(SLOTS)}")
        return

    user_id = str(user_info["user_id"])
    operation_id = _accessory_operation_id(
        event,
        "upgrade",
        user_id,
        f"{part}:{','.join(material_uids)}",
    )
    result = accessory_transaction_service.replay(operation_id, "upgrade")
    if result is None:
        data = _get_data(user_id)
        equipped = data.get("equipped", {})
        bag = data.get("bag", [])
        main_acc = equipped.get(part)
        if not main_acc:
            await handle_send(bot, event, f"{part}当前未装备饰品，无法升阶")
            return

        main_q = int(main_acc.get("quality", 1))
        if main_q >= 5:
            await handle_send(bot, event, "该饰品已达最高五阶，无法继续升阶")
            return
        need_cnt = _get_upgrade_cost(main_q)
        if len(material_uids) < need_cnt:
            await handle_send(bot, event, f"材料数量不足：当前升阶需 {need_cnt} 个材料UID")
            return

        uid_to_item = {
            str(accessory.get("uid", "")): accessory for accessory in bag
        }
        for material_uid in material_uids:
            material = uid_to_item.get(str(material_uid))
            if material is None:
                await handle_send(bot, event, f"材料UID无效或不在背包中：{material_uid}")
                return
            if not _is_same_accessory_for_upgrade(main_acc, material):
                await handle_send(bot, event, f"材料不匹配：{material_uid} 不是同阶同款饰品")
                return

        selected_uids = material_uids[:need_cnt]
        upgraded = deepcopy(main_acc)
        upgraded["quality"] = main_q + 1
        upgraded["wash_count"] = 0
        upgraded["affixes"] = _fit_affixes_to_quality(
            main_q + 1, upgraded.get("affixes", [])
        )
        _set_locked_affixes(
            upgraded,
            _normalize_locked_affixes(
                upgraded, len(upgraded.get("affixes", []))
            ),
        )
        result = accessory_transaction_service.upgrade(
            operation_id,
            user_id,
            part,
            deepcopy(equipped),
            deepcopy(bag),
            selected_uids,
            upgraded,
        )
        if not result.succeeded:
            messages = {
                "accessory_missing": f"{part}当前未装备饰品，无法升阶",
                "max_quality": "该饰品已达最高五阶，无法继续升阶",
                "material_missing": "升阶材料已变化，请重新查看饰品背包",
                "material_mismatch": "升阶材料不再满足同阶同款要求",
                "invalid_plan": "升阶结果校验失败，本次未消耗材料",
            }
            await handle_send(
                bot,
                event,
                messages.get(result.status, "饰品状态已变化，请重新查看后再试"),
            )
            return

    if result.accessory is None:
        await handle_send(bot, event, "饰品升阶结果缺失，请联系管理员处理")
        return
    main_acc = result.accessory
    new_q = int(main_acc.get("quality", 1))
    old_q = new_q - 1
    need_cnt = result.affected

    await handle_send(
        bot, event,
        f"升阶成功：{main_acc.get('name', '未知饰品')} {quality_to_cn(old_q)} → {quality_to_cn(new_q)}\n"
        f"消耗材料：{need_cnt}件同阶同款饰品\n"
        f"当前词条已保留，{quality_to_cn(new_q)}词条数为{_target_affix_count_for_quality(new_q)}条，洗练次数已重置",
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
            "【饰品预设】",
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
    operation_id = _accessory_operation_id(
        event, "save_preset", user_id, str(preset_idx)
    )
    save_result = accessory_transaction_service.replay(
        operation_id, "save_preset"
    )
    if save_result is None:
        data = _get_data(user_id)
        save_result = accessory_transaction_service.save_preset(
            operation_id,
            user_id,
            preset_idx,
            deepcopy(data.get("equipped", {})),
            deepcopy(_get_accessory_preset(user_id, preset_idx)),
        )
        if not save_result.succeeded:
            await handle_send(
                bot, event, "饰品装备或预设状态已变化，请重新查看后再保存"
            )
            return

    details = save_result.details or {}
    had_old = bool(details.get("had_old"))

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
    operation_id = _accessory_operation_id(
        event, "quick_equip_preset", user_id, str(preset_idx)
    )
    equip_result = accessory_transaction_service.replay(
        operation_id, "quick_equip_preset"
    )
    if equip_result is None:
        data = _get_data(user_id)
        preset = _get_accessory_preset(user_id, preset_idx)
        if not any(preset.get(slot) for slot in SLOTS):
            await handle_send(bot, event, f"饰品预设{preset_idx}为空，无法快速装备。")
            return
        equip_result = accessory_transaction_service.quick_equip_preset(
            operation_id,
            user_id,
            preset_idx,
            deepcopy(data.get("equipped", {})),
            deepcopy(data.get("bag", [])),
            deepcopy(preset),
        )
        if not equip_result.succeeded:
            messages = {
                "preset_empty": f"饰品预设{preset_idx}为空，无法快速装备。",
                "state_changed": "饰品装备、背包或预设状态已变化，请重新查看后再试",
            }
            await handle_send(
                bot,
                event,
                messages.get(equip_result.status, "快速装备饰品失败，请稍后重试"),
            )
            return

    result = equip_result.details or {}
    equipped_items = result.get("equipped", [])
    skipped_items = result.get("skipped", [])
    missing_items = result.get("missing", [])

    msg_lines = [f"【快速装备饰品{preset_idx}】"]

    if equipped_items:
        msg_lines.append("【成功装备】")
        msg_lines.extend([
            f" - {item['slot']}→{item['name']}" for item in equipped_items
        ])

    if skipped_items:
        msg_lines.append("【跳过】")
        for item in skipped_items:
            if item["reason"] == "already_equipped":
                msg_lines.append(f" - {item['slot']}已是目标饰品")
            else:
                msg_lines.append(
                    f" - {item.get('name', '未知饰品')}部位不匹配，跳过"
                )

    if missing_items:
        msg_lines.append("【失效记录已清理】")
        msg_lines.extend([
            f" - {item['slot']}预设饰品已不存在"
            for item in missing_items
        ])

    await handle_send(
        bot, event, "\n".join(msg_lines),
        md_type="背包",
        k1="我的饰品", v1="我的饰品",
        k2="饰品预设", v2="饰品预设",
        k3="饰品背包", v3="饰品背包"
    )
