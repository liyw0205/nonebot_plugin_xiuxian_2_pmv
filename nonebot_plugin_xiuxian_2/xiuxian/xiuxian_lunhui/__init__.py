import asyncio
import json
from datetime import datetime, timedelta
from ..on_compat import on_command
from nonebot.log import logger
from nonebot.params import CommandArg
from ..xiuxian_utils.lay_out import assign_bot, Cooldown
from ..xiuxian_utils.item_json import Items
from ..xiuxian_config import XiuConfig, convert_rank, added_ranks
from ..xiuxian_utils.xiuxian2_handle import XiuxianDateManage, XIUXIAN_IMPART_BUFF, PlayerDataManager, UserBuffDate
from ..xiuxian_utils.data_source import jsondata
from ..adapter_compat import (
    Bot,
    GROUP,
    Message,
    GroupMessageEvent,
    PrivateMessageEvent,
    MessageSegment
)
from ..xiuxian_utils.utils import (
    check_user, get_msg_pic,
    handle_send,
    send_help_message,
    log_message,
    update_statistics_value,
    number_to
)
from ..xiuxian_impart.impart_uitls import (
    impart_check,
    update_user_impart_data
)
from ...paths import get_paths
from .transaction_service import (
    CultivationResetService,
    LunhuiRecallService,
    LunhuiSettlementService,
)

xiuxian_impart = XIUXIAN_IMPART_BUFF()
player_data_manager = PlayerDataManager()
items = Items()
lunhui_recall_service = LunhuiRecallService(get_paths().game_db, get_paths().player_db)
lunhui_settlement_service = LunhuiSettlementService(get_paths().game_db, get_paths().player_db, get_paths().impart_db)
cultivation_reset_service = CultivationResetService(get_paths().game_db)
added_ranks = added_ranks()
confirm_lunhui_cache = {}
ROOT_RENAME_CARD_ID = 20025
ROOT_RENAME_CARD_NAME = "灵根改名卡"

__warring_help__ = f"""
**轮回重修**
---
**警言**
> 此举不可逆。修为、功法、神通、灵石、修炼等级、虚神界修炼时辰尽数散尽。
> 散尽毕生修为后，可凝万世道果为极致天赋。

**入轮回**
- 千世轮回
> 得【轮回灵根】，须达 {XiuConfig().lunhui_min_level}
- 万世轮回
> 得【真·轮回灵根】，须达 {XiuConfig().twolun_min_level}
- 永恒轮回
> 得【永恒灵根】，须达 {XiuConfig().threelun_min_level}
- 进入无限轮回
> 得【命运灵根】，须达 {XiuConfig().Infinite_reincarnation_min_level}
> 命运道果每 5 次轮回减单次加成 30%，最低 50%

**自废**
- 自废修为
> 仅感气境可用，尽散修为（慎）

**轮回印记**
> 每次轮回自动铭刻：主功法 / 辅修 / 神通 / 身法 / 瞳术。
> 后可以【回忆前世 + 类型】取回，如：回忆前世 主功法。
> 每类仅可取回一次，取后印记废。
> 须达轮回前境界 3 个大境界；无限轮回可减免（1 次 1 个小境界）。

> 轮回后更易灵根资质；每次赠【灵根改名卡】x1；装备与囊中之物不散。
""".strip()

cache_help_fk = {}
sql_message = XiuxianDateManage()  # sql类

warring_help = on_command("轮回重修帮助", aliases={"轮回帮助"}, priority=12, block=True)
lunhui = on_command('进入轮回', aliases={"开始轮回"}, priority=15,  block=True)
Infinite_reincarnation = on_command('进入无限轮回', priority=15,  block=True)
retrieve_memory = on_command("回忆前世", aliases={"回忆前世", "取回记忆"}, priority=15, block=True)
view_memory = on_command("轮回印记", priority=15, block=True)
resetting = on_command('自废修为', priority=15,  block=True)
confirm_lunhui = on_command('确认轮回', priority=15,  block=True)

@warring_help.handle(parameterless=[Cooldown(cd_time=0)])
async def warring_help_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """轮回重修帮助"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    msg = __warring_help__
    await send_help_message(bot, event, msg, k1="轮回", v1="进入轮回", k2="存档", v2="我的修仙信息", k3="印记", v3="轮回印记")
    await warring_help.finish()
        
@resetting.handle(parameterless=[Cooldown(cd_time=0)])
async def resetting_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await resetting.finish()
        
    user_id = user_info['user_id']
    event_id = str(getattr(event, "message_id", "") or getattr(event, "id", "") or "").strip()
    operation_id = f"cultivation-reset:{user_id}:{event_id}" if event_id else f"cultivation-reset:{user_id}:{datetime.now().timestamp()}"
    # 先回放：成功后境界变为江湖好手会挡住同事件幂等。
    prior = cultivation_reset_service.get_result(operation_id)
    if prior is not None and prior.succeeded:
        user_msg = sql_message.get_user_info_with_id(user_id)
        msg = f"{user_msg['user_name']}现在是一介凡人了！！\n该自废修为请求已经处理，无需重复提交。"
        await handle_send(bot, event, msg)
        await resetting.finish()
    user_msg = sql_message.get_user_info_with_id(user_id)
    user_name = user_msg['user_name']
    if user_msg['level'] in ['感气境初期', '感气境中期', '感气境圆满']:
        exp = user_msg['exp']
        result = cultivation_reset_service.reset(operation_id, user_id, user_msg['level'], exp)
        if result.status == "duplicate":
            msg = f"{user_name}现在是一介凡人了！！\n该自废修为请求已经处理，无需重复提交。"
            await handle_send(bot, event, msg)
            await resetting.finish()
        if not result.succeeded:
            await handle_send(bot, event, "角色状态已变化，本次自废修为未执行，请重新查看后再试。")
            await resetting.finish()
        msg = f"{user_name}现在是一介凡人了！！"
        if result.status == "applied":
            log_message(user_id, f"[自废修为] 从{user_msg['level']}自废至江湖好手，重置修为{number_to(exp)}")
            update_statistics_value(user_id, "自废修为次数")
        await handle_send(bot, event, msg)
        await resetting.finish()
    else:
        msg = f"道友境界未达要求，自废修为的最低境界为感气境！"
        await handle_send(bot, event, msg)
        await resetting.finish()
        
@lunhui.handle(parameterless=[Cooldown(cd_time=0)])
async def lunhui_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await lunhui.finish()
        
    user_id = user_info['user_id']
    user_msg = sql_message.get_user_info_with_id(user_id) 
    user_name = user_msg['user_name']
    user_root = user_msg['root_type']
    list_level_all = list(jsondata.level_data().keys())
    level = user_info['level']
    
    if str(user_id) in confirm_lunhui_cache:
        msg = "请发送【确认轮回】！"
        await handle_send(bot, event, msg, md_type="轮回", k1="确认", v1="确认轮回", k2="存档", v2="我的修仙信息", k3="帮助", v3="轮回帮助")
        await lunhui.finish()
    if user_root == '轮回道果':
        root_level = 7
        lunhui_level = XiuConfig().twolun_min_level
        lunhui_level2 = "万世轮回"
        msg = f"万世道果集一身，脱出凡道入仙道，恭喜大能{user_name}万世轮回成功！"
    elif user_root == '真·轮回道果':
        root_level = 8
        lunhui_level = XiuConfig().threelun_min_level
        lunhui_level2 = "永恒轮回"
        msg = f"穿越千劫万难，证得不朽之身，恭喜大能{user_name}步入永恒之道，成就无上永恒！"
    elif user_root == '永恒道果':
        root_level = 9
        lunhui_level = XiuConfig().Infinite_reincarnation_min_level
        lunhui_level2 = "无限轮回"
        msg = f"超越永恒，超脱命运，执掌因果轮回！恭喜大能{user_name}突破命运桎梏，成就无上命运道果！"
    elif user_root == '命运道果':
        await Infinite_reincarnation_(bot, event)
        await lunhui.finish()
    else:
        root_level = 6
        lunhui_level = XiuConfig().lunhui_min_level
        lunhui_level2 = "千世轮回"
        msg = f"千世轮回磨不灭，重回绝颠谁能敌，恭喜大能{user_name}轮回成功！"
        
    if list_level_all.index(level) >= list_level_all.index(lunhui_level):
        await confirm_lunhui_invite(bot, event, user_id, root_level, lunhui_level2, msg)
    else:
        msg = f"道友境界未达要求\n当前进入：{lunhui_level2}\n最低境界为：{lunhui_level}"
        await handle_send(bot, event, msg, md_type="轮回", k1="修为", v1="我的修为", k2="存档", v2="我的修仙信息", k3="帮助", v3="轮回帮助")
    await lunhui.finish()

@Infinite_reincarnation.handle(parameterless=[Cooldown(cd_time=0)])
async def Infinite_reincarnation_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await Infinite_reincarnation.finish()
        
    user_id = user_info['user_id']
    user_msg = sql_message.get_user_info_with_id(user_id) 
    user_name = user_msg['user_name']
    user_root = user_msg['root_type']
    list_level_all = list(jsondata.level_data().keys())
    level = user_info['level']
    
    if str(user_id) in confirm_lunhui_cache:
        msg = "请发送【确认轮回】！"
        await handle_send(bot, event, msg, md_type="轮回", k1="确认", v1="确认轮回", k2="存档", v2="我的修仙信息", k3="帮助", v3="轮回帮助")
        await Infinite_reincarnation.finish()

    if user_root != '命运道果' :
        msg = "道友还未完成轮回，请先进入轮回！"
        await handle_send(bot, event, msg, md_type="轮回", k1="轮回", v1="进入轮回", k2="存档", v2="我的修仙信息", k3="帮助", v3="轮回帮助")
        await Infinite_reincarnation.finish()
    if (list_level_all.index(level) >= list_level_all.index(XiuConfig().Infinite_reincarnation_min_level)) and user_root == '命运道果':
        msg = f"超越永恒，超脱命运，执掌因果轮回！\n恭喜大能{user_name}突破命运桎梏，成就无上命运道果！"
        await confirm_lunhui_invite(bot, event, user_id, 0, "无限轮回", msg)
    else:
        msg = f"道友境界未达要求，无限轮回的最低境界为{XiuConfig().Infinite_reincarnation_min_level}！"
        await handle_send(bot, event, msg, md_type="轮回", k1="修为", v1="我的修为", k2="存档", v2="我的修仙信息", k3="帮助", v3="轮回帮助")
    await Infinite_reincarnation.finish()

@retrieve_memory.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await resetting.finish()
    
    user_id = user_info['user_id']
    arg = args.extract_plain_text().strip()
    
    type_map = {
        "主功法": "main_buff",
        "辅修": "sub_buff",
        "神通": "sec_buff",
        "身法": "effect1_buff",
        "瞳术": "effect2_buff"
    }
    
    if not arg or arg not in type_map:
        msg = "可取回类型：主功法 / 辅修 / 神通 / 身法 / 瞳术\n示例：回忆前世 主功法"
        await handle_send(bot, event, msg)
        await handle_send(bot, event, msg, md_type="轮回", k1="功法", v1="回忆前世 主功法", k2="辅修", v2="回忆前世 辅修", k3="神通", v3="回忆前世 神通")
        return
    
    skill_type = type_map[arg]
    event_id = str(getattr(event, "message_id", "") or getattr(event, "id", "") or "").strip()
    operation_id = f"lunhui-recall:{event_id}:{user_id}:{skill_type}" if event_id else f"lunhui-recall:{user_id}:{skill_type}:{datetime.now().timestamp()}"
    # 先回放：成功后 retrieved 标记会挡住同事件幂等。
    prior = lunhui_recall_service.get_result(operation_id)
    if prior is not None and prior.succeeded:
        skill_name = items.get_data_by_item_id(prior.skill_id).get('name', '未知技能') if prior.skill_id else '未知技能'
        reason = f"成功回忆前世中的技能：{skill_name}\n该回忆请求已经处理，无需重复提交。"
        await handle_send(bot, event, reason, md_type="轮回", k1="功法", v1="回忆前世 主功法", k2="辅修", v2="回忆前世 辅修", k3="神通", v3="回忆前世 神通")
        return
    success, reason = retrieve_reincarnation_skill(user_id, skill_type, operation_id=operation_id)
    if success:
        if "无需重复提交" not in reason:
            # mark duplicate from service
            pass
        log_message(user_id, f"[回忆前世] 取回{arg}：{reason}")
        update_statistics_value(user_id, "回忆前世次数")
        update_statistics_value(user_id, f"回忆前世{arg}")

    await handle_send(bot, event, reason, md_type="轮回", k1="功法", v1="回忆前世 主功法", k2="辅修", v2="回忆前世 辅修", k3="神通", v3="回忆前世 神通")

@view_memory.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await resetting.finish()
    
    user_id = user_info['user_id']
    memory = get_reincarnation_memory(user_id)
    
    if not memory:
        await handle_send(bot, event, "你没有任何轮回印记。")
        return
    
    lines = ["你的轮回印记："]
    
    type_names = {
        "main_buff": "主功法",
        "sub_buff": "辅修功法",
        "sec_buff": "神通",
        "effect1_buff": "身法",
        "effect2_buff": "瞳术"
    }
    
    for key, name in type_names.items():
        skill_id = memory.get(key, 0)
        retrieved = memory["retrieved"].get(key.replace("_buff", ""), False)
        
        if skill_id == 0:
            lines.append(f"  {name}：无（前世未修炼）")
        elif retrieved:
            lines.append(f"  {name}：{items.get_data_by_item_id(skill_id)['name']}（已取回）")
        else:
            lines.append(f"  {name}：{items.get_data_by_item_id(skill_id)['name']}")
    
    await handle_send(bot, event, "\n".join(lines), md_type="轮回", k1="功法", v1="回忆前世 主功法", k2="辅修", v2="回忆前世 辅修", k3="神通", v3="回忆前世 神通")

@confirm_lunhui.handle(parameterless=[Cooldown(cd_time=0)])
async def confirm_lunhui_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """处理确认轮回"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await confirm_lunhui.finish()

    user_id = user_info['user_id']
    event_id = str(getattr(event, "message_id", "") or getattr(event, "id", "") or "").strip()
    # event-scoped op：成功后 invite cache 会清空，不能把 invite_id 放进 operation_id/payload。
    operation_id = f"lunhui-settlement:{event_id}:{user_id}" if event_id else f"lunhui-settlement:{user_id}:{datetime.now().timestamp()}"
    prior = lunhui_settlement_service.get_result(operation_id)
    if prior is not None and prior.succeeded:
        msg = (
            f"轮回已完成（重放）。\n保留灵石至{number_to(prior.stone)}"
            f"{f'，祈愿石 x{prior.wishing_stones}' if prior.wishing_stones else ''}"
            f"\n该轮回请求已经处理，无需重复提交。"
        )
        await handle_send(bot, event, msg, md_type="轮回", k1="修为", v1="我的修为", k2="存档", v2="我的修仙信息", k3="印记", v3="轮回印记")
        await confirm_lunhui.finish()

    if str(user_id) not in confirm_lunhui_cache:
        msg = "没有待处理的轮回！"
        await handle_send(bot, event, msg, md_type="轮回", k1="轮回", v1="进入轮回", k2="存档", v2="我的修仙信息", k3="帮助", v3="轮回帮助")
        await confirm_lunhui.finish()

    confirm_data = confirm_lunhui_cache[str(user_id)]
    root_level = confirm_data['root_level']
    original_msg = confirm_data['msg']
    impart_data_draw = await impart_check(user_id)
    user_msg = sql_message.get_user_info_with_id(user_id)
    buff_info = UserBuffDate(user_id).BuffInfo or {}
    expected_buffs = {
        key: int(buff_info.get(key, 0) or 0)
        for key in ("main_buff", "sub_buff", "sec_buff", "effect1_buff", "effect2_buff")
    }
    impart_exp_day = int(impart_data_draw.get("exp_day", 0) or 0) if impart_data_draw else 0
    impart_stone = int(impart_data_draw.get("stone_num", 0) or 0) if impart_data_draw else 0
    result = lunhui_settlement_service.settle(
        operation_id, user_id, user_msg["level"], root_level, user_msg["root_type"],
        ROOT_RENAME_CARD_ID, ROOT_RENAME_CARD_NAME,
        expected_exp=user_msg["exp"], expected_stone=user_msg["stone"],
        expected_root_level=user_msg["root_level"], expected_buffs=expected_buffs,
        expected_impart_exp_day=impart_exp_day, expected_impart_stone=impart_stone,
        user_name=user_msg["user_name"],
    )
    if result.status == "duplicate":
        msg = (
            f"轮回已完成（重放）。\n保留灵石至{number_to(result.stone)}"
            f"{f'，祈愿石 x{result.wishing_stones}' if result.wishing_stones else ''}"
            f"\n该轮回请求已经处理，无需重复提交。"
        )
        await handle_send(bot, event, msg, md_type="轮回", k1="修为", v1="我的修为", k2="存档", v2="我的修仙信息", k3="印记", v3="轮回印记")
        await confirm_lunhui.finish()
    if not result.succeeded:
        await handle_send(bot, event, "轮回确认状态已变化，本次未执行，请重新发起轮回。", md_type="轮回")
        await confirm_lunhui.finish()

    msg = f"{original_msg}！\n轮回馈赠：{ROOT_RENAME_CARD_NAME} x1"
    if result.wishing_stones:
        msg += f"\n思恋结晶转化：祈愿石 x{result.wishing_stones}"
    if result.status == "applied":
        log_message(
            user_id,
            f"[轮回] {user_msg['root_type']}于{user_msg['level']}确认轮回，重置修为{number_to(user_msg['exp'])}，保留灵石至{number_to(result.stone)}"
        )
    await handle_send(bot, event, msg, md_type="轮回", k1="修为", v1="我的修为", k2="存档", v2="我的修仙信息", k3="印记", v3="轮回印记")

    if str(user_id) in confirm_lunhui_cache and confirm_lunhui_cache[str(user_id)].get("invite_id") == confirm_data["invite_id"]:
        del confirm_lunhui_cache[str(user_id)]
    await confirm_lunhui.finish()

async def confirm_lunhui_invite(bot, event, user_id, root_level, lunhui_level2, msg):
    """发送确认轮回"""
    invite_id = f"{user_id}_lunhui_{datetime.now().timestamp()}"
    confirm_lunhui_cache[str(user_id)] = {
        'root_level': root_level,
        'msg': msg,
        'invite_id': invite_id
    }

    # 设置60秒过期
    asyncio.create_task(expire_confirm_lunhui_invite(user_id, invite_id, bot, event))

    msg = f"您即将进入【{lunhui_level2}】，请在60秒内确认！\n发送【确认轮回】以继续，或等待60秒后自动取消。"
    await handle_send(bot, event, msg, md_type="轮回", k1="确认", v1="确认轮回", k2="存档", v2="我的修仙信息", k3="帮助", v3="轮回帮助")

async def expire_confirm_lunhui_invite(user_id, invite_id, bot, event):
    """确认轮回过期处理"""
    await asyncio.sleep(60)
    if str(user_id) in confirm_lunhui_cache and confirm_lunhui_cache[str(user_id)]['invite_id'] == invite_id:
        del confirm_lunhui_cache[str(user_id)]
        msg = "确认轮回已过期！"
        await handle_send(bot, event, msg, md_type="轮回", k1="轮回", v1="进入轮回", k2="存档", v2="我的修仙信息", k3="帮助", v3="轮回帮助")

def save_reincarnation_memory(user_id):
    """轮回时保存当前功法/技能记忆"""
    buff = UserBuffDate(user_id).BuffInfo
    
    memory = {
        "main_buff":    buff.get('main_buff', 0),
        "sub_buff":     buff.get('sub_buff', 0),
        "sec_buff":     buff.get('sec_buff', 0),
        "effect1_buff": buff.get('effect1_buff', 0),
        "effect2_buff": buff.get('effect2_buff', 0),
        # 记录时的境界，用于后续判断可取回的最低境界
        "memory_level": sql_message.get_user_info_with_id(user_id)['level'],
        # 已取回的标记（每种技能只能取一次）
        "retrieved_main": 0,
        "retrieved_sub": 0,
        "retrieved_sec": 0,
        "retrieved_effect1": 0,
        "retrieved_effect2": 0
    }
    
    # 字段化存储，每个属性独立存储
    for field, value in memory.items():
        player_data_manager.update_or_write_data(
            str(user_id),
            "reincarnation_memory",
            field,
            value,
            data_type="TEXT" if field == "memory_level" else "INTEGER"
        )
    
    logger.info(f"[轮回印记] 用户 {user_id} 轮回记忆已保存")
    return memory


def get_reincarnation_memory(user_id):
    """读取轮回印记"""
    data = player_data_manager.get_fields(str(user_id), "reincarnation_memory")
    if not data:
        return None
    
    memory = {
        "main_buff": data.get("main_buff", 0),
        "sub_buff": data.get("sub_buff", 0),
        "sec_buff": data.get("sec_buff", 0),
        "effect1_buff": data.get("effect1_buff", 0),
        "effect2_buff": data.get("effect2_buff", 0),
        "memory_level": data.get("memory_level", ""),
        "retrieved": {
            "main": bool(data.get("retrieved_main", 0)),
            "sub": bool(data.get("retrieved_sub", 0)),
            "sec": bool(data.get("retrieved_sec", 0)),
            "effect1": bool(data.get("retrieved_effect1", 0)),
            "effect2": bool(data.get("retrieved_effect2", 0))
        }
    }
    
    return memory


def can_retrieve_skill(user_id, skill_type):
    """
    判断某类技能是否可以取回
    返回 (can_retrieve: bool, reason: str, required_level_name: str or None)
    """
    memory = get_reincarnation_memory(user_id)
    user_info = sql_message.get_user_info_with_id(user_id)
    if not memory:
        return False, "你没有任何轮回印记", None
    
    # 检查是否已取回过
    retrieved_key = {
        "main_buff": "main",
        "sub_buff": "sub",
        "sec_buff": "sec",
        "effect1_buff": "effect1",
        "effect2_buff": "effect2"
    }.get(skill_type)
    
    if not retrieved_key:
        return False, "无效的技能类型", None
        
    if memory["retrieved"].get(retrieved_key, False):
        return False, "该类技能的轮回印记已被取回过", None
    
    # 获取轮回前的境界
    old_level = memory.get("memory_level")
    if not old_level:
        return False, "轮回记忆数据异常", None
    
    # 获取境界数值（数字越大境界越低）
    old_rank_score, rank_list = convert_rank(old_level)
    if old_rank_score is None:
        return False, "轮回前境界数据异常", None

    # 计算取回偏移量：基础9级 + 轮回等级加成（上限9级）
    root_level = user_info.get('root_level', 0)
    total_offset = 9 + min(root_level, 9)
    
    # 计算要求境界数值（数值越大，要求境界越低）
    required_rank_score = old_rank_score + total_offset
    
    # 确保不超出境界表范围（最低只能到江湖好手）
    max_score = convert_rank("江湖好手")[0]
    required_rank_score = min(required_rank_score, max_score)
    
    # 获取对应的境界名称用于提示
    target_idx = len(rank_list) - required_rank_score - 1
    target_idx = max(0, min(target_idx, len(rank_list) - 1))
    min_level_name = rank_list[target_idx]
    
    # 当前境界数值
    current_rank_score, _ = convert_rank(user_info['level'])
    
    # 判断当前境界是否达到要求
    if current_rank_score > required_rank_score:
        return False, f"境界不足，需要达到【{min_level_name}】才能取回该记忆", min_level_name
    
    # 记忆中是否有该技能
    skill_id = memory.get(skill_type, 0)
    if skill_id == 0:
        return False, f"轮回记忆中没有记录{arg_to_name(skill_type)}相关技能", None
    
    return True, "可取回", None

def arg_to_name(skill_type):
    """辅助转换显示名称"""
    names = {
        "main_buff": "主功法", "sub_buff": "辅修", "sec_buff": "神通",
        "effect1_buff": "身法", "effect2_buff": "瞳术"
    }
    return names.get(skill_type, skill_type)


def retrieve_reincarnation_skill(user_id, skill_type, operation_id=None):
    """
    执行取回某类轮回技能
    返回 (success: bool, msg: str)
    """
    if operation_id:
        prior = lunhui_recall_service.get_result(operation_id)
        if prior is not None and prior.succeeded:
            skill_name = items.get_data_by_item_id(prior.skill_id).get('name', '未知技能') if prior.skill_id else '未知技能'
            return True, f"成功回忆前世中的技能：{skill_name}\n该回忆请求已经处理，无需重复提交。"
    can, reason, _ = can_retrieve_skill(user_id, skill_type)
    if not can:
        return False, reason
    
    memory = get_reincarnation_memory(user_id)
    skill_id = memory.get(skill_type, 0)
    if skill_id == 0:
        return False, "记忆中没有该技能"
    
    result = lunhui_recall_service.recall(operation_id or f"lunhui-recall:{user_id}:{skill_type}", user_id, skill_type, skill_id)
    if result.status == "duplicate":
        skill_name = items.get_data_by_item_id(result.skill_id).get('name', '未知技能') if result.skill_id else '未知技能'
        return True, f"成功回忆前世中的技能：{skill_name}\n该回忆请求已经处理，无需重复提交。"
    if not result.succeeded:
        return False, "轮回印记状态已变化，请重试"
    
    skill_name = items.get_data_by_item_id(skill_id).get('name', '未知技能')
    return True, f"成功回忆前世中的技能：{skill_name}"
