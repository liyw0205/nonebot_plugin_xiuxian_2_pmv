try:
    import ujson as json
except ImportError:
    import json
import random
from pathlib import Path
from datetime import datetime

from ..on_compat import on_command
from nonebot.params import CommandArg

from ..adapter_compat import Bot, GroupMessageEvent, PrivateMessageEvent, Message
from ..xiuxian_buff import trigger_mentor_breakthrough_reward, trigger_partner_exp_share
from ..xiuxian_config import XiuConfig, convert_rank
from ..xiuxian_utils.data_source import jsondata
from ..xiuxian_utils.item_json import Items
from ..xiuxian_utils.lay_out import assign_bot, Cooldown
from ..xiuxian_utils.utils import (
    check_user, handle_send, number_to, send_msg_handler,
    log_message, update_statistics_value
)
from ..xiuxian_utils.xiuxian2_handle import OtherSet, UserBuffDate, XiuxianDateManage
from ..xiuxian_title.title_data import check_and_unlock_titles

sql_message = XiuxianDateManage()
PLAYERSDATA = Path() / "data" / "xiuxian" / "players"
tribulation_cd2 = int(XiuConfig().tribulation_cd * 60)

level_up = on_command("突破", priority=6, block=True)
level_up_dr = on_command("渡厄突破", priority=7, block=True)
level_up_drjd = on_command("渡厄金丹突破", aliases={"金丹突破"}, priority=7, block=True)
level_up_zj = on_command("直接突破", aliases={"破"}, priority=7, block=True)
level_up_lx = on_command("连续突破", aliases={"快速突破"}, priority=7, block=True)
level_up_dr_lx = on_command("连续渡厄突破", aliases={"连续渡厄", "快速渡厄突破"}, priority=7, block=True)
level_up_drjd_lx = on_command("连续渡厄金丹突破", aliases={"连续金丹突破", "快速金丹突破"}, priority=7, block=True)
user_leveluprate = on_command('我的突破概率', aliases={"突破概率", "概率"}, priority=5, block=True)
tribulation_info = on_command("渡劫", priority=5, block=True)
start_tribulation = on_command("开始渡劫", priority=6, block=True)
destiny_tribulation = on_command("天命渡劫", priority=6, block=True)
heart_devil_tribulation = on_command("渡心魔劫", priority=6, block=True)
fusion_destiny_tribulation_pill = on_command("融合天命渡劫丹", aliases={"合成天命渡劫丹"}, priority=5, block=True)
fusion_destiny_pill = on_command("融合天命丹", aliases={"合成天命丹"}, priority=5, block=True)


def get_user_tribulation_info(user_id):
    """获取用户渡劫信息"""
    user_id = str(user_id)
    legacy_data = None
    legacy_path = PLAYERSDATA / user_id / "tribulation_info.json"

    if legacy_path.exists():
        try:
            with open(legacy_path, "r", encoding="utf-8") as f:
                legacy_data = json.load(f)
        except Exception:
            legacy_data = None

        if legacy_data and not sql_message.has_user_tribulation_info(user_id):
            default_data = sql_message.get_user_tribulation_info(user_id)
            for key in default_data:
                if key in legacy_data:
                    default_data[key] = legacy_data[key]
            sql_message.save_user_tribulation_info(user_id, default_data)

        try:
            legacy_path.unlink()
        except OSError:
            pass

    return sql_message.get_user_tribulation_info(user_id)

def save_user_tribulation_info(user_id, data):
    """保存用户渡劫信息"""
    sql_message.save_user_tribulation_info(user_id, data)

def clear_user_tribulation_info(user_id):
    """清空用户渡劫信息(渡劫成功后调用)"""
    user_id = str(user_id)
    sql_message.clear_user_tribulation_info(user_id)
    legacy_path = PLAYERSDATA / user_id / "tribulation_info.json"
    if legacy_path.exists():
        try:
            legacy_path.unlink()
        except OSError:
            pass

def refresh_achievement_titles(user_id):
    """统计变更后自动解锁称号成就，失败不影响主流程。"""
    try:
        check_and_unlock_titles(user_id)
    except Exception as e:
        log_message(user_id, f"[成就称号] 自动检查失败：{e}")


def trigger_breakthrough_relation_rewards(user_id, new_level):
    return (
        trigger_partner_exp_share(user_id, new_level)
        + trigger_mentor_breakthrough_reward(user_id, new_level)
    )

def record_level_up_result(
    user_id, method, attempts=1, success=False, target_level=None,
    fail_count=0, exp_loss=0, exp_gain=0, item_name=None, item_count=0
):
    update_statistics_value(user_id, "突破次数", increment=attempts)
    if success:
        update_statistics_value(user_id, "突破成功")
    if fail_count:
        update_statistics_value(user_id, "突破失败", increment=fail_count)
    if exp_loss:
        update_statistics_value(user_id, "突破损失修为", increment=exp_loss)
    if exp_gain:
        update_statistics_value(user_id, "突破获得修为", increment=exp_gain)
    if item_name and item_count:
        update_statistics_value(user_id, f"{item_name}消耗", increment=item_count)
    refresh_achievement_titles(user_id)

    result = "成功" if success else "失败"
    extra = []
    if target_level:
        extra.append(f"目标境界：{target_level}")
    if fail_count:
        extra.append(f"失败{fail_count}次")
    if exp_loss:
        extra.append(f"损失修为{number_to(exp_loss)}")
    if exp_gain:
        extra.append(f"获得修为{number_to(exp_gain)}")
    if item_name and item_count:
        extra.append(f"消耗{item_name}{item_count}个")
    suffix = "，" + "，".join(extra) if extra else ""
    log_message(user_id, f"[{method}] 突破{result}，尝试{attempts}次{suffix}")

def record_tribulation_result(user_id, method, success, target_level=None, rate=None, item_name=None, item_count=0):
    update_statistics_value(user_id, "渡劫次数")
    update_statistics_value(user_id, "渡劫成功" if success else "渡劫失败")
    if item_name and item_count:
        update_statistics_value(user_id, f"{item_name}消耗", increment=item_count)
    refresh_achievement_titles(user_id)

    result = "成功" if success else "失败"
    extra = []
    if target_level:
        extra.append(f"目标境界：{target_level}")
    if rate is not None:
        extra.append(f"成功率{rate}%")
    if item_name and item_count:
        extra.append(f"消耗{item_name}{item_count}个")
    suffix = "，" + "，".join(extra) if extra else ""
    log_message(user_id, f"[{method}] 渡劫{result}{suffix}")

def record_heart_devil_result(user_id, success, rate, devil_name=None, item_used=False):
    update_statistics_value(user_id, "心魔劫次数")
    update_statistics_value(user_id, "心魔劫成功" if success else "心魔劫失败")
    if item_used:
        update_statistics_value(user_id, "天命丹消耗")
    refresh_achievement_titles(user_id)

    result = "成功" if success else "失败"
    name_msg = f"{devil_name}，" if devil_name else ""
    item_msg = "，消耗天命丹1个" if item_used else ""
    log_message(user_id, f"[渡心魔劫] {name_msg}{result}，当前渡劫成功率{rate}%{item_msg}")

@tribulation_info.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """查看渡劫信息"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await tribulation_info.finish()
    
    user_id = user_info['user_id']
    tribulation_data = get_user_tribulation_info(user_id)
    
    # 构建消息
    msg = "✨【渡劫信息】✨\n"
    msg += f"当前境界：{user_info['level']}\n"
    
    # 检查是否需要渡劫
    level_name = user_info['level']
    levels = convert_rank('江湖好手')[1]
    current_index = levels.index(level_name)
    
    # 检查是否需要渡劫
    if level_name.endswith('圆满') and levels.index(level_name) < levels.index(XiuConfig().tribulation_min_level):
        msg = f"道友当前境界{level_name}无需渡劫，请使用【突破】指令！"
        await handle_send(bot, event, msg, md_type="修仙", k1="突破", v1="突破", k2="存档", v2="我的修仙信息", k3="修为", v3="我的修为")
        await tribulation_info.finish()

    if current_index == 0:  # 已经是最高境界
        msg += "道友已是至高境界，无需渡劫！"
        await handle_send(bot, event, msg)
        await tribulation_info.finish()
    else:
        next_level = levels[current_index + 1]
        next_level_data = jsondata.level_data()[next_level]
        current_exp = int(user_info['exp'])
        required_exp = int(next_level_data['power'])
        
        # 检查渡劫条件：境界圆满且修为达到下一境界要求
        need_tribulation = (
            level_name.endswith('圆满') and 
            current_exp >= required_exp
        )
        
        if need_tribulation:
            msg += (
                f"下一境界：{next_level}\n"
                f"当前修为：{number_to(current_exp)}/{number_to(required_exp)}\n"
                f"渡劫成功率：{tribulation_data['current_rate']}%\n"
                f"════════════\n"
                f"【开始渡劫】尝试渡劫\n"
                f"【天命渡劫】使用天命渡劫丹\n"
                f"【渡心魔劫】挑战心魔\n"
                f"【融合天命渡劫丹】天命渡劫"
            )
        else:
            if not level_name.endswith('圆满'):
                msg += f"道友境界尚未圆满，无法渡劫！"
            else:
                # 计算还需要多少修为
                remaining_exp = max(0, required_exp - current_exp)
                msg += (
                    f"下一境界：{next_level}\n"
                    f"当前修为：{number_to(current_exp)}/{number_to(required_exp)}\n"
                    f"还需修为：{number_to(remaining_exp)}\n"
                    f"════════════\n"
                    f"请继续修炼，待修为足够后再来渡劫！"
                )
    
    await handle_send(bot, event, msg, md_type="修仙", k1="开始", v1="开始渡劫", k2="天命", v2="天命渡劫", k3="心魔劫", v3="渡心魔劫")
    await tribulation_info.finish()

@fusion_destiny_pill.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """融合天命丹"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await fusion_destiny_pill.finish()
    
    user_id = user_info['user_id']
    args = args.extract_plain_text().strip()
    
    # 解析数量参数
    try:
        num = int(args) if args else 2  # 默认2个渡厄丹合成1个天命丹
        num = max(2, min(num, 10))
    except ValueError:
        msg = "请输入有效的数量(2-10)！"
        await handle_send(bot, event, msg)
        await fusion_destiny_pill.finish()
    
    # 检查渡厄丹数量
    back_msg = sql_message.get_back_msg(user_id) or []
    elixir_count = 0
    for item in back_msg:
        if item['goods_id'] == 1999:  # 渡厄丹ID
            elixir_count = item['goods_num']
            break
    
    if elixir_count < num:
        msg = f"融合需要{num}个渡厄丹，你只有{elixir_count}个！"
        await handle_send(bot, event, msg)
        await fusion_destiny_pill.finish()
    
    # 计算成功率（每个渡厄丹10%）
    success_rate = min(100, num * 10)  # 上限100%
    roll = random.randint(1, 100)
    
    if roll <= success_rate:  # 成功
        # 扣除渡厄丹
        sql_message.update_back_j(user_id, 1999, num)
        
        # 获得天命丹
        destiny_count = 1  # 成功固定获得1个
        sql_message.send_back(user_id, 1996, "天命丹", "丹药", destiny_count, 1)
        
        msg = (
            f"✨融合成功！消耗{num}个渡厄丹获得1个天命丹✨"
        )
        log_message(user_id, f"[融合天命丹] 成功，消耗渡厄丹{num}个，获得天命丹1个")
        update_statistics_value(user_id, "天命丹融合次数")
        update_statistics_value(user_id, "天命丹融合成功")
        update_statistics_value(user_id, "渡厄丹消耗", increment=num)
    else:  # 失败
        # 扣除渡厄丹
        sql_message.update_back_j(user_id, 1999, num)
        
        msg = (
            f"融合失败！消耗了{num}个渡厄丹\n"
            f"当前成功率：{success_rate}%\n"
            f"（每颗渡厄丹提供10%成功率，10颗必成功）"
        )
        log_message(user_id, f"[融合天命丹] 失败，消耗渡厄丹{num}个，成功率{success_rate}%")
        update_statistics_value(user_id, "天命丹融合次数")
        update_statistics_value(user_id, "天命丹融合失败")
        update_statistics_value(user_id, "渡厄丹消耗", increment=num)
    
    await handle_send(bot, event, msg)
    await fusion_destiny_pill.finish()

@fusion_destiny_tribulation_pill.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """融合天命渡劫丹"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await fusion_destiny_tribulation_pill.finish()
    
    user_id = user_info['user_id']
    args = args.extract_plain_text().strip()
    
    # 解析数量参数
    try:
        num = int(args) if args else 2  # 默认2个天命丹合成1个天命渡劫丹
        num = max(2, min(num, 10))
    except ValueError:
        msg = "请输入有效的数量(2-10)！"
        await handle_send(bot, event, msg)
        await fusion_destiny_tribulation_pill.finish()
    
    # 检查天命丹数量
    back_msg = sql_message.get_back_msg(user_id) or []
    elixir_count = 0
    for item in back_msg:
        if item['goods_id'] == 1996:  # 天命丹ID
            elixir_count = item['goods_num']
            break
    
    if elixir_count < num:
        msg = f"融合需要{num}个天命丹，你只有{elixir_count}个！\n请发送【融合天命丹】获得"
        await handle_send(bot, event, msg)
        await fusion_destiny_tribulation_pill.finish()
    
    # 计算成功率（每个天命丹10%）
    success_rate = min(100, num * 10)  # 上限100%
    roll = random.randint(1, 100)
    
    if roll <= success_rate:  # 成功
        # 扣除天命丹
        sql_message.update_back_j(user_id, 1996, num)
        
        # 获得天命渡劫丹
        destiny_count = 1  # 成功固定获得1个
        sql_message.send_back(user_id, 1997, "天命渡劫丹", "丹药", destiny_count, 1)
        
        msg = (
            f"✨融合成功！消耗{num}个天命丹获得1个天命渡劫丹✨"
        )
        log_message(user_id, f"[融合天命渡劫丹] 成功，消耗天命丹{num}个，获得天命渡劫丹1个")
        update_statistics_value(user_id, "天命渡劫丹融合次数")
        update_statistics_value(user_id, "天命渡劫丹融合成功")
        update_statistics_value(user_id, "天命丹消耗", increment=num)
    else:  # 失败
        # 扣除天命丹
        sql_message.update_back_j(user_id, 1996, num)
        
        msg = (
            f"融合失败！消耗了{num}个天命丹\n"
            f"当前成功率：{success_rate}%\n"
            f"（每颗天命丹提供10%成功率，10颗必成功）"
        )
        log_message(user_id, f"[融合天命渡劫丹] 失败，消耗天命丹{num}个，成功率{success_rate}%")
        update_statistics_value(user_id, "天命渡劫丹融合次数")
        update_statistics_value(user_id, "天命渡劫丹融合失败")
        update_statistics_value(user_id, "天命丹消耗", increment=num)
    
    await handle_send(bot, event, msg)

@start_tribulation.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """开始渡劫"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await start_tribulation.finish()
    
    user_id = user_info['user_id']
    tribulation_data = get_user_tribulation_info(user_id)
    tribulation_cd = tribulation_cd2
    user_buff_info = UserBuffDate(user_id).BuffInfo
    if int(user_buff_info.get('main_buff', 0)) == 9931:
        tribulation_cd = int(tribulation_cd * 0.5)
        
    # 检查境界是否可以渡劫
    level_name = user_info['level']
    levels = convert_rank('江湖好手')[1]
    current_index = levels.index(level_name)
    
    # 检查是否需要渡劫
    if level_name.endswith('圆满') and levels.index(level_name) < levels.index(XiuConfig().tribulation_min_level):
        msg = f"道友当前境界{level_name}无需渡劫，请使用【突破】指令！"
        await handle_send(bot, event, msg, md_type="修仙", k1="突破", v1="突破", k2="存档", v2="我的修仙信息", k3="修为", v3="我的修为")
        await start_tribulation.finish()

    if current_index == 0:  # 已经是最高境界
        msg = "道友已是至高境界，无需渡劫！"
        await handle_send(bot, event, msg)
        await start_tribulation.finish()
    
    next_level = levels[current_index + 1]
    next_level_data = jsondata.level_data()[next_level]
    current_exp = int(user_info['exp'])
    required_exp = int(next_level_data['power'])
    
    # 检查渡劫条件：境界圆满且修为达标
    if not level_name.endswith('圆满'):
        msg = f"当前境界：{user_info['level']}\n道友境界尚未圆满，无法渡劫！"
        await handle_send(bot, event, msg, md_type="修仙", k1="开始", v1="开始渡劫", k2="天命", v2="天命渡劫", k3="心魔劫", v3="渡心魔劫")
        await start_tribulation.finish()
    if not (current_exp >= required_exp):
        remaining_exp = max(0, required_exp - current_exp)
        msg = (
            f"渡劫条件不足！\n"
            f"当前境界：{level_name}\n"
            f"下一境界：{next_level}\n"
            f"当前修为：{number_to(current_exp)}/{number_to(required_exp)}\n"
            f"还需修为：{number_to(remaining_exp)}\n"
            f"════════════\n"
            f"请继续修炼，待修为足够后再来渡劫！"
        )
        await handle_send(bot, event, msg, md_type="修仙", k1="开始", v1="开始渡劫", k2="天命", v2="天命渡劫", k3="心魔劫", v3="渡心魔劫")
        await start_tribulation.finish()
    
    # 检查是否有天命丹
    has_destiny_pill = False
    back = sql_message.get_back_msg(user_id) or []
    for item in back:
        if item['goods_id'] == 1996:  # 天命丹ID
            has_destiny_pill = True
            break

    # 检查冷却时间
    if tribulation_data['last_time']:
        if has_destiny_pill:  # 使用天命丹降低冷却
            tribulation_cd = int(tribulation_cd * 0.75)
        last_time = datetime.strptime(tribulation_data['last_time'], '%Y-%m-%d %H:%M:%S.%f')
        cd = OtherSet().date_diff(datetime.now(), last_time)
        if cd < tribulation_cd:
            remaining = tribulation_cd - cd
            hours = remaining // 3600
            minutes = (remaining % 3600) // 60
            msg = f"渡劫冷却中，还需{hours}小时{minutes}分钟！"
            await handle_send(bot, event, msg, md_type="修仙", k1="开始", v1="开始渡劫", k2="天命", v2="天命渡劫", k3="心魔劫", v3="渡心魔劫")
            await start_tribulation.finish()

    # 开始渡劫
    success_rate = tribulation_data['current_rate']
    roll = random.randint(1, 100)
    
    if roll <= success_rate:  # 渡劫成功
        sql_message.updata_level(user_id, next_level)
        share_msg = trigger_breakthrough_relation_rewards(user_id, next_level)
        sql_message.update_power2(user_id)
        clear_user_tribulation_info(user_id)
        record_tribulation_result(user_id, "开始渡劫", True, target_level=next_level, rate=success_rate)
        
        msg = (
            f"⚡⚡⚡渡劫成功⚡⚡⚡️\n"
            f"历经九九雷劫，道友终成{next_level}！\n"
            f"当前境界：{next_level}{share_msg}"
        )
    else:  # 渡劫失败
        new_rate = min(
            success_rate + 10,
            XiuConfig().tribulation_max_rate
        )
        tribulation_data['current_rate'] = new_rate
        if has_destiny_pill:  # 使用天命丹避免概率降低
            sql_message.update_back_j(user_id, 1996, use_key=1)
            record_tribulation_result(
                user_id, "开始渡劫", False, target_level=next_level,
                rate=new_rate, item_name="天命丹", item_count=1
            )
            msg = (
                f"渡劫失败！\n"
                f"雷劫之下，道心受损！\n"
                f"幸得天命丹护体，下次渡劫成功率提升至：{new_rate}%"
            )
        else:
            record_tribulation_result(user_id, "开始渡劫", False, target_level=next_level, rate=new_rate)
            
            msg = (
                f"渡劫失败！\n"
                f"雷劫之下，道心受损！\n"
                f"下次渡劫成功率提升至：{new_rate}%"
            )
        tribulation_data['last_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
        save_user_tribulation_info(user_id, tribulation_data)    
    await handle_send(bot, event, msg, md_type="修仙", k1="开始", v1="开始渡劫", k2="天命", v2="天命渡劫", k3="心魔劫", v3="渡心魔劫")
    await start_tribulation.finish()

@destiny_tribulation.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """天命渡劫"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await destiny_tribulation.finish()
    
    user_id = user_info['user_id']
    tribulation_data = get_user_tribulation_info(user_id)
    tribulation_cd = tribulation_cd2
    user_buff_info = UserBuffDate(user_id).BuffInfo
    if int(user_buff_info.get('main_buff', 0)) == 9931:
        tribulation_cd = int(tribulation_cd * 0.5)
    
    # 检查是否有天命渡劫丹
    back = sql_message.get_back_msg(user_id) or []
    has_item = False
    for item in back:
        if item['goods_id'] == 1997:
            has_item = True
            break

    # 检查冷却时间
    if tribulation_data['last_time']:
        if has_item:
            tribulation_cd = int(tribulation_cd * 0.75)
        last_time = datetime.strptime(tribulation_data['last_time'], '%Y-%m-%d %H:%M:%S.%f')
        cd = OtherSet().date_diff(datetime.now(), last_time)
        if cd < tribulation_cd:
            hours = (tribulation_cd - cd) // 3600
            minutes = ((tribulation_cd - cd) % 3600) // 60
            msg = f"渡劫冷却中，还需{hours}小时{minutes}分钟！"
            await handle_send(bot, event, msg, md_type="修仙", k1="开始", v1="开始渡劫", k2="天命", v2="天命渡劫", k3="心魔劫", v3="渡心魔劫")
            await destiny_tribulation.finish()
                
    if not has_item:
        msg = f"道友天命渡劫丹不足！\n请发送【融合天命渡劫丹】获得"
        await handle_send(bot, event, msg, md_type="修仙", k1="开始", v1="开始渡劫", k2="天命", v2="天命渡劫", k3="融合", v3="融合天命渡劫丹")
        await destiny_tribulation.finish()
    
    # 检查境界是否可以渡劫
    level_name = user_info['level']
    levels = convert_rank('江湖好手')[1]
    current_index = levels.index(level_name)
    
    # 检查是否需要渡劫
    if level_name.endswith('圆满') and levels.index(level_name) < levels.index(XiuConfig().tribulation_min_level):
        msg = f"道友当前境界{level_name}无需渡劫，请使用【突破】指令！"
        await handle_send(bot, event, msg, md_type="修仙", k1="突破", v1="突破", k2="存档", v2="我的修仙信息", k3="修为", v3="我的修为")
        await destiny_tribulation.finish()

    if current_index == 0:  # 已经是最高境界
        msg = "道友已是至高境界，无需渡劫！"
        await handle_send(bot, event, msg)
        await destiny_tribulation.finish()
    
    next_level = levels[current_index + 1]
    next_level_data = jsondata.level_data()[next_level]
    current_exp = int(user_info['exp'])
    required_exp = int(next_level_data['power'])
    
    # 检查渡劫条件：境界圆满且修为达标
    if not level_name.endswith('圆满'):
        msg = f"当前境界：{user_info['level']}\n道友境界尚未圆满，无法渡劫！"
        await handle_send(bot, event, msg, md_type="修仙", k1="开始", v1="开始渡劫", k2="天命", v2="天命渡劫", k3="心魔劫", v3="渡心魔劫")
        await destiny_tribulation.finish()
    if not (current_exp >= required_exp):
        remaining_exp = max(0, required_exp - current_exp)
        msg = (
            f"渡劫条件不足！\n"
            f"当前境界：{level_name}\n"
            f"下一境界：{next_level}\n"
            f"当前修为：{number_to(current_exp)}/{number_to(required_exp)}\n"
            f"还需修为：{number_to(remaining_exp)}\n"
            f"════════════\n"
            f"请继续修炼，待修为足够后再来渡劫！"
        )
        await handle_send(bot, event, msg, md_type="修仙", k1="开始", v1="开始渡劫", k2="天命", v2="天命渡劫", k3="心魔劫", v3="渡心魔劫")
        await destiny_tribulation.finish()
    
    # 使用天命渡劫丹
    sql_message.update_back_j(user_id, 1997, use_key=1)
    
    # 必定成功
    sql_message.updata_level(user_id, next_level)
    share_msg = trigger_breakthrough_relation_rewards(user_id, next_level)
    sql_message.update_power2(user_id)
    clear_user_tribulation_info(user_id)
    record_tribulation_result(
        user_id, "天命渡劫", True, target_level=next_level,
        item_name="天命渡劫丹", item_count=1
    )
    
    msg = (
        f"✨天命所归，渡劫成功✨\n"
        f"道友轻松突破至{next_level}！\n"
        f"当前境界：{next_level}{share_msg}"
    )
    
    await handle_send(bot, event, msg)
    await destiny_tribulation.finish()

@heart_devil_tribulation.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """渡心魔劫"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await heart_devil_tribulation.finish()
    
    user_id = user_info['user_id']
    tribulation_data = get_user_tribulation_info(user_id)
    tribulation_cd = int(tribulation_cd2 * 0.5)
    user_buff_info = UserBuffDate(user_id).BuffInfo
    if int(user_buff_info.get('main_buff', 0)) == 9931:
        tribulation_cd = int(tribulation_cd * 0.5)
    
    # 检查渡劫概率是否已达上限
    if tribulation_data['current_rate'] >= XiuConfig().tribulation_max_rate:
        msg = random.choice([
            "道友道心已臻至完美，无需再渡心魔劫！",
            "心魔已消，道友道心澄明如镜！",
            "恭喜道友，心魔已无法侵扰你的道心！"
        ])
        await handle_send(bot, event, msg, md_type="修仙", k1="开始", v1="开始渡劫", k2="天命", v2="天命渡劫", k3="心魔劫", v3="渡心魔劫")
        await heart_devil_tribulation.finish()
    
    # 检查心魔劫次数
    heart_devil_count = tribulation_data.get('heart_devil_count', 0)
    if heart_devil_count >= 5:
        msg = "道友已无需渡心魔劫！"
        await handle_send(bot, event, msg)
        await heart_devil_tribulation.finish()
    
    # 更新心魔劫次数
    tribulation_data['heart_devil_count'] = heart_devil_count + 1
    save_user_tribulation_info(user_id, tribulation_data)
    
    # 检查境界是否可以渡劫
    level_name = user_info['level']
    levels = convert_rank('江湖好手')[1]
    current_index = levels.index(level_name)
   
    # 检查是否需要渡劫
    if level_name.endswith('圆满') and levels.index(level_name) < levels.index(XiuConfig().tribulation_min_level):
        msg = f"道友当前境界{level_name}无需渡劫，请使用【突破】指令！"
        await handle_send(bot, event, msg, md_type="修仙", k1="突破", v1="突破", k2="存档", v2="我的修仙信息", k3="修为", v3="我的修为")
        await heart_devil_tribulation.finish()

    if current_index == 0:  # 已经是最高境界
        msg = "道友已是至高境界，无需渡劫！"
        await handle_send(bot, event, msg)
        await heart_devil_tribulation.finish()
    
    next_level = levels[current_index + 1]
    next_level_data = jsondata.level_data()[next_level]
    current_exp = int(user_info['exp'])
    required_exp = int(next_level_data['power'])
    
    # 检查渡劫条件：境界圆满且修为达标
    if not (current_exp >= required_exp):
        remaining_exp = max(0, required_exp - current_exp)
        msg = (
            f"渡劫条件不足！\n"
            f"当前境界：{level_name}\n"
            f"下一境界：{next_level}\n"
            f"当前修为：{number_to(current_exp)}/{number_to(required_exp)}\n"
            f"还需修为：{number_to(remaining_exp)}\n"
            f"════════════\n"
            f"请继续修炼，待修为足够后再来渡劫！"
        )
        await handle_send(bot, event, msg, md_type="修仙", k1="开始", v1="开始渡劫", k2="天命", v2="天命渡劫", k3="心魔劫", v3="渡心魔劫")
        await heart_devil_tribulation.finish()
    
    # 检查是否有天命丹
    back = sql_message.get_back_msg(user_id) or []
    has_destiny_pill = False
    for item in back:
        if item['goods_id'] == 1996:  # 天命丹ID
            has_destiny_pill = True
            break

    # 检查冷却时间
    if tribulation_data['last_time']:
        if has_destiny_pill:  # 使用天命丹降低冷却
            tribulation_cd = int(tribulation_cd * 0.75)
        last_time = datetime.strptime(tribulation_data['last_time'], '%Y-%m-%d %H:%M:%S.%f')
        cd = OtherSet().date_diff(datetime.now(), last_time)
        if cd < tribulation_cd:
            hours = (tribulation_cd - cd) // 3600
            minutes = ((tribulation_cd - cd) % 3600) // 60
            msg = f"渡劫冷却中，还需{hours}小时{minutes}分钟！"
            await handle_send(bot, event, msg, md_type="修仙", k1="开始", v1="开始渡劫", k2="天命", v2="天命渡劫", k3="心魔劫", v3="渡心魔劫")
            await heart_devil_tribulation.finish()
        
    # 随机决定渡劫类型 (1:直接成功, 2:直接失败, 3:战斗判断)
    tribulation_type = random.choices([1, 2, 3], weights=[0.2, 0.2, 0.6])[0]
    
    if tribulation_type == 1:  # 直接成功
        new_rate = min(tribulation_data['current_rate'] + 20, XiuConfig().tribulation_max_rate)
        tribulation_data['current_rate'] = new_rate
        tribulation_data['last_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
        save_user_tribulation_info(user_id, tribulation_data)
        record_heart_devil_result(user_id, True, new_rate)
        
        msg = (
            f"✨天赐良机，渡劫成功✨\n"
            f"道友福缘深厚，渡过了心魔劫！\n"
            f"渡劫成功率提升至{new_rate}%！"
        )
        await handle_send(bot, event, msg, md_type="修仙", k1="开始", v1="开始渡劫", k2="天命", v2="天命渡劫", k3="心魔劫", v3="渡心魔劫")
        await heart_devil_tribulation.finish()
        
    elif tribulation_type == 2:  # 直接失败
        item_used = False
        if has_destiny_pill:  # 使用天命丹避免概率降低
            sql_message.update_back_j(user_id, 1996, use_key=1)
            item_used = True
            msg = (
                f"💀渡劫失败💀\n"
                f"心魔突然爆发，道心受损！\n"
                f"幸得天命丹护体，下次渡劫成功率保持：{tribulation_data['current_rate']}%"
            )
        else:
            new_rate = max(tribulation_data['current_rate'] - 20, XiuConfig().tribulation_base_rate)
            tribulation_data['current_rate'] = new_rate
            
            msg = (
                f"💀渡劫失败💀\n"
                f"心魔突然爆发，道心受损！\n"
                f"下次渡劫成功率降低至{new_rate}%！"
            )
        tribulation_data['last_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
        save_user_tribulation_info(user_id, tribulation_data)
        record_heart_devil_result(user_id, False, tribulation_data['current_rate'], item_used=item_used)
        await handle_send(bot, event, msg, md_type="修仙", k1="开始", v1="开始渡劫", k2="天命", v2="天命渡劫", k3="心魔劫", v3="渡心魔劫")
        await heart_devil_tribulation.finish()
        
    else:  # 战斗判断
        # 心魔类型和属性
        heart_devil_types = [
            {"name": "贪欲心魔", "scale": 0.01, 
             "win_desc": "战胜贪念，道心更加坚定", 
             "lose_desc": "贪念缠身，欲壑难填"},
            {"name": "嗔怒心魔", "scale": 0.02, 
             "win_desc": "化解怒火，心境更加平和", 
             "lose_desc": "怒火中烧，理智全失"},
            {"name": "痴妄心魔", "scale": 0.03, 
             "win_desc": "破除执念，心境更加通透", 
             "lose_desc": "执念深重，难以自拔"},
            {"name": "傲慢心魔", "scale": 0.04, 
             "win_desc": "克服傲慢，更加谦逊有礼", 
             "lose_desc": "目中无人，狂妄自大"},
            {"name": "嫉妒心魔", "scale": 0.05, 
             "win_desc": "消除妒火，心境更加宽广", 
             "lose_desc": "妒火中烧，心怀怨恨"},
            {"name": "恐惧心魔", "scale": 0.08, 
             "win_desc": "战胜恐惧，勇气倍增", 
             "lose_desc": "畏首畏尾，胆小如鼠"},
            {"name": "懒惰心魔", "scale": 0.1, 
             "win_desc": "克服懒惰，更加勤奋", 
             "lose_desc": "懈怠懒散，不思进取"},
            {"name": "七情心魔", "scale": 0.15, 
             "win_desc": "调和七情，心境更加平衡", 
             "lose_desc": "七情六欲，纷扰不休"},
            {"name": "六欲心魔", "scale": 0.2, 
             "win_desc": "超脱欲望，心境更加纯净", 
             "lose_desc": "欲望缠身，难以解脱"},
            {"name": "天魔幻象", "scale": 0.25, 
             "win_desc": "识破幻象，道心更加稳固", 
             "lose_desc": "天魔入体，幻象丛生"},
            {"name": "心魔劫主", "scale": 0.3, 
             "win_desc": "战胜心魔之主，道心大进", 
             "lose_desc": "心魔之主，万劫之源"}
        ]
        
        # 随机选择心魔类型
        devil_data = random.choice(heart_devil_types)
        devil_name = devil_data["name"]
        scale = devil_data["scale"]

        player = sql_message.get_player_data(user_id)
        # 生成心魔属性
        devil_info = {
            "气血": int(player['气血'] * 100),
            "总血量": int(player['气血'] * scale),
            "真元": int(player['真元'] * scale),
            "攻击": int(player['攻击'] * scale // 2),
            "name": devil_name,
            "jj": "祭道境",
            "desc": devil_data["lose_desc"]  # 默认显示负面描述
        }
        
        # 执行战斗
        result, victor, _ = await Boss_fight(user_id, devil_info, type_in=1, bot_id=bot.self_id)
        
        if victor == "群友赢了":  # 战斗胜利
            new_rate = min(tribulation_data['current_rate'] + 20, XiuConfig().tribulation_max_rate)
            tribulation_data['current_rate'] = new_rate
            tribulation_data['last_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
            save_user_tribulation_info(user_id, tribulation_data)
            record_heart_devil_result(user_id, True, new_rate, devil_name=devil_name)
            
            msg = (
                f"⚔️战胜{devil_name}，道心升华⚔️\n"
                f"{devil_data['win_desc']}\n"
                f"经过艰苦战斗，道友战胜了{devil_name}！\n"
                f"渡劫成功率提升至{new_rate}%！"
            )
        else:  # 战斗失败
            item_used = False
            if has_destiny_pill:  # 使用天命丹避免概率降低
                sql_message.update_back_j(user_id, 1996, use_key=1)
                item_used = True
                msg = (
                    f"💀败于{devil_name}，道心受损💀\n"
                    f"{devil_data['lose_desc']}\n"
                    f"幸得天命丹护体，下次渡劫成功率保持：{tribulation_data['current_rate']}%"
                )
            else:
                new_rate = max(tribulation_data['current_rate'] - 20, XiuConfig().tribulation_base_rate)
                tribulation_data['current_rate'] = new_rate
                
                msg = (
                    f"💀败于{devil_name}，道心受损💀\n"
                    f"{devil_data['lose_desc']}\n"
                    f"道友不敌{devil_name}，渡劫成功率降低至{new_rate}%！"
                )
            tribulation_data['last_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
            save_user_tribulation_info(user_id, tribulation_data)        
            record_heart_devil_result(
                user_id, False, tribulation_data['current_rate'],
                devil_name=devil_name, item_used=item_used
            )
        await send_msg_handler(bot, event, result, )
        await handle_send(bot, event, msg, md_type="修仙", k1="开始", v1="开始渡劫", k2="天命", v2="天命渡劫", k3="心魔劫", v3="渡心魔劫")
        await heart_devil_tribulation.finish()

@level_up.handle(parameterless=[Cooldown(stamina_cost=1)])
async def level_up_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """突破"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await level_up.finish()
    user_id = user_info['user_id']
    if user_info['hp'] is None:
        # 判断用户气血是否为空
        sql_message.update_user_hp(user_id)
    user_msg = sql_message.get_user_info_with_id(user_id)  # 用户信息
    user_leveluprate = int(user_msg['level_up_rate'])  # 用户失败次数加成
    level_cd = user_msg['level_up_cd']
    if level_cd:
        # 校验是否存在CD
        time_now = datetime.now()
        cd = OtherSet().date_diff(time_now, level_cd)  # 获取second
        if cd < XiuConfig().level_up_cd * 60:
            # 如果cd小于配置的cd，返回等待时间
            msg = f"目前无法突破，还需要{XiuConfig().level_up_cd - (cd // 60)}分钟"
            sql_message.update_user_stamina(user_id, 12, 1)
            await handle_send(bot, event, msg, md_type="修仙", k1="直接突破", v1="直接突破", k2="渡厄", v2="渡厄突破", k3="修为", v3="我的修为")
            await level_up.finish()
    else:
        pass

    level_name = user_msg['level']  # 用户境界
    levels = convert_rank('江湖好手')[1]
    
    # 检查是否需要渡劫
    if level_name.endswith('圆满') and levels.index(level_name) >= levels.index(XiuConfig().tribulation_min_level):
        msg = f"道友当前境界{level_name}需要渡劫才能突破，请使用【渡劫】指令！"
        await handle_send(bot, event, msg, md_type="修仙", k1="渡劫", v1="渡劫", k2="存档", v2="我的修仙信息", k3="修为", v3="我的修为")
        await level_up.finish()

    level_rate = jsondata.level_rate_data()[level_name]  # 对应境界突破的概率
    user_backs = sql_message.get_back_msg(user_id) or []  # list(back)
    items = Items()
    pause_flag = False
    elixir_name = None
    elixir_desc = None
    for back in user_backs:
        if int(back['goods_id']) == 1999:  # 检测到有对应丹药
            pause_flag = True
            elixir_name = back['goods_name']
            elixir_desc = items.get_data_by_item_id(1999)['desc']
            break
    main_rate_buff = UserBuffDate(user_id).get_user_main_buff_data()#功法突破概率提升，别忘了还有渡厄突破
    number = main_rate_buff['number'] if main_rate_buff is not None else 0
    if pause_flag:
        msg = f"道友背包中备有丹药：{elixir_name}，效果：{elixir_desc}，突破已经准备就绪\n请发送【渡厄突破】或【直接突破】来选择是否使用丹药突破！\n本次突破概率为：{level_rate + user_leveluprate + number}% "
        await handle_send(bot, event, msg, md_type="修仙", k1="直接突破", v1="直接突破", k2="渡厄", v2="渡厄突破", k3="修为", v3="我的修为")
        await level_up.finish()
    else:
        msg = f"道友背包中暂无【渡厄丹】，突破已经准备就绪\n请发送【直接突破】来突破！请注意，本次突破失败将会损失部分修为！\n本次突破概率为：{level_rate + user_leveluprate + number}% "
        await handle_send(bot, event, msg, md_type="修仙", k1="直接突破", v1="直接突破", k2="渡厄", v2="渡厄突破", k3="修为", v3="我的修为")
        await level_up.finish()

@level_up_zj.handle(parameterless=[Cooldown(cd_time=0)])
async def level_up_zj_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """直接突破"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await level_up_zj.finish()
    user_id = user_info['user_id']
    if user_info['hp'] is None:
        # 判断用户气血是否为空
        sql_message.update_user_hp(user_id)
    user_msg = sql_message.get_user_info_with_id(user_id)  # 用户信息
    level_cd = user_msg['level_up_cd']
    if level_cd:
        # 校验是否存在CD
        time_now = datetime.now()
        cd = OtherSet().date_diff(time_now, level_cd)  # 获取second
        if cd < XiuConfig().level_up_cd * 60:
            # 如果cd小于配置的cd，返回等待时间
            msg = f"目前无法突破，还需要{XiuConfig().level_up_cd - (cd // 60)}分钟"
            sql_message.update_user_stamina(user_id, 6, 1)
            await handle_send(bot, event, msg, md_type="修仙", k1="直接突破", v1="直接突破", k2="渡厄", v2="渡厄突破", k3="修为", v3="我的修为")
            await level_up_zj.finish()
    else:
        pass

    level_name = user_msg['level']  # 用户境界
    levels = convert_rank('江湖好手')[1]
    
    # 检查是否需要渡劫
    if level_name.endswith('圆满') and levels.index(level_name) >= levels.index(XiuConfig().tribulation_min_level):
        msg = f"道友当前境界{level_name}需要渡劫才能突破，请使用【渡劫】指令！"
        await handle_send(bot, event, msg, md_type="修仙", k1="渡劫", v1="渡劫", k2="存档", v2="我的修仙信息", k3="修为", v3="我的修为")
        await level_up_zj.finish()

    level_name = user_msg['level']  # 用户境界
    exp = user_msg['exp']  # 用户修为
    level_rate = jsondata.level_rate_data()[level_name]  # 对应境界突破的概率
    leveluprate = int(user_msg['level_up_rate'])  # 用户失败次数加成
    main_rate_buff = UserBuffDate(user_id).get_user_main_buff_data()#功法突破概率提升，别忘了还有渡厄突破
    main_exp_buff = UserBuffDate(user_id).get_user_main_buff_data()#功法突破扣修为减少
    exp_buff = main_exp_buff['exp_buff'] if main_exp_buff is not None else 0
    number = main_rate_buff['number'] if main_rate_buff is not None else 0
    le = OtherSet().get_type(exp, level_rate + leveluprate + number, level_name)
    if le == "失败":
        # 突破失败
        sql_message.updata_level_cd(user_id)  # 更新突破CD
        # 失败惩罚，随机扣减修为
        percentage = random.randint(
            XiuConfig().level_punishment_floor, XiuConfig().level_punishment_limit
        )
        now_exp = int(int(exp) * ((percentage / 100) * (1 - exp_buff))) #功法突破扣修为减少
        sql_message.update_j_exp(user_id, now_exp)  # 更新用户修为
        nowhp = user_msg['hp'] - (now_exp / 2) if (user_msg['hp'] - (now_exp / 2)) > 0 else 1
        nowmp = user_msg['mp'] - now_exp if (user_msg['mp'] - now_exp) > 0 else 1
        sql_message.update_user_hp_mp(user_id, nowhp, nowmp)  # 修为掉了，血量、真元也要掉
        update_rate = 1 if int(level_rate * XiuConfig().level_up_probability) <= 1 else int(
            level_rate * XiuConfig().level_up_probability)  # 失败增加突破几率
        sql_message.update_levelrate(user_id, leveluprate + update_rate)
        msg = f"道友突破失败,境界受损,修为减少{number_to(now_exp)}，下次突破成功率增加{update_rate}%，道友不要放弃！"
        record_level_up_result(user_id, "直接突破", success=False, fail_count=1, exp_loss=now_exp)
        await handle_send(bot, event, msg, md_type="修仙", k1="直接突破", v1="直接突破", k2="渡厄", v2="渡厄突破", k3="修为", v3="我的修为")
        await level_up_zj.finish()

    elif type(le) == list:
        # 突破成功
        sql_message.updata_level(user_id, le[0])  # 更新境界
        share_msg = trigger_breakthrough_relation_rewards(user_id, le[0])
        sql_message.update_power2(user_id)  # 更新战力
        sql_message.updata_level_cd(user_id)  # 更新CD
        sql_message.update_levelrate(user_id, 0)
        sql_message.update_user_hp(user_id)  # 重置用户HP，mp，atk状态
        msg = f"恭喜道友突破{le[0]}成功！{share_msg}"
        record_level_up_result(user_id, "直接突破", success=True, target_level=le[0])
        await handle_send(bot, event, msg, md_type="修仙", k1="直接突破", v1="直接突破", k2="渡厄", v2="渡厄突破", k3="修为", v3="我的修为")
        await level_up_zj.finish()
    else:
        # 最高境界
        msg = le
        await handle_send(bot, event, msg)
        await level_up_zj.finish()

@level_up_lx.handle(parameterless=[Cooldown(stamina_cost=15)])  # 连续突破消耗15体力
async def level_up_lx_continuous(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """连续突破5次"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await level_up_lx.finish()
    
    user_id = user_info['user_id']
    if user_info['hp'] is None:
        sql_message.update_user_hp(user_id)
    
    user_msg = sql_message.get_user_info_with_id(user_id)
    level_cd = user_msg['level_up_cd']
    
    # 检查突破CD
    if level_cd:
        time_now = datetime.now()
        cd = OtherSet().date_diff(time_now, level_cd)
        if cd < XiuConfig().level_up_cd * 60:
            msg = f"目前无法突破，还需要{XiuConfig().level_up_cd - (cd // 60)}分钟"
            sql_message.update_user_stamina(user_id, 6, 1)
            await handle_send(bot, event, msg, md_type="修仙", k1="直接突破", v1="直接突破", k2="渡厄", v2="渡厄突破", k3="修为", v3="我的修为")
            await level_up_lx.finish()

    level_name = user_msg['level']  # 用户境界
    levels = convert_rank('江湖好手')[1]
    
    # 检查是否需要渡劫
    if level_name.endswith('圆满') and levels.index(level_name) >= levels.index(XiuConfig().tribulation_min_level):
        msg = f"道友当前境界{level_name}需要渡劫才能突破，请使用【渡劫】指令！"
        await handle_send(bot, event, msg, md_type="修仙", k1="渡劫", v1="渡劫", k2="存档", v2="我的修仙信息", k3="修为", v3="我的修为")
        await level_up_lx.finish()

    level_name = user_msg['level']
    exp = user_msg['exp']
    level_rate = jsondata.level_rate_data()[level_name]
    leveluprate = int(user_msg['level_up_rate'])
    main_rate_buff = UserBuffDate(user_id).get_user_main_buff_data()
    main_exp_buff = UserBuffDate(user_id).get_user_main_buff_data()
    exp_buff = main_exp_buff['exp_buff'] if main_exp_buff is not None else 0
    number = main_rate_buff['number'] if main_rate_buff is not None else 0
    
    success = False
    result_msg = ""
    attempts = 0
    fail_count = 0
    total_exp_loss = 0
    target_level = None
    
    for i in range(5):
        attempts += 1
        le = OtherSet().get_type(exp, level_rate + leveluprate + number, level_name)
        
        if isinstance(le, str):
            if le == "失败":
                # 突破失败
                percentage = random.randint(
                    XiuConfig().level_punishment_floor, XiuConfig().level_punishment_limit
                )
                now_exp = int(int(exp) * ((percentage / 100) * (1 - exp_buff)))
                sql_message.update_j_exp(user_id, now_exp)
                exp -= now_exp
                
                nowhp = user_msg['hp'] - (now_exp / 2) if (user_msg['hp'] - (now_exp / 2)) > 0 else 1
                nowmp = user_msg['mp'] - now_exp if (user_msg['mp'] - now_exp) > 0 else 1
                sql_message.update_user_hp_mp(user_id, nowhp, nowmp)
                fail_count += 1
                total_exp_loss += now_exp
                
                update_rate = 1 if int(level_rate * XiuConfig().level_up_probability) <= 1 else int(
                    level_rate * XiuConfig().level_up_probability)
                leveluprate += update_rate
                sql_message.update_levelrate(user_id, leveluprate)
                
                result_msg += f"第{attempts}次突破失败，修为减少{number_to(now_exp)}，下次突破成功率增加{update_rate}%\n"
            else:
                # 修为不足或已是最高境界
                result_msg += le
                break
        elif isinstance(le, list):
            # 突破成功
            sql_message.updata_level(user_id, le[0])
            share_msg = trigger_breakthrough_relation_rewards(user_id, le[0])
            sql_message.update_power2(user_id)
            sql_message.update_levelrate(user_id, 0)
            sql_message.update_user_hp(user_id)
            result_msg += f"第{attempts}次突破成功，达到{le[0]}境界！{share_msg}"
            success = True
            target_level = le[0]
            break
    
    if not success and attempts == 5 and "修为不足以突破" not in result_msg:
        result_msg += "连续5次突破尝试结束，未能突破成功。"
    
    sql_message.updata_level_cd(user_id)  # 更新突破CD
    record_level_up_result(
        user_id, "连续突破", attempts=attempts, success=success,
        target_level=target_level, fail_count=fail_count, exp_loss=total_exp_loss
    )
    await handle_send(bot, event, result_msg)
    await level_up_lx.finish()
    
@level_up_drjd.handle(parameterless=[Cooldown(stamina_cost=1)])
async def level_up_drjd_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """渡厄 金丹 突破"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await level_up_drjd.finish()
    user_id = user_info['user_id']
    if user_info['hp'] is None:
        # 判断用户气血是否为空
        sql_message.update_user_hp(user_id)
    user_msg = sql_message.get_user_info_with_id(user_id)  # 用户信息
    level_cd = user_msg['level_up_cd']
    if level_cd:
        # 校验是否存在CD
        time_now = datetime.now()
        cd = OtherSet().date_diff(time_now, level_cd)  # 获取second
        if cd < XiuConfig().level_up_cd * 60:
            # 如果cd小于配置的cd，返回等待时间
            msg = f"目前无法突破，还需要{XiuConfig().level_up_cd - (cd // 60)}分钟"
            sql_message.update_user_stamina(user_id, 4, 1)
            await handle_send(bot, event, msg, md_type="修仙", k1="直接突破", v1="直接突破", k2="渡厄", v2="渡厄突破", k3="修为", v3="我的修为")
            await level_up_drjd.finish()
    else:
        pass

    level_name = user_msg['level']  # 用户境界
    levels = convert_rank('江湖好手')[1]
    
    # 检查是否需要渡劫
    if level_name.endswith('圆满') and levels.index(level_name) >= levels.index(XiuConfig().tribulation_min_level):
        msg = f"道友当前境界{level_name}需要渡劫才能突破，请使用【渡劫】指令！"
        await handle_send(bot, event, msg, md_type="修仙", k1="渡劫", v1="渡劫", k2="存档", v2="我的修仙信息", k3="修为", v3="我的修为")
        await level_up_drjd.finish()

    elixir_name = "渡厄金丹"
    level_name = user_msg['level']  # 用户境界
    exp = user_msg['exp']  # 用户修为
    level_rate = jsondata.level_rate_data()[level_name]  # 对应境界突破的概率
    user_leveluprate = int(user_msg['level_up_rate'])  # 用户失败次数加成
    main_rate_buff = UserBuffDate(user_id).get_user_main_buff_data()#功法突破概率提升
    number = main_rate_buff['number'] if main_rate_buff is not None else 0
    le = OtherSet().get_type(exp, level_rate + user_leveluprate + number, level_name)
    user_backs = sql_message.get_back_msg(user_id) or []  # list(back)
    pause_flag = False
    for back in user_backs:
        if int(back['goods_id']) == 1998:  # 检测到有对应丹药
            pause_flag = True
            elixir_name = back['goods_name']
            break

    if not pause_flag:
        msg = f"道友突破需要使用{elixir_name}，但您的背包中没有该丹药！"
        sql_message.update_user_stamina(user_id, 4, 1)
        await handle_send(bot, event, msg, md_type="修仙", k1="直接突破", v1="直接突破", k2="渡厄", v2="渡厄突破", k3="修为", v3="我的修为")
        await level_up_drjd.finish()

    if le == "失败":
        # 突破失败
        sql_message.updata_level_cd(user_id)  # 更新突破CD
        if pause_flag:
            # 使用丹药减少的sql
            sql_message.update_back_j(user_id, 1998, use_key=1)
            now_exp = int(int(exp) * 0.1)
            sql_message.update_exp(user_id, now_exp)  # 渡厄金丹增加用户修为
            update_rate = 1 if int(level_rate * XiuConfig().level_up_probability) <= 1 else int(
                level_rate * XiuConfig().level_up_probability)  # 失败增加突破几率
            sql_message.update_levelrate(user_id, user_leveluprate + update_rate)
            msg = f"道友突破失败，但是使用了丹药{elixir_name}，本次突破失败不扣除修为反而增加了一成，下次突破成功率增加{update_rate}%！！"
            record_level_up_result(
                user_id, "渡厄金丹突破", success=False, fail_count=1,
                exp_gain=now_exp, item_name="渡厄金丹", item_count=1
            )
        else:
            # 失败惩罚，随机扣减修为
            percentage = random.randint(
                XiuConfig().level_punishment_floor, XiuConfig().level_punishment_limit
            )
            main_exp_buff = UserBuffDate(user_id).get_user_main_buff_data()#功法突破扣修为减少
            exp_buff = main_exp_buff['exp_buff'] if main_exp_buff is not None else 0
            now_exp = int(int(exp) * ((percentage / 100) * exp_buff))
            sql_message.update_j_exp(user_id, now_exp)  # 更新用户修为
            nowhp = user_msg['hp'] - (now_exp / 2) if (user_msg['hp'] - (now_exp / 2)) > 0 else 1
            nowmp = user_msg['mp'] - now_exp if (user_msg['mp'] - now_exp) > 0 else 1
            sql_message.update_user_hp_mp(user_id, nowhp, nowmp)  # 修为掉了，血量、真元也要掉
            update_rate = 1 if int(level_rate * XiuConfig().level_up_probability) <= 1 else int(
                level_rate * XiuConfig().level_up_probability)  # 失败增加突破几率
            sql_message.update_levelrate(user_id, user_leveluprate + update_rate)
            msg = f"道友未备好{elixir_name}，突破失败，境界受损，修为减少{number_to(now_exp)}，下次突破成功率增加{update_rate}%，道友不要放弃！"
            record_level_up_result(user_id, "渡厄金丹突破", success=False, fail_count=1, exp_loss=now_exp)
        await handle_send(bot, event, msg, md_type="修仙", k1="直接突破", v1="直接突破", k2="渡厄", v2="渡厄突破", k3="修为", v3="我的修为")
        await level_up_drjd.finish()

    elif type(le) == list:
        # 突破成功
        sql_message.updata_level(user_id, le[0])  # 更新境界
        share_msg = trigger_breakthrough_relation_rewards(user_id, le[0])
        sql_message.update_power2(user_id)  # 更新战力
        sql_message.updata_level_cd(user_id)  # 更新CD
        sql_message.update_levelrate(user_id, 0)
        sql_message.update_user_hp(user_id)  # 重置用户HP，mp，atk状态
        now_exp = int(int(exp) * 0.1)
        sql_message.update_exp(user_id, now_exp)  # 渡厄金丹增加用户修为
        msg = f"恭喜道友突破{le[0]}成功，因为使用了渡厄金丹，修为也增加了一成！！{share_msg}"
        record_level_up_result(
            user_id, "渡厄金丹突破", success=True, target_level=le[0],
            exp_gain=now_exp, item_name="渡厄金丹", item_count=1
        )
        await handle_send(bot, event, msg, md_type="修仙", k1="直接突破", v1="直接突破", k2="渡厄", v2="渡厄突破", k3="修为", v3="我的修为")
        await level_up_drjd.finish()
    else:
        # 最高境界
        msg = le
        await handle_send(bot, event, msg)
        await level_up_drjd.finish()


@level_up_dr.handle(parameterless=[Cooldown(stamina_cost=2)])
async def level_up_dr_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """渡厄 突破"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await level_up_dr.finish()
    user_id = user_info['user_id']
    if user_info['hp'] is None:
        # 判断用户气血是否为空
        sql_message.update_user_hp(user_id)
    user_msg = sql_message.get_user_info_with_id(user_id)  # 用户信息
    level_cd = user_msg['level_up_cd']
    if level_cd:
        # 校验是否存在CD
        time_now = datetime.now()
        cd = OtherSet().date_diff(time_now, level_cd)  # 获取second
        if cd < XiuConfig().level_up_cd * 60:
            # 如果cd小于配置的cd，返回等待时间
            msg = f"目前无法突破，还需要{XiuConfig().level_up_cd - (cd // 60)}分钟"
            sql_message.update_user_stamina(user_id, 8, 1)
            await handle_send(bot, event, msg, md_type="修仙", k1="直接突破", v1="直接突破", k2="渡厄", v2="渡厄突破", k3="修为", v3="我的修为")
            await level_up_dr.finish()
    else:
        pass

    level_name = user_msg['level']  # 用户境界
    levels = convert_rank('江湖好手')[1]
    
    # 检查是否需要渡劫
    if level_name.endswith('圆满') and levels.index(level_name) >= levels.index(XiuConfig().tribulation_min_level):
        msg = f"道友当前境界{level_name}需要渡劫才能突破，请使用【渡劫】指令！"
        await handle_send(bot, event, msg, md_type="修仙", k1="渡劫", v1="渡劫", k2="存档", v2="我的修仙信息", k3="修为", v3="我的修为")
        await level_up_dr.finish()

    elixir_name = "渡厄丹"
    level_name = user_msg['level']  # 用户境界
    exp = user_msg['exp']  # 用户修为
    level_rate = jsondata.level_rate_data()[level_name]  # 对应境界突破的概率
    user_leveluprate = int(user_msg['level_up_rate'])  # 用户失败次数加成
    main_rate_buff = UserBuffDate(user_id).get_user_main_buff_data()#功法突破概率提升
    number = main_rate_buff['number'] if main_rate_buff is not None else 0
    le = OtherSet().get_type(exp, level_rate + user_leveluprate + number, level_name)
    user_backs = sql_message.get_back_msg(user_id) or []  # list(back)
    pause_flag = False
    for back in user_backs:
        if int(back['goods_id']) == 1999:  # 检测到有对应丹药
            pause_flag = True
            elixir_name = back['goods_name']
            break
    
    if not pause_flag:
        msg = f"道友突破需要使用{elixir_name}，但您的背包中没有该丹药！"
        sql_message.update_user_stamina(user_id, 8, 1)
        await handle_send(bot, event, msg, md_type="修仙", k1="直接突破", v1="直接突破", k2="渡厄", v2="渡厄突破", k3="修为", v3="我的修为")
        await level_up_dr.finish()

    if le == "失败":
        # 突破失败
        sql_message.updata_level_cd(user_id)  # 更新突破CD
        if pause_flag:
            # todu，丹药减少的sql
            sql_message.update_back_j(user_id, 1999, use_key=1)
            update_rate = 1 if int(level_rate * XiuConfig().level_up_probability) <= 1 else int(
                level_rate * XiuConfig().level_up_probability)  # 失败增加突破几率
            sql_message.update_levelrate(user_id, user_leveluprate + update_rate)
            msg = f"道友突破失败，但是使用了丹药{elixir_name}，本次突破失败不扣除修为下次突破成功率增加{update_rate}%，道友不要放弃！"
            record_level_up_result(
                user_id, "渡厄突破", success=False, fail_count=1,
                item_name="渡厄丹", item_count=1
            )
        else:
            # 失败惩罚，随机扣减修为
            percentage = random.randint(
                XiuConfig().level_punishment_floor, XiuConfig().level_punishment_limit
            )
            main_exp_buff = UserBuffDate(user_id).get_user_main_buff_data()#功法突破扣修为减少
            exp_buff = main_exp_buff['exp_buff'] if main_exp_buff is not None else 0
            now_exp = int(int(exp) * ((percentage / 100) * (1 - exp_buff)))
            sql_message.update_j_exp(user_id, now_exp)  # 更新用户修为
            nowhp = user_msg['hp'] - (now_exp / 2) if (user_msg['hp'] - (now_exp / 2)) > 0 else 1
            nowmp = user_msg['mp'] - now_exp if (user_msg['mp'] - now_exp) > 0 else 1
            sql_message.update_user_hp_mp(user_id, nowhp, nowmp)  # 修为掉了，血量、真元也要掉
            update_rate = 1 if int(level_rate * XiuConfig().level_up_probability) <= 1 else int(
                level_rate * XiuConfig().level_up_probability)  # 失败增加突破几率
            sql_message.update_levelrate(user_id, user_leveluprate + update_rate)
            msg = f"道友未备好{elixir_name}，突破失败，境界受损，修为减少{number_to(now_exp)}，下次突破成功率增加{update_rate}%，道友不要放弃！"
            record_level_up_result(user_id, "渡厄突破", success=False, fail_count=1, exp_loss=now_exp)
        await handle_send(bot, event, msg, md_type="修仙", k1="直接突破", v1="直接突破", k2="渡厄", v2="渡厄突破", k3="修为", v3="我的修为")
        await level_up_dr.finish()

    elif type(le) == list:
        # 突破成功
        sql_message.updata_level(user_id, le[0])  # 更新境界
        share_msg = trigger_breakthrough_relation_rewards(user_id, le[0])
        sql_message.update_power2(user_id)  # 更新战力
        sql_message.updata_level_cd(user_id)  # 更新CD
        sql_message.update_levelrate(user_id, 0)
        sql_message.update_user_hp(user_id)  # 重置用户HP，mp，atk状态
        msg = f"恭喜道友突破{le[0]}成功{share_msg}"
        record_level_up_result(
            user_id, "渡厄突破", success=True, target_level=le[0],
            item_name="渡厄丹", item_count=1
        )
        await handle_send(bot, event, msg, md_type="修仙", k1="直接突破", v1="直接突破", k2="渡厄", v2="渡厄突破", k3="修为", v3="我的修为")
        await level_up_dr.finish()
    else:
        # 最高境界
        msg = le
        await handle_send(bot, event, msg)
        await level_up_dr.finish()

@level_up_dr_lx.handle(parameterless=[Cooldown(stamina_cost=15)])
async def level_up_dr_lx_continuous(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """连续渡厄突破5次"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await level_up_dr_lx.finish()
    
    user_id = user_info['user_id']
    if user_info['hp'] is None:
        sql_message.update_user_hp(user_id)
    
    user_msg = sql_message.get_user_info_with_id(user_id)
    level_cd = user_msg['level_up_cd']
    
    # 检查突破CD
    if level_cd:
        time_now = datetime.now()
        cd = OtherSet().date_diff(time_now, level_cd)
        if cd < XiuConfig().level_up_cd * 60:
            msg = f"目前无法突破，还需要{XiuConfig().level_up_cd - (cd // 60)}分钟"
            sql_message.update_user_stamina(user_id, 15, 1)
            await handle_send(bot, event, msg, md_type="修仙", k1="直接突破", v1="直接突破", k2="渡厄", v2="渡厄突破", k3="修为", v3="我的修为")
            await level_up_dr_lx.finish()

    level_name = user_msg['level']  # 用户境界
    levels = convert_rank('江湖好手')[1]
    
    # 检查是否需要渡劫
    if level_name.endswith('圆满') and levels.index(level_name) >= levels.index(XiuConfig().tribulation_min_level):
        msg = f"道友当前境界{level_name}需要渡劫才能突破，请使用【渡劫】指令！"
        await handle_send(bot, event, msg, md_type="修仙", k1="渡劫", v1="渡劫", k2="存档", v2="我的修仙信息", k3="修为", v3="我的修为")
        await level_up_dr_lx.finish()

    level_name = user_msg['level']
    exp = user_msg['exp']
    level_rate = jsondata.level_rate_data()[level_name]
    leveluprate = int(user_msg['level_up_rate'])
    main_rate_buff = UserBuffDate(user_id).get_user_main_buff_data()
    number = main_rate_buff['number'] if main_rate_buff is not None else 0
    
    # 检查渡厄丹数量（只需要1个即可开始）
    user_backs = sql_message.get_back_msg(user_id) or []
    dr_pill_count = 0
    for back in user_backs:
        if int(back['goods_id']) == 1999:  # 渡厄丹ID
            dr_pill_count = back['goods_num']
            break
    
    if dr_pill_count < 1:
        msg = f"渡厄突破至少需要1个！"
        await handle_send(bot, event, msg, md_type="修仙", k1="直接突破", v1="直接突破", k2="渡厄", v2="渡厄突破", k3="修为", v3="我的修为")
        await level_up_dr_lx.finish()
    
    success = False
    result_msg = ""
    attempts = 0
    pills_used = 0
    max_attempts = 5
    fail_count = 0
    target_level = None
    
    for i in range(max_attempts):
        attempts += 1
        
        # 检查是否还有渡厄丹
        if pills_used >= dr_pill_count:
            result_msg += f"\n第{attempts}次突破：渡厄丹不足，突破终止！"
            break
        
        le = OtherSet().get_type(exp, level_rate + leveluprate + number, level_name)
        
        if isinstance(le, str):
            if le == "失败":
                # 突破失败，使用渡厄丹
                pills_used += 1
                sql_message.update_back_j(user_id, 1999, 1)  # 消耗1个渡厄丹
                fail_count += 1
                
                update_rate = 1 if int(level_rate * XiuConfig().level_up_probability) <= 1 else int(
                    level_rate * XiuConfig().level_up_probability)
                leveluprate += update_rate
                sql_message.update_levelrate(user_id, leveluprate)
                
                result_msg += f"第{attempts}次突破失败，下次突破成功率增加{update_rate}%\n"
                
                # 检查是否还有丹药继续下一次尝试
                if pills_used >= dr_pill_count and attempts < max_attempts:
                    result_msg += f"渡厄丹已用完，无法继续突破！"
                    break
                    
            else:
                # 修为不足或已是最高境界
                result_msg += f"第{attempts}次突破：{le}\n"
                break
        elif isinstance(le, list):
            # 突破成功
            pills_used += 1
            sql_message.update_back_j(user_id, 1999, 1)  # 消耗1个渡厄丹
            sql_message.updata_level(user_id, le[0])
            share_msg = trigger_breakthrough_relation_rewards(user_id, le[0])
            sql_message.update_power2(user_id)
            sql_message.update_levelrate(user_id, 0)
            sql_message.update_user_hp(user_id)
            result_msg += f"第{attempts}次突破成功，达到{le[0]}境界！{share_msg}\n"
            success = True
            target_level = le[0]
            break
    
    if not success and attempts == max_attempts and "修为不足以突破" not in result_msg:
        result_msg += f"连续渡厄突破失败，未能突破成功。"
    
    # 更新突破CD
    sql_message.updata_level_cd(user_id)
    
    # 添加消耗统计
    result_msg += f"\n本次连续突破共消耗{pills_used}个渡厄丹，剩余{dr_pill_count - pills_used}个"
    record_level_up_result(
        user_id, "连续渡厄突破", attempts=attempts, success=success,
        target_level=target_level, fail_count=fail_count,
        item_name="渡厄丹", item_count=pills_used
    )
    
    await handle_send(bot, event, result_msg)
    await handle_send(bot, event, result_msg, md_type="修仙", k1="速锁", v1="突破", k2="存档", v2="我的修仙信息", k3="修为", v3="我的修为")
    await level_up_dr_lx.finish()

@level_up_drjd_lx.handle(parameterless=[Cooldown(stamina_cost=15)])
async def level_up_drjd_lx_continuous(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """连续渡厄金丹突破5次"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await level_up_drjd_lx.finish()
    
    user_id = user_info['user_id']
    if user_info['hp'] is None:
        sql_message.update_user_hp(user_id)
    
    user_msg = sql_message.get_user_info_with_id(user_id)
    level_cd = user_msg['level_up_cd']
    
    # 检查突破CD
    if level_cd:
        time_now = datetime.now()
        cd = OtherSet().date_diff(time_now, level_cd)
        if cd < XiuConfig().level_up_cd * 60:
            msg = f"目前无法突破，还需要{XiuConfig().level_up_cd - (cd // 60)}分钟"
            sql_message.update_user_stamina(user_id, 15, 1)
            await handle_send(bot, event, msg, md_type="修仙", k1="直接突破", v1="直接突破", k2="渡厄", v2="渡厄突破", k3="修为", v3="我的修为")
            await level_up_drjd_lx.finish()

    level_name = user_msg['level']  # 用户境界
    levels = convert_rank('江湖好手')[1]
    
    # 检查是否需要渡劫
    if level_name.endswith('圆满') and levels.index(level_name) >= levels.index(XiuConfig().tribulation_min_level):
        msg = f"道友当前境界{level_name}需要渡劫才能突破，请使用【渡劫】指令！"
        await handle_send(bot, event, msg, md_type="修仙", k1="渡劫", v1="渡劫", k2="存档", v2="我的修仙信息", k3="修为", v3="我的修为")
        await level_up_drjd_lx.finish()

    level_name = user_msg['level']
    exp = user_msg['exp']
    level_rate = jsondata.level_rate_data()[level_name]
    leveluprate = int(user_msg['level_up_rate'])
    main_rate_buff = UserBuffDate(user_id).get_user_main_buff_data()
    number = main_rate_buff['number'] if main_rate_buff is not None else 0
    
    # 检查渡厄金丹数量（只需要1个即可开始）
    user_backs = sql_message.get_back_msg(user_id) or []
    drjd_pill_count = 0
    for back in user_backs:
        if int(back['goods_id']) == 1998:  # 渡厄金丹ID
            drjd_pill_count = back['goods_num']
            break
    
    if drjd_pill_count < 1:
        msg = f"渡厄金丹突破至少需要1个！"
        await handle_send(bot, event, msg, md_type="修仙", k1="直接突破", v1="直接突破", k2="渡厄", v2="渡厄突破", k3="修为", v3="我的修为")
        await level_up_drjd_lx.finish()
    
    success = False
    result_msg = ""
    attempts = 0
    pills_used = 0
    max_attempts = 5
    fail_count = 0
    total_exp_gain = 0
    target_level = None
    
    for i in range(max_attempts):
        attempts += 1
        
        # 检查是否还有渡厄金丹
        if pills_used >= drjd_pill_count:
            result_msg += f"\n第{attempts}次突破：渡厄金丹不足，突破终止！"
            break
        
        le = OtherSet().get_type(exp, level_rate + leveluprate + number, level_name)
        
        if isinstance(le, str):
            if le == "失败":
                # 突破失败，使用渡厄金丹
                pills_used += 1
                sql_message.update_back_j(user_id, 1998, 1)  # 消耗1个渡厄金丹
                
                # 失败增加修为（渡厄金丹特性）
                now_exp = int(int(exp) * 0.1)
                sql_message.update_exp(user_id, now_exp)
                fail_count += 1
                total_exp_gain += now_exp
                
                update_rate = 1 if int(level_rate * XiuConfig().level_up_probability) <= 1 else int(
                    level_rate * XiuConfig().level_up_probability)
                leveluprate += update_rate
                sql_message.update_levelrate(user_id, leveluprate)
                
                result_msg += f"第{attempts}次突破失败，修为增加{number_to(now_exp)}，下次突破成功率增加{update_rate}%\n"
                
                # 检查是否还有丹药继续下一次尝试
                if pills_used >= drjd_pill_count and attempts < max_attempts:
                    result_msg += f"渡厄金丹已用完，无法继续突破！"
                    break
                    
            else:
                # 修为不足或已是最高境界
                result_msg += f"第{attempts}次突破：{le}\n"
                break
        elif isinstance(le, list):
            # 突破成功
            pills_used += 1
            sql_message.update_back_j(user_id, 1998, 1)  # 消耗1个渡厄金丹
            sql_message.updata_level(user_id, le[0])
            share_msg = trigger_breakthrough_relation_rewards(user_id, le[0])
            sql_message.update_power2(user_id)
            sql_message.update_levelrate(user_id, 0)
            sql_message.update_user_hp(user_id)
            
            # 成功增加修为（渡厄金丹特性）
            now_exp = int(int(exp) * 0.1)
            sql_message.update_exp(user_id, now_exp)
            total_exp_gain += now_exp
            
            result_msg += f"第{attempts}次突破成功，达到{le[0]}境界！修为增加{number_to(now_exp)}{share_msg}\n"
            success = True
            target_level = le[0]
            break
    
    if not success and attempts == max_attempts and "修为不足以突破" not in result_msg:
        result_msg += f"连续渡厄金丹突破失败，未能突破成功。"
    
    # 更新突破CD
    sql_message.updata_level_cd(user_id)
    
    result_msg += f"\n本次突破共消耗{pills_used}个渡厄金丹，剩余{drjd_pill_count - pills_used}个"
    record_level_up_result(
        user_id, "连续渡厄金丹突破", attempts=attempts, success=success,
        target_level=target_level, fail_count=fail_count,
        exp_gain=total_exp_gain, item_name="渡厄金丹", item_count=pills_used
    )
    
    await handle_send(bot, event, result_msg)
    await level_up_drjd_lx.finish()

@user_leveluprate.handle(parameterless=[Cooldown(cd_time=0)])
async def user_leveluprate_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """我的突破概率"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await user_leveluprate.finish()
    user_id = user_info['user_id']
    user_msg = sql_message.get_user_info_with_id(user_id)  # 用户信息
    leveluprate = int(user_msg['level_up_rate'])  # 用户失败次数加成
    level_name = user_msg['level']  # 用户境界
    level_rate = jsondata.level_rate_data()[level_name]  # 
    main_rate_buff = UserBuffDate(user_id).get_user_main_buff_data()#功法突破概率提升
    number =  main_rate_buff['number'] if main_rate_buff is not None else 0
    msg = f"道友下一次突破成功概率为{level_rate + leveluprate + number}%"
    await handle_send(bot, event, msg, md_type="修仙", k1="直接突破", v1="直接突破", k2="渡厄", v2="渡厄突破", k3="修为", v3="我的修为")
    await user_leveluprate.finish()
