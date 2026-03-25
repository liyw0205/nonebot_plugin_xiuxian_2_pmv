try:
    import ujson as json
except ImportError:
    import json
import re
from pathlib import Path
from datetime import datetime
import random
import os
from nonebot.rule import Rule
from nonebot import get_bots, get_bot, on_command, require
from nonebot.params import CommandArg
from ..adapter_compat import (
    Bot,
    GROUP,
    Message,
    GroupMessageEvent,
    PrivateMessageEvent,
    MessageSegment
)
from ..xiuxian_utils.lay_out import assign_bot, put_bot, layout_bot_dict, Cooldown
from ..xiuxian_utils.data_source import jsondata
from nonebot.permission import SUPERUSER
from nonebot.log import logger
from ..xiuxian_utils.xiuxian2_handle import (
    XiuxianDateManage ,UserBuffDate, OtherSet, leave_harm_time
)
from ..xiuxian_config import convert_rank, base_rank, XiuConfig, JsonConfig
from .makeboss import createboss, createboss_jj, create_all_bosses
from .bossconfig import get_boss_config
from .old_boss_info import old_boss_info
from ..xiuxian_utils.player_fight import Boss_fight
from ..xiuxian_utils.item_json import Items
items = Items()
from ..xiuxian_utils.utils import (
    number_to, check_user, check_user_type,
    get_msg_pic,
    send_msg_handler, log_message, handle_send, update_statistics_value
)
from .boss_limit import boss_limit, player_data_manager
from .. import DRIVER
# boss定时任务
scheduler = require("nonebot_plugin_apscheduler").scheduler

conf_data = JsonConfig().read_data()
config = get_boss_config()
group_boss = {}
groups = config['open']
battle_flag = {}
sql_message = XiuxianDateManage()  # sql类
BOSSDROPSPATH = Path() / "data" / "xiuxian" / "boss掉落物"

create = on_command("世界BOSS生成", aliases={"世界boss生成", "世界Boss生成", "生成世界BOSS", "生成世界boss", "生成世界Boss"}, permission=SUPERUSER, priority=5, block=True)
generate_all = on_command("世界BOSS全部生成", aliases={"世界boss全部生成", "世界Boss全部生成", "生成全部世界BOSS", "生成全部世界boss", "生成全部世界Boss"}, permission=SUPERUSER, priority=5, block=True)
create_appoint = on_command("世界BOSS指定生成", aliases={"世界boss指定生成", "世界Boss指定生成", "指定生成世界BOSS", "指定生成世界boss", "指定生成世界Boss"}, permission=SUPERUSER, priority=5)
boss_info = on_command("世界BOSS查询", aliases={"世界boss查询", "世界Boss查询", "查询世界BOSS", "查询世界boss", "查询世界Boss"}, priority=6, block=True)
boss_info2 = on_command("世界BOSS列表", aliases={"世界boss列表", "世界Boss列表"}, priority=6, block=True)
battle = on_command("世界BOSS讨伐", aliases={"世界boss讨伐", "世界Boss讨伐", "讨伐世界BOSS", "讨伐世界boss", "讨伐世界Boss"}, priority=6, block=True)
boss_help = on_command("世界BOSS帮助", aliases={"世界boss帮助", "世界Boss帮助"}, priority=5, block=True)
boss_admin = on_command("世界BOSS管理", aliases={"世界boss管理", "世界Boss管理"}, priority=5, block=True)
boss_delete = on_command("世界BOSS天罚", aliases={"世界boss天罚", "世界Boss天罚", "天罚世界BOSS", "天罚世界boss", "天罚世界Boss"}, permission=SUPERUSER, priority=7, block=True)
boss_delete_all = on_command("世界BOSS全部天罚", aliases={"世界boss全部天罚", "世界Boss全部天罚", "天罚全部世界BOSS", "天罚全部世界boss", "天罚全部世界Boss"}, permission=SUPERUSER, priority=5, block=True)
boss_integral_info = on_command("世界BOSS信息", aliases={"世界boss信息", "世界Boss信息"}, priority=10, block=True)
boss_integral_store = on_command("世界BOSS商店", aliases={"世界boss商店", "世界Boss商店", "世界boss积分商店", "世界Boss积分商店", "世界BOSS积分商店"}, priority=10, block=True)
boss_integral_use = on_command("世界BOSS兑换", aliases={"世界boss兑换", "世界Boss兑换"}, priority=6, block=True)
boss_integral_rank = on_command("世界BOSS积分排行榜", aliases={"世界boss积分排行榜", "世界BOSS排行榜", "世界boss排行榜"}, priority=6, block=True)
challenge_scarecrow = on_command("挑战稻草人", aliases={"挑战稻草人", "挑战稻草人"}, priority=6, block=True)
challenge_training_puppet = on_command("挑战训练傀儡", aliases={"挑战训练傀儡", "挑战训练傀儡"}, priority=6, block=True)

__boss_help__ = f"""
世界BOSS系统帮助

🔹🔹 查询指令：
  ▶ 查询世界BOSS - 查看BOSS列表
  ▶ 世界BOSS列表 [页码] - 分页查看BOSS详情
  ▶ 世界BOSS信息 - 查看个人信息
  ▶ 世界BOSS积分排行榜 - 查看排行榜
  ▶ 世界BOSS商店 - 查看可兑换物品

🔹🔹 战斗指令：
  ▶ 讨伐世界BOSS [编号] - 挑战指定BOSS
  ▶ 挑战稻草人 - 练习战斗技巧（无消耗）
  ▶ 挑战训练傀儡 [境界] [名称] - 自定义训练对手

【特色功能】
🌟 境界压制系统：高境界打低境界BOSS收益降低
🌟 积分兑换商店：用战斗积分兑换珍稀道具
🌟 随机掉落系统：击败BOSS有机会获得特殊物品
🌟 自动刷新机制：每小时自动清理部分BOSS

【注意事项】
⚠ 全服定时自动生成BOSS
⚠ 重伤状态下无法挑战BOSS
⚠ 世界积分可永久保存，请合理使用

输入具体指令查看详细用法，祝道友斩妖除魔，早日得道！
""".strip()

__boss_help__2 = f"""
世界BOSS系统管理

🔹🔹 生成指令：
  ▶ 世界BOSS生成 [数量] - 生成随机境界BOSS
  ▶ 世界BOSS指定生成 [境界] [名称] - 生成指定BOSS
  ▶ 世界BOSS全部生成 - 一键生成所有境界BOSS

🔹🔹 管理指令：
  ▶ 天罚世界BOSS [编号] - 删除指定BOSS
  ▶ 天罚全部世界BOSS - 清空所有BOSS
  ▶ 重置世界BOSS - 重置所有玩家世界BOSS额度
""".strip()

@DRIVER.on_startup
async def read_boss_():
    global group_boss
    group_boss.update(old_boss_info.read_boss_info())
    logger.opt(colors=True).info(f"<green>历史boss数据读取成功</green>")


@DRIVER.on_startup
async def set_boss_generation():
    try:
        # 根据配置的时间参数执行自动生成全部BOSS
        hours = config['Boss生成时间参数']['hours']
        minutes = config['Boss生成时间参数']['minutes']
        
        # 计算总分钟数
        total_minutes = hours * 60 + minutes
        
        if total_minutes > 0:
            scheduler.add_job(
                func=generate_all_bosses_task,
                trigger='interval',
                minutes=total_minutes,
                id="generate_all_bosses",
                misfire_grace_time=60
            )
            logger.opt(colors=True).success(f"<green>已开启自动生成全部世界BOSS定时任务，每{hours}小时{minutes}分钟执行一次！</green>")
        else:
            logger.opt(colors=True).warning(f"<yellow>Boss生成时间参数配置为0，不开启自动生成BOSS定时任务</yellow>")
    except Exception as e:
        logger.opt(colors=True).warning(f"<red>警告,自动生成BOSS定时任务加载失败!,{e}!</red>")

async def generate_all_bosses_task():
    global group_boss
    group_id = "000000"  # 全局BOSS存储键
    
    # 生成全部BOSS
    bosses = create_all_bosses()
    group_boss[group_id] = bosses
    old_boss_info.save_boss(group_boss)
    
    # 发送通知
    msg = f"天道循环，已自动生成全部 {len(bosses)} 个境界的世界BOSS！"
    
    # 只向已开启通知的群发送消息
    for notify_group_id in groups:
        if notify_group_id == "000000":
            continue
        bot = get_bot()
        await bot.send_group_msg(group_id=int(notify_group_id), message=msg)

@DRIVER.on_shutdown
async def save_boss_():
    global group_boss
    old_boss_info.save_boss(group_boss)
    logger.opt(colors=True).info(f"<green>boss数据已保存</green>")

async def set_boss_limits_reset():
    boss_limit.reset_limits()
    logger.opt(colors=True).info(f"<green>世界BOSS重置成功！</green>")

@boss_help.handle(parameterless=[Cooldown(cd_time=1.4)])
async def boss_help_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    msg = __boss_help__ 
    await handle_send(bot, event, msg, md_type="世界BOSS", k1="查询", v1="查询世界BOSS", k2="信息", v2="世界BOSS信息", k3="商店", v3="世界BOSS商店")
    await boss_help.finish()

@boss_admin.handle(parameterless=[Cooldown(cd_time=1.4)])
async def boss_admin_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    msg = __boss_help__2 
    await handle_send(bot, event, msg)
    await boss_admin.finish()
    
@boss_delete.handle(parameterless=[Cooldown(cd_time=1.4)])
async def boss_delete_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """天罚世界boss"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    msg = args.extract_plain_text().strip()
    global group_boss
    group_id = "000000"
    boss_num = re.findall(r"\d+", msg)  # boss编号    

    if boss_num:
        boss_num = int(boss_num[0])
    else:
        msg = f"请输入正确的世界Boss编号!"
        await handle_send(bot, event, msg)
        await boss_delete.finish()
    bosss = None
    try:
        bosss = group_boss.get(group_id, [])
    except:
        msg = f"尚未生成世界Boss,请等待世界boss刷新!"
        await handle_send(bot, event, msg)
        await boss_delete.finish()

    if not bosss:
        msg = f"尚未生成世界Boss,请等待世界boss刷新!"
        await handle_send(bot, event, msg)
        await boss_delete.finish()

    index = len(group_boss[group_id])

    if not (0 < boss_num <= index):
        msg = f"请输入正确的世界Boss编号!"
        await handle_send(bot, event, msg)
        await boss_delete.finish()

    group_boss[group_id].remove(group_boss[group_id][boss_num - 1])
    old_boss_info.save_boss(group_boss)
    msg = f"该世界Boss被突然从天而降的神雷劈中,烟消云散了"
    await handle_send(bot, event, msg)
    await boss_delete.finish()


@boss_delete_all.handle(parameterless=[Cooldown(cd_time=1.4)])
async def boss_delete_all_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """天罚全部世界boss"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    msg = args.extract_plain_text().strip()
    global group_boss
    group_id = "000000"        
    bosss = None
    try:
        bosss = group_boss.get(group_id, [])
    except:
        msg = f"尚未生成世界Boss,请等待世界boss刷新!"
        await handle_send(bot, event, msg)
        await boss_delete_all.finish()

    if not bosss:
        msg = f"尚未生成世界Boss,请等待世界boss刷新!"
        await handle_send(bot, event, msg)
        await boss_delete_all.finish()

    group_boss[group_id] = []    
    old_boss_info.save_boss(group_boss)
    msg = f"所有的世界Boss都烟消云散了~~"
    await handle_send(bot, event, msg)
    await boss_delete_all.finish()

@battle.handle(parameterless=[Cooldown(stamina_cost=config['讨伐世界Boss体力消耗'])])
async def battle_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """讨伐世界boss"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    global group_boss 
    group_boss = old_boss_info.read_boss_info()
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await battle.finish()

    user_id = user_info['user_id']
    
    # 检查每日讨伐次数限制
    today_battle_count = boss_limit.get_battle_count(user_id)
    battle_count = 30
    if today_battle_count >= battle_count:
        msg = f"今日讨伐次数已达上限（{battle_count}次），请明日再来！"
        await handle_send(bot, event, msg)
        await battle.finish()
    
    is_type, msg = check_user_type(user_id, 0)  # 需要无状态的用户
    if not is_type:
        await handle_send(bot, event, msg, md_type="0", k2="修仙帮助", v2="修仙帮助", k3="世界BOSS帮助", v3="世界BOSS帮助")
        await battle.finish()
    
    sql_message.update_last_check_info_time(user_id) # 更新查看修仙信息时间
    msg = args.extract_plain_text().strip()
    group_id = "000000"
    boss_num = re.findall(r"\d+", msg)  # boss编号
    
    if boss_num:
        boss_num = int(boss_num[0])
    else:
        msg = f"请输入正确的世界Boss编号!"
        sql_message.update_user_stamina(user_id, 20, 1)
        await handle_send(bot, event, msg, md_type="世界BOSS", k1="再次", v1="讨伐世界BOSS", k2="查询", v2="查询世界BOSS", k3="状态", v3="我的状态")
        await battle.finish()
    
    bosss = None
    try:
        bosss = group_boss.get(group_id, [])
    except:
        msg = f"尚未生成世界Boss,请等待世界boss刷新!"
        sql_message.update_user_stamina(user_id, 20, 1)
        await handle_send(bot, event, msg)
        await battle.finish()

    if not bosss:
        msg = f"尚未生成世界Boss,请等待世界boss刷新!"
        sql_message.update_user_stamina(user_id, 20, 1)
        await handle_send(bot, event, msg)
        await battle.finish()

    index = len(group_boss[group_id])
    if not (0 < boss_num <= index):
        msg = f"请输入正确的世界Boss编号!"
        sql_message.update_user_stamina(user_id, 20, 1)
        await handle_send(bot, event, msg, md_type="世界BOSS", k1="再次", v1="讨伐世界BOSS", k2="查询", v2="查询世界BOSS", k3="状态", v3="我的状态")
        await battle.finish()

    if user_info['hp'] is None or user_info['hp'] == 0:
        sql_message.update_user_hp(user_id)

    if user_info['hp'] <= user_info['exp'] / 10:
        time = leave_harm_time(user_id)
        msg = f"重伤未愈，动弹不得！距离脱离危险还需要{time}分钟！\n"
        msg += f"请道友进行闭关，或者使用药品恢复气血，不要干等，没有自动回血！！！"
        sql_message.update_user_stamina(user_id, 20, 1)
        await handle_send(bot, event, msg, md_type="世界BOSS", k1="闭关", v1="闭关", k2="丹药", v2="丹药背包", k3="状态", v3="我的状态")
        await battle.finish()
    
    user1_sub_buff_data = UserBuffDate(user_info['user_id']).get_user_sub_buff_data()
    exp_buff = user1_sub_buff_data['exp'] if user1_sub_buff_data is not None else 0
    bossinfo = group_boss[group_id][boss_num - 1]
    if bossinfo['jj'] == '零':
        boss_rank = convert_rank((bossinfo['jj']))[0]
    else:
        boss_rank = convert_rank((bossinfo['jj'] + '中期'))[0]
    user_rank = convert_rank(user_info['level'])[0]
    rank_name_list = convert_rank(user_info["level"])[1]
    if boss_rank - user_rank >= 5:
        msg = f"道友已是{user_info['level']}之人，妄图抢小辈的Boss，可耻！"
        sql_message.update_user_stamina(user_id, 20, 1)
        await handle_send(bot, event, msg, md_type="世界BOSS", k1="讨伐", v1="讨伐世界BOSS", k2="查询", v2="查询世界BOSS", k3="列表", v3="世界BOSS列表")
        await battle.finish()
    if user_rank - boss_rank >= 7:
        required_rank_name = rank_name_list[len(rank_name_list) - (boss_rank + 4)]
        msg = f"道友，您的实力尚需提升至{required_rank_name}，目前仅为{user_info['level']}，不宜过早挑战Boss，还请三思。"
        sql_message.update_user_stamina(user_id, 20, 1)
        await handle_send(bot, event, msg, md_type="世界BOSS", k1="讨伐", v1="讨伐世界BOSS", k2="查询", v2="查询世界BOSS", k3="列表", v3="世界BOSS列表")
        await battle.finish()
    
    more_msg = ''
    battle_flag[group_id] = True
    boss_all_hp = bossinfo['总血量']
    boss_old_hp = bossinfo['气血']
    boss_max_stone = bossinfo['max_stone']  # 使用最大灵石计算奖励
    
    # 执行战斗并获取结果
    result, victor, bossinfo_new = await Boss_fight(user_id, bossinfo, bot_id=bot.self_id)
    
    # 计算实际造成的伤害（不超过BOSS最大生命值的20%）
    max_single_damage = boss_all_hp * 0.2  # 单次最大伤害限制
    total_damage = boss_old_hp - bossinfo_new['气血']
    actual_damage = min(boss_old_hp - bossinfo_new['气血'], max_single_damage)
    
    # 更新BOSS血量
    boss_now_hp = max(boss_old_hp - actual_damage, 0)
    bossinfo_new['气血'] = boss_now_hp
    
    # 获取今日已获得的积分和灵石
    today_integral = int(boss_limit.get_integral(user_id))
    today_stone = int(boss_limit.get_stone(user_id))
    
    # 设置每日上限
    integral_limit = 12000
    stone_limit = 300000000
    
    # 初始化奖励变量
    boss_integral = 0
    get_stone = 0
    
    rank_penalty = 1.0
    
    # 检查境界压制（用户境界高于BOSS）
    if user_rank < boss_rank:
        # 境界差越大，衰减越严重
        rank_diff = boss_rank - user_rank
        if rank_diff == 1:
            rank_penalty = 0.95  # 高1个小境界，衰减5%
        elif rank_diff == 2:
            rank_penalty = 0.9  # 高2个小境界，衰减10%
        elif rank_diff == 3:
            rank_penalty = 0.8  # 高3个小境界，衰减20%
        elif rank_diff == 4:
            rank_penalty = 0.7  # 高4个小境界，衰减30%
        else:  # rank_diff >= 4
            rank_penalty = 0.5  # 高4个及以上小境界，衰减50%
    
    damage_ratio = min(total_damage / boss_all_hp, 0.20)
    
    # 境界加成（只有在没有境界压制时才应用）
    if rank_penalty == 1.0:
        boss_integral = int(boss_integral * (1 + (0.3 * (user_rank - boss_rank))))
        points_bonus = int(30 * (user_rank - boss_rank))
        more_msg = f"道友低boss境界{user_rank - boss_rank}层，获得{points_bonus}%积分加成！"
    
    # 应用灵石加成
    stone_buff = user1_sub_buff_data['stone'] if user1_sub_buff_data is not None else 0
    get_stone = int(get_stone * (1 + stone_buff))

    # 应用积分加成
    integral_buff = user1_sub_buff_data['integral'] if user1_sub_buff_data is not None else 0
    boss_integral = int(boss_integral * (1 + integral_buff))

    # 计算积分奖励
    if today_integral >= integral_limit:
        boss_integral = 0
        integral_msg = "今日积分已达上限，无法获得更多积分！"
    else:
        boss_integral = max(int(damage_ratio * 3000), 1)
        # 应用境界压制衰减
        boss_integral = int(boss_integral * rank_penalty)
        boss_integral = min(boss_integral, integral_limit - today_integral)
        if boss_integral <= 0:
            boss_integral = 1
        integral_msg = f"获得世界积分：{boss_integral}点"

    # 计算灵石奖励
    if today_stone >= stone_limit:
        get_stone = 0
        stone_msg = "今日灵石已达上限，无法获得更多灵石！"
    else:
        get_stone = int(boss_max_stone * damage_ratio)
        # 应用境界压制衰减
        get_stone = int(get_stone * rank_penalty)
        get_stone = min(get_stone, stone_limit - today_stone)        
        if get_stone <= 0:
            get_stone = 1
        stone_msg = f"获得灵石{number_to(get_stone)}枚"        

    # 修为奖励
    exp_msg = ""
    if exp_buff > 0 and user_info['root'] != "凡人" and victor == "群友赢了":
        now_exp = int((user_info['exp']) * exp_buff / 10000 * min(0.1 * max(user_rank // 3, 1), 1))
        sql_message.update_exp(user_id, now_exp)
        exp_msg = f"，获得修为{number_to(now_exp)}点！"
    
    # 掉落物品
    drops_id, drops_info = boss_drops(user_rank, boss_rank, bossinfo, user_info)
    drops_msg = ""
    
    # 更新数据
    sql_message.update_ls(user_id, get_stone, 1)
    boss_limit.update_stone(user_id, get_stone)
    
    user_boss_fight_info = get_user_boss_fight_info(user_id)
    user_boss_fight_info['boss_integral'] += boss_integral
    boss_limit.update_integral(user_id, boss_integral)
    save_user_boss_fight_info(user_id, user_boss_fight_info)
    
    if victor == "群友赢了":
        msg = f"恭喜道友击败{bossinfo['name']}，共造成 {number_to(total_damage)} 伤害，{stone_msg}，{more_msg}{integral_msg}{exp_msg}"
        if boss_now_hp >= 0:
            # 移除并生成新BOSS
            group_boss[group_id].remove(group_boss[group_id][boss_num - 1])
            new_boss = createboss_jj(bossinfo['jj'])
        if new_boss:  
            group_boss[group_id].insert(boss_num - 1, new_boss)
        if drops_id and boss_rank < convert_rank('遁一境中期')[0]:           
            drops_msg = f"boss的尸体上好像有什么东西，凑近一看居然是{drops_info['name']}！"
            msg += f"\n{drops_msg}"
            if drops_info['type'] in ["特殊道具", "神物"]:
                sql_message.send_back(user_info['user_id'], drops_info['id'], drops_info['name'], drops_info['type'], 1, 1)            
            else:
                sql_message.send_back(user_info['user_id'], drops_info['id'], drops_info['name'], drops_info['type'], 1)
    else:
        msg = f"道友不敌{bossinfo['name']}，共造成 {number_to(total_damage)} 伤害，重伤逃遁，临逃前{stone_msg}，{more_msg}{integral_msg}"
        # 更新BOSS状态（不扣除灵石）
        group_boss[group_id][boss_num - 1] = bossinfo_new
        roll = random.randint(1, 100)
        if drops_id and boss_rank < convert_rank('遁一境中期')[0] and roll > 50:           
            drops_msg = f"路上好像有什么东西，凑近一看居然是{drops_info['name']}！"
            msg += f"\n{drops_msg}"
            if drops_info['type'] in ["特殊道具", "神物"]:
                sql_message.send_back(user_info['user_id'], drops_info['id'], drops_info['name'], drops_info['type'], 1, 1)            
            else:
                sql_message.send_back(user_info['user_id'], drops_info['id'], drops_info['name'], drops_info['type'], 1)
    
    if user_info['root'] == "凡人" and boss_integral < 0:
        msg += f"\n如果出现负积分，说明你境界太高了，玩凡人就不要那么高境界了！！！"
    
    old_boss_info.save_boss(group_boss)
    battle_flag[group_id] = False
    # 更新讨伐次数
    boss_limit.update_battle_count(user_id)
    update_statistics_value(user_id, "讨伐世界BOSS")
    await send_msg_handler(bot, event, result)
    if drops_id:
        await handle_send(bot, event, msg, md_type="世界BOSS", k1="再次", v1=f"讨伐世界BOSS{boss_num}", k2="查询", v2=f"查询世界BOSS{boss_num}", k3="物品", v3=f"查看效果 {drops_info['name']}")
    else:
        await handle_send(bot, event, msg, md_type="世界BOSS", k1="再次", v1=f"讨伐世界BOSS{boss_num}", k2="查询", v2=f"查询世界BOSS{boss_num}", k3="状态", v3="我的状态")
    log_message(user_id, msg)
    await battle.finish()

@challenge_scarecrow.handle(parameterless=[Cooldown(stamina_cost=1, cd_time=30)])
async def challenge_scarecrow_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """挑战稻草人"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    group_id = "000000"
    isUser, user_info, msg = check_user(event)
    sql_message = XiuxianDateManage()

    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await challenge_scarecrow.finish()

    user_id = user_info['user_id']
    sql_message.update_last_check_info_time(user_id)

    # 检查用户状态
    if user_info['hp'] is None or user_info['hp'] == 0:
        sql_message.update_user_hp(user_id)
    if user_info['hp'] <= user_info['exp'] / 10:
        time = leave_harm_time(user_id)
        msg = f"重伤未愈，动弹不得！距离脱离危险还需要{time}分钟！\n"
        msg += f"请道友进行闭关，或者使用药品恢复气血，不要干等，没有自动回血！！！"
        sql_message.update_user_stamina(user_id, 20, 1)
        await handle_send(bot, event, msg, md_type="世界BOSS", k1="闭关", v1="闭关", k2="丹药", v2="丹药背包", k3="状态", v3="我的状态")
        await challenge_scarecrow.finish()

    scarecrow_hp = int(jsondata.level_data()["至高"]["power"]) * 10000

    # 定义稻草人属性（固定）
    scarecrow_info = {
            "气血": scarecrow_hp,
            "总血量": scarecrow_hp,
            "真元": 100,
            "攻击": 0,
            "name": "稻草人",
            "jj": "感气境",
            "is_scarecrow": True
        }

    # 战斗逻辑
    battle_flag[group_id] = True
    boss_all_hp = scarecrow_info['总血量']
    # 打之前的血量
    boss_old_hp = scarecrow_info['气血']
    result, victor, bossinfo_new = await Boss_fight(user_id, scarecrow_info, type_in=1, bot_id=bot.self_id)      
    # 打之后的血量
    boss_now_hp = bossinfo_new['气血']
    # 计算总伤害
    total_damage = boss_old_hp - boss_now_hp
    # 输出结果并处理奖励
    if victor == "群友赢了":
        msg = f"奇迹！道友击败了稻草人，共造成 {number_to(total_damage)} 伤害！不过它又站起来了，继续等待挑战者！"
    else:
        msg = f"道友挑战稻草人，奋力攻击后共造成 {number_to(total_damage)} 伤害，稻草人岿然不动，继续等待挑战者！"

    battle_flag[group_id] = False

    await send_msg_handler(bot, event, result)
    await handle_send(bot, event, msg, md_type="世界BOSS", k1="再次", v1="挑战稻草人", k2="丹药", v2="丹药背包", k3="状态", v3="我的状态")
    await challenge_scarecrow.finish()


@challenge_training_puppet.handle(parameterless=[Cooldown(stamina_cost=1, cd_time=30)])
async def challenge_training_puppet_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """挑战训练傀儡"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    group_id = "000000"
    isUser, user_info, msg = check_user(event)
    sql_message = XiuxianDateManage()

    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await challenge_training_puppet.finish()

    user_id = user_info['user_id']
    sql_message.update_last_check_info_time(user_id)

    # 检查用户状态
    if user_info['hp'] is None or user_info['hp'] == 0:
        sql_message.update_user_hp(user_id)
    if user_info['hp'] <= user_info['exp'] / 10:
        time = leave_harm_time(user_id)
        msg = f"重伤未愈，动弹不得！距离脱离危险还需要{time}分钟！\n"
        msg += f"请道友进行闭关，或者使用药品恢复气血，不要干等，没有自动回血！！！"
        sql_message.update_user_stamina(user_id, 20, 1)
        await handle_send(bot, event, msg, md_type="世界BOSS", k1="闭关", v1="闭关", k2="丹药", v2="丹药背包", k3="状态", v3="我的状态")
        await challenge_training_puppet.finish()

    
    arg_list = args.extract_plain_text().split()
    boss_name = "散发着威压的尸体"
    if len(arg_list) == 0:
        # 根据玩家的大境界确定训练傀儡的境界
        player_jj = (user_info['level'])
        scarecrow_jj = player_jj[:3]
        if player_jj == "江湖好手":
            scarecrow_jj = "感气境"
    if len(arg_list) >= 1:
        scarecrow_jj = arg_list[0]  # 用户指定的境界
        if len(arg_list) == 2:
            boss_name = arg_list[1]

    player = sql_message.get_player_data(user_id)
    bossinfo = createboss_jj(scarecrow_jj, boss_name)
    if bossinfo is None:
        boss_name = "散发着威压的尸体"
        scarecrow_jj = "祭道境"
        bossinfo = createboss_jj(scarecrow_jj, boss_name)

    # 计算训练傀儡的属性
    scarecrow_atk = (player['攻击'] // 2)
    scarecrow_mp = (player['真元'] // 2)
    scarecrow_hp = (player['气血'] * 100)

    # 定义训练傀儡属性
    scarecrow_info = {
        "气血": scarecrow_hp,
        "总血量": scarecrow_hp,
        "真元": scarecrow_mp,
        "攻击": scarecrow_atk,
        "name": boss_name,
        "jj": scarecrow_jj
    }

    # 战斗逻辑
    battle_flag[group_id] = True
    boss_all_hp = scarecrow_info['总血量']
    # 打之前的血量
    boss_old_hp = scarecrow_info['气血']
    result, victor, bossinfo_new = await Boss_fight(user_id, scarecrow_info, type_in=1, bot_id=bot.self_id)      
    # 打之后的血量
    boss_now_hp = bossinfo_new['气血']
    # 计算总伤害
    total_damage = boss_old_hp - boss_now_hp
    # 输出结果并处理奖励
    if victor == "群友赢了":

        msg = f"奇迹！道友击败了训练傀儡，共造成 {number_to(total_damage)} 伤害，！不过它又站起来了，继续等待挑战者！"
    elif victor == "Boss赢了":
        msg = f"道友挑战训练傀儡，奋力攻击后共造成 {number_to(total_damage)} 伤害，训练傀儡岿然不动，继续等待挑战者！"

    battle_flag[group_id] = False
    await send_msg_handler(bot, event, result)
    await handle_send(bot, event, msg, md_type="世界BOSS", k1="再次", v1="挑战训练傀儡", k2="丹药", v2="丹药背包", k3="状态", v3="我的状态")
    await challenge_training_puppet.finish()
    
    
@boss_info.handle(parameterless=[Cooldown(cd_time=1.4)])
async def boss_info_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """查询世界boss"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    group_id = "000000"
    global group_boss 
    group_boss = old_boss_info.read_boss_info()    
    bosss = None
    try:
        bosss = group_boss.get(group_id, [])
    except:
        msg = f"尚未生成世界Boss,请等待世界boss刷新!"
        await handle_send(bot, event, msg)
        await boss_info.finish()

    msg = args.extract_plain_text().strip()
    boss_num = re.findall(r"\d+", msg)  # boss编号

    if not bosss:
        msg = f"尚未生成世界Boss,请等待世界boss刷新!"
        await handle_send(bot, event, msg)
        await boss_info.finish()

    Flag = False  # True查对应Boss
    if boss_num:
        boss_num = int(boss_num[0])
        index = len(group_boss[group_id])
        if not (0 < boss_num <= index):
            msg = f"请输入正确的世界Boss编号!"
            await handle_send(bot, event, msg, md_type="世界BOSS", k1="查询", v1="查询世界BOSS", k2="列表", v2="世界BOSS列表", k3="状态", v3="我的状态")
            await boss_info.finish()

        Flag = True

    bossmsgs = ""
    if Flag:  # 查单个Boss信息
        boss = group_boss[group_id][boss_num - 1]
        bossmsgs = f'''
世界Boss:{boss['name']}
境界：{boss['jj']}
总血量：{number_to(boss['总血量'])}
剩余血量：{number_to(boss['气血'])}
攻击：{number_to(boss['攻击'])}
携带灵石：{number_to(boss['stone'])}
        '''
        msg = bossmsgs
        if int(boss["气血"] / boss["总血量"]) < 0.5:
            boss_name = boss["name"] + "_c"
        else:
            boss_name = boss["name"]
        pic = await get_msg_pic(f"@{event.sender.nickname}\n" + msg, boss_name=boss_name)
        await bot.send(event=event, message=MessageSegment.image(bot, pic))
        await boss_info.finish()
    else:
        i = 1
        for boss in bosss:
            bossmsgs += f"编号{i}、{boss['jj']}Boss:{boss['name']} \n"
            i += 1
        msg = bossmsgs
        await handle_send(bot, event, msg, md_type="世界BOSS", k1="讨伐", v1="讨伐世界BOSS", k2="查询", v2="查询世界BOSS", k3="状态", v3="我的状态")
        await boss_info.finish()
        
        
@boss_info2.handle(parameterless=[Cooldown(cd_time=1.4)])
async def boss_info2_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """查询世界boss"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    group_id = "000000"
    global group_boss 
    group_boss = old_boss_info.read_boss_info()    
    bosss = None
    try:
        bosss = group_boss.get(group_id, [])
    except:
        msg = f"尚未生成世界Boss,请等待世界boss刷新!"
        await handle_send(bot, event, msg)
        await boss_info2.finish()

    arg_list = args.extract_plain_text().strip()

    if not bosss:
        msg = f"尚未生成世界Boss,请等待世界boss刷新!"
        await handle_send(bot, event, msg)
        await boss_info2.finish()

    per_page = 5
    total_items = len(bosss)  # 总BOSS数量
    total_pages = (total_items + per_page - 1) // per_page
    
    current_page = re.findall(r"\d+", arg_list)
    if current_page:
        current_page = int(current_page[0])
    else:
        current_page = 1
    if current_page < 1 or current_page > total_pages:
        msg = f"页码错误，有效范围为1~{total_pages}页！"
        await handle_send(bot, event, msg, md_type="世界BOSS", k1="列表", v1="世界BOSS列表1", k2="讨伐", v2="讨伐世界BOSS", k3="状态", v3="我的状态")
        await boss_info2.finish()
    start_index = (current_page - 1) * per_page
    end_index = start_index + per_page
    paged_bosses = bosss[start_index:end_index]
    title = f"世界BOSS列表（第{current_page}/{total_pages}页）"
    footer = f"提示：发送 世界BOSS列表+页码 查看其他页（共{total_pages}页）"
    paged_msgs = []
    for i, boss in enumerate(paged_bosses, start=start_index + 1):
        paged_msgs.append(f"编号{i} \nBoss:{boss['name']} \n境界：{boss['jj']} \n总血量：{number_to(boss['总血量'])} \n剩余血量：{number_to(boss['气血'])} \n攻击：{number_to(boss['攻击'])} \n携带灵石：{number_to(boss['stone'])}")
    paged_msgs.append(footer)
    await send_msg_handler(bot, event, f'世界BOSS列表 - 第{current_page}页', bot.self_id, paged_msgs, title=title)
    await boss_info2.finish()

@generate_all.handle(parameterless=[Cooldown(cd_time=1.4)])
async def generate_all_bosses(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    bosses = create_all_bosses()  # 自动计算最高境界
    group_boss["000000"] = bosses  # 替换当前 BOSS 列表
    old_boss_info.save_boss(group_boss)
    await bot.send(event, f"已生成全部 {len(bosses)} 个境界的 BOSS！")


@create.handle(parameterless=[Cooldown(cd_time=1.4)])
async def create_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """生成世界boss - 每个境界只生成一个"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    group_id = "000000"    

    try:
        group_boss[group_id]
    except:
        group_boss[group_id] = []

    boss_jj = createboss()
    for boss in group_boss[group_id][:]:
        if boss['jj'] == boss_jj:
            group_boss[group_id].remove(boss)
            break
    
    bossinfo = createboss_jj(boss_jj)
    
    group_boss[group_id].append(bossinfo)
    old_boss_info.save_boss(group_boss)
    msg = f"已生成{boss_jj}Boss:{bossinfo['name']}，诸位道友请击败Boss获得奖励吧!"
    await handle_send(bot, event, msg)
    await create.finish()

@create_appoint.handle(parameterless=[Cooldown(cd_time=1.4)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """生成指定世界boss - 替换同境界BOSS"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    group_id = "000000"    

    try:
        group_boss[group_id]
    except:
        group_boss[group_id] = []

    # 解析参数
    arg_list = args.extract_plain_text().split()
    if len(arg_list) < 1:
        msg = f"请输入正确的指令，例如：生成指定世界boss 祭道境 少姜"
        await handle_send(bot, event, msg)
        await create_appoint.finish()

    boss_jj = arg_list[0]  # 用户指定的境界
    boss_name = arg_list[1] if len(arg_list) > 1 else None  # 用户指定的Boss名称

    # 检查是否已有同境界BOSS，有则删除
    for boss in group_boss[group_id][:]:
        if boss['jj'] == boss_jj:
            group_boss[group_id].remove(boss)
            break

    # 生成指定BOSS
    bossinfo = createboss_jj(boss_jj, boss_name)
    if bossinfo is None:
        msg = f"请输入正确的境界，例如：生成指定世界boss 祭道境"
        await handle_send(bot, event, msg)
        await create_appoint.finish()

    group_boss[group_id].append(bossinfo)
    old_boss_info.save_boss(group_boss)
    msg = f"已生成{boss_jj}Boss:{bossinfo['name']}，诸位道友请击败Boss获得奖励吧！"
    await handle_send(bot, event, msg)
    await create_appoint.finish()

@boss_integral_store.handle(parameterless=[Cooldown(cd_time=1.4)])
async def boss_integral_store_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """世界积分商店"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await boss_integral_store.finish()

    user_id = user_info['user_id']    
    user_boss_fight_info = get_user_boss_fight_info(user_id)
    boss_integral_shop = config['世界积分商品']
    
    # 获取页码参数
    arg = args.extract_plain_text().strip()
    page = 1
    if arg.isdigit():
        page = int(arg)
    
    # 分页设置
    per_page = 10  # 每页显示10个商品
    total_items = len(boss_integral_shop)
    total_pages = (total_items + per_page - 1) // per_page
    
    # 检查页码是否有效
    if page < 1 or page > total_pages:
        msg = f"页码错误，有效范围为1~{total_pages}页！"
        await handle_send(bot, event, msg)
        await boss_integral_store.finish()
    
    # 构建消息
    title = f"道友目前拥有的世界积分：{user_boss_fight_info['boss_integral']}点"
    l_msg = []
    l_msg.append(f"════════════\n【世界积分商店】第{page}/{total_pages}页")
    
    if boss_integral_shop != {}:
        # 计算当前页的商品范围
        start_index = (page - 1) * per_page
        end_index = min(start_index + per_page, total_items)
        
        # 获取当前页的商品
        shop_items = list(boss_integral_shop.items())[start_index:end_index]
        
        for item_id, item_info in shop_items:
            item_data = items.get_data_by_item_id(item_id)
            weekly_limit = item_info.get('weekly_limit', 1)
            already_purchased = boss_limit.get_weekly_purchases(user_id, item_id)
            msg = f"编号:{item_id}\n"
            msg += f"名字：{item_data['name']}\n"
            msg += f"描述：{item_data.get('desc', '暂无描述')}\n"
            msg += f"所需世界积分：{item_info['cost']}点\n"
            msg += f"每周限购：{weekly_limit - already_purchased}/{weekly_limit}个\n"
            msg += f"════════════"
            l_msg.append(msg)
    else:
        l_msg.append(f"世界积分商店内空空如也！")

    l_msg.append(f"提示：发送 世界BOSS商店+页码 查看其他页（共{total_pages}页）")
    page = ["翻页", f"世界BOSS商店 {page + 1}", "信息", "世界BOSS信息", "兑换", "世界BOSS兑换", f"{page}/{total_pages}"]
    await send_msg_handler(bot, event, '世界积分商店', bot.self_id, l_msg, title=title, page=page)
    await boss_integral_store.finish()

@boss_integral_info.handle(parameterless=[Cooldown(cd_time=1.4)])
async def boss_integral_info_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """世界BOSS信息"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await boss_integral_info.finish()
    
    user_id = user_info['user_id']    
    user_boss_fight_info = get_user_boss_fight_info(user_id)
    
    # 获取今日已获得的积分和灵石和讨伐次数
    today_integral = int(boss_limit.get_integral(user_id))
    today_stone = int(boss_limit.get_stone(user_id))
    today_battle_count = boss_limit.get_battle_count(user_id)
    
    # 设置每日上限
    integral_limit = 12000
    stone_limit = 300000000
    battle_count = 30
    
    # 构建消息
    msg = f"""
════════════
当前世界积分：{user_boss_fight_info['boss_integral']}点
════════════
今日已获积分：{today_integral}/{integral_limit}点
今日已获灵石：{number_to(today_stone)}/{number_to(stone_limit)}枚
今日讨伐次数：{today_battle_count}/{battle_count}次
════════════
提示：每日0点重置获取上限
""".strip()
    
    await handle_send(bot, event, msg, md_type="世界BOSS", k1="商店", v1="世界BOSS商店", k2="查询", v2="查询世界BOSS", k3="列表", v3="世界BOSS列表")
    await boss_integral_info.finish()
    
@boss_integral_use.handle(parameterless=[Cooldown(cd_time=1.4)])
async def boss_integral_use_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """世界积分商店兑换"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await boss_integral_use.finish()

    user_id = user_info['user_id']
    msg = args.extract_plain_text().strip()
    shop_info = re.findall(r"(\d+)\s*(\d*)", msg)
    
    if shop_info:
        shop_id = int(shop_info[0][0])
        quantity = int(shop_info[0][1]) if shop_info[0][1] else 1
    else:
        msg = f"请输入正确的商品编号！"
        await handle_send(bot, event, msg, md_type="世界BOSS", k1="兑换", v1="世界BOSS兑换", k2="商店", v2="世界BOSS商店", k3="信息", v3="世界BOSS信息")
        await boss_integral_use.finish()

    boss_integral_shop = config['世界积分商品']
    is_in = False
    cost = None
    item_id = None
    weekly_limit = None
    
    if boss_integral_shop:
        if str(shop_id) in boss_integral_shop:
            is_in = True
            cost = boss_integral_shop[str(shop_id)]['cost']
            weekly_limit = boss_integral_shop[str(shop_id)].get('weekly_limit', 1)
            item_id = shop_id
            item_info = Items().get_data_by_item_id(item_id)
    else:
        msg = f"世界积分商店内空空如也！"
        await handle_send(bot, event, msg)
        await boss_integral_use.finish()
        
    if is_in:
        # 检查每周限购
        already_purchased = boss_limit.get_weekly_purchases(user_id, shop_id)
        max_quantity = weekly_limit - already_purchased
        if quantity > max_quantity:
            quantity = max_quantity
        if quantity <= 0:
            msg = f"{item_info['name']}已到限购无法再购买！"
            await handle_send(bot, event, msg, md_type="世界BOSS", k1="兑换", v1="世界BOSS兑换", k2="商店", v2="世界BOSS商店", k3="信息", v3="世界BOSS信息")
            await boss_integral_use.finish()
            
        user_boss_fight_info = get_user_boss_fight_info(user_id)
        total_cost = cost * quantity
        
        if user_boss_fight_info['boss_integral'] < total_cost:
            msg = f"道友的世界积分不满足兑换条件呢"
            await handle_send(bot, event, msg, md_type="世界BOSS", k1="兑换", v1="世界BOSS兑换", k2="商店", v2="世界BOSS商店", k3="信息", v3="世界BOSS信息")
            await boss_integral_use.finish()
        else:
            user_boss_fight_info['boss_integral'] -= total_cost
            save_user_boss_fight_info(user_id, user_boss_fight_info)
            
            # 更新每周购买记录
            boss_limit.update_weekly_purchase(user_id, shop_id, quantity)
           
            sql_message.send_back(user_id, item_id, item_info['name'], item_info['type'], quantity, 1)
            msg = f"道友成功兑换获得：{item_info['name']}{quantity}个"
            await handle_send(bot, event, msg, md_type="世界BOSS", k1="兑换", v1="世界BOSS兑换", k2="商店", v2="世界BOSS商店", k3="信息", v3="世界BOSS信息")
            await boss_integral_use.finish()
    else:
        msg = f"该编号不在商品列表内哦，请检查后再兑换"
        await handle_send(bot, event, msg, md_type="世界BOSS", k1="兑换", v1="世界BOSS兑换", k2="商店", v2="世界BOSS商店", k3="信息", v3="世界BOSS信息")
        await boss_integral_use.finish()

@boss_integral_rank.handle(parameterless=[Cooldown(cd_time=1.4)])
async def boss_integral_rank_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """世界BOSS积分排行榜"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await boss_integral_rank.finish()

    # 获取所有用户的boss_integral数据
    all_user_integral = player_data_manager.get_all_field_data("integral", "boss_integral")
    
    # 排序数据
    sorted_integral = sorted(all_user_integral, key=lambda x: x[1], reverse=True)
    
    # 生成排行榜
    rank_msg = "✨【世界BOSS积分排行榜】✨\n"
    rank_msg += "-----------------------------------\n"
    for i, (user_id, integral) in enumerate(sorted_integral[:50], start=1):
        user_info = sql_message.get_user_info_with_id(user_id)
        rank_msg += f"第{i}位 | {user_info['user_name']} | {number_to(integral)}\n"
    
    await handle_send(bot, event, rank_msg)
    await boss_integral_rank.finish()

def get_user_boss_fight_info(user_id):
    boss_integral = player_data_manager.get_field_data(str(user_id), "boss_limit", "integral")
    if boss_integral is None:
        boss_integral = 0
    user_boss_fight_info = {"boss_integral": boss_integral}
    return user_boss_fight_info

def save_user_boss_fight_info(user_id, data):
    user_id = str(user_id)
    player_data_manager.update_or_write_data(user_id, "boss_limit", "integral", data["boss_integral"])

BOSSDROPSPATH = Path() / "data" / "xiuxian" / "boss掉落物" / "boss掉落物.json"

class BossDrops:
    def __init__(self):
        self.drops_data = self.load_drops_data()
        
    def load_drops_data(self):
        """加载掉落物数据"""
        try:
            with open(BOSSDROPSPATH, "r", encoding="UTF-8") as f:
                return json.load(f)
        except Exception as e:
            print(f"加载BOSS掉落物数据失败: {e}")
            return {}
    
    def get_drop_by_id(self, drop_id):
        """通过ID获取掉落物"""
        return self.drops_data.get(str(drop_id))
    
    def get_random_drop(self, user_level):
        """
        根据用户等级随机获取一个掉落物
        :param user_level: 用户境界等级
        :return: (掉落物ID, 掉落物信息)
        """
        if not self.drops_data:
            return None, None
            
        # 计算适合用户等级的掉落物范围
        zx_rank = base_rank(user_level, 5)
        # 筛选符合条件的掉落物
        eligible_drops = []
        for drop_id, drop_info in self.drops_data.items():
            if drop_info.get('rank', 0) >= zx_rank:
                eligible_drops.append((drop_id, drop_info))
                
        if not eligible_drops:
            return None, None
            
        return random.choice(eligible_drops)

def boss_drops(user_rank, boss_rank, boss, user_info):
    """
    改进后的BOSS掉落函数
    :param user_rank: 用户境界等级
    :param boss_rank: BOSS境界等级
    :param boss: BOSS信息
    :param user_info: 用户信息
    :return: (掉落物ID, 掉落物信息) 或 (None, None)
    """
    drops_system = BossDrops()
    
    # 基础掉落概率检查(10%)
    roll = random.randint(1, 100)
    if roll >= 10: 
        return None, None
        
    # 境界差距过大时极低概率掉落(5%)
    if boss_rank - user_rank >= 4 or user_rank - boss_rank >= 4:
        roll = random.randint(1, 100)
        if roll >= 5: 
            return None, None
        
    # 获取随机掉落物
    drop_id, drop_info = drops_system.get_random_drop(user_info['level'])
    
    return drop_id, drop_info
