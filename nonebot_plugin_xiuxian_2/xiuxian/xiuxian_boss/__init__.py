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
from nonebot.adapters.onebot.v11 import (
    Bot,
    GROUP,
    Message,
    GroupMessageEvent,
    PrivateMessageEvent,
    GROUP_ADMIN,
    GROUP_OWNER,
    ActionFailed,
    MessageSegment
)
from ..xiuxian_utils.lay_out import assign_bot, put_bot, layout_bot_dict, Cooldown
from ..xiuxian_utils.data_source import jsondata
from nonebot.permission import SUPERUSER
from nonebot.log import logger
from ..xiuxian_utils.xiuxian2_handle import (
    XiuxianDateManage ,OtherSet, UserBuffDate,
    XIUXIAN_IMPART_BUFF, leave_harm_time
)
from ..xiuxian_config import convert_rank, XiuConfig, JsonConfig
from .makeboss import createboss, createboss_jj, create_all_bosses
from .bossconfig import get_boss_config, savef_boss
from .old_boss_info import old_boss_info
from ..xiuxian_utils.player_fight import Boss_fight
from ..xiuxian_utils.item_json import Items
items = Items()
from ..xiuxian_utils.utils import (
    number_to, check_user, check_user_type,
    get_msg_pic, CommandObjectID,
    pic_msg_format, send_msg_handler, handle_send
)
from .. import DRIVER
# boss定时任务
require('nonebot_plugin_apscheduler')
from nonebot_plugin_apscheduler import scheduler

conf_data = JsonConfig().read_data()
config = get_boss_config()
cache_help = {}
del_boss_id = XiuConfig().del_boss_id
gen_boss_id = XiuConfig().gen_boss_id
group_boss = {}
groups = config['open']
battle_flag = {}
sql_message = XiuxianDateManage()  # sql类
xiuxian_impart = XIUXIAN_IMPART_BUFF()
BOSSDROPSPATH = Path() / "data" / "xiuxian" / "boss掉落物"

create = on_command("生成世界boss", aliases={"生成世界Boss", "生成世界BOSS"}, permission=SUPERUSER, priority=5, block=True)
generate_all = on_command("生成全部世界boss", aliases={"生成全部世界Boss", "生成全部世界BOSS"}, permission=SUPERUSER, priority=5, block=True)
create_appoint = on_command("生成指定世界boss", aliases={"生成指定世界boss", "生成指定世界BOSS", "生成指定BOSS", "生成指定boss"}, permission=SUPERUSER, priority=5,)
boss_info = on_command("查询世界boss", aliases={"查询世界Boss", "查询世界BOSS", "查询boss", "世界Boss查询", "世界BOSS查询", "boss查询"}, priority=6, block=True)
boss_info2 = on_command("查询世界boss列表", aliases={"查询世界Boss列表", "查询世界BOSS列表", "查询boss列表", "世界Boss列表查询", "世界BOSS列表查询", "boss列表查询"}, priority=6, block=True)
set_group_boss = on_command("世界boss", aliases={"世界Boss", "世界BOSS"}, priority=13, permission=SUPERUSER, block=True)
battle = on_command("讨伐boss", aliases={"讨伐世界boss", "讨伐Boss", "讨伐BOSS", "讨伐世界Boss", "讨伐世界BOSS"}, priority=6, block=True)
boss_help = on_command("世界boss帮助", aliases={"世界Boss帮助", "世界BOSS帮助"}, priority=5, block=True)
boss_delete = on_command("天罚boss", aliases={"天罚世界boss", "天罚Boss", "天罚BOSS", "天罚世界Boss", "天罚世界BOSS"}, permission=SUPERUSER, priority=7, block=True)
boss_delete_all = on_command("天罚所有boss", aliases={"天罚所有世界boss", "天罚所有Boss", "天罚所有BOSS", "天罚所有世界Boss","天罚所有世界BOSS", "天罚全部boss", "天罚全部世界boss"}, permission=SUPERUSER, priority=5, block=True)
boss_integral_info = on_command("世界积分查看",aliases={"查看世界积分", "查询世界积分", "世界积分查询"} ,priority=10, block=True)
boss_integral_store = on_command("世界积分商店",aliases={"查看世界商店", "查询世界商店", "世界商店查询"} ,priority=10, block=True)
boss_integral_use = on_command("世界积分兑换", priority=6, block=True)
challenge_scarecrow = on_command("挑战稻草人", priority=6, block=True)
challenge_training_puppet = on_command("挑战训练傀儡", priority=6, block=True)

__boss_help__ = f"""
世界BOSS系统帮助          

【指令大全】
🔹 生成指令：
  ▶ 生成世界boss [数量] - 生成随机境界BOSS（超管权限）
  ▶ 生成指定世界boss [境界] [名称] - 生成指定BOSS（超管权限）
  ▶ 生成全部世界boss - 一键生成所有境界BOSS（超管权限）

🔹 查询指令：
  ▶ 查询世界boss - 查看全服BOSS列表
  ▶ 查询世界boss列表 [页码] - 分页查看BOSS详情
  ▶ 世界积分查看 - 查看个人积分
  ▶ 世界积分商店 - 查看可兑换物品

🔹 战斗指令：
  ▶ 讨伐boss [编号] - 挑战指定BOSS
  ▶ 挑战稻草人 - 练习战斗技巧（无消耗）
  ▶ 挑战训练傀儡 [境界] [名称] - 自定义训练对手

🔹 管理指令：
  ▶ 天罚boss [编号] - 删除指定BOSS（超管权限）
  ▶ 天罚所有boss - 清空所有BOSS（超管权限）
  ▶ 世界boss 开启/关闭 - 管理群通知（管理员权限）

【特色功能】
🌟 境界压制系统：高境界打低境界BOSS收益降低
🌟 积分兑换商店：用战斗积分兑换珍稀道具
🌟 随机掉落系统：击败BOSS有机会获得特殊物品
🌟 自动刷新机制：每小时自动清理部分BOSS

【注意事项】
⚠ 全服每{config['Boss生成时间参数']['hours']}小时自动生成BOSS
⚠ 重伤状态下无法挑战BOSS
⚠ 世界积分可永久保存，请合理使用

输入具体指令查看详细用法，祝道友斩妖除魔，早日得道！
""".strip()

@DRIVER.on_startup
async def read_boss_():
    global group_boss
    group_boss.update(old_boss_info.read_boss_info())
    logger.opt(colors=True).info(f"<green>历史boss数据读取成功</green>")


@DRIVER.on_startup
async def set_boss_punishment():
    try:
        # 每小时执行天罚
        scheduler.add_job(
            func=punish_all_bosses,
            trigger='interval',
            hours=1,
            id="punish_all_bosses",
            misfire_grace_time=60
        )
        logger.opt(colors=True).success(f"<green>已开启每小时执行天罚世界BOSS定时任务！</green>")
    except Exception as e:
        logger.opt(colors=True).warning(f"<red>警告,天罚定时任务加载失败!,{e}!</red>")

async def punish_all_bosses():
    global group_boss
    group_id = "000000"  # 全局BOSS存储键

    # 获取当前BOSS列表
    bosss = group_boss.get(group_id, [])
    if not bosss:
        logger.opt(colors=True).info(f"<yellow>当前没有世界BOSS，无需天罚</yellow>")
        return
        
    now = datetime.now()
    current_hour = now.hour   
    severe_punishment_hours = {8, 12, 20, 0}
    
    if current_hour in severe_punishment_hours:
        delete_count = max(1, len(bosss) // 2)
        logger.opt(colors=True).warning(f"<yellow>现在是 {current_hour}:00，执行严重天罚！</yellow>")
    else:
        delete_count = min(random.randint(5, 20), len(bosses))
        
    delete_count = min(delete_count, len(bosss))

    # 记录被天罚BOSS的境界
    punished_bosses = random.sample(bosss, delete_count)
    punished_jj_list = [boss['jj'] for boss in punished_bosses]
    punished_names = [boss['name'] for boss in punished_bosses]

    # 从列表中移除被天罚的BOSS
    for boss in punished_bosses:
        group_boss[group_id].remove(boss)

    # 保存更新后的BOSS数据
    old_boss_info.save_boss(group_boss)
    logger.opt(colors=True).info(f"<green>天罚已随机清除了 {delete_count} 个世界BOSS: {', '.join(punished_names)}</green>")

    # 生成与被天罚BOSS相同境界的新BOSS
    current_boss_count = len(group_boss[group_id])
    
    generated_bosses = []
    for jj in punished_jj_list:
        if current_boss_count <= 0:
            break
        bossinfo = createboss_jj(jj, None)  # 生成指定境界的随机BOSS
        if bossinfo:
            group_boss[group_id].append(bossinfo)
            generated_bosses.append(bossinfo['name'])
            current_boss_count -= 1

    if generated_bosses:
        old_boss_info.save_boss(group_boss)
        logger.opt(colors=True).info(f"<green>已生成{len(generated_bosses)}个新BOSS: {', '.join(generated_bosses)}</green>")

    # 发送通知
    msg = f"天雷降临，随机天罚了 {delete_count} 个世界BOSS：{', '.join(punished_names)}！"
    if generated_bosses:
        msg += f"\n天道循环，又孕育出了新的BOSS：{', '.join(generated_bosses)}"
    
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


@boss_help.handle(parameterless=[Cooldown(at_sender=False)])
async def boss_help_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, session_id: int = CommandObjectID()):
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    group_id = "000000"
    send_group_id = "000000"
    if str(send_group_id) in groups:
        msg = __boss_help__ + f"\n非指令:1、拥有定时任务:每{groups[str(send_group_id)]['hours']}小时{groups[str(send_group_id)]['minutes']}分钟生成一只随机大境界的世界Boss"
    else:
        msg = __boss_help__ 
    await handle_send(bot, event, msg)
    await boss_help.finish()


@boss_delete.handle(parameterless=[Cooldown(at_sender=False)])
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


@boss_delete_all.handle(parameterless=[Cooldown(at_sender=False)])
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


@battle.handle(parameterless=[Cooldown(stamina_cost=config['讨伐世界Boss体力消耗'], at_sender=False)])
async def battle_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """讨伐世界boss"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    global group_boss 
    group_boss = old_boss_info.read_boss_info()
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await battle.finish()

    user_id = user_info['user_id']
    is_type, msg = check_user_type(user_id, 0)  # 需要无状态的用户
    if not is_type:
        await handle_send(bot, event, msg)
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
        await handle_send(bot, event, msg)
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
        await handle_send(bot, event, msg)
        await battle.finish()

    if user_info['hp'] is None or user_info['hp'] == 0:
        # 判断用户气血是否为空
        sql_message.update_user_hp(user_id)

    if user_info['hp'] <= user_info['exp'] / 10:
        time = leave_harm_time(user_id)
        msg = f"重伤未愈，动弹不得！距离脱离危险还需要{time}分钟！\n"
        msg += f"请道友进行闭关，或者使用药品恢复气血，不要干等，没有自动回血！！！"
        sql_message.update_user_stamina(user_id, 20, 1)
        await handle_send(bot, event, msg)
        await battle.finish()

    player = {"user_id": None, "道号": None, "气血": None, "攻击": None, "真元": None, '会心': None, '防御': 0}
    userinfo = sql_message.get_user_real_info(user_id)
    user_weapon_data = UserBuffDate(userinfo['user_id']).get_user_weapon_data()

    impart_data = xiuxian_impart.get_user_impart_info_with_id(user_id)
    boss_atk = impart_data['boss_atk'] if impart_data['boss_atk'] is not None else 0
    user_armor_data = UserBuffDate(userinfo['user_id']).get_user_armor_buff_data() #boss战防具会心
    user_main_data = UserBuffDate(userinfo['user_id']).get_user_main_buff_data() #boss战功法会心
    user1_sub_buff_data = UserBuffDate(userinfo['user_id']).get_user_sub_buff_data() #boss战辅修功法信息
    integral_buff = user1_sub_buff_data['integral'] if user1_sub_buff_data is not None else 0 #boss战积分加成
    exp_buff = user1_sub_buff_data['exp'] if user1_sub_buff_data is not None else 0
    
    if  user_main_data != None: #boss战功法会心
        main_crit_buff = user_main_data['crit_buff']
    else:
        main_crit_buff = 0
  
    if  user_armor_data != None: #boss战防具会心
        armor_crit_buff = user_armor_data['crit_buff']
    else:
        armor_crit_buff = 0
    
    if user_weapon_data != None: #boss战武器会心
        player['会心'] = int(((user_weapon_data['crit_buff']) + (armor_crit_buff) + (main_crit_buff)) * 100)
    else:
        player['会心'] = (armor_crit_buff + main_crit_buff) * 100

    player['user_id'] = userinfo['user_id']
    player['道号'] = userinfo['user_name']
    player['气血'] = userinfo['hp']
    player['攻击'] = int(userinfo['atk'] * (1 + boss_atk))
    player['真元'] = userinfo['mp']
    player['exp'] = userinfo['exp']

    bossinfo = group_boss[group_id][boss_num - 1]
    if bossinfo['jj'] == '零':
        boss_rank = convert_rank((bossinfo['jj']))[0]
    else:
        boss_rank = convert_rank((bossinfo['jj'] + '中期'))[0]
    user_rank = convert_rank(userinfo['level'])[0]
    rank_name_list = convert_rank(user_info["level"])[1]
    if boss_rank - user_rank >= 4:
        msg = f"道友已是{userinfo['level']}之人，妄图抢小辈的Boss，可耻！"
        await handle_send(bot, event, msg)
        await battle.finish()
    if user_rank - boss_rank >= 4:
        required_rank_name = rank_name_list[len(rank_name_list) - (boss_rank + 4)]
        msg = f"道友，您的实力尚需提升至{required_rank_name}，目前仅为{userinfo['level']}，不宜过早挑战Boss，还请三思。"
        await handle_send(bot, event, msg)
        await battle.finish()
    more_msg = ''
    battle_flag[group_id] = True
    boss_all_hp = bossinfo['总血量']
    # 打之前的血量
    boss_old_hp = bossinfo['气血']
    boss_old_stone = bossinfo['stone']
    boss_now_stone = int(round(bossinfo['max_stone'] // 3))
    result, victor, bossinfo_new, get_stone = await Boss_fight(player, bossinfo, bot_id=bot.self_id)
    # 打之后的血量
    boss_now_hp = bossinfo_new['气血']
    # 计算总伤害
    total_damage = boss_old_hp - boss_now_hp
    if victor == "Boss赢了":
        group_boss[group_id][boss_num - 1] = bossinfo_new
        if boss_old_stone == 0:
            get_stone = 1
        if get_stone > boss_old_stone:
            get_stone = boss_old_stone
        if get_stone == 0:
            stone_buff = user1_sub_buff_data['stone']
            get_stone = int(boss_old_stone * ((boss_old_hp - boss_now_hp) / boss_all_hp) * (1 + stone_buff))
        if get_stone > boss_now_stone:
            get_stone = boss_now_stone
        bossinfo['stone'] = boss_old_stone - get_stone
        sql_message.update_ls(user_id, get_stone, 1)
        boss_integral = int(((boss_old_hp - boss_now_hp) / boss_all_hp) * 1500)
        boss_integral = min(boss_integral, 1500)
        if boss_integral < 5:  # 摸一下不给
            boss_integral = 0
        if user_info['root'] == "凡人":
            boss_integral = int(boss_integral * (1 + (user_rank - boss_rank)))
            points_bonus = int(80 * (user_rank - boss_rank))
            more_msg = f"道友低boss境界{user_rank - boss_rank}层，获得{points_bonus}%积分加成！"

        user_boss_fight_info = get_user_boss_fight_info(user_id)
        user_boss_fight_info['boss_integral'] += boss_integral
        top_user_info = sql_message.get_top1_user()
        top_user_exp = top_user_info['exp']
        save_user_boss_fight_info(user_id, user_boss_fight_info)
        
        if exp_buff > 0 and user_info['root'] != "凡人":
            now_exp = int(((top_user_exp * 0.1) / user_info['exp']) / (exp_buff * (1 / (convert_rank(user_info['level'])[0] + 1))))
            if now_exp > 1000000:
                now_exp = int(1000000 / random.randint(5, 10))
            sql_message.update_exp(user_id, now_exp)
            exp_msg = f"，获得修为{int(now_exp)}点！"
        else:
            exp_msg = f" "
            
        msg = f"道友不敌{bossinfo['name']}，共造成 {number_to(total_damage)} 伤害，重伤逃遁，临逃前收获灵石{get_stone}枚，{more_msg}获得世界积分：{boss_integral}点{exp_msg} "
        if user_info['root'] == "凡人" and boss_integral < 0:
            msg += f"\n如果出现负积分，说明你境界太高了，玩凡人就不要那么高境界了！！！"
        battle_flag[group_id] = False
        try:
            await send_msg_handler(bot, event, result)
        except ActionFailed:
            msg += f"Boss战消息发送错误,可能被风控!"
        await handle_send(bot, event, msg)
        await battle.finish()
    
    elif victor == "群友赢了":
        # 新增boss战斗积分点数
        boss_all_hp = bossinfo['总血量']  # 总血量
        boss_integral = 1000
        killed_jj = bossinfo['jj']
        if user_info['root'] == "凡人":
            boss_integral = int(boss_integral * (1 + (user_rank - boss_rank)))
            points_bonus = int(80 * (user_rank - boss_rank))
            more_msg = f"道友低boss境界{user_rank - boss_rank}层，获得{points_bonus}%积分加成！"
        else:
            if boss_rank - user_rank > 2:
                boss_integral = int(boss_integral // 2)
                get_stone = int(get_stone // 2)
                more_msg = f"道友的境界超过boss太多了,不齿！"
                
            if boss_rank - user_rank > 3:
                boss_integral = int(boss_integral // 5)
                get_stone = int(get_stone // 5)
                
        top_user_info = sql_message.get_top1_user()
        top_user_exp = top_user_info['exp']
        
        if exp_buff > 0 and user_info['root'] != "凡人":
            now_exp = int(((top_user_exp * 0.1) / user_info['exp']) / (exp_buff * (1 / (convert_rank(user_info['level'])[0] + 1))))
            if now_exp > 1000000:
                now_exp = int(1000000 / random.randint(5, 10))
            sql_message.update_exp(user_id, now_exp)
            exp_msg = f"，获得修为{int(now_exp)}点！"
        else:
            exp_msg = f" "
                
        drops_id, drops_info = boss_drops(user_rank, boss_rank, bossinfo, userinfo)
        if drops_id == None:
            drops_msg = " "
        elif boss_rank < convert_rank('遁一境中期')[0]:           
            drops_msg = f"boss的尸体上好像有什么东西， 凑近一看居然是{drops_info['name']}！ "
            sql_message.send_back(user_info['user_id'], drops_info['id'],drops_info['name'], drops_info['type'], 1)
        else :
            drops_msg = " "
            
        boss_jj = createboss()
        for boss in group_boss[group_id][:]:
            if boss['jj'] == boss_jj:
                group_boss[group_id].remove(boss)
                break
    
        bossinfo = createboss_jj(boss_jj)    
        group_boss[group_id].append(bossinfo)
        old_boss_info.save_boss(group_boss)
            
        if boss_old_stone == 0:
            get_stone = 1
        sql_message.update_ls(user_id, get_stone, 1)
        user_boss_fight_info = get_user_boss_fight_info(user_id)
        user_boss_fight_info['boss_integral'] += boss_integral
        save_user_boss_fight_info(user_id, user_boss_fight_info)
        msg = f"恭喜道友击败{bossinfo['name']}，共造成 {number_to(total_damage)} 伤害，收获灵石{get_stone}枚，{more_msg}获得世界积分：{boss_integral}点!{exp_msg} {drops_msg}"
        if user_info['root'] == "凡人" and boss_integral < 0:
           msg += f"\n如果出现负积分，说明你这凡人境界太高了(如果总世界积分为负数，会帮你重置成0)，玩凡人就不要那么高境界了！！！"
        try:
            await send_msg_handler(bot, event, result)
        except ActionFailed:
            msg += f"Boss战消息发送错,可能被风控!"
        old_boss_info.save_boss(group_boss)
        await handle_send(bot, event, msg)
        await battle.finish()


@challenge_scarecrow.handle(parameterless=[Cooldown(stamina_cost=1, cd_time=30, at_sender=False)])
async def challenge_scarecrow_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """挑战稻草人"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    group_id = "000000"
    isUser, user_info, msg = check_user(event)
    sql_message = XiuxianDateManage()

    if not isUser:
        await handle_send(bot, event, msg)
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
        await handle_send(bot, event, msg)
        await challenge_scarecrow.finish()

    # 获取玩家信息
    player = {"user_id": None, "道号": None, "气血": None, "攻击": None, "真元": None, '会心': None, '防御': 0}
    userinfo = sql_message.get_user_real_info(user_id)
    user_weapon_data = UserBuffDate(userinfo['user_id']).get_user_weapon_data()
    impart_data = xiuxian_impart.get_user_impart_info_with_id(user_id)
    boss_atk = impart_data['boss_atk'] if impart_data and impart_data['boss_atk'] is not None else 0
    user_armor_data = UserBuffDate(userinfo['user_id']).get_user_armor_buff_data()
    user_main_data = UserBuffDate(userinfo['user_id']).get_user_main_buff_data()

    player['user_id'] = userinfo['user_id']
    player['道号'] = userinfo['user_name']
    player['气血'] = userinfo['hp']
    player['攻击'] = int(userinfo['atk'] * (1 + boss_atk))
    player['真元'] = userinfo['mp']
    player['exp'] = userinfo['exp']
    player['会心'] = (user_weapon_data['crit_buff'] + user_armor_data['crit_buff'] + user_main_data['crit_buff']) * 100 if user_weapon_data and user_armor_data and user_main_data else 0
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
    result, victor, bossinfo_new, get_stone = await Boss_fight(player, scarecrow_info, type_in=1, bot_id=bot.self_id)      
    # 打之后的血量
    boss_now_hp = bossinfo_new['气血']
    # 计算总伤害
    total_damage = boss_old_hp - boss_now_hp
    # 输出结果并处理奖励
    if victor == "群友赢了":
        msg = f"奇迹！道友击败了稻草人，共造成 {number_to(total_damage)} 伤害！不过它又站起来了，继续等待挑战者！"
    elif victor == "Boss赢了":
        msg = f"道友挑战稻草人，奋力攻击后共造成 {number_to(total_damage)} 伤害，稻草人岿然不动，继续等待挑战者！"

    battle_flag[group_id] = False

    try:
        await send_msg_handler(bot, event, result)
    except ActionFailed:
            msg += f"\nBoss战消息发送错误,可能被风控!"
    await handle_send(bot, event, msg)
    await challenge_scarecrow.finish()


@challenge_training_puppet.handle(parameterless=[Cooldown(stamina_cost=1, cd_time=30, at_sender=False)])
async def challenge_training_puppet_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """挑战训练傀儡"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    group_id = "000000"
    isUser, user_info, msg = check_user(event)
    sql_message = XiuxianDateManage()

    if not isUser:
        await handle_send(bot, event, msg)
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
        await handle_send(bot, event, msg)
        await challenge_training_puppet.finish()

    # 获取玩家信息
    player = {"user_id": None, "道号": None, "气血": None, "攻击": None, "真元": None, '会心': None, '防御': 0}
    userinfo = sql_message.get_user_real_info(user_id)
    user_weapon_data = UserBuffDate(userinfo['user_id']).get_user_weapon_data()
    impart_data = xiuxian_impart.get_user_impart_info_with_id(user_id)
    boss_atk = impart_data['boss_atk'] if impart_data and impart_data['boss_atk'] is not None else 0
    user_armor_data = UserBuffDate(userinfo['user_id']).get_user_armor_buff_data()
    user_main_data = UserBuffDate(userinfo['user_id']).get_user_main_buff_data()

    player['user_id'] = userinfo['user_id']
    player['道号'] = userinfo['user_name']
    player['气血'] = userinfo['hp']
    player['攻击'] = int(userinfo['atk'] * (1 + boss_atk))
    player['真元'] = userinfo['mp']
    player['exp'] = userinfo['exp']
    player['会心'] = (user_weapon_data['crit_buff'] + user_armor_data['crit_buff'] + user_main_data['crit_buff']) * 100 if user_weapon_data and user_armor_data and user_main_data else 0
    
    arg_list = args.extract_plain_text().split()
    boss_name = "散发着威压的尸体"
    if len(arg_list) == 0:
        # 根据玩家的大境界确定训练傀儡的境界
        player_jj = (userinfo['level'])
        scarecrow_jj = player_jj[:3]
        if player_jj == "江湖好手":
            scarecrow_jj = "感气境"
    if len(arg_list) >= 1:
        scarecrow_jj = arg_list[0]  # 用户指定的境界
        if len(arg_list) == 2:
            boss_name = arg_list[1]

    
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
    result, victor, bossinfo_new, get_stone = await Boss_fight(player, scarecrow_info, type_in=1, bot_id=bot.self_id)      
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
    try:
        await send_msg_handler(bot, event, result)
    except ActionFailed:
        msg += f"\nBoss战消息发送错误,可能被风控!"
    await handle_send(bot, event, msg)
    await challenge_training_puppet.finish()
    
    
@boss_info.handle(parameterless=[Cooldown(at_sender=False)])
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
            await handle_send(bot, event, msg)
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
        if isinstance(event, GroupMessageEvent):
           await bot.send_group_msg(group_id=event.group_id, message=MessageSegment.image(pic))
        else:
            await bot.send_private_msg(user_id=event.user_id, message=MessageSegment.image(pic))
        await boss_info.finish()
    else:
        i = 1
        for boss in bosss:
            bossmsgs += f"编号{i}、{boss['jj']}Boss:{boss['name']} \n"
            i += 1
        msg = bossmsgs
        await handle_send(bot, event, msg)
        await boss_info.finish()
        
        
@boss_info2.handle(parameterless=[Cooldown(at_sender=False)])
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

    per_page = 50
    total_items = len(bosss)  # 总BOSS数量
    total_pages = (total_items + per_page - 1) // per_page
    
    current_page = re.findall(r"\d+", arg_list)
    if current_page:
        current_page = int(current_page[0])
    else:
        current_page = 1
    if current_page < 1 or current_page > total_pages:
        msg = f"页码错误，有效范围为1~{total_pages}页！"
        await handle_send(bot, event, msg)
        await boss_info2.finish()
    start_index = (current_page - 1) * per_page
    end_index = start_index + per_page
    paged_bosses = bosss[start_index:end_index]
    msgs = f"世界BOSS列表"
    header = f"{msgs}（第{current_page}/{total_pages}页）"
    footer = f"提示：发送 查询世界BOSS列表+页码 查看其他页（共{total_pages}页）"
    paged_msgs = [header]
    for i, boss in enumerate(paged_bosses, start=start_index + 1):
        paged_msgs.append(f"编号{i} \nBoss:{boss['name']} \n境界：{boss['jj']} \n总血量：{number_to(boss['总血量'])} \n剩余血量：{number_to(boss['气血'])} \n攻击：{number_to(boss['攻击'])} \n携带灵石：{number_to(boss['stone'])}")
    paged_msgs.append(footer)
    await send_msg_handler(bot, event, f'世界BOSS列表 - 第{current_page}页', bot.self_id, paged_msgs)
    await boss_info2.finish()

@generate_all.handle()
async def generate_all_bosses(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    bosses = create_all_bosses()  # 自动计算最高境界
    group_boss["000000"] = bosses  # 替换当前 BOSS 列表
    old_boss_info.save_boss(group_boss)
    await bot.send(event, f"已生成全部 {len(bosses)} 个境界的 BOSS！")


@create.handle(parameterless=[Cooldown(at_sender=False)])
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

@create_appoint.handle()
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
    
@set_group_boss.handle(parameterless=[Cooldown(at_sender=False)])
async def set_group_boss_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """设置群世界boss通知开关"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    mode = args.extract_plain_text().strip()
    group_id = str(send_group_id)  # 使用实际群号
    isInGroup = group_id in config['open']  # 检查群号是否在通知列表中

    if mode == '开启':
        if isInGroup:
            msg = f"本群已开启世界Boss通知，请勿重复开启!"
        else:
            # 添加群号到通知列表
            config['open'][group_id] = {}
            savef_boss(config)
            msg = f"已为本群开启世界Boss通知!"
        await handle_send(bot, event, msg)
        await set_group_boss.finish()

    elif mode == '关闭':
        if isInGroup:
            del config['open'][group_id]
            savef_boss(config)
            msg = f"已为本群关闭世界Boss通知!"
        else:
            msg = f"本群未开启世界Boss通知!"
        await handle_send(bot, event, msg)
        await set_group_boss.finish()
        
    elif mode == '帮助':
        msg = __boss_help__
        await handle_send(bot, event, msg)
        await set_group_boss.finish()


@boss_integral_store.handle(parameterless=[Cooldown(at_sender=False)])
async def boss_integral_store_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """世界积分商店"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await boss_integral_store.finish()

    user_id = user_info['user_id']    
    user_boss_fight_info = get_user_boss_fight_info(user_id)
    boss_integral_shop = config['世界积分商品']
    l_msg = [f"道友目前拥有的世界积分：{user_boss_fight_info['boss_integral']}点"]
    if boss_integral_shop != {}:
        for k, v in boss_integral_shop.items():
            msg = f"编号:{k}\n"
            msg += f"描述：{v['desc']}\n"
            msg += f"所需世界积分：{v['cost']}点"
            l_msg.append(msg)
    else:
        l_msg.append(f"世界积分商店内空空如也！")
    await send_msg_handler(bot, event, '世界积分商店', bot.self_id, l_msg)
    await boss_integral_store.finish()


@boss_integral_info.handle(parameterless=[Cooldown(at_sender=False)])
async def boss_integral_info_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """世界积分"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await boss_integral_info.finish()
    user_id = user_info['user_id']    
    user_boss_fight_info = get_user_boss_fight_info(user_id)
    msg = f"道友目前拥有的世界积分：{user_boss_fight_info['boss_integral']}点"
    await handle_send(bot, event, msg)
    await boss_integral_info.finish()

@boss_integral_use.handle(parameterless=[Cooldown(at_sender=False)])
async def boss_integral_use_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """世界积分商店兑换"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await boss_integral_use.finish()

    user_id = user_info['user_id']
    msg = args.extract_plain_text().strip()
    shop_info = re.findall(r"(\d+)\s*(\d*)", msg)
    

    if shop_info:
        shop_id = int(shop_info[0][0])
        quantity = int(shop_info[0][1]) if shop_info[0][1] else 1
    else:
        msg = f"请输入正确的商品编号！"
        await handle_send(bot, event, msg)
        await boss_integral_use.finish()

    boss_integral_shop = config['世界积分商品']
    is_in = False
    cost = None
    item_id = None
    if boss_integral_shop:
        for k, v in boss_integral_shop.items():
            if shop_id == int(k):
                is_in = True
                cost = v['cost']
                item_id = v['id']
                break
    else:
        msg = f"世界积分商店内空空如也！"
        await handle_send(bot, event, msg)
        await boss_integral_use.finish()
    if is_in:
        user_boss_fight_info = get_user_boss_fight_info(user_id)
        total_cost = cost * quantity
        if user_boss_fight_info['boss_integral'] < total_cost:
            msg = f"道友的世界积分不满足兑换条件呢"
            await handle_send(bot, event, msg)
            await boss_integral_use.finish()
        else:
            user_boss_fight_info['boss_integral'] -= total_cost
            save_user_boss_fight_info(user_id, user_boss_fight_info)
            item_info = Items().get_data_by_item_id(item_id)
            sql_message.send_back(user_id, item_id, item_info['name'], item_info['type'], quantity)  # 兑换指定数量
            msg = f"道友成功兑换获得：{item_info['name']}{quantity}个"
            await handle_send(bot, event, msg)
            await boss_integral_use.finish()
    else:
        msg = f"该编号不在商品列表内哦，请检查后再兑换"
        await handle_send(bot, event, msg)
        await boss_integral_use.finish()


PLAYERSDATA = Path() / "data" / "xiuxian" / "players"


def get_user_boss_fight_info(user_id):
    try:
        user_boss_fight_info = read_user_boss_fight_info(user_id)
    except Exception as e:
        # 如果读取失败，初始化默认值并保存
        user_boss_fight_info = {"boss_integral": 0}
        save_user_boss_fight_info(user_id, user_boss_fight_info)
        logger.opt(colors=True).warning(f"<yellow>用户 {user_id} 的BOSS战斗信息读取失败，已初始化默认值: {e}</yellow>")
    return user_boss_fight_info


def read_user_boss_fight_info(user_id):
    user_id = str(user_id)

    FILEPATH = PLAYERSDATA / user_id / "boss_fight_info.json"
    if not os.path.exists(FILEPATH):
        data = {"boss_integral": 0}
        with open(FILEPATH, "w", encoding="UTF-8") as f:
            json.dump(data, f, indent=4)
    else:
        with open(FILEPATH, "r", encoding="UTF-8") as f:
            data = json.load(f)

    # 检查 boss_integral 键值是否为负数
    if "boss_integral" in data and data["boss_integral"] < 0:
        data["boss_integral"] = 0
        with open(FILEPATH, "w", encoding="UTF-8") as f:
            json.dump(data, f, indent=4)

    return data


def save_user_boss_fight_info(user_id, data):
    user_id = str(user_id)

    if not os.path.exists(PLAYERSDATA / user_id):
        logger.opt(colors=True).info("<green>目录不存在，创建目录</green>")
        os.makedirs(PLAYERSDATA / user_id)

    FILEPATH = PLAYERSDATA / user_id / "boss_fight_info.json"
    data = json.dumps(data, ensure_ascii=False, indent=4)
    save_mode = "w" if os.path.exists(FILEPATH) else "x"
    with open(FILEPATH, mode=save_mode, encoding="UTF-8") as f:
        f.write(data)
        f.close()

def get_dict_type_rate(data_dict):
    """根据字典内概率,返回字典key"""
    temp_dict = {}
    for i, v in data_dict.items():
        try:
            temp_dict[i] = v["type_rate"]
        except:
            continue
    key = OtherSet().calculated(temp_dict)
    return key

def get_goods_type():
    data_dict = BOSSDLW['宝物']
    return get_dict_type_rate(data_dict)

def get_story_type():
    """根据概率返回事件类型"""
    data_dict = BOSSDLW
    return get_dict_type_rate(data_dict)

BOSSDLW ={"衣以候": "衣以侯布下了禁制镜花水月，",
    "金凰儿": "金凰儿使用了神通：金凰天火罩！",
    "九寒": "九寒使用了神通：寒冰八脉！",
    "莫女": "莫女使用了神通：圣灯启语诀！",
    "术方": "术方使用了神通：天罡咒！",
    "卫起": "卫起使用了神通：雷公铸骨！",
    "血枫": "血枫使用了神通：混世魔身！",
    "以向": "以向使用了神通：云床九练！",
    "砂鲛": "不说了！开鳖！",
    "神风王": "不说了！开鳖！",
    "鲲鹏": "鲲鹏使用了神通：逍遥游！",
    "天龙": "天龙使用了神通：真龙九变！",
    "历飞雨": "厉飞雨使用了神通：天煞震狱功！",
    "外道贩卖鬼": "不说了！开鳖！",
    "元磁道人": "元磁道人使用了法宝：元磁神山！",
    "散发着威压的尸体": "尸体周围爆发了出强烈的罡气！"
    }

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
        user_rank = convert_rank(user_level)[0]
        min_rank = max(convert_rank(user_level)[0] - 17, 8)
        max_rank = min(random.randint(min_rank, min_rank + 30), 55)
        
        # 筛选符合条件的掉落物
        eligible_drops = []
        for drop_id, drop_info in self.drops_data.items():
            if min_rank <= drop_info.get('rank', 0) <= max_rank:
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
    
    # 基础掉落概率检查(30%)
    if random.random() > 0.3:
        return None, None
        
    # 境界差距过大时极低概率掉落(5%)
    if user_rank - boss_rank >= 4 and random.random() > 0.05:
        return None, None
        
    # 获取随机掉落物
    drop_id, drop_info = drops_system.get_random_drop(user_info['level'])
    
    return drop_id, drop_info
