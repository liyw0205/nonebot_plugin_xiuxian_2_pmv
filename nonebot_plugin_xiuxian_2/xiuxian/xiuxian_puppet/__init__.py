import random
from datetime import datetime
from nonebot import require
from nonebot import on_command
from nonebot.adapters.onebot.v11 import Bot, Message, GroupMessageEvent, PrivateMessageEvent

from ..xiuxian_config import convert_rank
from ..xiuxian_utils.item_json import Items
from ..xiuxian_utils.lay_out import assign_bot
from ..xiuxian_utils.utils import check_user, log_message, handle_send
from ..xiuxian_utils.xiuxian2_handle import (get_player_info, save_player_info,
                                             UserBuffDate, XiuxianDateManage, XIUXIAN_IMPART_BUFF)

sql_message = XiuxianDateManage()  # sql类
xiuxian_impart = XIUXIAN_IMPART_BUFF()
items = Items()

# 引入定时任务
scheduler = require("nonebot_plugin_apscheduler").scheduler

# 命令处理器
buy_puppet = on_command("购买灵田傀儡", aliases={"购买傀儡", "灵田傀儡购买"}, priority=5, block=True)
upgrade_puppet = on_command("灵田傀儡升级", aliases={"傀儡升级", "升级傀儡"}, priority=5, block=True)
start_puppet = on_command("灵田傀儡开启", aliases={"开启傀儡", "启动傀儡"}, priority=5, block=True)
stop_puppet = on_command("灵田傀儡关闭", aliases={"关闭傀儡", "停止傀儡"}, priority=5, block=True)
puppet_info = on_command("我的灵田", aliases={"灵田信息", "灵田状态"}, priority=5, block=True)
puppet_help = on_command("傀儡帮助", aliases={"灵田傀儡帮助", "灵田傀儡指令"}, priority=5, block=True)

__puppet_help__ = f"""
灵田傀儡帮助信息:
指令：
购买灵田傀儡: 购买灵田傀儡，需要消耗1000万灵石
灵田傀儡升级: 可以减少每次灵田收取的灵石 (最高等级3)
灵田傀儡开启: 开启后，傀儡会自动收取成熟的灵药
灵田傀儡关闭: 关闭傀儡自动收取
我的灵田: 查看灵田信息和傀儡信息
傀儡帮助: 查看灵田傀儡指令
"""

# 傀儡配置
PUPPET_CONFIG = {
    1: {
        "upgrade_cost": 50000000,
        "harvest_cost": 5000000,
        "next_level": 2
    },
    2: {
        "upgrade_cost": 100000000,
        "harvest_cost": 3500000,
        "next_level": 3
    },
    3: {
        "upgrade_cost": 0,
        "harvest_cost": 2000000,
        "next_level": None
    }
}


@puppet_help.handle()
async def puppet_help_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """傀儡帮助"""
    bot, _ = await assign_bot(bot=bot, event=event)
    msg = __puppet_help__
    await handle_send(bot, event, msg)
    await puppet_help.finish()


# 每小时整点执行（比如 15:00、16:00、17:00）
@scheduler.scheduled_job("cron", minute=1, id="auto_harvest")
async def auto_harvest_scheduled():
    """每小时自动收取任务"""
    try:
        enabled_users = sql_message.get_all_enabled_puppets()

        for user_id in enabled_users:
            await check_and_harvest(user_id)

    except Exception as e:
        print(f"自动收取任务出错: {e}")


async def check_and_harvest(user_id):
    """检查并执行收取"""

    user_info = sql_message.get_user_info_with_id(user_id)

    if int(user_info['blessed_spot_flag']) == 0:
        msg = f"道友还没有洞天福地呢，请发送洞天福地购买吧~"
        return msg

    mix_elixir_info = get_player_info(user_id, "mix_elixir_info")
    GETCONFIG = {
        "time_cost": 23,  # 单位小时
        "加速基数": 0.05
    }
    last_time = mix_elixir_info['收取时间']
    puppet_level = mix_elixir_info['灵田傀儡']
    harvest_cost = PUPPET_CONFIG[puppet_level]['harvest_cost']

    if last_time != 0:
        nowtime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # str
        timedeff = round((datetime.strptime(nowtime, '%Y-%m-%d %H:%M:%S') - datetime.strptime(last_time,
                                                                                              '%Y-%m-%d %H:%M:%S')).total_seconds() / 3600,
                         2)
        if timedeff >= round(GETCONFIG['time_cost'] * (1 - (GETCONFIG['加速基数'] * mix_elixir_info['药材速度'])), 2):

            if user_info['stone'] < harvest_cost:
                msg = f"道友灵石数量不足，无法驱动傀儡，灵田傀儡已关闭"
                # 灵石不足，关闭自动收取
                sql_message.set_puppet_status(user_id, 0)
                log_message(user_id, msg)
                return msg

            yaocai_id_list = items.get_random_id_list_by_rank_and_item_type(
                max(convert_rank(user_info['level'])[0] - 22, 16), ['药材'])
            # 加入传承
            impart_data = xiuxian_impart.get_user_impart_info_with_id(user_id)
            impart_reap_per = impart_data['impart_reap_per'] if impart_data is not None else 0
            # 功法灵田收取加成
            main_reap = UserBuffDate(user_id).get_user_main_buff_data()

            if main_reap != None:  # 功法灵田收取加成
                reap_buff = main_reap['reap_buff']
            else:
                reap_buff = 0
            num = mix_elixir_info['灵田数量'] + mix_elixir_info['收取等级'] + impart_reap_per + reap_buff
            msg = '傀儡收取\n'
            if not yaocai_id_list:
                sql_message.send_back(user_info['user_id'], 3001, '恒心草', '药材', num)  # 没有合适的，保底
                msg += f"道友成功收获药材：恒心草 {num} 个！\n"
            else:
                i = 1
                give_dict = {}
                while i <= num:
                    id = random.choice(yaocai_id_list)
                    try:
                        give_dict[id] += 1
                        i += 1
                    except LookupError:
                        give_dict[id] = 1
                        i += 1
                for k, v in give_dict.items():
                    goods_info = items.get_data_by_item_id(k)
                    msg += f"道友成功收获药材：{goods_info['name']} {v} 个！\n"
                    sql_message.send_back(user_info['user_id'], k, goods_info['name'], '药材', v)
            mix_elixir_info['收取时间'] = nowtime
            save_player_info(user_id, mix_elixir_info, "mix_elixir_info")
            sql_message.update_ls(user_id, harvest_cost, 2) # 扣取驱动傀儡灵石
            # await handle_send(bot, event, msg)
            log_message(user_id, msg)
            # await yaocai_get.finish()
            return msg
        else:
            remaining_time = round(GETCONFIG['time_cost'] * (1 - (GETCONFIG['加速基数'] * mix_elixir_info['药材速度'])),
                                   2) - timedeff
            hours = int(remaining_time)
            minutes = int((remaining_time - hours) * 60)
            msg = f"道友的灵田还不能收取，下次收取时间为：{hours}小时{minutes}分钟之后"
            # await handle_send(bot, event, msg)
            # await yaocai_get.finish()
            return msg


@buy_puppet.handle()
async def buy_puppet_handler(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """购买灵田傀儡"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)

    if not isUser:
        await handle_send(bot, event, msg)
        await buy_puppet.finish()

    if int(user_info['blessed_spot_flag']) == 0:
        msg = f"道友还没有洞天福地呢，请发送洞天福地购买来购买吧~"
        await handle_send(bot, event, msg)
        await buy_puppet.finish()

    user_id = user_info['user_id']
    mix_elixir_info = get_player_info(user_id, "mix_elixir_info")
    puppet_level = mix_elixir_info['灵田傀儡']

    if puppet_level > 0:
        msg = "道友已经拥有灵田傀儡了！"
        await handle_send(bot, event, msg)
        await buy_puppet.finish()

    cost = 10000000
    if user_info['stone'] < cost:
        msg = f"购买灵田傀儡需要 {cost} 灵石，道友的灵石不足！"
        await handle_send(bot, event, msg)
        await buy_puppet.finish()

    # 扣除灵石并更新傀儡
    sql_message.update_ls(user_id, cost, 2)
    mix_elixir_info['灵田傀儡'] = 1
    save_player_info(user_id, mix_elixir_info, 'mix_elixir_info')

    msg = f"恭喜道友成功购买灵田傀儡！消耗灵石：{cost}，当前傀儡等级：1级"
    await handle_send(bot, event, msg)
    await buy_puppet.finish()


@upgrade_puppet.handle()
async def upgrade_puppet_handler(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """灵田傀儡升级"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)

    if not isUser:
        await handle_send(bot, event, msg)
        await upgrade_puppet.finish()

    user_id = user_info['user_id']
    mix_elixir_info = get_player_info(user_id, "mix_elixir_info")
    puppet_level = mix_elixir_info['灵田傀儡']

    if puppet_level == 0:
        msg = "道友还没有灵田傀儡，请先购买！"
        await handle_send(bot, event, msg)
        await upgrade_puppet.finish()

    if puppet_level >= 3:
        msg = "道友的灵田傀儡已经达到最高等级！"
        await handle_send(bot, event, msg)
        await upgrade_puppet.finish()

    next_puppet_level = puppet_level+ 1
    cost = PUPPET_CONFIG[puppet_level]['upgrade_cost']

    if user_info['stone'] < cost:
        msg = f"升级到{next_puppet_level}级需要 {cost} 灵石，道友的灵石不足！"
        await handle_send(bot, event, msg)
        await upgrade_puppet.finish()

    # 扣除灵石并升级
    sql_message.update_ls(user_id, cost, 2)
    mix_elixir_info['灵田傀儡'] = next_puppet_level
    save_player_info(user_id, mix_elixir_info, 'mix_elixir_info')

    msg = f"恭喜道友成功将灵田傀儡升级到{next_puppet_level}级！消耗灵石：{cost}"
    await handle_send(bot, event, msg)
    await upgrade_puppet.finish()


@start_puppet.handle()
async def start_puppet_handler(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """开启灵田傀儡"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)

    if not isUser:
        await handle_send(bot, event, msg)
        await start_puppet.finish()

    user_id = user_info['user_id']
    mix_elixir_info = get_player_info(user_id, "mix_elixir_info")
    puppet_level = mix_elixir_info['灵田傀儡']

    if puppet_level == 0:
        msg = "道友还没有灵田傀儡，请先购买！"
        await handle_send(bot, event, msg)
        await start_puppet.finish()

    # 数据库灵田傀儡 参数设置成 1
    sql_message.set_puppet_status(user_id, 1)

    harvest_cost = PUPPET_CONFIG[puppet_level]['harvest_cost']

    msg = f"灵田傀儡已开启！每小时将自动检测并收取灵田，每次收取消耗灵石：{harvest_cost}"
    await handle_send(bot, event, msg)
    await start_puppet.finish()


@stop_puppet.handle()
async def stop_puppet_handler(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """关闭灵田傀儡"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)

    if not isUser:
        await handle_send(bot, event, msg)
        await stop_puppet.finish()

    user_id = user_info['user_id']
    mix_elixir_info = get_player_info(user_id, "mix_elixir_info")
    puppet_level = mix_elixir_info['灵田傀儡']

    if puppet_level == 0:
        msg = "道友还没有灵田傀儡，请先购买！"
        await handle_send(bot, event, msg)
        await stop_puppet.finish()

    # 数据库灵田傀儡 参数设置成 0 关闭
    sql_message.set_puppet_status(user_id, 0)

    msg = "灵田傀儡已关闭！"
    await handle_send(bot, event, msg)
    await stop_puppet.finish()


@puppet_info.handle()
async def puppet_info_handler(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """灵田信息"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)

    if not isUser:
        await handle_send(bot, event, msg)
        await puppet_info.finish()

    if int(user_info['blessed_spot_flag']) == 0:
        msg = f"道友还没有洞天福地呢，请发送洞天福地购买来购买吧~"
        await handle_send(bot, event, msg)
        await puppet_info.finish()

    user_id = user_info['user_id']
    mix_elixir_info = get_player_info(user_id, "mix_elixir_info")
    last_time = mix_elixir_info['收取时间']
    puppet_level = mix_elixir_info['灵田傀儡']

    if not mix_elixir_info:
        msg = "获取灵田信息失败！"
        await handle_send(bot, event, msg)
        await puppet_info.finish()

    # 计算下次收取时间
    GETCONFIG = {
        "time_cost": 23,
        "加速基数": 0.05
    }

    nowtime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # str
    timedeff = round((datetime.strptime(nowtime, '%Y-%m-%d %H:%M:%S') -
                      datetime.strptime(last_time,'%Y-%m-%d %H:%M:%S')).total_seconds() / 3600,2)

    if timedeff >= round(GETCONFIG['time_cost'] * (1 - (GETCONFIG['加速基数'] * mix_elixir_info['药材速度'])), 2):
        elixir_time = "灵药已成熟，可以收取"
    else:
        remaining_time = round(GETCONFIG['time_cost'] * (1 - (GETCONFIG['加速基数'] * mix_elixir_info['药材速度'])), 2) - timedeff
        hours = int(remaining_time)
        minutes = int((remaining_time - hours) * 60)
        elixir_time = f"灵药成熟还需{hours}小时{minutes}分钟"

    msg = "道友的灵田信息：\n"
    msg += f"灵田数量：{mix_elixir_info['灵田数量']}\n"
    msg += f"药材速度：增加{mix_elixir_info['药材速度'] * 100}%\n"
    msg += f"灵药状态：{elixir_time}\n"
    if puppet_level > 0:
        status = "关闭"
        puppet_status = sql_message.check_puppet_status(user_id)  # 返回 0 或 1
        if puppet_status == 1:
            status = "开启"

        msg += f"傀儡等级：{puppet_level}级\n"
        msg += f"傀儡状态：{status}\n"

    await handle_send(bot, event, msg)
    await puppet_info.finish()
