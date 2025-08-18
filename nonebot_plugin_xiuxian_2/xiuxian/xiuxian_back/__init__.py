import asyncio
import random
import time
import re
import os
import json
from pathlib import Path
from datetime import datetime
from nonebot import on_command, require, on_fullmatch
from nonebot.adapters.onebot.v11 import (
    Bot,
    GROUP,
    Message,
    GroupMessageEvent,
    PrivateMessageEvent,
    MessageSegment,
    GROUP_ADMIN,
    GROUP_OWNER,
    ActionFailed
)
from ..xiuxian_utils.lay_out import assign_bot, assign_bot_group, Cooldown, CooldownIsolateLevel
from nonebot.log import logger
from nonebot.params import CommandArg
from nonebot.permission import SUPERUSER
from .back_util import (
    get_user_main_back_msg, get_user_yaocai_back_msg, get_user_danyao_back_msg, check_equipment_can_use,
    get_use_equipment_sql, get_shop_data, save_shop,
    get_item_msg, get_item_msg_rank, check_use_elixir,
    get_use_jlq_msg, get_no_use_equipment_sql
)
from .backconfig import get_auction_config, savef_auction, remove_auction_item
from ..xiuxian_utils.item_json import Items
from ..xiuxian_utils.utils import (
    check_user, get_msg_pic, 
    send_msg_handler, CommandObjectID,
    Txt2Img, number_to, handle_send, handle_pagination
)
from ..xiuxian_utils.xiuxian2_handle import (
    XiuxianDateManage, get_weapon_info_msg, get_armor_info_msg,
    get_sec_msg, get_main_info_msg, get_sub_info_msg, UserBuffDate
)
from ..xiuxian_config import XiuConfig, convert_rank

items = Items()
config = get_auction_config()
groups = config['open']  # list，群交流会使用
auction = {}
AUCTIONSLEEPTIME = 120  # 拍卖初始等待时间（秒）
cache_help = {}
auction_offer_flag = False  # 拍卖标志
AUCTIONOFFERSLEEPTIME = 30  # 每次拍卖增加拍卖剩余的时间（秒）
auction_offer_time_count = 0  # 计算剩余时间
auction_offer_all_count = 0  # 控制线程等待时间
auction_time_config = config['拍卖会定时参数'] # 定时配置
sql_message = XiuxianDateManage()  # sql类
# 定时任务
set_auction_by_scheduler = require("nonebot_plugin_apscheduler").scheduler
reset_day_num_scheduler = require("nonebot_plugin_apscheduler").scheduler

# 仙肆系统配置
XIANSHI_TYPES = ["药材", "装备", "丹药", "技能"]  # 允许上架的类型
MIN_PRICE = 600000  # 最低上架价格60万灵石
MAX_QUANTITY = 10   # 单次最大上架数量

# 文件路径
XIANSHI_DATA_PATH = Path(__file__).parent / "xianshi_data"
XIANSHI_DATA_PATH.mkdir(parents=True, exist_ok=True)

xiuxian_shop_view = on_command("仙肆查看", priority=5, block=True)
xian_shop_add = on_command("仙肆上架", priority=5, block=True)
xianshi_auto_add = on_command("仙肆自动上架", priority=5, block=True)
xianshi_fast_add = on_command("仙肆快速上架", priority=5, block=True)
xianshi_fast_buy = on_command("仙肆快速购买", priority=5, block=True)
xian_shop_remove = on_command("仙肆下架", priority=5, block=True)
xian_buy = on_command("仙肆购买", priority=5, block=True)
my_xian_shop = on_command("我的仙肆", priority=5, block=True)
xian_shop_added_by_admin = on_command("系统仙肆上架", priority=5, permission=SUPERUSER, block=True)
xian_shop_remove_by_admin = on_command("系统仙肆下架", priority=5, permission=SUPERUSER, block=True)
xian_shop_off_all = on_fullmatch("清空仙肆", priority=3, permission=SUPERUSER, block=True)

# 坊市系统配置
FANGSHI_TYPES = ["药材", "装备", "丹药", "技能"]  # 允许上架的类型
FANGSHI_MIN_PRICE = 600000  # 最低上架价格60万灵石
FANGSHI_MAX_QUANTITY = 10   # 单次最大上架数量

# 文件路径
FANGSHI_DATA_PATH = Path(__file__).parent / "fangshi_data"
FANGSHI_DATA_PATH.mkdir(parents=True, exist_ok=True)

# 初始化命令
shop_view = on_command("坊市查看", priority=5, permission=GROUP, block=True)
shop_added = on_command("坊市上架", priority=5, permission=GROUP, block=True)
fangshi_auto_add = on_command("坊市自动上架", priority=5, permission=GROUP, block=True)
fangshi_fast_add = on_command("坊市快速上架", priority=5, permission=GROUP, block=True)
fangshi_fast_buy = on_command("坊市快速购买", priority=5, permission=GROUP, block=True)
shop_remove = on_command("坊市下架", priority=5, permission=GROUP, block=True)
buy = on_command("坊市购买", priority=5, permission=GROUP, block=True)
my_shop = on_command("我的坊市", priority=5, permission=GROUP, block=True)
shop_added_by_admin = on_command("系统坊市上架", priority=5, permission=SUPERUSER, block=True)
shop_remove_by_admin = on_command("系统坊市下架", priority=5, permission=SUPERUSER, block=True)
shop_off_all = on_fullmatch("清空坊市", priority=3, permission=SUPERUSER, block=True)

# 鬼市系统配置
GUISHI_TYPES = ["药材", "装备", "技能"]  # 允许交易的类型
GUISHI_MIN_PRICE = 600000  # 最低交易价格60万灵石
GUISHI_MAX_QUANTITY = 100   # 单次最大交易数量
# 配置参数
BANNED_ITEM_IDS = ["15357", "9935", "9940"]  # 禁止交易的物品ID
MAX_QIUGOU_ORDERS = 10  # 最大求购订单数
MAX_BAITAN_ORDERS = 10  # 最大摆摊订单数

# 文件路径
GUISHI_DATA_PATH = Path(__file__).parent / "guishi_data"
GUISHI_DATA_PATH.mkdir(parents=True, exist_ok=True)

# 鬼市命令
guishi_deposit = on_command("鬼市存灵石", priority=5, block=True)
guishi_withdraw = on_command("鬼市取灵石", priority=5, block=True)
guishi_take_item = on_command("鬼市取物品", priority=5, block=True)
guishi_info = on_command("鬼市信息", priority=5, block=True)
guishi_qiugou = on_command("鬼市求购", priority=5, block=True)
guishi_cancel_qiugou = on_command("鬼市取消求购", priority=5, block=True)
guishi_baitan = on_command("鬼市摆摊", priority=5, block=True)
guishi_shoutan = on_command("鬼市收摊", priority=5, block=True)

# 其他原有命令保持不变
chakan_wupin = on_command("查看修仙界物品", aliases={"查看"}, priority=5, block=True)
check_item_effect = on_command("查看效果", aliases={"查", "效果"}, priority=6, block=True)
goods_re_root = on_command("炼金", priority=6, block=True)
fast_alchemy = on_command("快速炼金", aliases={"一键炼金"}, priority=6, block=True)
auction_view = on_command("拍卖品查看", aliases={"查看拍卖品"}, priority=8, permission=GROUP, block=True)
main_back = on_command('我的背包', aliases={'我的物品'}, priority=10, block=True)
yaocai_back = on_command('药材背包', priority=10, block=True)
danyao_back = on_command('丹药背包', priority=10, block=True)
use = on_command("使用", priority=15, block=True)
no_use_zb = on_command("换装", aliases={'卸装'}, priority=5, block=True)
auction_added = on_command("提交拍卖品", aliases={"拍卖品提交"}, priority=10, permission=GROUP, block=True)
auction_withdraw = on_command("撤回拍卖品", aliases={"拍卖品撤回"}, priority=10, permission=GROUP, block=True)
set_auction = on_command("拍卖会", priority=4, permission=GROUP and (SUPERUSER | GROUP_ADMIN | GROUP_OWNER), block=True)
creat_auction = on_fullmatch("举行拍卖会", priority=5, permission=GROUP and SUPERUSER, block=True)
offer_auction = on_command("拍卖", priority=5, permission=GROUP, block=True)
back_help = on_command("交易帮助", aliases={"背包帮助", "仙肆帮助", "坊市帮助", "鬼市", "拍卖帮助"}, priority=8, block=True)
xiuxian_sone = on_fullmatch("灵石", priority=4, block=True)

# 重置丹药每日使用次数
@reset_day_num_scheduler.scheduled_job("cron", hour=0, minute=0, )
async def reset_day_num_scheduler_():
    sql_message.day_num_reset()
    logger.opt(colors=True).info(f"<green>每日丹药使用次数重置成功！</green>")


# 定时任务生成拍卖会
@set_auction_by_scheduler.scheduled_job("cron", hour=auction_time_config['hours'], minute=auction_time_config['minutes'])
async def set_auction_by_scheduler_():
    global auction, auction_offer_flag, auction_offer_all_count, auction_offer_time_count
    if groups:
        if auction:
            logger.opt(colors=True).info(f"<green>已存在一场拍卖会，已清除！</green>")
            auction = {}

    auction_items = []
    try:
        # 用户拍卖品
        user_auction_id_list = get_user_auction_id_list()
        for auction_id in user_auction_id_list:
            user_auction_info = get_user_auction_price_by_id(auction_id)
            auction_items.append((auction_id, user_auction_info['quantity'], user_auction_info['start_price'], True))

        # 系统拍卖品
        auction_id_list = get_auction_id_list()
        auction_count = random.randint(3, 8)  # 随机挑选系统拍卖品数量
        auction_ids = random.sample(auction_id_list, auction_count)
        for auction_id in auction_ids:
            item_info = items.get_data_by_item_id(auction_id)
            item_quantity = 1
            if item_info['type'] in ['神物', '丹药']:
                item_quantity = random.randint(1, 3) # 丹药的话随机挑1-3个
            auction_items.append((auction_id, item_quantity, get_auction_price_by_id(auction_id)['start_price'], False))
    except LookupError:
        logger.opt(colors=True).info("<red>获取不到拍卖物品的信息，请检查配置文件！</red>")
        return
    
    # 打乱拍卖品顺序
    random.shuffle(auction_items)
    
    logger.opt(colors=True).info("<red>野生的大世界定时拍卖会出现了！！！，请管理员在这个时候不要重启机器人</red>")
    msg = f"大世界定时拍卖会出现了！！！\n"
    msg = f"请各位道友稍作准备，拍卖即将开始...\n"
    msg += f"本场拍卖会共有{len(auction_items)}件物品，将依次拍卖，分别是：\n"
    for idx, (auction_id, item_quantity, start_price, is_user_auction) in enumerate(auction_items):
        item_name = items.get_data_by_item_id(auction_id)['name']
        if is_user_auction:
            owner_info = sql_message.get_user_info_with_id(get_user_auction_price_by_id(auction_id)['user_id'])
            owner_name = owner_info['user_name']
            msg += f"{idx + 1}号：{item_name}x{item_quantity}（由{owner_name}道友提供）\n"
        else:
            msg += f"{idx + 1}号：{item_name}x{item_quantity}（由拍卖场提供）\n"

    for gid in groups:
        bot = await assign_bot_group(group_id=gid)
        try:
            await handle_send(bot, event, msg)
        except ActionFailed:
            continue
    
    auction_results = []  # 拍卖结果
    for i, (auction_id, item_quantity, start_price, is_user_auction) in enumerate(auction_items):
        auction_info = items.get_data_by_item_id(auction_id)

        auction = {
            'id': auction_id,
            'user_id': 0,
            'now_price': start_price,
            'name': auction_info['name'],
            'type': auction_info['type'],
            'quantity': item_quantity,
            'start_time': datetime.now(),
            'group_id': 0
        }

        
        if i + 1 == len(auction_items):
            msg = f"最后一件拍卖品为：\n{get_auction_msg(auction_id)}\n"
        else:
            msg = f"第{i + 1}件拍卖品为：\n{get_auction_msg(auction_id)}\n"
        msg += f"\n底价为{start_price}，加价不少于{int(start_price * 0.05)}"
        msg += f"\n竞拍时间为:{AUCTIONSLEEPTIME}秒，请诸位道友发送 拍卖+金额 来进行拍卖吧！"

        if auction['quantity'] > 1:
            msg += f"\n注意：拍卖品共{auction['quantity']}件，最终价为{auction['quantity']}x成交价。\n"

        if i + 1 < len(auction_items):
            next_item_name = items.get_data_by_item_id(auction_items[i + 1][0])['name']
            msg += f"\n下一件拍卖品为：{next_item_name}，请心仪的道友提前开始准备吧！"

        for gid in groups:
            bot = await assign_bot_group(group_id=gid)
            try:
                await handle_send(bot, event, msg)
            except ActionFailed:
                continue

     
        remaining_time = AUCTIONSLEEPTIME # 第一轮定时
        while remaining_time > 0:
            await asyncio.sleep(10)
            remaining_time -= 10


        while auction_offer_flag:  # 有人拍卖
            if auction_offer_all_count == 0:
                auction_offer_flag = False
                break

            logger.opt(colors=True).info(f"<green>有人拍卖，本次等待时间：{auction_offer_all_count * AUCTIONOFFERSLEEPTIME}秒</green>")
            first_time = auction_offer_all_count * AUCTIONOFFERSLEEPTIME
            auction_offer_all_count = 0
            auction_offer_flag = False
            await asyncio.sleep(first_time)
            logger.opt(colors=True).info(f"<green>总计等待时间{auction_offer_time_count * AUCTIONOFFERSLEEPTIME}秒，当前拍卖标志：{auction_offer_flag}，本轮等待时间：{first_time}</green>")

        logger.opt(colors=True).info(f"<green>等待时间结束，总计等待时间{auction_offer_time_count * AUCTIONOFFERSLEEPTIME}秒</green>")
        if auction['user_id'] == 0:
            msg = f"很可惜，{auction['name']}流拍了\n"
            if i + 1 == len(auction_items):
                msg += f"本场拍卖会到此结束，开始整理拍卖会结果，感谢各位道友参与！"
                
            for gid in groups:
                bot = await assign_bot_group(group_id=gid)
                try:
                    await handle_send(bot, event, msg)
                except ActionFailed:  # 发送群消息失败
                    continue
            auction_results.append((auction_id, None, auction['group_id'], auction_info['type'], auction['now_price'], auction['quantity']))
            auction = {}
            continue
        
        user_info = sql_message.get_user_info_with_id(auction['user_id'])
        msg = f"(拍卖锤落下)！！！\n"
        msg += f"恭喜来自群{auction['group_id']}的{user_info['user_name']}道友成功拍下：{auction['type']}-{auction['name']}x{auction['quantity']}，将在拍卖会结算后送到您手中。\n"
        if i + 1 == len(auction_items):
            msg += f"本场拍卖会到此结束，开始整理拍卖会结果，感谢各位道友参与！"

        auction_results.append((auction_id, user_info['user_id'], auction['group_id'], 
                                auction_info['type'], auction['now_price'], auction['quantity']))
        auction = {}
        auction_offer_time_count = 0
        for gid in groups:

            bot = await assign_bot_group(group_id=gid)
            try:
                await handle_send(bot, event, msg)
            except ActionFailed:
                continue

        await asyncio.sleep(random.randint(5, 30))

    # 拍卖会结算
    logger.opt(colors=True).info(f"<green>野生的大世界定时拍卖会结束了！！！</green>")
    end_msg = f"本场拍卖会结束！感谢各位道友的参与。\n拍卖结果整理如下：\n"
    for idx, (auction_id, user_id, group_id, item_type, final_price, quantity) in enumerate(auction_results):
        item_name = items.get_data_by_item_id(auction_id)['name']
        final_user_info = sql_message.get_user_info_with_id(user_id)
        if user_id:
            if final_user_info['stone'] < (int(final_price) * quantity):
                end_msg += f"{idx + 1}号拍卖品：{item_name}x{quantity} - 道友{final_user_info['user_name']}的灵石不足，流拍了\n"
            else:
                sql_message.update_ls(user_id, int(final_price) * quantity, 2)
                sql_message.send_back(user_id, auction_id, item_name, item_type, quantity)
                end_msg += f"{idx + 1}号拍卖品：{item_name}x{quantity}由群{group_id}的{final_user_info['user_name']}道友成功拍下\n"

            user_auction_info = get_user_auction_price_by_id(auction_id)
            if user_auction_info:
                seller_id = user_auction_info['user_id']
                auction_earnings = int(final_price) * quantity * 0.7 # 收个手续费
                sql_message.update_ls(seller_id, auction_earnings, 1)

            remove_auction_item(auction_id)

            auction = {}
            auction_offer_time_count = 0
        else:
            end_msg += f"{idx + 1}号拍卖品：{item_name}x{quantity} - 流拍了\n"

    for gid in groups:
        bot = await assign_bot_group(group_id=gid)
        try:
            await handle_send(bot, event, end_msg)
        except ActionFailed:  # 发送群消息失败
            continue

    return


@check_item_effect.handle(parameterless=[Cooldown(at_sender=False)])
async def check_item_effect_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """查看物品效果，支持物品名或ID"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)

    # 检查用户是否已注册修仙
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg)
        await check_item_effect.finish()

    # 获取用户输入的物品名或ID
    input_str = args.extract_plain_text().strip()
    if not input_str:
        msg = "请输入物品名称或ID！\n例如：查看效果 渡厄丹 或 查看效果 1999"
        await handle_send(bot, event, msg)
        await check_item_effect.finish()

    # 判断输入是ID还是名称
    goods_id = None
    if input_str.isdigit():  # 如果是纯数字，视为ID
        goods_id = int(input_str)
        item_info = items.get_data_by_item_id(goods_id)
        if not item_info:
            msg = f"ID {goods_id} 对应的物品不存在，请检查输入！"
            await handle_send(bot, event, msg)
            await check_item_effect.finish()
    else:  # 视为物品名称
        for k, v in items.items.items():
            if input_str == v['name']:
                goods_id = k
                break
        if goods_id is None:
            msg = f"物品 {input_str} 不存在，请检查名称是否正确！"
            await handle_send(bot, event, msg)
            await check_item_effect.finish()
    item_msg = get_item_msg(goods_id)
    if goods_id == 15053 or input_str == "补偿":
        await check_item_effect.finish()
    # 构造返回消息
    msg = f"\nID：{goods_id}\n{item_msg}"
    await handle_send(bot, event, msg)
    await check_item_effect.finish()
    
@back_help.handle(parameterless=[Cooldown(at_sender=False)])
async def back_help_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """交易系统帮助"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    message = str(event.message)
    
    # 提取中文关键词
    rank_msg = r'[\u4e00-\u9fa5]+'
    message = re.findall(rank_msg, message)
    
    # 帮助内容分块
    help_sections = {
        "背包": """
【背包帮助】
🔹 我的背包 [页码] - 查看背包物品
🔹 药材背包 [页码] - 查看药材类物品
🔹 丹药背包 [页码] - 查看丹药类物品
🔹 使用+物品名 [数量] - 使用物品
🔹 换装/卸装+装备名 - 卸下装备
🔹 炼金+物品名 [数量] - 将物品转化为灵石
🔹 快速炼金 类型 品阶 - 批量炼金指定类型物品
🔹 查看修仙界物品+类型 [页码] - 查看物品图鉴
🔹 查看效果+物品名 - 查看物品详情
🔹 灵石 - 查看当前灵石数量
""".strip(),
        "仙肆": """
【仙肆帮助】（全服交易）
🔸 仙肆查看 [类型] [页码] - 查看全服仙肆
  ▶ 支持类型：技能|装备|丹药|药材
🔸 仙肆上架 物品 金额 [数量] - 上架物品
  ▶ 最低金额60万灵石，手续费10-30%
🔸 仙肆快速上架 物品 [金额] - 快速上架10个物品
  ▶ 自动匹配最低价，数量固定10个（或全部）
🔸 仙肆快速购买 物品 - 快速购买物品
  ▶ 自动匹配最低价，可快速购买5种物品
🔸 仙肆自动上架 类型 品阶 [数量] - 批量上架
  ▶ 示例：仙肆自动上架 装备 通天
🔸 仙肆购买 编号 [数量] - 购买物品
🔸 仙肆下架 编号 - 下架自己的物品
🔸 我的仙肆 [页码] - 查看自己上架的物品
""".strip(),
        "坊市": """
【坊市帮助】（群内交易）
🔸 坊市查看 [类型] [页码] - 查看群坊市
  ▶ 支持类型：技能|装备|丹药|药材
🔸 坊市上架 物品 金额 [数量] - 上架物品
  ▶ 最低金额60万灵石，手续费10-30%
🔸 坊市快速上架 物品 [金额] - 快速上架10个物品
  ▶ 自动匹配最低价，数量固定10个（或全部）
🔸 坊市快速购买 物品 - 快速购买物品
  ▶ 自动匹配最低价，可快速购买5种物品
🔸 坊市自动上架 类型 品阶 [数量] - 批量上架
  ▶ 示例：坊市自动上架 药材 五品
🔸 坊市购买 编号 [数量] - 购买物品
🔸 坊市下架 编号 - 下架自己的物品
🔸 我的坊市 [页码] - 查看自己上架的物品
""".strip(),
        "鬼市": """
【鬼市帮助】（匿名交易）
👻 鬼市存灵石 数量 - 存入灵石到鬼市账户
👻 鬼市取灵石 数量 - 取出灵石（收取20%暂存费）
👻 鬼市信息 - 查看鬼市账户和交易信息
👻 鬼市求购 物品 价格 [数量] - 发布求购订单
👻 鬼市取消求购 订单ID - 取消求购订单
👻 鬼市摆摊 物品 价格 [数量] - 摆摊出售物品
👻 鬼市收摊 摊位ID - 收摊并结算
""".strip(),
        "拍卖": f"""
【拍卖帮助】
🎫 查看拍卖品 - 查看待拍卖物品
🎫 提交拍卖品 物品 底价 [数量] - 提交拍卖
🎫 拍卖+金额 - 参与竞拍
🎫 撤回拍卖品 编号 - 撤回自己的拍卖品
🎫 举行拍卖会 - (管理员)开启拍卖
⏰ 每日{auction_time_config['hours']}点自动举行拍卖会
""".strip(),
        "交易": """
【交易系统总览】
输入以下关键词查看详细帮助：
🔹 背包帮助 - 背包相关功能
🔹 仙肆帮助 - 全服交易市场
🔹 坊市帮助 - 群内交易市场
🔹 拍卖帮助 - 拍卖会功能

【系统规则】
💰 手续费规则：
  - 500万以下：10%
  - 500-1000万：15% 
  - 1000-2000万：20%
  - 2000万以上：30%
""".strip()
    }
    
    # 默认显示交易总览
    if not message:
        msg = help_sections["交易"]
    else:
        # 获取第一个中文关键词
        keyword = message[0]
        
        # 检查是否包含特定关键词
        if "背包" in keyword:
            msg = help_sections["背包"]
        elif "仙肆" in keyword:
            msg = help_sections["仙肆"]
        elif "坊市" in keyword:
            msg = help_sections["坊市"]
        elif "鬼市" in keyword:
            msg = help_sections["鬼市"]
        elif "拍卖" in keyword or "拍卖会" in keyword:
            msg = help_sections["拍卖"]
        elif "全部" in keyword:
            msg = (
                help_sections["背包"] + "\n\n" + 
                help_sections["仙肆"] + "\n\n" + 
                help_sections["坊市"] + "\n\n" + 
                help_sections["鬼市"] + "\n\n" + 
                help_sections["拍卖"]
            )
        elif "交易" in keyword:
            msg = help_sections["交易"]
        else:
            # 默认显示交易总览和可用指令
            msg = "请输入正确的帮助关键词：\n"
            msg += "背包帮助 | 仙肆帮助 | 坊市帮助 | 拍卖帮助 | 交易帮助\n"
            msg += "或输入'交易帮助全部'查看完整帮助"
    
    await handle_send(bot, event, f"\n{msg}")
    await back_help.finish()

@xiuxian_sone.handle(parameterless=[Cooldown(at_sender=False)])
async def xiuxian_sone_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """我的灵石信息"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await xiuxian_sone.finish()
    msg = f"当前灵石：{user_info['stone']}({number_to(user_info['stone'])})"
    await handle_send(bot, event, msg)
    await xiuxian_sone.finish()

def get_xianshi_index():
    """获取仙肆索引数据"""
    index_file = XIANSHI_DATA_PATH / "仙肆索引.json"
    try:
        if index_file.exists():
            with open(index_file, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception as e:
        logger.error(f"读取仙肆索引失败: {e}")
    return {"next_id": 1, "items": {}}

def save_xianshi_index(data):
    """保存仙肆索引"""
    index_file = XIANSHI_DATA_PATH / "仙肆索引.json"
    try:
        with open(index_file, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        return True
    except Exception as e:
        logger.error(f"保存仙肆索引失败: {e}")
        return False

def get_xianshi_type_data(item_type):
    """获取指定类型的仙肆数据"""
    if item_type not in XIANSHI_TYPES:
        return None
    
    type_file = XIANSHI_DATA_PATH / f"仙肆_{item_type}.json"
    try:
        if type_file.exists():
            with open(type_file, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception as e:
        logger.error(f"读取仙肆{item_type}数据失败: {e}")
    return {}

def save_xianshi_type_data(item_type, data):
    """保存指定类型的仙肆数据"""
    if item_type not in XIANSHI_TYPES:
        return False
    
    type_file = XIANSHI_DATA_PATH / f"仙肆_{item_type}.json"
    try:
        with open(type_file, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        return True
    except Exception as e:
        logger.error(f"保存仙肆{item_type}数据失败: {e}")
        return False

def get_item_type_by_id(goods_id):
    """根据物品ID获取类型"""
    # 这里需要接入您的物品系统
    # 示例: return items.get_data_by_item_id(goods_id)['type']
    return items.get_data_by_item_id(goods_id)['type']

def generate_unique_id(existing_ids):
    """生成6-10位随机不重复ID"""
    while True:
        # 使用时间戳+随机数确保唯一性
        timestamp_part = int(time.time() % 10000)
        random_part = random.randint(100, 99999)
        new_id = int(f"{timestamp_part}{random_part}") % 10**10  # 确保不超过10位
        
        # 限制在6-10位
        new_id = max(100000, min(new_id, 9999999999))
        
        # 检查是否已存在
        if str(new_id) not in existing_ids:
            return str(new_id)

def generate_fangshi_id(existing_ids):
    """生成6-10位随机不重复坊市ID"""
    while True:
        # 组合时间戳和随机数确保唯一性
        timestamp_part = int(time.time() % 10000)  # 取时间戳后4位
        random_part = random.randint(100, 99999)   # 5位随机数
        new_id = int(f"{timestamp_part}{random_part}") % 10**10  # 确保不超过10位
        
        # 限制在6-10位范围
        new_id = max(100000, min(new_id, 9999999999))
        
        # 检查是否已存在
        if str(new_id) not in existing_ids:
            return str(new_id)

def get_fangshi_index(group_id):
    """获取坊市索引数据"""
    index_file = FANGSHI_DATA_PATH / f"坊市索引_{group_id}.json"
    try:
        if index_file.exists():
            with open(index_file, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception as e:
        logger.error(f"读取坊市索引失败: {e}")
    return {"next_id": 1, "items": {}}

def save_fangshi_index(group_id, data):
    """保存坊市索引"""
    index_file = FANGSHI_DATA_PATH / f"坊市索引_{group_id}.json"
    try:
        with open(index_file, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        return True
    except Exception as e:
        logger.error(f"保存坊市索引失败: {e}")
        return False

def get_fangshi_type_data(group_id, item_type):
    """获取指定类型的坊市数据"""
    if item_type not in FANGSHI_TYPES:
        return None
    
    type_file = FANGSHI_DATA_PATH / f"坊市_{group_id}_{item_type}.json"
    try:
        if type_file.exists():
            with open(type_file, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception as e:
        logger.error(f"读取坊市{item_type}数据失败: {e}")
    return {}

def save_fangshi_type_data(group_id, item_type, data):
    """保存指定类型的坊市数据"""
    if item_type not in FANGSHI_TYPES:
        return False
    
    type_file = FANGSHI_DATA_PATH / f"坊市_{group_id}_{item_type}.json"
    try:
        with open(type_file, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        return True
    except Exception as e:
        logger.error(f"保存坊市{item_type}数据失败: {e}")
        return False

def get_xianshi_min_price(goods_name):
    """获取仙肆中该物品的最低价格"""
    min_price = None
    
    # 遍历所有类型
    for item_type in ["药材", "装备", "丹药", "技能"]:
        type_file = XIANSHI_DATA_PATH / f"仙肆_{item_type}.json"
        if not type_file.exists():
            continue
        
        with open(type_file, "r", encoding="utf-8") as f:
            type_items = json.load(f)
        
        for item in type_items.values():
            if item["name"] == goods_name:
                if min_price is None or item["price"] < min_price:
                    min_price = item["price"]
    
    return min_price

def get_fangshi_min_price(group_id, goods_name):
    """获取坊市中该物品的最低价格，如果不存在则获取仙肆最低价"""
    min_price = None
    
    # 1. 先在坊市查找最低价
    for item_type in ["药材", "装备", "丹药", "技能"]:
        type_file = FANGSHI_DATA_PATH / f"坊市_{group_id}_{item_type}.json"
        if not type_file.exists():
            continue
        
        with open(type_file, "r", encoding="utf-8") as f:
            type_items = json.load(f)
        
        for item in type_items.values():
            if item["name"] == goods_name:
                if min_price is None or item["price"] < min_price:
                    min_price = item["price"]
    
    # 2. 如果坊市没有，再查找仙肆最低价
    if min_price is None:
        min_price = get_xianshi_min_price(goods_name)
    
    return min_price
        
@xian_shop_add.handle(parameterless=[Cooldown(1.4, at_sender=False)])
async def xian_shop_add_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """仙肆上架"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg)
        await xian_shop_add.finish()
    
    user_id = user_info['user_id']
    args = args.extract_plain_text().split()
    
    if len(args) < 2:
        msg = "请输入正确指令！格式：仙肆上架 物品名称 价格 [数量]"
        await handle_send(bot, event, msg)
        await xian_shop_add.finish()
    
    goods_name = args[0]
    try:
        price = max(int(args[1]), MIN_PRICE)
        quantity = int(args[2]) if len(args) > 2 else 1
        quantity = min(quantity, MAX_QUANTITY)
    except ValueError:
        msg = "请输入有效的价格和数量！"
        await handle_send(bot, event, msg)
        await xian_shop_add.finish()
    
    # 检查背包是否有该物品（需要接入您的背包系统）
    back_msg = sql_message.get_back_msg(user_id)
    goods_info = None
    for item in back_msg:
        if item['goods_name'] == goods_name:
            goods_info = item
            break
    
    if not goods_info:
        msg = f"请检查该道具 {goods_name} 是否在背包内！"
        await handle_send(bot, event, msg)
        await xian_shop_add.finish()
    
    # 检查绑定物品
    if goods_info['bind_num'] >= goods_info['goods_num']:
        msg = f"该物品是绑定物品，无法上架！"
        await handle_send(bot, event, msg)
        await xian_shop_add.finish()
    
    # 检查可上架数量
    available_num = goods_info['goods_num'] - goods_info['bind_num']
    if quantity > available_num:
        msg = f"可上架数量不足！背包有{goods_info['goods_num']}个（{goods_info['bind_num']}个绑定），最多可上架{available_num}个"
        await handle_send(bot, event, msg)
        await xian_shop_add.finish()
    
    # 获取物品类型
    goods_type = get_item_type_by_id(goods_info['goods_id'])
    if goods_type not in XIANSHI_TYPES:
        msg = f"该物品类型不允许上架！允许类型：{', '.join(XIANSHI_TYPES)}"
        await handle_send(bot, event, msg)
        await xian_shop_add.finish()
    
    # 计算手续费
    total_price = price * quantity
    if total_price <= 5000000:
        fee_rate = 0.1
    elif total_price <= 10000000:
        fee_rate = 0.15
    elif total_price <= 20000000:
        fee_rate = 0.2
    else:
        fee_rate = 0.3
    
    # 计算手续费（按单个物品计算）
    single_fee = int(price * fee_rate)
    total_fee = single_fee * quantity
    
    if user_info['stone'] < total_fee:
        msg = f"灵石不足支付手续费！需要{total_fee}灵石，当前拥有{user_info['stone']}灵石"
        await handle_send(bot, event, msg)
        await xian_shop_add.finish()
    
    # 扣除总手续费
    sql_message.update_ls(user_id, total_fee, 2)
    
    # 为每个物品创建独立条目
    success_count = 0
    for _ in range(quantity):
        # 生成唯一ID
        index_data = get_xianshi_index()
        existing_ids = set(index_data["items"].keys())
        xianshi_id = generate_unique_id(existing_ids)
        
        # 添加到索引
        index_data["items"][xianshi_id] = {
            "type": goods_type,
            "user_id": user_id
        }
        save_xianshi_index(index_data)
        
        # 添加到类型文件，数量固定为1
        type_items = get_xianshi_type_data(goods_type)
        type_items[xianshi_id] = {
            "id": xianshi_id,
            "goods_id": goods_info['goods_id'],
            "name": goods_name,
            "type": goods_type,
            "price": price,
            "quantity": 1,  # 固定为1
            "user_id": user_id,
            "user_name": user_info['user_name'],
            "desc": get_item_msg(goods_info['goods_id'])
        }
        save_xianshi_type_data(goods_type, type_items)
        
        # 从背包扣除1个物品
        sql_message.update_back_j(user_id, goods_info['goods_id'], num=1)
        success_count += 1
    
    msg = f"成功上架 {goods_name} x{success_count} 到仙肆！\n"
    msg += f"单价: {number_to(price)} 灵石\n"
    msg += f"总手续费: {number_to(total_fee)} 灵石"
    
    await handle_send(bot, event, msg)
    await xian_shop_add.finish()

@xian_shop_remove.handle(parameterless=[Cooldown(1.4, at_sender=False)])
async def xian_shop_remove_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """仙肆下架"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg)
        await xian_shop_remove.finish()
    
    user_id = user_info['user_id']
    args = args.extract_plain_text().split()
    
    if not args:
        msg = "请输入要下架的物品名称！"
        await handle_send(bot, event, msg)
        await xian_shop_remove.finish()
    
    goods_name = args[0]
    quantity = int(args[1]) if len(args) > 1 and args[1].isdigit() else None
    
    # 获取所有类型中该用户上架的该物品
    user_items = []
    index_data = get_xianshi_index()
    
    for xianshi_id, item_info in index_data["items"].items():
        if str(item_info["user_id"]) == str(user_id):
            type_items = get_xianshi_type_data(item_info["type"])
            if xianshi_id in type_items and type_items[xianshi_id]["name"] == goods_name:
                user_items.append({
                    "id": xianshi_id,
                    "price": type_items[xianshi_id]["price"],
                    "type": item_info["type"]
                })
    
    if not user_items:
        msg = f"您在仙肆中没有上架 {goods_name}！"
        await handle_send(bot, event, msg)
        await xian_shop_remove.finish()
    
    # 按价格从低到高排序
    user_items.sort(key=lambda x: x["price"])
    
    # 确定要下架的数量
    if quantity is None:
        # 没指定数量则下架最低价的1个
        items_to_remove = [user_items[0]]
    else:
        # 指定数量则下架价格从低到高的指定数量
        items_to_remove = user_items[:quantity]
    
    # 执行下架操作
    removed_count = 0
    for item in items_to_remove:
        # 从类型文件中移除
        type_items = get_xianshi_type_data(item["type"])
        if item["id"] in type_items:
            # 退回物品到背包
            item_data = type_items[item["id"]]
            sql_message.send_back(
                user_id,
                item_data["goods_id"],
                goods_name,
                item["type"],
                1  # 每个条目数量固定为1
            )
            
            # 从系统中移除
            del type_items[item["id"]]
            save_xianshi_type_data(item["type"], type_items)
            
            # 从索引中移除
            del index_data["items"][item["id"]]
            save_xianshi_index(index_data)
            
            removed_count += 1
    
    msg = f"成功下架 {goods_name} x{removed_count}！已退回背包"
    if len(user_items) > removed_count:
        msg += f"\n(仙肆中仍有 {len(user_items)-removed_count} 个 {goods_name})"
    
    await handle_send(bot, event, msg)
    await xian_shop_remove.finish()

@xiuxian_shop_view.handle(parameterless=[Cooldown(at_sender=False)])
async def xiuxian_shop_view_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """仙肆查看"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg)
        await xiuxian_shop_view.finish()
    
    # 解析参数
    args_str = args.extract_plain_text().strip()
    
    # 情况1：无参数 - 显示可用类型
    if not args_str:
        msg = "请指定查看类型：【药材、装备、丹药、技能】"
        await handle_send(bot, event, msg)
        await xiuxian_shop_view.finish()
    
    # 解析类型和页码
    item_type = None
    current_page = 1
    
    # 检查是否直接拼接类型和页码（无空格）
    for t in XIANSHI_TYPES:
        if args_str.startswith(t):
            item_type = t
            remaining = args_str[len(t):].strip()
            if remaining.isdigit():
                current_page = int(remaining)
            break
    
    # 情况2：有空格分隔
    if item_type is None:
        parts = args_str.split(maxsplit=1)
        if parts[0] in XIANSHI_TYPES:
            item_type = parts[0]
            if len(parts) > 1 and parts[1].isdigit():
                current_page = int(parts[1])
    
    # 检查类型有效性
    if item_type not in XIANSHI_TYPES:
        msg = f"无效类型！可用类型：【{', '.join(XIANSHI_TYPES)}】"
        await handle_send(bot, event, msg)
        await xiuxian_shop_view.finish()
    
    # 读取对应类型的物品数据
    type_items = get_xianshi_type_data(item_type)
    if not type_items:
        msg = f"仙肆中暂无{item_type}类物品！"
        await handle_send(bot, event, msg)
        await xiuxian_shop_view.finish()
    
    # 处理物品显示逻辑
    system_items = []  # 存储系统物品
    user_items = {}    # 存储用户物品（按名称分组，只保留最低价）
    
    for item_id, item in type_items.items():
        if item['user_id'] == 0:  # 系统物品
            system_items.append(item)
        else:  # 用户物品
            item_name = item['name']
            # 如果还没有记录或者当前价格更低，更新记录
            if item_name not in user_items or item['price'] < user_items[item_name]['price']:
                user_items[item_name] = item
    
    # 合并系统物品和用户物品，并按价格排序
    items_list = sorted(system_items + list(user_items.values()), key=lambda x: x['name'])
    
    # 分页处理
    per_page = 10
    total_pages = (len(items_list) + per_page - 1) // per_page
    current_page = max(1, min(current_page, total_pages))
    
    if current_page > total_pages:
        msg = f"页码超出范围，最多{total_pages}页！"
        await handle_send(bot, event, msg)
        await xiuxian_shop_view.finish()
    
    # 构建消息
    start_idx = (current_page - 1) * per_page
    end_idx = start_idx + per_page
    paged_items = items_list[start_idx:end_idx]

    # 构建消息
    msg_list = [f"☆------仙肆 {item_type}------☆"]
    for item in paged_items:
        price_str = number_to(item['price'])
        msg = f"\n{item['name']} {price_str}灵石 \nID:{item['id']}"
        
        # 处理数量显示
        if isinstance(item['quantity'], str) and item['quantity'] == "无限":
            msg += f" 不限量"
        elif item['quantity'] > 1:
            msg += f" 限售:{item['quantity']}"
        
        msg_list.append(msg)
    
    msg_list.append(f"\n第 {current_page}/{total_pages} 页")
    if total_pages > 1:
        next_page_cmd = f"仙肆查看{item_type}{current_page + 1}"
        msg_list.append(f"输入 {next_page_cmd} 查看下一页")
    
    await send_msg_handler(bot, event, '仙肆查看', bot.self_id, msg_list)
    await xiuxian_shop_view.finish()

@my_xian_shop.handle(parameterless=[Cooldown(at_sender=False)])
async def my_xian_shop_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """我的仙肆"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg)
        await my_xian_shop.finish()
    
    # 获取页码
    try:
        current_page = int(args.extract_plain_text().strip())
    except:
        current_page = 1
    
    user_id = user_info['user_id']
    
    # 从索引获取用户所有物品
    index_data = get_xianshi_index()
    user_items = []
    
    for xianshi_id, item_info in index_data["items"].items():
        if str(item_info["user_id"]) == str(user_id):
            # 从对应类型文件读取详细信息
            type_items = get_xianshi_type_data(item_info["type"])
            if xianshi_id in type_items:
                user_items.append(type_items[xianshi_id])
    
    # 按价格排序
    user_items.sort(key=lambda x: x['name'])
    
    # 检查是否有上架物品
    if not user_items:
        msg = "您在仙肆中没有上架任何物品！"
        await handle_send(bot, event, msg)
        await my_xian_shop.finish()
    
    # 分页处理
    per_page = 20
    total_pages = (len(user_items) + per_page - 1) // per_page
    current_page = max(1, min(current_page, total_pages))
    
    # 构建消息
    start_idx = (current_page - 1) * per_page
    end_idx = start_idx + per_page
    paged_items = user_items[start_idx:end_idx]
    
    msg_list = [f"☆------{user_info['user_name']}的仙肆物品------☆"]
    for item in paged_items:
        price_str = number_to(item['price'])
        msg = f"{item['name']} {price_str}灵石"
        if item['quantity'] > 1:
            msg += f" x{item['quantity']}"
        msg_list.append(msg)
    
    msg_list.append(f"\n第 {current_page}/{total_pages} 页")
    if total_pages > 1:
        msg_list.append(f"输入 我的仙肆 {current_page + 1} 查看下一页")
    
    await send_msg_handler(bot, event, '我的仙肆', bot.self_id, msg_list)
    await my_xian_shop.finish()

@xian_buy.handle(parameterless=[Cooldown(1.4, at_sender=False)])
async def xian_buy_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """仙肆购买"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg)
        await xian_buy.finish()
    
    user_id = user_info['user_id']
    args = args.extract_plain_text().split()
    
    if len(args) < 1:
        msg = "请输入要购买的仙肆ID！"
        await handle_send(bot, event, msg)
        await xian_buy.finish()
    
    xianshi_id = args[0]
    quantity = int(args[1]) if len(args) > 1 else 1
    
    # 从索引查找物品
    index_data = get_xianshi_index()
    if xianshi_id not in index_data["items"]:
        msg = f"未找到仙肆ID为 {xianshi_id} 的物品！"
        await handle_send(bot, event, msg)
        await xian_buy.finish()
    
    item_info = index_data["items"][xianshi_id]
    
    # 检查是否是自己的物品
    if str(item_info["user_id"]) == str(user_id):
        msg = "不能购买自己上架的物品！"
        await handle_send(bot, event, msg)
        await xian_buy.finish()
    
    # 从类型文件获取详细信息
    type_items = get_xianshi_type_data(item_info["type"])
    if xianshi_id not in type_items:
        msg = "物品数据异常，请联系管理员！"
        await handle_send(bot, event, msg)
        await xian_buy.finish()
    
    item = type_items[xianshi_id]
    seller_name = "系统" if item["user_id"] == 0 else item["user_name"]
    
    # 检查库存（系统无限物品跳过检查）
    if not (isinstance(item["quantity"], str) and item["quantity"] == "无限"):
        if item["quantity"] < quantity:
            msg = f"库存不足！只有 {item['quantity']} 个可用"
            await handle_send(bot, event, msg)
            await xian_buy.finish()
    
    # 计算总价
    total_price = item["price"] * quantity
    
    # 检查灵石是否足够
    if user_info["stone"] < total_price:
        msg = f"灵石不足！需要 {number_to(total_price)} 灵石，当前拥有 {number_to(user_info['stone'])} 灵石"
        await handle_send(bot, event, msg)
        await xian_buy.finish()
    
    try:
        # 扣除买家灵石
        sql_message.update_ls(user_id, total_price, 2)
        
        # 给卖家灵石（如果不是系统物品）
        if item_info["user_id"] != 0:
            seller_id = item_info["user_id"]
            sql_message.update_ls(seller_id, total_price, 1)
        
        # 给买家物品
        sql_message.send_back(
            user_id,
            item["goods_id"],
            item["name"],
            item["type"],
            quantity,
            1
        )
        
        # 更新库存（系统无限物品不更新）
        if not (isinstance(item["quantity"], str) and item["quantity"] == "无限"):
            item["quantity"] -= quantity
            if item["quantity"] <= 0:
                # 从系统中移除
                del index_data["items"][xianshi_id]
                del type_items[xianshi_id]
            else:
                type_items[xianshi_id] = item
            
            # 保存更改
            save_xianshi_index(index_data)
            save_xianshi_type_data(item_info["type"], type_items)
        
        msg = f"成功购买 {item['name']} x{quantity}（来自{seller_name}）！花费 {number_to(total_price)} 灵石"
        await handle_send(bot, event, msg)
    except Exception as e:
        logger.error(f"仙肆购买出错: {e}")
        msg = "购买过程中出现错误，请稍后再试！"
        await handle_send(bot, event, msg)
    
    await xian_buy.finish()

@xian_shop_added_by_admin.handle(parameterless=[Cooldown(1.4, at_sender=False)])
async def xian_shop_added_by_admin_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """系统仙肆上架"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    args = args.extract_plain_text().split()
    
    if len(args) < 1:
        msg = "请输入正确指令！格式：系统仙肆上架 物品名称 [价格] [数量]"
        await handle_send(bot, event, msg)
        await xian_shop_added_by_admin.finish()
    
    goods_name = args[0]
    try:
        price = int(args[1]) if len(args) > 1 else MIN_PRICE
        price = max(price, MIN_PRICE)
        quantity = int(args[2]) if len(args) > 2 else 0  # 0表示无限
    except ValueError:
        msg = "请输入有效的价格和数量！"
        await handle_send(bot, event, msg)
        await xian_shop_added_by_admin.finish()
    
    # 查找物品ID
    goods_id = None
    for k, v in items.items.items():
        if goods_name == v['name']:
            goods_id = k
            break
    
    if not goods_id:
        msg = f"未找到物品 {goods_name}！"
        await handle_send(bot, event, msg)
        await xian_shop_added_by_admin.finish()
    
    # 获取物品类型
    goods_type = get_item_type_by_id(goods_id)
    if goods_type not in XIANSHI_TYPES:
        msg = f"该物品类型不允许上架！允许类型：{', '.join(XIANSHI_TYPES)}"
        await handle_send(bot, event, msg)
        await xian_shop_added_by_admin.finish()
    
    # 添加到仙肆系统
    # 获取索引数据
    index_data = get_xianshi_index()
    xianshi_id = str(index_data["next_id"])  # 使用顺序ID
    
    # 更新下一个ID
    index_data["next_id"] += 1
    
    # 添加到索引
    index_data["items"][xianshi_id] = {
        "type": goods_type,
        "user_id": 0  # 0表示系统物品
    }
    save_xianshi_index(index_data)
    
    # 添加到类型文件
    type_items = get_xianshi_type_data(goods_type)
    type_items[xianshi_id] = {
        "id": xianshi_id,
        "goods_id": goods_id,
        "name": goods_name,
        "type": goods_type,
        "price": price,
        "quantity": "无限" if quantity == 0 else quantity,  # 0表示无限
        "user_id": 0,
        "user_name": "系统",
        "desc": get_item_msg(goods_id)
    }
    save_xianshi_type_data(goods_type, type_items)
    
    msg = f"系统成功上架 {goods_name} 到仙肆！\n"
    msg += f"价格: {number_to(price)} 灵石\n"
    msg += f"数量: {'无限' if quantity == 0 else quantity}\n"
    msg += f"仙肆ID: {xianshi_id}"
    
    await handle_send(bot, event, msg)
    await xian_shop_added_by_admin.finish()

@xianshi_auto_add.handle(parameterless=[Cooldown(1.4, at_sender=False)])
async def xianshi_auto_add_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """仙肆自动上架（按类型和品阶批量上架）"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg)
        await xianshi_auto_add.finish()
    
    user_id = user_info['user_id']
    args = args.extract_plain_text().split()
    
    # 指令格式检查
    if len(args) < 2:
        msg = "指令格式：仙肆自动上架 [类型] [品阶] [数量]\n" \
              "▶ 类型：装备|法器|防具|药材|技能|全部\n" \
              "▶ 品阶：全部|人阶|黄阶|...|上品通天法器（输入'品阶帮助'查看完整列表）\n" \
              "▶ 数量：可选，默认1个，最多10个"
        await handle_send(bot, event, msg)
        await xianshi_auto_add.finish()
    
    item_type = args[0]  # 物品类型
    rank_name = " ".join(args[1:-1]) if len(args) > 2 else args[1]  # 处理多字品阶名
    quantity = int(args[-1]) if args[-1].isdigit() else 1  # 数量参数
    
    # 数量限制
    quantity = max(1, min(quantity, 10))
    
    # === 类型检查 ===
    type_mapping = {
        "装备": ["法器", "防具"],
        "技能": ["功法", "神通", "辅修功法", "身法", "瞳术"],
        "功法": ["功法"],
        "神通": ["神通"],
        "辅修功法": ["辅修功法"],
        "身法": ["身法"],
        "瞳术": ["瞳术"],
        "法器": ["法器"],
        "防具": ["防具"],
        "药材": ["药材"],
        "全部": ["法器", "防具", "药材", "功法", "神通", "辅修功法", "身法", "瞳术"]
    }
    
    if item_type not in type_mapping:
        msg = f"❌❌❌❌ 无效类型！可用类型：{', '.join(type_mapping.keys())}"
        await handle_send(bot, event, msg)
        await xianshi_auto_add.finish()
    
    # === 品阶检查 ===
    rank_map = {
        # --- 装备品阶---
        "符器": ["下品符器", "上品符器"],
        "法器": ["下品法器", "上品法器"],
        "玄器": ["下品玄器", "上品玄器"],
        "纯阳": ["下品纯阳", "上品纯阳"],
        "纯阳法器": ["下品纯阳法器", "上品纯阳法器"],
        "通天": ["下品通天", "上品通天"],
        "通天法器": ["下品通天法器", "上品通天法器"],
        "仙器": ["下品仙器", "上品仙器"],
        "下品符器": ["下品符器"],
        "上品符器": ["上品符器"],
        "下品法器": ["下品法器"],
        "上品法器": ["上品法器"],
        "下品玄器": ["下品玄器"],
        "上品玄器": ["上品玄器"],
        "下品纯阳": ["下品纯阳"],
        "上品纯阳": ["上品纯阳"],
        "下品纯阳法器": ["下品纯阳法器"],
        "上品纯阳法器": ["上品纯阳法器"],
        "下品通天": ["下品通天"],
        "上品通天": ["上品通天"],
        "下品通天法器": ["下品通天法器"],
        "上品通天法器": ["上品通天法器"],
        
        # --- 药材品阶---
        "一品药材": ["一品药材"],
        "二品药材": ["二品药材"],
        "三品药材": ["三品药材"],
        "四品药材": ["四品药材"],
        "五品药材": ["五品药材"],
        "六品药材": ["六品药材"],
        "七品药材": ["七品药材"],
        "八品药材": ["八品药材"],
        "九品药材": ["九品药材"],
        
        # --- 功法品阶---
        "人阶下品": "人阶下品", "人阶上品": "人阶上品",
        "黄阶下品": "黄阶下品", "黄阶上品": "黄阶上品",
        "玄阶下品": "玄阶下品", "玄阶上品": "玄阶上品",
        "地阶下品": "地阶下品", "地阶上品": "地阶上品",
        "天阶下品": "天阶下品", "天阶上品": "天阶上品",
        "仙阶下品": "仙阶下品", "仙阶上品": "仙阶上品",
        "人阶": ["人阶下品", "人阶上品"],
        "黄阶": ["黄阶下品", "黄阶上品"],
        "玄阶": ["玄阶下品", "玄阶上品"],
        "地阶": ["地阶下品", "地阶上品"],
        "天阶": ["天阶下品", "天阶上品"],
        "仙阶": ["仙阶下品", "仙阶上品"],
        
        # --- 全部品阶（不包含仙器、九品药材和仙阶功法）---
        "全部": [
            # 装备
            "下品符器", "上品符器", "下品法器", "上品法器", "下品玄器", "上品玄器",
            "下品纯阳", "上品纯阳", "下品纯阳法器", "上品纯阳法器", "下品通天", "上品通天", "下品通天法器", "上品通天法器",
            # 药材
            "一品药材", "二品药材", "三品药材", "四品药材",
            "五品药材", "六品药材", "七品药材", "八品药材",
            # 功法
            "人阶下品", "人阶上品", "黄阶下品", "黄阶上品",
            "玄阶下品", "玄阶上品", "地阶下品", "地阶上品",
            "天阶下品", "天阶上品"
        ]
    }
    
    if rank_name not in rank_map:
        msg = f"❌❌❌❌ 无效品阶！输入'品阶帮助'查看完整列表"
        await handle_send(bot, event, msg)
        await xianshi_auto_add.finish()
    
    # === 获取背包物品 ===
    back_msg = sql_message.get_back_msg(user_id)
    if not back_msg:
        msg = "💼💼💼💼 道友的背包空空如也！"
        await handle_send(bot, event, msg)
        await xianshi_auto_add.finish()
    
    # === 筛选物品 ===
    target_types = type_mapping[item_type]
    target_ranks = rank_map[rank_name]
    
    items_to_add = []
    for item in back_msg:
        item_info = items.get_data_by_item_id(item['goods_id'])
        if not item_info:
            continue
            
        # 类型匹配
        type_match = (
            item['goods_type'] in target_types or 
            item_info.get('item_type', '') in target_types
        )
        
        # 品阶匹配
        rank_match = item_info.get('level', '') in target_ranks
        
        if type_match and rank_match:
            available_num = item['goods_num'] - item['bind_num']
            if available_num > 0:
                items_to_add.append({
                    'id': item['goods_id'],
                    'name': item['goods_name'],
                    'type': item['goods_type'],
                    'available_num': available_num,
                    'info': item_info
                })
    
    if not items_to_add:
        msg = f"🔍🔍🔍🔍 背包中没有符合条件的【{item_type}·{rank_name}】物品"
        await handle_send(bot, event, msg)
        await xianshi_auto_add.finish()
    
    # === 自动上架逻辑 ===
    success_count = 0
    total_fee = 0
    result_msg = []
    
    for item in items_to_add:
        # 获取仙肆最低价
        min_price = get_xianshi_min_price(item['name'])
        
        # 如果没有最低价，则使用炼金价格+100万
        if min_price is None:
            base_rank = convert_rank('江湖好手')[0]
            item_rank = get_item_msg_rank(item['id'])
            price = max(MIN_PRICE, (base_rank - 16) * 100000 - item_rank * 100000 + 1000000)
        else:
            price = min_price
        
        # 确定实际上架数量
        actual_quantity = min(quantity, item['available_num'])
        
        # 计算总手续费
        total_price = price * actual_quantity
        if total_price <= 5000000:
            fee_rate = 0.1
        elif total_price <= 10000000:
            fee_rate = 0.15
        elif total_price <= 20000000:
            fee_rate = 0.2
        else:
            fee_rate = 0.3
        
        total_fee += int(total_price * fee_rate)
        
        # 检查灵石是否足够支付手续费
        if user_info['stone'] < total_fee:
            result_msg.append(f"{item['name']} - 灵石不足支付手续费！需要{total_fee}灵石")
            continue
        
        # 为每个物品创建独立条目
        for _ in range(actual_quantity):
            # 扣除手续费和物品
            sql_message.update_back_j(user_id, item['id'], num=1)
            
            # 添加到仙肆系统
            index_data = get_xianshi_index()
            existing_ids = set(index_data["items"].keys())
            xianshi_id = generate_unique_id(existing_ids)
            
            # 添加到索引
            index_data["items"][xianshi_id] = {
                "type": item['type'],
                "user_id": user_id
            }
            save_xianshi_index(index_data)
            
            # 添加到类型文件
            type_items = get_xianshi_type_data(item['type'])
            type_items[xianshi_id] = {
                "id": xianshi_id,
                "goods_id": item['id'],
                "name": item['name'],
                "type": item['type'],
                "price": price,
                "quantity": 1,  # 每个条目数量固定为1
                "user_id": user_id,
                "user_name": user_info['user_name'],
                "desc": get_item_msg(item['id'])
            }
            save_xianshi_type_data(item['type'], type_items)
            
            success_count += 1
        
        result_msg.append(f"{item['name']} x{actual_quantity} - 单价:{number_to(price)}")
    
    if success_count == 0:
        msg = "没有物品被成功上架！"
        await handle_send(bot, event, msg)
        await xianshi_auto_add.finish()
    
    # 扣除总手续费
    sql_message.update_ls(user_id, total_fee, 2)
    
    # 构建结果消息
    msg = [
        f"✨ 成功上架 {success_count} 件物品",
        *result_msg,
        f"💎 总手续费: {number_to(total_fee)}灵石"
    ]
    
    await send_msg_handler(bot, event, '仙肆自动上架', bot.self_id, msg)
    await xianshi_auto_add.finish()

@xianshi_fast_add.handle(parameterless=[Cooldown(1.4, at_sender=False)])
async def xianshi_fast_add_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """仙肆快速上架（按物品名快速上架）"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg)
        await xianshi_fast_add.finish()
    
    user_id = user_info['user_id']
    args = args.extract_plain_text().split()
    
    if len(args) < 1:
        msg = "指令格式：仙肆快速上架 物品名 [价格]\n" \
              "▶ 价格：可选，不填则自动匹配仙肆最低价\n" \
              "▶ 数量：固定为10个（或背包中全部数量）"
        await handle_send(bot, event, msg)
        await xianshi_fast_add.finish()
    
    goods_name = args[0]
    # 尝试解析价格参数
    try:
        price = int(args[1]) if len(args) > 1 else None
    except ValueError:
        msg = "请输入有效的价格！"
        await handle_send(bot, event, msg)
        await xianshi_fast_add.finish()
    
    # 检查背包是否有该物品
    back_msg = sql_message.get_back_msg(user_id)
    goods_info = None
    for item in back_msg:
        if item['goods_name'] == goods_name:
            goods_info = item
            break
    
    if not goods_info:
        msg = f"请检查该道具 {goods_name} 是否在背包内！"
        await handle_send(bot, event, msg)
        await xianshi_fast_add.finish()
    
    # 检查可上架数量（固定为10或背包中全部数量）
    available_num = goods_info['goods_num'] - goods_info['bind_num']
    quantity = min(10, available_num)  # 最多10个
    
    if quantity <= 0:
        msg = f"可上架数量不足！背包有{goods_info['goods_num']}个（{goods_info['bind_num']}个绑定），没有可上架数量"
        await handle_send(bot, event, msg)
        await xianshi_fast_add.finish()
    
    # 获取物品类型
    goods_type = get_item_type_by_id(goods_info['goods_id'])
    if goods_type not in XIANSHI_TYPES:
        msg = f"该物品类型不允许上架！允许类型：{', '.join(XIANSHI_TYPES)}"
        await handle_send(bot, event, msg)
        await xianshi_fast_add.finish()
    
    # 获取价格（如果用户未指定价格）
    if price is None:
        # 获取仙肆最低价
        min_price = get_xianshi_min_price(goods_name)
        
        # 如果没有最低价，则使用炼金价格+100万
        if min_price is None:
            base_rank = convert_rank('江湖好手')[0]
            item_rank = get_item_msg_rank(goods_info['goods_id'])
            price = max(MIN_PRICE, (base_rank - 16) * 100000 - item_rank * 100000 + 1000000)
        else:
            price = min_price
    else:
        # 检查用户指定的价格是否低于最低价
        price = max(price, MIN_PRICE)  # 确保不低于系统最低价
    
    # 计算总手续费
    total_price = price * quantity
    if total_price <= 5000000:
        fee_rate = 0.1
    elif total_price <= 10000000:
        fee_rate = 0.15
    elif total_price <= 20000000:
        fee_rate = 0.2
    else:
        fee_rate = 0.3
    
    fee = int(total_price * fee_rate)
    
    if user_info['stone'] < fee:
        msg = f"灵石不足支付手续费！需要{fee}灵石，当前拥有{user_info['stone']}灵石"
        await handle_send(bot, event, msg)
        await xianshi_fast_add.finish()
    
    # 扣除总手续费
    sql_message.update_ls(user_id, fee, 2)
    
    # 为每个物品创建独立条目
    for _ in range(quantity):
        # 从背包扣除1个物品
        sql_message.update_back_j(user_id, goods_info['goods_id'], num=1)
        
        # 添加到仙肆系统
        index_data = get_xianshi_index()
        existing_ids = set(index_data["items"].keys())
        xianshi_id = generate_unique_id(existing_ids)
        
        # 添加到索引
        index_data["items"][xianshi_id] = {
            "type": goods_type,
            "user_id": user_id
        }
        save_xianshi_index(index_data)
        
        # 添加到类型文件
        type_items = get_xianshi_type_data(goods_type)
        type_items[xianshi_id] = {
            "id": xianshi_id,
            "goods_id": goods_info['goods_id'],
            "name": goods_name,
            "type": goods_type,
            "price": price,
            "quantity": 1,  # 每个条目数量固定为1
            "user_id": user_id,
            "user_name": user_info['user_name'],
            "desc": get_item_msg(goods_info['goods_id'])
        }
        save_xianshi_type_data(goods_type, type_items)
    
    msg = f"成功上架 {goods_name} x{quantity} 到仙肆！\n"
    msg += f"单价: {number_to(price)} 灵石\n"
    msg += f"总价: {number_to(total_price)} 灵石\n"
    msg += f"手续费: {number_to(fee)} 灵石"
    
    await handle_send(bot, event, msg)
    await xianshi_fast_add.finish()

@xianshi_fast_buy.handle(parameterless=[Cooldown(1.4, at_sender=False)])
async def xianshi_fast_buy_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """仙肆快速购买"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg)
        await xianshi_fast_buy.finish()
    
    user_id = user_info['user_id']
    args = args.extract_plain_text().split()
    
    if len(args) < 1:
        msg = "指令格式：仙肆快速购买 物品名1,物品名2 [数量]\n" \
              "▶ 物品名：支持1-5个物品（可重复），用逗号分隔\n" \
              "▶ 示例：仙肆快速购买 两仪心经,两仪心经,两仪心经"
        await handle_send(bot, event, msg)
        await xianshi_fast_buy.finish()
    
    # 解析物品名列表（允许重复且保留顺序）
    goods_names = args[0].split(",")
    if len(goods_names) > 5:
        msg = "一次最多指定5个物品名（可重复）！"
        await handle_send(bot, event, msg)
        await xianshi_fast_buy.finish()
    
    quantity_per_item = 1
    
    # 获取所有用户物品（不包括系统物品）
    index_data = get_xianshi_index()
    user_items = []
    
    for xianshi_id, item_info in index_data["items"].items():
        if item_info["user_id"] != 0:  # 排除系统物品
            type_items = get_xianshi_type_data(item_info["type"])
            if xianshi_id in type_items:
                item_data = type_items[xianshi_id]
                if item_data["name"] in goods_names:
                    user_items.append({
                        "id": xianshi_id,
                        "goods_id": item_data["goods_id"],
                        "name": item_data["name"],
                        "type": item_info["type"],
                        "price": item_data["price"],
                        "seller_id": item_info["user_id"],
                        "seller_name": item_data["user_name"]
                    })
    
    if not user_items:
        msg = "仙肆中没有符合条件的用户物品！"
        await handle_send(bot, event, msg)
        await xianshi_fast_buy.finish()
    
    # 按价格从低到高排序
    user_items.sort(key=lambda x: x["price"])
    
    # 执行购买（严格按照输入顺序处理每个物品名）
    total_cost = 0
    success_items = []
    failed_items = []
    
    for name in goods_names:
        # 查找该物品所有可购买项（按价格排序）
        available = [item for item in user_items if item["name"] == name]
        remaining = quantity_per_item
        purchased = 0
        item_total = 0
        
        for item in available:
            if remaining <= 0:
                break
            
            try:
                # 检查物品是否已被购买（可能被前一轮购买）
                if item["id"] not in index_data["items"]:
                    continue
                
                # 执行购买
                sql_message.update_ls(user_id, item["price"], 2)  # 扣钱
                sql_message.update_ls(item["seller_id"], item["price"], 1)  # 给卖家
                sql_message.send_back(user_id, item["goods_id"], item["name"], item["type"], 1, 1)
                
                # 从系统中移除
                type_items = get_xianshi_type_data(item["type"])
                del index_data["items"][item["id"]]
                del type_items[item["id"]]
                save_xianshi_index(index_data)
                save_xianshi_type_data(item["type"], type_items)
                
                purchased += 1
                item_total += item["price"]
                total_cost += item["price"]
                remaining -= 1
                
            except Exception as e:
                logger.error(f"快速购买出错: {e}")
                continue
        
        # 记录结果（每个name单独记录）
        if purchased > 0:
            success_items.append(f"{name}×{purchased} ({number_to(item_total)}灵石)")
        if remaining > 0:
            failed_items.append(f"{name}×{remaining}（库存不足）")
    
    # 构建结果消息
    msg_parts = []
    if success_items:
        msg_parts.append("成功购买：")
        msg_parts.extend(success_items)
        msg_parts.append(f"总计花费：{number_to(total_cost)}灵石")
    if failed_items:
        msg_parts.append("购买失败：")
        msg_parts.extend(failed_items)
    
    msg = "\n".join(msg_parts)
    await handle_send(bot, event, msg)
    await xianshi_fast_buy.finish()
    
@xian_shop_remove_by_admin.handle(parameterless=[Cooldown(1.4, at_sender=False)])
async def xian_shop_remove_by_admin_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """系统仙肆下架"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    arg = args.extract_plain_text().strip()
    
    if not arg:
        msg = "请输入要下架的仙肆ID或物品名称！"
        await handle_send(bot, event, msg)
        await xian_shop_remove_by_admin.finish()
    
    index_data = get_xianshi_index()
    removed_items = []
    
    if arg.isdigit():  # 按ID下架
        xianshi_id = arg
        if xianshi_id in index_data["items"]:
            item_info = index_data["items"][xianshi_id]
            type_items = get_xianshi_type_data(item_info["type"])
            if xianshi_id in type_items:
                item_data = type_items[xianshi_id]
                removed_items.append(item_data)
                
                # 如果是用户物品，退回
                if item_info["user_id"] != 0:
                    sql_message.send_back(
                        item_info["user_id"],
                        item_data["goods_id"],
                        item_data["name"],
                        item_info["type"],
                        item_data["quantity"]
                    )
                
                # 从系统中移除
                del index_data["items"][xianshi_id]
                del type_items[xianshi_id]
                save_xianshi_index(index_data)
                save_xianshi_type_data(item_info["type"], type_items)
        else:
            msg = f"未找到仙肆ID为 {xianshi_id} 的物品！"
            await handle_send(bot, event, msg)
            await xian_shop_remove_by_admin.finish()
    else:  # 按名称下架
        goods_name = arg
        # 解析数量
        parts = goods_name.split()
        quantity = None
        if len(parts) > 1 and parts[-1].isdigit():
            quantity = int(parts[-1])
            goods_name = " ".join(parts[:-1])
        
        # 查找所有匹配的用户物品（不包括系统物品）
        user_items = []
        for xianshi_id, item_info in index_data["items"].items():
            if item_info["user_id"] != 0:  # 排除系统物品
                type_items = get_xianshi_type_data(item_info["type"])
                if xianshi_id in type_items and type_items[xianshi_id]["name"] == goods_name:
                    user_items.append({
                        "id": xianshi_id,
                        "price": type_items[xianshi_id]["price"],
                        "type": item_info["type"],
                        "user_id": item_info["user_id"],
                        "item_data": type_items[xianshi_id]
                    })
        
        if not user_items:
            msg = f"仙肆中没有用户上架的 {goods_name} 物品！"
            await handle_send(bot, event, msg)
            await xian_shop_remove_by_admin.finish()
        
        # 按价格从低到高排序
        user_items.sort(key=lambda x: x["price"])
        
        # 确定要下架的数量
        if quantity is None:
            # 没指定数量则下架最低价的1个
            items_to_remove = [user_items[0]]
        else:
            # 指定数量则下架价格从低到高的指定数量
            items_to_remove = user_items[:quantity]
        
        # 执行下架操作
        for item in items_to_remove:
            # 从类型文件中移除
            type_items = get_xianshi_type_data(item["type"])
            if item["id"] in type_items:
                item_data = item["item_data"]
                removed_items.append(item_data)
                
                # 退回物品给用户
                sql_message.send_back(
                    item["user_id"],
                    item_data["goods_id"],
                    item_data["name"],
                    item["type"],
                    item_data["quantity"]
                )
                
                # 从系统中移除
                del index_data["items"][item["id"]]
                del type_items[item["id"]]
                save_xianshi_index(index_data)
                save_xianshi_type_data(item["type"], type_items)
    
    if removed_items:
        msg = "成功下架以下物品：\n"
        for item in removed_items:
            owner = "系统" if item["user_id"] == 0 else item["user_name"]
            msg += f"ID:{item['id']} {item['name']} x{item['quantity']} (已退回给:{owner})\n"
    else:
        msg = "没有物品被下架！"
    
    await handle_send(bot, event, msg)
    await xian_shop_remove_by_admin.finish()

@xian_shop_off_all.handle(parameterless=[Cooldown(60, isolate_level=CooldownIsolateLevel.GLOBAL, parallel=1)])
async def xian_shop_off_all_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """清空仙肆"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg)
        await xian_shop_off_all.finish()
    
    msg = "正在清空全服仙肆，请稍候..."
    await handle_send(bot, event, msg)
    
    # 获取所有物品
    index_data = get_xianshi_index()
    removed_items = []
    
    for xianshi_id, item_info in index_data["items"].items():
        type_items = get_xianshi_type_data(item_info["type"])
        if xianshi_id in type_items:
            item = type_items[xianshi_id]
            removed_items.append(item)
            
            # 如果是用户物品，退回
            if item_info["user_id"] != 0:
                sql_message.send_back(
                    item_info["user_id"],
                    item["goods_id"],
                    item["name"],
                    item_info["type"],
                    item["quantity"]
                )
    
    # 清空所有数据
    for item_type in XIANSHI_TYPES:
        save_xianshi_type_data(item_type, {})
    
    save_xianshi_index({"next_id": 1, "items": {}})
    
    if removed_items:
        msg = "成功清空仙肆！共下架以下物品：\n"
        for item in removed_items[:10]:  # 最多显示10条
            owner = "系统" if item["user_id"] == 0 else item["user_name"]
            msg += f"ID:{item['id']} {item['name']} x{item['quantity']} (来自:{owner})\n"
        if len(removed_items) > 10:
            msg += f"...等共{len(removed_items)}件物品"
    else:
        msg = "仙肆已经是空的，没有物品被下架！"
    
    await handle_send(bot, event, msg)
    await xian_shop_off_all.finish()

@shop_added.handle(parameterless=[Cooldown(1.4, at_sender=False)])
async def shop_added_(bot: Bot, event: GroupMessageEvent, args: Message = CommandArg()):
    """坊市上架"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg)
        await shop_added.finish()
    
    group_id = str(event.group_id)
    user_id = user_info['user_id']
    args = args.extract_plain_text().split()
    
    if len(args) < 2:
        msg = "请输入正确指令！格式：坊市上架 物品名称 价格 [数量]"
        await handle_send(bot, event, msg)
        await shop_added.finish()
    
    goods_name = args[0]
    try:
        price = int(args[1])
        quantity = int(args[2]) if len(args) > 2 else 1
        quantity = min(quantity, 10)  # 限制最大数量为10
    except ValueError:
        msg = "请输入有效的价格和数量！"
        await handle_send(bot, event, msg)
        await shop_added.finish()
    
    # 原有价格限制逻辑
    if price < FANGSHI_MIN_PRICE:  # 最低60万灵石
        msg = "坊市最低价格为60万灵石！"
        await handle_send(bot, event, msg)
        await shop_added.finish()
    
    # 检查仙肆最低价
    xianshi_min_price = get_xianshi_min_price(goods_name)
    if xianshi_min_price is not None:
        min_price = max(FANGSHI_MIN_PRICE, xianshi_min_price // 2)
        max_price = xianshi_min_price * 2
        if price < min_price or price > max_price:
            msg = f"该物品在仙肆的最低价格为{xianshi_min_price}，坊市价格限制为{min_price}-{max_price}灵石！"
            await handle_send(bot, event, msg)
            await shop_added.finish()
    
    # 检查背包物品
    back_msg = sql_message.get_back_msg(user_id)
    goods_info = None
    for item in back_msg:
        if item['goods_name'] == goods_name:
            goods_info = item
            break
    
    if not goods_info:
        msg = f"请检查该道具 {goods_name} 是否在背包内！"
        await handle_send(bot, event, msg)
        await shop_added.finish()
    
    # 检查可上架数量
    available_num = goods_info['goods_num'] - goods_info['bind_num']
    if quantity > available_num:
        msg = f"可上架数量不足！背包有{goods_info['goods_num']}个（{goods_info['bind_num']}个绑定），最多可上架{available_num}个"
        await handle_send(bot, event, msg)
        await shop_added.finish()
    
    # 获取物品类型
    goods_type = get_item_type_by_id(goods_info['goods_id'])
    if goods_type not in ["药材", "装备", "丹药", "技能"]:
        msg = "只能上架药材、装备、丹药或技能类物品！"
        await handle_send(bot, event, msg)
        await shop_added.finish()
    
    # 计算总手续费
    total_price = price * quantity
    if total_price <= 6000000:
        fee_rate = 0.1
    elif total_price <= 10000000:
        fee_rate = 0.15
    elif total_price <= 20000000:
        fee_rate = 0.2
    else:
        fee_rate = 0.3
    
    fee = int(total_price * fee_rate)
    if user_info['stone'] < fee:
        msg = f"灵石不足支付手续费！需要{fee}灵石，当前拥有{user_info['stone']}灵石"
        await handle_send(bot, event, msg)
        await shop_added.finish()
    
    # 扣除手续费和物品
    sql_message.update_ls(user_id, fee, 2)
    sql_message.update_back_j(user_id, goods_info['goods_id'], num=quantity)
    
    # 为每个物品创建独立条目
    success_count = 0
    for _ in range(quantity):
        # 添加到坊市系统
        # 生成唯一坊市ID
        index_data = get_fangshi_index(group_id)
        existing_ids = set(index_data["items"].keys())
        fangshi_id = generate_fangshi_id(existing_ids)
        
        # 添加到索引
        index_data["items"][fangshi_id] = {
            "type": goods_type,
            "user_id": user_id
        }
        save_fangshi_index(group_id, index_data)
        
        # 添加到类型文件
        type_items = get_fangshi_type_data(group_id, goods_type)
        type_items[fangshi_id] = {
            "id": fangshi_id,
            "goods_id": goods_info['goods_id'],
            "name": goods_name,
            "type": goods_type,
            "price": price,
            "quantity": 1,  # 每个条目数量固定为1
            "user_id": user_id,
            "user_name": user_info['user_name'],
            "desc": get_item_msg(goods_info['goods_id'])
        }
        save_fangshi_type_data(group_id, goods_type, type_items)
        success_count += 1
    
    msg = f"成功上架 {goods_name} x{success_count} 到坊市！\n"
    msg += f"单价: {number_to(price)} 灵石\n"
    msg += f"总价: {number_to(total_price)} 灵石\n"
    msg += f"手续费: {number_to(fee)} 灵石"
    
    await handle_send(bot, event, msg)
    await shop_added.finish()

@fangshi_auto_add.handle(parameterless=[Cooldown(1.4, at_sender=False)])
async def fangshi_auto_add_(bot: Bot, event: GroupMessageEvent, args: Message = CommandArg()):
    """坊市自动上架（按类型和品阶批量上架）"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg)
        await fangshi_auto_add.finish()
    
    group_id = str(event.group_id)
    user_id = user_info['user_id']
    args = args.extract_plain_text().split()
    
    # 指令格式检查
    if len(args) < 2:
        msg = "指令格式：坊市自动上架 [类型] [品阶] [数量]\n" \
              "▶ 类型：装备|法器|防具|药材|技能|全部\n" \
              "▶ 品阶：全部|人阶|黄阶|...|上品通天法器（输入'品阶帮助'查看完整列表）\n" \
              "▶ 数量：可选，默认1个，最多10个"
        await handle_send(bot, event, msg)
        await fangshi_auto_add.finish()
    
    item_type = args[0]  # 物品类型
    rank_name = " ".join(args[1:-1]) if len(args) > 2 else args[1]  # 处理多字品阶名
    quantity = int(args[-1]) if args[-1].isdigit() else 1  # 数量参数
    
    # 数量限制
    quantity = max(1, min(quantity, 10))
    
    # === 类型检查 ===
    type_mapping = {
        "装备": ["法器", "防具"],
        "技能": ["功法", "神通", "辅修功法", "身法", "瞳术"],
        "功法": ["功法"],
        "神通": ["神通"],
        "辅修功法": ["辅修功法"],
        "身法": ["身法"],
        "瞳术": ["瞳术"],
        "法器": ["法器"],
        "防具": ["防具"],
        "药材": ["药材"],
        "全部": ["法器", "防具", "药材", "功法", "神通", "辅修功法", "身法", "瞳术"]
    }
    
    if item_type not in type_mapping:
        msg = f"❌❌❌❌ 无效类型！可用类型：{', '.join(type_mapping.keys())}"
        await handle_send(bot, event, msg)
        await fangshi_auto_add.finish()
    
    # === 品阶检查 ===
    rank_map = {
        # --- 装备品阶---
        "符器": ["下品符器", "上品符器"],
        "法器": ["下品法器", "上品法器"],
        "玄器": ["下品玄器", "上品玄器"],
        "纯阳": ["下品纯阳", "上品纯阳"],
        "纯阳法器": ["下品纯阳法器", "上品纯阳法器"],
        "通天": ["下品通天", "上品通天"],
        "通天法器": ["下品通天法器", "上品通天法器"],
        "仙器": ["下品仙器", "上品仙器"],
        "下品符器": ["下品符器"],
        "上品符器": ["上品符器"],
        "下品法器": ["下品法器"],
        "上品法器": ["上品法器"],
        "下品玄器": ["下品玄器"],
        "上品玄器": ["上品玄器"],
        "下品纯阳": ["下品纯阳"],
        "上品纯阳": ["上品纯阳"],
        "下品纯阳法器": ["下品纯阳法器"],
        "上品纯阳法器": ["上品纯阳法器"],
        "下品通天": ["下品通天"],
        "上品通天": ["上品通天"],
        "下品通天法器": ["下品通天法器"],
        "上品通天法器": ["上品通天法器"],
        
        # --- 药材品阶---
        "一品药材": ["一品药材"],
        "二品药材": ["二品药材"],
        "三品药材": ["三品药材"],
        "四品药材": ["四品药材"],
        "五品药材": ["五品药材"],
        "六品药材": ["六品药材"],
        "七品药材": ["七品药材"],
        "八品药材": ["八品药材"],
        "九品药材": ["九品药材"],
        
        # --- 功法品阶---
        "人阶下品": "人阶下品", "人阶上品": "人阶上品",
        "黄阶下品": "黄阶下品", "黄阶上品": "黄阶上品",
        "玄阶下品": "玄阶下品", "玄阶上品": "玄阶上品",
        "地阶下品": "地阶下品", "地阶上品": "地阶上品",
        "天阶下品": "天阶下品", "天阶上品": "天阶上品",
        "仙阶下品": "仙阶下品", "仙阶上品": "仙阶上品",
        "人阶": ["人阶下品", "人阶上品"],
        "黄阶": ["黄阶下品", "黄阶上品"],
        "玄阶": ["玄阶下品", "玄阶上品"],
        "地阶": ["地阶下品", "地阶上品"],
        "天阶": ["天阶下品", "天阶上品"],
        "仙阶": ["仙阶下品", "仙阶上品"],
        
        # --- 全部品阶（不包含仙器、九品药材和仙阶功法）---
        "全部": [
            # 装备
            "下品符器", "上品符器", "下品法器", "上品法器", "下品玄器", "上品玄器",
            "下品纯阳", "上品纯阳", "下品纯阳法器", "上品纯阳法器", "下品通天", "上品通天", "下品通天法器", "上品通天法器",
            # 药材
            "一品药材", "二品药材", "三品药材", "四品药材",
            "五品药材", "六品药材", "七品药材", "八品药材",
            # 功法
            "人阶下品", "人阶上品", "黄阶下品", "黄阶上品",
            "玄阶下品", "玄阶上品", "地阶下品", "地阶上品",
            "天阶下品", "天阶上品"
        ]
    }
    
    if rank_name not in rank_map:
        msg = f"❌❌❌❌ 无效品阶！输入'品阶帮助'查看完整列表"
        await handle_send(bot, event, msg)
        await fangshi_auto_add.finish()
    
    # === 获取背包物品 ===
    back_msg = sql_message.get_back_msg(user_id)
    if not back_msg:
        msg = "💼💼💼💼 道友的背包空空如也！"
        await handle_send(bot, event, msg)
        await fangshi_auto_add.finish()
    
    # === 筛选物品 ===
    target_types = type_mapping[item_type]
    target_ranks = rank_map[rank_name]
    
    items_to_add = []
    for item in back_msg:
        item_info = items.get_data_by_item_id(item['goods_id'])
        if not item_info:
            continue
            
        # 类型匹配
        type_match = (
            item['goods_type'] in target_types or 
            item_info.get('item_type', '') in target_types
        )
        
        # 品阶匹配
        rank_match = item_info.get('level', '') in target_ranks
        
        if type_match and rank_match:
            available_num = item['goods_num'] - item['bind_num']
            if available_num > 0:
                items_to_add.append({
                    'id': item['goods_id'],
                    'name': item['goods_name'],
                    'type': item['goods_type'],
                    'available_num': available_num,
                    'info': item_info
                })
    
    if not items_to_add:
        msg = f"🔍🔍🔍🔍 背包中没有符合条件的【{item_type}·{rank_name}】物品"
        await handle_send(bot, event, msg)
        await fangshi_auto_add.finish()
    
    # === 自动上架逻辑 ===
    success_count = 0
    total_fee = 0
    result_msg = []
    
    for item in items_to_add:
        # 获取坊市最低价
        min_price = get_fangshi_min_price(group_id, item['name'])
        
        # 如果没有最低价，则使用炼金价格+100万
        if min_price is None:
            base_rank = convert_rank('江湖好手')[0]
            item_rank = get_item_msg_rank(item['id'])
            price = max(FANGSHI_MIN_PRICE, (base_rank - 16) * 100000 - item_rank * 100000 + 1000000)
        else:
            price = min_price
        
        # 确定实际上架数量
        actual_quantity = min(quantity, item['available_num'])
        
        # 计算总手续费
        total_price = price * actual_quantity
        if total_price <= 5000000:
            fee_rate = 0.1
        elif total_price <= 10000000:
            fee_rate = 0.15
        elif total_price <= 20000000:
            fee_rate = 0.2
        else:
            fee_rate = 0.3
        
        total_fee += int(total_price * fee_rate)
        
        # 检查灵石是否足够支付手续费
        if user_info['stone'] < total_fee:
            result_msg.append(f"{item['name']} - 灵石不足支付手续费！需要{total_fee}灵石")
            continue
        
        # 为每个物品创建独立条目
        for _ in range(actual_quantity):
            # 扣除手续费和物品
            sql_message.update_back_j(user_id, item['id'], num=1)
            
            # 添加到坊市系统
            index_data = get_fangshi_index(group_id)
            existing_ids = set(index_data["items"].keys())
            fangshi_id = generate_fangshi_id(existing_ids)
            
            # 添加到索引
            index_data["items"][fangshi_id] = {
                "type": item['type'],
                "user_id": user_id
            }
            save_fangshi_index(group_id, index_data)
            
            # 添加到类型文件
            type_items = get_fangshi_type_data(group_id, item['type'])
            type_items[fangshi_id] = {
                "id": fangshi_id,
                "goods_id": item['id'],
                "name": item['name'],
                "type": item['type'],
                "price": price,
                "quantity": 1,  # 每个条目数量固定为1
                "user_id": user_id,
                "user_name": user_info['user_name'],
                "desc": get_item_msg(item['id'])
            }
            save_fangshi_type_data(group_id, item['type'], type_items)
            
            success_count += 1
        
        result_msg.append(f"{item['name']} x{actual_quantity} - 单价:{number_to(price)}")
    
    if success_count == 0:
        msg = "没有物品被成功上架！"
        await handle_send(bot, event, msg)
        await fangshi_auto_add.finish()
    
    # 扣除总手续费
    sql_message.update_ls(user_id, total_fee, 2)
    
    # 构建结果消息
    msg = [
        f"✨ 成功上架 {success_count} 件物品",
        *result_msg,
        f"💎 总手续费: {number_to(total_fee)}灵石"
    ]
    
    await send_msg_handler(bot, event, '坊市自动上架', bot.self_id, msg)
    await fangshi_auto_add.finish()

@fangshi_fast_add.handle(parameterless=[Cooldown(1.4, at_sender=False)])
async def fangshi_fast_add_(bot: Bot, event: GroupMessageEvent, args: Message = CommandArg()):
    """坊市快速上架（按物品名快速上架）"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg)
        await fangshi_fast_add.finish()
    
    group_id = str(event.group_id)
    user_id = user_info['user_id']
    args = args.extract_plain_text().split()
    
    if len(args) < 1:
        msg = "指令格式：坊市快速上架 物品名 [价格]\n" \
              "▶ 价格：可选，不填则自动匹配坊市最低价\n" \
              "▶ 数量：固定为10个（或背包中全部数量）"
        await handle_send(bot, event, msg)
        await fangshi_fast_add.finish()
    
    goods_name = args[0]
    # 尝试解析价格参数
    try:
        price = int(args[1]) if len(args) > 1 else None
    except ValueError:
        msg = "请输入有效的价格！"
        await handle_send(bot, event, msg)
        await fangshi_fast_add.finish()
    
    # 检查背包是否有该物品
    back_msg = sql_message.get_back_msg(user_id)
    goods_info = None
    for item in back_msg:
        if item['goods_name'] == goods_name:
            goods_info = item
            break
    
    if not goods_info:
        msg = f"请检查该道具 {goods_name} 是否在背包内！"
        await handle_send(bot, event, msg)
        await fangshi_fast_add.finish()
    
    # 检查可上架数量（固定为10或背包中全部数量）
    available_num = goods_info['goods_num'] - goods_info['bind_num']
    quantity = min(10, available_num)  # 最多10个
    
    if quantity <= 0:
        msg = f"可上架数量不足！背包有{goods_info['goods_num']}个（{goods_info['bind_num']}个绑定），没有可上架数量"
        await handle_send(bot, event, msg)
        await fangshi_fast_add.finish()
    
    # 获取物品类型
    goods_type = get_item_type_by_id(goods_info['goods_id'])
    if goods_type not in FANGSHI_TYPES:
        msg = f"该物品类型不允许上架！允许类型：{', '.join(FANGSHI_TYPES)}"
        await handle_send(bot, event, msg)
        await fangshi_fast_add.finish()
    
    # 获取价格（如果用户未指定价格）
    if price is None:
        # 获取坊市最低价
        min_price = get_fangshi_min_price(group_id, goods_name)
        
        # 如果没有最低价，则使用炼金价格+100万
        if min_price is None:
            base_rank = convert_rank('江湖好手')[0]
            item_rank = get_item_msg_rank(goods_info['goods_id'])
            price = max(FANGSHI_MIN_PRICE, (base_rank - 16) * 100000 - item_rank * 100000 + 1000000)
        else:
            price = min_price
    else:
        # 检查用户指定的价格是否符合限制
        xianshi_min = get_xianshi_min_price(goods_name)
        if xianshi_min is not None:
            min_price = max(FANGSHI_MIN_PRICE, xianshi_min // 2)
            max_price = xianshi_min * 2
            if price < min_price or price > max_price:
                msg = f"该物品在仙肆的最低价格为{xianshi_min}，坊市价格限制为{min_price}-{max_price}灵石！"
                await handle_send(bot, event, msg)
                await fangshi_fast_add.finish()
        else:
            if price < FANGSHI_MIN_PRICE:
                price = max(price, FANGSHI_MIN_PRICE)
    
    # 计算总手续费
    total_price = price * quantity
    if total_price <= 5000000:
        fee_rate = 0.1
    elif total_price <= 10000000:
        fee_rate = 0.15
    elif total_price <= 20000000:
        fee_rate = 0.2
    else:
        fee_rate = 0.3
    
    fee = int(total_price * fee_rate)
    
    if user_info['stone'] < fee:
        msg = f"灵石不足支付手续费！需要{fee}灵石，当前拥有{user_info['stone']}灵石"
        await handle_send(bot, event, msg)
        await fangshi_fast_add.finish()
    
    # 扣除总手续费
    sql_message.update_ls(user_id, fee, 2)
    
    # 为每个物品创建独立条目
    for _ in range(quantity):
        # 从背包扣除1个物品
        sql_message.update_back_j(user_id, goods_info['goods_id'], num=1)
        
        # 添加到坊市系统
        index_data = get_fangshi_index(group_id)
        existing_ids = set(index_data["items"].keys())
        fangshi_id = generate_fangshi_id(existing_ids)
        
        # 添加到索引
        index_data["items"][fangshi_id] = {
            "type": goods_type,
            "user_id": user_id
        }
        save_fangshi_index(group_id, index_data)
        
        # 添加到类型文件
        type_items = get_fangshi_type_data(group_id, goods_type)
        type_items[fangshi_id] = {
            "id": fangshi_id,
            "goods_id": goods_info['goods_id'],
            "name": goods_name,
            "type": goods_type,
            "price": price,
            "quantity": 1,  # 每个条目数量固定为1
            "user_id": user_id,
            "user_name": user_info['user_name'],
            "desc": get_item_msg(goods_info['goods_id'])
        }
        save_fangshi_type_data(group_id, goods_type, type_items)
    
    msg = f"成功上架 {goods_name} x{quantity} 到坊市！\n"
    msg += f"单价: {number_to(price)} 灵石\n"
    msg += f"总价: {number_to(total_price)} 灵石\n"
    msg += f"手续费: {number_to(fee)} 灵石"
    
    await handle_send(bot, event, msg)
    await fangshi_fast_add.finish()

@fangshi_fast_buy.handle(parameterless=[Cooldown(1.4, at_sender=False)])
async def fangshi_fast_buy_(bot: Bot, event: GroupMessageEvent, args: Message = CommandArg()):
    """坊市快速购买"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg)
        await fangshi_fast_buy.finish()
    
    group_id = str(event.group_id)
    user_id = user_info['user_id']
    args = args.extract_plain_text().split()
    
    if len(args) < 1:
        msg = "指令格式：坊市快速购买 物品名1,物品名2 [数量]\n" \
              "▶ 物品名：支持1-5个物品（可重复），用逗号分隔\n" \
              "▶ 示例：坊市快速购买 两仪心经,两仪心经,两仪心经"
        await handle_send(bot, event, msg)
        await fangshi_fast_buy.finish()
    
    # 解析物品名列表（允许重复且保留顺序）
    goods_names = args[0].split(",")
    if len(goods_names) > 5:
        msg = "一次最多指定5个物品名（可重复）！"
        await handle_send(bot, event, msg)
        await fangshi_fast_buy.finish()
    
    quantity_per_item = 1
    
    # 获取所有用户物品（不包括系统物品）
    index_data = get_fangshi_index(group_id)
    user_items = []
    
    for fangshi_id, item_info in index_data["items"].items():
        if item_info["user_id"] != 0:  # 排除系统物品
            type_items = get_fangshi_type_data(group_id, item_info["type"])
            if fangshi_id in type_items:
                item_data = type_items[fangshi_id]
                if item_data["name"] in goods_names:
                    user_items.append({
                        "id": fangshi_id,
                        "goods_id": item_data["goods_id"],
                        "name": item_data["name"],
                        "type": item_info["type"],
                        "price": item_data["price"],
                        "seller_id": item_info["user_id"],
                        "seller_name": item_data["user_name"]
                    })
    
    if not user_items:
        msg = "坊市中没有符合条件的用户物品！"
        await handle_send(bot, event, msg)
        await fangshi_fast_buy.finish()
    
    # 按价格从低到高排序
    user_items.sort(key=lambda x: x["price"])
    
    # 执行购买（严格按照输入顺序处理每个物品名）
    total_cost = 0
    success_items = []
    failed_items = []
    
    for name in goods_names:
        # 查找该物品所有可购买项（按价格排序）
        available = [item for item in user_items if item["name"] == name]
        remaining = quantity_per_item
        purchased = 0
        item_total = 0
        
        for item in available:
            if remaining <= 0:
                break
            
            try:
                # 检查物品是否已被购买（可能被前一轮购买）
                if item["id"] not in index_data["items"]:
                    continue
                
                # 检查灵石是否足够
                if user_info['stone'] < item["price"]:
                    failed_items.append(f"{item['name']}×1（灵石不足）")
                    continue
                
                # 执行购买
                sql_message.update_ls(user_id, item["price"], 2)  # 扣钱
                sql_message.update_ls(item["seller_id"], item["price"], 1)  # 给卖家
                sql_message.send_back(user_id, item["goods_id"], item["name"], item["type"], 1, 1)
                
                # 从系统中移除
                type_items = get_fangshi_type_data(group_id, item["type"])
                del index_data["items"][item["id"]]
                del type_items[item["id"]]
                save_fangshi_index(group_id, index_data)
                save_fangshi_type_data(group_id, item["type"], type_items)
                
                purchased += 1
                item_total += item["price"]
                total_cost += item["price"]
                remaining -= 1
                
            except Exception as e:
                logger.error(f"坊市快速购买出错: {e}")
                continue
        
        # 记录结果（每个name单独记录）
        if purchased > 0:
            success_items.append(f"{name}×{purchased} ({number_to(item_total)}灵石)")
        if remaining > 0:
            failed_items.append(f"{name}×{remaining}（库存不足）")
    
    # 构建结果消息
    msg_parts = []
    if success_items:
        msg_parts.append("成功购买：")
        msg_parts.extend(success_items)
        msg_parts.append(f"总计花费：{number_to(total_cost)}灵石")
    if failed_items:
        msg_parts.append("购买失败：")
        msg_parts.extend(failed_items)
    
    msg = "\n".join(msg_parts)
    await handle_send(bot, event, msg)
    await fangshi_fast_buy.finish()
    
@shop_remove.handle(parameterless=[Cooldown(1.4, at_sender=False)])
async def shop_remove_(bot: Bot, event: GroupMessageEvent, args: Message = CommandArg()):
    """坊市下架"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg)
        await shop_remove.finish()
    
    group_id = str(event.group_id)
    user_id = user_info['user_id']
    args = args.extract_plain_text().split()
    
    if not args:
        msg = "请输入要下架的物品名称！"
        await handle_send(bot, event, msg)
        await shop_remove.finish()
    
    goods_name = args[0]
    quantity = int(args[1]) if len(args) > 1 and args[1].isdigit() else None
    
    # 获取所有类型中该用户上架的该物品
    user_items = []
    index_data = get_fangshi_index(group_id)
    
    for fangshi_id, item_info in index_data["items"].items():
        if str(item_info["user_id"]) == str(user_id):
            type_items = get_fangshi_type_data(group_id, item_info["type"])
            if fangshi_id in type_items and type_items[fangshi_id]["name"] == goods_name:
                user_items.append({
                    "id": fangshi_id,
                    "price": type_items[fangshi_id]["price"],
                    "type": item_info["type"]
                })
    
    if not user_items:
        msg = f"您在坊市中没有上架 {goods_name}！"
        await handle_send(bot, event, msg)
        await shop_remove.finish()
    
    # 按价格从低到高排序
    user_items.sort(key=lambda x: x["price"])
    
    # 确定要下架的数量
    if quantity is None:
        # 没指定数量则下架最低价的1个
        items_to_remove = [user_items[0]]
    else:
        # 指定数量则下架价格从低到高的指定数量
        items_to_remove = user_items[:quantity]
    
    # 执行下架操作
    removed_count = 0
    for item in items_to_remove:
        # 从类型文件中移除
        type_items = get_fangshi_type_data(group_id, item["type"])
        if item["id"] in type_items:
            # 退回物品到背包
            item_data = type_items[item["id"]]
            sql_message.send_back(
                user_id,
                item_data["goods_id"],
                goods_name,
                item["type"],
                1  # 每个条目数量固定为1
            )
            
            # 从系统中移除
            del type_items[item["id"]]
            save_fangshi_type_data(group_id, item["type"], type_items)
            
            # 从索引中移除
            del index_data["items"][item["id"]]
            save_fangshi_index(group_id, index_data)
            
            removed_count += 1
    
    msg = f"成功下架 {goods_name} x{removed_count}！已退回背包"
    if len(user_items) > removed_count:
        msg += f"\n(坊市中仍有 {len(user_items)-removed_count} 个 {goods_name})"
    
    await handle_send(bot, event, msg)
    await shop_remove.finish()

@buy.handle(parameterless=[Cooldown(1.4, at_sender=False)])
async def buy_(bot: Bot, event: GroupMessageEvent, args: Message = CommandArg()):
    """坊市购买"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg)
        await buy.finish()
    
    group_id = str(event.group_id)
    user_id = user_info['user_id']
    args = args.extract_plain_text().split()
    
    if len(args) < 1:
        msg = "请输入要购买的坊市ID！"
        await handle_send(bot, event, msg)
        await buy.finish()
    
    fangshi_id = args[0]
    quantity = int(args[1]) if len(args) > 1 else 1
    
    # 从索引查找物品
    index_data = get_fangshi_index(group_id)
    if fangshi_id not in index_data["items"]:
        msg = f"未找到坊市ID为 {fangshi_id} 的物品！"
        await handle_send(bot, event, msg)
        await buy.finish()
    
    item_info = index_data["items"][fangshi_id]
    
    # 检查是否是自己的物品
    if str(item_info["user_id"]) == str(user_id):
        msg = "不能购买自己上架的物品！"
        await handle_send(bot, event, msg)
        await buy.finish()
    
    # 从类型文件获取详细信息
    type_items = get_fangshi_type_data(group_id, item_info["type"])
    if fangshi_id not in type_items:
        msg = "物品数据异常，请联系管理员！"
        await handle_send(bot, event, msg)
        await buy.finish()
    
    item = type_items[fangshi_id]
    seller_name = "系统" if item["user_id"] == 0 else item["user_name"]
    # 检查库存（无限物品跳过检查）
    if not (isinstance(item["quantity"], str) and item["quantity"] == "无限"):
        if item["quantity"] < quantity:
            msg = f"库存不足！只有 {item['quantity']} 个可用"
            await handle_send(bot, event, msg)
            await buy.finish()
    
    # 计算总价
    total_price = item["price"] * quantity
    
    # 检查灵石是否足够
    if user_info["stone"] < total_price:
        msg = f"灵石不足！需要 {number_to(total_price)} 灵石，当前拥有 {number_to(user_info['stone'])} 灵石"
        await handle_send(bot, event, msg)
        await buy.finish()
    
    try:
        # 扣除买家灵石
        sql_message.update_ls(user_id, total_price, 2)
        
        # 给卖家灵石（如果不是系统物品）
        if item_info["user_id"] != 0:
            seller_id = item_info["user_id"]
            sql_message.update_ls(seller_id, total_price, 1)
        
        # 给买家物品
        sql_message.send_back(
            user_id,
            item["goods_id"],
            item["name"],
            item["type"],
            quantity,
            1
        )
        
        # 更新库存（无限物品不更新）
        if not (isinstance(item["quantity"], str) and item["quantity"] == "无限"):
            item["quantity"] -= quantity
            if item["quantity"] <= 0:
                # 从系统中移除
                del index_data["items"][fangshi_id]
                del type_items[fangshi_id]
            else:
                type_items[fangshi_id] = item
            
            # 保存更改
            save_fangshi_index(group_id, index_data)
            save_fangshi_type_data(group_id, item_info["type"], type_items)
        
        msg = f"成功购买 {item['name']} x{quantity}（来自{seller_name}）！花费 {number_to(total_price)} 灵石"
        await handle_send(bot, event, msg)
    except Exception as e:
        logger.error(f"坊市购买出错: {e}")
        msg = "购买过程中出现错误，请稍后再试！"
        await handle_send(bot, event, msg)
    
    await buy.finish()

@shop_view.handle(parameterless=[Cooldown(at_sender=False)])
async def shop_view_(bot: Bot, event: GroupMessageEvent, args: Message = CommandArg()):
    """坊市查看"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg)
        await shop_view.finish()
    
    group_id = str(event.group_id)
    
    # 解析参数
    args_str = args.extract_plain_text().strip()
    
    # 情况1：无参数 - 显示可用类型
    if not args_str:
        msg = "请指定查看类型：【药材、装备、丹药、技能】"
        await handle_send(bot, event, msg)
        await shop_view.finish()
    
    # 解析类型和页码
    item_type = None
    current_page = 1
    
    # 检查是否直接拼接类型和页码（无空格）
    for t in FANGSHI_TYPES:
        if args_str.startswith(t):
            item_type = t
            remaining = args_str[len(t):].strip()
            if remaining.isdigit():
                current_page = int(remaining)
            break
    
    # 情况2：有空格分隔
    if item_type is None:
        parts = args_str.split(maxsplit=1)
        if parts[0] in FANGSHI_TYPES:
            item_type = parts[0]
            if len(parts) > 1 and parts[1].isdigit():
                current_page = int(parts[1])
    
    # 检查类型有效性
    if item_type not in FANGSHI_TYPES:
        msg = f"无效类型！可用类型：【{', '.join(FANGSHI_TYPES)}】"
        await handle_send(bot, event, msg)
        await shop_view.finish()
    
    # 读取对应类型的物品数据
    type_items = get_fangshi_type_data(group_id, item_type)
    if not type_items:
        msg = f"坊市中暂无{item_type}类物品！"
        await handle_send(bot, event, msg)
        await shop_view.finish()
    
    # 处理物品显示逻辑
    system_items = []  # 存储系统物品
    user_items = {}    # 存储用户物品（按名称分组，只保留最低价）
    
    for item_id, item in type_items.items():
        if item['user_id'] == 0:  # 系统物品
            system_items.append(item)
        else:  # 用户物品
            item_name = item['name']
            # 如果还没有记录或者当前价格更低，更新记录
            if item_name not in user_items or item['price'] < user_items[item_name]['price']:
                user_items[item_name] = item
    
    # 合并系统物品和用户物品，并按价格排序
    items_list = sorted(system_items + list(user_items.values()), key=lambda x: x['name'])
    
    # 分页处理
    per_page = 10
    total_pages = (len(items_list) + per_page - 1) // per_page
    current_page = max(1, min(current_page, total_pages))
    
    if current_page > total_pages:
        msg = f"页码超出范围，最多{total_pages}页！"
        await handle_send(bot, event, msg)
        await shop_view.finish()
    
    # 构建消息
    start_idx = (current_page - 1) * per_page
    end_idx = start_idx + per_page
    paged_items = items_list[start_idx:end_idx]

    # 构建消息
    msg_list = [f"☆------坊市 {item_type}------☆"]
    for item in paged_items:
        price_str = number_to(item['price'])
        msg = f"\n{item['name']} {price_str}灵石 \nID:{item['id']}"
        
        # 处理数量显示
        if isinstance(item['quantity'], str) and item['quantity'] == "无限":
            msg += f" 不限量"
        elif item['quantity'] > 1:
            msg += f" 限售:{item['quantity']}"
        
        msg_list.append(msg)
    
    msg_list.append(f"\n第 {current_page}/{total_pages} 页")
    if total_pages > 1:
        next_page_cmd = f"坊市查看{item_type}{current_page + 1}"
        msg_list.append(f"输入 {next_page_cmd} 查看下一页")
    
    await send_msg_handler(bot, event, '坊市查看', bot.self_id, msg_list)
    await shop_view.finish()

@my_shop.handle(parameterless=[Cooldown(at_sender=False)])
async def my_shop_(bot: Bot, event: GroupMessageEvent, args: Message = CommandArg()):
    """我的坊市"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg)
        await my_shop.finish()
    
    group_id = str(event.group_id)
    user_id = user_info['user_id']
    
    # 获取页码
    try:
        current_page = int(args.extract_plain_text().strip())
    except:
        current_page = 1
    
    # 从索引获取用户所有物品
    index_data = get_fangshi_index(group_id)
    user_items = []
    
    for fangshi_id, item_info in index_data["items"].items():
        if str(item_info["user_id"]) == str(user_id):
            # 从对应类型文件读取详细信息
            type_items = get_fangshi_type_data(group_id, item_info["type"])
            if fangshi_id in type_items:
                user_items.append(type_items[fangshi_id])
    
    # 按价格排序
    user_items.sort(key=lambda x: x['name'])
    
    # 检查是否有上架物品
    if not user_items:
        msg = "您在坊市中没有上架任何物品！"
        await handle_send(bot, event, msg)
        await my_shop.finish()
    
    # 分页处理
    per_page = 20
    total_pages = (len(user_items) + per_page - 1) // per_page
    current_page = max(1, min(current_page, total_pages))
    
    # 构建消息
    start_idx = (current_page - 1) * per_page
    end_idx = start_idx + per_page
    paged_items = user_items[start_idx:end_idx]
    
    msg_list = [f"☆------{user_info['user_name']}的坊市物品------☆"]
    for item in paged_items:
        price_str = number_to(item['price'])
        msg = f"{item['name']} {price_str}灵石"
        if isinstance(item['quantity'], int) and item['quantity'] > 1:
            msg += f" x{item['quantity']}"
        msg_list.append(msg)
    
    msg_list.append(f"\n第 {current_page}/{total_pages} 页")
    if total_pages > 1:
        msg_list.append(f"输入 我的坊市 {current_page + 1} 查看下一页")
    
    await send_msg_handler(bot, event, '我的坊市', bot.self_id, msg_list)
    await my_shop.finish()

@shop_added_by_admin.handle(parameterless=[Cooldown(1.4, at_sender=False)])
async def shop_added_by_admin_(bot: Bot, event: GroupMessageEvent, args: Message = CommandArg()):
    """系统坊市上架"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    args = args.extract_plain_text().split()
    
    if len(args) < 1:
        msg = "请输入正确指令！格式：系统坊市上架 物品名称 [价格] [数量]"
        await handle_send(bot, event, msg)
        await shop_added_by_admin.finish()
    
    group_id = str(event.group_id)
    goods_name = args[0]
    try:
        price = int(args[1]) if len(args) > 1 else FANGSHI_MIN_PRICE
        price = max(price, FANGSHI_MIN_PRICE)
        quantity = int(args[2]) if len(args) > 2 else 0  # 0表示无限
    except ValueError:
        msg = "请输入有效的价格和数量！"
        await handle_send(bot, event, msg)
        await shop_added_by_admin.finish()
    
    # 查找物品ID
    goods_id = None
    for k, v in items.items.items():
        if goods_name == v['name']:
            goods_id = k
            break
    
    if not goods_id:
        msg = f"未找到物品 {goods_name}！"
        await handle_send(bot, event, msg)
        await shop_added_by_admin.finish()
    
    # 获取物品类型
    goods_type = get_item_type_by_id(goods_id)
    if goods_type not in FANGSHI_TYPES:
        msg = f"该物品类型不允许上架！允许类型：{', '.join(FANGSHI_TYPES)}"
        await handle_send(bot, event, msg)
        await shop_added_by_admin.finish()
    
    # 添加到坊市系统
    # 获取索引数据
    index_data = get_fangshi_index(group_id)
    fangshi_id = str(index_data["next_id"])  # 使用顺序ID
    
    # 更新下一个ID
    index_data["next_id"] += 1
    
    # 添加到索引
    index_data["items"][fangshi_id] = {
        "type": goods_type,
        "user_id": 0  # 0表示系统物品
    }
    save_fangshi_index(group_id, index_data)
    
    # 添加到类型文件
    type_items = get_fangshi_type_data(group_id, goods_type)
    type_items[fangshi_id] = {
        "id": fangshi_id,
        "goods_id": goods_id,
        "name": goods_name,
        "type": goods_type,
        "price": price,
        "quantity": "无限" if quantity == 0 else quantity,  # 0表示无限
        "user_id": 0,
        "user_name": "系统",
        "desc": get_item_msg(goods_id)
    }
    save_fangshi_type_data(group_id, goods_type, type_items)
    
    msg = f"系统成功上架 {goods_name} 到坊市！\n"
    msg += f"价格: {number_to(price)} 灵石\n"
    msg += f"数量: {'无限' if quantity == 0 else quantity}\n"
    msg += f"坊市ID: {fangshi_id}"
    
    await handle_send(bot, event, msg)
    await shop_added_by_admin.finish()

@shop_remove_by_admin.handle(parameterless=[Cooldown(1.4, at_sender=False)])
async def shop_remove_by_admin_(bot: Bot, event: GroupMessageEvent, args: Message = CommandArg()):
    """系统坊市下架"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    group_id = str(event.group_id)
    arg = args.extract_plain_text().strip()
    
    if not arg:
        msg = "请输入要下架的坊市ID或物品名称！"
        await handle_send(bot, event, msg)
        await shop_remove_by_admin.finish()
    
    index_data = get_fangshi_index(group_id)
    removed_items = []
    
    if arg.isdigit():  # 按ID下架
        fangshi_id = arg
        if fangshi_id in index_data["items"]:
            item_info = index_data["items"][fangshi_id]
            type_items = get_fangshi_type_data(group_id, item_info["type"])
            if fangshi_id in type_items:
                item_data = type_items[fangshi_id]
                removed_items.append(item_data)
                
                # 如果是用户物品，退回给用户
                if item_info["user_id"] != 0:
                    sql_message.send_back(
                        item_info["user_id"],
                        item_data["goods_id"],
                        item_data["name"],
                        item_info["type"],
                        item_data["quantity"]
                    )
                
                # 从系统中移除
                del index_data["items"][fangshi_id]
                del type_items[fangshi_id]
                save_fangshi_index(group_id, index_data)
                save_fangshi_type_data(group_id, item_info["type"], type_items)
        else:
            msg = f"未找到坊市ID为 {fangshi_id} 的物品！"
            await handle_send(bot, event, msg)
            await shop_remove_by_admin.finish()
    else:  # 按名称下架
        goods_name = arg
        # 解析数量
        parts = goods_name.split()
        quantity = None
        if len(parts) > 1 and parts[-1].isdigit():
            quantity = int(parts[-1])
            goods_name = " ".join(parts[:-1])
        
        # 查找所有匹配的用户物品（不包括系统物品）
        user_items = []
        for fangshi_id, item_info in index_data["items"].items():
            if item_info["user_id"] != 0:  # 排除系统物品
                type_items = get_fangshi_type_data(group_id, item_info["type"])
                if fangshi_id in type_items and type_items[fangshi_id]["name"] == goods_name:
                    user_items.append({
                        "id": fangshi_id,
                        "price": type_items[fangshi_id]["price"],
                        "type": item_info["type"],
                        "user_id": item_info["user_id"],
                        "item_data": type_items[fangshi_id]
                    })
        
        if not user_items:
            msg = f"坊市中没有用户上架的 {goods_name} 物品！"
            await handle_send(bot, event, msg)
            await shop_remove_by_admin.finish()
        
        # 按价格从低到高排序
        user_items.sort(key=lambda x: x["price"])
        
        # 确定要下架的数量
        if quantity is None:
            # 没指定数量则下架最低价的1个
            items_to_remove = [user_items[0]]
        else:
            # 指定数量则下架价格从低到高的指定数量
            items_to_remove = user_items[:quantity]
        
        # 执行下架操作
        for item in items_to_remove:
            # 从类型文件中移除
            type_items = get_fangshi_type_data(group_id, item["type"])
            if item["id"] in type_items:
                removed_items.append(item["item_data"])
                
                # 退回物品给用户
                sql_message.send_back(
                    item["user_id"],
                    item["item_data"]["goods_id"],
                    item["item_data"]["name"],
                    item["type"],
                    item["item_data"]["quantity"]
                )
                
                # 从系统中移除
                del index_data["items"][item["id"]]
                del type_items[item["id"]]
                save_fangshi_index(group_id, index_data)
                save_fangshi_type_data(group_id, item["type"], type_items)
    
    if removed_items:
        msg = "成功下架以下物品：\n"
        for item in removed_items:
            owner = "系统" if item["user_id"] == 0 else sql_message.get_user_info_with_id(item["user_id"])["user_name"]
            msg += f"ID:{item['id']} {item['name']} x{item['quantity']} (已退回给:{owner})\n"
    else:
        msg = "没有物品被下架！"
    
    await handle_send(bot, event, msg)
    await shop_remove_by_admin.finish()

@shop_off_all.handle(parameterless=[Cooldown(60, isolate_level=CooldownIsolateLevel.GROUP, parallel=1)])
async def shop_off_all_(bot: Bot, event: GroupMessageEvent):
    """清空坊市"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg)
        await shop_off_all.finish()
    
    group_id = str(event.group_id)
    
    msg = "正在清空坊市，请稍候..."
    await handle_send(bot, event, msg)
    
    # 获取所有物品
    index_data = get_fangshi_index(group_id)
    removed_items = []
    
    for fangshi_id, item_info in index_data["items"].items():
        type_items = get_fangshi_type_data(group_id, item_info["type"])
        if fangshi_id in type_items:
            item = type_items[fangshi_id]
            removed_items.append(item)
            
            # 如果是用户物品，退回
            if item_info["user_id"] != 0:
                sql_message.send_back(
                    item_info["user_id"],
                    item["goods_id"],
                    item["name"],
                    item_info["type"],
                    item["quantity"]
                )
    
    # 清空所有数据
    for item_type in FANGSHI_TYPES:
        save_fangshi_type_data(group_id, item_type, {})
    
    save_fangshi_index(group_id, {"next_id": 1, "items": {}})
    
    if removed_items:
        msg = "成功清空坊市！共下架以下物品：\n"
        for item in removed_items[:10]:  # 最多显示10条
            owner = "系统" if item["user_id"] == 0 else item["user_name"]
            msg += f"{item['name']} x{item['quantity']} (来自:{owner})\n"
        if len(removed_items) > 10:
            msg += f"...等共{len(removed_items)}件物品"
    else:
        msg = "坊市已经是空的，没有物品被下架！"
    
    await handle_send(bot, event, msg)
    await shop_off_all.finish()

def get_guishi_user_data(user_id):
    """获取用户鬼市数据"""
    user_file = GUISHI_DATA_PATH / f"user_{user_id}.json"
    try:
        if user_file.exists():
            with open(user_file, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception as e:
        logger.error(f"读取鬼市用户数据失败: {e}")
    return {
        "stone": 0,  # 鬼市账户灵石
        "qiugou_orders": {},  # 求购订单 {order_id: {item_name, price, quantity, filled}}
        "baitan_orders": {},  # 摆摊订单 {order_id: {item_id, item_name, price, quantity, sold}}
        "items": {}  # 暂存物品 {item_id: {name, type, quantity}}
    }

def save_guishi_user_data(user_id, data):
    """保存用户鬼市数据"""
    user_file = GUISHI_DATA_PATH / f"user_{user_id}.json"
    try:
        with open(user_file, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        return True
    except Exception as e:
        logger.error(f"保存鬼市用户数据失败: {e}")
        return False

def generate_guishi_id(existing_ids):
    """生成6-10位随机不重复ID"""
    while True:
        # 使用时间戳+随机数确保唯一性
        timestamp_part = int(time.time() % 10000)
        random_part = random.randint(100, 99999)
        new_id = int(f"{timestamp_part}{random_part}") % 10**10  # 确保不超过10位
        
        # 限制在6-10位
        new_id = max(100000, min(new_id, 9999999999))
        
        # 检查是否已存在
        if str(new_id) not in existing_ids:
            return str(new_id)

async def process_guishi_transactions(user_id):
    """处理鬼市交易"""
    user_data = get_guishi_user_data(user_id)
    transactions = []
    
    # 处理求购订单
    for order_id, order in list(user_data["qiugou_orders"].items()):
        # 查找匹配的摆摊订单（价格<=求购价）
        matched_orders = []
        for other_user_file in GUISHI_DATA_PATH.glob("user_*.json"):
            if other_user_file.name == f"user_{user_id}.json":
                continue  # 跳过自己的订单
                
            other_data = json.loads(other_user_file.read_text(encoding="utf-8"))
            for other_order_id, other_order in other_data["baitan_orders"].items():
                if (other_order["item_name"] == order["item_name"] and 
                    other_order["price"] <= order["price"] and
                    other_order["quantity"] - other_order.get("sold", 0) > 0):
                    matched_orders.append((other_user_file, other_data, other_order_id, other_order))
        
        # 按价格从低到高排序
        matched_orders.sort(key=lambda x: x[3]["price"])
        
        for other_user_file, other_data, other_order_id, other_order in matched_orders:
            if order.get("filled", 0) >= order["quantity"]:
                break  # 订单已完成
                
            available = other_order["quantity"] - other_order.get("sold", 0)
            needed = order["quantity"] - order.get("filled", 0)
            trade_num = min(available, needed)
            
            # 检查鬼市账户余额是否足够
            total_cost = trade_num * other_order["price"]
            if user_data["stone"] < total_cost:
                continue  # 余额不足，跳过
                
            # 执行交易
            user_data["stone"] -= total_cost
            other_data["stone"] += total_cost
            
            # 更新订单状态
            order["filled"] = order.get("filled", 0) + trade_num
            other_order["sold"] = other_order.get("sold", 0) + trade_num
            
            # 转移物品
            item_id = other_order["item_id"]
            if item_id not in user_data["items"]:
                user_data["items"][item_id] = {
                    "name": other_order["item_name"],
                    "type": items.get_data_by_item_id(item_id)["type"],
                    "quantity": 0
                }
            user_data["items"][item_id]["quantity"] += trade_num
            
            # 记录交易（修改这里）
            transactions.append(f"求购：已收购 {other_order['item_name']} x{trade_num} (花费{number_to(total_cost)}灵石)")
            
            # 保存对方数据
            save_guishi_user_data(other_user_file.stem.split("_")[1], other_data)
            
        # 检查订单是否完成
        if order.get("filled", 0) >= order["quantity"]:
            user_data["qiugou_orders"].pop(order_id)
            transactions.append(f"求购订单 {order_id} 已完成")
    
    # 处理摆摊订单
    for order_id, order in list(user_data["baitan_orders"].items()):
        # 查找匹配的求购订单（价格>=摆摊价）
        matched_orders = []
        for other_user_file in GUISHI_DATA_PATH.glob("user_*.json"):
            if other_user_file.name == f"user_{user_id}.json":
                continue  # 跳过自己的订单
                
            other_data = json.loads(other_user_file.read_text(encoding="utf-8"))
            for other_order_id, other_order in other_data["qiugou_orders"].items():
                if (other_order["item_name"] == order["item_name"] and 
                    other_order["price"] >= order["price"] and
                    other_order["quantity"] - other_order.get("filled", 0) > 0):
                    matched_orders.append((other_user_file, other_data, other_order_id, other_order))
        
        # 按价格从高到低排序
        matched_orders.sort(key=lambda x: -x[3]["price"])
        
        for other_user_file, other_data, other_order_id, other_order in matched_orders:
            if order.get("sold", 0) >= order["quantity"]:
                break  # 订单已完成
                
            available = order["quantity"] - order.get("sold", 0)
            needed = other_order["quantity"] - other_order.get("filled", 0)
            trade_num = min(available, needed)
            
            # 检查对方鬼市账户余额是否足够
            total_cost = trade_num * order["price"]
            if other_data["stone"] < total_cost:
                continue  # 对方余额不足，跳过
                
            # 执行交易
            other_data["stone"] -= total_cost
            user_data["stone"] += total_cost
            
            # 更新订单状态
            order["sold"] = order.get("sold", 0) + trade_num
            other_order["filled"] = other_order.get("filled", 0) + trade_num
            
            # 转移物品
            item_id = other_order.get("item_id")  # 求购订单可能没有item_id
            if item_id:
                if item_id not in other_data["items"]:
                    other_data["items"][item_id] = {
                        "name": order["item_name"],
                        "type": items.get_data_by_item_id(item_id)["type"],
                        "quantity": 0
                    }
                other_data["items"][item_id]["quantity"] += trade_num
            
            # 记录交易（修改这里）
            transactions.append(f"摆摊：已出售 {order['item_name']} x{trade_num} (获得{number_to(total_cost)}灵石)")
            
            # 保存对方数据
            save_guishi_user_data(other_user_file.stem.split("_")[1], other_data)
            
        # 检查订单是否完成
        if order.get("sold", 0) >= order["quantity"]:
            user_data["baitan_orders"].pop(order_id)
            transactions.append(f"摆摊订单 {order_id} 已完成")
    
    # 保存用户数据
    save_guishi_user_data(user_id, user_data)
    
    return transactions

@guishi_deposit.handle(parameterless=[Cooldown(1.4, at_sender=False)])
async def guishi_deposit_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """鬼市存灵石"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg)
        await guishi_deposit.finish()
    
    user_id = user_info['user_id']
    amount_str = args.extract_plain_text().strip()
    
    if not amount_str.isdigit():
        msg = "请输入正确的灵石数量！"
        await handle_send(bot, event, msg)
        await guishi_deposit.finish()
    
    amount = int(amount_str)
    if amount <= 0:
        msg = "存入数量必须大于0！"
        await handle_send(bot, event, msg)
        await guishi_deposit.finish()
    
    if user_info['stone'] < amount:
        msg = f"灵石不足！当前拥有 {user_info['stone']} 灵石"
        await handle_send(bot, event, msg)
        await guishi_deposit.finish()
    
    # 扣除用户灵石
    sql_message.update_ls(user_id, amount, 2)
    
    # 存入鬼市账户
    user_data = get_guishi_user_data(user_id)
    user_data["stone"] += amount
    save_guishi_user_data(user_id, user_data)
    
    msg = f"成功存入 {number_to(amount)} 灵石到鬼市账户！"
    await handle_send(bot, event, msg)
    await guishi_deposit.finish()

@guishi_withdraw.handle(parameterless=[Cooldown(1.4, at_sender=False)])
async def guishi_withdraw_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """鬼市取灵石（收取20%暂存费）"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg)
        await guishi_withdraw.finish()
    
    user_id = user_info['user_id']
    amount_str = args.extract_plain_text().strip()
    
    if not amount_str.isdigit():
        msg = "请输入正确的灵石数量！"
        await handle_send(bot, event, msg)
        await guishi_withdraw.finish()
    
    amount = int(amount_str)
    if amount <= 0:
        msg = "取出数量必须大于0！"
        await handle_send(bot, event, msg)
        await guishi_withdraw.finish()
    
    user_data = get_guishi_user_data(user_id)
    if user_data["stone"] < amount:
        msg = f"鬼市账户余额不足！当前余额 {user_data['stone']} 灵石"
        await handle_send(bot, event, msg)
        await guishi_withdraw.finish()
    
    # 计算手续费（20%）
    fee = int(amount * 0.2)
    actual_amount = amount - fee
    
    # 更新鬼市账户
    user_data["stone"] -= amount
    save_guishi_user_data(user_id, user_data)
    
    # 给用户灵石
    sql_message.update_ls(user_id, actual_amount, 1)
    
    msg = f"成功取出 {number_to(amount)} 灵石（扣除20%暂存费，实际到账 {number_to(actual_amount)} 灵石）"
    await handle_send(bot, event, msg)
    await guishi_withdraw.finish()

@guishi_info.handle(parameterless=[Cooldown(at_sender=False)])
async def guishi_info_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """鬼市信息"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg)
        await guishi_info.finish()
    
    user_id = user_info['user_id']
    user_data = get_guishi_user_data(user_id)
    
    # 处理交易
    transactions = await process_guishi_transactions(user_id)
    
    # 构建消息
    msg = f"☆------鬼市账户信息------☆\n"
    msg += f"账户余额：{number_to(user_data['stone'])} 灵石\n"
    
    if transactions:
        msg += "\n☆------最近交易------☆\n"
        msg += "\n".join(transactions) + "\n"
    else:
        msg += "\n☆------最近交易------☆\n"
        msg += "无\n"
    
    msg += "\n☆------求购订单------☆\n"
    if user_data["qiugou_orders"]:
        for order_id, order in user_data["qiugou_orders"].items():
            filled = order.get("filled", 0)
            status = f"{filled}/{order['quantity']}" if order["quantity"] > 1 else "进行中"
            if filled >= order["quantity"]:
                status = "已完成"
            msg += f"ID:{order_id} {order['item_name']} 单价:{number_to(order['price'])} 状态:{status}\n"
    else:
        msg += "无\n"
    
    msg += "\n☆------摆摊订单------☆\n"
    if user_data["baitan_orders"]:
        for order_id, order in user_data["baitan_orders"].items():
            sold = order.get("sold", 0)
            status = f"{sold}/{order['quantity']}" if order["quantity"] > 1 else "进行中"
            if sold >= order["quantity"]:
                status = "已完成"
            msg += f"ID:{order_id} {order['item_name']} 单价:{number_to(order['price'])} 状态:{status}\n"
    else:
        msg += "无\n"
    
    msg += "\n☆------暂存物品------☆\n"
    if user_data["items"]:
        for item_id, item in user_data["items"].items():
            msg += f"{item['name']} x{item['quantity']}\n"
    else:
        msg += "无\n"
    
    await handle_send(bot, event, msg)
    await guishi_info.finish()

@guishi_qiugou.handle(parameterless=[Cooldown(1.4, at_sender=False)])
async def guishi_qiugou_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """鬼市求购"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg)
        await guishi_qiugou.finish()
    
    user_id = user_info['user_id']
    args = args.extract_plain_text().split()
    
    if len(args) < 2:
        msg = "指令格式：鬼市求购 物品名称 价格 [数量]\n数量不填默认为1"
        await handle_send(bot, event, msg)
        await guishi_qiugou.finish()
    
    goods_name = args[0]
    try:
        price = int(args[1])
        quantity = int(args[2]) if len(args) > 2 else 1
        quantity = min(quantity, GUISHI_MAX_QUANTITY)
    except ValueError:
        msg = "请输入有效的价格和数量！"
        await handle_send(bot, event, msg)
        await guishi_qiugou.finish()
    
    # 检查禁止交易的物品
    goods_id = None
    for k, v in items.items.items():
        if goods_name == v['name']:
            if str(k) in BANNED_ITEM_IDS:
                msg = f"物品 {goods_name} 禁止在鬼市交易！"
                await handle_send(bot, event, msg)
                await guishi_qiugou.finish()
            goods_id = k
            break
    
    if not goods_id:
        msg = f"物品 {goods_name} 不存在！"
        await handle_send(bot, event, msg)
        await guishi_qiugou.finish()
    
    # 检查订单数量限制
    user_data = get_guishi_user_data(user_id)
    if len(user_data["qiugou_orders"]) >= MAX_QIUGOU_ORDERS:
        msg = f"您的求购订单已达上限({MAX_QIUGOU_ORDERS})，请先取消部分订单！"
        await handle_send(bot, event, msg)
        await guishi_qiugou.finish()
    
    # 检查鬼市账户余额是否足够
    user_data = get_guishi_user_data(user_id)
    total_cost = price * quantity
    if user_data["stone"] < total_cost:
        msg = f"鬼市账户余额不足！需要 {number_to(total_cost)} 灵石，当前余额 {number_to(user_data['stone'])} 灵石"
        await handle_send(bot, event, msg)
        await guishi_qiugou.finish()
    
    # 生成订单ID
    existing_ids = set(user_data["qiugou_orders"].keys())
    order_id = generate_guishi_id(existing_ids)
    
    # 添加求购订单
    user_data["qiugou_orders"][order_id] = {
        "item_name": goods_name,
        "price": price,
        "quantity": quantity,
        "filled": 0
    }
    
    # 冻结相应灵石
    user_data["stone"] -= total_cost
    save_guishi_user_data(user_id, user_data)
    
    # 处理可能的即时交易
    transactions = await process_guishi_transactions(user_id)
    
    msg = f"成功发布求购订单！\n"
    msg += f"物品：{goods_name}\n"
    msg += f"价格：{number_to(price)} 灵石\n"
    msg += f"数量：{quantity}\n"
    msg += f"订单ID：{order_id}\n"
    
    if transactions:
        msg += "\n☆------交易结果------☆\n"
        msg += "\n".join(transactions)
    
    await handle_send(bot, event, msg)
    await guishi_qiugou.finish()

@guishi_cancel_qiugou.handle(parameterless=[Cooldown(1.4, at_sender=False)])
async def guishi_cancel_qiugou_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """取消鬼市求购（支持无参数自动取消已完成、指定ID或全部取消）"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg)
        await guishi_cancel_qiugou.finish()
    
    user_id = user_info['user_id']
    arg = args.extract_plain_text().strip()
    
    user_data = get_guishi_user_data(user_id)
    if not user_data["qiugou_orders"]:
        msg = "您当前没有求购订单！"
        await handle_send(bot, event, msg)
        await guishi_cancel_qiugou.finish()
    
    # 处理不同参数情况
    if not arg:  # 无参数，自动取消已完成的
        completed_orders = []
        refund_total = 0
        for order_id, order in list(user_data["qiugou_orders"].items()):
            if order.get("filled", 0) >= order["quantity"]:
                completed_orders.append(order_id)
                # 已完成订单的灵石已全部扣除，无需退还
        
        if not completed_orders:
            msg = "没有已完成的求购订单可自动取消！"
            await handle_send(bot, event, msg)
            await guishi_cancel_qiugou.finish()
        
        msg = "已自动取消以下已完成订单：\n"
        for order_id in completed_orders:
            order = user_data["qiugou_orders"][order_id]
            msg += f"ID:{order_id} {order['item_name']} x{order['quantity']}\n"
            del user_data["qiugou_orders"][order_id]
        
    elif arg == "全部":  # 取消所有求购订单
        msg = "已取消所有求购订单：\n"
        refund_total = 0
        for order_id, order in list(user_data["qiugou_orders"].items()):
            filled = order.get("filled", 0)
            refund = (order["quantity"] - filled) * order["price"]
            refund_total += refund
            
            msg += f"ID:{order_id} {order['item_name']} 已购:{filled}/{order['quantity']}\n"
            del user_data["qiugou_orders"][order_id]
        
        if refund_total > 0:
            user_data["stone"] += refund_total
            msg += f"\n退还 {number_to(refund_total)} 灵石到鬼市账户"
        
    else:  # 指定ID取消
        order_id = arg
        if order_id not in user_data["qiugou_orders"]:
            msg = f"未找到求购订单 {order_id}！"
            await handle_send(bot, event, msg)
            await guishi_cancel_qiugou.finish()
        
        order = user_data["qiugou_orders"][order_id]
        filled = order.get("filled", 0)
        refund = (order["quantity"] - filled) * order["price"]
        
        # 退还灵石
        user_data["stone"] += refund
        del user_data["qiugou_orders"][order_id]
        
        msg = f"已取消求购订单 {order_id}：\n"
        msg += f"物品：{order['item_name']}\n"
        msg += f"价格：{number_to(order['price'])} 灵石\n"
        msg += f"已购：{filled}/{order['quantity']}\n"
        if refund > 0:
            msg += f"退还 {number_to(refund)} 灵石到鬼市账户"
    
    save_guishi_user_data(user_id, user_data)
    await handle_send(bot, event, msg)
    await guishi_cancel_qiugou.finish()

@guishi_baitan.handle(parameterless=[Cooldown(1.4, at_sender=False)])
async def guishi_baitan_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """鬼市摆摊"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg)
        await guishi_baitan.finish()
    
    user_id = user_info['user_id']
    args = args.extract_plain_text().split()
    
    if len(args) < 2:
        msg = "指令格式：鬼市摆摊 物品名称 价格 [数量]\n数量不填默认为1"
        await handle_send(bot, event, msg)
        await guishi_baitan.finish()
    
    goods_name = args[0]
    try:
        price = int(args[1])
        quantity = int(args[2]) if len(args) > 2 else 1
        quantity = min(quantity, GUISHI_MAX_QUANTITY)
    except ValueError:
        msg = "请输入有效的价格和数量！"
        await handle_send(bot, event, msg)
        await guishi_baitan.finish()
    
    # 检查背包物品
    back_msg = sql_message.get_back_msg(user_id)
    goods_info = None
    for item in back_msg:
        if item['goods_name'] == goods_name:
            if str(item['goods_id']) in BANNED_ITEM_IDS:
                msg = f"物品 {goods_name} 禁止在鬼市交易！"
                await handle_send(bot, event, msg)
                await guishi_baitan.finish()
            goods_info = item
            break
    
    if not goods_info:
        msg = f"请检查该道具 {goods_name} 是否在背包内！"
        await handle_send(bot, event, msg)
        await guishi_baitan.finish()
    
    # 检查订单数量限制
    user_data = get_guishi_user_data(user_id)
    if len(user_data["baitan_orders"]) >= MAX_BAITAN_ORDERS:
        msg = f"您的摆摊订单已达上限({MAX_BAITAN_ORDERS})，请先收摊部分订单！"
        await handle_send(bot, event, msg)
        await guishi_baitan.finish()
    
    # 检查物品总数量
    if goods_info['goods_num'] < quantity:
        msg = f"数量不足！背包仅有 {goods_info['goods_num']} 个 {goods_name}"
        await handle_send(bot, event, msg)
        await guishi_baitan.finish()
    
    # 获取物品类型
    goods_type = get_item_type_by_id(goods_info['goods_id'])
    if goods_type not in GUISHI_TYPES:
        msg = f"该物品类型不允许交易！允许类型：{', '.join(GUISHI_TYPES)}"
        await handle_send(bot, event, msg)
        await guishi_baitan.finish()
    
    # 从背包扣除物品
    sql_message.update_back_j(user_id, goods_info['goods_id'], num=quantity)
    
    # 生成订单ID
    user_data = get_guishi_user_data(user_id)
    existing_ids = set(user_data["baitan_orders"].keys())
    order_id = generate_guishi_id(existing_ids)
    
    # 添加摆摊订单
    user_data["baitan_orders"][order_id] = {
        "item_id": goods_info['goods_id'],
        "item_name": goods_name,
        "price": price,
        "quantity": quantity,
        "sold": 0
    }
    save_guishi_user_data(user_id, user_data)
    
    # 处理可能的即时交易
    transactions = await process_guishi_transactions(user_id)
    
    msg = f"成功摆摊！\n"
    msg += f"物品：{goods_name}\n"
    msg += f"价格：{number_to(price)} 灵石\n"
    msg += f"数量：{quantity}\n"
    msg += f"摊位ID：{order_id}\n"
    
    if transactions:
        msg += "\n☆------交易结果------☆\n"
        msg += "\n".join(transactions)
    
    await handle_send(bot, event, msg)
    await guishi_baitan.finish()

@guishi_shoutan.handle(parameterless=[Cooldown(1.4, at_sender=False)])
async def guishi_shoutan_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """鬼市收摊（支持无参数自动收摊已完成、指定ID或全部收摊）"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg)
        await guishi_shoutan.finish()
    
    user_id = user_info['user_id']
    arg = args.extract_plain_text().strip()
    
    user_data = get_guishi_user_data(user_id)
    if not user_data["baitan_orders"]:
        msg = "您当前没有摆摊订单！"
        await handle_send(bot, event, msg)
        await guishi_shoutan.finish()
    
    # 处理不同参数情况
    if not arg:  # 无参数，自动收摊已完成的
        completed_orders = []
        for order_id, order in list(user_data["baitan_orders"].items()):
            if order.get("sold", 0) >= order["quantity"]:
                completed_orders.append(order_id)
        
        if not completed_orders:
            msg = "没有已完成的摆摊订单可自动收摊！"
            await handle_send(bot, event, msg)
            await guishi_shoutan.finish()
        
        msg = "已自动收摊以下已完成订单：\n"
        for order_id in completed_orders:
            order = user_data["baitan_orders"][order_id]
            msg += f"ID:{order_id} {order['item_name']} x{order['quantity']}\n"
            del user_data["baitan_orders"][order_id]
        
    elif arg == "全部":  # 收摊所有订单
        msg = "已收摊所有摆摊订单：\n"
        for order_id, order in list(user_data["baitan_orders"].items()):
            sold = order.get("sold", 0)
            remaining = order["quantity"] - sold
            
            # 退还未售出的物品
            if remaining > 0:
                sql_message.send_back(
                    user_id,
                    order["item_id"],
                    order["item_name"],
                    items.get_data_by_item_id(order["item_id"])["type"],
                    remaining
                )
            
            msg += f"ID:{order_id} {order['item_name']} 已售:{sold}/{order['quantity']}\n"
            del user_data["baitan_orders"][order_id]
        
    else:  # 指定ID收摊
        order_id = arg
        if order_id not in user_data["baitan_orders"]:
            msg = f"未找到摆摊订单 {order_id}！"
            await handle_send(bot, event, msg)
            await guishi_shoutan.finish()
        
        order = user_data["baitan_orders"][order_id]
        sold = order.get("sold", 0)
        remaining = order["quantity"] - sold
        
        # 退还未售出的物品
        if remaining > 0:
            sql_message.send_back(
                user_id,
                order["item_id"],
                order["item_name"],
                items.get_data_by_item_id(order["item_id"])["type"],
                remaining
            )
        
        msg = f"已收摊订单 {order_id}：\n"
        msg += f"物品：{order['item_name']}\n"
        msg += f"价格：{number_to(order['price'])} 灵石\n"
        msg += f"已售：{sold}/{order['quantity']}\n"
        if remaining > 0:
            msg += f"退还 {remaining} 个到背包"
        
        del user_data["baitan_orders"][order_id]
    
    save_guishi_user_data(user_id, user_data)
    await handle_send(bot, event, msg)
    await guishi_shoutan.finish()

guishi_take_item = on_command("鬼市取物品", priority=5, block=True)

@guishi_take_item.handle(parameterless=[Cooldown(1.4, at_sender=False)])
async def guishi_take_item_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """取出暂存在鬼市的物品"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg)
        await guishi_take_item.finish()
    
    user_id = user_info['user_id']
    arg = args.extract_plain_text().strip()
    
    user_data = get_guishi_user_data(user_id)
    if not user_data["items"]:
        msg = "您的鬼市暂存中没有物品！"
        await handle_send(bot, event, msg)
        await guishi_take_item.finish()
    
    # 处理不同参数情况
    if not arg:  # 无参数，显示暂存物品列表
        msg = "☆------鬼市暂存物品------☆\n"
        msg += "请使用'鬼市取物品 物品名'或'鬼市取物品 全部'取出物品\n\n"
        for item_id, item in user_data["items"].items():
            msg += f"{item['name']} x{item['quantity']}\n"
        await handle_send(bot, event, msg)
        await guishi_take_item.finish()
    
    if arg == "全部":  # 取出所有物品
        msg = "已从鬼市取出以下物品：\n"
        for item_id, item in list(user_data["items"].items()):
            sql_message.send_back(
                user_id,
                item_id,
                item["name"],
                item["type"],
                item["quantity"]
            )
            msg += f"{item['name']} x{item['quantity']}\n"
            del user_data["items"][item_id]
        
        save_guishi_user_data(user_id, user_data)
        await handle_send(bot, event, msg)
        await guishi_take_item.finish()
    
    # 取出指定物品
    matched_items = []
    for item_id, item in user_data["items"].items():
        if arg == item["name"]:
            matched_items.append((item_id, item))
    
    if not matched_items:
        msg = f"暂存中没有名为 {arg} 的物品！"
        await handle_send(bot, event, msg)
        await guishi_take_item.finish()
    
    # 处理多个同名物品情况（理论上不会出现，因为鬼市合并了同名物品）
    for item_id, item in matched_items:
        sql_message.send_back(
            user_id,
            item_id,
            item["name"],
            item["type"],
            item["quantity"]
        )
        del user_data["items"][item_id]
    
    save_guishi_user_data(user_id, user_data)
    msg = f"已从鬼市取出 {arg} x{matched_items[0][1]['quantity']}"
    await handle_send(bot, event, msg)
    await guishi_take_item.finish()
    
@auction_withdraw.handle(parameterless=[Cooldown(1.4, at_sender=False, isolate_level=CooldownIsolateLevel.GROUP)])
async def auction_withdraw_(bot: Bot, event: GroupMessageEvent, args: Message = CommandArg()):
    """用户撤回拍卖品"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    group_id = "000000"
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await auction_withdraw.finish()

    group_id = "000000"
    if group_id not in groups:
        msg = '尚未开启拍卖会功能，请联系管理员开启！'
        await handle_send(bot, event, msg)
        await auction_withdraw.finish()

    config = get_auction_config()
    user_auctions = config.get('user_auctions', [])

    if not user_auctions:
        msg = f"拍卖会目前没有道友提交的物品！"
        await handle_send(bot, event, msg)
        await auction_withdraw.finish()

    arg = args.extract_plain_text().strip()
    auction_index = int(arg) - 1
    if auction_index < 0 or auction_index >= len(user_auctions):
        msg = f"请输入正确的编号"
        await handle_send(bot, event, msg)
        await auction_withdraw.finish()

    auction = user_auctions[auction_index]
    goods_name, details = list(auction.items())[0]
    if details['user_id'] != user_info['user_id']:
        msg = f"这不是你的拍卖品！"
        await handle_send(bot, event, msg)
        await auction_withdraw.finish()

    sql_message.send_back(details['user_id'], details['id'], goods_name, details['goods_type'], details['quantity'])
    user_auctions.pop(auction_index)
    config['user_auctions'] = user_auctions
    savef_auction(config)

    msg = f"成功撤回拍卖品：{goods_name}x{details['quantity']}！"
    await handle_send(bot, event, msg)

    await auction_withdraw.finish()

@goods_re_root.handle(parameterless=[Cooldown(at_sender=False)])
async def goods_re_root_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """炼金"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    group_id = "000000"
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await goods_re_root.finish()
    user_id = user_info['user_id']
    args = args.extract_plain_text().split()
    if args is None:
        msg = "请输入要炼化的物品！"
        await handle_send(bot, event, msg)
        await goods_re_root.finish()
        
    # 判断输入是ID还是名称
    goods_id = None
    if args[0].isdigit():
        goods_id = int(args[0])
        item_info = items.get_data_by_item_id(goods_id)
        if not item_info:
            msg = f"ID {goods_id} 对应的物品不存在，请检查输入！"
            await handle_send(bot, event, msg)
            await goods_re_root.finish()
        goods_name = item_info['name']
    else:  # 视为物品名称
        goods_name = args[0]
    back_msg = sql_message.get_back_msg(user_id)  # 背包sql信息,list(back)
    if back_msg is None:
        msg = "道友的背包空空如也！"
        await handle_send(bot, event, msg)
        await goods_re_root.finish()
    in_flag = False  # 判断指令是否正确，道具是否在背包内
    goods_id = None
    goods_type = None
    goods_state = None
    goods_num = None
    for back in back_msg:
        if goods_name == back['goods_name']:
            in_flag = True
            goods_id = back['goods_id']
            goods_type = back['goods_type']
            goods_state = back['state']
            goods_num = back['goods_num']
            break
    if not in_flag:
        msg = f"请检查该道具 {goods_name} 是否在背包内！"
        await handle_send(bot, event, msg)
        await goods_re_root.finish()

    if goods_type == "装备" and int(goods_state) == 1 and int(goods_num) == 1:
        msg = f"装备：{goods_name}已经被道友装备在身，无法炼金！"
        await handle_send(bot, event, msg)
        await goods_re_root.finish()

    if get_item_msg_rank(goods_id) == 520:
        msg = "此类物品不支持！"
        await handle_send(bot, event, msg)
        await goods_re_root.finish()
    try:
        if 1 <= int(args[1]) <= int(goods_num):
            num = int(args[1])
    except:
            num = 1 
    price = int((convert_rank('江湖好手')[0] - 16) * 100000 - get_item_msg_rank(goods_id) * 100000) * num
    if price <= 0:
        msg = f"物品：{goods_name}炼金失败，凝聚{number_to(price)}枚灵石，记得通知晓楠！"
        await handle_send(bot, event, msg)
        await goods_re_root.finish()

    sql_message.update_back_j(user_id, goods_id, num=num)
    sql_message.update_ls(user_id, price, 1)
    msg = f"物品：{goods_name} 数量：{num} 炼金成功，凝聚{number_to(price)}枚灵石！"
    await handle_send(bot, event, msg)
    await goods_re_root.finish()

@fast_alchemy.handle(parameterless=[Cooldown(at_sender=False)])
async def fast_alchemy_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """快速炼金（支持装备/药材/全部类型 + 全部品阶）"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg)
        await fast_alchemy.finish()
    
    user_id = user_info['user_id']
    args = args.extract_plain_text().split()
    
    # 指令格式检查
    if len(args) < 2:
        msg = "指令格式：快速炼金 [类型] [品阶]\n" \
              "▶ 类型：装备|法器|防具|药材|全部\n" \
              "▶ 品阶：全部|人阶|黄阶|...|上品通天法器（输入'品阶帮助'查看完整列表）"
        await handle_send(bot, event, msg)
        await fast_alchemy.finish()
    
    item_type = args[0]  # 物品类型
    rank_name = " ".join(args[1:])  # 处理多字品阶名（如"上品纯阳法器"）

    # === 保护机制 ===
    if item_type.lower() == "全部" and rank_name.lower() == "全部":
        msg = "⚠️ 为防止误操作，不能同时选择【全部类型】和【全部品阶】！"
        await handle_send(bot, event, msg)
        await fast_alchemy.finish()

    # === 类型检查 ===
    type_mapping = {
        "装备": ["法器", "防具"],
        "技能": ["功法", "神通", "辅修功法", "身法", "瞳术"],
        "功法": ["功法"],
        "神通": ["神通"],
        "辅修功法": ["辅修功法"],
        "身法": ["身法"],
        "瞳术": ["瞳术"],
        "法器": ["法器"],
        "防具": ["防具"],
        "药材": ["药材"],
        "全部": ["法器", "防具", "药材", "功法", "神通", "辅修功法", "身法", "瞳术"]
    }
    
    if item_type not in type_mapping:
        msg = f"❌❌ 无效类型！可用类型：{', '.join(type_mapping.keys())}"
        await handle_send(bot, event, msg)
        await fast_alchemy.finish()
    
    # === 品阶检查 ===
    rank_map = {
        # --- 装备品阶---
        "符器": ["下品符器", "上品符器"],
        "法器": ["下品法器", "上品法器"],
        "玄器": ["下品玄器", "上品玄器"],
        "纯阳": ["下品纯阳", "上品纯阳"],
        "纯阳法器": ["下品纯阳法器", "上品纯阳法器"],
        "通天": ["下品通天", "上品通天"],
        "通天法器": ["下品通天法器", "上品通天法器"],
        "仙器": ["下品仙器", "上品仙器"],
        "下品符器": ["下品符器"],
        "上品符器": ["上品符器"],
        "下品法器": ["下品法器"],
        "上品法器": ["上品法器"],
        "下品玄器": ["下品玄器"],
        "上品玄器": ["上品玄器"],
        "下品纯阳": ["下品纯阳"],
        "上品纯阳": ["上品纯阳"],
        "下品纯阳法器": ["下品纯阳法器"],
        "上品纯阳法器": ["上品纯阳法器"],
        "下品通天": ["下品通天"],
        "上品通天": ["上品通天"],
        "下品通天法器": ["下品通天法器"],
        "上品通天法器": ["上品通天法器"],
        
        # --- 药材品阶---
        "一品药材": ["一品药材"],
        "二品药材": ["二品药材"],
        "三品药材": ["三品药材"],
        "四品药材": ["四品药材"],
        "五品药材": ["五品药材"],
        "六品药材": ["六品药材"],
        "七品药材": ["七品药材"],
        "八品药材": ["八品药材"],
        "九品药材": ["九品药材"],
        
        # --- 功法品阶---
        "人阶下品": "人阶下品", "人阶上品": "人阶上品",
        "黄阶下品": "黄阶下品", "黄阶上品": "黄阶上品",
        "玄阶下品": "玄阶下品", "玄阶上品": "玄阶上品",
        "地阶下品": "地阶下品", "地阶上品": "地阶上品",
        "天阶下品": "天阶下品", "天阶上品": "天阶上品",
        "仙阶下品": "仙阶下品", "仙阶上品": "仙阶上品",
        "人阶": ["人阶下品", "人阶上品"],
        "黄阶": ["黄阶下品", "黄阶上品"],
        "玄阶": ["玄阶下品", "玄阶上品"],
        "地阶": ["地阶下品", "地阶上品"],
        "天阶": ["天阶下品", "天阶上品"],
        "仙阶": ["仙阶下品", "仙阶上品"],
        
        # --- 全部品阶（不包含仙器、九品药材和仙阶功法）---
        "全部": [
            # 装备
            "下品符器", "上品符器", "下品法器", "上品法器", "下品玄器", "上品玄器",
            "下品纯阳", "上品纯阳", "下品纯阳法器", "上品纯阳法器", "下品通天", "上品通天", "下品通天法器", "上品通天法器",
            # 药材
            "一品药材", "二品药材", "三品药材", "四品药材",
            "五品药材", "六品药材", "七品药材", "八品药材",
            # 功法
            "人阶下品", "人阶上品", "黄阶下品", "黄阶上品",
            "玄阶下品", "玄阶上品", "地阶下品", "地阶上品",
            "天阶下品", "天阶上品"
        ]
    }
    
    if rank_name not in rank_map:
        msg = f"❌❌ 无效品阶！输入'品阶帮助'查看完整列表"
        await handle_send(bot, event, msg)
        await fast_alchemy.finish()
    
    # === 获取背包物品 ===
    back_msg = sql_message.get_back_msg(user_id)
    if not back_msg:
        msg = "💼💼 道友的背包空空如也！"
        await handle_send(bot, event, msg)
        await fast_alchemy.finish()
    
    # === 筛选物品 ===
    target_types = type_mapping[item_type]
    target_ranks = rank_map[rank_name]
    
    items_to_alchemy = []
    for item in back_msg:
        item_info = items.get_data_by_item_id(item['goods_id'])
        if not item_info:
            continue
            
        # 类型匹配
        type_match = (
            item['goods_type'] in target_types or 
            item_info.get('item_type', '') in target_types
        )
        
        # 品阶匹配
        rank_match = item_info.get('level', '') in target_ranks
        
        if type_match and rank_match:
            available_num = item['goods_num']
            if available_num > 0:
                # 计算价格（基础rank - 物品rank）* 100000
                base_rank = convert_rank('江湖好手')[0]
                item_rank = get_item_msg_rank(item['goods_id'])
                price = max(1, (base_rank - 16) * 100000 - item_rank * 100000)  # 防止负数
                
                items_to_alchemy.append({
                    'id': item['goods_id'],
                    'name': item['goods_name'],
                    'quantity': available_num,
                    'price': price
                })

    # === 执行炼金 ===
    if not items_to_alchemy:
        msg = f"🔍🔍 背包中没有符合条件的【{item_type}·{rank_name}】物品"
        await handle_send(bot, event, msg)
        await fast_alchemy.finish()
    
    total_stone = 0
    result_msg = []
    
    for item in items_to_alchemy:
        item_total = item['price'] * item['quantity']
        total_stone += item_total
        
        # 从背包移除
        sql_message.update_back_j(user_id, item['id'], num=item['quantity'])
        
        # 记录结果
        result_msg.append(
            f"{item['name']} ×{item['quantity']} → {number_to(item_total)}灵石"
        )
    
    # 增加灵石
    sql_message.update_ls(user_id, total_stone, 1)
    
    # === 返回结果 ===
    msg = [
        f"✨ 成功炼金 {len(items_to_alchemy)} 件物品",
        *result_msg,
        f"💎💎 总计获得：{number_to(total_stone)}灵石"
    ]
    
    await send_msg_handler(bot, event, '快速炼金', bot.self_id, msg)
    await fast_alchemy.finish()

@no_use_zb.handle(parameterless=[Cooldown(at_sender=False)])
async def no_use_zb_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """卸载物品（只支持装备）
    ["user_id", "goods_id", "goods_name", "goods_type", "goods_num", "create_time", "update_time",
    "remake", "day_num", "all_num", "action_time", "state"]
    """
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await no_use_zb.finish()
    user_id = user_info['user_id']
    arg = args.extract_plain_text().strip()

    back_msg = sql_message.get_back_msg(user_id)  # 背包sql信息,list(back)
    if back_msg is None:
        msg = "道友的背包空空如也！"
        await handle_send(bot, event, msg)
        await no_use_zb.finish()
    in_flag = False  # 判断指令是否正确，道具是否在背包内
    goods_id = None
    goods_type = None
    for back in back_msg:
        if arg == back['goods_name']:
            in_flag = True
            goods_id = back['goods_id']
            goods_type = back['goods_type']
            break
    if not in_flag:
        msg = f"请检查道具 {arg} 是否在背包内！"
        await handle_send(bot, event, msg)
        await no_use_zb.finish()

    if goods_type == "装备":
        if not check_equipment_can_use(user_id, goods_id):
            sql_str, item_type = get_no_use_equipment_sql(user_id, goods_id)
            for sql in sql_str:
                sql_message.update_back_equipment(sql)
            if item_type == "法器":
                sql_message.updata_user_faqi_buff(user_id, 0)
            if item_type == "防具":
                sql_message.updata_user_armor_buff(user_id, 0)
            msg = f"成功卸载装备{arg}！"
            await handle_send(bot, event, msg)
            await no_use_zb.finish()
        else:
            msg = "装备没有被使用，无法卸载！"
            await handle_send(bot, event, msg)
            await no_use_zb.finish()
    else:
        msg = "目前只支持卸载装备！"
        await handle_send(bot, event, msg)
        await no_use_zb.finish()


@use.handle(parameterless=[Cooldown(at_sender=False)])
async def use_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """使用物品"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await use.finish()
    user_id = user_info['user_id']
    args = args.extract_plain_text().split()
    if not args:
        msg = "请输入要使用的物品名称！"
        await handle_send(bot, event, msg)
        await use.finish()
    
    arg = args[0]  # 物品名称
    back_msg = sql_message.get_back_msg(user_id)  # 获取背包信息
    if back_msg is None:
        msg = "道友的背包空空如也！"
        await handle_send(bot, event, msg)
        await use.finish()
    
    # 检查物品是否在背包中
    in_flag = False
    goods_id = None
    goods_type = None
    goods_num = None
    for back in back_msg:
        if arg == back['goods_name']:
            in_flag = True
            goods_id = back['goods_id']
            goods_type = back['goods_type']
            goods_num = back['goods_num']
            break
    
    if not in_flag:
        msg = f"请检查该道具 {arg} 是否在背包内！"
        await handle_send(bot, event, msg)
        await use.finish()
    
    # 处理使用数量的通用逻辑
    num = 1
    try:
        if len(args) > 1 and 1 <= int(args[1]) <= int(goods_num):
            num = int(args[1])
        elif len(args) > 1 and int(args[1]) > int(goods_num):
            msg = f"道友背包中的{arg}数量不足，当前仅有{goods_num}个！"
            await handle_send(bot, event, msg)
            await use.finish()
    except ValueError:
        num = 1
    
    # 根据物品类型处理逻辑
    goods_info = items.get_data_by_item_id(goods_id)

    if goods_type == "礼包":
        package_name = goods_info['name']
        msg_parts = []
        i = 1
        while True:
            buff_key = f'buff_{i}'
            name_key = f'name_{i}'
            type_key = f'type_{i}'
            amount_key = f'amount_{i}'

            if name_key not in goods_info:
                break

            item_name = goods_info[name_key]
            item_amount = goods_info.get(amount_key, 1) * num
            item_type = goods_info.get(type_key)
            buff_id = goods_info.get(buff_key)

            if item_name == "灵石":
                key = 1 if item_amount > 0 else 2  # 正数增加，负数减少
                sql_message.update_ls(user_id, abs(item_amount), key)
                msg_parts.append(f"获得灵石 {item_amount} 枚\n")
            else:
                # 调整 goods_type
                if item_type in ["辅修功法", "神通", "功法", "身法", "瞳术"]:
                    goods_type_item = "技能"
                elif item_type in ["法器", "防具"]:
                    goods_type_item = "装备"
                else:
                    goods_type_item = item_type  # 包括 "礼包" 类型，直接放入背包

                if buff_id is not None:
                    sql_message.send_back(user_id, buff_id, item_name, goods_type_item, item_amount, 1)
                    msg_parts.append(f"获得 {item_name} x{item_amount}\n")
            
            i += 1
        sql_message.update_back_j(user_id, goods_id, num=num, use_key=1)
        msg = f"道友打开了 {num} 个 {package_name}:\n" + "".join(msg_parts)

    elif goods_type == "装备":
        if not check_equipment_can_use(user_id, goods_id):
            msg = "该装备已被装备，请勿重复装备！"
        else:
            sql_str, item_type = get_use_equipment_sql(user_id, goods_id)
            for sql in sql_str:
                sql_message.update_back_equipment(sql)
            if item_type == "法器":
                sql_message.updata_user_faqi_buff(user_id, goods_id)
            if item_type == "防具":
                sql_message.updata_user_armor_buff(user_id, goods_id)
            msg = f"成功装备 {arg}！"

    elif goods_type == "技能":
        user_buff_info = UserBuffDate(user_id).BuffInfo
        skill_info = goods_info
        skill_type = skill_info['item_type']
        if skill_type == "神通":
            if int(user_buff_info['sec_buff']) == int(goods_id):
                msg = f"道友已学会该神通：{skill_info['name']}，请勿重复学习！"
            else:
                sql_message.update_back_j(user_id, goods_id)
                sql_message.updata_user_sec_buff(user_id, goods_id)
                msg = f"恭喜道友学会神通：{skill_info['name']}！"
        elif skill_type == "身法":
            if int(user_buff_info['effect1_buff']) == int(goods_id):
                msg = f"道友已学会该身法：{skill_info['name']}，请勿重复学习！"
            else:
                sql_message.update_back_j(user_id, goods_id)
                sql_message.updata_user_effect1_buff(user_id, goods_id)
                msg = f"恭喜道友学会身法：{skill_info['name']}！"
        elif skill_type == "瞳术":
            if int(user_buff_info['effect2_buff']) == int(goods_id):
                msg = f"道友已学会该瞳术：{skill_info['name']}，请勿重复学习！"
            else:
                sql_message.update_back_j(user_id, goods_id)
                sql_message.updata_user_effect2_buff(user_id, goods_id)
                msg = f"恭喜道友学会瞳术：{skill_info['name']}！"
        elif skill_type == "功法":
            if int(user_buff_info['main_buff']) == int(goods_id):
                msg = f"道友已学会该功法：{skill_info['name']}，请勿重复学习！"
            else:
                sql_message.update_back_j(user_id, goods_id)
                sql_message.updata_user_main_buff(user_id, goods_id)
                msg = f"恭喜道友学会功法：{skill_info['name']}！"
        elif skill_type == "辅修功法":
            if int(user_buff_info['sub_buff']) == int(goods_id):
                msg = f"道友已学会该辅修功法：{skill_info['name']}，请勿重复学习！"
            else:
                sql_message.update_back_j(user_id, goods_id)
                sql_message.updata_user_sub_buff(user_id, goods_id)
                msg = f"恭喜道友学会辅修功法：{skill_info['name']}！"
        else:
            msg = f"发生未知错误！"

    elif goods_type == "丹药":
        msg = check_use_elixir(user_id, goods_id, num)
        
    elif goods_type == "特殊道具":
        msg = f"请发送:道具使用{goods_info['name']}"

    elif goods_type == "神物":
        user_info = sql_message.get_user_info_with_id(user_id)
        user_rank = convert_rank(user_info['level'])[0]
        goods_rank = goods_info['rank']
        goods_name = goods_info['name']
        if goods_rank < user_rank:
            msg = f"神物：{goods_name}的使用境界为{goods_info['境界']}以上，道友不满足使用条件！"
        else:
            exp = goods_info['buff'] * num
            user_hp = int(user_info['hp'] + (exp / 2))
            user_mp = int(user_info['mp'] + exp)
            user_atk = int(user_info['atk'] + (exp / 10))
            sql_message.update_exp(user_id, exp)
            sql_message.update_power2(user_id)
            sql_message.update_user_attribute(user_id, user_hp, user_mp, user_atk)
            sql_message.update_back_j(user_id, goods_id, num=num, use_key=1)
            msg = f"道友成功使用神物：{goods_name} {num} 个，修为增加 {exp} 点！"

    elif goods_type == "聚灵旗":
        msg = get_use_jlq_msg(user_id, goods_id)

    else:
        msg = "该类型物品调试中，未开启！"

    # 发送结果消息
    await handle_send(bot, event, msg)
    await use.finish()

@auction_view.handle(parameterless=[Cooldown(at_sender=False, isolate_level=CooldownIsolateLevel.GROUP)])
async def auction_view_(bot: Bot, event: GroupMessageEvent, args: Message = CommandArg()):
    """查看拍卖会物品"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    group_id = "000000"
    if not isUser:
        await handle_send(bot, event, msg)
        await auction_view.finish()
    
    if group_id not in groups:
        msg = '尚未开启拍卖会功能，请联系管理员开启！'
        await handle_send(bot, event, msg)
        await auction_view.finish()

    config = get_auction_config()
    user_auctions = config.get('user_auctions', [])
   

    if not user_auctions:
        msg = "拍卖会目前没有道友提交的物品！"
        await handle_send(bot, event, msg)
        await auction_view.finish()

    auction_list_msg = "拍卖会物品列表:\n"
    
    for idx, auction in enumerate(user_auctions):
        for goods_name, details in auction.items():
            user_info = sql_message.get_user_info_with_id(details['user_id'])
            auction_list_msg += f"编号: {idx + 1}\n物品名称: {goods_name}\n物品类型：{details['goods_type']}\n所有者：{user_info['user_name']}\n底价: {details['start_price']} 枚灵石\n数量: {details['quantity']}\n"
            auction_list_msg += "☆------------------------------☆\n"

    await handle_send(bot, event, auction_list_msg)

    await auction_view.finish()


@creat_auction.handle(parameterless=[Cooldown(at_sender=False)])
async def creat_auction_(bot: Bot, event: GroupMessageEvent):
    global auction, auction_offer_flag, auction_offer_all_count, auction_offer_time_count
    group_id = "000000"
    bot = await assign_bot_group(group_id=group_id)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await creat_auction.finish()
        
    if group_id not in groups:
        msg = '尚未开启拍卖会功能，请联系管理员开启！'
        await handle_send(bot, event, msg)
        await creat_auction.finish()

    if auction:
        msg = "已存在一场拍卖会，请等待拍卖会结束！"
        await handle_send(bot, event, msg)
        await creat_auction.finish()

    auction_items = []
    try:
        # 用户拍卖品
        user_auction_id_list = get_user_auction_id_list()
        for auction_id in user_auction_id_list:
            user_auction_info = get_user_auction_price_by_id(auction_id)
            auction_items.append((auction_id, user_auction_info['quantity'], user_auction_info['start_price'], True))

        # 系统拍卖品
        auction_id_list = get_auction_id_list()
        auction_count = random.randint(1, 2)  # 随机挑选系统拍卖品数量
        auction_ids = random.sample(auction_id_list, auction_count)
        for auction_id in auction_ids:
            item_info = items.get_data_by_item_id(auction_id)
            item_quantity = 1
            if item_info['type'] in ['神物', '丹药']:
                item_quantity = random.randint(1, 3) # 如果是丹药的话随机挑1-3个
            auction_items.append((auction_id, item_quantity, get_auction_price_by_id(auction_id)['start_price'], False))
    except LookupError:
        msg = f"获取不到拍卖物品的信息，请检查配置文件！"
        await handle_send(bot, event, msg)
        await creat_auction.finish()

    # 打乱拍卖品顺序
    random.shuffle(auction_items)

    msg = f"请各位道友稍作准备，拍卖即将开始...\n"
    msg += f"本场拍卖会共有{len(auction_items)}件物品，将依次拍卖，分别是：\n"
    for idx, (auction_id, item_quantity, start_price, is_user_auction) in enumerate(auction_items):
        item_name = items.get_data_by_item_id(auction_id)['name']
        if is_user_auction:
            owner_info = sql_message.get_user_info_with_id(get_user_auction_price_by_id(auction_id)['user_id'])
            owner_name = owner_info['user_name']
            msg += f"{idx + 1}号：{item_name}x{item_quantity}（由{owner_name}道友提供）\n"
        else:
            msg += f"{idx + 1}号：{item_name}x{item_quantity}（由拍卖场提供）\n"
    
    for gid in groups:
        bot = await assign_bot_group(group_id=gid)
        try:
            await handle_send(bot, event, msg)
        except ActionFailed:
            continue
    
    auction_results = []  # 拍卖结果
    for i, (auction_id, item_quantity, start_price, is_user_auction) in enumerate(auction_items):
        auction_info = items.get_data_by_item_id(auction_id)

        auction = {
            'id': auction_id,
            'user_id': 0,
            'now_price': start_price,
            'name': auction_info['name'],
            'type': auction_info['type'],
            'quantity': item_quantity,
            'start_time': datetime.now(),
            'group_id': group_id
        }
        
        if i + 1 == len(auction_items):
            msg = f"最后一件拍卖品为：\n{get_auction_msg(auction_id)}\n"
        else:
            msg = f"第{i + 1}件拍卖品为：\n{get_auction_msg(auction_id)}\n"
        msg += f"\n底价为{start_price}，加价不少于{int(start_price * 0.05)}"
        msg += f"\n竞拍时间为:{AUCTIONSLEEPTIME}秒，请诸位道友发送 拍卖+金额 来进行拍卖吧！"

        if auction['quantity'] > 1:
            msg += f"\n注意：拍卖品共{auction['quantity']}件，最终价为{auction['quantity']}x成交价。\n"

        if i + 1 < len(auction_items):
            next_item_name = items.get_data_by_item_id(auction_items[i + 1][0])['name']
            msg += f"\n下一件拍卖品为：{next_item_name}，请心仪的道友提前开始准备吧！"

        for gid in groups:
            bot = await assign_bot_group(group_id=gid)
            try:
                await handle_send(bot, event, msg)
            except ActionFailed:
                continue
        
        remaining_time = AUCTIONSLEEPTIME # 第一轮定时
        while remaining_time > 0:
            await asyncio.sleep(10)
            remaining_time -= 10

        while auction_offer_flag:  # 有人拍卖
            if auction_offer_all_count == 0:
                auction_offer_flag = False
                break

            logger.opt(colors=True).info(f"<green>有人拍卖，本次等待时间：{auction_offer_all_count * AUCTIONOFFERSLEEPTIME}秒</green>")
            first_time = auction_offer_all_count * AUCTIONOFFERSLEEPTIME
            auction_offer_all_count = 0
            auction_offer_flag = False
            await asyncio.sleep(first_time)
            logger.opt(colors=True).info(f"<green>总计等待时间{auction_offer_time_count * AUCTIONOFFERSLEEPTIME}秒，当前拍卖标志：{auction_offer_flag}，本轮等待时间：{first_time}</green>")

        logger.opt(colors=True).info(f"<green>等待时间结束，总计等待时间{auction_offer_time_count * AUCTIONOFFERSLEEPTIME}秒</green>")
        if auction['user_id'] == 0:
            msg = f"很可惜，{auction['name']}流拍了\n"
            if i + 1 == len(auction_items):
                msg += f"本场拍卖会到此结束，开始整理拍卖会结果，感谢各位道友参与！"

            for gid in groups:
                bot = await assign_bot_group(group_id=gid)
                try:
                    await handle_send(bot, event, msg)
                except ActionFailed:
                    continue
            auction_results.append((auction_id, None, auction['group_id'], auction_info['type'], auction['now_price'], auction['quantity']))
            auction = {}
            continue
        
        user_info = sql_message.get_user_info_with_id(auction['user_id'])
        msg = f"(拍卖锤落下)！！！\n"
        msg += f"恭喜来自群{auction['group_id']}的{user_info['user_name']}道友成功拍下：{auction['type']}-{auction['name']}x{auction['quantity']}，将在拍卖会结算后送到您手中。\n"
        if i + 1 == len(auction_items):
            msg += f"本场拍卖会到此结束，开始整理拍卖会结果，感谢各位道友参与！"

        auction_results.append((auction_id, user_info['user_id'], auction['group_id'], 
                                auction_info['type'], auction['now_price'], auction['quantity']))
        auction = {}
        auction_offer_time_count = 0
        for gid in groups:
            bot = await assign_bot_group(group_id=gid)
            try:
                await handle_send(bot, event, msg)
            except ActionFailed:
                continue
        
    # 拍卖会结算
    end_msg = f"本场拍卖会结束！感谢各位道友的参与。\n拍卖结果整理如下：\n"
    for idx, (auction_id, user_id, group_id, item_type, final_price, quantity) in enumerate(auction_results):
        item_name = items.get_data_by_item_id(auction_id)['name']
        final_user_info = sql_message.get_user_info_with_id(user_id)
        if user_id:
            if final_user_info['stone'] < (int(final_price) * quantity):
                end_msg += f"{idx + 1}号拍卖品：{item_name}x{quantity} - 道友{final_user_info['user_name']}的灵石不足，流拍了\n"
            else:
                sql_message.update_ls(user_id, int(final_price) * quantity, 2)
                sql_message.send_back(user_id, auction_id, item_name, item_type, quantity)
                end_msg += f"{idx + 1}号拍卖品：{item_name}x{quantity}由群{group_id}的{final_user_info['user_name']}道友成功拍下\n"

            user_auction_info = get_user_auction_price_by_id(auction_id)
            if user_auction_info:
                seller_id = user_auction_info['user_id']
                auction_earnings = int(final_price * quantity * 0.7) # 收个手续费
                sql_message.update_ls(seller_id, auction_earnings, 1)

            remove_auction_item(auction_id)

            auction = {}
            auction_offer_time_count = 0
        else:
            end_msg += f"{idx + 1}号拍卖品：{item_name}x{quantity} - 流拍了\n"

    for gid in groups:
        bot = await assign_bot_group(group_id=gid)
        try:
            await handle_send(bot, event, end_msg)
        except ActionFailed:  # 发送群消息失败
            continue

    await creat_auction.finish()


@offer_auction.handle(parameterless=[Cooldown(1.4, at_sender=False, isolate_level=CooldownIsolateLevel.GLOBAL)])
async def offer_auction_(bot: Bot, event: GroupMessageEvent, args: Message = CommandArg()):
    """拍卖"""
    group_id = "000000"
    bot = await assign_bot_group(group_id=group_id)
    isUser, user_info, msg = check_user(event)
    global auction, auction_offer_flag, auction_offer_all_count, auction_offer_time_count
    if not isUser:
        await handle_send(bot, event, msg)
        await offer_auction.finish()

    if group_id not in groups:
        msg = f"尚未开启拍卖会功能，请联系管理员开启！"
        await handle_send(bot, event, msg)
        await offer_auction.finish()

    if not auction:
        msg = f"不存在拍卖会，请等待拍卖会开启！"
        await handle_send(bot, event, msg)
        await offer_auction.finish()

    price = args.extract_plain_text().strip()
    try:
        price = int(price)
    except ValueError:
        msg = f"请发送正确的灵石数量"
        await handle_send(bot, event, msg)
        await offer_auction.finish()

    now_price = auction['now_price']
    min_price = int(now_price * 0.05)  # 最低加价5%
    if price <= 0 or price <= auction['now_price'] or price > user_info['stone']:
        msg = f"走开走开，别捣乱！小心清空你灵石捏"
        await handle_send(bot, event, msg)
        await offer_auction.finish()
    if price - now_price < min_price:
        msg = f"拍卖不得少于当前竞拍价的5%，目前最少加价为：{min_price}灵石，目前竞拍价为：{now_price}!"
        await handle_send(bot, event, msg)
        await offer_auction.finish()

    auction_offer_flag = True  # 有人拍卖
    auction_offer_time_count += 1
    auction_offer_all_count += 1

    auction['user_id'] = user_info['user_id']
    auction['now_price'] = price
    auction['group_id'] = group_id

    logger.opt(colors=True).info(f"<green>{user_info['user_name']}({auction['user_id']})竞价了！！</green>")

    now_time = datetime.now()
    dif_time = (now_time - auction['start_time']).total_seconds()
    remaining_time = int(AUCTIONSLEEPTIME - dif_time + AUCTIONOFFERSLEEPTIME * auction_offer_time_count)
    msg = (
        f"来自群{group_id}的{user_info['user_name']}道友拍卖：{number_to(price)}枚灵石！" +
        f"竞拍时间增加：{AUCTIONOFFERSLEEPTIME}秒，竞拍剩余时间：{remaining_time}秒"
    )
    error_msg = None
    for group_id in groups:
        bot = await assign_bot_group(group_id=group_id)
        try:
            await handle_send(bot, event, msg)
        except ActionFailed:
            continue
    logger.opt(colors=True).info(
        f"<green>有人拍卖，拍卖标志：{auction_offer_flag}，当前等待时间：{auction_offer_all_count * AUCTIONOFFERSLEEPTIME}，总计拍卖次数：{auction_offer_time_count}</green>")
    if error_msg is None:
        await offer_auction.finish()
    else:
        msg = error_msg
        await handle_send(bot, event, msg)
        await offer_auction.finish()


@auction_added.handle(parameterless=[Cooldown(1.4, isolate_level=CooldownIsolateLevel.GROUP)])
async def auction_added_(bot: Bot, event: GroupMessageEvent, args: Message = CommandArg()):
    """用户提交拍卖品"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    group_id = "000000"
    if not isUser:
        await handle_send(bot, event, msg)
        await auction_added.finish()

    if group_id not in groups:
        msg = f"尚未开启拍卖会功能，请联系管理员开启！"
        await handle_send(bot, event, msg)
        await auction_added.finish()

    user_id = user_info['user_id']
    args = args.extract_plain_text().split()
    goods_name = args[0] if len(args) > 0 else None
    price_str = args[1] if len(args) > 1 else "1"
    quantity_str = args[2] if len(args) > 2 else "1"

    if not goods_name:
        msg = f"请输入正确指令！例如：提交拍卖品 物品 可选参数为(金额 数量)"
        await handle_send(bot, event, msg)
        await auction_added.finish()

    back_msg = sql_message.get_back_msg(user_id)  # 获取背包信息
    if back_msg is None:
        msg = f"道友的背包空空如也！"
        await handle_send(bot, event, msg)
        await auction_added.finish()

    # 物品是否存在于背包中
    in_flag = False
    goods_id = None
    goods_type = None
    goods_state = None
    goods_num = None
    goods_bind_num = None
    for back in back_msg:
        if goods_name == back['goods_name']:
            in_flag = True
            goods_id = back['goods_id']
            goods_type = back['goods_type']
            goods_state = back['state']
            goods_num = back['goods_num']
            goods_bind_num = back['bind_num']
            break

    if not in_flag:
        msg = f"请检查该道具 {goods_name} 是否在背包内！"
        await handle_send(bot, event, msg)
        await auction_added.finish()

    try:
        price = int(price_str)
        quantity = int(quantity_str)
        if price <= 0 or quantity <= 0 or quantity > goods_num:
            raise ValueError("价格和数量必须为正数，或者超过了你拥有的数量!")
    except ValueError as e:
        msg = f"请输入正确的金额和数量: {str(e)}"
        await handle_send(bot, event, msg)
        await auction_added.finish()

    if goods_type == "装备" and int(goods_state) == 1 and int(goods_num) == 1:
        msg = f"装备：{goods_name}已经被道友装备在身，无法提交！"
        await handle_send(bot, event, msg)
        await auction_added.finish()

    if int(goods_num) <= int(goods_bind_num):
        msg = f"该物品是绑定物品，无法提交！"
        await handle_send(bot, event, msg)
        await auction_added.finish()
    if goods_type == "聚灵旗" or goods_type == "炼丹炉":
        if user_info['root'] == "凡人":
            pass
        else:
            msg = f"道友职业无法上架！"
            await handle_send(bot, event, msg)
            await auction_added.finish()

    config = get_auction_config()

    user_auction = {
        goods_name: {
            'id': goods_id,
            'goods_type': goods_type,
            'user_id': user_id,
            'start_price': price,
            'quantity': quantity
        }
    }
    config['user_auctions'].append(user_auction)

    savef_auction(config)
    sql_message.update_back_j(user_id, goods_id, num=quantity)

    msg = f"道友的拍卖品：{goods_name}成功提交，底价：{number_to(price)}枚灵石，数量：{quantity}"
    msg += f"\n下次拍卖将优先拍卖道友的拍卖品！！！"
    await handle_send(bot, event, msg)
    await auction_added.finish()


@set_auction.handle(parameterless=[Cooldown(at_sender=False)])
async def set_auction_(bot: Bot, event: GroupMessageEvent, args: Message = CommandArg()):
    """拍卖会开关"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    mode = args.extract_plain_text().strip()
    group_id = "000000"
    is_in_group = is_in_groups(event)  # True在，False不在

    if mode == '开启':
        if is_in_group:
            msg = "已开启拍卖会，请勿重复开启!"
            await handle_send(bot, event, msg)
            await set_auction.finish()
        else:
            config['open'].append(group_id)
            savef_auction(config)
            msg = "已开启拍卖会"
            await handle_send(bot, event, msg)
            await set_auction.finish()

    elif mode == '关闭':
        if is_in_group:
            config['open'].remove(group_id)
            savef_auction(config)
            msg = "已关闭拍卖会!"
            await handle_send(bot, event, msg)
            await set_auction.finish()
        else:
            msg = "未开启拍卖会!"
            await handle_send(bot, event, msg)
            await set_auction.finish()

    else:
        await set_auction.finish()


@chakan_wupin.handle(parameterless=[Cooldown(at_sender=False)])
async def chakan_wupin_(
    bot: Bot, 
    event: GroupMessageEvent | PrivateMessageEvent, 
    args: Message = CommandArg()
):
    """查看修仙界物品（支持 类型+页码 或 类型 + 空格 + 页码）"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    args_str = args.extract_plain_text().strip()
    
    # 支持的类型列表
    valid_types = ["功法", "辅修功法", "神通", "身法", "瞳术", "丹药", "合成丹药", "法器", "防具", "特殊物品"]
    
    # 解析类型和页码
    item_type = None
    current_page = 1  # 默认第一页
    
    # 情况1：用户输入类似 "神通2"（无空格）
    for t in valid_types:
        if args_str.startswith(t) and len(args_str) > len(t):
            remaining = args_str[len(t):].strip()
            if remaining.isdigit():  # 检查剩余部分是否是数字
                item_type = t
                current_page = int(remaining)
                break
    
    if item_type is None:
        parts = args_str.split(maxsplit=1)  # 只分割第一个空格
        if len(parts) == 2 and parts[0] in valid_types and parts[1].isdigit():
            item_type = parts[0]
            current_page = int(parts[1])
        elif args_str in valid_types:  # 仅类型，无页码
            item_type = args_str
        else:
            msg = "请输入正确类型【功法|辅修功法|神通|身法|瞳术|丹药|合成丹药|法器|防具|特殊物品】！！！"
            await handle_send(bot, event, msg)
            await chakan_wupin.finish()
    
    # 获取物品数据
    if item_type == "特殊物品":
        # 特殊物品包括聚灵旗和特殊道具
        jlq_data = items.get_data_by_item_type(["聚灵旗"])
        special_data = items.get_data_by_item_type(["特殊物品"])
        item_data = {**jlq_data, **special_data}
    else:
        item_data = items.get_data_by_item_type([item_type])
    
    msg_list = []
    
    for item_id, item_info in item_data.items():
        name = item_info['name']
        if item_type in ["功法", "辅修功法", "神通", "身法", "瞳术", "法器", "防具"]:
            desc = get_item_msg(item_id)
            msg = f"ID：{item_id}\n{desc}"
        elif item_type == "特殊物品":
            if item_info['type'] == "聚灵旗":
                msg = f"ID：{item_id}\n名字：{name}\n效果：{item_info['desc']}\n修炼速度：{item_info['修炼速度'] * 100}%\n药材速度：{item_info['药材速度'] * 100}%"
            else:  # 特殊道具
                msg = f"ID：{item_id}\n名字：{name}\n效果：{item_info.get('desc', '十分神秘的东西，谁也不知道它的作用')}"
        else:  # 丹药、合成丹药
            rank = item_info.get('境界', '')
            desc = item_info.get('desc', '')
            msg = f"※{rank}丹药:{name}，效果：{desc}\n"
        msg_list.append(msg)
    
    # 分页处理
    title = f"修仙界物品列表-{item_type}"
    msgs = await handle_pagination(
        msg_list, 
        current_page, 
        title=title, 
        empty_msg=f"修仙界暂无{item_type}类物品"
    )
    
    if isinstance(msgs, str):  # 空提示消息
        await handle_send(bot, event, msgs)
    else:  # 分页消息列表
        await send_msg_handler(bot, event, title, bot.self_id, msgs)
    
    await chakan_wupin.finish()

@main_back.handle(parameterless=[Cooldown(cd_time=10, at_sender=False)])
async def main_back_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """我的背包"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await main_back.finish()
    
    # 获取页码
    try:
        current_page = int(args.extract_plain_text().strip())
    except:
        current_page = 1
    
    user_id = user_info['user_id']
    msg_list = get_user_main_back_msg(user_id)
    title = f"{user_info['user_name']}的背包"
    msgs = await handle_pagination(
        msg_list, 
        current_page,
        title=title,
        empty_msg="道友的背包空空如也！"
    )
    
    if isinstance(msgs, str):
        await handle_send(bot, event, msgs)
    else:
        await send_msg_handler(bot, event, '背包', bot.self_id, msgs)
    
    await main_back.finish()

@yaocai_back.handle(parameterless=[Cooldown(cd_time=10, at_sender=False)])
async def yaocai_back_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """药材背包"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await yaocai_back.finish()
    
    # 获取页码
    try:
        current_page = int(args.extract_plain_text().strip())
    except:
        current_page = 1
    
    user_id = user_info['user_id']
    msg_list = get_user_yaocai_back_msg(user_id)
    title = f"{user_info['user_name']}的药材背包"
    msgs = await handle_pagination(
        msg_list, 
        current_page,
        title=title,
        empty_msg="道友的药材背包空空如也！"
    )
    
    if isinstance(msgs, str):
        await handle_send(bot, event, msgs)
    else:
        await send_msg_handler(bot, event, '药材背包', bot.self_id, msgs)
    
    await yaocai_back.finish()

@danyao_back.handle(parameterless=[Cooldown(cd_time=10, at_sender=False)])
async def danyao_back_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """丹药背包"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await danyao_back.finish()
    
    # 获取页码
    try:
        current_page = int(args.extract_plain_text().strip())
    except:
        current_page = 1
    
    user_id = user_info['user_id']
    msg_list = get_user_danyao_back_msg(user_id)
    title = f"{user_info['user_name']}的丹药背包"
    msgs = await handle_pagination(
        msg_list, 
        current_page,
        title=title,
        empty_msg="道友的丹药背包空空如也！"
    )
    
    if isinstance(msgs, str):
        await handle_send(bot, event, msgs)
    else:
        await send_msg_handler(bot, event, '丹药背包', bot.self_id, msgs)
    
    await danyao_back.finish()

def reset_dict_num(dict_):
    i = 1
    temp_dict = {}
    for k, v in dict_.items():
        temp_dict[i] = v
        temp_dict[i]['编号'] = i
        i += 1
    return temp_dict


def get_user_auction_id_list():
    user_auctions = config['user_auctions']
    user_auction_id_list = []
    for auction in user_auctions:
        for k, v in auction.items():
            user_auction_id_list.append(v['id'])
    return user_auction_id_list

def get_auction_id_list():
    auctions = config['auctions']
    auction_id_list = []
    for k, v in auctions.items():
        auction_id_list.append(v['id'])
    return auction_id_list

def get_user_auction_price_by_id(id):
    user_auctions = config['user_auctions']
    user_auction_info = None
    for auction in user_auctions:
        for k, v in auction.items():
            if int(v['id']) == int(id):
                user_auction_info = v
                break
        if user_auction_info:
            break
    return user_auction_info

def get_auction_price_by_id(id):
    auctions = config['auctions']
    auction_info = None
    for k, v in auctions.items():
        if int(v['id']) == int(id):
            auction_info = v
            break
    return auction_info


def is_in_groups(event: GroupMessageEvent):
    return str(event.group_id) in groups


def get_auction_msg(auction_id):
    item_info = items.get_data_by_item_id(auction_id)
    _type = item_info['type']
    msg = None
    if _type == "装备":
        if item_info['item_type'] == "防具":
            msg = get_armor_info_msg(auction_id, item_info)
        if item_info['item_type'] == '法器':
            msg = get_weapon_info_msg(auction_id, item_info)

    if _type == "技能":
        if item_info['item_type'] == '神通':
            msg = f"{item_info['level']}-{item_info['name']}:\n"
            msg += f"效果：{get_sec_msg(item_info)}"
        if item_info['item_type'] == '功法':
            msg = f"{item_info['level']}-{item_info['name']}\n"
            msg += f"效果：{get_main_info_msg(auction_id)[1]}"
        if item_info['item_type'] == '辅修功法': #辅修功法10
            msg = f"{item_info['level']}-{item_info['name']}\n"
            msg += f"效果：{get_sub_info_msg(auction_id)[1]}"
            
    if _type == "神物":
        msg = f"{item_info['name']}\n"
        msg += f"效果：{item_info['desc']}"

    if _type == "丹药":
        msg = f"{item_info['name']}\n"
        msg += f"效果：{item_info['desc']}"

    return msg
