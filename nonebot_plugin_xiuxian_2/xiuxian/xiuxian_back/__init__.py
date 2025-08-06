import asyncio
import random
import re
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

check_item_effect = on_command("查看效果", aliases={"查看物品"}, priority=5, block=True)
goods_re_root = on_command("炼金", priority=6, block=True)
# 坊市查看指令
shop_view = on_command("坊市查看", priority=5, permission=GROUP, block=True)
auction_view = on_command("拍卖品查看", aliases={"查看拍卖品"}, priority=8, permission=GROUP, block=True)
shop_added = on_command("坊市上架", priority=10, permission=GROUP, block=True)
shop_added_by_admin = on_command("系统坊市上架", priority=5, permission=SUPERUSER, block=True)
shop_off = on_command("坊市下架", priority=5, permission=GROUP, block=True)
shop_off_all = on_fullmatch("清空坊市", priority=3, permission=SUPERUSER, block=True)
main_back = on_command('我的背包', aliases={'我的物品'}, priority=10, block=True)
yaocai_back = on_command('药材背包', priority=10, block=True)
danyao_back = on_command('丹药背包', priority=10, block=True)
use = on_command("使用", priority=15, block=True)
no_use_zb = on_command("换装", priority=5, block=True)
buy = on_command("坊市购买", priority=5, permission=GROUP, block=True)
auction_added = on_command("提交拍卖品", aliases={"拍卖品提交"}, priority=10, permission=GROUP, block=True)
auction_withdraw = on_command("撤回拍卖品", aliases={"拍卖品撤回"}, priority=10, permission=GROUP, block=True)
set_auction = on_command("拍卖会", priority=4, permission=GROUP and (SUPERUSER | GROUP_ADMIN | GROUP_OWNER), block=True)
creat_auction = on_fullmatch("举行拍卖会", priority=5, permission=GROUP and SUPERUSER, block=True)
offer_auction = on_command("拍卖", priority=5, permission=GROUP, block=True)
back_help = on_command("背包帮助", aliases={"坊市帮助"}, priority=8, block=True)
xiuxian_sone = on_fullmatch("灵石", priority=4, block=True)
chakan_wupin = on_command("查看修仙界物品", priority=25, block=True)
# 仙肆查看
xiuxian_shop_view = on_command("仙肆查看", priority=5, block=True)
# 仙肆上架（仅限超管）
xian_shop_added_by_admin = on_command("系统仙肆上架", priority=5, permission=SUPERUSER, block=True)
# 仙肆上架（用户权限）
xian_shop_add = on_command("仙肆上架", priority=5, block=True)
# 仙肆下架（用户和超管权限）
xian_shop_remove = on_command("仙肆下架", priority=5, block=True)
# 仙肆购买
xian_buy = on_command("仙肆购买", priority=5, block=True)
# 我的坊市
my_shop = on_command("我的坊市", priority=5, permission=GROUP, block=True)
# 我的仙肆
my_xian_shop = on_command("我的仙肆", priority=5, permission=GROUP, block=True)

__back_help__ = f"""
修仙交易系统帮助

【背包管理】
🔹 我的背包 [页码] - 查看背包物品
🔹 药材背包 [页码] - 查看药材类物品
🔹 丹药背包 [页码] - 查看丹药类物品
🔹 使用+物品名 [数量] - 使用物品
🔹 换装+装备名 - 卸下装备
🔹 炼金+物品名 [数量] - 将物品转化为灵石

【坊市交易】（群内）
🔸 坊市查看 [类型] [页码] - 查看群坊市
  ▶ 支持类型：技能|装备|丹药|药材
🔸 坊市上架 物品 金额 [数量] - 上架物品
  ▶ 最低金额50万灵石，手续费10-30%
🔸 坊市购买 编号 [数量] - 购买物品
🔸 坊市下架 编号 - 下架自己的物品

【仙肆交易】（全服）
🔸 仙肆查看 [类型] [页码] - 查看全服仙肆
🔸 仙肆上架 物品 金额 [数量] - 上架物品
🔸 仙肆购买 编号 [数量] - 购买物品
🔸 仙肆下架 编号 - 下架自己的物品

【拍卖会】
🎫 查看拍卖品 - 查看待拍卖物品
🎫 提交拍卖品 物品 底价 [数量] - 提交拍卖
🎫 拍卖+金额 - 参与竞拍
🎫 撤回拍卖品 编号 - 撤回自己的拍卖品

【其他功能】
🔍 查看效果+物品名 - 查看物品详情
📜 查看修仙界物品+类型 [页码] 
  ▶ 支持类型：功法|神通|丹药|法器|防具等
💎 灵石 - 查看当前灵石数量

【系统规则】
⏰ 每日{auction_time_config['hours']}点自动举行拍卖会
💰 手续费规则：
  - 500万以下：10%
  - 500-1000万：15% 
  - 1000-2000万：20%
  - 2000万以上：30%

输入具体指令查看详细用法，祝道友交易愉快！
""".strip()


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
async def back_help_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, session_id: int = CommandObjectID()):
    """背包帮助"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)

    # 检查用户是否已注册修仙
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg)
        await back_help.finish()
    else:
        msg = __back_help__
        await handle_send(bot, event, msg)
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


buy_lock = asyncio.Lock()

@shop_view.handle(parameterless=[Cooldown(at_sender=False)])
async def shop_view_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args=CommandArg()):
    """坊市查看 [类型]"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    group_id = str(event.group_id)

    # 检查用户是否注册修仙
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg)
        await shop_view.finish()

    # 获取用户输入的类型
    arg_list = args.extract_plain_text().split()
    input_type = arg_list[0]
    if not input_type:
        msg = "请输入要查看的类型，例如：坊市查看 技能|装备|丹药|药材"
        await handle_send(bot, event, msg)
        await xiuxian_shop_view.finish()

    # 获取商店数据
    shop_data = get_shop_data(group_id) 
    if not shop_data or shop_data.get(group_id) == {}:
        msg = "坊市目前空空如也！"
        await handle_send(bot, event, msg)
        await xiuxian_shop_view.finish()

    # 根据类型过滤商店数据
    data_list = []
    for k, v in shop_data["000000"].items():
        if v["goods_type"] == input_type:  # 使用 goods_type 进行精确匹配
            msg = f"编号：{k}\n"
            msg += f"{v['desc']}\n"
            msg += f"价格：{v['price']}枚灵石\n"
            if v['user_id'] != 0:
                msg += f"拥有人：{v['user_name']}道友\n"
                msg += f"数量：{v['stock']}\n"
            else:
                msg += f"系统出售\n"
                msg += f"数量：无限\n"
            data_list.append(msg)

    if not data_list:
        msg = f"坊市中暂无 {input_type} 类型的物品！"
        await handle_send(bot, event, msg)
        await xiuxian_shop_view.finish()

    msg_list = data_list
    try:
        # 直接从消息中提取数字作为页码
        current_page = int(arg_list[1]) if arg_list[1] is not None else 1
    except (IndexError, ValueError, TypeError):
        current_page = 1  # 默认第一页
    
    per_page = 31  # 每页10个物品
    total_items = len(msg_list)
    total_pages = (total_items + per_page - 1) // per_page
    
    # 页码有效性检查
    if current_page < 1 or current_page > total_pages:
        msg = f"@{event.sender.nickname}\n页码错误，有效范围为1~{total_pages}页！"
        await handle_send(bot, event, msg)
        await main_back.finish()
    
    # 计算当前页数据范围
    start_index = (current_page - 1) * per_page
    end_index = start_index + per_page
    paged_items = msg_list[start_index:end_index]
    # 构建消息内容
    msgs = f"坊市 - {input_type}"
    header = f"{msgs}（第{current_page}/{total_pages}页）"
    footer = f"提示：发送 坊市查看{input_type}+页码 查看其他页（共{total_pages}页）"
    final_msg = [header, *paged_items, footer]
    msg = final_msg
    
    # 发送消息处理
    await send_msg_handler(bot, event, f'坊市 - {input_type}', bot.self_id, msg)
    await shop_view.finish()
   

@xiuxian_shop_view.handle(parameterless=[Cooldown(at_sender=False)])
async def xiuxian_shop_view_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args=CommandArg()):
    """仙肆查看 [类型]"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    group_id = "000000"

    # 检查用户是否注册修仙
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg)
        await xiuxian_shop_view.finish()

    # 获取用户输入的类型
    arg_list = args.extract_plain_text().split()
    input_type = arg_list[0]
    if not input_type:
        msg = "请输入要查看的类型，例如：仙肆查看 技能|装备|丹药|药材"
        await handle_send(bot, event, msg)
        await xiuxian_shop_view.finish()

    # 获取商店数据
    shop_data = get_shop_data("000000")  # 使用全服 group_id
    if not shop_data or shop_data.get("000000") == {}:
        msg = "仙肆目前空空如也！"
        await handle_send(bot, event, msg)
        await xiuxian_shop_view.finish()

    # 根据类型过滤商店数据
    data_list = []
    for k, v in shop_data["000000"].items():
        if v["goods_type"] == input_type:  # 使用 goods_type 进行精确匹配
            msg = f"编号：{k}\n"
            msg += f"{v['desc']}\n"
            msg += f"价格：{v['price']}枚灵石\n"
            if v['user_id'] != 0:
                msg += f"拥有人：{v['user_name']}道友\n"
                msg += f"数量：{v['stock']}\n"
            else:
                msg += f"系统出售\n"
                msg += f"数量：无限\n"
            data_list.append(msg)

    if not data_list:
        msg = f"仙肆中暂无 {input_type} 类型的物品！"
        await handle_send(bot, event, msg)
        await xiuxian_shop_view.finish()

    msg_list = data_list
    try:
        # 直接从消息中提取数字作为页码
        current_page = int(arg_list[1]) if arg_list[1] is not None else 1
    except (IndexError, ValueError, TypeError):
        current_page = 1  # 默认第一页
    
    per_page = 31  # 每页10个物品
    total_items = len(msg_list)
    total_pages = (total_items + per_page - 1) // per_page
    
    # 页码有效性检查
    if current_page < 1 or current_page > total_pages:
        msg = f"@{event.sender.nickname}\n页码错误，有效范围为1~{total_pages}页！"
        await handle_send(bot, event, msg)
        await main_back.finish()
    
    # 计算当前页数据范围
    start_index = (current_page - 1) * per_page
    end_index = start_index + per_page
    paged_items = msg_list[start_index:end_index]
    # 构建消息内容
    msgs = f"仙肆 - {input_type}"
    header = f"{msgs}（第{current_page}/{total_pages}页）"
    footer = f"提示：发送 仙肆查看{input_type}+页码 查看其他页（共{total_pages}页）"
    final_msg = [header, *paged_items, footer]
    msg = final_msg
    
    # 发送消息处理
    await send_msg_handler(bot, event, f'仙肆 - {input_type}', bot.self_id, msg)
    await xiuxian_shop_view.finish()
    
        
@xian_shop_add.handle(parameterless=[Cooldown(1.4, at_sender=False, isolate_level=CooldownIsolateLevel.GROUP, parallel=1)])
async def xian_shop_add_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """仙肆上架"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    group_id = "000000"
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await shop_added.finish()
    user_id = user_info['user_id']
    user_stone_num = user_info['stone']
    args = args.extract_plain_text().split()
    goods_name = args[0] if len(args) > 0 else None
    price_str = args[1] if len(args) > 1 else "500000"  # 默认为500000
    quantity_str = args[2] if len(args) > 2 else "1"  # 默认为1
    if len(args) == 0:
        # 没有输入任何参数
        msg = "请输入正确指令！例如：坊市上架 物品 可选参数为(金额 数量)"
        await handle_send(bot, event, msg)
        await shop_added.finish()
    elif len(args) == 1:
        # 只提供了物品名称
        goods_name, price_str = args[0], "500000"
        quantity_str = "1"
    elif len(args) == 2:
        # 提供了物品名称和价格
        goods_name, price_str = args[0], args[1]
        quantity_str = "1"
    else:
        # 提供了物品名称、价格和数量
        goods_name, price_str, quantity_str = args[0], args[1], args[2]

    back_msg = sql_message.get_back_msg(user_id)  # 背包sql信息,dict
    if back_msg is None:
        msg = "道友的背包空空如也！"
        await handle_send(bot, event, msg)
        await shop_added.finish()
    in_flag = False  # 判断指令是否正确，道具是否在背包内
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
        await shop_added.finish()
    price = None
    
    # 解析价格
    try:
        price = int(price_str)
        if price <= 0:
            raise ValueError("价格必须为正数！")
    except ValueError as e:
        msg = f"请输入正确的金额: {str(e)}"
        await handle_send(bot, event, msg)
        await shop_added.finish()
    # 解析数量
    try:
        quantity = int(quantity_str)
        if quantity <= 0 or quantity > goods_num:  # 检查指定的数量是否合法
            raise ValueError("数量必须为正数或者小于等于你拥有的物品数!")
    except ValueError as e:
        msg = f"请输入正确的数量: {str(e)}"
        await handle_send(bot, event, msg)
        await shop_added.finish()
    price = max(price, 500000)  # 最低价格为50w
    if goods_type == "装备" and int(goods_state) == 1 and int(goods_num) == 1:
        msg = f"装备：{goods_name}已经被道友装备在身，无法上架！"
        await handle_send(bot, event, msg)
        await shop_added.finish()

    if price <= 5000000:
        rate = 0.1
    elif price <= 10000000:
        rate = 0.15
    elif price <= 20000000:
        rate = 0.2
    else:
        rate = 0.3

    give_stone_num = max(int(price * rate), 5000)
    if int(give_stone_num) > int(user_stone_num):
        msg = f"道友的手续费不够，请重新输入！"
        await handle_send(bot, event, msg)
        await shop_added.finish()
                
    if int(goods_num) <= int(goods_bind_num):
        msg = "该物品是绑定物品，无法上架！"
        await handle_send(bot, event, msg)
        await shop_added.finish()
    if goods_type == "聚灵旗" or goods_type == "炼丹炉":
        if user_info['root'] == "凡人" :
            pass
        else:
            msg = "道友职业无法上架！"
            await handle_send(bot, event, msg)
            await shop_added.finish() 
    shop_data = get_shop_data(group_id)

    num = 0
    for k, v in shop_data[group_id].items():
        if str(v['user_id']) == str(user_info['user_id']):
            num += 1
        else:
            pass
    if num >= 5 :
        msg = "每人只可上架五个物品！"
        await handle_send(bot, event, msg)
        await shop_added.finish()

    if shop_data == {}:
        shop_data["000000"] = {}
    id_ = len(shop_data["000000"]) + 1
    shop_data["000000"][id_] = {
        'user_id': user_id,
        'goods_name': goods_name,
        'goods_id': goods_id,
        'goods_type': goods_type,
        'desc': get_item_msg(goods_id),
        'price': price,
        'user_name': user_info['user_name'],
        'stock': quantity,  # 物品数量
    }
    sql_message.update_back_j(user_id, goods_id, num = quantity)
    sql_message.update_ls(user_id, give_stone_num, 2)
    save_shop(shop_data)
    msg = f"物品：{goods_name}成功上架仙肆，金额：{price}枚灵石，仙肆收取手续费：{give_stone_num}，数量{quantity}！"
    await handle_send(bot, event, msg)
    await shop_added.finish()
    

@xian_shop_remove.handle(parameterless=[Cooldown(1.4, at_sender=False, isolate_level=CooldownIsolateLevel.GROUP, parallel=1)])
async def xian_shop_remove_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """仙肆下架"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    group_id = "000000"
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await xian_shop_remove.finish()

    user_id = user_info["user_id"]
    is_superuser = await SUPERUSER(bot, event)  # 检查是否为超管
    shop_data = get_shop_data("000000")

    if shop_data["000000"] == {}:
        msg = "仙肆目前空空如也，无需下架！"
        await handle_send(bot, event, msg)
        await xian_shop_remove.finish()

    arg = args.extract_plain_text().strip()
    if not arg:
        msg = "请输入要下架的物品编号！例如：仙肆下架 1"
        await handle_send(bot, event, msg)
        await xian_shop_remove.finish()

    try:
        item_id = str(int(arg))
        if item_id not in shop_data["000000"]:
            msg = f"仙肆中不存在编号为 {item_id} 的物品！"
            await handle_send(bot, event, msg)
            await xian_shop_remove.finish()

        # 获取物品信息
        goods_info = shop_data["000000"][item_id]
        goods_name = goods_info["goods_name"]
        goods_owner_id = goods_info["user_id"]
        goods_id = goods_info["goods_id"]
        goods_quantity = goods_info.get("stock", 1)

        # 权限检查
        if not is_superuser and str(goods_owner_id) != str(user_id):
            msg = "你只能下架自己上架的物品！"
            await handle_send(bot, event, msg)
            await xian_shop_remove.finish()

        # 如果是用户自己下架且不是系统物品，则退还物品到背包
        if goods_owner_id != 0 and str(goods_owner_id) == str(user_id):
            sql_message.send_back(user_id, goods_id, goods_name, goods_info["goods_type"], goods_quantity)
            msg = f"已成功下架物品：{goods_name}，{goods_quantity} 个已退回你的背包！"
        else:
            msg = f"已成功下架物品：{goods_name}！"

        # 删除物品并重置编号
        del shop_data["000000"][item_id]
        shop_data["000000"] = reset_dict_num(shop_data["000000"])
        save_shop(shop_data)

        await handle_send(bot, event, msg)
    except ValueError:
        msg = "请输入有效的物品编号！"
        await handle_send(bot, event, msg)

    await xian_shop_remove.finish()
        

@xian_shop_added_by_admin.handle(parameterless=[Cooldown(1.4, at_sender=False, isolate_level=CooldownIsolateLevel.GROUP, parallel=1)])
async def xian_shop_added_by_admin_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """系统上架仙肆"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    group_id = "000000"
    args = args.extract_plain_text().split()
    if not args:
        msg = "请输入正确指令！例如：系统仙肆上架 物品 金额"
        await handle_send(bot, event, msg)
        await xian_shop_added_by_admin.finish()
    goods_name = args[0]
    goods_id = -1
    for k, v in items.items.items():
        if goods_name == v['name']:
            goods_id = k
            break
    if goods_id == -1:
        msg = f"不存在物品：{goods_name}的信息，请检查名字是否输入正确！"
        await handle_send(bot, event, msg)
        await xian_shop_added_by_admin.finish()
    price = None
    try:
        price = int(args[1])
        if price < 0:
            msg = "请不要设置负数！"
            await handle_send(bot, event, msg)
            await xian_shop_added_by_admin.finish()
    except:
        msg = "请输入正确的金额！"
        await handle_send(bot, event, msg)
        await xian_shop_added_by_admin.finish()

    shop_data = get_shop_data("000000")  # 使用全服 group_id
    goods_info = items.get_data_by_item_id(goods_id)

    id_ = len(shop_data["000000"]) + 1
    shop_data["000000"][id_] = {}
    shop_data["000000"][id_]['user_id'] = 0
    shop_data["000000"][id_]['goods_name'] = goods_name
    shop_data["000000"][id_]['goods_id'] = goods_id
    shop_data["000000"][id_]['goods_type'] = goods_info['type']
    shop_data["000000"][id_]['desc'] = get_item_msg(goods_id)
    shop_data["000000"][id_]['price'] = price
    shop_data["000000"][id_]['user_name'] = '系统'
    save_shop(shop_data)
    msg = f"物品：{goods_name}成功上架仙肆，金额：{price}枚灵石！"
    await handle_send(bot, event, msg)
    await xian_shop_added_by_admin.finish()

@xian_buy.handle(parameterless=[Cooldown(1.4, at_sender=False, isolate_level=CooldownIsolateLevel.GROUP)])
async def xian_buy_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """仙肆购买"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    group_id = "000000"
    async with buy_lock:
        isUser, user_info, msg = check_user(event)
        if not isUser:
            await handle_send(bot, event, msg)
            await xian_buy.finish()
        user_id = user_info['user_id']
        shop_data = get_shop_data("000000")  # 使用全服 group_id
        
        if shop_data["000000"] == {}:
            msg = "仙肆目前空空如也！"
            await handle_send(bot, event, msg)
            await xian_buy.finish()
        input_args = args.extract_plain_text().strip().split()
        if len(input_args) < 1:
            msg = "请输入正确指令！例如：仙肆购买 物品编号 数量"
            await handle_send(bot, event, msg)
            await xian_buy.finish()
        try:
            arg = int(input_args[0])
            goods_info = shop_data["000000"].get(str(arg))
            if not goods_info:
                raise ValueError("编号对应的商品不存在！")
            purchase_quantity = int(input_args[1]) if len(input_args) > 1 else 1
            if purchase_quantity <= 0:
                raise ValueError("购买数量必须是正数！")
            if 'stock' in goods_info and purchase_quantity > goods_info['stock']:
                raise ValueError("购买数量超过库存限制！")
        except ValueError as e:
            msg = f"{str(e)}"
            await handle_send(bot, event, msg)
            await xian_buy.finish()
        
        shop_user_id = shop_data["000000"][str(arg)]['user_id']
        goods_price = goods_info['price'] * purchase_quantity
        goods_stock = goods_info.get('stock', 1)
        if user_info['stone'] < goods_price:
            msg = '没钱还敢来买东西！！'
        elif int(user_id) == int(shop_user_id):
            msg = "道友自己的东西就不要自己购买啦！"
        elif purchase_quantity > goods_stock and shop_user_id != 0:
            msg = "库存不足，无法购买所需数量！"
        else:
            shop_goods_name = shop_data["000000"][str(arg)]['goods_name']
            shop_user_name = shop_data["000000"][str(arg)]['user_name']
            shop_goods_id = shop_data["000000"][str(arg)]['goods_id']
            shop_goods_type = shop_data["000000"][str(arg)]['goods_type']
            sql_message.update_ls(user_id, goods_price, 2)
            sql_message.send_back(user_id, shop_goods_id, shop_goods_name, shop_goods_type, purchase_quantity)
            save_shop(shop_data)
            if shop_user_id == 0:  # 系统出售
                msg = f"道友成功购买{purchase_quantity}个{shop_goods_name}，消耗灵石{goods_price}枚！"
            else:
                goods_info['stock'] -= purchase_quantity
                if goods_info['stock'] <= 0:
                    del shop_data["000000"][str(arg)]
                else:
                    shop_data["000000"][str(arg)] = goods_info
                
                give_stone = goods_price
                msg = f"道友成功购买{purchase_quantity}个{shop_user_name}道友寄售的{shop_goods_name}，消耗灵石{goods_price}枚！"
                sql_message.update_ls(shop_user_id, give_stone, 1)
            shop_data["000000"] = reset_dict_num(shop_data["000000"])
            save_shop(shop_data)
        await handle_send(bot, event, msg)
        await xian_buy.finish()
        
@buy.handle(parameterless=[Cooldown(1.4, at_sender=False, isolate_level=CooldownIsolateLevel.GROUP)])
async def buy_(bot: Bot, event: GroupMessageEvent, args: Message = CommandArg()):
    """购物"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    group_id = str(event.group_id)
    async with buy_lock:
        isUser, user_info, msg = check_user(event)
        if not isUser:
            await handle_send(bot, event, msg)
            await buy.finish()
        user_id = user_info['user_id']
        group_id = str(event.group_id)
        shop_data = get_shop_data(group_id)
        
        if shop_data[group_id] == {}:
            msg = "坊市目前空空如也！"
            await handle_send(bot, event, msg)
            await buy.finish()
        input_args = args.extract_plain_text().strip().split()
        if len(input_args) < 1:
            # 没有输入任何参数
            msg = "请输入正确指令！例如：坊市购买 物品编号 数量"
            await handle_send(bot, event, msg)
            await buy.finish()
        else:
            try:
                arg = int(input_args[0])
                if len(input_args) == 0:
                    msg = "请输入正确指令！例如：坊市购买 物品编号 数量"

                goods_info = shop_data[group_id].get(str(arg))
                if not goods_info:
                    raise ValueError("编号对应的商品不存在！")

                purchase_quantity = int(input_args[1]) if len(input_args) > 1 else 1
                if purchase_quantity <= 0:
                    raise ValueError("购买数量必须是正数！")
    
                if 'stock' in goods_info and purchase_quantity > goods_info['stock']:
                    raise ValueError("购买数量超过库存限制！")
            except ValueError as e:
                msg = f"{str(e)}"
                await handle_send(bot, event, msg)
                await buy.finish()
        shop_user_id = shop_data[group_id][str(arg)]['user_id']
        goods_price = goods_info['price'] * purchase_quantity
        goods_stock = goods_info.get('stock', 1)
        if user_info['stone'] < goods_price:
            msg = '没钱还敢来买东西！！'
            await handle_send(bot, event, msg)
            await buy.finish()
        elif int(user_id) == int(shop_data[group_id][str(arg)]['user_id']):
            msg = "道友自己的东西就不要自己购买啦！"
            await handle_send(bot, event, msg)
            await buy.finish()
        elif purchase_quantity > goods_stock and shop_user_id != 0:
            msg = "库存不足，无法购买所需数量！"
            await handle_send(bot, event, msg)
        else:
            shop_goods_name = shop_data[group_id][str(arg)]['goods_name']
            shop_user_name = shop_data[group_id][str(arg)]['user_name']
            shop_goods_id = shop_data[group_id][str(arg)]['goods_id']
            shop_goods_type = shop_data[group_id][str(arg)]['goods_type']
            sql_message.update_ls(user_id, goods_price, 2)
            sql_message.send_back(user_id, shop_goods_id, shop_goods_name, shop_goods_type, purchase_quantity)
            save_shop(shop_data)

            if shop_user_id == 0:  # 0为系统
                msg = f"道友成功购买{purchase_quantity}个{shop_goods_name}，消耗灵石{goods_price}枚！"
            else:
                goods_info['stock'] -= purchase_quantity
                if goods_info['stock'] <= 0:
                    del shop_data[group_id][str(arg)]  # 库存为0，移除物品
                else:
                    shop_data[group_id][str(arg)] = goods_info
                service_charge = int(goods_price * 0.1)  # 手续费10%
                give_stone = goods_price - service_charge
                msg = f"道友成功购买{purchase_quantity}个{shop_user_name}道友寄售的{shop_goods_name}，消耗灵石{goods_price}枚！"
                sql_message.update_ls(shop_user_id, give_stone, 1)
            shop_data[group_id] = reset_dict_num(shop_data[group_id])
            save_shop(shop_data)
            await handle_send(bot, event, msg)
            await buy.finish()


@shop_added_by_admin.handle(parameterless=[Cooldown(1.4, at_sender=False, isolate_level=CooldownIsolateLevel.GROUP, parallel=1)])
async def shop_added_by_admin_(bot: Bot, event: GroupMessageEvent, args: Message = CommandArg()):
    """系统上架坊市"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    group_id = str(event.group_id)
    args = args.extract_plain_text().split()
    if not args:
        msg = "请输入正确指令！例如：系统坊市上架 物品 金额"
        await handle_send(bot, event, msg)
        await shop_added_by_admin.finish()
    goods_name = args[0]
    goods_id = -1
    for k, v in items.items.items():
        if goods_name == v['name']:
            goods_id = k
            break
        else:
            continue
    if goods_id == -1:
        msg = "不存在物品：{goods_name}的信息，请检查名字是否输入正确！"
        await handle_send(bot, event, msg)
        await shop_added_by_admin.finish()
    price = None
    try:
        price = args[1]
    except LookupError:
        msg = "请输入正确指令！例如：系统坊市上架 物品 金额"
        await handle_send(bot, event, msg)
        await shop_added_by_admin.finish()
    try:
        price = int(price)
        if price < 0:
            msg = "请不要设置负数！"
            await handle_send(bot, event, msg)
            await shop_added_by_admin.finish()
    except LookupError:
        msg = "请输入正确的金额！"
        await handle_send(bot, event, msg)
        await shop_added_by_admin.finish()

    try:
        var = args[2]
        msg = "请输入正确指令！例如：系统坊市上架 物品 金额"
        await handle_send(bot, event, msg)
        await shop_added_by_admin.finish()
    except LookupError:
        pass

    group_id = str(event.group_id)
    shop_data = get_shop_data(group_id)
    if shop_data == {}:
        shop_data[group_id] = {}
    goods_info = items.get_data_by_item_id(goods_id)

    id_ = len(shop_data[group_id]) + 1
    shop_data[group_id][id_] = {}
    shop_data[group_id][id_]['user_id'] = 0
    shop_data[group_id][id_]['goods_name'] = goods_name
    shop_data[group_id][id_]['goods_id'] = goods_id
    shop_data[group_id][id_]['goods_type'] = goods_info['type']
    shop_data[group_id][id_]['desc'] = get_item_msg(goods_id)
    shop_data[group_id][id_]['price'] = price
    shop_data[group_id][id_]['user_name'] = '系统'
    save_shop(shop_data)
    msg = f"物品：{goods_name}成功上架坊市，金额：{price}枚灵石！"
    await handle_send(bot, event, msg)
    await shop_added_by_admin.finish()


@shop_added.handle(parameterless=[Cooldown(1.4, at_sender=False, isolate_level=CooldownIsolateLevel.GROUP)])
async def shop_added_(bot: Bot, event: GroupMessageEvent, args: Message = CommandArg()):
    """用户上架坊市"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    group_id = str(event.group_id)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await shop_added.finish()
    user_id = user_info['user_id']
    user_stone_num = user_info['stone']
    args = args.extract_plain_text().split()
    goods_name = args[0] if len(args) > 0 else None
    price_str = args[1] if len(args) > 1 else "500000"  # 默认为500000
    quantity_str = args[2] if len(args) > 2 else "1"  # 默认为1
    if len(args) == 0:
        # 没有输入任何参数
        msg = "请输入正确指令！例如：坊市上架 物品 可选参数为(金额 数量)"
        await handle_send(bot, event, msg)
        await shop_added.finish()
    elif len(args) == 1:
        # 只提供了物品名称
        goods_name, price_str = args[0], "500000"
        quantity_str = "1"
    elif len(args) == 2:
        # 提供了物品名称和价格
        goods_name, price_str = args[0], args[1]
        quantity_str = "1"
    else:
        # 提供了物品名称、价格和数量
        goods_name, price_str, quantity_str = args[0], args[1], args[2]

    back_msg = sql_message.get_back_msg(user_id)  # 背包sql信息,dict
    if back_msg is None:
        msg = "道友的背包空空如也！"
        await handle_send(bot, event, msg)
        await shop_added.finish()
    in_flag = False  # 判断指令是否正确，道具是否在背包内
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
        await shop_added.finish()
    price = None
    
    # 解析价格
    try:
        price = int(price_str)
        if price <= 0:
            raise ValueError("价格必须为正数！")
    except ValueError as e:
        msg = f"请输入正确的金额: {str(e)}"
        await handle_send(bot, event, msg)
        await shop_added.finish()
    # 解析数量
    try:
        quantity = int(quantity_str)
        if quantity <= 0 or quantity > goods_num:  # 检查指定的数量是否合法
            raise ValueError("数量必须为正数或者小于等于你拥有的物品数!")
    except ValueError as e:
        msg = f"请输入正确的数量: {str(e)}"
        await handle_send(bot, event, msg)
        await shop_added.finish()
    price = max(price, 500000)  # 最低价格为50w
    if goods_type == "装备" and int(goods_state) == 1 and int(goods_num) == 1:
        msg = f"装备：{goods_name}已经被道友装备在身，无法上架！"
        await handle_send(bot, event, msg)
        await shop_added.finish()
        
    if price <= 5000000:
        rate = 0.1
    elif price <= 10000000:
        rate = 0.15
    elif price <= 20000000:
        rate = 0.2
    else:
        rate = 0.3

    give_stone_num = max(int(price * rate), 5000)
    if int(give_stone_num) > int(user_stone_num):
        msg = f"道友的手续费不够，请重新输入！"
        await handle_send(bot, event, msg)
        await shop_added.finish()
    
    if int(goods_num) <= int(goods_bind_num):
        msg = "该物品是绑定物品，无法上架！"
        await handle_send(bot, event, msg)
        await shop_added.finish()
    if goods_type == "聚灵旗" or goods_type == "炼丹炉":
        if user_info['root'] == "凡人" :
            pass
        else:
            msg = "道友职业无法上架！"
            await handle_send(bot, event, msg)
            await shop_added.finish() 

    group_id = str(event.group_id)
    shop_data = get_shop_data(group_id)

    num = 0
    for k, v in shop_data[group_id].items():
        if str(v['user_id']) == str(user_info['user_id']):
            num += 1
        else:
            pass
    if num >= 5 :
        msg = "每人只可上架五个物品！"
        await handle_send(bot, event, msg)
        await shop_added.finish()

    if shop_data == {}:
        shop_data[group_id] = {}
    id_ = len(shop_data[group_id]) + 1
    shop_data[group_id][id_] = {
        'user_id': user_id,
        'goods_name': goods_name,
        'goods_id': goods_id,
        'goods_type': goods_type,
        'desc': get_item_msg(goods_id),
        'price': price,
        'user_name': user_info['user_name'],
        'stock': quantity,  # 物品数量
    }
    sql_message.update_back_j(user_id, goods_id, num = quantity)
    sql_message.update_ls(user_id, give_stone_num, 2)
    save_shop(shop_data)
    msg = f"物品：{goods_name}成功上架坊市，金额：{price}枚灵石，坊市收取手续费：{give_stone_num}，数量{quantity}！"
    await handle_send(bot, event, msg)
    await shop_added.finish()


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
    price = int((convert_rank('江湖好手')[0] + 5) * 100000 - get_item_msg_rank(goods_id) * 100000) * num
    if price <= 0:
        msg = f"物品：{goods_name}炼金失败，凝聚{price}枚灵石，记得通知晓楠！"
        await handle_send(bot, event, msg)
        await goods_re_root.finish()

    sql_message.update_back_j(user_id, goods_id, num=num)
    sql_message.update_ls(user_id, price, 1)
    msg = f"物品：{goods_name} 数量：{num} 炼金成功，凝聚{price}枚灵石！"
    await handle_send(bot, event, msg)
    await goods_re_root.finish()


@shop_off.handle(parameterless=[Cooldown(1.4, at_sender=False, isolate_level=CooldownIsolateLevel.GROUP, parallel=1)])
async def shop_off_(bot: Bot, event: GroupMessageEvent, args: Message = CommandArg()):
    """下架商品"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    group_id = str(event.group_id)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await shop_off.finish()
    user_id = user_info['user_id']
    group_id = str(event.group_id)
    shop_data = get_shop_data(group_id)
    if shop_data[group_id] == {}:
        msg = "坊市目前空空如也！"
        await handle_send(bot, event, msg)
        await shop_off.finish()

    arg = args.extract_plain_text().strip()
    shop_user_name = shop_data[group_id][str(arg)]['user_name']
    try:
        arg = int(arg)
        if arg <= 0 or arg > len(shop_data[group_id]):
            msg = "请输入正确的编号！"
            await handle_send(bot, event, msg)
            await shop_off.finish()
    except ValueError:
        msg = "请输入正确的编号！"
        await handle_send(bot, event, msg)
        await shop_off.finish()

    if shop_data[group_id][str(arg)]['user_id'] == user_id:
        sql_message.send_back(user_id, shop_data[group_id][str(arg)]['goods_id'],
                              shop_data[group_id][str(arg)]['goods_name'], shop_data[group_id][str(arg)]['goods_type'],
                              shop_data[group_id][str(arg)]['stock'])
        msg = f"成功下架物品：{shop_data[group_id][str(arg)]['goods_name']}！"
        del shop_data[group_id][str(arg)]
        shop_data[group_id] = reset_dict_num(shop_data[group_id])
        save_shop(shop_data)
        await handle_send(bot, event, msg)
        await shop_off.finish()

    elif event.sender.role == "admin" or event.sender.role == "owner" or event.get_user_id() in bot.config.superusers:
        if shop_data[group_id][str(arg)]['user_id'] == 0:  # 这么写为了防止bot.send发送失败，不结算
            msg = f"成功下架物品：{shop_data[group_id][str(arg)]['goods_name']}！"
            del shop_data[group_id][str(arg)]
            shop_data[group_id] = reset_dict_num(shop_data[group_id])
            save_shop(shop_data)
            await handle_send(bot, event, msg)
            await shop_off.finish()
        else:
            sql_message.send_back(shop_data[group_id][str(arg)]['user_id'], shop_data[group_id][str(arg)]['goods_id'],
                                  shop_data[group_id][str(arg)]['goods_name'],
                                  shop_data[group_id][str(arg)]['goods_type'], shop_data[group_id][str(arg)]['stock'])
            msg1 = f"道友上架的{shop_data[group_id][str(arg)]['stock']}个{shop_data[group_id][str(arg)]['goods_name']}已被管理员{user_info['user_name']}下架！"
            del shop_data[group_id][str(arg)]
            shop_data[group_id] = reset_dict_num(shop_data[group_id])
            save_shop(shop_data)
            try:
                await handle_send(bot, event, msg1)
            except ActionFailed:
                pass

    else:
        msg = "这东西不是你的！"
        await handle_send(bot, event, msg)
        await shop_off.finish()


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
        f"来自群{group_id}的{user_info['user_name']}道友拍卖：{price}枚灵石！" +
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

    msg = f"道友的拍卖品：{goods_name}成功提交，底价：{price}枚灵石，数量：{quantity}"
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
        msg = __back_help__
        await handle_send(bot, event, msg)
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
    valid_types = ["功法", "辅修功法", "神通", "身法", "瞳术", "丹药", "合成丹药", "法器", "防具"]
    
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
            msg = "请输入正确类型【功法|辅修功法|神通|身法|瞳术|丹药|合成丹药|法器|防具】！！！"
            await handle_send(bot, event, msg)
            await chakan_wupin.finish()
    
    # 获取物品数据
    item_data = items.get_data_by_item_type([item_type])
    msg_list = []
    
    for item_id, item_info in item_data.items():
        name = item_info['name']
        if item_type in ["功法", "辅修功法", "神通", "身法", "瞳术", "法器", "防具"]:
            desc = get_item_msg(item_id)
            msg = f"ID：{item_id}\n{desc}"
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


@shop_off_all.handle(parameterless=[Cooldown(60, isolate_level=CooldownIsolateLevel.GROUP, parallel=1)])
async def shop_off_all_(bot: Bot, event: GroupMessageEvent):
    """坊市清空"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await shop_off_all.finish()
    group_id = str(event.group_id)
    shop_data = get_shop_data(group_id)
    if shop_data[group_id] == {}:
        msg = "坊市目前空空如也！"
        await handle_send(bot, event, msg)
        await shop_off_all.finish()

    msg = "正在清空,稍等！"
    await handle_send(bot, event, msg)

    list_msg = []
    msg = ""
    num = len(shop_data[group_id])
    for x in range(num):
        x = num - x
        if shop_data[group_id][str(x)]['user_id'] == 0:  # 这么写为了防止bot.send发送失败，不结算
            msg += f"成功下架系统物品：{shop_data[group_id][str(x)]['goods_name']}!\n"
            del shop_data[group_id][str(x)]
            save_shop(shop_data)
        else:
            sql_message.send_back(shop_data[group_id][str(x)]['user_id'], shop_data[group_id][str(x)]['goods_id'],
                                  shop_data[group_id][str(x)]['goods_name'],
                                  shop_data[group_id][str(x)]['goods_type'], shop_data[group_id][str(x)]['stock'])
            msg += f"成功下架{shop_data[group_id][str(x)]['user_name']}的{shop_data[group_id][str(x)]['stock']}个{shop_data[group_id][str(x)]['goods_name']}!\n"
            del shop_data[group_id][str(x)]
            save_shop(shop_data)
    shop_data[group_id] = reset_dict_num(shop_data[group_id])
    save_shop(shop_data)
    list_msg.append(
                    {"type": "node", "data": {"name": "执行清空坊市ing", "uin": bot.self_id,
                                              "content": msg}})
    try:
        await send_msg_handler(bot, event, list_msg)
    except ActionFailed:
        await handle_send(bot, event, msg)
    await shop_off_all.finish()


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
