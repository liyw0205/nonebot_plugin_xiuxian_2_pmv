try:
    import ujson as json
except ImportError:
    import json
import random
from pathlib import Path
from datetime import datetime

from nonebot import on_command
from nonebot.log import logger
from nonebot.params import CommandArg

from ..adapter_compat import Bot, GroupMessageEvent, PrivateMessageEvent, Message
from ..xiuxian_utils.item_json import Items
from ..xiuxian_utils.lay_out import assign_bot, Cooldown
from ..xiuxian_utils.utils import check_user, handle_send, number_to, send_help_message
from ..xiuxian_utils.xiuxian2_handle import XiuxianDateManage
from .stone_limit import stone_limit

items = Items()
sql_message = XiuxianDateManage()

give_xiangyuan = on_command("送仙缘", priority=5, block=True)
get_xiangyuan = on_command("抢仙缘", priority=5, block=True)
xiangyuan_list = on_command("仙缘列表", priority=5, block=True)
xiangyuan_help = on_command("仙缘帮助", priority=15, block=True)


# 仙缘数据路径
XIANGYUAN_DATA_PATH = Path(__file__).parent / "xiangyuan_data"
XIANGYUAN_DATA_PATH.mkdir(parents=True, exist_ok=True)

# 仙缘配置
XIANGYUAN_MIN_STONE = 1000000      # 最少灵石
XIANGYUAN_MAX_STONE = 1000000000   # 最多灵石
XIANGYUAN_MIN_RECEIVERS = 1        # 最少人数
XIANGYUAN_MAX_RECEIVERS = 50       # 最多人数
XIANGYUAN_SEND_LIMIT = 3           # 每日送仙缘次数限制
XIANGYUAN_RECEIVE_LIMIT = 3        # 每日抢仙缘次数限制

def get_xiangyuan_data(group_id):
    """获取群仙缘数据"""
    file_path = XIANGYUAN_DATA_PATH / f"xiangyuan_{group_id}.json"
    try:
        if file_path.exists():
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                return data
    except:
        pass
    return {"gifts": {}, "last_id": 1}

def save_xiangyuan_data(group_id, data):
    """保存群仙缘数据"""
    file_path = XIANGYUAN_DATA_PATH / f"xiangyuan_{group_id}.json"
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

def get_user_name(user_id):
    """根据用户 ID 获取道号"""
    user_info = sql_message.get_user_info_with_id(user_id)
    return user_info['user_name'] if user_info else f"未知 ({user_id})"

def calculate_xiangyuan_reward(gift, is_last_receiver):
    """
    计算仙缘奖励
    - 如果是最后一位或只有 1 位：获得全部剩余灵石
    - 否则：等分剩余灵石的一半 + 随机分配另一半
    """
    remaining_stone = gift["remaining_stone"]
    receiver_count = gift["receiver_count"]
    received = gift["received"]
    remaining_receivers = receiver_count - received
    
    if is_last_receiver or receiver_count == 1:
        # 最后一位或只有一位，获得全部
        return remaining_stone
    else:
        # 等分一半 + 随机分配另一半
        half_stone = remaining_stone // 2
        base_reward = half_stone // remaining_receivers  # 等分部分
        random_pool = remaining_stone - half_stone  # 随机池
        
        # 随机分配部分 (0 到 random_pool/remaining_receivers*2 之间)
        if remaining_receivers > 0:
            random_reward = random.randint(0, int(random_pool / remaining_receivers * 2))
        else:
            random_reward = 0
        
        return base_reward + random_reward

def parse_xiangyuan_content(content_str: str, user_id: int):
    """
    支持以下写法：
    送仙缘 1000000 5
    送仙缘 灵石x1000000 5
    送仙缘 1000000x灵石 5
    送仙缘 灵石1000000 5
    送仙缘 1000000灵石,精铁符剑x2 5
    """
    stone_amount = 0
    items_list = []
    error_msg = ""

    # 替换中文逗号、顿号、全角空格 → 统一用英文逗号分割
    content_str = content_str.replace('，', ',').replace('、', ',').replace('　', ' ')
    parts = [p.strip() for p in content_str.split(',') if p.strip()]

    for part in parts:
        # ------------------- 尝试当作纯灵石 -------------------
        if part.isdigit() and int(part) > XIANGYUAN_MAX_RECEIVERS:  # 避免把人数误判
            try:
                num = int(part)
                if stone_amount > 0:
                    return 0, [], "出现了多个纯数字灵石段，请明确写'灵石x数量'"
                stone_amount = num
                continue
            except:
                pass

        # ------------------- 尝试 数量x物品 / 物品x数量 -------------------
        if 'x' in part.lower():
            left, right = part.split('x', 1)
            left = left.strip()
            right = right.strip()

            # 尝试 数量x物品
            if left.isdigit():
                qty_str, name = left, right
            # 尝试 物品x数量
            elif right.isdigit():
                name, qty_str = left, right
            else:
                continue  # 都不是数字×格式，跳过

            try:
                qty = int(qty_str)
                if qty < 1 or qty > 10:
                    return 0, [], f"物品数量需在1~10之间：{part}"
            except:
                continue

            name = name.strip()
            if not name:
                continue

            # 特殊处理纯“灵石”关键字
            if name in ("灵石", "stone", "ls", "灵石x", "灵石X"):
                if stone_amount > 0:
                    return 0, [], "出现了多个灵石段"
                stone_amount = qty
                continue

            # 其它物品
            goods_id, goods_info = items.get_data_by_item_name(name)
            if not goods_id:
                error_msg = f"物品不存在：{name}"
                continue

            item_type = goods_info.get('type', '')
            if item_type not in ['装备', '技能', '药材']:
                error_msg = f"仅允许赠送【装备/技能/药材】，{name}类型不符合"
                continue

            if goods_info.get('level') == '无上':
                error_msg = f"禁止赠送【无上】品阶物品：{name}"
                continue

            trade_num = sql_message.goods_num(user_id, goods_id, num_type='trade')
            if trade_num < qty:
                error_msg = f"{name} 可交易数量不足，仅剩{trade_num}个"
                continue

            items_list.append({
                'goods_id': goods_id,
                'name': name,
                'type': item_type,
                'quantity': qty
            })
            continue

        # ------------------- 尝试 灵石 + 纯数字 -------------------
        if '灵石' in part:
            num_part = part.replace('灵石', '').strip()
            if num_part.isdigit():
                num = int(num_part)
                if stone_amount > 0:
                    return 0, [], "出现了多个灵石段"
                stone_amount = num
                continue

    if stone_amount == 0 and not items_list:
        error_msg = "未解析到任何有效的灵石或物品"

    return stone_amount, items_list, error_msg

@give_xiangyuan.handle(parameterless=[Cooldown(cd_time=1.4)])
async def give_xiangyuan_(bot: Bot, event: GroupMessageEvent, args: Message = CommandArg()):
    """送仙缘"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg)
        await give_xiangyuan.finish()
    
    user_id = user_info["user_id"]
    group_id = str(event.group_id)
    args_text = args.extract_plain_text().strip()
    arg_list = args_text.split()
    
    # 检查每日送仙缘次数
    send_count = stone_limit.get_xiangyuan_send_count(user_id)
    if send_count >= XIANGYUAN_SEND_LIMIT:
        msg = f"道友今日已送{send_count}次仙缘，达到上限 ({XIANGYUAN_SEND_LIMIT}次/日)，明日再来吧！"
        await handle_send(bot, event, msg, md_type="修仙", k1="抢仙缘", v1="抢仙缘", k2="仙缘列表", v2="仙缘列表", k3="帮助", v3="仙缘帮助")
        await give_xiangyuan.finish()
    
    # 解析参数 (最后一个是人数，前面是内容)
    if len(arg_list) < 2:
        msg = "指令格式：送仙缘 内容 人数\n示例：送仙缘 1000000 5\n或：送仙缘 灵石x1000000,精铁符剑x1 5"
        await handle_send(bot, event, msg)
        await give_xiangyuan.finish()
    
    receiver_count_str = arg_list[-1]
    content_str = " ".join(arg_list[:-1])
    
    if not receiver_count_str.isdigit():
        msg = "人数必须是数字！"
        await handle_send(bot, event, msg)
        await give_xiangyuan.finish()
        
    receiver_count = int(receiver_count_str)
    receiver_count = max(XIANGYUAN_MIN_RECEIVERS, min(receiver_count, XIANGYUAN_MAX_RECEIVERS))
    
    # 解析内容
    stone_amount, items_list, error_msg = parse_xiangyuan_content(content_str, user_id)
    if error_msg:
        await handle_send(bot, event, error_msg)
        await give_xiangyuan.finish()
    
    # 校验灵石范围
    if stone_amount > 0:
        if stone_amount < XIANGYUAN_MIN_STONE:
            msg = f"灵石数量不能少于{number_to(XIANGYUAN_MIN_STONE)}！"
            await handle_send(bot, event, msg)
            await give_xiangyuan.finish()
        if stone_amount > XIANGYUAN_MAX_STONE:
            msg = f"灵石数量不能多于{number_to(XIANGYUAN_MAX_STONE)}！"
            await handle_send(bot, event, msg)
            await give_xiangyuan.finish()
        
        # 检查灵石是否足够
        if stone_amount > int(user_info['stone']):
            msg = f"道友的灵石不够，请重新输入！"
            await handle_send(bot, event, msg)
            await give_xiangyuan.finish()
    
    # 扣除资源
    if stone_amount > 0:
        sql_message.update_ls(user_id, stone_amount, 2)
    
    for item in items_list:
        sql_message.update_back_j(user_id, item['goods_id'], num=item['quantity'])
    
    # 创建仙缘记录
    xiangyuan_data = get_xiangyuan_data(group_id)
    xiangyuan_id = xiangyuan_data["last_id"]
    xiangyuan_data["last_id"] += 1
    
    xiangyuan_data["gifts"][str(xiangyuan_id)] = {
        "id": xiangyuan_id,
        "giver_id": user_id,
        "giver_name": user_info['user_name'],
        "stone_amount": stone_amount,
        "remaining_stone": stone_amount,
        "items": items_list,  # 新增物品列表
        "receiver_count": receiver_count,
        "received": 0,
        "receivers": [],
        "create_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    
    save_xiangyuan_data(group_id, xiangyuan_data)
    
    # 更新送仙缘次数
    stone_limit.update_xiangyuan_send_count(user_id)
    
    # 构建消息
    msg = f"✨【仙缘 #{xiangyuan_id}】✨\n"
    msg += f"送出者：{user_info['user_name']}\n"
    if stone_amount > 0:
        msg += f"灵石总额：{number_to(stone_amount)}\n"
    if items_list:
        item_desc = ", ".join([f"{i['name']}x{i['quantity']}" for i in items_list])
        msg += f"包含物品：{item_desc}\n"
    msg += f"可领取人数：{receiver_count}人\n"
    msg += f"今日剩余送仙缘次数：{XIANGYUAN_SEND_LIMIT - send_count - 1}次\n"
    msg += "\n同群道友可发送【抢仙缘】获取仙缘"
    
    await handle_send(bot, event, msg, md_type="修仙", k1="抢仙缘", v1="抢仙缘", k2="仙缘列表", v2="仙缘列表", k3="帮助", v3="仙缘帮助")
    await give_xiangyuan.finish()

@get_xiangyuan.handle(parameterless=[Cooldown(cd_time=1.4)])
async def get_xiangyuan_(bot: Bot, event: GroupMessageEvent):
    """抢仙缘"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg)
        await get_xiangyuan.finish()
    
    user_id = user_info["user_id"]
    group_id = str(event.group_id)
    
    # 检查每日抢仙缘次数
    receive_count = stone_limit.get_xiangyuan_receive_count(user_id)
    if receive_count >= XIANGYUAN_RECEIVE_LIMIT:
        msg = f"道友今日已抢{receive_count}次仙缘，达到上限 ({XIANGYUAN_RECEIVE_LIMIT}次/日)，明日再来吧！"
        await handle_send(bot, event, msg, md_type="修仙", k1="送仙缘", v1="送仙缘", k2="仙缘列表", v2="仙缘列表", k3="帮助", v3="仙缘帮助")
        await get_xiangyuan.finish()
    
    # 获取仙缘数据
    xiangyuan_data = get_xiangyuan_data(group_id)
    
    if not xiangyuan_data["gifts"]:
        msg = "当前没有可领取的仙缘！\n道友可以发送【送仙缘】来创建仙缘"
        await handle_send(bot, event, msg)
        await get_xiangyuan.finish()
    
    # 过滤可领取的仙缘
    available_gifts = []
    for gift_id, gift in xiangyuan_data["gifts"].items():
        if (gift["received"] < gift["receiver_count"] and
            user_id not in gift["receivers"]):
            available_gifts.append((gift_id, gift))
    
    if not available_gifts:
        msg = "没有可领取的仙缘了！\n所有仙缘都已被领取完毕，或没有符合领取条件的仙缘"
        await handle_send(bot, event, msg)
        await get_xiangyuan.finish()
    
    # 随机选择一个可领取的仙缘
    gift_id, gift = random.choice(available_gifts)
    
    # 判断是否是最后一位领取者
    is_last_receiver = (gift["received"] + 1) >= gift["receiver_count"]
    is_single_gift = gift["receiver_count"] == 1
    
    # 计算灵石奖励
    reward = calculate_xiangyuan_reward(gift, is_last_receiver)
    
    # 处理物品奖励 (先到先得，直到发完)
    received_items = []
    if gift.get("items"):
        for item in gift["items"]:
            if item["quantity"] > 0:
                # 发放物品
                sql_message.send_back(
                    user_id,
                    item["goods_id"],
                    item["name"],
                    item["type"],
                    1,
                    1
                )
                item["quantity"] -= 1
                received_items.append(f"{item['name']}x1")
    
    if reward <= 0 and not received_items:
        msg = f"仙缘 #{gift_id}\n（由【{gift['giver_name']}】送出）\n"
        msg += "可惜仙缘池已空，道友来晚了一步！"
        await handle_send(bot, event, msg)
        await get_xiangyuan.finish()
    
    # 更新仙缘数据
    gift["received"] += 1
    gift["remaining_stone"] -= reward
    gift["receivers"].append(user_id)
    
    # 保存更新后的仙缘数据
    xiangyuan_data["gifts"][gift_id] = gift
    save_xiangyuan_data(group_id, xiangyuan_data)
    
    # 发放灵石奖励
    if reward > 0:
        sql_message.update_ls(user_id, reward, 1)
    
    # 更新抢仙缘次数
    stone_limit.update_xiangyuan_receive_count(user_id)
    
    # 构建消息
    msg = f"🎉【仙缘 #{gift_id}】🎉\n"
    msg += f"（由【{gift['giver_name']}】送出）\n"
    msg += "═════════════\n"
    
    if reward > 0:
        if is_single_gift:
            msg += f"🎯 独享仙缘！\n"
            msg += f"获得灵石：{number_to(reward)}\n"
        elif is_last_receiver:
            msg += f"🏆 最后机会！\n"
            msg += f"获得剩余全部：{number_to(reward)}\n"
        else:
            remaining_receivers = gift["receiver_count"] - gift["received"]
            msg += f"💰 获得灵石：{number_to(reward)}\n"
            msg += f"剩余可领取：{remaining_receivers}人\n"
            msg += f"池中剩余：{number_to(gift['remaining_stone'])}灵石\n"
    
    if received_items:
        msg += f"🎁 获得物品：{', '.join(received_items)}\n"
    
    msg += "═════════════\n"
    msg += f"今日剩余抢仙缘次数：{XIANGYUAN_RECEIVE_LIMIT - receive_count - 1}次"
    
    await handle_send(bot, event, msg, md_type="修仙", k1="送仙缘", v1="送仙缘", k2="仙缘列表", v2="仙缘列表", k3="帮助", v3="仙缘帮助")
    await get_xiangyuan.finish()

@xiangyuan_list.handle(parameterless=[Cooldown(cd_time=1.4)])
async def xiangyuan_list_(bot: Bot, event: GroupMessageEvent):
    """仙缘列表"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg)
        await xiangyuan_list.finish()
    
    group_id = str(event.group_id)
    xiangyuan_data = get_xiangyuan_data(group_id)
    
    if not xiangyuan_data["gifts"]:
        msg = "当前没有仙缘可领取！\n发送【送仙缘 内容 人数】创建仙缘"
        await handle_send(bot, event, msg)
        await xiangyuan_list.finish()
    
    # 构建消息
    msg_parts = ["✨【本群仙缘列表】✨\n═════════════"]
    
    active_count = 0
    for gift_id, gift in xiangyuan_data["gifts"].items():
        if gift["received"] < gift["receiver_count"]:
            active_count += 1
            progress = f"{gift['received']}/{gift['receiver_count']}"
            
            content_desc = []
            if gift.get("stone_amount", 0) > 0:
                content_desc.append(f"灵石{number_to(gift['remaining_stone'])}")
            if gift.get("items"):
                active_items = [f"{i['name']}x{i['quantity']}" for i in gift["items"] if i["quantity"] > 0]
                if active_items:
                    content_desc.append(f"物品{','.join(active_items)}")
            
            content_str = " | ".join(content_desc) if content_desc else "空"
            
            msg_parts.append(
                f"\n🎁 仙缘 #{gift_id}\n"
                f"送出者：{gift['giver_name']}\n"
                f"内容：{content_str}\n"
                f"进度：{progress}"
            )
    
    if active_count == 0:
        msg = "所有仙缘都已被领取完毕！\n发送【送仙缘】创建新的仙缘吧"
        await handle_send(bot, event, msg)
        await xiangyuan_list.finish()
    
    msg_parts.append(f"\n═════════════\n共有{active_count}个仙缘可领取")
    
    await handle_send(bot, event, "\n".join(msg_parts), md_type="修仙", k1="送仙缘", v1="送仙缘", k2="抢仙缘", v2="抢仙缘", k3="帮助", v3="仙缘帮助")
    await xiangyuan_list.finish()

__xiangyuan_notes__ = f"""
【仙缘系统】✨
════════════
🌟 核心功能
→ 赠送仙缘：发送"送仙缘 内容 人数"
→ 领取仙缘：发送"抢仙缘"
→ 查看仙缘：发送"仙缘列表"

🌟 使用示例
1. 仅赠送灵石:
   送仙缘 1000000 5
   → 赠送 100 万灵石，5 人可领取
   
2. 赠送灵石 + 物品:
   送仙缘 灵石x1000000,精铁符剑x1 5
   → 赠送 100 万灵石 +1 个精铁符剑，5 人可领取
   → 物品将按领取顺序发放，发完为止

🌟 规则说明
1. 每日限送{XIANGYUAN_SEND_LIMIT}次仙缘
2. 每日限抢{XIANGYUAN_RECEIVE_LIMIT}次仙缘
3. 灵石范围：{number_to(XIANGYUAN_MIN_STONE)} - {number_to(XIANGYUAN_MAX_STONE)}
4. 人数范围：{XIANGYUAN_MIN_RECEIVERS} - {XIANGYUAN_MAX_RECEIVERS}人
5. 物品限制：仅限【装备/技能/药材】，品阶不可为【无上】，必须为非绑定物品
6. 最后一位领取者获得剩余全部灵石

🌟 温馨提示
1. 赠送前请确认灵石充足且物品未绑定
2. 领取前可先查看仙缘列表
3. 珍惜仙缘，广结善缘
""".strip()

@xiangyuan_help.handle(parameterless=[Cooldown(cd_time=1.4)])
async def xiangyuan_help_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """仙缘帮助"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    msg = __xiangyuan_notes__
    await send_help_message(
        bot, event, msg,
        k1="送仙缘", v1="送仙缘",
        k2="抢仙缘", v2="抢仙缘",
        k3="列表", v3="仙缘列表"
    )
    await xiangyuan_help.finish()

async def reset_xiangyuan_daily():
    """每日 0 点重置仙缘次数限制"""
    stone_limit.reset_xiangyuan_limits()
    logger.opt(colors=True).info(f"<green>每日仙缘次数限制已重置！</green>")
    
    msg = await clear_all_xiangyuan()
    logger.info(f"{msg}")

async def clear_all_xiangyuan():
    """清空所有群的仙缘（超级管理员）"""
    
    # 获取所有仙缘数据文件
    xiangyuan_files = list(XIANGYUAN_DATA_PATH.glob("xiangyuan_*.json"))
    
    if not xiangyuan_files:
        return "当前没有仙缘数据可清空！"
    
    total_groups = 0
    total_gifts = 0
    total_refund_stone = 0
    total_refund_items = 0
    
    # 遍历所有群的仙缘文件
    for file_path in xiangyuan_files:
        group_id = file_path.stem.split("_")[1]
        
        try:
            # 读取该群的仙缘数据
            with open(file_path, "r", encoding="utf-8") as f:
                xiangyuan_data = json.load(f)
            
            if not xiangyuan_data.get("gifts"):
                continue
            
            # 退还未领取的资源
            group_refund_stone = 0
            group_refund_items = 0
            group_gifts = len(xiangyuan_data["gifts"])
            
            for gift_id, gift in xiangyuan_data["gifts"].items():
                giver_id = gift["giver_id"]
                
                if gift.get("remaining_stone", 0) > 0:
                    sql_message.update_ls(giver_id, gift["remaining_stone"], 1)
                    group_refund_stone += gift["remaining_stone"]
                    total_refund_stone += gift["remaining_stone"]
                    logger.info(f"清空仙缘：已退还群{group_id}用户{giver_id}灵石{number_to(gift['remaining_stone'])}")

                if gift.get("items"):
                    for item in gift["items"]:
                        if item.get("quantity", 0) > 0:
                            sql_message.send_back(
                                giver_id,
                                item["goods_id"],
                                item["name"],
                                item["type"],
                                item["quantity"],
                                1
                            )
                            group_refund_items += item["quantity"]
                            total_refund_items += item["quantity"]
                            logger.info(f"清空仙缘：已退还群{group_id}用户{giver_id}物品{item['name']}x{item['quantity']}")
            
            # 清空该群的仙缘数据
            xiangyuan_data["gifts"] = {}
            xiangyuan_data["last_id"] = 1  # 重置 ID 计数器
            
            # 保存清空后的数据
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(xiangyuan_data, f, ensure_ascii=False, indent=4)
            
            total_groups += 1
            total_gifts += group_gifts
            
            if group_refund_stone > 0 or group_refund_items > 0:
                logger.info(f"已清空群 {group_id} 的仙缘，退还灵石{number_to(group_refund_stone)}，物品{group_refund_items}个")
            
        except Exception as e:
            logger.error(f"清空群 {group_id} 仙缘时出错：{str(e)}")
            continue
    
    return f"已清空{total_groups}个群{total_gifts}个记录，退还灵石{number_to(total_refund_stone)}，物品{total_refund_items}个"
