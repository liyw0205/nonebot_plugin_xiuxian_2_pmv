import random
import time
from pathlib import Path

from ...paths import get_paths
from ..on_compat import on_command
from nonebot.log import logger
from nonebot.params import CommandArg

from ..adapter_compat import Bot, GroupMessageEvent, PrivateMessageEvent, Message
from ..xiuxian_utils.item_json import Items
from ..xiuxian_utils.json_store import load_json_file
from ..xiuxian_utils.lay_out import assign_bot, Cooldown
from ..xiuxian_config import XiuConfig
from ..xiuxian_utils.utils import check_user, handle_send, number_to, send_help_message
from ..xiuxian_utils.xiuxian2_handle import XiuxianDateManage
from .stone_limit import stone_limit
from .transaction_service import XiangyuanSettlementService

items = Items()
sql_message = XiuxianDateManage()
xiangyuan_settlement_service = XiangyuanSettlementService(
    get_paths().game_db, get_paths().player_db
)

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
    legacy_data = load_json_file(file_path, {"gifts": {}, "last_id": 1}, dict)
    return xiangyuan_settlement_service.get_group(group_id, legacy_data=legacy_data)

def _xiangyuan_operation_id(event, action, user_id):
    event_id = str(
        getattr(event, "message_id", "") or getattr(event, "id", "") or ""
    ).strip()
    if event_id:
        return f"xiangyuan:{event_id}:{action}:{user_id}"
    return f"xiangyuan:{action}:{user_id}:{time.time_ns()}"

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
            except Exception:
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
            except Exception:
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

@give_xiangyuan.handle(parameterless=[Cooldown(cd_time=0)])
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
    
    legacy_path = XIANGYUAN_DATA_PATH / f"xiangyuan_{group_id}.json"
    legacy_data = load_json_file(legacy_path, {"gifts": {}, "last_id": 1}, dict)
    result = xiangyuan_settlement_service.create(
        _xiangyuan_operation_id(event, "create", user_id),
        group_id, user_id, user_info["user_name"], stone_amount, items_list,
        receiver_count, XIANGYUAN_SEND_LIMIT, legacy_data=legacy_data,
    )
    if result.status == "limit_reached":
        msg = f"道友今日已送{result.send_count}次仙缘，达到上限 ({XIANGYUAN_SEND_LIMIT}次/日)，明日再来吧！"
        await handle_send(bot, event, msg)
        await give_xiangyuan.finish()
    if result.status == "stone_insufficient":
        await handle_send(bot, event, "道友的灵石不够，请重新输入！")
        await give_xiangyuan.finish()
    if result.status == "item_insufficient":
        await handle_send(bot, event, "可交易物品数量已发生变化，请重新输入！")
        await give_xiangyuan.finish()
    if not result.succeeded:
        await handle_send(bot, event, "仙缘创建状态已变化，请重新操作！")
        await give_xiangyuan.finish()
    xiangyuan_id = result.gift_id
    
    # 构建消息
    msg = f"✨【仙缘 #{xiangyuan_id}】✨\n"
    msg += f"送出者：{user_info['user_name']}\n"
    if stone_amount > 0:
        msg += f"灵石总额：{number_to(stone_amount)}\n"
    if items_list:
        item_desc = ", ".join([f"{i['name']}x{i['quantity']}" for i in items_list])
        msg += f"包含物品：{item_desc}\n"
    msg += f"可领取人数：{receiver_count}人\n"
    msg += f"今日剩余送仙缘次数：{XIANGYUAN_SEND_LIMIT - result.send_count}次\n"
    msg += "\n同群道友可发送【抢仙缘】获取仙缘"
    
    await handle_send(bot, event, msg, md_type="修仙", k1="抢仙缘", v1="抢仙缘", k2="仙缘列表", v2="仙缘列表", k3="帮助", v3="仙缘帮助")
    await give_xiangyuan.finish()

@get_xiangyuan.handle(parameterless=[Cooldown(cd_time=0)])
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
    
    item_ids = [item["goods_id"] for item in gift.get("items", ()) if item["quantity"] > 0]
    legacy_path = XIANGYUAN_DATA_PATH / f"xiangyuan_{group_id}.json"
    legacy_data = load_json_file(legacy_path, {"gifts": {}, "last_id": 1}, dict)
    result = xiangyuan_settlement_service.claim(
        _xiangyuan_operation_id(event, "claim", user_id), group_id, gift_id,
        user_id, reward, item_ids, XIANGYUAN_RECEIVE_LIMIT,
        XiuConfig().max_goods_num, legacy_data=legacy_data,
    )
    if result.status == "limit_reached":
        await handle_send(bot, event, f"道友今日已抢{result.receive_count}次仙缘，达到上限 ({XIANGYUAN_RECEIVE_LIMIT}次/日)，明日再来吧！")
        await get_xiangyuan.finish()
    if result.status == "inventory_full":
        await handle_send(bot, event, "背包空间不足，无法领取仙缘！")
        await get_xiangyuan.finish()
    if not result.succeeded:
        await handle_send(bot, event, "这份仙缘已被抢先领取，请重新尝试！")
        await get_xiangyuan.finish()
    reward = result.stone
    received_items = [f"{name}x{amount}" for _, name, amount in result.items]
    is_last_receiver = result.received >= result.receiver_count
    is_single_gift = result.receiver_count == 1
    
    # 构建消息
    msg = f"【仙缘 {gift_id}】\n"
    msg += f"（由【{result.giver_name}】送出）\n"
    
    if reward > 0:
        if is_single_gift:
            msg += f"🎯 独享仙缘！\n"
            msg += f"获得灵石：{number_to(reward)}\n"
        elif is_last_receiver:
            msg += f"🏆 最后机会！\n"
            msg += f"获得剩余全部：{number_to(reward)}\n"
        else:
            remaining_receivers = result.receiver_count - result.received
            msg += f"💰 获得灵石：{number_to(reward)}\n"
            msg += f"剩余可领取：{remaining_receivers}人\n"
            msg += f"池中剩余：{number_to(result.remaining_stone)}灵石\n"
    
    if received_items:
        msg += f"🎁 获得物品：{', '.join(received_items)}\n"
    
    msg += f"今日剩余抢仙缘次数：{XIANGYUAN_RECEIVE_LIMIT - result.receive_count}次"
    
    await handle_send(bot, event, msg, md_type="修仙", k1="送仙缘", v1="送仙缘", k2="仙缘列表", v2="仙缘列表", k3="帮助", v3="仙缘帮助")
    await get_xiangyuan.finish()

@xiangyuan_list.handle(parameterless=[Cooldown(cd_time=0)])
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
    msg_parts = ["【本群仙缘列表】"]
    
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
→ 赠送仙缘
> 发送"送仙缘 内容 人数"
→ 领取仙缘
> 发送"抢仙缘"
→ 查看仙缘
> 发送"仙缘列表"

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
3. 灵石范围
> {number_to(XIANGYUAN_MIN_STONE)} - {number_to(XIANGYUAN_MAX_STONE)}
4. 人数范围
> {XIANGYUAN_MIN_RECEIVERS} - {XIANGYUAN_MAX_RECEIVERS}人
5. 物品限制
> 仅限【装备/技能/药材】，品阶不可为【无上】，必须为非绑定物品
6. 最后一位领取者获得剩余全部灵石

🌟 温馨提示
1. 赠送前请确认灵石充足且物品未绑定
2. 领取前可先查看仙缘列表
3. 珍惜仙缘，广结善缘
""".strip()

@xiangyuan_help.handle(parameterless=[Cooldown(cd_time=0)])
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
    total_groups, total_gifts, total_refund_stone, total_refund_items = (
        xiangyuan_settlement_service.clear_all(XiuConfig().max_goods_num)
    )
    if total_gifts == 0:
        return "当前没有仙缘数据可清空！"
    return f"已清空{total_groups}个群{total_gifts}个记录，退还灵石{number_to(total_refund_stone)}，物品{total_refund_items}个"
