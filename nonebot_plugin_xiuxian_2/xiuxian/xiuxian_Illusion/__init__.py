import random
import json
import os
from pathlib import Path
from datetime import datetime
from nonebot import on_command
from nonebot.params import CommandArg
from nonebot.log import logger
from nonebot.adapters.onebot.v11 import (
    Bot,
    Message,
    GroupMessageEvent,
    PrivateMessageEvent
)
from nonebot.permission import SUPERUSER
from ..xiuxian_utils.lay_out import assign_bot, Cooldown
from ..xiuxian_utils.utils import (
    check_user, check_user_type, 
    get_msg_pic, log_message, handle_send, 
    number_to, send_msg_handler, update_statistics_value
)
from ..xiuxian_utils.xiuxian2_handle import XiuxianDateManage
from ..xiuxian_utils.item_json import Items
from ..xiuxian_config import convert_rank, base_rank
from .IllusionData import *
sql_message = XiuxianDateManage()
items = Items()

# 定义命令
illusion_start = on_command("幻境寻心", priority=5, block=True)
illusion_choice = on_command("心境试炼", priority=5, block=True)
illusion_reset = on_command("重置幻境", permission=SUPERUSER, priority=5, block=True)
illusion_clear = on_command("清空幻境", permission=SUPERUSER, priority=5, block=True)

async def reset_illusion_data():
    IllusionData.reset_player_data_only()
    logger.opt(colors=True).info("<green>幻境寻心玩家数据已重置</green>")

@illusion_start.handle(parameterless=[Cooldown(cd_time=1.4)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """幻境寻心 - 生成幻境或查看结果"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await illusion_start.finish()
    
    user_id = user_info["user_id"]
    illusion_info = IllusionData.get_or_create_user_illusion_info(user_id)
    
    # 检查问题索引是否有效
    if illusion_info["question_index"] is None or illusion_info["question_index"] >= len(DEFAULT_QUESTIONS):
        msg = "幻境寻心功能暂时无法使用，请联系管理员检查问题配置"
        await handle_send(bot, event, msg)
        await illusion_start.finish()
    
    # 检查是否已经参与过今日的幻境
    if illusion_info["today_choice"] is not None:
        question = DEFAULT_QUESTIONS[illusion_info["question_index"]]["question"]
        choice = illusion_info["today_choice"]
        msg = (
            f"\n═══  幻境寻心  ════\n"
            f"今日问题：{question}\n"
            f"你的选择：{choice}\n"
            f"════════════\n"
            f"每日8点重置，请明日再来！"
        )
        await handle_send(bot, event, msg, md_type="幻境寻心", k1="寻心", v1="幻境寻心", k2="存档", v2="我的修仙信息", k3="帮助", v3="修仙帮助")
        await illusion_start.finish()
    
    # 获取当前问题数据
    question_data = DEFAULT_QUESTIONS[illusion_info["question_index"]]
    question = question_data["question"]
    options = question_data["options"]
    
    # 显示问题和选项
    msg = "\n═══  幻境寻心  ════\n"
    msg += f"今日问题：{question}\n"
    msg += "请选择："
    for i, option in enumerate(options, 1):
        msg += f"\n{i}. {option}"
    msg += "\n════════════\n"
    msg += "使用【心境试炼+数字】进行选择"
    
    await handle_send(bot, event, msg, md_type="幻境寻心", k1="试炼壹", v1="心境试炼 1", k2="试炼贰", v2="心境试炼 2", k3="试炼叁", v3="心境试炼 3")
    await illusion_start.finish()

@illusion_choice.handle(parameterless=[Cooldown(cd_time=1.4)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """心境试炼 - 选择幻境"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await illusion_choice.finish()
    
    user_id = user_info["user_id"]
    illusion_info = IllusionData.get_or_create_user_illusion_info(user_id)
    
    # 检查问题索引是否有效
    if illusion_info["question_index"] is None or illusion_info["question_index"] >= len(DEFAULT_QUESTIONS):
        msg = "幻境寻心功能暂时无法使用，请联系管理员检查问题配置"
        await handle_send(bot, event, msg)
        await illusion_choice.finish()
    
    # 获取用户输入的数字
    choice_input = args.extract_plain_text().strip()
    
    try:
        choice_num = int(choice_input)
    except ValueError:
        msg = "请输入有效的数字！"
        await handle_send(bot, event, msg)
        await illusion_choice.finish()
    
    # 获取当前问题数据
    question_data = DEFAULT_QUESTIONS[illusion_info["question_index"]]
    options = question_data["options"]
    explanations = question_data.get("explanations", [])
    
    # 检查输入是否有效
    if choice_num < 1 or choice_num > len(options):
        msg = f"请输入有效的选择数字(1-{len(options)})！"
        await handle_send(bot, event, msg, md_type="幻境寻心", k1="试炼壹", v1="心境试炼 1", k2="试炼贰", v2="心境试炼 2", k3="试炼叁", v3="心境试炼 3")
        await illusion_choice.finish()
    
    # 检查是否已经参与过今日的幻境
    if illusion_info["today_choice"] is not None:
        msg = "今日已经参与过幻境寻心，请明日再来！"
        await handle_send(bot, event, msg)
        await illusion_choice.finish()
    
    # 记录用户选择
    selected_option = options[choice_num - 1]  # 获取不带数字的选项文本
    selected_explanation = explanations[choice_num - 1] if choice_num - 1 < len(explanations) else "暂无详细解释"
    illusion_info["today_choice"] = selected_option
    illusion_info["last_participate"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    IllusionData.save_user_illusion_info(user_id, illusion_info)
    
    # 更新问题统计数据
    IllusionData.update_question_stats(illusion_info["question_index"], choice_num - 1)
    
    # 获取当前问题的统计数据
    stats = IllusionData.get_stats()
    question_stats = stats["question_stats"][illusion_info["question_index"]]
    counts = question_stats
    total_choices = sum(counts)
    
    # 计算当前选择的排名
    sorted_counts = sorted([(i+1, count) for i, count in enumerate(counts)], key=lambda x: -x[1])
    rank_dict = {x[0]: i+1 for i, x in enumerate(sorted_counts)}
    user_rank = rank_dict[choice_num]
    choice_count = counts[choice_num - 1]
    
    # 计算当前选择的占比
    percentage = choice_count / total_choices * 100 if total_choices > 0 else 100
    
    base_weight = 25

    # 根据选择占比确定权重调整
    if percentage < 30:  # 少数派
        exp_weight = base_weight
        stone_weight = base_weight
        item_weight = base_weight + 25
    elif 30 <= percentage <= 70:  # 平均派
        exp_weight = base_weight
        stone_weight = base_weight
        item_weight = base_weight
    else:  # 多数派
        exp_weight = base_weight
        stone_weight = base_weight + 25
        item_weight = base_weight

    # 权重列表和对应的奖励类型
    weights = [exp_weight, stone_weight, item_weight]
    reward_types = ['exp', 'stone', 'item']

    # 根据权重随机选择奖励类型
    selected_reward_type = random.choices(reward_types, weights=weights, k=1)[0]

    # 根据选择的奖励类型给予相应奖励
    reward_msg = ""
    if selected_reward_type == 'exp':  # 修为奖励
        user_rank = max(convert_rank(user_info['level'])[0] // 3, 1)
        exp_reward = int(user_info["exp"] * 0.01 * min(0.1 * user_rank, 1))
        sql_message.update_exp(user_id, exp_reward)
        reward_msg = f"你的选择是少数派的选择(第{choice_count}位道友)，获得修为：{number_to(exp_reward)}点"
    elif selected_reward_type == 'stone':  # 灵石奖励
        stone_reward = int(random.randint(1000000, 10000000) * (1 + min(choice_count * 0.1, 2.0)))
        sql_message.update_ls(user_id, stone_reward, 1)
        reward_msg = f"你的选择是多数派的选择(第{choice_count}位道友)，获得灵石：{number_to(stone_reward)}枚"
    elif selected_reward_type == 'item':  # 物品奖励
        item_msg = _give_random_item(user_id, user_info["level"])
        reward_msg = f"你的选择是平均派的选择(第{choice_count}位道友)，获得：{item_msg}"

    msg = (
        f"\n═══  幻境寻心  ════\n"
        f"今日问题：{question_data['question']}\n"
        f"你的选择：{selected_option}\n"
        f"════════════\n"
        f"【解析】\n{selected_explanation}\n"
        f"════════════\n"
        f"{reward_msg}\n"
        f"════════════\n"
        f"每日8点重置，请明日再来！"
    )
    update_statistics_value(user_id, "寻心次数")
    await handle_send(bot, event, msg)
    await illusion_choice.finish()

@illusion_reset.handle(parameterless=[Cooldown(cd_time=1.4)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """重置幻境数据(管理员) - 重置玩家数据和问题统计数据"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    
    IllusionData.reset_all_data()
    
    msg = "所有用户的幻境寻心数据和问题统计数据已重置！"
    await handle_send(bot, event, msg)
    await illusion_reset.finish()

@illusion_clear.handle(parameterless=[Cooldown(cd_time=1.4)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """清空幻境数据(管理员) - 仅清空玩家数据"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    
    IllusionData.reset_player_data_only()
    
    msg = "所有用户的幻境寻心数据已清空！"
    await handle_send(bot, event, msg)
    await illusion_clear.finish()

def _give_random_item(user_id, user_level):
    """给予随机物品奖励"""    
    # 随机选择物品类型
    item_types = ["功法", "神通", "药材", "法器", "防具", "身法", "瞳术"]
    item_type = random.choice(item_types)
    if item_type in ["法器", "防具", "辅修功法", "身法", "瞳术"]:
        zx_rank = base_rank(user_level, 16)
    else:
        zx_rank = base_rank(user_level, 5)

    # 获取随机物品
    item_id_list = items.get_random_id_list_by_rank_and_item_type(zx_rank, item_type)

    # 自定义物品ID列表
    zdyid_list = ["20001", "20005", "20006", "20007", "20010", "20017"]

    # 合并系统生成物品ID与自定义物品ID
    combined_id_list = []
    if item_id_list:
        combined_id_list.extend(item_id_list)
    if zdyid_list:
        combined_id_list.extend(zdyid_list)

    if not combined_id_list:
        return "无"

    item_id = random.choice(combined_id_list)
    item_info = items.get_data_by_item_id(item_id)

    if item_info is None:
        return "无"

    # 给予物品
    sql_message.send_back(
        user_id, 
        item_id, 
        item_info["name"], 
        item_info["type"], 
        1
    )

    return f"{item_info.get('level', '特殊道具')}:{item_info['name']}"
