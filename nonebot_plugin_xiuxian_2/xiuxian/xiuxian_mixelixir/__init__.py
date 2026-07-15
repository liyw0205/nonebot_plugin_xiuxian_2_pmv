import random
import asyncio
import re
import json
import time
from ..on_compat import on_command
from nonebot.params import EventPlainText, CommandArg
from ..adapter_compat import (
    Bot,
    GROUP,
    Message,
    GroupMessageEvent,
    PrivateMessageEvent,
    MessageSegment
)
from ..xiuxian_utils.lay_out import assign_bot, Cooldown
from ..xiuxian_utils.xiuxian2_handle import (
    XiuxianDateManage, get_player_info, save_player_info,
    UserBuffDate, XIUXIAN_IMPART_BUFF
)
from ..xiuxian_utils.utils import (
    check_user, send_msg_handler,
    get_msg_pic, handle_send, log_message,
    send_help_message
)
from ..xiuxian_utils.item_json import Items
from .mixelixirutil import get_mix_elixir_msg, tiaohe, check_mix, make_dict, get_elixir_recipe_msg
from ..xiuxian_config import convert_rank, XiuConfig, added_ranks
from datetime import datetime
from .mix_elixir_config import MIXELIXIRCONFIG
from ...paths import get_paths
from .transaction_service import MixelixirHarvestService
from .transaction_service import MixelixirHarvestLevelUpgradeService
from .transaction_service import MixelixirRecipeService
from .transaction_service import MixelixirRefineCostService
from .transaction_service import MixelixirRefineRewardService

sql_message = XiuxianDateManage()  # sql类
mixelixir_harvest_service = MixelixirHarvestService(get_paths().game_db, get_paths().player_db)
mixelixir_harvest_level_upgrade_service = MixelixirHarvestLevelUpgradeService(
    get_paths().game_db, get_paths().player_db
)
mixelixir_recipe_service = MixelixirRecipeService(get_paths().game_db)
mixelixir_refine_cost_service = MixelixirRefineCostService(get_paths().game_db)
mixelixir_refine_reward_service = MixelixirRefineRewardService(get_paths().game_db, get_paths().player_db)
xiuxian_impart = XIUXIAN_IMPART_BUFF()
items = Items()
added_rank = added_ranks()
cache_help = {}

mix_elixir = on_command("炼丹", priority=17, block=True)
mix_make = on_command("配方", priority=5, block=True)
elixir_help = on_command("炼丹帮助", priority=7, block=True)
mix_elixir_help = on_command("炼丹配方帮助", priority=7, block=True)
yaocai_get = on_command("灵田收取", aliases={"灵田结算"}, priority=8, block=True)
my_mix_elixir_info = on_command("我的炼丹信息", aliases={"炼丹信息"}, priority=6, block=True)
mix_elixir_sqdj_up = on_command("升级收取等级", priority=6, block=True)
mix_elixir_dykh_up = on_command("升级丹药控火", priority=6, block=True)

__elixir_help__ = f"""
【炼丹帮助】
指令：
炼丹:会检测背包内的药材,自动生成配方【一次最多匹配25种药材】
配方:发送配方领取丹药【配方主药.....】
炼丹配方帮助:查看炼丹配方规则
灵田收取、灵田结算:收取洞天福地里灵田的药材
我的炼丹信息:查询自己的炼丹信息
升级收取等级:每一个等级会增加灵田收取的数量
升级丹药控火:每一个等级会增加炼丹的产出数量
"""

__mix_elixir_help__ = f"""
炼丹配方信息
1、炼丹需要主药、药引、辅药
2、主药和药引控制炼丹时的冷热调和,冷热失和则炼不出丹药
3、草药的类型控制产出丹药的类型
"""


@mix_elixir_sqdj_up.handle(parameterless=[Cooldown(cd_time=0)])
async def mix_elixir_sqdj_up_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """收取等级升级"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await mix_elixir_sqdj_up.finish()
    user_id = user_info['user_id']
    if int(user_info['blessed_spot_flag']) == 0:
        msg = f"道友还没有洞天福地呢，请发送洞天福地购买吧~"
        await handle_send(bot, event, msg, md_type="炼丹", k1="购买", v1="洞天福地购买", k2="查看", v2="洞天福地查看", k3="帮助", v3="洞天福地帮助")
        await mix_elixir_sqdj_up.finish()
    SQDJCONFIG = MIXELIXIRCONFIG['收取等级']
    mix_elixir_info = get_player_info(user_id, "mix_elixir_info")
    now_level = mix_elixir_info['收取等级']
    event_id = str(getattr(event, "message_id", "") or getattr(event, "id", "") or "").strip()
    operation_id = f"mixelixir-harvest-level:{event_id}:{user_id}" if event_id else f"mixelixir-harvest-level:{user_id}:{time.time_ns()}"
    # 先回放：成功后等级达上限会挡住同事件幂等。
    prior = mixelixir_harvest_level_upgrade_service.get_result(operation_id)
    if prior is not None and prior.succeeded:
        msg = f"道友消耗灵石{prior.cost}枚，收取等级目前为：{prior.level}级，可以使灵田收获的药材增加{prior.level}个！\n该升级请求已经处理，无需重复提交。"
        await handle_send(bot, event, msg, md_type="炼丹", k1="升级", v1="升级丹药控火", k2="信息", v2="我的炼丹信息", k3="帮助", v3="炼丹帮助")
        await mix_elixir_sqdj_up.finish()
    if now_level >= len(SQDJCONFIG):
        msg = f"道友的收取等级已达到最高等级，无法升级了"
        await handle_send(bot, event, msg, md_type="炼丹", k1="升级", v1="升级丹药控火", k2="信息", v2="我的炼丹信息", k3="帮助", v3="炼丹帮助")
        await mix_elixir_sqdj_up.finish()
    next_level_cost = SQDJCONFIG[str(now_level + 1)]['level_up_cost']
    if int(user_info['stone']) < next_level_cost:
        msg = f"下一个收取等级需要灵石{next_level_cost}枚，道友当前灵石不足。"
        await handle_send(bot, event, msg, md_type="炼丹", k1="升级", v1="升级丹药控火", k2="信息", v2="我的炼丹信息", k3="帮助", v3="炼丹帮助")
        await mix_elixir_sqdj_up.finish()
    upgrade = mixelixir_harvest_level_upgrade_service.upgrade(
        operation_id,
        user_id,
        now_level,
        mix_elixir_info['炼丹经验'],
        user_info['stone'],
        now_level + 1,
        next_level_cost,
    )
    if upgrade.status == "duplicate":
        msg = f"道友消耗灵石{upgrade.cost}枚，收取等级目前为：{upgrade.level}级，可以使灵田收获的药材增加{upgrade.level}个！\n该升级请求已经处理，无需重复提交。"
        await handle_send(bot, event, msg, md_type="炼丹", k1="升级", v1="升级丹药控火", k2="信息", v2="我的炼丹信息", k3="帮助", v3="炼丹帮助")
        await mix_elixir_sqdj_up.finish()
    if not upgrade.succeeded:
        msg = "灵石或炼丹状态已变化，本次收取等级升级未结算，请重新查看后再试。"
        await handle_send(bot, event, msg, md_type="炼丹", k1="升级", v1="升级收取等级", k2="信息", v2="我的炼丹信息", k3="帮助", v3="炼丹帮助")
        await mix_elixir_sqdj_up.finish()
    msg = f"道友消耗灵石{upgrade.cost}枚，收取等级目前为：{upgrade.level}级，可以使灵田收获的药材增加{upgrade.level}个！"
    await handle_send(bot, event, msg, md_type="炼丹", k1="升级", v1="升级丹药控火", k2="信息", v2="我的炼丹信息", k3="帮助", v3="炼丹帮助")
    await mix_elixir_sqdj_up.finish()


@mix_elixir_dykh_up.handle(parameterless=[Cooldown(cd_time=0)])
async def mix_elixir_dykh_up_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """丹药控火升级"""
    from .transaction_service import MixelixirFireControlUpgradeService

    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await mix_elixir_dykh_up.finish()
    user_id = user_info['user_id']
    DYKHCONFIG = MIXELIXIRCONFIG['丹药控火']
    mix_elixir_info = get_player_info(user_id, "mix_elixir_info")
    now_level = mix_elixir_info['丹药控火']
    event_id = str(getattr(event, "message_id", "") or getattr(event, "id", "") or "").strip()
    operation_id = f"mixelixir-fire-control:{event_id}:{user_id}" if event_id else f"mixelixir-fire-control:{user_id}:{time.time_ns()}"
    upgrade_service = MixelixirFireControlUpgradeService(get_paths().game_db, get_paths().player_db)
    prior = upgrade_service.get_result(operation_id)
    if prior is not None and prior.succeeded:
        msg = f"道友消耗灵石{prior.cost}枚，丹药控火等级目前为：{prior.level}级，可以使炼丹收获的丹药增加{prior.level}个！\n该升级请求已经处理，无需重复提交。"
        await handle_send(bot, event, msg, md_type="炼丹", k1="升级", v1="升级丹药控火", k2="信息", v2="我的炼丹信息", k3="帮助", v3="炼丹帮助")
        await mix_elixir_dykh_up.finish()
    if now_level >= len(DYKHCONFIG):
        msg = f"道友的丹药控火等级已达到最高等级，无法升级了"
        await handle_send(bot, event, msg, md_type="炼丹", k1="升级", v1="升级丹药控火", k2="信息", v2="我的炼丹信息", k3="帮助", v3="炼丹帮助")
        await mix_elixir_dykh_up.finish()
    next_level_cost = DYKHCONFIG[str(now_level + 1)]['level_up_cost']
    if int(user_info['stone']) < next_level_cost:
        msg = f"下一个丹药控火等级需要灵石{next_level_cost}枚，道友当前灵石不足。"
        await handle_send(bot, event, msg, md_type="炼丹", k1="升级", v1="升级丹药控火", k2="信息", v2="我的炼丹信息", k3="帮助", v3="炼丹帮助")
        await mix_elixir_dykh_up.finish()
    upgrade = upgrade_service.upgrade(
        operation_id,
        user_id,
        now_level,
        mix_elixir_info['炼丹经验'],
        user_info['stone'],
        now_level + 1,
        next_level_cost,
    )
    if upgrade.status == "duplicate":
        msg = f"道友消耗灵石{upgrade.cost}枚，丹药控火等级目前为：{upgrade.level}级，可以使炼丹收获的丹药增加{upgrade.level}个！\n该升级请求已经处理，无需重复提交。"
        await handle_send(bot, event, msg, md_type="炼丹", k1="升级", v1="升级丹药控火", k2="信息", v2="我的炼丹信息", k3="帮助", v3="炼丹帮助")
        await mix_elixir_dykh_up.finish()
    if not upgrade.succeeded:
        msg = "灵石或炼丹状态已变化，本次丹药控火升级未结算，请重新查看后再试。"
        await handle_send(bot, event, msg, md_type="炼丹", k1="升级", v1="升级丹药控火", k2="信息", v2="我的炼丹信息", k3="帮助", v3="炼丹帮助")
        await mix_elixir_dykh_up.finish()
    msg = f"道友消耗灵石{upgrade.cost}枚，丹药控火等级目前为：{upgrade.level}级，可以使炼丹收获的丹药增加{upgrade.level}个！"
    await handle_send(bot, event, msg, md_type="炼丹", k1="升级", v1="升级丹药控火", k2="信息", v2="我的炼丹信息", k3="帮助", v3="炼丹帮助")
    await mix_elixir_dykh_up.finish()


@yaocai_get.handle(parameterless=[Cooldown(stamina_cost=1)])
async def yaocai_get_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """灵田收取"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await yaocai_get.finish()

    user_id = user_info['user_id']
    if int(user_info['blessed_spot_flag']) == 0:
        msg = f"道友还没有洞天福地呢，请发送洞天福地购买吧~"
        await handle_send(bot, event, msg, md_type="炼丹", k1="购买", v1="洞天福地购买", k2="查看", v2="洞天福地查看", k3="帮助", v3="洞天福地帮助")
        await yaocai_get.finish()
    mix_elixir_info = get_player_info(user_id, "mix_elixir_info")
    GETCONFIG = {
        "time_cost": 23,  # 单位小时
        "加速基数": 0.05
    }
    event_id = str(getattr(event, "message_id", "") or getattr(event, "id", "") or "").strip()
    operation_id = f"mixelixir-harvest:{event_id}:{user_id}" if event_id else f"mixelixir-harvest:{user_id}:{time.time_ns()}"
    # 先回放：成功后收取时间推进会挡住同事件幂等。
    prior = mixelixir_harvest_service.get_result(operation_id)
    if prior is not None and prior.succeeded:
        msg = "".join(
            f"道友成功收获药材：{reward.name} {reward.quantity} 个！\n"
            for reward in prior.rewards
        ) + "该收取请求已经处理，无需重复提交。"
        l_msg = [msg]
        await send_msg_handler(bot, event, '灵田收取', bot.self_id, l_msg)
        await yaocai_get.finish()
    last_time = mix_elixir_info['收取时间']
    if last_time != 0:
        nowtime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # str
        timedeff = round((datetime.strptime(nowtime, '%Y-%m-%d %H:%M:%S') - datetime.strptime(last_time,
                                                                                              '%Y-%m-%d %H:%M:%S')).total_seconds() / 3600,
                         2)
        if timedeff >= round(GETCONFIG['time_cost'] * (1 - (GETCONFIG['加速基数'] * mix_elixir_info['药材速度'])), 2):
            yaocai_id_list = items.get_random_id_list_by_rank_and_item_type(
                max(convert_rank(user_info['level'])[0] - added_rank, 16), ['药材'])
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
            rewards = []
            if not yaocai_id_list:
                rewards.append((3001, "恒心草", num))
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
                    rewards.append((k, goods_info['name'], v))
            harvest = mixelixir_harvest_service.harvest(
                operation_id,
                user_id,
                last_time,
                nowtime,
                rewards,
                max_goods_num=XiuConfig().max_goods_num,
            )
            if harvest.status == "duplicate":
                msg = "".join(
                    f"道友成功收获药材：{reward.name} {reward.quantity} 个！\n"
                    for reward in harvest.rewards
                ) + "该收取请求已经处理，无需重复提交。"
                l_msg = [msg]
                await send_msg_handler(bot, event, '灵田收取', bot.self_id, l_msg)
                await yaocai_get.finish()
            if harvest.status in {"state_changed", "user_missing"}:
                msg = "灵田状态已变化，本次未发放药材，请重新查看后再试。"
                await handle_send(bot, event, msg, md_type="炼丹", k1="收取", v1="灵田收取", k2="查看", v2="洞天福地查看", k3="帮助", v3="洞天福地帮助")
                await yaocai_get.finish()
            msg = "".join(
                f"道友成功收获药材：{reward.name} {reward.quantity} 个！\n"
                for reward in harvest.rewards
            )
            l_msg = [msg]
            await send_msg_handler(bot, event, '灵田收取', bot.self_id, l_msg)
            await yaocai_get.finish()
        else:
            remaining_time = round(GETCONFIG['time_cost'] * (1 - (GETCONFIG['加速基数'] * mix_elixir_info['药材速度'])),
                                   2) - timedeff
            hours = int(remaining_time)
            minutes = int((remaining_time - hours) * 60)
            msg = f"道友的灵田还不能收取，下次收取时间为：{hours}小时{minutes}分钟之后"
            await handle_send(bot, event, msg, md_type="炼丹", k1="收取", v1="灵田收取", k2="查看", v2="洞天福地查看", k3="帮助", v3="洞天福地帮助")
            await yaocai_get.finish()


@my_mix_elixir_info.handle(parameterless=[Cooldown(cd_time=0)])
async def my_mix_elixir_info_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """我的炼丹信息"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await my_mix_elixir_info.finish()
    user_id = user_info['user_id']
    mix_elixir_info = get_player_info(user_id, 'mix_elixir_info')
    title = "【道友的炼丹信息】"
    l_msg = []
    msg = f"药材收取等级：{mix_elixir_info['收取等级']}\n"
    msg += f"丹药控火等级：{mix_elixir_info['丹药控火']}\n"
    msg += f"丹药耐药性等级：{mix_elixir_info['丹药耐药性']}\n"
    msg += f"炼丹经验：{mix_elixir_info['炼丹经验']}\n"
    l_msg.append(msg)
    if mix_elixir_info['炼丹记录'] != {}:
        l_msg.append(f"☆------道友的炼丹记录------☆")
        i = 1
        for k, v in mix_elixir_info['炼丹记录'].items():
            msg = f"编号：{i},{v['name']}，炼成次数：{v['num']}次"
            l_msg.append(msg)
            i += 1
    await send_msg_handler(bot, event, '炼丹信息', bot.self_id, l_msg, title=title)
    await my_mix_elixir_info.finish()


@elixir_help.handle(parameterless=[Cooldown(cd_time=0)])
async def elixir_help_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """炼丹帮助"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    msg = __elixir_help__
    await send_help_message(bot, event, msg, k1="炼丹", v1="炼丹", k2="配方", v2="配方", k3="配方帮助", v3="炼丹配方帮助")
    await elixir_help.finish()


@mix_elixir_help.handle(parameterless=[Cooldown(cd_time=0)])
async def mix_elixir_help_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """炼丹配方帮助"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    msg = __mix_elixir_help__
    await send_help_message(bot, event, msg, k1="炼丹", v1="炼丹", k2="配方", v2="配方", k3="炼丹帮助", v3="炼丹帮助")
    await mix_elixir_help.finish()


user_ldl_dict = {}
user_ldl_flag = {}


def remove_herbs_by_levels(herb_dict, levels_to_remove):
    """根据数字品级删除对应药材"""
    level_map = {i: f"{['零', '一', '二', '三', '四', '五', '六', '七', '八', '九'][i]}品药材" for i in
                 range(1, 10)}
    remove_levels = {level_map[l] for l in levels_to_remove if 1 <= l <= 9}
    return {k: v for k, v in herb_dict.items() if v.get("level") not in remove_levels}



def _recipe_inventory_snapshot(user_back, goods_type):
    return [
        {"id": int(item["goods_id"]), "name": str(item["goods_name"]), "quantity": int(item["goods_num"])}
        for item in user_back
        if item.get("goods_type") == goods_type and int(item.get("goods_num", 0) or 0) > 0
    ]


def _saved_recipe(recipe, furnace_id, furnace_name):
    material_specs = (
        ("主药", "主药_num"),
        ("药引", "药引_num"),
        ("辅药", "辅药_num"),
    )
    materials = []
    key_parts = []
    for name_key, quantity_key in material_specs:
        name = str(recipe.get(name_key, ""))
        quantity = int(recipe.get(quantity_key, 0) or 0)
        key_parts.append(f"{name_key}{name}{quantity}")
        if quantity:
            item_id, _ = items.get_data_by_item_name(name)
            if not item_id:
                raise ValueError(f"recipe material not found: {name}")
            materials.append({"id": int(item_id), "name": name, "quantity": quantity})
    reward = items.get_data_by_item_id(recipe["id"])
    return {
        "recipe_key": "".join(key_parts) + f"丹炉{furnace_name}",
        "materials": materials,
        "furnace": {"id": int(furnace_id), "name": str(furnace_name)},
        "reward_id": int(recipe["id"]),
        "reward_name": str(reward["name"]),
    }
@mix_elixir.handle(parameterless=[Cooldown(cd_time=10)])
async def mix_elixir_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """炼丹,用来生成配方"""
    global user_ldl_dict, user_ldl_flag
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await mix_elixir.finish()
    user_id = user_info['user_id']
    user_back = sql_message.get_back_msg(user_id)
    if not user_back:
        msg = "道友的背包空空如也，无法炼丹"
        await handle_send(bot, event, msg)
        await mix_elixir.finish()

    yaocai_dict = {}
    user_ldl_flag[user_id] = False  # 初始化炼丹炉标志
    for back in user_back:
        if back['goods_type'] == "药材":
            yaocai_dict[back['goods_id']] = items.get_data_by_item_id(back['goods_id'])
            yaocai_dict[back['goods_id']]['num'] = back['goods_num']
        elif back['goods_type'] == "炼丹炉":
            if user_id not in user_ldl_dict:
                user_ldl_dict[user_id] = {}
            user_ldl_dict[user_id][back['goods_id']] = back['goods_name']
            user_ldl_flag[user_id] = True

    if yaocai_dict == {}:
        msg = "道友的背包内没有药材，无法炼丹！"
        await handle_send(bot, event, msg)
        await mix_elixir.finish()

    if not user_ldl_flag[user_id]:
        msg = "道友背包内没有炼丹炉，无法炼丹！"
        await handle_send(bot, event, msg)
        await mix_elixir.finish()

    msg = "正在生成丹方，请稍候..."
    await handle_send(bot, event, msg)

    input_str = args.extract_plain_text().strip()  # 获取用户输入的物品名
    if input_str:
        msg = "请输入丹药名称！\n例如：炼丹 灭神古丸"
        # ====== 解析 丹药名 + 可变数量 的品级数字 ======
        parts = input_str.split()
        dan_name = parts[0]  # 丹药名
        try:
            remove_level_nums = set(map(int, parts[1:]))  # 如 {7,8}
        except ValueError:
            msg = "请输入正确的药材品级数字！\n例如：炼丹 灭神古丸 7 8"
            await handle_send(bot, event, msg, md_type="炼丹", k1="炼丹", v1="炼丹", k2="信息", v2="我的炼丹信息", k3="帮助", v3="炼丹帮助")
            await mix_elixir.finish()
        target_elixir_id, target_elixir = Items().get_data_by_item_name(dan_name)
        if not target_elixir_id or target_elixir is None:
            msg = "请输入有效丹药名称！"
            await handle_send(bot, event, msg, md_type="炼丹", k1="炼丹", v1="炼丹", k2="信息", v2="我的炼丹信息", k3="帮助", v3="炼丹帮助")
            await mix_elixir.finish()
        if "elixir_config" not in target_elixir:
            msg = f"{dan_name}不支持炼丹！"
            await handle_send(bot, event, msg, md_type="炼丹", k1="炼丹", v1="炼丹", k2="信息", v2="我的炼丹信息", k3="帮助", v3="炼丹帮助")
            await mix_elixir.finish()
        yaocai_dict = remove_herbs_by_levels(yaocai_dict, remove_level_nums)  # 删除指定品质药材
        mix_elixir_msgs = await get_elixir_recipe_msg(target_elixir_id, target_elixir, yaocai_dict, top_n=10)
    else:
        dan_name = None
        yaocai_dict = await make_dict(yaocai_dict)
        mix_elixir_msgs = await get_mix_elixir_msg(yaocai_dict)  # 现在返回一个配方列表

    if not mix_elixir_msgs:  # 如果没有找到任何配方
        msg = "道友当前药材尚凑不出可炼丹方！"
        await handle_send(bot, event, msg, md_type="炼丹", k1="炼丹", v1="炼丹", k2="信息", v2="我的炼丹信息", k3="帮助", v3="炼丹帮助")
        await mix_elixir.finish()
    else:
        furnace_id, ldl_name = sorted(user_ldl_dict[user_id].items(), key=lambda x: x[0], reverse=False)[0]
        saved_recipes = [
            _saved_recipe(recipe, furnace_id, ldl_name) for recipe in mix_elixir_msgs
        ]
        event_id = str(getattr(event, "message_id", "") or getattr(event, "id", "") or "").strip()
        operation_id = f"mixelixir-recipe:{event_id}:{user_id}" if event_id else f"mixelixir-recipe:{user_id}:{time.time_ns()}"
        saved = mixelixir_recipe_service.save(
            operation_id,
            user_id,
            int(user_info.get("mixelixir_num", 0) or 0),
            _recipe_inventory_snapshot(user_back, "药材"),
            _recipe_inventory_snapshot(user_back, "炼丹炉"),
            saved_recipes,
        )
        if not saved.succeeded:
            await handle_send(bot, event, "背包或炼丹次数状态已变化，请重新生成丹方。")
            await mix_elixir.finish()
        if not dan_name:
            title = "炼丹配方"
        else:
            title = f"【{dan_name}】配方"
        msg_list = []  # 构建多个配方的消息
        for idx, mix_elixir_msg in enumerate(mix_elixir_msgs, 1):
            goods_info = items.get_data_by_item_id(mix_elixir_msg['id'])
            msg = f"配方{idx}：\n"
            msg += f"名字：{goods_info['name']}\n"
            msg += f"效果：{goods_info['desc']}\n"
            msg += f"配方：{mix_elixir_msg['配方简写']}丹炉{ldl_name}"
            msg += f"\n【药材清单】\n"
            msg += f"主药：{mix_elixir_msg['主药']},{mix_elixir_msg['主药_level']}，数量：{mix_elixir_msg['主药_num']}\n"
            msg += f"药引：{mix_elixir_msg['药引']},{mix_elixir_msg['药引_level']}，数量：{mix_elixir_msg['药引_num']}\n"
            if mix_elixir_msg['辅药_num'] != 0:
                msg += f"辅药：{mix_elixir_msg['辅药']},{mix_elixir_msg['辅药_level']}，数量：{mix_elixir_msg['辅药_num']}\n"
            msg_list.append(msg)

        # 将所有配方的消息合并发送
        await send_msg_handler(bot, event, '配方', bot.self_id, msg_list, title=title)
        await mix_elixir.finish()


# 配方
@mix_make.handle(parameterless=[Cooldown(cd_time=0)])
async def mix_make_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, mode: str = EventPlainText()):
    """配方,用来炼制丹药"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await mix_make.finish()
    user_id = user_info['user_id']
    pattern = r"主药([\u4e00-\u9fa5]+)(\d+)药引([\u4e00-\u9fa5]+)(\d+)辅药([\u4e00-\u9fa5]+)(\d+)丹炉([\u4e00-\u9fa5]+)+"
    matched = re.search(pattern, mode)
    if user_info['mixelixir_num'] >= 100:
        msg = "道友今日炼丹已达上限，请明日再来！"
        await handle_send(bot, event, msg, md_type="炼丹", k1="丹药", v1="丹药背包", k2="药材", v2="药材背包", k3="帮助", v3="炼丹帮助")
        await mix_make.finish()
    if matched is None:
        msg = f"请输入正确的配方！"
        await handle_send(bot, event, msg, md_type="炼丹", k1="炼丹", v1="配方", k2="药材", v2="药材背包", k3="帮助", v3="炼丹帮助")
        await mix_make.finish()
    else:
        zhuyao_name = matched.groups()[0]
        zhuyao_num = int(matched.groups()[1])  # 数量一定会有
        check, zhuyao_goods_id = await check_yaocai_name_in_back(user_id, zhuyao_name, zhuyao_num)
        if not check:
            msg = f"请检查主药：{zhuyao_name} 是否在背包中，或者数量是否足够！"
            await handle_send(bot, event, msg, md_type="炼丹", k1="炼丹", v1="配方", k2="信息", v2="我的炼丹信息", k3="丹药", v3="丹药背包")
            await mix_make.finish()
        yaoyin_name = matched.groups()[2]
        yaoyin_num = int(matched.groups()[3])  # 数量一定会有
        check, yaoyin_goods_id = await check_yaocai_name_in_back(user_id, yaoyin_name, yaoyin_num)
        if not check:
            msg = f"请检查药引：{yaoyin_name} 是否在背包中，或者数量是否足够！"
            await handle_send(bot, event, msg, md_type="炼丹", k1="炼丹", v1="配方", k2="信息", v2="我的炼丹信息", k3="丹药", v3="丹药背包")
            await mix_make.finish()
        fuyao_name = matched.groups()[4]
        fuyao_num = int(matched.groups()[5])
        check, fuyao_goods_id = await check_yaocai_name_in_back(user_id, fuyao_name, fuyao_num)
        if not check:
            msg = f"请检查辅药：{fuyao_name} 是否在背包中，或者数量是否足够！"
            await handle_send(bot, event, msg, md_type="炼丹", k1="炼丹", v1="配方", k2="信息", v2="我的炼丹信息", k3="丹药", v3="丹药背包")
            await mix_make.finish()
        if zhuyao_name == fuyao_name:
            check, fuyao_goods_id = await check_yaocai_name_in_back(user_id, fuyao_name, fuyao_num + zhuyao_num)
            if not check:
                msg = f"请检查主药：{zhuyao_name} 是否在背包中，或者数量是否足够！"
                await handle_send(bot, event, msg, md_type="炼丹", k1="炼丹", v1="配方", k2="信息", v2="我的炼丹信息", k3="丹药", v3="丹药背包")
                await mix_make.finish()
        if yaoyin_name == fuyao_name:
            check, fuyao_goods_id = await check_yaocai_name_in_back(user_id, fuyao_name, fuyao_num + yaoyin_num)
            if not check:
                msg = f"请检查药引：{yaoyin_name} 是否在背包中，或者数量是否足够！"
                await handle_send(bot, event, msg, md_type="炼丹", k1="炼丹", v1="配方", k2="信息", v2="我的炼丹信息", k3="丹药", v3="丹药背包")
                await mix_make.finish()

        ldl_name = matched.groups()[6]
        check, ldl_info = await check_ldl_name_in_back(user_id, ldl_name)
        if not check:
            msg = f"请检查炼丹炉：{ldl_name} 是否在背包中！"
            await handle_send(bot, event, msg, md_type="炼丹", k1="炼丹", v1="配方", k2="信息", v2="我的炼丹信息", k3="丹药", v3="丹药背包")
            await mix_make.finish()
        # 检测通过
        zhuyao_info = Items().get_data_by_item_id(zhuyao_goods_id)
        yaoyin_info = Items().get_data_by_item_id(yaoyin_goods_id)
        if await tiaohe(zhuyao_info, zhuyao_num, yaoyin_info, yaoyin_num):  # 调和失败
            msg = f"冷热调和失败！小心炸炉哦~"
            await handle_send(bot, event, msg, md_type="炼丹", k1="炼丹", v1="配方", k2="信息", v2="我的炼丹信息", k3="丹药", v3="丹药背包")
            await mix_make.finish()
        else:
            saved_recipe = mixelixir_recipe_service.find(user_id, mode)
            if saved_recipe is None:
                await handle_send(bot, event, "该丹方未保存或已经使用，请先重新执行炼丹生成丹方。")
                await mix_make.finish()
            recipe_set_id, recipe_snapshot = saved_recipe
            elixir_config = {
                str(zhuyao_info['主药']['type']): zhuyao_info['主药']['power'] * zhuyao_num
            }
            fuyao_info = Items().get_data_by_item_id(fuyao_goods_id)
            elixir_config[str(fuyao_info['辅药']['type'])] = fuyao_info['辅药']['power'] * fuyao_num
            is_mix, id = await check_mix(elixir_config)
            if is_mix:
                mix_elixir_info = get_player_info(user_id, 'mix_elixir_info')
                goods_info = Items().get_data_by_item_id(id)
                # 加入传承
                impart_data = xiuxian_impart.get_user_impart_info_with_id(user_id)
                impart_mix_per = impart_data['impart_mix_per'] if impart_data is not None else 0
                # 功法炼丹数加成
                main_dan_data = UserBuffDate(user_id).get_user_main_buff_data()

                if main_dan_data != None:  # 功法炼丹数量加成
                    main_dan = main_dan_data['dan_buff']
                else:
                    main_dan = 0
                # 功法炼丹经验加成
                main_dan_exp = UserBuffDate(user_id).get_user_main_buff_data()

                if main_dan_exp != None:  # 功法炼丹经验加成
                    main_exp = main_dan_exp['dan_exp']
                else:
                    main_exp = 0

                num = 1 + ldl_info['buff'] + mix_elixir_info['丹药控火'] + impart_mix_per + main_dan  # 炼丹数量提升
                expected_mix_state = {
                    "丹药控火": mix_elixir_info["丹药控火"],
                    "炼丹记录": mix_elixir_info["炼丹记录"],
                    "炼丹经验": mix_elixir_info["炼丹经验"],
                }
                updated_mix_state = json.loads(json.dumps(expected_mix_state, ensure_ascii=False))
                records = updated_mix_state["炼丹记录"]
                record_key = str(id)
                current = records.get(record_key, {"name": goods_info["name"], "num": 0})
                now_num = int(current.get("num", 0) or 0)
                exp_count = max(0, min(num, int(goods_info["mix_all"]) - now_num))
                exp_gain = (int(goods_info["mix_exp"]) + int(main_exp)) * exp_count
                records[record_key] = {"name": goods_info["name"], "num": now_num + num}
                updated_mix_state["炼丹经验"] = int(updated_mix_state["炼丹经验"]) + exp_gain

                event_id = str(getattr(event, "message_id", "") or getattr(event, "id", "") or "").strip()
                operation_id = f"mixelixir-cost:{event_id}:{user_id}" if event_id else f"mixelixir-cost:{user_id}:{time.time_ns()}"
                started = mixelixir_refine_cost_service.start(
                    operation_id,
                    user_id,
                    recipe_set_id,
                    recipe_snapshot["recipe_key"],
                    int(user_info.get("mixelixir_num", 0) or 0),
                    num,
                    expected_mix_state,
                    updated_mix_state,
                )
                if started.status == "duplicate":
                    claim_operation = f"mixelixir-reward:{event_id}:{user_id}" if event_id else f"mixelixir-reward:{user_id}:{time.time_ns()}"
                    claimed = mixelixir_refine_reward_service.claim(
                        claim_operation, user_id, started.task_id, XiuConfig().max_goods_num
                    )
                    if claimed.succeeded:
                        msg = f"恭喜道友成功炼成丹药：{claimed.reward_name}{claimed.reward_quantity}枚\n该炼丹请求已经处理，无需重复提交。"
                        await handle_send(bot, event, msg, md_type="炼丹", k1="炼丹", v1="配方", k2="信息", v2="我的炼丹信息", k3="丹药", v3="丹药背包")
                        await mix_make.finish()
                if not started.succeeded:
                    msg = "药材、丹炉或炼丹状态已变化，本次未消耗药材，请重新生成丹方。"
                    await handle_send(bot, event, msg, md_type="炼丹", k1="炼丹", v1="炼丹", k2="信息", v2="我的炼丹信息", k3="药材", v3="药材背包")
                    await mix_make.finish()

                claim_operation = f"mixelixir-reward:{event_id}:{user_id}" if event_id else f"mixelixir-reward:{user_id}:{time.time_ns()}"
                claimed = mixelixir_refine_reward_service.claim(
                    claim_operation, user_id, started.task_id, XiuConfig().max_goods_num
                )
                if claimed.status == "duplicate":
                    msg = f"恭喜道友成功炼成丹药：{claimed.reward_name}{claimed.reward_quantity}枚\n该炼丹请求已经处理，无需重复提交。"
                    await handle_send(bot, event, msg, md_type="炼丹", k1="炼丹", v1="配方", k2="信息", v2="我的炼丹信息", k3="丹药", v3="丹药背包")
                    await mix_make.finish()
                if not claimed.succeeded:
                    msg = "丹药领取状态已变化，材料消耗记录已保留，请重新提交同一配方领取。"
                    await handle_send(bot, event, msg, md_type="炼丹", k1="炼丹", v1="配方", k2="信息", v2="我的炼丹信息", k3="丹药", v3="丹药背包")
                    await mix_make.finish()
                msg = f"恭喜道友成功炼成丹药：{claimed.reward_name}{claimed.reward_quantity}枚"
                if exp_gain:
                    msg += f"\n获得炼丹经验{exp_gain}点"
                await handle_send(bot, event, msg, md_type="炼丹", k1="炼丹", v1="配方", k2="信息", v2="我的炼丹信息", k3="丹药", v3="丹药背包")
                await mix_make.finish()
            else:
                msg = f"没有炼成丹药哦~就不扣你药材啦"
                await handle_send(bot, event, msg, md_type="炼丹", k1="炼丹", v1="配方", k2="信息", v2="我的炼丹信息", k3="丹药", v3="丹药背包")
                await mix_make.finish()


async def check_yaocai_name_in_back(user_id, yaocai_name, yaocai_num):
    flag = False
    goods_id = 0
    user_back = sql_message.get_back_msg(user_id) or []
    for back in user_back:
        if back['goods_type'] == '药材':
            if Items().get_data_by_item_id(back['goods_id'])['name'] == yaocai_name:
                if int(back['goods_num']) >= int(yaocai_num) and int(yaocai_num) >= 1:
                    flag = True
                    goods_id = back['goods_id']
                    break
            else:
                continue
        else:
            continue
    return flag, goods_id


async def check_ldl_name_in_back(user_id, ldl_name):
    flag = False
    goods_info = {}
    user_back = sql_message.get_back_msg(user_id) or []
    for back in user_back:
        if back['goods_type'] == '炼丹炉':
            if back['goods_name'] == ldl_name:
                flag = True
                goods_info = Items().get_data_by_item_id(back['goods_id'])
                break
            else:
                continue
        else:
            continue
    return flag, goods_info
