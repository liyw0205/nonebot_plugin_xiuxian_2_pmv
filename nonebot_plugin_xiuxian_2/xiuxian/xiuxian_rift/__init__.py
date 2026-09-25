import hashlib
import random
import time
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from nonebot import get_bots, get_bot
from ...paths import get_paths
from ...infrastructure.ids import UUIDGenerator
from ...infrastructure.clock import SystemClock
from ..on_compat import on_command
from nonebot.params import CommandArg
from ..adapter_compat import (
    Bot,
    GROUP,
    Message,
    GroupMessageEvent,
    PrivateMessageEvent,
    MessageSegment
)
from ..messaging import delivery_service
from .old_rift_info import GLOBAL_RIFT_KEY, old_rift_info
from .. import DRIVER
from ...bootstrap.legacy import register_legacy_shutdown, register_legacy_startup
from ..xiuxian_utils.lay_out import assign_bot, assign_bot_group, Cooldown
from nonebot.log import logger
from ..xiuxian_utils import db_backend
from ..xiuxian_utils.utils import (
    check_user, check_user_type,
    send_msg_handler, get_msg_pic, log_message, handle_send,
    build_md_command_link, number_to,
)
from .riftconfig import get_rift_config
from .jsondata import save_rift_data, read_rift_data
from ...features.rift.application import RiftApplication
from ...features.rift.domain import (
    RiftBossBattleResolver,
    RiftDamageEventResolver,
    RiftTreasureResolver,
)

from ..xiuxian_config import XiuConfig, convert_rank
from ..xiuxian_map import (
    get_player_current_position,
    get_random_trial_node,
    get_random_trial_nodes_by_realm,
)
from ..xiuxian_utils.player_fight import Boss_fight
from . import jsondata
from .riftmake import (
    STORY, Rift, get_rift_type, get_story_type, NONEMSG, get_battle_type,
    TREASUREMSG, TREASUREMSG_1, TREASUREMSG_2, TREASUREMSG_3, TREASUREMSG_4,
    TREASUREMSG_5, get_armor, get_main_info, get_sec_info, get_sub_info,
    get_weapon, items,
)
from ..xiuxian_utils.numeric_bind import percent_exp_reward

runtime_clock = SystemClock()
rift_application = RiftApplication(
    get_paths().game_db,
    get_paths().player_db,
    damage_event_resolver=RiftDamageEventResolver(
        battle_config=STORY['战斗'],
        exp_reward=percent_exp_reward,
        format_number=number_to,
    ),
    boss_battle_resolver=RiftBossBattleResolver(
        boss_config=STORY['战斗']['Boss战斗'],
        battle_runner=Boss_fight,
        rank_score=lambda level: convert_rank(level)[0],
        level_power=lambda level: jsondata.level_data()[level]["power"],
        max_exp_factor=XiuConfig().closing_exp_upper_limit * 0.1,
        exp_reward=percent_exp_reward,
        format_number=number_to,
    ),
    treasure_resolver=RiftTreasureResolver(
        treasure_config=STORY["宝物"],
        messages={
            "法器": TREASUREMSG,
            "防具": TREASUREMSG_1,
            "功法": TREASUREMSG_2,
            "神通": TREASUREMSG_3,
            "灵石": TREASUREMSG_4,
            "辅修功法": TREASUREMSG_5,
        },
        weapon_provider=get_weapon,
        armor_provider=get_armor,
        main_provider=get_main_info,
        secondary_provider=get_sec_info,
        sub_provider=get_sub_info,
        item_lookup=items.get_data_by_item_id,
        format_number=number_to,
    ),
    clock=runtime_clock,
)
cache_help = {}
group_rift = {}  # dict
config = get_rift_config() # 获取秘境配置
runtime_ids = UUIDGenerator()
groups = config['open']  # list


def _event_id(event) -> str:
    return str(
        getattr(event, "message_id", "") or getattr(event, "id", "") or ""
    ).strip()


def _parse_rift_datetime(value) -> datetime:
    from ..xiuxian_utils.cd_time import is_blank_cd_time, parse_cd_datetime

    if isinstance(value, datetime):
        return value
    # 坏/空时间：用 epoch，elapsed → 极大，避免 type=3 永远 not_ready
    if is_blank_cd_time(value):
        return datetime(1970, 1, 1)
    parsed = parse_cd_datetime(value, default=None)
    if parsed is None:
        return datetime(1970, 1, 1)
    return parsed


def _rift_elapsed_minutes(value, *, now: datetime | None = None) -> int:
    from ..xiuxian_utils.cd_time import is_blank_cd_time, parse_cd_datetime

    # 坏/空/不可解析时间：给足时长，让 type=3 能走结算而不是卡「尚未到结算时间」
    if is_blank_cd_time(value):
        return 10**9
    if not isinstance(value, datetime) and parse_cd_datetime(value, default=None) is None:
        return 10**9
    try:
        started_at = _parse_rift_datetime(value)
        current = now or runtime_clock.now()
        if current.tzinfo is None:
            current = current.replace(tzinfo=timezone.utc)
        if started_at.tzinfo is None:
            current = current.astimezone(timezone.utc).replace(tzinfo=None)
        else:
            current = current.astimezone(started_at.tzinfo)
        return max(0, int((current - started_at).total_seconds() // 60))
    except Exception:
        return 10**9



def _rift_operation_seed(operation_id: str, scope: str) -> int:
    return int.from_bytes(
        hashlib.sha256(f"{scope}:{operation_id}".encode("utf-8")).digest(),
        "big",
    )


def _scheduled_generation_operation_id(now: datetime | None = None) -> str:
    now = now or runtime_clock.now()
    slot = 0 if now.hour < 12 else 12
    return f"rift-generation:scheduled:{now:%Y-%m-%d}:{slot:02d}"


def _build_fixed_rift(operation_id: str) -> Rift:
    """Roll the complete rift plan deterministically for an operation."""
    random_state = random.getstate()
    seed = int.from_bytes(
        hashlib.sha256(str(operation_id).encode("utf-8")).digest(), "big"
    )
    try:
        random.seed(seed)
        rift = Rift()
        rift.name = get_rift_type()
        rift.rank = config['rift'][rift.name]['rank']
        rift.time = config['rift'][rift.name]['time']
        assign_rift_trial_node(rift)
        return rift
    finally:
        random.setstate(random_state)


def _rift_world_snapshot(rift: Rift) -> dict:
    value = build_rift_data(rift)
    value["l_user_id"] = [str(user_id) for user_id in rift.l_user_id]
    return value


def _rift_from_world_state(state) -> Rift:
    rift = Rift()
    for field_name, value in state.rift_data.items():
        setattr(rift, field_name, value)
    rift.l_user_id = list(state.participants)
    return rift


def _sync_world_projection(state, *, save_legacy=True) -> Rift:
    rift = _rift_from_world_state(state)
    group_rift[state.rift_key] = rift
    if save_legacy:
        try:
            old_rift_info.save_rift(group_rift)
        except Exception as exc:
            logger.error(f"同步秘境全局兼容缓存失败: {exc}")
    return rift


def _generation_outcome(outcome):
    data = dict(outcome.data or {})
    state_data = data.get("state")
    state = SimpleNamespace(**state_data) if isinstance(state_data, dict) else None
    return SimpleNamespace(
        status="duplicate" if outcome.replayed else data.get("status", outcome.code),
        state=state,
        succeeded=outcome.ok,
    )


def _termination_outcome(outcome):
    data = dict(outcome.data or {})
    return SimpleNamespace(
        status="duplicate" if outcome.replayed else data.get("status", outcome.code),
        rift_name=str(data.get("rift_name", "")),
        succeeded=outcome.ok,
    )


def _load_current_rift():
    state_data = rift_application.current_world(rift_key=GLOBAL_RIFT_KEY)
    if state_data is None:
        return None, None
    state = SimpleNamespace(**state_data)
    return state, _sync_world_projection(state, save_legacy=False)


def _sync_entry_projection(user_id, entry) -> None:
    state_data = rift_application.current_world(rift_key=GLOBAL_RIFT_KEY)
    state = SimpleNamespace(**state_data) if state_data is not None else None
    if state is not None:
        _sync_world_projection(state)
    try:
        save_rift_data(user_id, entry.rift_data)
    except Exception as exc:
        logger.error(f"同步用户 {user_id} 秘境兼容缓存失败: {exc}")

my_rift_count = on_command("秘境次数", aliases={"秘境进度"}, priority=7, block=True)
explore_rift = on_command("探索秘境", priority=5, block=True)
rift_help = on_command("秘境帮助", priority=6, block=True)
complete_rift = on_command("秘境结算", aliases={"结算秘境"}, priority=7, block=True)
break_rift = on_command("秘境终止", aliases={"终止秘境"}, priority=7, block=True)

__rift_help__ = """
**秘境帮助**
---
**指令**
- 探索秘境
> 进入秘境获取奖励
- 秘境结算
> 领取秘境奖励
- 秘境终止
> 放弃当前秘境（可能损失奖励）
- 秘境次数
> 查看秘境保底与次数

**刷新**
- 每日自动生成
> 0点与12点
- 秘境等级随机

> 探索时间越长奖励越好；可用道具提升收益。中途终止可能损失奖励。
""".strip()

__rift_help_md__ = f"""
**秘境帮助**
---
**指令**
- {build_md_command_link("探索秘境")}
> 进入秘境获取奖励
- {build_md_command_link("秘境结算")}
> 领取秘境奖励
- {build_md_command_link("秘境终止")}
> 放弃当前秘境（可能损失奖励）
- {build_md_command_link("秘境次数")}
> 查看秘境保底与次数

**刷新**
- 每日自动生成
> 0点与12点
- 秘境等级随机

> 探索时间越长奖励越好；可用道具提升收益。中途终止可能损失奖励。

---
{build_md_command_link("探索", "探索秘境")} | {build_md_command_link("结算", "秘境结算")} | {build_md_command_link("存档", "我的修仙信息")}
""".strip()



@register_legacy_startup
async def read_rift_():
    """读取历史秘境数据"""
    legacy = old_rift_info.read_rift_info()
    state_data = rift_application.current_world(rift_key=GLOBAL_RIFT_KEY)
    if state_data is None and GLOBAL_RIFT_KEY in legacy:
        state_data = rift_application.bootstrap_world(
            rift_key=GLOBAL_RIFT_KEY,
            legacy_snapshot=_rift_world_snapshot(legacy[GLOBAL_RIFT_KEY]),
        )
    state = SimpleNamespace(**state_data) if state_data is not None else None
    if state is not None:
        _sync_world_projection(state)
    logger.opt(colors=True).info("<green>历史rift数据读取成功</green>")

@register_legacy_shutdown
async def save_rift_():
    """保存秘境数据"""
    state_data = rift_application.current_world(rift_key=GLOBAL_RIFT_KEY)
    state = SimpleNamespace(**state_data) if state_data is not None else None
    if state is not None:
        _sync_world_projection(state)
    logger.opt(colors=True).info(f"<green>rift数据已保存</green>")

# 定时任务生成秘境
async def scheduled_rift_generation():
    """
    定时任务：每天0,12点触发秘境生成
    """
    await generate_rift_for_group()   
    
    logger.info("秘境定时生成完成")

      
async def generate_rift_for_group():
    """为群组生成新的秘境"""
    operation_id = _scheduled_generation_operation_id()
    rift = _build_fixed_rift(operation_id)
    result = _generation_outcome(
        rift_application.generate(
            operation_id=operation_id,
            rift_key=GLOBAL_RIFT_KEY,
            rift_plan=_rift_world_snapshot(rift),
        )
    )
    if not result.succeeded or result.state is None:
        logger.warning(f"秘境生成状态冲突: {result.status}")
        return result
    rift = _sync_world_projection(result.state)
    msg = build_rift_appear_msg(rift)
    logger.info(msg)
    if result.status == "applied":
        for notify_group_id in groups:
            bot = get_bot()
            await delivery_service.send_to_group(bot, notify_group_id, msg)
    return result


def _normalise_rift_target_node(node_info: dict) -> dict:
    return {
        "realm": node_info.get("realm", ""),
        "heaven": node_info.get("heaven", ""),
        "node_id": node_info.get("node_id", ""),
        "node_name": node_info.get("node_name", ""),
        "node_type": node_info.get("node_type", ""),
    }


def get_rift_target_nodes(rift: Rift) -> list[dict]:
    target_nodes = [
        _normalise_rift_target_node(node)
        for node in getattr(rift, "target_nodes", [])
        if isinstance(node, dict) and node.get("node_id")
    ]
    if target_nodes:
        return target_nodes

    if getattr(rift, "target_node_id", ""):
        return [{
            "realm": getattr(rift, "target_realm", ""),
            "heaven": getattr(rift, "target_heaven", ""),
            "node_id": getattr(rift, "target_node_id", ""),
            "node_name": getattr(rift, "target_node_name", ""),
            "node_type": "试炼",
        }]

    return []


def format_rift_target_nodes(rift: Rift) -> str:
    target_nodes = get_rift_target_nodes(rift)
    return "\n".join(
        f"{index}. {node['realm']}·{node['heaven']}·{node['node_name']}"
        for index, node in enumerate(target_nodes, 1)
    )


def _entry_success_message(rift_data: dict, *, bypass_position=False) -> str:
    target_nodes = [
        node
        for node in rift_data.get("target_nodes", [])
        if isinstance(node, dict)
    ]
    target_msg = "\n".join(
        f"{index}. {node.get('realm', '')}·{node.get('heaven', '')}·"
        f"{node.get('node_name', '')}"
        for index, node in enumerate(target_nodes, 1)
    )
    suffix = ""
    if target_msg:
        bypass_msg = "\n秘藏令已绕过位置要求。" if bypass_position else ""
        suffix = f"{bypass_msg}\n秘境可探索地点：\n{target_msg}"
    return (
        f"进入秘境：{rift_data['name']}，探索需要花费时间："
        f"{rift_data['time']}分钟！{suffix}"
    )


def assign_rift_trial_node(rift: Rift):
    """给秘境在每一界各绑定一个地图试炼节点。"""
    node_infos = get_random_trial_nodes_by_realm()
    if not node_infos:
        node_info = get_random_trial_node()
        node_infos = [node_info] if node_info else []
    if not node_infos:
        return rift
    rift.target_nodes = [_normalise_rift_target_node(node_info) for node_info in node_infos]
    first_node = rift.target_nodes[0]
    rift.target_realm = first_node["realm"]
    rift.target_heaven = first_node["heaven"]
    rift.target_node_id = first_node["node_id"]
    rift.target_node_name = first_node["node_name"]
    return rift


def build_rift_appear_msg(rift: Rift) -> str:
    target_msg = format_rift_target_nodes(rift)
    if target_msg:
        return (
            f"野生的{rift.name}出现在以下地点：\n"
            f"{target_msg}\n"
            f"请诸位道友前往任一目标节点后发送 探索秘境 来加入吧！"
        )
    return f"野生的{rift.name}出现了！请诸位道友发送 探索秘境 来加入吧！"


def build_rift_data(rift: Rift) -> dict:
    target_nodes = get_rift_target_nodes(rift)
    first_node = target_nodes[0] if target_nodes else {}
    return {
        "name": rift.name,
        "time": rift.time,
        "rank": rift.rank,
        "target_nodes": target_nodes,
        "target_realm": first_node.get("realm", getattr(rift, "target_realm", "")),
        "target_heaven": first_node.get("heaven", getattr(rift, "target_heaven", "")),
        "target_node_id": first_node.get("node_id", getattr(rift, "target_node_id", "")),
        "target_node_name": first_node.get("node_name", getattr(rift, "target_node_name", "")),
    }


def check_rift_target_position(user_id: int, rift: Rift) -> tuple[bool, str]:
    """普通探索需要玩家位于秘境绑定的任一试炼节点。"""
    target_nodes = get_rift_target_nodes(rift)
    if not target_nodes:
        return True, ""

    current = get_player_current_position(str(user_id))
    target_positions = {
        (node["realm"], node["heaven"], node["node_id"])
        for node in target_nodes
    }
    if current and (
        current.get("realm"),
        current.get("heaven"),
        current.get("node_id"),
    ) in target_positions:
        return True, ""

    target_msg = format_rift_target_nodes(rift)
    if current:
        current_msg = f"{current['realm']}·{current['heaven']}·{current['node_name']}"
        return False, f"本次秘境可在以下地点探索：\n{target_msg}\n道友当前在【{current_msg}】，请先前往任一目标节点再探索。"
    return False, f"本次秘境可在以下地点探索：\n{target_msg}\n请先前往任一目标节点再探索。"

def update_rift_explore_count(user_id: int, do_give: bool = True) -> str:
    """Return the authoritative progress without mutating query state."""
    count = _rift_progress_snapshot(user_id)
    need = 10 - count
    if need <= 0:
        return (
            f"道友当前秘境完成次数：{count}/10\n"
            "已可领取秘境奖励，请进行一次秘境结算来领取！"
        )
    return (
        f"道友当前秘境完成次数：{count}/10\n"
        f"再完成 {need} 次即可获得秘境奖励"
    )

@my_rift_count.handle(parameterless=[Cooldown(cd_time=3)])
async def show_rift_progress(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """显示秘境探索进度"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await my_rift_count.finish()

    user_id = user_info['user_id']

    # 只查询，不赠送
    progress_msg = update_rift_explore_count(user_id, do_give=False)

    await handle_send(bot, event, progress_msg)
    await my_rift_count.finish()

@rift_help.handle(parameterless=[Cooldown(cd_time=0)])
async def rift_help_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """秘境帮助"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    await handle_send(
        bot,
        event,
        __rift_help_md__,
        native_markdown=True,
        fallback_msg=__rift_help__,
    )
    await rift_help.finish()

async def create_rift(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """生成秘境（手动触发，通常由管理员使用）"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    operation_id = f"rift-generation:manual:{_event_id(event) or runtime_ids.new_id()}"
    rift = _build_fixed_rift(operation_id)
    result = _generation_outcome(
        rift_application.generate(
            operation_id=operation_id,
            rift_key=GLOBAL_RIFT_KEY,
            rift_plan=_rift_world_snapshot(rift),
        )
    )
    if not result.succeeded or result.state is None:
        await handle_send(bot, event, "生成未覆盖：当前已有秘境，或生成请求冲突，本次未改写。")
        return
    rift = _sync_world_projection(result.state)
    msg = build_rift_appear_msg(rift)
    await handle_send(bot, event, msg, md_type="秘境", k1="探索", v1="探索秘境", k2="结算", v2="秘境结算", k3="帮助", v3="秘境帮助")
    return


@explore_rift.handle(parameterless=[Cooldown(cd_time=0.5)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """探索秘境"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await explore_rift.finish()
    user_id = user_info['user_id']
    event_id = _event_id(event)
    operation_id = f"rift-entry:{event_id or runtime_ids.new_id()}:{user_id}"
    is_type, msg = check_user_type(user_id, 0)  # 需要无状态的用户
    if not is_type:
        await handle_send(bot, event, msg, md_type="0", k2="修仙帮助", v2="修仙帮助", k3="秘境帮助", v3="秘境帮助")
        await explore_rift.finish()
    else:
        current_state, current_rift = _load_current_rift()
        if current_state is None or current_rift is None:
            msg = '野外秘境尚未生成，请道友耐心等待!'
            await handle_send(bot, event, msg)
            await explore_rift.finish()
        if str(user_id) in current_rift.l_user_id:
            msg = '道友已经参加过本次秘境啦，请把机会留给更多的道友！'
            await handle_send(bot, event, msg)
            await explore_rift.finish()
        
        user_rank = convert_rank(user_info["level"])[0]
        required_rank_for_check = convert_rank("感气境中期")[0] - current_rift.rank
         
        if user_rank > required_rank_for_check:
            rank_name_list = convert_rank(user_info["level"])[1] # 获取用户境界的文字描述列表
            
            msg = f"秘境凶险万分，道友的境界不足，无法进入秘境：{current_rift.name}，请道友提升境界后再来！"
            await handle_send(bot, event, msg)
            await explore_rift.finish()

        can_reach_rift, position_msg = check_rift_target_position(user_id, current_rift)
        if not can_reach_rift:
            await handle_send(bot, event, position_msg)
            await explore_rift.finish()

        rift_data = build_rift_data(current_rift)
        stamina_cost = (
            0 if str(user_id) in DRIVER.config.superusers else 6
        )
        try:
            entry_outcome = rift_application.enter(
                operation_id=operation_id,
                user_id=user_id,
                rift_key=GLOBAL_RIFT_KEY,
                rift_data=rift_data,
                duration=rift_data["time"],
                expected_generation_id=current_state.generation_id,
                expected_revision=current_state.revision,
                stamina_cost=stamina_cost,
                expected_stamina=int(user_info.get("user_stamina", 0)),
            )
            entry_data = entry_outcome.data or {}
            entry_status = str(entry_data.get("status", entry_outcome.code))
            entry = type("EntryResult", (), {"succeeded": entry_outcome.ok, "status": entry_status, "rift_data": entry_data.get("rift_data", rift_data)})()
            if entry_outcome.replayed:
                _sync_entry_projection(user_id, entry)
                await handle_send(bot, event, _entry_success_message(entry.rift_data))
                await explore_rift.finish()
        except Exception as exc:
            logger.opt(exception=exc).error("秘境进入事务执行失败")
            await handle_send(bot, event, "秘境进入失败：请求未生效。")
            await explore_rift.finish()
        if not entry.succeeded:
            if entry.status == "already_joined":
                await handle_send(bot, event, "道友已经参加过本次秘境啦，请把机会留给更多的道友！")
                await explore_rift.finish()
            if entry.status == "stamina_missing":
                await handle_send(bot, event, "你没有足够的体力，请等待体力恢复后再试！")
                await explore_rift.finish()
            await handle_send(bot, event, "进入秘境未完成：秘境状态已更新，请重新进入。")
            await explore_rift.finish()
        _sync_entry_projection(user_id, entry)
        msg = _entry_success_message(entry.rift_data)
        await handle_send(bot, event, msg, md_type="秘境", k1="结算", v1="秘境结算", k2="加速", v2="道具使用 秘境加速券", k3="大加速", v3="道具使用 秘境大加速券", k4="钥匙", v4="道具使用 秘境钥匙")
        await explore_rift.finish()

async def use_rift_explore(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, item_id, quantity):
    """使用秘藏令"""
    async def _check_and_enter_rift(user_id, user_info, bot, event):
        current_state, current_rift = _load_current_rift()
        if current_state is None or current_rift is None:
            return False, '野外秘境尚未生成，请道友耐心等待!'
                
        user_rank = convert_rank(user_info["level"])[0]
        required_rank_for_check = convert_rank("感气境中期")[0] - current_rift.rank
        
        if user_rank > required_rank_for_check:
            return False, f"秘境凶险万分，道友的境界不足，无法进入秘境：{current_rift.name}，请道友提升境界后再来！"
        
        return True, (current_state, current_rift)

    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return
    user_id = user_info['user_id']
    event_id = _event_id(event)
    operation_id = f"rift-ticket-entry:{event_id or runtime_ids.new_id()}:{user_id}"
    is_type, msg = check_user_type(user_id, 0)  # 需要无状态的用户
    if not is_type:
        await handle_send(bot, event, msg, md_type="0", k2="修仙帮助", v2="修仙帮助", k3="秘境帮助", v3="秘境帮助")
        return
    else:
        can_enter, check_msg_or_rift = await _check_and_enter_rift(user_id, user_info, bot, event)
        if not can_enter:
            await handle_send(bot, event, check_msg_or_rift)
            return

        current_state, current_rift = check_msg_or_rift

        rift_data = build_rift_data(current_rift)
        try:
            entry_outcome = rift_application.enter(
                operation_id=operation_id,
                user_id=user_id,
                rift_key=GLOBAL_RIFT_KEY,
                rift_data=rift_data,
                duration=rift_data["time"],
                item_id=item_id,
                expected_generation_id=current_state.generation_id,
                expected_revision=current_state.revision,
            )
            entry_data = entry_outcome.data or {}
            entry_status = str(entry_data.get("status", entry_outcome.code))
            if entry_outcome.replayed:
                await handle_send(bot, event, _entry_success_message(entry_data.get("rift_data", rift_data), bypass_position=True))
                return
            entry = type("EntryResult", (), {"succeeded": entry_outcome.ok, "status": entry_status, "rift_data": entry_data.get("rift_data", rift_data)})()
        except Exception as exc:
            logger.opt(exception=exc).error("秘藏令进入秘境事务执行失败")
            await handle_send(bot, event, "秘藏令进入失败：请求未生效。")
            return
        if not entry.succeeded:
            if entry.status == "already_joined":
                await handle_send(bot, event, "道友已经参加过本次秘境啦，请把机会留给更多的道友！")
                return
            if entry.status == "ticket_missing":
                await handle_send(bot, event, "秘藏令数量不足，请重新查看背包。")
                return
            await handle_send(bot, event, "开启秘境未完成：秘境状态已更新，请重新开启。")
            return
        _sync_entry_projection(user_id, entry)
        msg = _entry_success_message(entry.rift_data, bypass_position=True)
        await handle_send(bot, event, msg, md_type="秘境", k1="结算", v1="秘境结算", k2="加速", v2="道具使用 秘境加速券", k3="大加速", v3="道具使用 秘境大加速券", k4="钥匙", v4="道具使用 秘境钥匙")
        return

def _rift_progress_snapshot(user_id):
    with db_backend.connection(get_paths().player_db) as conn:
        if not conn.table_exists("rift") or not conn.column_exists(
            "rift", "explore_count"
        ):
            return 0
        row = conn.execute(
            'SELECT "explore_count" FROM "rift" WHERE user_id=%s',
            (str(user_id),),
        ).fetchone()
        return int(row[0] or 0) if row else 0


def _roll_rift_progress(count, operation_id=""):
    if int(count) + 1 < 10:
        new_count = int(count) + 1
        return None, f"\n当前秘境完成次数：{new_count}/10（再完成 {10 - new_count} 次可获秘境奖励）"
    rewards = {
        "秘藏令": 20007, "秘境钥匙": 20001, "神秘经书·残": 20008, "神秘经书": 20009,
        "灵签宝箓": 20010, "秘境加速券": 20012, "秘境大加速券": 20013,
        "斩妖令": 20018, "解绑符": 20019,
    }
    rng = (
        random.Random(_rift_operation_seed(operation_id, "progress"))
        if operation_id
        else random
    )
    name, item_id = rng.choice(list(rewards.items()))
    reward = {"id": item_id, "name": name, "type": "特殊道具", "amount": 1}
    return reward, f"\n【秘境累计完成10次！】\n赠送道友 {name} x1！"


async def _roll_rift_event(user_info, rift_info, bot_id, operation_id=""):
    """Fix the random event, battle result and all persistence deltas."""
    random_state = random.getstate()
    if operation_id:
        random.seed(_rift_operation_seed(operation_id, "event"))
    try:
        rift_rank = rift_info["rank"]  # 秘境等级
        rift_type = get_story_type()  # 无事、宝物、战斗
        battle_result = None
        result_msg = ""
        result_name = None
        outcome = {"delta": {}, "items": [], "statistics": {}}

        if rift_type == "无事":
            result_msg = random.choice(NONEMSG)
        elif rift_type == "战斗":
            battle_type = get_battle_type()
            if battle_type == "掉血事件":
                outcome = rift_application.roll_damage_event(
                    "掉血事件", user_info, random_source=random
                )
                result_msg = outcome["message"]
            elif battle_type == "Boss战斗":
                battle_result, result_msg, outcome = await rift_application.roll_boss_battle(
                    user_info, rift_rank, bot_id, random_source=random
                )
        elif rift_type == "宝物":
            result_name, result_msg, outcome = rift_application.roll_treasure(
                user_info, rift_rank, random_source=random
            )
        outcome["message"] = result_msg
        return battle_result, result_name, outcome
    finally:
        random.setstate(random_state)


async def _roll_rift_boss_event(user_info, rift_rank, bot_id, operation_id):
    random_state = random.getstate()
    random.seed(_rift_operation_seed(operation_id, "demon-token"))
    try:
        return await rift_application.roll_boss_battle(
            user_info, rift_rank, bot_id, random_source=random
        )
    finally:
        random.setstate(random_state)

@complete_rift.handle(parameterless=[Cooldown(cd_time=0)])
async def complete_rift_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """秘境结算"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await complete_rift.finish()

    user_id = user_info['user_id']
    event_id = _event_id(event)
    operation_id = f"rift-settlement:{event_id or runtime_ids.new_id()}:{user_id}"
    if event_id:
        replay = rift_application.replay_settlement(operation_id=operation_id)
        if replay is not None:
            await handle_send(bot, event, replay.message)
            await complete_rift.finish()

    is_type, msg = check_user_type(user_id, 3)  # 需要在秘境的用户
    if not is_type:
        await handle_send(bot, event, msg, md_type="3", k2="修仙帮助", v2="修仙帮助", k3="秘境帮助", v3="秘境帮助")
        await complete_rift.finish()
    else:
        rift_info = None
        try:
            rift_info = read_rift_data(user_id)
        except Exception as e:
            logger.error(f"读取用户 {user_id} 秘境数据失败: {e}")
            msg = '发生未知错误，秘境数据读取失败！'
            await handle_send(bot, event, msg)
            await complete_rift.finish()

        try:
            user_cd_message = rift_application.read_cooldown(user_id)
            exp_time = _rift_elapsed_minutes(user_cd_message['create_time'])
            time2 = int(rift_info["time"])
        except Exception as exc:
            logger.opt(exception=exc).error("秘境结算时间读取失败")
            await handle_send(bot, event, "秘境结算失败：请求未生效。")
            await complete_rift.finish()
        if exp_time < time2:
            msg = f"进行中的：{rift_info['name']}探索，预计{time2 - exp_time}分钟后可结束"
            await handle_send(bot, event, msg, md_type="秘境", k1="结算", v1="秘境结算", k2="加速", v2="道具使用 秘境加速券", k3="大加速", v3="道具使用 秘境大加速券", k4="钥匙", v4="道具使用 秘境钥匙")
            await complete_rift.finish()
        else:
            try:
                battle_result, result_name, outcome = await _roll_rift_event(
                    user_info, rift_info, bot.self_id, operation_id
                )
                explore_count = _rift_progress_snapshot(user_id)
                progress_reward, progress_msg = _roll_rift_progress(
                    explore_count, operation_id
                )
                outcome["progress_reward"] = progress_reward
                outcome.setdefault("statistics", {})["秘境次数"] = (
                    int(outcome.get("statistics", {}).get("秘境次数", 0)) + 1
                )
                outcome["message"] = f"{outcome['message']}{progress_msg}"
                result = rift_application.settle(
                    operation_id=operation_id, user_id=str(user_id), rift_info=rift_info,
                    user_state={
                        key: int(user_info.get(key, 0))
                        for key in ("stone", "exp", "hp", "mp")
                    },
                    explore_count=explore_count, outcome=outcome,
                    max_goods_num=XiuConfig().max_goods_num,
                )
            except Exception as exc:
                logger.opt(exception=exc).error("秘境普通结算事务执行失败")
                await handle_send(bot, event, "秘境结算失败：请求未生效。")
                await complete_rift.finish()
            if not result.succeeded:
                messages = {
                    "inventory_full": "背包容量不足，秘境结算未执行。",
                    "resource_missing": "当前资源不足，秘境结算未执行。",
                    "not_ready": "秘境探索尚未到结算时间。",
                }
                await handle_send(
                    bot,
                    event,
                    messages.get(
                        result.status,
                        "秘境结算未完成：未到结算时间或已结算，请重新查询秘境。",
                    ),
                )
                await complete_rift.finish()
            if battle_result is not None:
                await send_msg_handler(
                    bot,
                    event,
                    battle_result,
                    title=result.message.split("\n", 1)[0],
                )
            if result_name:
                await handle_send(
                    bot,
                    event,
                    result.message,
                    md_type="秘境",
                    k1="物品",
                    v1=f"查看效果 {result_name}",
                    k2="闭关",
                    v2="闭关",
                    k3="帮助",
                    v3="秘境帮助",
                )
            else:
                await handle_send(bot, event, result.message)
            log_message(user_id, result.message)
            await complete_rift.finish()


@break_rift.handle(parameterless=[Cooldown(cd_time=0)])
async def break_rift_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """终止探索秘境"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await break_rift.finish()
    user_id = user_info['user_id']
    event_id = _event_id(event)
    operation_id = f"rift-termination:{event_id or runtime_ids.new_id()}:{user_id}"
    if event_id:
        replay = rift_application.replay_termination(
            operation_id=operation_id, user_id=user_id
        )
        if replay is not None and replay.succeeded:
            await handle_send(
                bot,
                event,
                f"已终止{replay.rift_name}秘境的探索！",
            )
            await break_rift.finish()

    is_type, msg = check_user_type(user_id, 3)  # 需要在秘境的用户
    if not is_type:
        await handle_send(bot, event, msg, md_type="3", k2="修仙帮助", v2="修仙帮助", k3="秘境帮助", v3="秘境帮助")
        await break_rift.finish()
    else:
        user_id = user_info['user_id']
        rift_info = None
        try:
            rift_info = read_rift_data(user_id)
        except Exception as e:
            logger.error(f"读取用户 {user_id} 秘境数据失败: {e}")
            msg = '发生未知错误，秘境数据读取失败！'
            await handle_send(bot, event, msg)
            await break_rift.finish()

        try:
            result = _termination_outcome(
                rift_application.terminate(
                    operation_id=operation_id,
                    user_id=user_id,
                    rift_data=rift_info,
                )
            )
        except Exception as exc:
            logger.opt(exception=exc).error("秘境终止事务执行失败")
            await handle_send(bot, event, "秘境终止失败：请求未生效。")
            await break_rift.finish()
        if not result.succeeded:
            msg = {
                "not_active": "终止失败：当前没有进行中的秘境。",
                "state_changed": "终止未结算：秘境当前状态已更新。",
                "duplicate": "该终止请求已处理。",
            }.get(result.status, f"终止失败（{result.status}）。")
            await handle_send(bot, event, msg)
            await break_rift.finish()
        msg = f"已终止{rift_info['name']}秘境的探索！"
        await handle_send(bot, event, msg)
        await break_rift.finish()

async def use_rift_key(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, item_id, quantity):
    """使用秘境钥匙"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return

    user_id = user_info['user_id']
    event_id = _event_id(event)
    operation_id = f"rift-key-event:{event_id or runtime_ids.new_id()}:{user_id}"
    if event_id:
        replay = rift_application.replay_key_event(operation_id=operation_id)
        if replay is not None:
            await handle_send(bot, event, replay.message)
            return

    # 检查是否在秘境中
    is_type, msg = check_user_type(user_id, 3)  # 类型 3 表示在秘境中
    if not is_type:
        await handle_send(bot, event, msg, md_type="3", k2="修仙帮助", v2="修仙帮助", k3="秘境帮助", v3="秘境帮助")
        return

    # 读取秘境信息
    rift_info = None
    try:
        rift_info = read_rift_data(user_id)
    except Exception as e:
        logger.error(f"读取用户 {user_id} 秘境数据失败: {e}")
        msg = "秘境数据读取失败，请稍后再试！"
        await handle_send(bot, event, msg)
        return

    try:
        battle_result, result_name, outcome = await _roll_rift_event(
            user_info, rift_info, bot.self_id, operation_id
        )
        explore_count = _rift_progress_snapshot(user_id)
        progress_reward, progress_msg = _roll_rift_progress(
            explore_count, operation_id
        )
        outcome["progress_reward"] = progress_reward
        outcome.setdefault("statistics", {})["秘境次数"] = (
            int(outcome.get("statistics", {}).get("秘境次数", 0)) + 1
        )
        outcome["message"] = f"{outcome['message']}{progress_msg}"
        result = rift_application.event_settle(
            operation_id=operation_id, user_id=str(user_id), item_id=item_id,
            rift_info=rift_info,
            user_state={key: int(user_info.get(key, 0)) for key in ("stone", "exp", "hp", "mp")},
            explore_count=explore_count, outcome=outcome,
            max_goods_num=XiuConfig().max_goods_num,
        )
    except Exception as exc:
        logger.opt(exception=exc).error("秘境钥匙结算事务执行失败")
        await handle_send(bot, event, "秘境钥匙结算失败：请求未生效。")
        return
    if not result.succeeded:
        messages = {
            "item_missing": "秘境钥匙数量不足，请重新查看背包。",
            "inventory_full": "背包容量不足，秘境钥匙结算未执行。",
            "resource_missing": "当前资源不足，秘境钥匙结算未执行。",
            "not_active": "当前没有可使用秘境钥匙结算的秘境探索。",
        }
        await handle_send(
            bot,
            event,
            messages.get(
                result.status,
                "使用秘境钥匙未完成：钥匙或秘境状态已更新，请重新查看背包。",
            ),
        )
        return
    if battle_result is not None:
        await send_msg_handler(bot, event, battle_result, title=outcome["message"].split("\n", 1)[0])
    if result_name:
        await handle_send(bot, event, result.message, md_type="秘境", k1="物品", v1=f"查看效果 {result_name}", k2="闭关", v2="闭关", k3="帮助", v3="秘境帮助")
    else:
        await handle_send(bot, event, result.message)
    log_message(user_id, result.message)
    return

async def use_rift_boss(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, item_id, quantity):
    """使用斩妖令"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return

    user_id = user_info['user_id']
    event_id = _event_id(event)
    operation_id = (
        f"rift-demon-token-battle:{event_id or runtime_ids.new_id()}:{user_id}"
    )
    if event_id:
        replay = rift_application.replay_demon_token_battle(operation_id=operation_id)
        if replay is not None:
            await handle_send(bot, event, replay.message)
            return

    # 检查是否在秘境中
    is_type, msg = check_user_type(user_id, 3)  # 类型 3 表示在秘境中
    if not is_type:
        await handle_send(bot, event, msg, md_type="3", k2="修仙帮助", v2="修仙帮助", k3="秘境帮助", v3="秘境帮助")
        return

    # 读取秘境信息并立即结算
    try:
        rift_info = read_rift_data(user_id)
    except Exception as e:
        logger.error(f"读取用户 {user_id} 秘境数据失败: {e}")
        msg = "秘境数据读取失败，请稍后再试！"
        await handle_send(bot, event, msg)
        return

    try:
        battle_result, result_msg, outcome = await _roll_rift_boss_event(
            user_info, rift_info["rank"], bot.self_id, operation_id
        )
        explore_count = _rift_progress_snapshot(user_id)
        progress_reward, progress_msg = _roll_rift_progress(
            explore_count, operation_id
        )
        outcome["progress_reward"] = progress_reward
        outcome.setdefault("statistics", {})["秘境次数"] = (
            int(outcome.get("statistics", {}).get("秘境次数", 0)) + 1
        )
        outcome["message"] = (
            f"秘境 {rift_info['name']} 已使用斩妖令结算！\n"
            f"战斗结果：{result_msg}{progress_msg}"
        )
        settlement = rift_application.settle_demon_token_battle(
            operation_id=operation_id,
            user_id=str(user_id),
            item_id=item_id,
            expected_rift=rift_info,
            expected_user={
                key: int(user_info.get(key, 0))
                for key in ("stone", "exp", "hp", "mp")
            },
            expected_explore_count=explore_count,
            outcome=outcome,
            max_goods_num=XiuConfig().max_goods_num,
        )
    except Exception as exc:
        logger.opt(exception=exc).error("斩妖令结算事务执行失败")
        await handle_send(bot, event, "斩妖令结算失败：请求未生效。")
        return
    if not settlement.succeeded:
        messages = {
            "item_missing": "斩妖令数量不足，请重新查看背包。",
            "inventory_full": "背包容量不足，斩妖令结算未执行。",
            "resource_missing": "当前资源不足，斩妖令结算未执行。",
            "not_active": "当前没有可使用斩妖令结算的秘境探索。",
        }
        await handle_send(
            bot,
            event,
            messages.get(
                settlement.status,
                "使用斩妖令未完成：斩妖令或秘境状态已更新，请重新查看背包。",
            ),
        )
        return
    await send_msg_handler(bot, event, battle_result, title=result_msg)
    log_message(user_id, settlement.message)
    await handle_send(bot, event, settlement.message)
    return

async def _use_rift_speedup(
    bot: Bot,
    event: GroupMessageEvent | PrivateMessageEvent,
    item_id,
    remaining_ratio: int,
    reduction_text: str,
):
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return
    
    user_id = user_info['user_id']
    
    event_id = str(getattr(event, "message_id", "") or getattr(event, "id", "") or "").strip()
    operation_id = f"rift-speedup:{event_id or runtime_ids.new_id()}:{user_id}"
    try:
        outcome = rift_application.speedup(
            operation_id=operation_id, user_id=str(user_id),
            item_id=item_id, remaining_ratio=remaining_ratio,
        )
        result = SimpleNamespace(**dict(outcome.data or {}), status=outcome.status, succeeded=outcome.ok)
    except Exception as exc:
        logger.opt(exception=exc).error("秘境加速事务执行失败")
        await handle_send(bot, event, "秘境加速失败：请求未生效。")
        return
    if not result.succeeded:
        messages = {
            "not_needed": "秘境探索时间已经小于等于10分钟，无需使用加速券！",
            "item_missing": "加速券数量不足，请重新查看背包。",
            "not_active": "当前没有可加速的秘境探索。",
        }
        await handle_send(bot, event, messages.get(result.status, f"加速失败（{result.status}）。"))
        return

    # The database is authoritative; keep the legacy file as a post-commit projection.
    try:
        save_rift_data(user_id, result.rift_data)
    except Exception as exc:
        logger.error(f"同步用户 {user_id} 秘境加速缓存失败: {exc}")

    exp_time = _rift_elapsed_minutes(result.create_time)
    time2 = result.new_time
    
    if exp_time >= time2:
        rift_status = "可结算"
    else:
        rift_status = f"探索{result.rift_data['name']} {time2 - exp_time}分后"

    msg = f"秘境探索时间减少{reduction_text}了！\n当前状态：{rift_status}"
    await handle_send(bot, event, msg, md_type="秘境", k1="结算", v1="秘境结算", k2="加速", v2="道具使用 秘境加速券", k3="大加速", v3="道具使用 秘境大加速券", k4="钥匙", v4="道具使用 秘境钥匙")
    return


async def use_rift_speedup(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, item_id, quantity):
    """使用秘境加速券"""
    return await _use_rift_speedup(bot, event, item_id, 50, "50%")


async def use_rift_big_speedup(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, item_id, quantity):
    """使用秘境大加速券"""
    return await _use_rift_speedup(bot, event, item_id, 10, "90%")
