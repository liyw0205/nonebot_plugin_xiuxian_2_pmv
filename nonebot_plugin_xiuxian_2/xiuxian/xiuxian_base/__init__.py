import re
import random
import asyncio
import time
from datetime import datetime
from pathlib import Path
from nonebot.typing import T_State
from ...paths import get_paths
from ..xiuxian_utils.lay_out import assign_bot, Cooldown
from nonebot import get_bot
from ..on_compat import on_command
from ..adapter_compat import (
    Bot,
    GROUP,
    Message,
    MessageEvent,
    GroupMessageEvent,
    PrivateMessageEvent,
    get_at_user_id,
)
from nonebot.log import logger
from nonebot.params import CommandArg
from ..xiuxian_utils.data_source import jsondata
from ..xiuxian_utils.player_fight import Boss_fight
from ..xiuxian_utils.xiuxian2_handle import (
    XiuxianDateManage, PlayerDataManager, XiuxianJsonDate, OtherSet, 
    UserBuffDate, XIUXIAN_IMPART_BUFF, leave_harm_time
)
from ..xiuxian_config import XiuConfig, JsonConfig, convert_rank
from ..xiuxian_utils.utils import (
    check_user, check_user_type,
    get_msg_pic, number_to,
    Txt2Img, send_msg_handler, handle_send, get_logs, log_message, get_statistics_data, update_statistics_value,
    send_help_message
)
from ..xiuxian_utils.item_json import Items
from ..xiuxian_utils.season_service import get_current_season
from ..xiuxian_utils.season_rank_service import (
    DEFAULT_SEASON_RANK_TYPES,
    get_top_season_rank,
    get_user_current_season_entries,
)
from ..xiuxian_tasks.task_data import record_task_progress
from .stone_limit import stone_limit
from .lottery_settlement_service import LotterySettlementService
from .sign_service import SignInService
from .player_rename_service import PlayerRenameService
from .stone_gift_service import StoneGiftService
from .stone_contest_service import StoneContestService
from .stone_robbery_service import StoneRobberySettlementService
from .registration_batch import RegistrationBatcher, RegistrationRequest
from .breakthrough_tribulation import *  # noqa: F401,F403
from .xiangyuan import clear_all_xiangyuan, reset_xiangyuan_daily  # noqa: F401

items = Items()
sql_message = XiuxianDateManage()  # sql类
sign_in_service = SignInService(get_paths().game_db)
lottery_settlement_service = LotterySettlementService(
    get_paths().game_db,
    Path(__file__).with_name("lottery_pool.json"),
)
player_rename_service = PlayerRenameService(get_paths().game_db)
stone_gift_service = StoneGiftService(get_paths().game_db)
stone_contest_service = StoneContestService(get_paths().game_db)
stone_robbery_service = StoneRobberySettlementService(
    get_paths().game_db, get_paths().player_db
)
registration_batcher = RegistrationBatcher(sql_message)
player_data_manager = PlayerDataManager()
xiuxian_impart = XIUXIAN_IMPART_BUFF()
PLAYERSDATA = get_paths().players
qqq = XiuConfig().qqq
tribulation_cd2 = int(XiuConfig().tribulation_cd * 60)
gfqq = on_command("官群", aliases={"交流群"}, priority=8, block=True)
run_xiuxian = on_command("我要修仙", aliases={"开始修仙"}, priority=8, block=True)
restart = on_command("重入仙途", priority=7, block=True)
sign_in = on_command("修仙签到", priority=13, block=True)
hongyun = on_command("鸿运", aliases={"查看中奖", "奖池查询"}, priority=5, block=True)
help_in = on_command("修仙帮助", aliases={"修仙菜单"}, priority=12, block=True)
rank = on_command("排行榜", aliases={"修仙排行榜", "灵石排行榜", "战力排行榜", "境界排行榜", "宗门排行榜", "轮回排行榜"},
                  priority=7, block=True)
season_rank = on_command("赛季榜", aliases={"赛季排行榜", "赛季排行", "赛季信息"}, priority=7, block=True)
my_season_rank = on_command("我的赛季", aliases={"我的赛季榜", "个人赛季"}, priority=7, block=True)
remaname = on_command("修仙改名", priority=5, block=True)
root_rename = on_command("灵根改名", priority=5, block=True)
give_stone = on_command("送灵石", permission=GROUP, priority=6, block=True)
steal_stone = on_command("偷灵石", aliases={"飞龙探云手"}, permission=GROUP, priority=6, block=True)
rob_stone = on_command("抢灵石", aliases={"抢劫"}, permission=GROUP, priority=6, block=True)
user_stamina = on_command('我的体力', aliases={'体力'}, priority=5, block=True)
level_help = on_command("灵根帮助", aliases={"灵根列表"}, priority=15, block=True)
level1_help = on_command("品阶帮助", aliases={"品阶列表"}, priority=15, block=True)
level2_help = on_command("境界帮助", aliases={"境界列表"}, priority=15, block=True)
view_logs = on_command("修仙日志", aliases={"查看日志", "我的日志", "查日志", "日志记录"}, priority=5, block=True)

view_data = on_command("修仙数据", aliases={"统计数据", "我的数据", "查数据", "数据记录", "统计信息"}, priority=5, block=True)


def _sign_operation_id(event, user_id):
    event_id = str(
        getattr(event, "message_id", "") or getattr(event, "id", "") or ""
    ).strip()
    if event_id:
        return f"sign:{event_id}:{user_id}"
    return f"sign:{user_id}:{time.time_ns()}"


def _lottery_operation_id(sign_operation_id):
    return f"lottery:{sign_operation_id}"


def _player_rename_operation_id(event, rename_type, user_id):
    event_id = str(
        getattr(event, "message_id", "") or getattr(event, "id", "") or ""
    ).strip()
    if event_id:
        return f"player-rename:{event_id}:{rename_type}:{user_id}"
    return f"player-rename:{rename_type}:{user_id}:{time.time_ns()}"


def _stone_gift_operation_id(event, sender_id, recipient_id):
    event_id = str(
        getattr(event, "message_id", "") or getattr(event, "id", "") or ""
    ).strip()
    if event_id:
        return f"stone-gift:{event_id}:{sender_id}:{recipient_id}"
    return f"stone-gift:{sender_id}:{recipient_id}:{time.time_ns()}"


def _stone_theft_operation_id(event, thief_id):
    event_id = str(
        getattr(event, "message_id", "") or getattr(event, "id", "") or ""
    ).strip()
    if event_id:
        return f"stone-theft:{event_id}:{thief_id}"
    return f"stone-theft:{thief_id}:{time.time_ns()}"


def _stone_theft_messages(result, thief_name, victim_name):
    if result.outcome == "failure":
        return (
            "道友偷窃失手了，被对方发现并被派去华哥厕所义务劳工！"
            f"赔款{number_to(result.transferred_amount)}灵石",
            None,
        )
    if result.payer_balance == 0:
        return (
            f"{victim_name}道友已经被榨干了~",
            f"灵石被{thief_name}道友榨干了~",
        )
    return (
        f"共偷取{victim_name}道友{number_to(result.transferred_amount)}枚灵石！",
        f"被{thief_name}道友偷取{number_to(result.transferred_amount)}枚灵石！",
    )


def _stone_robbery_operation_id(event, robber_id):
    event_id = str(
        getattr(event, "message_id", "") or getattr(event, "id", "") or ""
    ).strip()
    if event_id:
        return f"stone-robbery:{event_id}:{robber_id}"
    return f"stone-robbery:{robber_id}:{time.time_ns()}"


def _stone_robbery_messages(result, robber_name):
    if result.winner_id == result.robber_id:
        if result.transferred_amount > 0:
            return (
                f"大战一番，战胜对手，获取灵石{number_to(result.transferred_amount)}枚！",
                f"被{robber_name}道友抢走{number_to(result.transferred_amount)}枚灵石！",
            )
        return (
            "大战一番，战胜对手，结果对方是个穷光蛋，一无所获！",
            f"未能抵御{robber_name}道友的抢劫，幸好身无分文！",
        )
    if result.transferred_amount > 0:
        return (
            f"大战一番，被对手反杀，损失灵石{number_to(result.transferred_amount)}枚！",
            f"成功反杀{robber_name}道友，获得{number_to(result.transferred_amount)}枚灵石战利品！",
        )
    return (
        "大战一番，被对手反杀，幸好身无分文，没有损失！",
        f"成功反杀{robber_name}道友，可惜对方是个穷光蛋，一无所获！",
    )


def _stone_robbery_player(user):
    user_id = str(user['user_id'])
    impart_data = xiuxian_impart.get_user_impart_info_with_id(user_id)
    armor_data = UserBuffDate(user_id).get_user_armor_buff_data()
    exp = int(user['exp'])
    return {
        "user_id": user_id,
        "道号": user['user_name'],
        "气血": exp // 2 if user['hp'] is None else int(user['hp']),
        "攻击": exp // 10 if user['atk'] is None else int(user['atk']),
        "真元": exp if user['mp'] is None else int(user['mp']),
        "会心": int(
            (0.01 + impart_data['impart_know_per'] if impart_data is not None else 0)
            * 100
        ),
        "爆伤": int(
            1.5 + impart_data['impart_burst_per'] if impart_data is not None else 0
        ),
        "防御": int(armor_data['def_buff']) if armor_data is not None else 0,
    }


xiuxian_world_info = on_command("修仙界信息", priority=5, block=True)

__level_help__ = """
【灵根体系】

至高道果
- 命运道果
- 永恒道果
- 轮回道果
- 异界道果

特殊灵根
- 机械灵根
- 混沌灵根
- 融合灵根

普通灵根
- 超品灵根
- 龙灵根
- 天灵根
- 异灵根
- 真灵根
- 伪灵根

> 灵根品质影响修炼速度。
""".strip()


__level1_help__ = """
【功法与法器品阶】

功法品阶体系
- 至高：无上
- 仙阶：极品 / 上品 / 下品
- 天阶：上品 / 下品
- 地阶：上品 / 下品
- 玄阶：上品 / 下品
- 黄阶：上品 / 下品
- 人阶：上品 / 下品

法器品阶体系
- 至高：无上
- 仙器：极品 / 上品 / 下品
- 通天：上品 / 下品
- 纯阳：上品 / 下品
- 玄器：上品 / 下品
- 法器：上品 / 下品
- 符器：上品 / 下品

> 品阶越高，效果越强。
""".strip()

__level2_help__ = f"""
【境界帮助】

江湖人
↓
感气境 → 练气境 → 筑基境
结丹境 → 金丹境 → 元神境 
化神境 → 炼神境 → 返虚境
大乘境 → 虚道境 → 斩我境 
遁一境 → 至尊境 → 微光境
星芒境 → 月华境 → 耀日境
祭道境 → 自在境 → 破虚境 
无界境 → 混元境 → 造化境
↓
永恒境
↓
至高
""".strip()

_SEASON_MODE_ALIASES = {
    "周榜": "weekly",
    "周赛季": "weekly",
    "本周": "weekly",
    "weekly": "weekly",
    "week": "weekly",
    "月榜": "monthly",
    "月赛季": "monthly",
    "本月": "monthly",
    "monthly": "monthly",
    "month": "monthly",
    "季度榜": "quarterly",
    "季榜": "quarterly",
    "季度赛季": "quarterly",
    "本季": "quarterly",
    "quarterly": "quarterly",
    "quarter": "quarterly",
}

_SEASON_RANK_TYPE_ALIASES = {
    "交易活跃榜": "交易活跃",
    "交易榜": "交易活跃",
    "交易": "交易活跃",
    "拍卖": "交易活跃",
    "讨伐榜": "讨伐",
    "讨伐": "讨伐",
    "boss": "讨伐",
    "世界boss": "讨伐",
    "世界事件": "讨伐",
    "宗门贡献榜": "宗门贡献",
    "宗门贡献": "宗门贡献",
    "宗门": "宗门贡献",
    "贡献": "宗门贡献",
    "试炼榜": "试炼",
    "副本榜": "试炼",
    "试炼": "试炼",
    "副本": "试炼",
    "战力榜": "战力",
    "战力": "战力",
}


def _parse_season_mode(text: str) -> str:
    text = str(text or "").strip().lower()
    for keyword, mode in _SEASON_MODE_ALIASES.items():
        if keyword.lower() in text:
            return mode
    return "monthly"


def _parse_season_rank_type(text: str) -> str | None:
    text = str(text or "").strip().lower()
    for keyword, rank_type in _SEASON_RANK_TYPE_ALIASES.items():
        if keyword.lower() in text:
            return rank_type
    return None


def _format_top_rows(title: str, rows, formatter, limit: int = 5) -> list[str]:
    lines = [f"\n【{title}】"]
    if not rows:
        lines.append("暂无上榜记录")
        return lines

    for index, row in enumerate(rows[:limit], 1):
        lines.append(f"{index}. {formatter(row)}")
    return lines


def _format_season_rank_rows(title: str, rows, limit: int = 5) -> list[str]:
    lines = [f"\n【{title}】"]
    if not rows:
        lines.append("本期暂无上榜记录")
        return lines

    for row in rows[:limit]:
        if str(row.get("user_id") or ""):
            name = row.get("user_name") or row.get("user_id")
        elif int(row.get("sect_id") or 0):
            name = row.get("sect_name") or f"宗门{row.get('sect_id')}"
        else:
            name = "未知"
        lines.append(f"{row.get('rank', '?')}. {name} 积分{number_to(row.get('score', 0))}")
    return lines


def _get_sect_weekly_rank_fallback(limit: int = 5):
    conn = getattr(sql_message, "conn", None)
    if not conn or not getattr(conn, "table_exists", lambda _table: False)("sect_weekly_goal"):
        return None

    week_key = get_current_season("weekly").key
    return sql_message._read_query(
        """
        SELECT
            g.sect_id,
            COALESCE(s.sect_name, g.sect_id) AS sect_name,
            SUM(g.progress) AS total_progress
        FROM sect_weekly_goal AS g
        LEFT JOIN sects AS s ON s.sect_id = g.sect_id
        WHERE g.week_key = %s
        GROUP BY g.sect_id, s.sect_name
        ORDER BY total_progress DESC
        LIMIT %s
        """,
        (week_key, max(1, min(int(limit or 5), 50))),
        dict_row=True,
    )


def _append_global_rank_preview(lines: list[str]) -> None:
    lines.append("\n本期还没有赛季记录，先看当前仙榜前列。")

    try:
        lines.extend(
            _format_top_rows(
                "修为境界榜 前五",
                sql_message.realm_top(),
                lambda row: f"{row[0]} {row[1]} 修为{number_to(row[2])}",
            )
        )
    except Exception as exc:
        logger.warning(f"赛季榜读取修为/境界榜失败：{exc}")
        lines.extend(["\n【修为境界榜 前五】", "暂不可看"])

    try:
        lines.extend(
            _format_top_rows(
                "灵石榜 前五",
                sql_message.stone_top(),
                lambda row: f"{row[0]} 灵石{number_to(row[1])}枚",
            )
        )
    except Exception as exc:
        logger.warning(f"赛季榜读取灵石榜失败：{exc}")
        lines.extend(["\n【灵石榜 前五】", "暂不可看"])

    try:
        lines.extend(
            _format_top_rows(
                "战力榜 前五",
                sql_message.power_top(),
                lambda row: f"{row[0]} 战力{number_to(row[1])}",
            )
        )
    except Exception as exc:
        logger.warning(f"赛季榜读取战力榜失败：{exc}")
        lines.extend(["\n【战力榜 前五】", "暂不可看"])

    try:
        lines.extend(
            _format_top_rows(
                "宗门建设榜 前五",
                sql_message.scale_top(),
                lambda row: f"{row[1]} 建设度{number_to(row[2])}",
            )
        )
    except Exception as exc:
        logger.warning(f"赛季榜读取宗门建设榜失败：{exc}")
        lines.extend(["\n【宗门建设榜 前五】", "暂不可看"])

    try:
        sect_weekly_rows = _get_sect_weekly_rank_fallback()
        if sect_weekly_rows is None:
            lines.extend(["\n【宗门周常榜 前五】", "宗门周常暂未开启。"])
        else:
            lines.extend(
                _format_top_rows(
                    "宗门周常榜 前五",
                    sect_weekly_rows,
                    lambda row: f"{row['sect_name']} 进度{number_to(row['total_progress'])}",
                )
            )
    except Exception as exc:
        logger.warning(f"赛季榜读取宗门周常排行失败：{exc}")
        lines.extend(["\n【宗门周常榜 前五】", "暂不可看"])


def _build_season_rank_message(mode: str = "monthly", rank_type: str | None = None) -> str:
    current = get_current_season(mode)
    weekly = get_current_season("weekly")
    monthly = get_current_season("monthly")
    quarterly = get_current_season("quarterly")

    lines = [
        "【赛季榜】",
        f"本期榜单：{current.name}",
        f"周榜：{weekly.key}",
        f"月榜：{monthly.key}",
        f"季度榜：{quarterly.key}",
        "",
    ]

    any_season_data = False
    if rank_type:
        try:
            rows = get_top_season_rank(rank_type, current.mode, limit=10)
        except Exception as exc:
            logger.warning(f"赛季榜读取{rank_type}失败：{exc}")
            rows = []
        any_season_data = bool(rows)
        lines.extend(_format_season_rank_rows(f"{current.name}{rank_type}榜 前十", rows, limit=10))
    else:
        for season_rank_type in DEFAULT_SEASON_RANK_TYPES:
            try:
                rows = get_top_season_rank(season_rank_type, current.mode, limit=5)
            except Exception as exc:
                logger.warning(f"赛季榜读取{season_rank_type}失败：{exc}")
                rows = []
            if rows:
                any_season_data = True
                lines.extend(
                    _format_season_rank_rows(
                        f"{current.name}{season_rank_type}榜 前五",
                        rows,
                        limit=5,
                    )
                )

    if not any_season_data:
        lines.append("\n本期暂无赛季积分记录。")
        _append_global_rank_preview(lines)

    lines.extend(
        [
            "\n可发送：赛季榜、赛季榜 周榜、赛季榜 月榜、赛季榜 季度榜、赛季榜 周榜 战力、我的赛季",
            "可看榜单：交易活跃、讨伐、宗门贡献、试炼、战力。",
        ]
    )
    return "\n".join(lines)


def _build_my_season_rank_message(user_id: str, mode: str | None = None) -> str:
    modes = [mode] if mode else ["weekly", "monthly", "quarterly"]
    entries = get_user_current_season_entries(user_id, modes=modes)
    lines = ["【我的赛季】"]

    if not entries:
        lines.append("当前周期暂无赛季积分记录。")
        lines.append("可通过交易、讨伐、宗门任务、副本等玩法累计赛季积分。")
        return "\n".join(lines)

    grouped: dict[str, list[dict]] = {}
    for row in entries:
        grouped.setdefault(str(row.get("mode") or ""), []).append(row)

    mode_names = {
        "weekly": "周榜",
        "monthly": "月榜",
        "quarterly": "季度榜",
    }
    for mode_key in ("weekly", "monthly", "quarterly"):
        rows = grouped.get(mode_key, [])
        if not rows:
            continue
        season = get_current_season(mode_key)
        lines.append(f"\n【{season.key}{mode_names.get(mode_key, mode_key)}】")
        for row in rows:
            lines.append(
                f"{row.get('rank_type')}：积分{number_to(row.get('score', 0))}，"
                f"当前第{row.get('rank', '?')}名"
            )

    return "\n".join(lines)

@gfqq.handle(parameterless=[Cooldown(cd_time=30)])
async def gfqq_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    msg = f"{qqq}"
    await handle_send(bot, event, msg)
    
@remaname.handle(parameterless=[Cooldown(cd_time=30)])
async def remaname_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """修改道号"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await remaname.finish()
    user_id = user_info['user_id']
    
    # 如果没有提供新道号，则生成随机道号
    user_name = args.extract_plain_text().strip()
    random_name = not user_name
    if random_name:
        # 生成不重复的道号
        while True:
            user_name = generate_daohao()
            if not sql_message.get_user_info_with_name(user_name):
                break
        result = player_rename_service.rename_user(
            _player_rename_operation_id(event, "user-name", user_id),
            user_id,
            user_name,
            stone_cost=XiuConfig().remaname,
        )
    else:
        # 检查名字长度（7个中文字符）
        if len(user_name) > 7:
            msg = "道号长度不能超过7个字符！"
            await handle_send(bot, event, msg, md_type="修仙", k1="改名", v1="修仙改名", k2="存档", v2="我的修仙信息", k3="帮助", v3="修仙帮助")
            await remaname.finish()
            
        msg = ""
        result = player_rename_service.rename_user(
            _player_rename_operation_id(event, "user-name", user_id),
            user_id,
            user_name,
            item_id=20011,
        )
    if result.status == "stone_insufficient":
        msg = f"修改道号需要消耗{XiuConfig().remaname}灵石，你的灵石不足！"
    elif result.status == "item_missing":
        msg = "修改道号需要消耗1个易名符！"
    elif result.status == "name_conflict":
        msg = "该道号已被使用，请选择其他道号！"
    elif result.status == "unchanged":
        msg = "新道号不能与当前道号相同！"
    elif result.succeeded:
        if random_name:
            msg = f"你获得了随机道号：{result.new_name}\n"
        msg += "道友的道号更新成啦~"
    else:
        msg = "道号或资产状态已经变化，请稍后重试！"
    await handle_send(bot, event, msg, md_type="修仙", k1="改名", v1="修仙改名", k2="存档", v2="我的修仙信息", k3="帮助", v3="修仙帮助")
    await remaname.finish()


@root_rename.handle(parameterless=[Cooldown(cd_time=30)])
async def root_rename_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """修改灵根名称"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await root_rename.finish()

    user_id = user_info['user_id']
    root_name = args.extract_plain_text().strip()
    if not root_name:
        msg = "请发送：灵根改名 新灵根名"
        await handle_send(bot, event, msg, md_type="修仙", k1="改名", v1="灵根改名", k2="存档", v2="我的修仙信息", k3="帮助", v3="修仙帮助")
        await root_rename.finish()

    if len(root_name) > 15:
        msg = "灵根名长度不能超过15个字符！"
        await handle_send(bot, event, msg, md_type="修仙", k1="改名", v1="灵根改名", k2="存档", v2="我的修仙信息", k3="帮助", v3="修仙帮助")
        await root_rename.finish()

    result = player_rename_service.rename_root(
        _player_rename_operation_id(event, "root", user_id),
        user_id,
        root_name,
        item_id=20025,
    )
    if result.status == "item_missing":
        msg = "修改灵根名需要消耗1个灵根改名卡！"
    elif result.status == "unchanged":
        msg = "新灵根名不能与当前灵根名相同！"
    elif result.succeeded:
        msg = f"灵根已改名为：{result.new_name}"
    else:
        msg = "灵根名或改名卡状态已经变化，请稍后重试！"
    await handle_send(bot, event, msg, md_type="修仙", k1="改名", v1="灵根改名", k2="存档", v2="我的修仙信息", k3="帮助", v3="修仙帮助")
    await root_rename.finish()


@run_xiuxian.handle(parameterless=[Cooldown(cd_time=0)])
async def run_xiuxian_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """我要修仙"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)

    # 先查当前激活身份是否已注册（内部已兼容：伪装ID > active_id > 本号）
    isUser, user_info, msg = check_user(event)

    # 已注册直接返回
    if isUser:
        await handle_send(
            bot,
            event,
            "您已踏入修仙世界，输入【我的修仙信息】查看数据吧！",
            md_type="修仙",
            k1="存档",
            v1="我的修仙信息",
            k2="帮助",
            v2="修仙帮助",
            k3="签到",
            v3="修仙签到"
        )
        await run_xiuxian.finish()

    # 未注册时，按统一生效ID规则计算：伪装ID > active_id > 本号
    real_user_id = str(event.get_user_id())

    # 先本号/化身
    active_id = player_data_manager.get_field_data(real_user_id, "avatar", "active_id")
    user_id = str(active_id) if active_id else real_user_id

    # 再伪装（优先级最高）
    try:
        from ..xiuxian_utils.utils import get_impersonating_target
        imp_id = get_impersonating_target(real_user_id)
        if imp_id:
            user_id = str(imp_id)
            logger.warning(f"管理员 {real_user_id} 正在伪装 {user_id} 执行【我要修仙】")
    except Exception:
        # 防御式处理，避免导入异常影响注册
        pass

    root, root_type = XiuxianJsonDate().linggen_get()
    rate = sql_message.get_root_rate(root_type, user_id)
    power = 100 * float(rate)
    create_time = str(datetime.now())

    # 并发注册直接尝试插入；道号冲突时只重试当前用户，不先做全表查重。
    for _ in range(20):
        user_name = generate_daohao()
        is_new_user, create_msg = await registration_batcher.submit(
            RegistrationRequest(user_id, root, root_type, int(power), create_time, user_name)
        )
        if is_new_user is not None:
            break
    else:
        await handle_send(bot, event, "当前注册人数较多，道号生成冲突，请稍后重试。")
        await run_xiuxian.finish()

    if is_new_user:
        # 补全初始气血
        new_user_info = sql_message.get_user_info_with_id(user_id)
        if new_user_info and (new_user_info.get('hp') is None or new_user_info.get('hp') == 0):
            sql_message.update_user_hp(user_id)

    final_msg = (
        f"{create_msg}\n"
        f"你获得了随机道号：{user_name}\n"
        f"耳边响起一个神秘人的声音：不要忘记仙途奇缘！\n"
        f"基础指令：修仙帮助"
    )
    await handle_send(
        bot,
        event,
        final_msg,
        md_type="修仙",
        k1="帮助",
        v1="修仙帮助",
        k2="存档",
        v2="我的修仙信息",
        k3="仙途奇缘",
        v3="仙途奇缘帮助"
    )
    await run_xiuxian.finish()

@sign_in.handle(parameterless=[Cooldown(cd_time=0)])
async def sign_in_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """修仙签到"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await sign_in.finish()
    user_id = user_info['user_id']
    
    sign_operation_id = _sign_operation_id(event, user_id)
    sign_result = sign_in_service.sign(
        sign_operation_id,
        user_id,
        XiuConfig().sign_in_lingshi_lower_limit,
        XiuConfig().sign_in_lingshi_upper_limit,
    )
    if not sign_result.succeeded:
        await handle_send(bot, event, "贪心的人是不会有好运的！")
        await sign_in.finish()
    result = f"签到成功，获取{sign_result.stone}块灵石!"

    lottery_result = await handle_lottery(
        user_info, _lottery_operation_id(sign_operation_id)
    )
    
    # 3. 组合签到结果和抽奖结果
    msg = f"{result}\n\n{lottery_result}"
    
    if sign_result.applied:
        log_message(user_id, msg)
        update_statistics_value(user_id, "修仙签到")
        record_task_progress(
            user_id, "sign_in", operation_id=f"task-progress:{sign_operation_id}"
        )
    await handle_send(bot, event, msg, md_type="修仙", k1="修仙签到", v1="修仙签到", k2="鸿运", v2="鸿运", k3="帮助", v3="修仙帮助")
    await sign_in.finish()

@hongyun.handle(parameterless=[Cooldown(cd_time=0)])
async def hongyun_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """查看中奖记录和当前奖池"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    business_date = datetime.now().date().isoformat()
    snapshot = lottery_settlement_service.get_snapshot(business_date)
    msg = "【鸿运当头】\n"
    msg += f"当前奖池累计：{number_to(snapshot.pool)}灵石\n"
    msg += f"本期参与人数：{snapshot.participants}位道友\n\n"
    
    last_winner = snapshot.last_winner
    if last_winner:
        msg += "上期中奖记录\n"
        msg += f"中奖道友：{last_winner.user_name}\n"
        msg += f"中奖时间：{last_winner.won_at}\n"
        msg += f"中奖金额：{number_to(last_winner.amount)}灵石\n"
    else:
        msg += "暂无历史中奖记录，道友快来签到吧！\n"
    
    msg += "\n※ 每次签到自动存入100万灵石到奖池，中奖号码将独享全部奖池！"
    
    await handle_send(bot, event, msg, md_type="修仙", k1="修仙签到", v1="修仙签到", k2="鸿运", v2="鸿运", k3="帮助", v3="修仙帮助")
    await hongyun.finish()

async def handle_lottery(user_info: dict, operation_id: str):
    """处理鸿运抽奖逻辑"""
    user_id = user_info['user_id']
    user_name = user_info['user_name']
    occurred_at = datetime.now()
    settled = lottery_settlement_service.settle(
        operation_id,
        user_id,
        user_name,
        occurred_at.date().isoformat(),
        occurred_at=occurred_at,
    )
    if settled.status == "operation_conflict":
        return "鸿运结算记录冲突，请联系管理员处理。"
    if settled.status == "user_missing":
        return "未找到修仙存档，本次鸿运未结算。"
    if settled.status == "already_participated" and not settled.lottery_number:
        return "本期鸿运已经参与，奖池继续累积~"
    if settled.prize_tier == "grand":
        return f"✨鸿运当头！恭喜道友获得特等奖！\n中奖号码：{settled.lottery_number}\n获得奖池中{number_to(settled.prize)}灵石！🎉🎉🎉"
    prize_names = {
        "first": "一等奖",
        "second": "二等奖",
        "third": "三等奖",
    }
    if settled.prize_tier in prize_names:
        return f"🎉恭喜道友获得{prize_names[settled.prize_tier]}！\n中奖号码：{settled.lottery_number}\n获得奖池的{number_to(settled.prize)}灵石！🎉"
    return "本次签到未中奖，奖池继续累积~"

@help_in.handle(parameterless=[Cooldown(cd_time=0)])
async def help_in_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """修仙帮助"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)

    msg = """【修仙帮助】

基础指令：
- 我要修仙：创建角色
- 我的修仙信息：查看存档
- 修仙签到：每日签到
- 鸿运：查看签到奖池
- 修仙改名 新道号：修改道号
- 灵根改名 新灵根名：修改灵根名
- 重入仙途：重置灵根
- 突破 / 直接突破：提升境界
- 闭关 / 出关：修炼修为
- 我的体力：查看体力
- 背包：查看物品
- 我的任务：查看任务
- 送灵石 道号 数量：赠送灵石
- 偷灵石 / 抢灵石 道号：夺取灵石

常用入口：
- 修仙数据 / 修仙日志 / 修仙界信息
- 修仙排行榜 / 赛季榜 / 我的赛季
- 境界帮助 / 品阶帮助 / 灵根帮助

功能入口：
- 背包帮助 / 饰品帮助 / 称号帮助
- 宗门帮助 / 关系帮助 / 炼体帮助
- 炼丹帮助 / 功法帮助 / 洞府帮助
- 地图帮助 / 秘境帮助 / 历练帮助
- 世界BOSS帮助 / 世界事件帮助
- 交易帮助 / 拍卖活动 / 灵庄帮助
- 宠物帮助 / 本命法宝帮助
- 传承帮助 / 虚神界帮助 / 前尘帮助
""".strip()

    await send_help_message(
        bot,
        event,
        msg,
        k1="存档",
        v1="我的修仙信息",
        k2="关系",
        v2="关系帮助",
        k3="背包",
        v3="背包帮助",
        k4="地图",
        v4="地图帮助",
        button_id=XiuConfig().button_id2,
    )
    await help_in.finish()

@level_help.handle(parameterless=[Cooldown(cd_time=0)])
async def level_help_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """灵根帮助"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    msg = __level_help__
    await send_help_message(bot, event, msg, k1="灵根帮助", v1="灵根帮助", k2="品阶帮助", v2="品阶帮助", k3="境界帮助", v3="境界帮助")
    await level_help.finish()

        
@level1_help.handle(parameterless=[Cooldown(cd_time=0)])
async def level1_help_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """品阶帮助"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    msg = __level1_help__
    await send_help_message(bot, event, msg, k1="灵根帮助", v1="灵根帮助", k2="境界帮助", v2="境界帮助", k3="修仙帮助", v3="修仙帮助")
    await level1_help.finish()
        
@level2_help.handle(parameterless=[Cooldown(cd_time=0)])
async def level2_help_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """境界帮助"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    msg = __level2_help__
    await send_help_message(bot, event, msg, k1="突破", v1="突破", k2="我的突破概率", v2="我的突破概率", k3="修仙帮助", v3="修仙帮助")
    await level2_help.finish()

@restart.handle(parameterless=[Cooldown(cd_time=0)])
async def restart_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, state: T_State):
    """刷新灵根信息"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await restart.finish()

    if user_info['stone'] < XiuConfig().remake:
        msg = "你的灵石还不够呢，快去赚点灵石吧！"
        await handle_send(bot, event, msg)
        await restart.finish()

    user_id = user_info['user_id']
    user_root = user_info['root_type']
  
    if user_root == '轮回道果' or user_root == '真·轮回道果' or user_root == '永恒道果' or user_root == '命运道果':
        msg = f"道友已入轮回，拥有{user_root}无需重入仙途！"
        await handle_send(bot, event, msg)
        await restart.finish()

    # 生成10个随机灵根选项
    linggen_options = []
    for _ in range(10):
        name, root_type = XiuxianJsonDate().linggen_get()
        linggen_options.append((name, root_type))
    
    # 显示所有随机生成的灵根选项
    linggen_list_msg = "本次随机生成的灵根有：\n"
    linggen_list_msg += "\n".join([f"{i+1}. {name} ({root_type})" for i, (name, root_type) in enumerate(linggen_options)])
    
    # 自动选择最佳灵根
    if JsonConfig().is_auto_root_selection_enabled():
        # 按灵根倍率排序选择最佳灵根
        selected_name, selected_root_type = max(linggen_options, 
                                             key=lambda x: jsondata.root_data()[x[1]]["type_speeds"])
        msg = sql_message.ramaker(selected_name, selected_root_type, user_id)
        await handle_send(bot, event, msg)
        await restart.finish()
    else:
        # 保留原来的手动选择逻辑
        state["user_id"] = user_id
        msg = f"{linggen_list_msg}\n\n请从以上灵根中选择一个:\n请输入对应的数字选择 (1-10):"
        state["linggen_options"] = linggen_options
        await handle_send(bot, event, msg, md_type="修仙", k1="手动选择", v1=" ", k2="自动最好", v2="最好", k3="刷新", v3="0")
        

@restart.receive()
async def handle_user_choice(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, state: T_State):
    user_choice = event.get_plaintext().strip()
    linggen_options = state["linggen_options"]
    user_id = state["user_id"]  # 从状态中获取用户ID
    selected_name, selected_root_type = max(linggen_options, key=lambda x: jsondata.root_data()[x[1]]["type_speeds"])

    if user_choice.isdigit(): # 判断数字
        user_choice = int(user_choice)
        if user_choice == 0:
            await restart_(bot, event, state)
            return
        elif 1 <= user_choice <= 10:
            selected_name, selected_root_type = linggen_options[user_choice - 1]
            msg = f"你选择了 {selected_name} 呢！\n"
    else:
        if user_choice == "最好":
            msg = "帮你自动选择最佳灵根了嗷！\n"        
        else:
            msg = "输入有误，帮你自动选择最佳灵根了嗷！\n"
   
    msg += sql_message.ramaker(selected_name, selected_root_type, user_id)

    await handle_send(bot, event, msg)


@rank.handle(parameterless=[Cooldown(cd_time=0)])
async def rank_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """排行榜"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    message = str(event.message)
    rank_msg = r'[\u4e00-\u9fa5]+'
    message = re.findall(rank_msg, message)
    if message:
        message = message[0]
    if message in ["排行榜", "修仙排行榜", "境界排行榜", "修为排行榜"]:
        p_rank = sql_message.realm_top()
        msg = "【位面境界排行榜 前五十】\n"
        num = 0
        for i in p_rank:
            num += 1
            msg += f"第{num}位：{i[0]} {i[1]}，修为{number_to(i[2])}\n"
            if num == 50:
                break
        await handle_send(bot, event, msg)
        await rank.finish()
    elif message == "灵石排行榜":
        a_rank = sql_message.stone_top()
        msg = "【位面灵石排行榜 前五十】\n"
        num = 0
        for i in a_rank:
            num += 1
            msg += f"第{num}位：{i[0]}，灵石{number_to(i[1])}枚\n"
            if num == 50:
                break
        await handle_send(bot, event, msg)
        await rank.finish()
    elif message == "战力排行榜":
        c_rank = sql_message.power_top()
        msg = "【位面战力排行榜 前五十】\n"
        num = 0
        for i in c_rank:
            num += 1
            msg += f"第{num}位：{i[0]}，战力{number_to(i[1])}\n"
            if num == 50:
                break
        await handle_send(bot, event, msg)
        await rank.finish()
    elif message in ["宗门排行榜", "宗门建设度排行榜"]:
        s_rank = sql_message.scale_top()
        msg = "【位面宗门建设排行榜 前五十】\n"
        num = 0
        for i in s_rank:
            num += 1
            msg += f"第{num}位：{i[1]}，建设度{number_to(i[2])}\n"
            if num == 50:
                break
        await handle_send(bot, event, msg)
        await rank.finish()
    elif message == "轮回排行榜":
        r_rank = sql_message.root_top()
        msg = "【轮回排行榜 前五十】\n"
        num = 0
        for i in r_rank:
            num += 1
            msg += f"第{num}位：{i[0]}，轮回{number_to(i[1])}次\n"
            if num == 50:
                break
        await handle_send(bot, event, msg)
        await rank.finish()


@season_rank.handle(parameterless=[Cooldown(cd_time=0)])
async def season_rank_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """赛季榜：展示当前赛季累计榜，暂无数据时展示常用排行榜参考"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    text = args.extract_plain_text() or str(event.message)
    mode = _parse_season_mode(text)
    rank_type = _parse_season_rank_type(text)
    msg = _build_season_rank_message(mode, rank_type)
    await handle_send(
        bot,
        event,
        msg,
        md_type="修仙",
        k1="周榜",
        v1="赛季榜 周榜",
        k2="月榜",
        v2="赛季榜 月榜",
        k3="季度榜",
        v3="赛季榜 季度榜",
    )
    await season_rank.finish()


@my_season_rank.handle(parameterless=[Cooldown(cd_time=0)])
async def my_season_rank_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """我的赛季：展示个人当前赛季累计积分"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await my_season_rank.finish()

    text = args.extract_plain_text() or str(event.message)
    mode = _parse_season_mode(text) if text else None
    if text and not any(keyword.lower() in str(text).strip().lower() for keyword in _SEASON_MODE_ALIASES):
        mode = None
    msg = _build_my_season_rank_message(user_info["user_id"], mode)
    await handle_send(
        bot,
        event,
        msg,
        md_type="修仙",
        k1="周榜",
        v1="赛季榜 周榜",
        k2="月榜",
        v2="赛季榜 月榜",
        k3="赛季榜",
        v3="赛季榜",
    )
    await my_season_rank.finish()


@user_stamina.handle(parameterless=[Cooldown(cd_time=0)])
async def user_stamina_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """我的体力信息"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await user_stamina.finish()
    msg = f"当前体力：{user_info['user_stamina']}\n每分钟回复：{XiuConfig().stamina_recovery_points}"
    await handle_send(bot, event, msg)
    await user_stamina.finish()


@give_stone.handle(parameterless=[Cooldown(cd_time=0)])
async def give_stone_(bot: Bot, event: GroupMessageEvent, args: Message = CommandArg()):
    """送灵石"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await give_stone.finish()
        
    user_id = user_info['user_id']
    hujiang_rank = convert_rank("江湖好手")[0]
    arg_list = args.extract_plain_text().split()
    
    if len(arg_list) < 2:
        msg = f"请输入正确的指令，例如：送灵石 少姜 600000"
        await handle_send(bot, event, msg)
        await give_stone.finish()
        
    stone_num = arg_list[1]  # 灵石数
    nick_name = arg_list[0]  # 道号
    
    if not stone_num.isdigit():
        msg = f"请输入正确的灵石数量！"
        await handle_send(bot, event, msg)
        await give_stone.finish()
        
    give_stone_num = int(stone_num)
    if give_stone_num <= 0:
        msg = "请输入大于零的灵石数量！"
        await handle_send(bot, event, msg)
        await give_stone.finish()

    give_qq = get_at_user_id(args)
    give_user = (
        sql_message.get_user_info_with_id(give_qq)
        if give_qq else sql_message.get_user_info_with_name(nick_name)
    )
    if not give_user:
        msg = "对方未踏入修仙界，不可赠送！"
        await handle_send(bot, event, msg)
        await give_stone.finish()
    recipient_id = str(give_user['user_id'])
    if recipient_id == str(user_id):
        await handle_send(bot, event, "请不要送灵石给自己！")
        await give_stone.finish()

    operation_id = _stone_gift_operation_id(event, user_id, recipient_id)
    result = stone_gift_service.get_operation(operation_id, user_id, recipient_id)
    if result is not None:
        msg = (
            f"共赠送{number_to(result.gross_amount)}枚灵石给{give_user['user_name']}道友！"
            f"收取手续费{number_to(result.fee_amount)}枚"
        )
        await handle_send(bot, event, msg)
        await give_stone.finish()

    user_rank = convert_rank(user_info['level'])[0]
    daily_send_limit = 100000000 + (hujiang_rank - user_rank) * 20000000
    already_sent = stone_limit.get_send_limit(user_id)
    remaining_send = daily_send_limit - already_sent
    if give_stone_num > remaining_send:
        msg = f"道友今日已送{number_to(already_sent)}灵石，还可赠送{number_to(remaining_send)}灵石！"
        await handle_send(bot, event, msg)
        await give_stone.finish()

    receiver_rank = convert_rank(give_user['level'])[0]
    daily_receive_limit = 100000000 + (hujiang_rank - receiver_rank) * 20000000
    already_received = stone_limit.get_receive_limit(recipient_id)
    remaining_receive = daily_receive_limit - already_received
    net_amount = give_stone_num - int(give_stone_num * 0.1)
    if net_amount > remaining_receive:
        msg = f"{give_user['user_name']}道友今日已收{number_to(already_received)}灵石，还可接收{number_to(remaining_receive)}灵石！"
        await handle_send(bot, event, msg)
        await give_stone.finish()

    result = stone_gift_service.transfer(
        operation_id,
        user_id,
        recipient_id,
        give_stone_num,
    )
    if result.status == "stone_insufficient":
        msg = "道友的灵石不够，请重新输入！"
    elif result.status == "recipient_missing":
        msg = "对方未踏入修仙界，不可赠送！"
    elif result.succeeded:
        if result.status == "transferred":
            stone_limit.update_send_limit(user_id, result.gross_amount)
            stone_limit.update_receive_limit(recipient_id, result.net_amount)
        msg = (
            f"共赠送{number_to(result.gross_amount)}枚灵石给{give_user['user_name']}道友！"
            f"收取手续费{number_to(result.fee_amount)}枚"
        )
    else:
        msg = "双方灵石状态已经变化，请稍后重试！"
    await handle_send(bot, event, msg)
    await give_stone.finish()


@steal_stone.handle(parameterless=[Cooldown(cd_time=300)])
async def steal_stone_(bot: Bot, event: GroupMessageEvent, args: Message = CommandArg()):
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await steal_stone.finish()
    
    user_id = str(user_info['user_id'])
    coststone_num = int(XiuConfig().tou)
    steal_qq = get_at_user_id(args)
    nick_name = args.extract_plain_text().split()[0] if args.extract_plain_text().split() else None
    if nick_name:
        give_message = sql_message.get_user_info_with_name(nick_name)
        if give_message:
            steal_qq = give_message['user_id']
        else:
            steal_qq = None

    if not steal_qq:
        await handle_send(bot, event, "对方未踏入修仙界，不要对杂修出手！")
        await steal_stone.finish()
    steal_qq = str(steal_qq)
    if steal_qq == user_id:
        await handle_send(bot, event, "请不要偷自己刷成就！")
        await steal_stone.finish()
    steal_user = sql_message.get_user_info_with_id(steal_qq)
    if not steal_user:
        await handle_send(bot, event, "对方未踏入修仙界，不要对杂修出手！")
        await steal_stone.finish()

    operation_id = _stone_theft_operation_id(event, user_id)
    previous = stone_contest_service.replay_theft(operation_id, user_id, steal_qq)
    if previous is not None:
        if not previous.succeeded:
            await handle_send(bot, event, "本次偷窃与首次请求目标不一致，未重复结算。")
            await steal_stone.finish()
        msg, _ = _stone_theft_messages(
            previous, user_info['user_name'], steal_user['user_name']
        )
        await handle_send(bot, event, msg)
        await steal_stone.finish()

    if coststone_num > int(user_info['stone']):
        await handle_send(bot, event, "道友的偷窃准备(灵石)不足，请打工之后再切格瓦拉！")
        await steal_stone.finish()
    steal_user_stone = min(max(0, int(steal_user['stone'])), 1000000)
    if steal_user_stone <= 0:
        await handle_send(bot, event, "对方已经没有灵石可偷了！")
        await steal_stone.finish()

    power_rate = OtherSet().get_power_rate(user_info['power'], steal_user['power'])
    if not isinstance(power_rate, int):
        await handle_send(bot, event, power_rate)
        await steal_stone.finish()

    steal_success = random.randint(0, 100)
    if steal_success > power_rate:
        outcome = "failure"
        requested_amount = coststone_num
    else:
        lower = max(1, int(XiuConfig().tou_lower_limit * steal_user_stone))
        upper = max(lower, int(XiuConfig().tou_upper_limit * steal_user_stone))
        requested_amount = min(random.randint(lower, upper), 1000000)
        outcome = "success"

    settlement = stone_contest_service.settle_theft(
        operation_id,
        user_id,
        steal_qq,
        outcome=outcome,
        requested_amount=requested_amount,
        penalty_amount=coststone_num,
        stamina_cost=10,
    )
    if settlement.status == "stamina_insufficient":
        msg = "你没有足够的体力，请等待体力恢复后再试！"
    elif settlement.status == "stone_insufficient":
        msg = "道友的偷窃准备(灵石)不足，请打工之后再切格瓦拉！"
    elif settlement.status == "payer_empty":
        msg = "对方已经没有灵石可偷了！"
    elif settlement.status == "user_missing":
        msg = "对方未踏入修仙界，不要对杂修出手！"
    elif not settlement.succeeded:
        msg = "双方灵石或体力状态已经变化，本次偷窃未结算。"
    else:
        msg, victim_msg = _stone_theft_messages(
            settlement, user_info['user_name'], steal_user['user_name']
        )
        if settlement.status == "settled" and victim_msg:
            log_message(user_id, msg)
            log_message(steal_qq, victim_msg)
    await handle_send(bot, event, msg)
    await steal_stone.finish()

@rob_stone.handle(parameterless=[Cooldown(cd_time=300)])
async def rob_stone_(bot: Bot, event: GroupMessageEvent, args: Message = CommandArg()):
    """抢劫"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await rob_stone.finish()
    
    user_id = str(user_info["user_id"])
    user_mes = sql_message.get_user_info_with_id(user_id)
    give_qq = get_at_user_id(args)
    nick_name = args.extract_plain_text().split()[0] if args.extract_plain_text().split() else None
    if nick_name:
        give_message = sql_message.get_user_info_with_name(nick_name)
        if give_message:
            give_qq = give_message['user_id']
        else:
            give_qq = None

    if not give_qq:
        await handle_send(bot, event, "对方未踏入修仙界，不可抢劫！")
        await rob_stone.finish()
    give_qq = str(give_qq)
    user_2 = sql_message.get_user_info_with_id(give_qq)
    if not user_mes or not user_2:
        await handle_send(bot, event, "对方未踏入修仙界，不可抢劫！")
        await rob_stone.finish()
    if give_qq == user_id:
        await handle_send(bot, event, "请不要抢自己刷成就！")
        await rob_stone.finish()

    operation_id = _stone_robbery_operation_id(event, user_id)
    previous = stone_robbery_service.replay(operation_id, user_id, give_qq)
    if previous is not None:
        if not previous.succeeded:
            await handle_send(bot, event, "本次抢劫与首次请求目标不一致，未重复结算。")
            await rob_stone.finish()
        await send_msg_handler(bot, event, '决斗场', bot.self_id, previous.battle_messages)
        msg, _ = _stone_robbery_messages(previous, user_mes['user_name'])
        await handle_send(bot, event, msg)
        await rob_stone.finish()

    if user_mes['root'] == "凡人":
        await handle_send(bot, event, "目前职业无法抢劫！")
        await rob_stone.finish()
    if user_2['root'] == "凡人":
        await handle_send(bot, event, "对方职业无法被抢劫！")
        await rob_stone.finish()

    is_type, msg = check_user_type(user_id, 0)
    if not is_type:
        await handle_send(bot, event, msg)
        await rob_stone.finish()
    is_type, msg = check_user_type(give_qq, 0)
    if not is_type:
        await handle_send(bot, event, "对方现在在闭关呢，无法抢劫！")
        await rob_stone.finish()

    victim_hp = user_2['exp'] // 2 if user_2['hp'] is None else user_2['hp']
    robber_hp = user_mes['exp'] // 2 if user_mes['hp'] is None else user_mes['hp']
    if victim_hp <= user_2['exp'] / 10:
        time_2 = leave_harm_time(give_qq)
        msg = f"对方重伤藏匿了，无法抢劫！距离对方脱离生命危险还需要{time_2}分钟！"
        await handle_send(bot, event, msg)
        await rob_stone.finish()
    if robber_hp <= user_mes['exp'] / 10:
        time_msg = leave_harm_time(user_id)
        msg = f"重伤未愈，动弹不得！距离脱离生命危险还需要{time_msg}分钟！"
        msg += "请道友进行闭关，或者使用药品恢复气血，不要干等，没有自动回血！！！"
        await handle_send(bot, event, msg)
        await rob_stone.finish()
    if int(user_mes['user_stamina'] or 0) < 15:
        await handle_send(bot, event, "你没有足够的体力，请等待体力恢复后再试！")
        await rob_stone.finish()

    expected_robber = {
        key: user_mes[key] for key in ("hp", "mp", "user_stamina", "exp", "stone")
    }
    expected_victim = {
        key: user_2[key] for key in ("hp", "mp", "user_stamina", "exp", "stone")
    }
    player1 = _stone_robbery_player(user_mes)
    player2 = _stone_robbery_player(user_2)
    battle_messages, winner_id, final = OtherSet().player_fight(player1, player2)
    settlement = stone_robbery_service.settle(
        operation_id,
        user_id,
        give_qq,
        expected_robber=expected_robber,
        expected_victim=expected_victim,
        robber_final=final[user_id],
        victim_final=final[give_qq],
        winner_id=winner_id,
        battle_messages=battle_messages,
        stamina_cost=15,
    )
    if settlement.status == "stamina_insufficient":
        msg = "你没有足够的体力，请等待体力恢复后再试！"
    elif settlement.status == "robber_injured":
        msg = "重伤未愈，本次抢劫未结算。"
    elif settlement.status == "victim_injured":
        msg = "对方已经重伤，本次抢劫未结算。"
    elif settlement.status == "user_missing":
        msg = "对方未踏入修仙界，不可抢劫！"
    elif not settlement.succeeded:
        msg = "双方战斗、灵石或体力状态已经变化，本次抢劫未结算。"
    else:
        await send_msg_handler(bot, event, '决斗场', bot.self_id, settlement.battle_messages)
        msg, victim_msg = _stone_robbery_messages(settlement, user_mes['user_name'])
        if settlement.status == "applied":
            log_message(user_id, msg)
            log_message(give_qq, victim_msg)
    await handle_send(bot, event, msg)
    await rob_stone.finish()

@view_logs.handle(parameterless=[Cooldown(cd_time=0)])
async def view_logs_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """查看修仙日志（增强版）"""
    args = args.extract_plain_text().split()
    date_str = None
    page = 1
    
    # 解析参数
    if len(args) >= 1:
        # 检查第一个参数是否是6位数字（日期格式yymmdd）
        if args[0].isdigit() and len(args[0]) == 6:
            date_str = args[0]
            # 如果有第二个参数且是数字，作为页码
            if len(args) >= 2 and args[1].isdigit():
                page = int(args[1])
        elif args[0].isdigit():
            # 如果只有一个数字参数，作为页码
            page = int(args[0])
    
    user_id = event.get_user_id()
    logs_data = get_logs(user_id, date_str=date_str, page=page)
    
    # 处理各种情况
    if "error" in logs_data:
        msg = f"获取日志失败：{logs_data['error']}"
        await handle_send(bot, event, msg)
        await view_logs.finish()
    
    if not logs_data["logs"]:
        if logs_data.get("available_dates"):
            # 有可用日期但当前日期无数据
            recent_dates = ", ".join(logs_data["available_dates"][:3])
            msg = f"{logs_data.get('message', '当前日期无日志')}\n最近有日志的日期：{recent_dates}"
        else:
            msg = logs_data.get('message', '暂无日志记录')
        await handle_send(bot, event, msg)
        await view_logs.finish()
    
    # 构建日志消息
    date_display = logs_data.get('date', '未知日期')
    current_page = logs_data['current_page']
    total_pages = logs_data['total_pages']
    
    # 添加提示信息
    header = f"【修仙日志】\n日期：{date_display}\n页码：{current_page}/{total_pages}\n"
    if logs_data.get('page_adjusted'):
        header += f"{logs_data['page_adjusted']}\n"
    if logs_data.get('date_auto_selected'):
        header += f"{logs_data['date_auto_selected']}\n"
    
    msg_parts = [header]
    
    for log in logs_data["logs"]:
        timestamp = log.get('timestamp', '未知时间')
        message = log.get('message', '')
        msg_parts.append(f"[{timestamp}]\n{message}")
    
    # 添加翻页提示
    if total_pages > 1:
        page_hint = f"发送【修仙日志 {date_display} 页码】查看其他页"
        if logs_data.get('available_dates'):
            recent_dates = "、".join(logs_data['available_dates'][:3])
            page_hint += f"\n其他可用日期：{recent_dates}"
        msg_parts.append(page_hint)
    
    await send_msg_handler(bot, event, '修仙日志', bot.self_id, msg_parts)
    await view_logs.finish()

@view_data.handle(parameterless=[Cooldown(cd_time=0)])
async def view_data_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """查看修仙数据"""
    user_id = event.get_user_id()
    stats_data = get_statistics_data(user_id)
    
    if not stats_data:
        msg = "暂无统计数据"
        await handle_send(bot, event, msg)
        await view_data.finish()
    
    sorted_keys = sorted(stats_data.keys())
    title = "【修仙统计数据】\n"
    stats_message = ""
    for key in sorted_keys:
        value = stats_data[key]
        formatted_value = str(value)
        stats_message += f"- {key}：{number_to(formatted_value)}\n"
    
    msg_list = []
    msg_list.append(stats_message)
    await send_msg_handler(bot, event, '统计数据', bot.self_id, msg_list, title=title)
    await view_data.finish()

def generate_daohao():
    """生成严格控制在2-7实际汉字长度的道号系统（完整词库版）"""
    # 拼接符号库（不计入总字数）
    connectors = ['·', '-', '※']
    
    # 姓氏库（单姓、复姓和三字姓）
    family_names = {
        'single': [
            '李', '王', '张', '刘', '陈', '杨', '赵', '黄', '周', '吴',
            '玄', '玉', '清', '云', '风', '霜', '雪', '月', '星', '阳',
            '金', '木', '水', '火', '土', '阴', '阳', '乾', '坤', '艮',
            '神', '仙', '圣', '佛', '魔', '妖', '鬼', '邪', '煞', '冥',
            '天', '昊', '穹', '苍', '幽', '冥', '太', '上', '元', '始',
            '剑', '刀', '枪', '戟', '弓', '琴', '棋', '书', '画', '符'
        ],
        'double': [
            '轩辕', '上官', '欧阳', '诸葛', '司马', '皇甫', '司空', '东方', '南宫', '西门',
            '长孙', '宇文', '慕容', '司徒', '令狐', '澹台', '公冶', '申屠', '太史', '端木',
            '青松', '白石', '碧泉', '紫竹', '金枫', '玉梅', '寒潭', '幽兰', '流云', '飞雪',
            '惊雷', '暮雨', '晨露', '晚霞', '孤峰', '断崖', '古木', '残阳', '新月', '繁星',
            '九霄', '太虚', '凌霄', '玄天', '紫霄', '青冥', '碧落', '黄泉', '星河', '月华',
            '昆仑', '蓬莱', '方丈', '瀛洲', '岱舆', '员峤', '峨眉', '青城', '天山', '沧海'
        ],
        'triple': [
            '太乙玄', '九幽寒', '凌霄子', '紫阳君', '玄冥上', '青莲剑', '白虹贯', '金乌曜',
            '玉虚宫', '碧游仙', '黄泉路', '血煞魔', '噬魂妖', '夺魄鬼', '摄心怪', '炼尸精'
        ]
    }

    # 名字库（单字、双字和三字）
    given_names = {
        'single': [
            '子', '尘', '空', '灵', '虚', '真', '元', '阳', '明', '玄',
            '霄', '云', '风', '雨', '雪', '霜', '露', '霞', '雾', '虹',
            '剑', '刃', '锋', '芒', '光', '影', '气', '意', '心', '神',
            '丹', '药', '炉', '鼎', '火', '炎', '金', '玉', '玄', '灵',
            '佛', '禅', '法', '僧', '念', '定', '慧', '戒', '忍', '悟',
            '龙', '凤', '麟', '龟', '虎', '雀', '鹏', '蛟', '猿', '鹤'
        ],
        'double': [
            '太虚', '紫阳', '玄灵', '玉真', '无尘', '逍遥', '长生', '不老', '凌霄', '琼华',
            '妙法', '通玄', '悟真', '明心', '见性', '合道', '冲虚', '守一', '抱朴', '坐忘',
            '青锋', '寒光', '流影', '断水', '破岳', '斩龙', '诛邪', '戮仙', '天问', '无尘',
            '九转', '七返', '五气', '三花', '金丹', '玉液', '炉火', '鼎纹', '药王', '灵枢',
            '菩提', '明镜', '般若', '金刚', '罗汉', '菩萨', '佛陀', '禅心', '觉悟', '轮回',
            '青龙', '白虎', '朱雀', '玄武', '麒麟', '凤凰', '鲲鹏', '蛟龙', '仙鹤', '灵龟'
        ],
        'triple': [
            '太乙剑', '九幽火', '凌霄子', '紫阳君', '玄冥气', '青莲剑', '白虹贯', '金乌曜',
            '玉虚宫', '碧游仙', '黄泉路', '血煞魔', '噬魂妖', '夺魄鬼', '摄心怪', '炼尸精',
            '混元一', '两仪生', '三才立', '四象成', '五行转', '六合聚', '七星列', '八卦演',
            '九宫变', '十方界', '百炼钢', '千幻影', '万法归', '亿劫渡', '无量寿', '永恒道'
        ]
    }

    # 修饰词库（1-5字）
    modifiers = {
        'single': [
            '子', '君', '公', '仙', '圣', '尊', '王', '皇', '帝', '祖',
            '魔', '妖', '鬼', '怪', '精', '灵', '魅', '魍', '魉', '尸',
            '神', '佛', '道', '儒', '剑', '刀', '枪', '戟', '弓', '琴'
        ],
        'double': [
            '真人', '真君', '上仙', '金仙', '天君', '星君', '元君', '道君', '老祖', '天尊',
            '剑仙', '剑魔', '剑圣', '剑痴', '剑狂', '剑鬼', '剑妖', '剑神', '剑尊', '剑帝',
            '丹圣', '药尊', '炉仙', '鼎君', '火灵', '炎帝', '金仙', '玉女', '玄师', '灵童',
            '尊者', '罗汉', '菩萨', '佛陀', '禅师', '法师', '和尚', '头陀', '沙弥', '比丘',
            '妖王', '魔尊', '鬼帝', '怪皇', '精主', '灵母', '魅仙', '魍圣', '魉神', '尸祖'
        ],
        'triple': [
            '大罗仙', '混元子', '太乙尊', '玄天君', '紫霄神', '青冥主', '碧落仙', '黄泉使',
            '星河君', '月华主', '日曜神', '云海仙', '风雷尊', '霜雪神', '虹霓使', '霞光君',
            '昆仑仙', '蓬莱客', '方丈僧', '瀛洲使', '岱舆君', '员峤主', '峨眉仙', '青城道',
            '金刚身', '罗汉果', '菩提心', '般若智', '明王怒', '如来掌', '天魔舞', '血煞阵'
        ],
        'quad': [
            '太乙救苦', '混元无极', '玄天上帝', '紫霄雷帝', '青冥剑主', '碧落黄泉', '星河倒悬',
            '月华如水', '日曜中天', '云海翻腾', '风雷激荡', '霜雪漫天', '虹霓贯日', '霞光万道',
            '昆仑之巅', '蓬莱仙岛', '方丈神山', '瀛洲幻境', '金刚不坏', '罗汉金身', '菩提般若',
            '明王怒火', '如来神掌', '天魔乱舞', '血煞冲天', '幽冥鬼域', '黄泉路上', '九幽之主',
            '噬魂夺魄'
        ]
    }

    # 选择修饰词类型（权重分配）
    mod_type = random.choices(
        ['single', 'double', 'triple', 'quad'],
        weights=[65, 20, 10, 5]
    )[0]
    modifier = random.choice(modifiers[mod_type])

    # 根据修饰词长度选择姓氏和名字
    if mod_type == 'quad':  # 5字修饰词特殊处理
        # 只能搭配单字姓或单字名
        if random.random() < 0.7:
            family_name = random.choice(family_names['single'])
            given_name = ""
        else:
            family_name = ""
            given_name = random.choice(given_names['single'])
    else:
        # 正常选择姓氏（单70%，复25%，三字5%）
        family_type = random.choices(
            ['single', 'double', 'triple'],
            weights=[70, 25, 5]
        )[0]
        family_name = random.choice(family_names[family_type])
        
        # 正常选择名字（单40%，双50%，三字10%）
        given_type = random.choices(
            ['single', 'double', 'triple'],
            weights=[40, 50, 10]
        )[0]
        given_name = random.choice(given_names[given_type])

    # 可选的拼接符号（30%概率添加）
    connector = random.choices(
        ['', random.choice(connectors)],
        weights=[70, 30]
    )[0]

    # 计算实际汉字长度（忽略连接符）
    def real_length(s):
        return len([c for c in s if c not in connectors])

    # 生成所有可能的结构选项（带权重）
    options = []

    # 1. 正向结构：姓[+连接符]+名[+连接符]+修饰词
    def add_option(parts, weight):
        s = connector.join(filter(None, parts))
        if 2 <= real_length(s) <= 7:
            options.append((s, weight))

    # 正向组合
    add_option([family_name, given_name, modifier], 25)  # 姓+名+修饰词
    add_option([family_name, modifier], 15)             # 姓+修饰词
    add_option([given_name, modifier], 15)              # 名+修饰词
    add_option([family_name, given_name], 10)          # 姓+名

    # 倒装组合（确保修饰词位置正确）
    add_option([modifier, given_name, family_name], 10)  # 修饰词+名+姓
    add_option([modifier, family_name], 8)               # 修饰词+姓
    add_option([modifier, given_name], 7)                # 修饰词+名

    # 单独使用（需长度2-7）
    if 2 <= len(modifier) <= 7:
        options.append((modifier, 5))  # 单独修饰词
    if family_name and given_name:
        add_option([family_name, given_name], 5)  # 姓+名（已添加，权重叠加）

    # 如果没有合适选项（理论上不会发生），返回保底结果
    if not options:
        return modifier[:7] if len(modifier) >= 2 else "道君"

    # 按权重随机选择
    daohao_list, weights = zip(*options)
    daohao = random.choices(daohao_list, weights=weights)[0]

    # 最终验证
    if not (2 <= real_length(daohao) <= 7):
        return generate_daohao()  # 重新生成
    
    return daohao

async def reset_lottery_participants():
    business_date = datetime.now().date().isoformat()
    lottery_settlement_service.get_snapshot(business_date)
    logger.opt(colors=True).info(
        f"<green>鸿运业务日已切换：{business_date}</green>"
    )
    
async def reset_stone_limits():
    stone_limit.reset_limits()
    logger.opt(colors=True).info(f"<green>每日灵石赠送额度已重置！</green>")

@xiuxian_world_info.handle(parameterless=[])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """
    修仙界信息
    """

    # 获取所有境界列表（从低到高）
    _, all_ranks = convert_rank("江湖好手")

    # 构建境界 → 人数 的映射
    realm_count = {}
    for rank in all_ranks:
        count = sql_message.get_user_count_by_level(rank)
        realm_count[rank] = count

    total_users = sum(realm_count.values())

    lines = []
    lines_title = f"【修仙界概况】\n当前共有道友 {total_users} 人"
    for rank in all_ranks:
        cnt = realm_count.get(rank, 0)
        if cnt > 0:
            lines.append(f"- {rank}：{number_to(cnt)} 人")
                
    await send_msg_handler(bot, event, '修仙界信息', bot.self_id, lines, title=lines_title)
