"""Bangumi 放送日历 — 仅指令查询，无 RSS / 无定时推送。"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from nonebot.params import CommandArg

from ..command import *
from ...xiuxian_utils.http_proxy import requests_get
from ...xiuxian_utils.utils import (
    parse_page_arg,
    paginate_text_blocks,
    build_pagination_buttons,
    send_help_message,
)

_BGM_CALENDAR = "https://api.bgm.tv/calendar"
_WEEKDAY_CN = ("周一", "周二", "周三", "周四", "周五", "周六", "周日")
_UA = {"User-Agent": "xiuxian-entertainment-bangumi/1.0"}


def _today_weekday_index() -> int:
    return datetime.now().weekday()


def fetch_bangumi_calendar(timeout: int = 25) -> list[dict[str, Any]]:
    """Bangumi API：7 天块，每项含 weekday + items（走 XiuConfig 自定义代理）。"""
    resp = requests_get(_BGM_CALENDAR, timeout=timeout, headers=_UA, use_config_proxy=True)
    resp.raise_for_status()
    data = resp.json()
    if not isinstance(data, list):
        return []
    return [x for x in data if isinstance(x, dict)]


def _format_item_line(index: int, it: dict[str, Any]) -> str:
    name_cn = (it.get("name_cn") or "").strip()
    name = (it.get("name") or "").strip()
    label = name_cn or name or "未知"
    if name_cn and name and name_cn != name:
        label = f"{name_cn}（{name}）"
    air = (it.get("air_time") or "").strip()
    rating = it.get("rating") or {}
    score = ""
    if isinstance(rating, dict) and rating.get("score"):
        score = f" 评分{rating['score']}"
    eps = it.get("eps")
    ep_info = f" 共{eps}话" if eps else ""
    line = f"{index}. {label}"
    if air:
        line += f" · {air}"
    return line + score + ep_info


def format_today_message(items: list[dict[str, Any]]) -> str:
    wd = _WEEKDAY_CN[_today_weekday_index()]
    today = datetime.now().strftime("%Y-%m-%d")
    lines = [f"【今日番剧】{today} {wd}", ""]
    if not items:
        lines.append("今日放送表暂无条目。")
    else:
        for i, it in enumerate(items, start=1):
            lines.append(_format_item_line(i, it))
    lines.append("")
    lines.append("数据来源：Bangumi 放送日历")
    return "\n".join(lines)


def format_week_message(days: list[dict[str, Any]], max_per_day: int = 40) -> str:
    today = datetime.now().strftime("%Y-%m-%d")
    lines = [f"【每周番剧放送表】{today}", ""]
    if not days:
        lines.append("暂无数据。")
        return "\n".join(lines)

    for block in days:
        wd = block.get("weekday") or {}
        wd_cn = (wd.get("cn") or "").strip() if isinstance(wd, dict) else ""
        if not wd_cn and isinstance(wd, dict):
            en = (wd.get("en") or "").strip()
            wd_cn = en or "未知"
        items = block.get("items") or []
        if not isinstance(items, list):
            items = []
        section = [f"【{wd_cn}】共 {len(items)} 部"]
        if not items:
            section.append("（无）")
        else:
            for i, it in enumerate(items[:max_per_day], start=1):
                if isinstance(it, dict):
                    section.append(_format_item_line(i, it))
            if len(items) > max_per_day:
                section.append(f"… 另有 {len(items) - max_per_day} 部未列出")
        lines.append("\n".join(section))
        lines.append("")

    lines.append("数据来源：Bangumi 放送日历")
    return "\n".join(lines).strip()


def _items_for_today(days: list[dict[str, Any]]) -> list[dict[str, Any]]:
    idx = _today_weekday_index()
    if idx < len(days):
        items = days[idx].get("items") or []
        if isinstance(items, list):
            return [x for x in items if isinstance(x, dict)]
    return []


today_bangumi_cmd = on_command(
    "今日番剧",
    aliases={"每日番剧", "番剧日历"},
    priority=5,
    block=True,
)

week_bangumi_cmd = on_command(
    "番剧周表",
    aliases={"每周番剧", "番剧总表"},
    priority=5,
    block=True,
)


@today_bangumi_cmd.handle(parameterless=[Cooldown(cd_time=8)])
async def today_bangumi_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    try:
        days = fetch_bangumi_calendar()
        items = _items_for_today(days)
        msg = format_today_message(items)
        await handle_send(
            bot,
            event,
            msg,
            md_type="娱乐",
            k1="周表",
            v1="番剧周表",
            k2="刷新",
            v2="今日番剧",
            k3="帮助",
            v3="娱乐帮助",
        )
    except Exception as e:
        await handle_send(
            bot,
            event,
            f"获取番剧失败：{e}",
            md_type="娱乐",
            k1="重试",
            v1="今日番剧",
            k3="帮助",
            v3="娱乐帮助",
        )
    await today_bangumi_cmd.finish()


@week_bangumi_cmd.handle(parameterless=[Cooldown(cd_time=10)])
async def week_bangumi_(
    bot: Bot,
    event: GroupMessageEvent | PrivateMessageEvent,
    args: Message = CommandArg(),
):
    page = parse_page_arg(args.extract_plain_text())
    try:
        days = fetch_bangumi_calendar()
        full = format_week_message(days)
        msg, page, total_pages = paginate_text_blocks(full, page, per_page=2)
        if total_pages > 1:
            msg = f"{msg}\n\n翻页：番剧周表 页码"
        button_kwargs = build_pagination_buttons(
            "番剧周表",
            page,
            total_pages,
            extras=[
                ("今日", "今日番剧"),
                ("娱乐帮助", "娱乐帮助"),
            ],
        )
        await send_help_message(bot, event, msg, **button_kwargs)
    except Exception as e:
        await handle_send(
            bot,
            event,
            f"获取番剧周表失败：{e}",
            md_type="娱乐",
            k1="重试",
            v1="番剧周表",
            k3="帮助",
            v3="娱乐帮助",
        )
    await week_bangumi_cmd.finish()