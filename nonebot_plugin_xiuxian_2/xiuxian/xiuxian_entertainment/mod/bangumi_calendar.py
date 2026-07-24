"""番剧放送日历 — 仅指令查询，无 RSS / 无定时推送。

数据源：Jikan `seasons/now`（MyAnimeList 当季放送）。
本机实测 api.bgm.tv 直连/代理均不可达；Jikan seasons/now 直连可用。
"""
from __future__ import annotations

import time
from datetime import datetime
from typing import Any

from nonebot.params import CommandArg

from ..command import *
from ...xiuxian_utils.http_proxy import requests_get, describe_proxy_request_error
from ...xiuxian_utils.utils import (
    parse_page_arg,
    paginate_text_blocks,
    build_pagination_buttons,
    send_help_message,
)

_JIKAN_SEASONS_NOW = "https://api.jikan.moe/v4/seasons/now"
_WEEKDAY_CN = ("周一", "周二", "周三", "周四", "周五", "周六", "周日")
# Jikan broadcast.day 常见写法
_JIKAN_DAY_TO_IDX = {
    "mondays": 0,
    "monday": 0,
    "tuesdays": 1,
    "tuesday": 1,
    "wednesdays": 2,
    "wednesday": 2,
    "thursdays": 3,
    "thursday": 3,
    "fridays": 4,
    "friday": 4,
    "saturdays": 5,
    "saturday": 5,
    "sundays": 6,
    "sunday": 6,
}
_UA = {
    "User-Agent": (
        "Mozilla/5.0 (Linux; Android 14) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36"
    ),
    "Accept": "application/json",
}
# 短缓存，降低 Jikan 限频与抖动
_CACHE_TTL_SEC = 900
_cache_at: float = 0.0
_cache_days: list[dict[str, Any]] | None = None


def _today_weekday_index() -> int:
    return datetime.now().weekday()


def _jikan_day_index(day: str | None) -> int | None:
    if not day:
        return None
    return _JIKAN_DAY_TO_IDX.get(str(day).strip().lower())


def _item_from_jikan(it: dict[str, Any]) -> dict[str, Any]:
    """转成旧 Bangumi 条目字段，复用现有 format_* 文案。"""
    title = (it.get("title") or "").strip()
    title_en = (it.get("title_english") or "").strip()
    title_jp = (it.get("title_japanese") or "").strip()
    # name_cn 优先日文/英文展示习惯：有日文用日文，否则 title
    name_cn = title_jp or title_en or title
    name = title if title and title != name_cn else (title_en or title or name_cn)
    broadcast = it.get("broadcast") if isinstance(it.get("broadcast"), dict) else {}
    air = (broadcast.get("string") or "").strip()
    score = it.get("score")
    rating: dict[str, Any] = {}
    if score is not None:
        try:
            rating = {"score": float(score)}
        except (TypeError, ValueError):
            rating = {}
    eps = it.get("episodes")
    return {
        "name_cn": name_cn or "未知",
        "name": name or name_cn or "未知",
        "air_time": air,
        "rating": rating,
        "eps": eps,
        "url": it.get("url") or "",
        "mal_id": it.get("mal_id"),
    }


def _empty_week_blocks() -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for i, cn in enumerate(_WEEKDAY_CN):
        out.append(
            {
                "weekday": {"cn": cn, "id": i + 1},
                "items": [],
            }
        )
    # 第 8 块：放送日未知（仅周表展示）
    out.append({"weekday": {"cn": "放送日未知", "id": 0}, "items": []})
    return out


def _http_get_json(url: str, *, timeout: int, use_proxy: bool) -> dict[str, Any]:
    resp = requests_get(
        url,
        timeout=timeout,
        headers=_UA,
        use_config_proxy=use_proxy,
    )
    resp.raise_for_status()
    data = resp.json()
    if not isinstance(data, dict):
        raise ValueError("Jikan 返回非 JSON 对象")
    return data


def _fetch_seasons_now_pages(timeout: int = 25, max_pages: int = 6) -> list[dict[str, Any]]:
    """拉取当季列表；直连优先，失败再代理。单页失败时保留已拉到的数据。"""
    last_err: BaseException | None = None

    def _load_all(use_proxy: bool) -> list[dict[str, Any]]:
        collected: list[dict[str, Any]] = []
        page = 1
        while page <= max_pages:
            url = f"{_JIKAN_SEASONS_NOW}?sfw=true&page={page}"
            page_ok = False
            page_err: BaseException | None = None
            for attempt in range(3):
                try:
                    payload = _http_get_json(url, timeout=timeout, use_proxy=use_proxy)
                    chunk = payload.get("data") or []
                    if not isinstance(chunk, list):
                        chunk = []
                    for it in chunk:
                        if isinstance(it, dict):
                            collected.append(it)
                    page_ok = True
                    pag = payload.get("pagination") or {}
                    if not pag.get("has_next_page"):
                        return collected
                    break
                except BaseException as e:
                    page_err = e
                    time.sleep(0.4 * (attempt + 1))
            if not page_ok:
                # 已有数据则返回部分结果，避免整表失败
                if collected:
                    return collected
                if page_err is not None:
                    raise page_err
                break
            page += 1
            if page <= max_pages:
                time.sleep(0.45)
        return collected

    # 1) 直连 2) 代理
    for use_proxy in (False, True):
        try:
            collected = _load_all(use_proxy)
            if collected:
                return collected
            last_err = ValueError("Jikan 当季列表为空")
        except BaseException as e:
            last_err = e
            continue
    if last_err is not None:
        raise last_err
    return []


def fetch_bangumi_calendar(timeout: int = 25) -> list[dict[str, Any]]:
    """返回 7（+未知）天块，每项含 weekday + items（兼容旧 format_*）。

    源：Jikan /v4/seasons/now（仅 airing=true 的条目按 broadcast.day 归入周几）。
    """
    global _cache_at, _cache_days
    now = time.time()
    if _cache_days is not None and (now - _cache_at) < _CACHE_TTL_SEC:
        return _cache_days

    raw = _fetch_seasons_now_pages(timeout=timeout)
    blocks = _empty_week_blocks()
    for it in raw:
        if not it.get("airing"):
            continue
        broadcast = it.get("broadcast") if isinstance(it.get("broadcast"), dict) else {}
        idx = _jikan_day_index(broadcast.get("day") if broadcast else None)
        item = _item_from_jikan(it)
        if idx is None:
            blocks[7]["items"].append(item)
        else:
            blocks[idx]["items"].append(item)

    # 周表仍按周一到周日；未知日单独一块
    _cache_days = blocks
    _cache_at = now
    return blocks


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
    lines.append("数据来源：Jikan / MyAnimeList 当季放送")
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
        # 周表默认只展示周一～周日；未知日若为空则跳过
        if wd_cn == "放送日未知" and not items:
            continue
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

    lines.append("数据来源：Jikan / MyAnimeList 当季放送")
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
            f"获取番剧失败：{describe_proxy_request_error(e)}",
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
            f"获取番剧周表失败：{describe_proxy_request_error(e)}",
            md_type="娱乐",
            k1="重试",
            v1="番剧周表",
            k3="帮助",
            v3="娱乐帮助",
        )
    await week_bangumi_cmd.finish()
