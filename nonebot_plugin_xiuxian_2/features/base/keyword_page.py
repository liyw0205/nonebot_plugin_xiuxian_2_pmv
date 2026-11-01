"""分页类视图的「页码或关键词」参数解析（纯函数，无 I/O）。

多个列表类视图都写成 `int(args)` 失败即回退第 1 页，玩家输入关键词时参数被静默吞掉。
这里提供一份通用实现，供背包、商店、图鉴等视图共用，避免每个视图各写一套。

约定：
- 纯数字才是页码；
- 其余按关键词处理，可带尾随页码（`灵芝 2`）；
- `0`/`-1` 之类不作为页码。
"""

from __future__ import annotations

from typing import Any, Iterable, Mapping

__all__ = [
    "empty_notice",
    "filter_lines",
    "filter_rows",
    "matches_keyword",
    "parse_page_arg",
]


def _is_page(token: str) -> bool:
    text = str(token or "").strip()
    return text.isdigit() and int(text) > 0


def parse_page_arg(raw: Any) -> dict[str, Any]:
    """把参数解析成 `{"keyword": str, "page": int}`。"""
    text = str(raw or "").strip()
    if not text:
        return {"keyword": "", "page": 1}
    if _is_page(text):
        return {"keyword": "", "page": int(text)}

    head, _, tail = text.rpartition(" ")
    if head.strip() and _is_page(tail):
        return {"keyword": head.strip(), "page": int(tail)}
    return {"keyword": text, "page": 1}


def _normalize(value: Any) -> str:
    return str(value or "").strip().casefold()


def matches_keyword(fields: Iterable[Any], keyword: str) -> bool:
    """任一字段包含关键词即命中（大小写不敏感）。"""
    query = _normalize(keyword)
    if not query:
        return True
    return any(query in _normalize(field) for field in fields)


def filter_rows(
    rows: Iterable[Any], keyword: str, *, fields=None
) -> list[Any]:
    """按关键词过滤行。

    `fields` 给定时对 `row[field]` 逐一匹配；否则按 `row` 自身（字符串）匹配。
    """
    query = str(keyword or "").strip()
    if not query:
        return list(rows)
    if fields is None:
        return [row for row in rows if matches_keyword([row], query)]
    kept = []
    for row in rows:
        values = []
        if isinstance(row, Mapping):
            values = [row.get(field) for field in fields]
        kept.append(row) if matches_keyword(values, query) else None
    return kept


def filter_lines(lines: Iterable[str], keyword: str) -> list[str]:
    """按关键词过滤已渲染的行。"""
    query = _normalize(keyword)
    if not query:
        return list(lines)
    return [line for line in lines if query in _normalize(line)]


def empty_notice(keyword: str, view_name: str = "背包") -> str:
    """区分「筛选无结果」与「本来就为空」。"""
    query = str(keyword or "").strip()
    name = str(view_name or "背包").strip() or "背包"
    if not query:
        return f"道友的{name}空空如也！"
    return f"{name}里没有包含「{query}」的内容，换个关键词试试。"
