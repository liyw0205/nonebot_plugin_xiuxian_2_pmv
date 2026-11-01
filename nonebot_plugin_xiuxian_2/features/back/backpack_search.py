"""背包参数解析与关键词筛选（纯函数，无 I/O）。

`我的背包` 原本只做 `int(args)`，解析失败就静默当成第 1 页：玩家输入
`我的背包 破境丹` 时看到的是背包首页，而不是破境丹——参数被无声吞掉。
这里把参数明确解析成「页码」或「关键词（可带页码）」两种意图，并给出
按名称/类型筛选的纯函数，供 markdown 与非 markdown 两条渲染路径共用。

不做任何数据库访问，也不改变排序规则：筛选后仍沿用原有排序。
"""

from __future__ import annotations

from typing import Any, Iterable, Mapping

BACKPACK_ARG_HINT = "我的背包 [页码|关键词]"


def _is_page(token: str) -> bool:
    text = str(token or "").strip()
    return text.isdigit() and int(text) > 0


def parse_backpack_arg(raw: Any) -> dict[str, Any]:
    """把参数解析成 `{"keyword": str, "page": int}`。

    - 空 → 第 1 页、无关键词；
    - 纯数字 → 该页、无关键词；
    - `关键词` → 第 1 页 + 关键词；
    - `关键词 页码` → 该页 + 关键词（页码非法则整串当关键词）。
    """
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


def matches_keyword(row: Mapping[str, Any], keyword: str) -> bool:
    """名称或类型包含关键词即命中（大小写不敏感）。"""
    query = _normalize(keyword)
    if not query:
        return True
    return query in _normalize(row.get("name")) or query in _normalize(row.get("goods_type"))


def filter_sections(
    sections: Iterable[tuple[str, list[dict[str, Any]]]], keyword: str
) -> list[tuple[str, list[dict[str, Any]]]]:
    """按关键词过滤 `(段名, 行)` 结构；空段整段丢弃，保持原顺序。"""
    query = str(keyword or "").strip()
    if not query:
        return [(title, list(rows)) for title, rows in sections]

    filtered: list[tuple[str, list[dict[str, Any]]]] = []
    for title, rows in sections:
        kept = [row for row in rows if matches_keyword(row, query)]
        if kept:
            filtered.append((title, kept))
    return filtered


def filter_lines(lines: Iterable[str], keyword: str) -> list[str]:
    """按关键词过滤已渲染的行（非 markdown 路径用）。"""
    query = _normalize(keyword)
    if not query:
        return list(lines)
    return [line for line in lines if query in _normalize(line)]


def empty_notice(keyword: str, bag_name: str = "背包") -> str:
    """没有命中时的提示：明确说明是筛选无结果，而不是背包为空。"""
    query = str(keyword or "").strip()
    name = str(bag_name or "背包").strip() or "背包"
    if not query:
        return f"道友的{name}空空如也！"
    return f"{name}里没有名称或类型包含「{query}」的物品，换个关键词试试。"


__all__ = [
    "BACKPACK_ARG_HINT",
    "empty_notice",
    "filter_lines",
    "filter_sections",
    "matches_keyword",
    "parse_backpack_arg",
]
