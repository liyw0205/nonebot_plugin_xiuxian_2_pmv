"""修仙帮助的关键词检索（只读）。

`修仙帮助` 现在是一整页静态文本，而仓库已登记 650+ 条命令/别名：玩家想找某个功能，
只能在长文本里逐行翻。本模块把已登记的 FeatureManifest 命令表做成可检索索引，
让 `修仙帮助 签到` 这类输入直接命中命令名或别名。

索引来源刻意与运行期注册保持一致：扫描 `features/*/manifest.py` 与
`bootstrap/platform_manifest.py` 的 `FEATURE`，并有用例把索引与
`plugin.build_registry()` 的登记结果逐条比对，防止两边漂移。

纯内存、只读：不读库、不写文件、不改任何命令行为。
"""

from __future__ import annotations

import importlib
import pkgutil
from typing import Any, Iterable, Mapping

MAX_MATCHES = 10

# 匹配强度：完全相等 > 前缀 > 包含
_RANK_EXACT = 0
_RANK_PREFIX = 1
_RANK_CONTAINS = 2
_RANK_LABEL = {_RANK_EXACT: "命令", _RANK_PREFIX: "前缀", _RANK_CONTAINS: "包含"}


def _normalize(text: Any) -> str:
    return str(text or "").strip().casefold()


def iter_feature_manifests(package: Any) -> list[Any]:
    """收集所有 FeatureManifest（features/*/manifest.py + platform_manifest.py）。"""
    manifests: list[Any] = []
    seen: set[str] = set()

    def _load(module_name: str) -> None:
        try:
            module = importlib.import_module(module_name)
        except Exception:
            return
        feature = getattr(module, "FEATURE", None)
        key = getattr(feature, "key", None)
        if feature is None or key in seen:
            return
        seen.add(key)
        manifests.append(feature)

    _load(f"{package.__name__}.bootstrap.platform_manifest")
    features_pkg = importlib.import_module(f"{package.__name__}.features")
    for info in pkgutil.iter_modules(features_pkg.__path__):
        if not info.ispkg:
            continue
        _load(f"{features_pkg.__name__}.{info.name}.manifest")
    return manifests


def build_entries(manifests: Iterable[Any]) -> list[dict[str, Any]]:
    """把 manifest 命令表摊平成可检索条目（命令名 + 别名）。"""
    entries: list[dict[str, Any]] = []
    for feature in manifests:
        title = str(getattr(feature, "title", "") or "")
        for command in getattr(feature, "commands", ()) or ():
            name = str(getattr(command, "name", "") or "").strip()
            if not name:
                continue
            entries.append(
                {
                    "command": name,
                    "aliases": tuple(str(alias) for alias in getattr(command, "aliases", ()) or ()),
                    "feature": title,
                    "permission": str(getattr(command, "permission", "") or ""),
                }
            )
    return entries


def search(entries: Iterable[Mapping[str, Any]], keyword: Any, *, limit: int = MAX_MATCHES) -> dict[str, Any]:
    """按命令名/别名检索。返回 `{"keyword", "matches", "total"}`。"""
    query = _normalize(keyword)
    payload: dict[str, Any] = {"keyword": str(keyword or "").strip(), "matches": [], "total": 0}
    if not query:
        return payload

    scored: list[tuple[int, int, dict[str, Any]]] = []
    for entry in entries:
        names = [_normalize(entry.get("command"))]
        names.extend(_normalize(alias) for alias in entry.get("aliases") or ())
        best: int | None = None
        matched_name = ""
        for name in names:
            if not name:
                continue
            if name == query:
                rank = _RANK_EXACT
            elif name.startswith(query):
                rank = _RANK_PREFIX
            elif query in name:
                rank = _RANK_CONTAINS
            else:
                continue
            if best is None or rank < best:
                best, matched_name = rank, name
        if best is None:
            continue
        scored.append(
            (
                best,
                len(_normalize(entry.get("command"))),
                {
                    "command": entry.get("command"),
                    "aliases": list(entry.get("aliases") or ()),
                    "feature": entry.get("feature") or "",
                    "matched_by": _RANK_LABEL[best],
                    "matched_name": matched_name,
                },
            )
        )

    scored.sort(key=lambda item: (item[0], item[1], str(item[2]["command"])))
    payload["total"] = len(scored)
    payload["matches"] = [item[2] for item in scored[: max(1, int(limit))]]
    return payload


def format_help_search(payload: Mapping[str, Any]) -> str:
    """渲染检索结果。纯函数：无 I/O、不依赖 NoneBot。"""
    keyword = str(payload.get("keyword") or "").strip()
    matches = list(payload.get("matches") or [])
    if not keyword:
        return "请给出要查找的关键词，例如：修仙帮助 签到"

    if not matches:
        return (
            f"没有找到与「{keyword}」相关的命令。\n"
            "可以换一个更短的关键词，或直接发送 `修仙帮助` 查看完整菜单。"
        )

    total = int(payload.get("total") or len(matches))
    lines = [f"【帮助检索】{keyword}（命中 {total} 条）"]
    for item in matches:
        line = f"- {item.get('command')}"
        aliases = list(item.get("aliases") or ())
        if aliases:
            line += f"（别名：{'、'.join(aliases)}）"
        if item.get("feature"):
            line += f" · {item['feature']}"
        lines.append(line)
    if total > len(matches):
        lines.append("")
        lines.append(f"仅显示前 {len(matches)} 条，请用更具体的关键词缩小范围。")
    return "\n".join(lines)


__all__ = [
    "MAX_MATCHES",
    "build_entries",
    "format_help_search",
    "iter_feature_manifests",
    "search",
]
