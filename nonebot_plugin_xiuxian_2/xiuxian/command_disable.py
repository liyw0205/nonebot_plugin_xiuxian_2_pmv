from __future__ import annotations

from typing import Any

from ..features.admin.command_control_application import AdminCommandControlApplication

COMMAND_DISABLE_EXEMPT_MODULE = "xiuxian_admin"


def _application() -> AdminCommandControlApplication:
    return AdminCommandControlApplication()


def load_command_disable_memory() -> dict[str, dict[str, Any]]:
    return _application().read_entries()


def save_command_disable_memory() -> None:
    """兼容旧调用：写接口即时持久化，无待存内存；此处仅校验当前状态可读。"""
    _application().read_entries()


def rebuild_alias_index(alias_map: dict[str, str]) -> None:
    _application().rebuild_alias_index(alias_map)


def known_commands() -> frozenset[str]:
    return _application().known_commands()


def known_modules() -> frozenset[str]:
    return _application().known_modules()


def resolve_primary_name(name: str) -> str:
    return _application().resolve_primary_name(name)


def is_command_disabled(name: str) -> bool:
    return _application().is_command_disabled(name)


def sync_command_registry(registry: dict[str, str]) -> dict[str, dict[str, Any]]:
    """registry: 主指令名 -> 子模块名（如 xiuxian_arena）。"""
    return _application().sync_command_registry(registry)


def set_command_disabled(name: str, *, disabled: bool) -> tuple[bool, str]:
    return _application().set_command_disabled(name, disabled=disabled)


def commands_in_module(module: str) -> list[str]:
    return _application().commands_in_module(module)


def _command_list_filter_tokens(raw_filter: str) -> list[str]:
    text = (raw_filter or "").strip()
    if not text:
        return []
    normalized = text.replace("，", ",").replace("/", ",")
    return [t.strip() for t in normalized.split(",") if t.strip()]


def collect_command_list_rows(
    raw_filter: str = "",
    *,
    only_disabled: bool = False,
) -> list[tuple[str, str, str]]:
    """主指令名、子模块、状态；按子模块再按指令名排序。"""
    return _application().collect_command_list_rows(raw_filter, only_disabled=only_disabled)


def collect_command_list_groups(
    raw_filter: str = "",
    *,
    only_disabled: bool = False,
) -> list[dict[str, Any]]:
    """按子模块分组，供 Web 指令管理页折叠展示。"""
    rows = collect_command_list_rows(raw_filter, only_disabled=only_disabled)
    groups: list[dict[str, Any]] = []
    current_mod: str | None = None
    bucket: list[dict[str, Any]] = []

    def flush(mod_key: str) -> None:
        nonlocal bucket
        if not bucket:
            return
        disabled_n = sum(1 for c in bucket if c["disabled"])
        groups.append(
            {
                "module": mod_key,
                "label": mod_key or "（未归类）",
                "commands": bucket,
                "total": len(bucket),
                "disabled_count": disabled_n,
            }
        )
        bucket = []

    for name, mod, status in rows:
        mod_key = mod or ""
        if mod_key != current_mod:
            flush(current_mod if current_mod is not None else "")
            current_mod = mod_key
        bucket.append(
            {
                "name": name,
                "disabled": status == "禁用",
            }
        )
    if current_mod is not None or bucket:
        flush(current_mod if current_mod is not None else "")
    return groups


def format_command_list_page(
    raw_filter: str = "",
    *,
    only_disabled: bool = False,
    page: int = 1,
    per_page: int = 30,
) -> tuple[str, int, int]:
    """分页列出登记指令；only_disabled 时仅显示已禁用。"""
    rows = collect_command_list_rows(raw_filter, only_disabled=only_disabled)
    tokens = _command_list_filter_tokens(raw_filter)

    if not rows:
        if tokens:
            hint = f"无匹配指令（筛选：{', '.join(tokens)}）"
        elif only_disabled:
            hint = "当前无已禁用指令"
        else:
            hint = "指令表为空，请重载插件或等待索引同步"
        return hint, 1, 1

    per_page = max(per_page, 1)
    total_pages = max((len(rows) + per_page - 1) // per_page, 1)
    page = min(max(page, 1), total_pages)
    start = (page - 1) * per_page
    page_rows = rows[start : start + per_page]

    disabled_n = sum(1 for _, _, s in rows if s == "禁用")
    title_parts = ["【指令列表】"]
    if only_disabled:
        title_parts.append("仅禁用")
    if tokens:
        title_parts.append(f"筛选：{', '.join(tokens)}")
    title_parts.append(f"共 {len(rows)} 条")
    if not only_disabled:
        title_parts.append(f"禁用 {disabled_n}")
    title = " ".join(title_parts)
    title += f"（第 {page}/{total_pages} 页）"

    lines: list[str] = [title, ""]
    current_mod = None
    for name, mod, status in page_rows:
        mod_label = mod or "（未归类）"
        if mod_label != current_mod:
            if current_mod is not None:
                lines.append("")
            lines.append(f"◆ {mod_label}")
            current_mod = mod_label
        lines.append(f"· {name} — {status}")

    lines.append("")
    lines.append("发送「指令列表 页码」翻页；「指令列表 禁用」或「指令列表 禁用 页码」只看禁用。")
    if tokens:
        lines.append("筛选示例：指令列表 存档、指令列表 xiuxian_arena")
    return "\n".join(lines), page, total_pages


def format_command_list(raw_filter: str = "", *, max_lines: int = 80) -> str:
    msg, _, _ = format_command_list_page(
        raw_filter, only_disabled=False, page=1, per_page=max_lines
    )
    return msg


def apply_disable_targets(
    raw: str,
    *,
    disabled: bool,
) -> tuple[list[str], list[str]]:
    """解析 指令禁用/解禁 参数：逗号分隔的指令名或子模块名。"""
    return _application().apply_disable_targets(raw, disabled=disabled)


def disabled_command_keys_for_route(
    command: tuple[str, ...] | None,
    text: str,
) -> set[str]:
    keys: set[str] = set()
    if command:
        if len(command) == 1:
            keys.add(command[0])
        keys.add(" ".join(command))
    plain = (text or "").strip()
    if plain:
        keys.add(plain)
    primaries = {resolve_primary_name(k) for k in keys if k}
    return {p for p in primaries if p}
