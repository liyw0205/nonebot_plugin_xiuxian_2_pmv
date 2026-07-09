from __future__ import annotations

from typing import Any

try:
    import ujson as json
except ImportError:
    import json

from nonebot.log import logger
from ..paths import get_paths

XIUXIAN_DATABASE = get_paths().data

COMMAND_DISABLE_FILE = XIUXIAN_DATABASE / "command_disable.json"

COMMAND_DISABLE_EXEMPT_MODULE = "xiuxian_admin"
_COMMAND_ENTRIES: dict[str, dict[str, Any]] = {}
# 触发词（含别名）-> 主指令名
_ALIAS_TO_PRIMARY: dict[str, str] = {}


def _ensure_parent() -> None:
    COMMAND_DISABLE_FILE.parent.mkdir(parents=True, exist_ok=True)


def _normalize_entry(value: Any, *, module: str = "") -> dict[str, Any]:
    if isinstance(value, dict):
        return {
            "disabled": bool(value.get("disabled", False)),
            "module": str(value.get("module") or module or ""),
        }
    return {"disabled": bool(value), "module": module or ""}


def load_command_disable_memory() -> dict[str, dict[str, Any]]:
    global _COMMAND_ENTRIES, _ALIAS_TO_PRIMARY
    if not COMMAND_DISABLE_FILE.exists():
        _COMMAND_ENTRIES = {}
        return _COMMAND_ENTRIES
    try:
        with open(COMMAND_DISABLE_FILE, "r", encoding="utf-8") as fp:
            raw = json.loads(fp.read())
    except Exception as e:
        logger.warning("[修仙 指令禁用] 读取 {} 失败：{}", COMMAND_DISABLE_FILE, e)
        return _COMMAND_ENTRIES
    if not isinstance(raw, dict):
        _COMMAND_ENTRIES = {}
        return _COMMAND_ENTRIES

    commands_raw = raw.get("commands", raw)
    if not isinstance(commands_raw, dict):
        _COMMAND_ENTRIES = {}
        return _COMMAND_ENTRIES

    entries: dict[str, dict[str, Any]] = {}
    for key, value in commands_raw.items():
        if not isinstance(key, str) or not key.strip():
            continue
        entries[key.strip()] = _normalize_entry(value)
    _COMMAND_ENTRIES = entries
    return _COMMAND_ENTRIES


def save_command_disable_memory() -> None:
    _ensure_parent()
    payload = {
        "commands": {
            name: {
                "disabled": bool(info.get("disabled", False)),
                "module": str(info.get("module") or ""),
            }
            for name, info in sorted(_COMMAND_ENTRIES.items())
        }
    }
    with open(COMMAND_DISABLE_FILE, "w", encoding="utf-8") as fp:
        fp.write(json.dumps(payload, ensure_ascii=False, indent=2))


def rebuild_alias_index(alias_map: dict[str, str]) -> None:
    global _ALIAS_TO_PRIMARY
    cleaned: dict[str, str] = {}
    for trigger, primary in alias_map.items():
        t = (trigger or "").strip()
        p = (primary or "").strip()
        if t and p:
            cleaned[t] = p
    _ALIAS_TO_PRIMARY = cleaned


def known_commands() -> frozenset[str]:
    return frozenset(_COMMAND_ENTRIES.keys())


def known_modules() -> frozenset[str]:
    mods = {
        str(info.get("module") or "").strip()
        for info in _COMMAND_ENTRIES.values()
        if str(info.get("module") or "").strip()
    }
    return frozenset(mods)


def resolve_primary_name(name: str) -> str:
    key = (name or "").strip()
    if not key:
        return ""
    return _ALIAS_TO_PRIMARY.get(key, key)


def is_command_disabled(name: str) -> bool:
    primary = resolve_primary_name(name)
    if not primary:
        return False
    entry = _COMMAND_ENTRIES.get(primary)
    if not entry:
        return False
    return bool(entry.get("disabled", False))


def sync_command_registry(registry: dict[str, str]) -> dict[str, dict[str, Any]]:
    """registry: 主指令名 -> 子模块名（如 xiuxian_arena）。"""
    global _COMMAND_ENTRIES
    if not _COMMAND_ENTRIES:
        load_command_disable_memory()

    previous = {
        name: {
            "disabled": bool(info.get("disabled", False)),
            "module": str(info.get("module") or ""),
        }
        for name, info in _COMMAND_ENTRIES.items()
    }

    merged: dict[str, dict[str, Any]] = {}
    for name in sorted(registry.keys()):
        mod = (registry.get(name) or "").strip()
        old = previous.get(name)
        if old is not None:
            merged[name] = {
                "disabled": bool(old.get("disabled", False)),
                "module": mod or str(old.get("module") or ""),
            }
        else:
            merged[name] = {"disabled": False, "module": mod}

    removed = set(previous.keys()) - set(merged.keys())
    added = set(merged.keys()) - set(previous.keys())
    _COMMAND_ENTRIES = merged
    save_command_disable_memory()

    if added or removed:
        logger.info(
            "[修仙 指令禁用] 已同步指令表 {} 条（新增 {}，移除 {}）",
            len(merged),
            len(added),
            len(removed),
        )
    return merged


def set_command_disabled(name: str, *, disabled: bool) -> tuple[bool, str]:
    primary = resolve_primary_name(name)
    if not primary:
        return False, "请指定指令名"
    if primary not in _COMMAND_ENTRIES:
        return False, f"未登记指令：{primary}"
    _COMMAND_ENTRIES[primary]["disabled"] = disabled
    save_command_disable_memory()
    return True, ""


def commands_in_module(module: str) -> list[str]:
    mod = (module or "").strip()
    if not mod or mod == COMMAND_DISABLE_EXEMPT_MODULE:
        return []
    return sorted(
        name
        for name, info in _COMMAND_ENTRIES.items()
        if str(info.get("module") or "") == mod
    )


def _command_list_filter_tokens(raw_filter: str) -> list[str]:
    text = (raw_filter or "").strip()
    if not text:
        return []
    normalized = text.replace("，", ",").replace("/", ",")
    return [t.strip() for t in normalized.split(",") if t.strip()]


def _command_list_match(name: str, mod: str, tokens: list[str]) -> bool:
    if not tokens:
        return True
    for token in tokens:
        if token == mod:
            return True
        if token in name:
            return True
        if token in mod:
            return True
    return False


def collect_command_list_rows(
    raw_filter: str = "",
    *,
    only_disabled: bool = False,
) -> list[tuple[str, str, str]]:
    """主指令名、子模块、状态；按子模块再按指令名排序。"""
    if not _COMMAND_ENTRIES:
        load_command_disable_memory()

    tokens = _command_list_filter_tokens(raw_filter)
    rows: list[tuple[str, str, str]] = []
    for name, info in _COMMAND_ENTRIES.items():
        mod = str(info.get("module") or "")
        if mod == COMMAND_DISABLE_EXEMPT_MODULE:
            continue
        disabled = bool(info.get("disabled", False))
        if only_disabled and not disabled:
            continue
        if not _command_list_match(name, mod, tokens):
            continue
        status = "禁用" if disabled else "启用"
        rows.append((name, mod, status))

    rows.sort(key=lambda r: ((r[1] or "\uffff"), r[0]))
    return rows


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
    text = (raw or "").strip()
    if not text:
        return [], ["请指定指令名或子模块，多个用英文逗号分隔"]

    tokens = [t.strip() for t in text.replace("，", ",").split(",") if t.strip()]
    if not tokens:
        return [], ["请指定指令名或子模块"]

    if not _COMMAND_ENTRIES:
        load_command_disable_memory()

    changed: list[str] = []
    errors: list[str] = []
    seen: set[str] = set()

    for token in tokens:
        if token == COMMAND_DISABLE_EXEMPT_MODULE:
            errors.append("xiuxian_admin 不参与指令禁用")
            continue

        if token in _COMMAND_ENTRIES:
            if str(_COMMAND_ENTRIES[token].get("module") or "") == COMMAND_DISABLE_EXEMPT_MODULE:
                errors.append(f"管理员指令不可禁用：{token}")
                continue
            if token not in seen:
                _COMMAND_ENTRIES[token]["disabled"] = disabled
                changed.append(token)
                seen.add(token)
            continue

        mod_cmds = commands_in_module(token)
        if mod_cmds:
            for name in mod_cmds:
                if name in seen:
                    continue
                _COMMAND_ENTRIES[name]["disabled"] = disabled
                changed.append(name)
                seen.add(name)
            continue

        resolved = resolve_primary_name(token)
        if resolved in _COMMAND_ENTRIES and resolved not in seen:
            _COMMAND_ENTRIES[resolved]["disabled"] = disabled
            changed.append(resolved)
            seen.add(resolved)
            continue

        if token.startswith("xiuxian_"):
            errors.append(f"子模块 {token} 下无已登记指令")
        else:
            errors.append(f"未登记：{token}")

    if changed:
        save_command_disable_memory()
    return changed, errors


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
