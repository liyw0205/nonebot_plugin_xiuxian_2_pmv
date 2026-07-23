"""结果卡 / 状态文案公共 helper（真 MD 通道）。

约定（与 docs/message_channel.md 一致）：
- 仅用于 handle_send 真 Markdown；send_msg_handler shell 代码框不要依赖 `>`。
- emoji 语义：✅完成 ❗可做 ⏳冷却 🔄进行中 ❌失败/不足 ⬜未开 🌱成熟 👑至高
- 不改指令名；娱乐/管理帮助不走此 helper。
"""
from __future__ import annotations


def status_emoji(kind: str) -> str:
    mapping = {
        "ok": "✅",
        "done": "✅",
        "success": "✅",
        "todo": "❗",
        "available": "❗",
        "warn": "❗",
        "cd": "⏳",
        "wait": "⏳",
        "running": "🔄",
        "progress": "🔄",
        "fail": "❌",
        "error": "❌",
        "empty": "⬜",
        "off": "⬜",
        "ripe": "🌱",
        "top": "👑",
        "big": "🎉",
        "half": "⚠️",
    }
    return mapping.get(str(kind or "").strip().lower(), "")


def prefix_status(kind: str, text: str) -> str:
    """在文案前加状态 emoji（已有同类前缀则不重复）。"""
    em = status_emoji(kind)
    t = str(text or "")
    if not em:
        return t
    if t.startswith(em):
        return t
    return f"{em} {t}" if t else em


def md_title_card(title: str, body: str) -> str:
    """标准结果卡：标题 + --- + 正文。"""
    title = str(title or "").strip() or "提示"
    body = str(body or "").strip()
    if body:
        return f"**{title}**\n---\n{body}"
    return f"**{title}**\n---"


def md_kv_lines(pairs: list[tuple[str, str]], *, quote_values: bool = True) -> str:
    """标签/值行；真 MD 下值可整段 `>` 缩小。"""
    lines: list[str] = []
    for k, v in pairs:
        k = str(k or "").strip()
        v = str(v or "").strip()
        if not k:
            continue
        lines.append(k)
        if quote_values:
            if v:
                for part in v.splitlines() or [""]:
                    lines.append(f"> {part}" if part else ">")
            else:
                lines.append(">")
        else:
            lines.append(v)
    return "\n".join(lines)
