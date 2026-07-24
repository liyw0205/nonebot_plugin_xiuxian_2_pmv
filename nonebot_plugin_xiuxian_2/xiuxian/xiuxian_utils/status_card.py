"""结果卡 / 状态文案 / 情境导航（真 MD 通道）。

约定（与 docs/message_channel.md 一致）：
- 仅用于 handle_send 真 Markdown；send_msg_handler shell 代码框不要依赖 `>`。
- emoji 语义：✅完成 ❗可做 ⏳冷却 🔄进行中 ❌失败/不足 ⬜未开 🌱成熟 👑至高
- 不改指令名；娱乐/管理帮助不走此 helper。
"""
from __future__ import annotations

from typing import Any, Iterable, Sequence


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


def result_card(
    title: str,
    *,
    kind: str = "ok",
    summary: str = "",
    pairs: Sequence[tuple[str, str]] | None = None,
    extra: str = "",
) -> str:
    """统一结果卡：标题 + 状态行 + 可选标签值。"""
    body_parts: list[str] = []
    line = prefix_status(kind, summary) if summary else status_emoji(kind)
    if line:
        body_parts.append(line)
    if pairs:
        kv = md_kv_lines(list(pairs))
        if kv:
            body_parts.append(kv)
    if extra:
        body_parts.append(str(extra).strip())
    return md_title_card(title, "\n".join(body_parts))


def cooldown_msg(scope: str, remain: str, *, tip: str = "") -> str:
    """冷却提示卡。"""
    body = prefix_status("cd", f"冷却中，还需{remain}")
    if tip:
        body = f"{body}\n{tip}"
    return md_title_card(scope or "提示", body)


# 玩法默认导航：按钮短名 -> 完整指令（不改指令名）
NAV_PRESETS: dict[str, tuple[tuple[str, str], ...]] = {
    "daily": (("签到", "修仙签到"), ("背包", "我的背包"), ("帮助", "修仙帮助")),
    "status": (("日常", "日常"), ("修为", "我的修为"), ("背包", "我的背包"), ("帮助", "修仙帮助")),
    "work": (("查看", "悬赏令查看"), ("刷新", "悬赏令确认刷新"), ("帮助", "悬赏令帮助")),
    "work_settle": (("刷新", "悬赏令刷新"), ("数据", "统计数据"), ("帮助", "悬赏令帮助")),
    "sign": (("日常", "日常"), ("鸿运", "鸿运"), ("帮助", "修仙帮助")),
    "bank": (("存灵石", "灵庄存灵石"), ("取灵石", "灵庄取灵石"), ("信息", "灵庄信息")),
    "breakthrough": (("直接突破", "直接突破"), ("渡厄", "渡厄突破"), ("修为", "我的修为")),
    "sect": (("任务", "宗门任务接取"), ("信息", "我的宗门"), ("日常", "日常")),
    "sect_task": (("完成", "宗门任务完成"), ("刷新", "宗门任务刷新"), ("信息", "我的宗门")),
    "mix": (("领取", "炼丹领取"), ("炼丹", "炼丹"), ("背包", "我的背包")),
    "mix_done": (("继续", "炼丹"), ("背包", "我的背包"), ("日常", "日常")),
    "dual": (("状态", "我的状态"), ("日常", "日常"), ("帮助", "修仙帮助")),
    "novice": (("签到", "修仙签到"), ("日常", "日常"), ("帮助", "修仙帮助")),
    "help": (("日常", "日常"), ("状态", "我的状态"), ("帮助", "修仙帮助")),
}


def nav_kwargs(
    preset: str | None = None,
    *,
    md_type: str = "修仙",
    buttons: Sequence[tuple[str, str]] | None = None,
    extra: Sequence[tuple[str, str]] | None = None,
    max_buttons: int = 4,
) -> dict[str, Any]:
    """生成 handle_send 的 md_type/k1/v1… 参数。"""
    pairs: list[tuple[str, str]] = []
    if buttons:
        pairs.extend((str(a), str(b)) for a, b in buttons if a and b)
    elif preset and preset in NAV_PRESETS:
        pairs.extend(NAV_PRESETS[preset])
    if extra:
        pairs.extend((str(a), str(b)) for a, b in extra if a and b)
    # 去重保序
    seen: set[str] = set()
    uniq: list[tuple[str, str]] = []
    for label, cmd in pairs:
        key = f"{label}\0{cmd}"
        if key in seen:
            continue
        seen.add(key)
        uniq.append((label, cmd))
        if len(uniq) >= max_buttons:
            break
    out: dict[str, Any] = {"md_type": md_type}
    for i, (label, cmd) in enumerate(uniq, start=1):
        out[f"k{i}"] = label
        out[f"v{i}"] = cmd
    return out


def daily_action_buttons(
    flags: dict[str, bool],
    *,
    max_actions: int = 3,
) -> list[tuple[str, str]]:
    """根据日常可做项生成主按钮（最多 max_actions），末位补枢纽。

    flags 键（True=可做/可点）：
      sign, work, field, sect_task, sect_elixir, rift, training, dual
    """
    priority: list[tuple[str, str, str]] = [
        ("sign", "签到", "修仙签到"),
        ("work", "悬赏", "悬赏令查看"),
        ("field", "灵田", "灵田收取"),
        ("sect_task", "宗门", "宗门任务接取"),
        ("sect_elixir", "丹药", "宗门丹药领取"),
        ("rift", "秘境", "秘境查看"),
        ("training", "历练", "历练"),
        ("dual", "双修", "双修"),
    ]
    actions: list[tuple[str, str]] = []
    for key, label, cmd in priority:
        if flags.get(key):
            actions.append((label, cmd))
        if len(actions) >= max_actions:
            break
    # 枢纽兜底
    if len(actions) < max_actions:
        actions.append(("背包", "我的背包"))
    if len(actions) < max_actions + 1:
        actions.append(("状态", "我的状态"))
    # 最后一格固定帮助感：若还有空位
    if len(actions) < 4:
        actions.append(("帮助", "修仙帮助"))
    return actions[:4]


def is_can_do_line(msg: str) -> bool:
    """日常行是否「可点」（❗/🌱，排除 ❌/⏳/✅/⬜/🔄）。"""
    t = str(msg or "")
    if "❗" in t or "🌱" in t:
        # 贡献不足等带 ❌ 优先不可点
        if "❌" in t:
            return False
        return True
    return False


def media_fail_hint(error: str | None, *, platform: str = "") -> str:
    """媒体解析失败：分类短文案（不塞修仙按钮）。"""
    err = str(error or "").strip()
    low = err.lower()
    plat = str(platform or "").strip() or "该平台"
    if not err:
        return f"❌ {plat}解析失败，请稍后再试或换一条链接。"
    if any(x in low for x in ("network", "unreachable", "timed out", "timeout", "连接", "超时", "不可达")):
        return f"❌ 网络不可达（{plat}）。国内站请直连；海外站需代理。"
    if any(x in low for x in ("风控", "登录", "cookie", "403", "401", "verify", "captcha")):
        return f"❌ {plat}风控或需登录，暂时解不出媒体。"
    if any(x in low for x in ("无", "empty", "未解析", "not found", "no media")):
        return f"❌ 未解析到可发送媒体：{err}"
    return f"❌ 解析失败：{err}"
