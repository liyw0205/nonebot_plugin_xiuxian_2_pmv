import ast
import re
from functools import lru_cache
from pathlib import Path
from typing import Any
from urllib.parse import quote

from nonebot.log import logger


def generate_command(msg, status=None, command=None, msg2=None):
    """
    根据状态生成不同的命令字符串。

    :param status: 字符串，表示状态，可以是 'start', 'end' 或 None
    :param msg: 字符串，表示消息内容
    :param command: 字符串，表示命令
    :param msg2: 字符串，表示附加消息内容
    :return: 生成的命令字符串
    """
    if status == 'start':
        return f"{msg}](mqqapi://aio/inlinecmd?command={command}&enter=false&reply=false){msg2}["
    elif status == 'end':
        return f"{msg}](mqqapi://aio/inlinecmd?command={command}&enter=false&reply=false){msg2}"
    else:
        return f"[{msg}"


def build_md_command_link(text, command=None):
    """生成 QQ 原生 Markdown 蓝字命令链接。"""
    display = str(text) if text is not None else " "
    display = display.replace("[", "").replace("]", "")
    display = display.replace("\r", " ").replace("\n", " ")
    command = str(command if command is not None else text)
    command = quote(command, safe="")
    return f"[{display}](mqqapi://aio/inlinecmd?command={command}&enter=false&reply=false)"


def escape_markdown_text(value: Any) -> str:
    """转义原生 Markdown 文本字段，避免用户内容破坏展示结构。"""
    return (
        str(value)
        .replace("\\", "\\\\")
        .replace("*", "\\*")
        .replace("_", "\\_")
        .replace("`", "\\`")
        .replace("[", "\\[")
        .replace("]", "\\]")
        .replace("(", "\\(")
        .replace(")", "\\)")
        .replace("#", "\\#")
    )


def strip_md_command_links(msg: str) -> str:
    """原生 Markdown 不可用时，将常见展示语法降级为纯文本。"""
    text = str(msg or " ")
    text = text.replace("\r", "\n")
    text = re.sub(r"!\[([^\]]*)\]\([^)]+\)", r"\1", text)
    text = re.sub(r"\[([^\]]+)\]\(mqqapi://aio/inlinecmd\?[^)]+\)", r"\1", text)
    text = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", text)

    lines = []
    in_code = False
    for raw_line in text.splitlines():
        stripped = raw_line.strip()
        if stripped.startswith("```"):
            in_code = not in_code
            continue

        line = raw_line.rstrip()
        if not in_code:
            if stripped == "---":
                line = "----------"
            line = re.sub(r"^\s{0,3}#{1,6}\s+", "", line)
            line = re.sub(r"^\s{0,3}>\s?", "  ", line)
            line = re.sub(r"\*\*([^*\n]+)\*\*", r"\1", line)
            line = re.sub(r"__([^_\n]+)__", r"\1", line)
            line = re.sub(r"`([^`\n]+)`", r"\1", line)
        lines.append(line)

    return "\n".join(lines).strip() or " "


_BRACKET_LINE_RE = re.compile(
    r"^(?P<indent>[ \t]*)(?P<title>【[^】\r\n]+】)(?P<rest>\s*\S.*)$"
)
# 帮助常见：悬赏令查看 - 浏览... / • 我的宗门 - 查看...
_HELP_CMD_DESC_RE = re.compile(
    r"^(?P<indent>[ \t]*)"
    r"(?P<bullet>(?:[-*•→·]|\d+[.、)]|[一二三四五六七八九十]+[.、])\s+)?"
    r"(?P<label>[^：:\-\r\n【】]{1,40}?)"
    r"(?P<sep>\s*[-—–]\s+|：|:)\s*"
    r"(?P<desc>\S.*)$"
)
# 句中：请发送【直接突破】来突破 / 输入【我的修仙信息】查看
_INLINE_SEND_CMD_RE = re.compile(
    r"(?P<prefix>请?发送|请?输入|请使用|可发送|可输入)"
    r"(?P<title>【[^】\r\n]{1,24}】)"
    r"(?P<rest>[^【\r\n]{0,40})$"
)


def _md_joiner(text: str) -> str:
    if "\r" in text:
        return "\r"
    return "\n"


def enhance_markdown_display(msg: str) -> str:
    """Markdown 展示优化（按真实帮助/欢迎文案覆盖）：

    1) 【标题】说明  →  【标题】 + > 说明
    2) 命令 - 说明 / 命令：说明（含项目符号）→ 命令 + > 说明
    3) 请发送【命令】后续说明 → 前缀 + 【命令】 + > 后续

    关闭 MD 时由 strip_md_command_links 清理 `>`。
    """
    text = str(msg or "")
    if not text:
        return text

    normalized = text.replace("\r\n", "\n").replace("\r", "\n")
    out: list[str] = []
    in_code = False
    for raw in normalized.split("\n"):
        stripped = raw.strip()
        if stripped.startswith("```"):
            in_code = not in_code
            out.append(raw)
            continue
        if in_code or not stripped:
            out.append(raw)
            continue
        # 已是引用/分隔/纯标题行，保持
        if stripped.startswith(">") or stripped in {"---", "***", "---"}:
            out.append(raw)
            continue

        # 1) 行首【标题】后有说明
        m = _BRACKET_LINE_RE.match(raw)
        if m:
            indent = m.group("indent") or ""
            title = m.group("title")
            rest = (m.group("rest") or "").strip()
            if rest and not rest.startswith(">") and "mqqapi://aio/inlinecmd" not in rest and "](" not in rest:
                out.append(f"{indent}{title}")
                out.append(f"{indent}> {rest}")
                continue
            out.append(raw)
            continue

        # 2) 帮助条目：命令 - 说明 / 命令：说明
        # 排除纯规则数值（如 初始积分：1000分、胜利：+20积分）里“说明”太短且像属性
        m = _HELP_CMD_DESC_RE.match(raw)
        if m:
            label = (m.group("label") or "").strip()
            desc = (m.group("desc") or "").strip()
            bullet = m.group("bullet") or ""
            indent = m.group("indent") or ""
            # 跳过：链接、已是引用、标签过短、描述过短
            if (
                label
                and desc
                and not desc.startswith(">")
                and "mqqapi://aio/inlinecmd" not in raw
                and "](" not in raw
                and len(label) >= 2
                and len(desc) >= 2
            ):
                # 数值/比例类短描述不拆（积分、次数、百分比、纯数字）
                if not re.fullmatch(r"[+\-−]?\d+.*|.*[%％分次级层].*|无|有|是|否", desc):
                    # 标签像命令/功能名时才拆（含中文且不是纯标点）
                    if re.search(r"[\u4e00-\u9fffA-Za-z]", label):
                        out.append(f"{indent}{bullet}{label}".rstrip())
                        out.append(f"{indent}> {desc}")
                        continue

        # 3) 句中 请发送/输入/使用【命令】后续说明
        m = _INLINE_SEND_CMD_RE.search(raw)
        if m and "mqqapi://aio/inlinecmd" not in raw and "](" not in raw:
            prefix = m.group("prefix")
            title = m.group("title")
            rest = (m.group("rest") or "").strip(" ，,。！!；;：:")
            # 只处理 rest 还有有效说明的
            if rest and not rest.startswith(">"):
                # 保留行首缩进与前缀前文本
                start = m.start()
                head = raw[:start].rstrip()
                indent = re.match(r"^[ \t]*", raw).group(0) if re.match(r"^[ \t]*", raw) else ""
                lines = []
                if head:
                    lines.append(head)
                lines.append(f"{indent}{prefix}{title}")
                lines.append(f"{indent}> {rest}")
                out.extend(lines)
                continue

        out.append(raw)

    return _md_joiner(text).join(out)


# 兼容旧名
enhance_markdown_bracket_lines = enhance_markdown_display


def warmup_help_command_cache() -> int:
    """启动预热：扫描命令表，避免首次帮助极慢。"""
    commands = _get_known_help_commands()
    return len(commands)


_EXTRA_HELP_COMMANDS = {
    "灵庄帮助", "灵庄存灵石", "灵庄取灵石", "灵庄升级会员", "灵庄信息", "灵庄结算",
    "悬赏令帮助", "悬赏令查看", "悬赏令刷新", "悬赏令确认刷新", "悬赏令接取",
    "悬赏令结算", "悬赏令确认结算", "悬赏令终止", "悬赏令重置",
}


def _const_str(node) -> str | None:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value.strip()
    return None


def _collect_str_constants(node) -> set[str]:
    values = set()
    if isinstance(node, (ast.Set, ast.List, ast.Tuple)):
        for item in node.elts:
            value = _const_str(item)
            if value:
                values.add(value)
    else:
        value = _const_str(node)
        if value:
            values.add(value)
    return values


@lru_cache(maxsize=1)
def _get_known_help_commands() -> frozenset[str]:
    """收集项目里真实注册过或明确作为按钮发送的命令。"""
    commands = set(_EXTRA_HELP_COMMANDS)
    root = Path(__file__).resolve().parents[1]
    for py_file in root.rglob("*.py"):
        if "__pycache__" in py_file.parts:
            continue
        try:
            tree = ast.parse(py_file.read_text(encoding="utf-8"), filename=str(py_file))
        except Exception as e:
            logger.warning(f"收集帮助命令失败: {py_file}: {e}")
            continue

        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue

            func_name = getattr(node.func, "id", None)
            if isinstance(node.func, ast.Attribute):
                func_name = node.func.attr

            if func_name == "on_command" and node.args:
                commands.update(_collect_str_constants(node.args[0]))
                for kw in node.keywords:
                    if kw.arg == "aliases":
                        commands.update(_collect_str_constants(kw.value))

    return frozenset(cmd.strip() for cmd in commands if cmd and cmd.strip() and cmd.strip() != " ")


def _looks_like_help_command(text: str) -> bool:
    text = str(text or "").strip()
    if not text:
        return False
    if any(ch in text for ch in "。！？；;"):
        return False
    heading_text = text.strip("：:")
    if heading_text in {
        "基础功能", "成长功能", "分解功能", "基础指令", "用户命令", "管理员命令",
        "管理操作", "道纹详情", "战斗机制", "查看信息", "核心功能", "使用示例",
        "规则说明", "温馨提示", "系统规则", "资源管理", "境界管理", "系统管理",
        "交易管理", "功能管理", "广播管理", "其他信息",
    }:
        return False
    if any(word in text for word in ("指令", "说明", "小贴士", "注意", "规则", "系统", "列表：", "如下", "示例", "例如", "格式", "可用", "支持", "安全")):
        return False
    cleaned = re.sub(r"[\s\[\]【】()（）<>《》]", "", text)
    cleaned = re.sub(r"[^\w\u4e00-\u9fff]", "", cleaned)
    return 1 < len(cleaned) <= 18


def _command_candidates_from_help_text(text: str) -> list[str]:
    command = str(text or "").strip()
    command = command.strip("[]【】<>《》")
    candidates = [command]
    no_param = re.sub(r"\s*[\[【<（(].*$", "", command).strip()
    if no_param:
        candidates.append(no_param)
    no_plus = re.sub(r"\s*[+＋].*$", "", no_param or command).strip()
    if no_plus:
        candidates.append(no_plus)

    result = []
    for item in candidates:
        if item and item not in result:
            result.append(item)
    return result


def _match_known_help_command(text: str, known_commands: set[str] | frozenset[str]) -> str | None:
    for command in _command_candidates_from_help_text(text):
        if command in known_commands:
            return command
    return None


def _link_single_help_command(text: str, known_commands: set[str] | frozenset[str]) -> str:
    leading = text[:len(text) - len(text.lstrip())]
    trailing = text[len(text.rstrip()):]
    core = text.strip()
    if not _looks_like_help_command(core):
        return text

    command = _match_known_help_command(core, known_commands)
    if not command:
        return text

    normalized = core.strip("[]【】<>《》")
    if normalized.startswith(command):
        suffix = normalized[len(command):].replace("[", "【").replace("]", "】")
        linked = f"{build_md_command_link(command, command)}{suffix}"
    else:
        linked = build_md_command_link(command, command)
    return f"{leading}{linked}{trailing}"


def _link_help_command_part(text: str, known_commands: set[str] | frozenset[str]) -> str:
    parts = re.split(r"(\s*(?:/|\||、|或)\s*)", text)
    linked_parts = []
    for part in parts:
        if part.strip() in {"/", "|", "、", "或"}:
            linked_parts.append(part)
            continue
        if not part.strip():
            linked_parts.append(part)
            continue
        linked_parts.append(_link_single_help_command(part, known_commands))
    return "".join(linked_parts)


def build_help_native_markdown(
    msg: str,
    buttons: list[tuple[str, str]] | None = None,
    append_buttons: bool = True,
) -> str:
    """将普通帮助文本转换为带蓝字命令的原生 Markdown 文本。"""
    text = str(msg or " ")
    known_commands = set(_get_known_help_commands())
    known_commands.update(str(command).strip() for _, command in buttons or [] if command)
    if "mqqapi://aio/inlinecmd" in text:
        md_text = text
    else:
        lines = []
        for line in text.splitlines():
            stripped = line.strip()
            if not stripped or stripped.startswith(("【", "※", "---", "***", ">")):
                lines.append(line)
                continue

            match = re.match(r"^(\s*(?:[-*•→·]|[0-9]+[.、)]|[一二三四五六七八九十]+[.、])\s*)?(.*)$", line)
            prefix = match.group(1) or ""
            body = match.group(2) if match else line
            if not body.strip():
                lines.append(line)
                continue

            desc_match = re.match(r"^(.+?)(\s+-\s+|：|:\s*)(.*)$", body)
            if desc_match:
                command_part, separator, desc = desc_match.groups()
                if desc.strip():
                    if command_part.strip() in {"发送", "命令", "指令", "例如", "示例", "格式", "用法"}:
                        lines.append(f"{prefix}{command_part}{separator}{_link_help_command_part(desc, known_commands)}")
                    else:
                        lines.append(f"{prefix}{_link_help_command_part(command_part, known_commands)}{separator}{desc}")
                else:
                    lines.append(line)
                continue

            if prefix or re.search(r"\s*(?:/|\||、|或)\s*", body):
                lines.append(f"{prefix}{_link_help_command_part(body, known_commands)}")
            else:
                lines.append(line)
        md_text = "\n".join(lines)

    if append_buttons:
        button_links = []
        for label, command in buttons or []:
            if label and command:
                button_links.append(build_md_command_link(label, command))
        if button_links:
            md_text = f"{md_text.rstrip()}\n\n---\n" + " | ".join(button_links)
    # 帮助条目统一展示优化
    return enhance_markdown_display(md_text)
