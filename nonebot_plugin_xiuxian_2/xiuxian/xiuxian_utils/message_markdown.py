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
    return md_text
