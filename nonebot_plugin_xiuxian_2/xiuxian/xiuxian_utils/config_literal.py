"""配置写回公共逻辑：合法 Python 字面量 + 写后语法自愈。

版本更新时会把旧内存配置写回新代码的 xiuxian_config.py；
若字符串含真换行却直接 f'"{val}"' 包裹，会 SyntaxError。
"""

from __future__ import annotations

import ast
import re
from pathlib import Path
from typing import Any


def format_config_value(field_type: str, new_value: Any) -> str:
    """把配置值格式化为可写入 xiuxian_config.py 的单行字面量。"""
    ft = str(field_type or "str")

    if ft == "bool":
        return "True" if str(new_value).lower() in ("true", "1", "yes", "on") else "False"

    if ft == "list[int]":
        if isinstance(new_value, str):
            cleaned = re.sub(r"[^0-9,]", "", new_value)
            items = [i.strip() for i in cleaned.split(",") if i.strip()]
            return f"[{', '.join(items)}]" if items else "[]"
        if isinstance(new_value, (list, tuple)):
            nums = []
            for x in new_value:
                try:
                    nums.append(str(int(x)))
                except Exception:
                    continue
            return f"[{', '.join(nums)}]"
        return "[]"

    if ft == "list[str]":
        if isinstance(new_value, str):
            cleaned = new_value.strip().replace("[", "").replace("]", "")
            items = [i.strip().strip("'").strip('"') for i in cleaned.split(",") if i.strip()]
            return "[" + ", ".join(repr(i) for i in items) + "]"
        if isinstance(new_value, (list, tuple)):
            return "[" + ", ".join(repr(str(i)) for i in new_value) + "]"
        return "[]"

    if ft == "int":
        try:
            return str(int(float(new_value)))
        except Exception:
            return "0"

    if ft == "float":
        try:
            return str(float(new_value))
        except Exception:
            return "0.0"

    # str / select / 其它：必须 repr，转义真换行
    if isinstance(new_value, str):
        val_str = new_value
        if len(val_str) >= 2 and (
            (val_str.startswith('"') and val_str.endswith('"'))
            or (val_str.startswith("'") and val_str.endswith("'"))
        ):
            # 仅当整段被包一层引号时去掉，避免误伤内容
            inner = val_str[1:-1]
            if "\n" not in val_str or "\\n" in val_str:
                val_str = inner
    else:
        val_str = str(new_value)
    return repr(val_str)


def apply_config_values_to_source(
    content: str,
    new_values: dict[str, Any],
    field_types: dict[str, str],
) -> tuple[str, list[str]]:
    """把 new_values 写进源码文本。返回 (new_content, skipped_fields)。"""
    skipped: list[str] = []
    out = content
    for field_name, new_value in (new_values or {}).items():
        if field_name not in field_types:
            continue
        formatted = format_config_value(field_types[field_name], new_value)
        # 匹配 self.xxx = ... 到行尾；若已是坏掉的多行字符串，先用 heal 再写
        pattern = rf"(self\.{re.escape(field_name)}\s*=\s*).+"
        if re.search(pattern, out):
            out = re.sub(
                pattern,
                lambda m, fv=formatted: f"{m.group(1)}{fv}",
                out,
                count=1,
            )
        else:
            skipped.append(field_name)
    return out, skipped


def heal_broken_string_assignments(content: str) -> tuple[str, bool]:
    """修复 self.xxx = \"a\\nb\" 被拆成真换行的语法错误。

    策略：找 self.name = \" 起、到下一个同缩进 self./# 之前，拼成一个字符串再 repr。
    """
    try:
        compile(content, "<xiuxian_config>", "exec")
        return content, False
    except SyntaxError:
        pass

    lines = content.splitlines(keepends=True)
    out: list[str] = []
    i = 0
    changed = False
    assign_start = re.compile(r"^(\s*)self\.(\w+)\s*=\s*(.*)$")

    while i < len(lines):
        line = lines[i]
        m = assign_start.match(line.rstrip("\n").rstrip("\r"))
        if not m:
            out.append(line)
            i += 1
            continue

        indent, name, rest = m.group(1), m.group(2), m.group(3)
        rest_s = rest.strip()
        # 正常单行字面量
        if rest_s and not (
            (rest_s.startswith('"') and not _is_closed_quote(rest_s, '"'))
            or (rest_s.startswith("'") and not _is_closed_quote(rest_s, "'"))
        ):
            out.append(line)
            i += 1
            continue

        # 未闭合字符串：吞后续行直到闭合或遇到下一个 self./#
        quote = rest_s[:1] if rest_s[:1] in "\"'" else '"'
        buf = [rest]
        j = i + 1
        closed = _is_closed_quote(rest_s, quote) if rest_s else False
        while j < len(lines) and not closed:
            nxt = lines[j]
            nxt_body = nxt.rstrip("\n").rstrip("\r")
            # 下一个赋值/注释（同级）则停止
            if re.match(r"^\s*self\.\w+\s*=", nxt_body) or re.match(r"^\s*#", nxt_body):
                break
            buf.append(nxt_body)
            joined_test = "\n".join(buf).strip()
            if joined_test.startswith(quote) and _is_closed_quote(joined_test, quote):
                closed = True
                j += 1
                break
            j += 1

        raw = "\n".join(buf).strip()
        # 去掉外层残缺引号，还原真实字符串
        if raw.startswith(quote):
            raw = raw[1:]
        if raw.endswith(quote):
            raw = raw[:-1]
        # 内容里的真换行保留
        fixed = f"{indent}self.{name} = {repr(raw)}\n"
        out.append(fixed)
        changed = True
        i = j if j > i else i + 1

    new_content = "".join(out)
    try:
        compile(new_content, "<xiuxian_config>", "exec")
        return new_content, changed
    except SyntaxError:
        # 再兜底：只修常见欢迎字段
        return _heal_known_welcome_fields(content)


def _is_closed_quote(s: str, quote: str) -> bool:
    if not s.startswith(quote):
        return True
    # 粗略：去掉转义后，引号成对
    body = s[1:]
    i = 0
    while i < len(body):
        ch = body[i]
        if ch == "\\" and i + 1 < len(body):
            i += 2
            continue
        if ch == quote:
            # 引号后只允许空白/注释
            return body[i + 1 :].strip() == "" or body[i + 1 :].strip().startswith("#")
        i += 1
    return False


def _heal_known_welcome_fields(content: str) -> tuple[str, bool]:
    defaults = {
        "group_welcome_msg": "欢迎道友入群！\n> 发送：\n【我要修仙】踏入修仙界\n【修仙帮助】查看玩法\n【娱乐帮助】查看娱乐功能。",
        "group_bot_join_msg": "必死之境机逢仙缘，修仙之路波澜壮阔！\n> 发送：\n【我要修仙】踏入修仙界\n【修仙帮助】查看玩法\n【娱乐帮助】查看娱乐功能。",
    }
    out = content
    changed = False
    for name, value in defaults.items():
        pat = re.compile(
            rf"(self\.{re.escape(name)}\s*=\s*)([\s\S]*?)(?=\n\s*(?:self\.|#))",
            re.M,
        )
        m = pat.search(out)
        if not m:
            continue
        # 若当前片段 compile 失败才替换
        snippet = m.group(0)
        try:
            compile(f"class T:\n def __init__(self):\n  {snippet}\n", "<s>", "exec")
            continue
        except SyntaxError:
            pass
        out = out[: m.start()] + m.group(1) + repr(value) + out[m.end() :]
        changed = True
    try:
        compile(out, "<xiuxian_config>", "exec")
        return out, changed
    except SyntaxError:
        return content, False


def write_config_values(
    config_file: Path,
    new_values: dict[str, Any],
    field_types: dict[str, str],
) -> tuple[bool, str]:
    config_file = Path(config_file)
    if not config_file.exists():
        return False, "配置文件不存在"
    try:
        content = config_file.read_text(encoding="utf-8")
        # 先自愈已坏文件，再写入
        content, healed = heal_broken_string_assignments(content)
        content, skipped = apply_config_values_to_source(content, new_values, field_types)
        content, healed2 = heal_broken_string_assignments(content)
        # 最终语法检查
        compile(content, str(config_file), "exec")
        config_file.write_text(content, encoding="utf-8")
        msg = "配置保存成功"
        if healed or healed2:
            msg += "（已自动修复多行字符串）"
        if skipped:
            msg += f"；跳过未找到字段: {', '.join(skipped[:8])}"
        return True, msg
    except Exception as e:
        return False, f"保存配置时出错: {e}"


def heal_config_file(config_file: Path) -> tuple[bool, str]:
    config_file = Path(config_file)
    if not config_file.exists():
        return False, "配置文件不存在"
    try:
        content = config_file.read_text(encoding="utf-8")
        new_content, changed = heal_broken_string_assignments(content)
        if not changed:
            # 即使标记未改，也确认可编译
            compile(content, str(config_file), "exec")
            return True, "配置语法正常"
        compile(new_content, str(config_file), "exec")
        config_file.write_text(new_content, encoding="utf-8")
        return True, "已修复配置文件中的多行字符串语法错误"
    except Exception as e:
        return False, f"修复失败: {e}"
