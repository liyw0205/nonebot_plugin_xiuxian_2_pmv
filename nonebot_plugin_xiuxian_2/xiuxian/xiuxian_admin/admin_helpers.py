import re
from datetime import datetime
from html import unescape
from urllib.parse import quote, unquote

from ..adapter_compat import get_user_id


def _admin_economy_context(event, action: str, **detail):
    operator_id = str(get_user_id(event))
    return {
        "source": "admin",
        "action": action,
        "trace_id": f"admin:{action}:{operator_id}:{datetime.now().strftime('%Y%m%d%H%M%S%f')}",
        "detail": {"operator_id": operator_id, **detail},
    }


def fix_mqqapi_inlinecmd_links(text: str) -> str:
    """
    修复 QQ Markdown 中的 mqqapi://aio/inlinecmd 链接。

    修复内容：
    1. mqqapi:/aio、mqqapi:///aio 统一为 mqqapi://aio
    2. &amp; 还原为 &
    3. command 参数避免重复编码
    4. 去除重复 enter/reply 参数
    5. 缺少 enter/reply 时自动补齐
    """
    if not text:
        return text

    # 统一 mqqapi 斜杠格式
    text = re.sub(
        r"mqqapi:/+aio/inlinecmd",
        "mqqapi://aio/inlinecmd",
        text
    )

    def repl(match):
        raw_url = match.group(0)

        # 关键：把 &amp; 还原成 &
        raw_url = unescape(raw_url)

        prefix = "mqqapi://aio/inlinecmd?"
        if not raw_url.startswith(prefix):
            return raw_url

        query = raw_url[len(prefix):]

        params = {}
        for part in query.split("&"):
            if not part:
                continue

            if "=" in part:
                k, v = part.split("=", 1)
            else:
                k, v = part, ""

            k = k.strip()
            v = v.strip()

            if not k:
                continue

            # 后出现的同名参数覆盖前面的，避免重复
            params[k] = v

        command = params.get("command", "")

        if command:
            # 先 unquote，再 quote，避免二次编码
            command = unquote(command).strip()

            # 如果你的命令中不需要空格，可以取消下一行注释
            # command = command.replace(" ", "")

            params["command"] = quote(command, safe="")

        # 补默认参数，但不重复添加
        params.setdefault("enter", "false")
        params.setdefault("reply", "false")

        # 建议固定输出顺序
        ordered_keys = ["command", "enter", "reply"]
        parts = []

        for k in ordered_keys:
            if k in params:
                parts.append(f"{k}={params[k]}")

        for k, v in params.items():
            if k not in ordered_keys:
                parts.append(f"{k}={v}")

        return prefix + "&".join(parts)

    return re.sub(
        r"mqqapi://aio/inlinecmd\?[^)\r\n]+",
        repl,
        text
    )


def _extract_keyboard_command(command: str) -> str:
    command = unescape(str(command or "").strip())
    if not command.startswith("mqqapi://aio/inlinecmd?"):
        return command

    match = re.search(r"(?:^|[?&])command=([^&\s]+)", command)
    if match:
        return unquote(match.group(1)).strip()
    return command


def _parse_keyboard_test_rows(raw: str) -> list[list[tuple[str, str]]]:
    text = str(raw or "").strip()
    if not text:
        text = "[1](2) 3"

    text = (
        text.replace("\\r\\n", "\n")
        .replace("\\n", "\n")
        .replace("\\r", "\n")
        .replace("\r\n", "\n")
        .replace("\r", "\n")
    )

    rows: list[list[tuple[str, str]]] = []
    token_pattern = re.compile(r"\[([^\]\n]+)\]\(([^\)\n]+)\)|(\S+)")

    for line in text.split("\n"):
        row: list[tuple[str, str]] = []
        for match in token_pattern.finditer(line):
            if match.group(1) is not None:
                label = match.group(1).strip()
                command = _extract_keyboard_command(match.group(2))
            else:
                label = match.group(3).strip()
                command = label

            if label and command:
                row.append((label, command))

        if row:
            rows.append(row)

    return rows or [[("1", "2"), ("3", "3")]]


def parse_broadcast_duration_and_content(raw: str) -> tuple[int, str]:
    """
    解析广播时间和内容。

    支持格式：
    - 群聊广播 1天 广播测试
    - 群聊广播 1小时 广播测试
    - 群聊广播 1分钟 广播测试
    - 群聊广播 1天1小时10分钟 广播测试
    - 群聊广播 广播测试

    规则：
    - 只解析开头第一个连续时间参数。
    - 时间格式固定为：x天/x小时/x分钟。
    - 可以顺序组合：1天10分钟、1天1小时10分钟。
    - 没有时间参数默认 1 天，也就是 1440 分钟。
    - 后面的广播内容允许有空格。
    """
    raw = str(raw or "").strip()

    if not raw:
        return 1440, ""

    # 固定只匹配开头的连续时间表达式。
    # 顺序固定：天 -> 小时 -> 分钟。
    # 示例可匹配：
    # 1天
    # 1小时
    # 1分钟
    # 1天10分钟
    # 1天1小时10分钟
    pattern = r"^(?:(\d+)天)?(?:(\d+)小时)?(?:(\d+)分钟)?(?=\s|$)"

    match = re.match(pattern, raw)

    if not match:
        return 1440, raw

    day_str, hour_str, minute_str = match.groups()

    # 三个都没有，说明开头不是时间参数
    if not day_str and not hour_str and not minute_str:
        return 1440, raw

    days = int(day_str or 0)
    hours = int(hour_str or 0)
    minutes = int(minute_str or 0)

    total_minutes = days * 1440 + hours * 60 + minutes

    if total_minutes <= 0:
        total_minutes = 1440

    content = raw[match.end():].strip()

    return total_minutes, content


def parse_clear_broadcast_kind(raw: str) -> str | None:
    """
    解析清空广播类型。

    支持：
    - 清空广播
    - 清空广播 全部
    - 清空广播 all
    - 清空广播 群聊
    - 清空广播 私聊
    - 清空广播 全局
    """
    raw = str(raw or "").strip().lower()

    if raw in ("", "全部", "所有", "all"):
        return None

    kind_map = {
        "群聊": "group",
        "群": "group",
        "group": "group",

        "私聊": "private",
        "私": "private",
        "private": "private",

        "全局": "global",
        "global": "global",
    }

    return kind_map.get(raw, raw)
