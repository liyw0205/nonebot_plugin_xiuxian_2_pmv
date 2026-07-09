from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any


_MARKDOWN_SPECIAL = re.compile(r"([\\`*_{}\[\]()#+.!|>~-])")


def escape_qq_markdown(value: Any) -> str:
    return _MARKDOWN_SPECIAL.sub(r"\\\1", str(value))


def render_markdown_template(template: str, values: Mapping[str, Any]) -> str:
    escaped = {key: escape_qq_markdown(value) for key, value in values.items()}
    try:
        return template.format_map(escaped)
    except (KeyError, ValueError) as exc:
        if isinstance(exc, KeyError):
            detail = f"缺少变量: {exc.args[0]}"
        else:
            detail = f"格式错误: {exc}"
        raise ValueError(f"Markdown 模板{detail}") from exc


__all__ = ["escape_qq_markdown", "render_markdown_template"]
