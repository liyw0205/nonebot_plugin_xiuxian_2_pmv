from __future__ import annotations

import re
from collections.abc import Mapping
import json
from pathlib import Path
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


class MarkdownTemplateRegistry:
    """从单个 JSON 文件有限热加载 Markdown 模板。"""

    def __init__(
        self,
        path: str | Path | None = None,
        *,
        max_templates: int = 100,
        max_template_length: int = 8192,
    ) -> None:
        self.path = Path(path).expanduser().resolve() if path else None
        self.max_templates = max_templates
        self.max_template_length = max_template_length
        self._file_signature: tuple[int, int] | None = None
        self._templates: dict[str, str] = {}

    def reload_if_changed(self) -> bool:
        if self.path is None or not self.path.is_file():
            return False
        stat = self.path.stat()
        signature = (stat.st_mtime_ns, stat.st_size)
        if signature == self._file_signature:
            return False
        data = json.loads(self.path.read_text(encoding="utf-8"))
        if not isinstance(data, dict) or len(data) > self.max_templates:
            raise ValueError("Markdown 模板文件必须是数量受限的对象映射")
        templates: dict[str, str] = {}
        for name, template in data.items():
            if not re.fullmatch(r"[A-Za-z0-9_.-]{1,64}", str(name)):
                raise ValueError(f"Markdown 模板名称不合法: {name}")
            if not isinstance(template, str) or len(template) > self.max_template_length:
                raise ValueError(f"Markdown 模板内容不合法: {name}")
            templates[str(name)] = template
        self._templates = templates
        self._file_signature = signature
        return True

    def render(self, name: str, values: Mapping[str, Any]) -> str:
        self.reload_if_changed()
        try:
            template = self._templates[name]
        except KeyError as exc:
            raise KeyError(f"Markdown 模板不存在: {name}") from exc
        return render_markdown_template(template, values)


__all__ = [
    "MarkdownTemplateRegistry",
    "escape_qq_markdown",
    "render_markdown_template",
]
