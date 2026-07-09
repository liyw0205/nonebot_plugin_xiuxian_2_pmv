from __future__ import annotations

from dataclasses import dataclass
from typing import Literal
from urllib.parse import urlparse

from ..adapter_compat import MessageSegment
from ..qq_compat.capabilities import QQCapabilities


ButtonStyle = Literal[0, 1]
PermissionType = Literal[0, 1, 2, 3]


@dataclass(frozen=True)
class ButtonSpec:
    label: str
    data: str
    button_id: str | None = None
    style: ButtonStyle = 1
    enter: bool = False
    reply: bool = False
    permission_type: PermissionType = 2
    specify_role_ids: tuple[str, ...] = ()
    specify_user_ids: tuple[str, ...] = ()

    def validate(self) -> None:
        label = self.label.strip()
        data = self.data.strip()
        if not label or len(label) > 20:
            raise ValueError("按钮文本长度必须位于 1 到 20 个字符")
        if not data or len(data) > 1024:
            raise ValueError("按钮数据长度必须位于 1 到 1024 个字符")
        parsed = urlparse(data)
        if parsed.scheme and parsed.scheme not in {"http", "https", "mqqapi"}:
            raise ValueError(f"按钮链接协议不受支持: {parsed.scheme}")
        if parsed.scheme in {"http", "https"} and not parsed.netloc:
            raise ValueError("HTTP 按钮链接缺少主机名")


@dataclass(frozen=True)
class KeyboardSpec:
    rows: tuple[tuple[ButtonSpec, ...], ...]

    def validate(self) -> None:
        if not self.rows or len(self.rows) > 5:
            raise ValueError("键盘必须包含 1 到 5 行按钮")
        for row in self.rows:
            if not row or len(row) > 5:
                raise ValueError("每行必须包含 1 到 5 个按钮")
            for button in row:
                button.validate()


def build_qq_keyboard(
    spec: KeyboardSpec,
    capabilities: QQCapabilities | None = None,
):
    capabilities = capabilities or QQCapabilities()
    if not capabilities.keyboard:
        raise RuntimeError("当前 QQ Bot 未启用 keyboard 能力")
    spec.validate()
    rows = []
    for row in spec.rows:
        buttons = []
        for button in row:
            buttons.append(
                MessageSegment.qq_inline_command_button(
                    button.label,
                    button.data,
                    button_id=button.button_id,
                    style=button.style,
                    enter=button.enter,
                    reply=button.reply,
                    permission_type=button.permission_type,
                    specify_role_ids=list(button.specify_role_ids) or None,
                    specify_user_ids=list(button.specify_user_ids) or None,
                )
            )
        rows.append(buttons)
    return MessageSegment.qq_inline_keyboard(rows)


def keyboard_plain_text(spec: KeyboardSpec) -> str:
    spec.validate()
    return "\n".join(" | ".join(button.label for button in row) for row in spec.rows)


__all__ = [
    "ButtonSpec",
    "ButtonStyle",
    "KeyboardSpec",
    "PermissionType",
    "build_qq_keyboard",
    "keyboard_plain_text",
]
