"""
修仙信息模块入口。

具体命令实现按功能拆分到子模块；这里保留导入门面，确保加载本包时完成命令注册，
并兼容外部从 xiuxian_info 直接导入工具函数。
"""

from .avatar import (
    avatar_switch_cmd,
    avatar_switch_cmd_,
    get_active_user_id,
    get_avatar_info,
    init_avatar_if_needed,
    my_id_cmd,
    my_id_cmd_,
    toggle_avatar,
)
from .changelog_command import changelog, changelog_
from .user_info import (
    get_user_xiuxian_info,
    xiuxian_message,
    xiuxian_message_,
    xiuxian_message_img,
    xiuxian_message_img_,
)

__all__ = [
    "avatar_switch_cmd",
    "avatar_switch_cmd_",
    "changelog",
    "changelog_",
    "get_active_user_id",
    "get_avatar_info",
    "get_user_xiuxian_info",
    "init_avatar_if_needed",
    "my_id_cmd",
    "my_id_cmd_",
    "toggle_avatar",
    "xiuxian_message",
    "xiuxian_message_",
    "xiuxian_message_img",
    "xiuxian_message_img_",
]
