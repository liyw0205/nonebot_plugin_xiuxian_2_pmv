from datetime import datetime
from typing import Any, Literal, TypeAlias
from urllib.parse import urlparse

from nonebot.compat import field_validator
from pydantic import BaseModel


class FriendAuthor(BaseModel):
    id: str
    user_openid: str | None = None
    union_openid: str | None = None
    username: str | None = None
    avatar: str | None = None


class GroupMemberAuthor(BaseModel):
    id: str
    bot: bool = False
    member_openid: str = ""
    member_role: Literal["member", "admin", "owner"] = "member"
    union_openid: str | None = None
    username: str | None = None
    avatar: str | None = None


class GroupMentionUser(BaseModel):
    scope: Literal["single"]
    bot: bool = False
    id: str
    is_you: bool = False
    member_openid: str | None = None
    username: str = ""
    member_role: str | None = None


class GroupMentionEveryone(BaseModel):
    scope: Literal["all"]
    is_you: Literal[True]
    username: str = ""


GroupMention: TypeAlias = GroupMentionUser | GroupMentionEveryone


class Attachment(BaseModel):
    content_type: str
    filename: str | None = None
    height: int | None = None
    width: int | None = None
    size: int | None = None
    url: str | None = None

    @field_validator("url", mode="after")
    def check_url(cls, v: str):
        if v and not urlparse(v).hostname:
            return f"https://{v}"
        return v


class Media(BaseModel):
    file_info: str


class _QQMessageScene(BaseModel):
    ext: list[str] = []
    source: str | None = None

    @field_validator("ext", mode="before")
    def ensure_ext_list(cls, v: Any):
        if v is None:
            return []
        if isinstance(v, str):
            return [v]
        return v


class _ReplyAuthor(BaseModel):
    bot: bool = False
    username: str | None = None
    id: str | None = None


class QQReplyMessage(BaseModel):
    id: str | None = None
    content: str = ""
    attachments: list[Attachment] | None = None
    message_type: int | None = None
    msg_idx: str | None = None
    author: _ReplyAuthor | None = None
    timestamp: str | None = None


class QQMessage(BaseModel):
    id: str
    content: str = ""
    timestamp: str | None = None
    mentions: list[GroupMention] | None = None
    attachments: list[Attachment] | None = None
    message_scene: _QQMessageScene | None = None
    message_type: int | None = None
    msg_idx: str | None = None
    msg_elements: list[QQReplyMessage] | None = None
    event_id: str | None = None
    message_reference: dict[str, Any] | None = None


class UserQQMessage(QQMessage):
    author: FriendAuthor


class GroupQQMessage(QQMessage):
    author: GroupMemberAuthor


class PostMessagesExtInfo(BaseModel):
    ref_idx: str | None = None
    msg_idx: str | None = None
    message_reference_id: str | None = None
    reference_id: str | None = None


class PostC2CMessagesReturn(BaseModel):
    id: str | None = None
    timestamp: datetime | None = None
    msg_id: str | None = None
    msg_seq: int | None = None
    ext_info: PostMessagesExtInfo | dict[str, Any] | None = None


class PostGroupMessagesReturn(BaseModel):
    id: str | None = None
    timestamp: datetime | None = None
    msg_id: str | None = None
    msg_seq: int | None = None
    ext_info: PostMessagesExtInfo | dict[str, Any] | None = None


class PostC2CFilesReturn(BaseModel):
    file_uuid: str | None = None
    file_info: str | None = None
    ttl: int | None = None


class UploadPartItem(BaseModel):
    index: int
    presigned_url: str


class UploadConfig(BaseModel):
    concurrency: int
    retry_timeout: int
    retry_delay: int


class PostC2CFilesPrepareReturn(BaseModel):
    upload_id: str
    block_size: int
    parts: list[UploadPartItem]
    upload_config: UploadConfig


class PostGroupFilesReturn(BaseModel):
    file_uuid: str | None = None
    file_info: str | None = None
    ttl: int | None = None


class PostGroupFilesPrepareReturn(BaseModel):
    upload_id: str
    block_size: int
    parts: list[UploadPartItem]
    upload_config: UploadConfig


class GroupMember(BaseModel):
    member_openid: str
    join_timestamp: datetime


class PostGroupMembersReturn(BaseModel):
    members: list[GroupMember]
    next_index: int | None = None


class MessageActionButton(BaseModel):
    template_id: Literal["1", "10"] = "1"  # 待废弃字段！！！
    callback_data: str | None = None
    feedback: bool | None = None  # 反馈按钮（赞踩按钮）
    tts: bool | None = None  # TTS 语音播放按钮
    re_generate: bool | None = None  # 重新生成按钮
    stop_generate: bool | None = None  # 停止生成按钮


class PromptAction(BaseModel):
    type: Literal[2] = 2


class PromptRenderData(BaseModel):
    label: str
    style: Literal[2] = 2


class PromptButton(BaseModel):
    render_data: PromptRenderData
    action: PromptAction


class PromptRow(BaseModel):
    buttons: list[PromptButton]


class PromptContent(BaseModel):
    rows: list[PromptRow]


class PromptKeyboardModel(BaseModel):
    content: PromptContent


class MessagePromptKeyboard(BaseModel):
    keyboard: PromptKeyboardModel


class MessageStream(BaseModel):
    state: Literal[1, 10, 11, 20]
    """1: 正文生成中, 10: 正文生成结束, 11: 引志消息生成中, 20: 引导消息生成结束。"""
    id: str | None = None
    """第一条不用填写，第二条需要填写第一个分片返回的 msgID"""
    index: int
    """从 1 开始"""
    reset: bool | None = None
    """只能用于流式消息没有发送完成时，reset 时 index 需要从 0 开始，需要填写流式 id"""


__all__ = [
    "Attachment",
    "FriendAuthor",
    "GroupMember",
    "GroupMemberAuthor",
    "GroupMention",
    "GroupMentionEveryone",
    "GroupMentionUser",
    "GroupQQMessage",
    "Media",
    "MessageActionButton",
    "MessagePromptKeyboard",
    "MessageStream",
    "PostMessagesExtInfo",
    "PostC2CFilesPrepareReturn",
    "PostC2CFilesReturn",
    "PostC2CMessagesReturn",
    "PostGroupFilesPrepareReturn",
    "PostGroupFilesReturn",
    "PostGroupMembersReturn",
    "PostGroupMessagesReturn",
    "PromptAction",
    "PromptButton",
    "PromptContent",
    "PromptKeyboardModel",
    "PromptRenderData",
    "PromptRow",
    "QQMessage",
    "QQReplyMessage",
    "UserQQMessage",
]
