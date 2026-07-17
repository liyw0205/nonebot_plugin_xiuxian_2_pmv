"""插件加载时强制使用内置能力：vendor 路径 + Intent/事件运行时补丁。

即使 `nb run` 已先 register 了 pip adapter，也能在 WebSocket Identify 前：
1. 打开 group_members intent（bit 24）
2. 注册 GROUP_MEMBER_ADD / GROUP_MEMBER_REMOVE 事件类

不依赖启动脚本 / PYTHONPATH。
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any


def inject_vendor_adapter_paths(plugin_root=None) -> list[str]:
    try:
        import nonebot.adapters as nonebot_adapters
    except Exception:
        return []

    from pathlib import Path

    roots: list[Path] = []
    if plugin_root is not None:
        roots.append(Path(plugin_root))
    roots.append(Path(__file__).resolve().parents[2])

    plugin = next((p for p in roots if p.exists()), None)
    if plugin is None:
        return []

    vendor = plugin / "xiuxian" / "xiuxian_adapter" / "vendor"
    vendor_paths: list[str] = []
    for name in ("adapter_qq", "adapter_onebot"):
        path = vendor / name / "nonebot" / "adapters"
        if path.is_dir():
            vendor_paths.append(str(path.resolve()))
    if not vendor_paths:
        return []

    vendor_set = set(vendor_paths)
    rest = [p for p in map(str, nonebot_adapters.__path__) if p not in vendor_set]
    while len(nonebot_adapters.__path__):
        nonebot_adapters.__path__.pop()
    for p in vendor_paths + rest:
        nonebot_adapters.__path__.append(p)
    return vendor_paths


def force_group_members_intent() -> bool:
    """强制 Identify 时带上 group_members bit。"""
    try:
        from nonebot.adapters.qq import config as qq_config
    except Exception:
        return False

    intents_cls = getattr(qq_config, "Intents", None)
    if intents_cls is None:
        return False

    # 1) 包装 to_int：无论字段是否存在，都 OR bit24
    if not getattr(intents_cls.to_int, "__xiuxian_group_members_forced__", False):
        original = intents_cls.to_int

        def to_int(self, *args, **kwargs):  # type: ignore[no-untyped-def]
            try:
                if hasattr(self, "group_members"):
                    try:
                        self.group_members = True
                    except Exception:
                        try:
                            object.__setattr__(self, "group_members", True)
                        except Exception:
                            pass
            except Exception:
                pass
            value = int(original(self, *args, **kwargs))
            return value | (1 << 24)

        to_int.__xiuxian_group_members_forced__ = True  # type: ignore[attr-defined]
        intents_cls.to_int = to_int  # type: ignore[method-assign]

    # 2) 字段默认 True（若存在）
    fields = getattr(intents_cls, "model_fields", None) or getattr(intents_cls, "__fields__", {})
    if "group_members" in fields:
        try:
            fields["group_members"].default = True
        except Exception:
            pass

    # 3) 已构造的 BotInfo
    try:
        from nonebot import get_driver

        for bot in getattr(get_driver().config, "qq_bots", None) or []:
            intent = getattr(bot, "intent", None)
            if intent is None:
                continue
            if hasattr(intent, "group_members"):
                try:
                    intent.group_members = True
                except Exception:
                    try:
                        object.__setattr__(intent, "group_members", True)
                    except Exception:
                        pass
    except Exception:
        pass
    return True


def _ensure_event_type_member(event_type_cls: type[Enum], name: str, value: str) -> Any:
    if name in event_type_cls.__members__:
        return event_type_cls[name]
    try:
        # Python 3.11 Enum: 用 functional extension 不安全；直接构造成员对象
        member = object.__new__(event_type_cls)
        member._name_ = name
        member._value_ = value
        # str Enum 需要同步 str 值
        if issubclass(event_type_cls, str):
            str.__init__(member, value)
        event_type_cls._value2member_map_[value] = member  # type: ignore[attr-defined]
        event_type_cls._member_map_[name] = member  # type: ignore[attr-defined]
        if name not in event_type_cls._member_names_:  # type: ignore[attr-defined]
            event_type_cls._member_names_.append(name)  # type: ignore[attr-defined]
        # bypass Enum.__setattr__
        type.__setattr__(event_type_cls, name, member)
        return member
    except Exception:
        # 兜底：用 value 字符串本身作为 type 标记
        return value


def force_group_member_events() -> bool:
    """给已加载的 pip adapter 补注册成员进退群事件。"""
    try:
        from nonebot.adapters.qq import event as qq_event
    except Exception:
        return False

    event_classes = getattr(qq_event, "EVENT_CLASSES", None)
    if not isinstance(event_classes, dict):
        return False

    # 已有则跳过
    if "GROUP_MEMBER_ADD" in event_classes and "GROUP_MEMBER_REMOVE" in event_classes:
        return True

    EventType = getattr(qq_event, "EventType", None)
    NoticeEvent = getattr(qq_event, "NoticeEvent", None)
    if EventType is None or NoticeEvent is None:
        return False

    try:
        from nonebot.compat import override
    except Exception:  # pragma: no cover
        def override(func):  # type: ignore
            return func

    add_type = _ensure_event_type_member(EventType, "GROUP_MEMBER_ADD", "GROUP_MEMBER_ADD")
    remove_type = _ensure_event_type_member(
        EventType, "GROUP_MEMBER_REMOVE", "GROUP_MEMBER_REMOVE"
    )

    # 复用已有基类或新建
    GroupMemberEvent = getattr(qq_event, "GroupMemberEvent", None)
    if GroupMemberEvent is None:

        class GroupMemberEvent(NoticeEvent):  # type: ignore[no-redef,valid-type,misc]
            timestamp: datetime
            group_openid: str
            member_openid: str

            @override
            def get_user_id(self) -> str:
                return self.member_openid

            @override
            def get_session_id(self) -> str:
                return f"group_{self.group_openid}_{self.member_openid}"

        qq_event.GroupMemberEvent = GroupMemberEvent  # type: ignore[attr-defined]

    if "GROUP_MEMBER_ADD" not in event_classes:

        class GroupMemberAddEvent(GroupMemberEvent):  # type: ignore[valid-type,misc]
            __type__ = add_type

        event_classes["GROUP_MEMBER_ADD"] = GroupMemberAddEvent
        qq_event.GroupMemberAddEvent = GroupMemberAddEvent  # type: ignore[attr-defined]

    if "GROUP_MEMBER_REMOVE" not in event_classes:

        class GroupMemberRemoveEvent(GroupMemberEvent):  # type: ignore[valid-type,misc]
            __type__ = remove_type

        event_classes["GROUP_MEMBER_REMOVE"] = GroupMemberRemoveEvent
        qq_event.GroupMemberRemoveEvent = GroupMemberRemoveEvent  # type: ignore[attr-defined]

    return True


def force_builtin_qq_adapter() -> dict[str, Any]:
    """一站式：路径注入 + intent + 事件补丁。"""
    injected = inject_vendor_adapter_paths()
    intent_ok = force_group_members_intent()
    events_ok = force_group_member_events()
    result = {
        "vendor_paths": injected,
        "intent_forced": intent_ok,
        "member_events_forced": events_ok,
    }
    try:
        from nonebot.log import logger

        logger.opt(colors=True).info(
            f"<green>[xiuxian_adapter]</green> 强制内置适配能力: "
            f"vendor={len(injected)} intent={intent_ok} member_events={events_ok}"
        )
    except Exception:
        pass
    return result
