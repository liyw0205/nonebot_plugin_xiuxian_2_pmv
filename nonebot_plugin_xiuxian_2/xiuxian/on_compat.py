from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterator, Mapping
from contextvars import ContextVar
import importlib
import re
import sys
from typing import TYPE_CHECKING

from nonebot import get_driver
from nonebot.internal.matcher.provider import MatcherProvider
from nonebot.log import logger
from nonebot.matcher import matchers
from nonebot.plugin.on import (
    on as _nb_on,
    on_command as _nb_on_command,
    on_endswith as _nb_on_endswith,
    on_fullmatch as _nb_on_fullmatch,
    on_keyword as _nb_on_keyword,
    on_message as _nb_on_message,
    on_regex as _nb_on_regex,
    on_shell_command as _nb_on_shell_command,
    on_startswith as _nb_on_startswith,
)
from nonebot.rule import TrieRule

if TYPE_CHECKING:
    from nonebot.adapters import Event
    from nonebot.matcher import Matcher


CommandValue = str | tuple[str, ...]
CommandInput = CommandValue | list[CommandValue] | set[CommandValue]
TextValue = str | tuple[str, ...]

_CURRENT_EVENT: ContextVar[object | None] = ContextVar(
    "xiuxian_on_compat_event",
    default=None,
)
_INSTALLED = False
_MATCHER_ROUTES: dict[type["Matcher"], "_RouteMeta"] = {}


class _RouteMeta:
    __slots__ = ("commands", "fullmatches", "generic", "prefixes")

    def __init__(
        self,
        *,
        commands: set[tuple[str, ...]] | None = None,
        prefixes: set[str] | None = None,
        fullmatches: set[str] | None = None,
        generic: bool = False,
    ):
        self.commands = commands or set()
        self.prefixes = prefixes or set()
        self.fullmatches = fullmatches or set()
        self.generic = generic


class _FilteredMatcherList(list):
    def __init__(
        self,
        source: list[type["Matcher"]],
        selected: list[type["Matcher"]],
    ):
        super().__init__(selected)
        self._source = source

    def append(self, matcher: type["Matcher"]) -> None:
        self._source.append(matcher)
        super().append(matcher)

    def remove(self, matcher: type["Matcher"]) -> None:
        self._source.remove(matcher)
        if matcher in self:
            super().remove(matcher)


def _is_enabled() -> bool:
    try:
        return bool(getattr(get_driver().config, "xiuxian_on_compat_gate", True))
    except Exception:
        return True


def _is_xiuxian_module(module_name: str | None) -> bool:
    if not module_name:
        return False
    return (
        module_name == "xiuxian"
        or module_name.startswith("xiuxian.")
        or module_name == "nonebot_plugin_xiuxian_2.xiuxian"
        or module_name.startswith("nonebot_plugin_xiuxian_2.xiuxian.")
        or ".xiuxian." in module_name
        or module_name.endswith(".xiuxian")
    )


def _should_route_matcher(matcher: type["Matcher"]) -> bool:
    return _is_xiuxian_module(getattr(matcher, "module_name", None))


def _command_tuple(command: CommandValue) -> tuple[str, ...]:
    if isinstance(command, str):
        return (command,)
    return tuple(command)


def _split_commands(
    cmd: CommandInput,
    aliases: set[CommandValue] | None,
) -> tuple[CommandValue, set[CommandValue] | None, set[tuple[str, ...]]]:
    if isinstance(cmd, (list, set)):
        raw_commands = list(cmd)
        if not raw_commands:
            raise ValueError("on_command requires at least one command")
        primary = raw_commands[0]
        alias_values = set(raw_commands[1:])
    else:
        primary = cmd
        alias_values = set()

    if aliases:
        alias_values.update(aliases)

    commands = {_command_tuple(primary)}
    commands.update(_command_tuple(alias) for alias in alias_values)
    return primary, alias_values or None, commands


def _normal_text_values(msg: TextValue) -> set[str]:
    return {msg} if isinstance(msg, str) else set(map(str, msg))


def _literal_regex_prefix(pattern: str) -> str:
    if not pattern.startswith("^"):
        return ""

    meta_chars = set(".^$*+?{}[]\\|()")
    escaped_literals = meta_chars | {" "}
    chars: list[str] = []
    i = 1
    while i < len(pattern):
        char = pattern[i]
        if char == "\\":
            if i + 1 < len(pattern) and pattern[i + 1] in escaped_literals:
                chars.append(pattern[i + 1])
                i += 2
                continue
            break
        if char in meta_chars:
            break
        chars.append(char)
        i += 1
    return "".join(chars)


def _register_route(matcher: type["Matcher"], meta: _RouteMeta) -> type["Matcher"]:
    if _should_route_matcher(matcher):
        _MATCHER_ROUTES[matcher] = meta
    return matcher


def _get_plain_text(event: "Event") -> str:
    try:
        return str(event.get_plaintext())
    except Exception:
        pass

    for attr in ("raw_message", "plaintext", "content"):
        value = getattr(event, attr, None)
        if value:
            return str(value)
    return ""


def _get_first_text(event: "Event") -> str:
    try:
        message = event.get_message()
    except Exception:
        return ""

    if not message:
        return ""

    segment = message[0]
    try:
        if not segment.is_text():
            return ""
    except Exception:
        if getattr(segment, "type", "") != "text":
            return ""

    return str(segment).lstrip()


def _get_command(event: "Event") -> tuple[str, ...] | None:
    try:
        if event.get_type() != "message":
            return None
    except Exception:
        return None

    text = _get_first_text(event)
    if not text:
        return None

    try:
        prefix = TrieRule.prefix.longest_prefix(text)
    except Exception:
        return None

    if not prefix:
        return None
    return prefix.value.command


def _route_event(event: "Event") -> tuple[tuple[str, ...] | None, str] | None:
    try:
        if event.get_type() != "message":
            return None
    except Exception:
        return None

    cached = getattr(event, "_xiuxian_on_compat_route", None)
    if cached is not None:
        return cached

    route = (_get_command(event), _get_plain_text(event))
    try:
        setattr(event, "_xiuxian_on_compat_route", route)
    except Exception:
        pass
    return route


class XiuxianOnCompatProvider(MatcherProvider):
    def __init__(self, source: Mapping[int, list[type["Matcher"]]]):
        self._matchers: defaultdict[int, list[type["Matcher"]]] = defaultdict(list)
        for priority, matcher_list in source.items():
            self._matchers[priority] = list(matcher_list)

        self._signature: tuple[tuple[int, int], ...] = ()
        self._routed_by_priority: dict[int, set[type["Matcher"]]] = {}
        self._command_index: dict[int, dict[tuple[str, ...], set[type["Matcher"]]]] = {}
        self._prefix_index: dict[int, list[tuple[str, set[type["Matcher"]]]]] = {}
        self._fullmatch_index: dict[int, dict[str, set[type["Matcher"]]]] = {}
        self._generic_index: dict[int, set[type["Matcher"]]] = {}
        self.rebuild(log=False)

    def __getitem__(self, priority: int) -> list[type["Matcher"]]:
        matcher_list = self._matchers[priority]
        event = _CURRENT_EVENT.get()
        if event is None or not _is_enabled():
            return matcher_list

        self._rebuild_if_changed()
        route = _route_event(event)  # type: ignore[arg-type]
        if route is None:
            return matcher_list

        command, text = route
        routed = self._routed_by_priority.get(priority)
        if not routed:
            return matcher_list

        selected: set[type["Matcher"]] = set()
        if command is not None:
            selected.update(self._command_index.get(priority, {}).get(command, ()))

        if text:
            selected.update(self._fullmatch_index.get(priority, {}).get(text, ()))
            for prefix, prefix_matchers in self._prefix_index.get(priority, ()):
                if text.startswith(prefix):
                    selected.update(prefix_matchers)

        selected.update(self._generic_index.get(priority, ()))
        if not selected:
            filtered = [matcher for matcher in matcher_list if matcher not in routed]
        else:
            filtered = [
                matcher
                for matcher in matcher_list
                if matcher not in routed or matcher in selected
            ]
        return _FilteredMatcherList(matcher_list, filtered)

    def __setitem__(self, priority: int, matcher_list: list[type["Matcher"]]) -> None:
        self._matchers[priority] = matcher_list
        self.rebuild()

    def __delitem__(self, priority: int) -> None:
        del self._matchers[priority]
        self.rebuild()

    def __iter__(self) -> Iterator[int]:
        return iter(self._matchers)

    def __len__(self) -> int:
        return len(self._matchers)

    def rebuild(self, *, log: bool = True) -> None:
        self._signature = self._make_signature()
        self._routed_by_priority.clear()
        self._command_index.clear()
        self._prefix_index.clear()
        self._fullmatch_index.clear()
        self._generic_index.clear()

        command_count = 0
        prefix_count = 0
        generic_count = 0

        for priority, matcher_list in self._matchers.items():
            for matcher in matcher_list:
                meta = _MATCHER_ROUTES.get(matcher)
                if meta is None:
                    continue

                self._routed_by_priority.setdefault(priority, set()).add(matcher)
                for command in meta.commands:
                    self._command_index.setdefault(priority, {}).setdefault(
                        command,
                        set(),
                    ).add(matcher)
                    command_count += 1

                for prefix in meta.prefixes:
                    if prefix:
                        self._add_prefix(priority, prefix, matcher)
                        prefix_count += 1

                for value in meta.fullmatches:
                    self._fullmatch_index.setdefault(priority, {}).setdefault(
                        value,
                        set(),
                    ).add(matcher)
                    command_count += 1

                if meta.generic or (
                    not meta.commands and not meta.prefixes and not meta.fullmatches
                ):
                    self._generic_index.setdefault(priority, set()).add(matcher)
                    generic_count += 1

        if log:
            logger.info(
                "[修仙 on_compat] 已索引命令/全匹配 {} 个，前缀/正则 {} 个，通用消息 {} 个",
                command_count,
                prefix_count,
                generic_count,
            )

    def _add_prefix(
        self,
        priority: int,
        prefix: str,
        matcher: type["Matcher"],
    ) -> None:
        entries = self._prefix_index.setdefault(priority, [])
        for existing_prefix, existing_matchers in entries:
            if existing_prefix == prefix:
                existing_matchers.add(matcher)
                return
        entries.append((prefix, {matcher}))
        entries.sort(key=lambda item: len(item[0]), reverse=True)

    def _make_signature(self) -> tuple[tuple[int, int], ...]:
        return tuple(
            (priority, len(matcher_list))
            for priority, matcher_list in sorted(self._matchers.items())
        )

    def _rebuild_if_changed(self) -> None:
        signature = self._make_signature()
        if signature != self._signature:
            self.rebuild()


def rebuild_on_compat_index() -> None:
    provider = getattr(matchers, "provider", None)
    if isinstance(provider, XiuxianOnCompatProvider):
        provider.rebuild()


def _wrap_handle_event(handle_event):
    if getattr(handle_event, "_xiuxian_on_compat_wrapped", False):
        return handle_event

    async def wrapped(bot, event):
        token = _CURRENT_EVENT.set(event)
        try:
            return await handle_event(bot, event)
        finally:
            _CURRENT_EVENT.reset(token)

    wrapped._xiuxian_on_compat_wrapped = True
    wrapped._xiuxian_on_compat_original = handle_event
    return wrapped


def _patch_handle_event() -> None:
    import nonebot.message as nonebot_message

    wrapped = _wrap_handle_event(nonebot_message.handle_event)
    nonebot_message.handle_event = wrapped

    for module_name in (
        "nonebot.adapters.onebot.v11.bot",
        "nonebot.adapters.onebot.v12.bot",
        "nonebot.adapters.qq.bot",
    ):
        try:
            module = importlib.import_module(module_name)
        except Exception:
            module = sys.modules.get(module_name)

        if module is not None and hasattr(module, "handle_event"):
            module.handle_event = wrapped


def install_on_compat() -> None:
    global _INSTALLED
    if _INSTALLED:
        return

    if not isinstance(matchers.provider, XiuxianOnCompatProvider):
        matchers.set_provider(XiuxianOnCompatProvider)
    _patch_handle_event()

    driver = get_driver()

    @driver.on_startup
    async def _refresh_on_compat_index():
        rebuild_on_compat_index()

    _INSTALLED = True


def on(*args, _depth: int = 0, **kwargs):
    install_on_compat()
    matcher = _nb_on(*args, _depth=_depth + 1, **kwargs)
    return _register_route(matcher, _RouteMeta(generic=True))


def on_message(*args, _depth: int = 0, **kwargs):
    install_on_compat()
    matcher = _nb_on_message(*args, _depth=_depth + 1, **kwargs)
    return _register_route(matcher, _RouteMeta(generic=True))


def on_command(
    cmd: CommandInput,
    rule=None,
    aliases: set[CommandValue] | None = None,
    force_whitespace: str | bool | None = None,
    _depth: int = 0,
    **kwargs,
):
    install_on_compat()
    primary, alias_values, commands = _split_commands(cmd, aliases)
    matcher = _nb_on_command(
        primary,
        rule=rule,
        aliases=alias_values,
        force_whitespace=force_whitespace,
        _depth=_depth + 1,
        **kwargs,
    )
    return _register_route(matcher, _RouteMeta(commands=commands))


def on_shell_command(
    cmd: CommandInput,
    rule=None,
    aliases: set[CommandValue] | None = None,
    parser=None,
    _depth: int = 0,
    **kwargs,
):
    install_on_compat()
    primary, alias_values, commands = _split_commands(cmd, aliases)
    matcher = _nb_on_shell_command(
        primary,
        rule=rule,
        aliases=alias_values,
        parser=parser,
        _depth=_depth + 1,
        **kwargs,
    )
    return _register_route(matcher, _RouteMeta(commands=commands))


def on_regex(
    pattern: str,
    flags: int | re.RegexFlag = 0,
    rule=None,
    _depth: int = 0,
    **kwargs,
):
    install_on_compat()
    matcher = _nb_on_regex(pattern, flags=flags, rule=rule, _depth=_depth + 1, **kwargs)
    regex_flags = re.RegexFlag(flags)
    if regex_flags & re.IGNORECASE:
        return _register_route(matcher, _RouteMeta(generic=True))

    prefix = _literal_regex_prefix(pattern)
    if prefix:
        return _register_route(matcher, _RouteMeta(prefixes={prefix}))
    return _register_route(matcher, _RouteMeta(generic=True))


def on_startswith(
    msg: TextValue,
    rule=None,
    ignorecase: bool = False,
    _depth: int = 0,
    **kwargs,
):
    install_on_compat()
    matcher = _nb_on_startswith(
        msg,
        rule=rule,
        ignorecase=ignorecase,
        _depth=_depth + 1,
        **kwargs,
    )
    if ignorecase:
        return _register_route(matcher, _RouteMeta(generic=True))

    return _register_route(matcher, _RouteMeta(prefixes=_normal_text_values(msg)))


def on_fullmatch(
    msg: TextValue,
    rule=None,
    ignorecase: bool = False,
    _depth: int = 0,
    **kwargs,
):
    install_on_compat()
    matcher = _nb_on_fullmatch(
        msg,
        rule=rule,
        ignorecase=ignorecase,
        _depth=_depth + 1,
        **kwargs,
    )
    if ignorecase:
        return _register_route(matcher, _RouteMeta(generic=True))

    return _register_route(matcher, _RouteMeta(fullmatches=_normal_text_values(msg)))


def on_endswith(
    msg: TextValue,
    rule=None,
    ignorecase: bool = False,
    _depth: int = 0,
    **kwargs,
):
    install_on_compat()
    matcher = _nb_on_endswith(
        msg,
        rule=rule,
        ignorecase=ignorecase,
        _depth=_depth + 1,
        **kwargs,
    )
    return _register_route(matcher, _RouteMeta(generic=True))


def on_keyword(keywords: set[str], *args, _depth: int = 0, **kwargs):
    install_on_compat()
    matcher = _nb_on_keyword(keywords, *args, _depth=_depth + 1, **kwargs)
    return _register_route(matcher, _RouteMeta(generic=True))


__all__ = [
    "XiuxianOnCompatProvider",
    "install_on_compat",
    "rebuild_on_compat_index",
    "on",
    "on_message",
    "on_command",
    "on_shell_command",
    "on_regex",
    "on_startswith",
    "on_fullmatch",
    "on_endswith",
    "on_keyword",
]
