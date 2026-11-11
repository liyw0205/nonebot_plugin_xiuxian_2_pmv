"""Offline command diagnostics using the live matcher registry and NoneBot DI.

Run in a fresh process: python -m tests.command_probe --report /tmp/report.json.
No driver startup hooks, jobs, Web server or real transport are started.
"""
from __future__ import annotations

import argparse
import asyncio
from collections import Counter
from contextlib import AsyncExitStack
from contextvars import ContextVar
from copy import deepcopy
import inspect
import json
import os
from pathlib import Path
import shutil
import sqlite3
import sys
import time
import traceback
from unittest.mock import patch

import tests

_boundary_denials = ContextVar("probe_boundary_denials", default=None)


def isolate_source():
    if "nonebot_plugin_xiuxian_2" in sys.modules:
        raise AssertionError("command diagnostics require a fresh process")
    source = Path(__file__).resolve().parents[1] / "nonebot_plugin_xiuxian_2"
    destination = tests._data_dir.parent / "command-source" / source.name

    def ignore(directory, names):
        return [name for name in names if name == "__pycache__" or not (Path(directory, name).is_dir() or name.endswith(".py"))]

    shutil.copytree(source, destination, ignore=ignore)
    sys.path.insert(0, str(destination.parent))


class ProbeBlocked(RuntimeError):
    """A dependency deliberately unavailable in the offline diagnostic."""


class ImmediateTrainingWait:
    """Replace only the command module's waiting dependency, not asyncio itself."""

    def __getattr__(self, name):
        return getattr(asyncio, name)

    async def sleep(self, seconds):
        if seconds != 60:
            raise AssertionError(f"unexpected training delay: {seconds}")


def install_boundary(root: Path) -> None:
    root = root.resolve()
    sys.dont_write_bytecode = True

    def audit(event, args):
        def deny(message):
            denials = _boundary_denials.get()
            if denials is not None:
                denials.append(message)
            raise ProbeBlocked(message)

        if event in {"socket.connect", "socket.connect_ex", "socket.bind", "subprocess.Popen", "os.system", "os.exec", "os.fork"}:
            deny(f"offline boundary: {event}")
        target = None
        if event == "open":
            path, mode, flags = args
            writing = bool(flags & (os.O_WRONLY | os.O_RDWR | os.O_CREAT | os.O_TRUNC))
            if writing and not isinstance(path, int):
                target = Path(path)
                if target == Path(os.devnull):
                    return
        elif event == "sqlite3.connect":
            database = str(args[0])
            if database != ":memory:" and not Path(database.removeprefix("file:").split("?", 1)[0]).resolve().is_relative_to(root):
                deny("offline boundary: database outside fixture")
        elif event in {"os.remove", "os.rmdir", "os.mkdir", "os.chmod", "os.rename"}:
            target = Path(args[0])
            if event == "os.mkdir" and target.is_dir():
                return
            dir_fd = args[1] if event in {"os.remove", "os.rmdir"} else None
            if isinstance(dir_fd, int) and dir_fd >= 0 and not target.is_absolute():
                target = Path(os.readlink(f"/proc/self/fd/{dir_fd}")) / target
            if event == "os.rename" and not Path(args[1]).resolve().is_relative_to(root.parent):
                deny("offline boundary: rename outside fixture")
        if target is not None and not target.resolve().is_relative_to(root.parent):
            deny(f"offline boundary: {event} outside fixture: {target}")

    sys.addaudithook(audit)


async def prepare():
    import nonebot

    nonebot.init(command_start={"", "/"}, superusers={"900003"}, log_level="ERROR")
    import nonebot_plugin_xiuxian_2
    from nonebot_plugin_xiuxian_2.bootstrap.context import build_runtime_context
    from nonebot_plugin_xiuxian_2.plugin import startup
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.xiuxian2_handle import (
        PlayerDataManager, TradeDataManager, XIUXIAN_IMPART_BUFF, XiuxianDateManage,
    )

    game = XiuxianDateManage()
    player = PlayerDataManager()
    trade = TradeDataManager()
    impart = XIUXIAN_IMPART_BUFF()
    from nonebot_plugin_xiuxian_2.adapters.nonebot.gateway import NoneBotMessageGateway
    from nonebot_plugin_xiuxian_2.xiuxian.messaging.delivery import delivery_service

    context = build_runtime_context(
        data_dir=tests._data_dir, legacy_startup=False,
        message_gateway=NoneBotMessageGateway(delivery_service),
    )
    state, _, context, lifecycle = await startup(context)
    if state.error:
        raise AssertionError(state.error)
    # Migrated matcher closures resolve the same runtime context as driver startup.
    hook = nonebot.get_driver()._xiuxian_refactored_lifecycle_hooks[0]
    closure = inspect.getclosurevars(hook).nonlocals
    closure["holder"].update(context=context, lifecycle=lifecycle)
    for uid, name in (("900001", "诊断道友"), ("900002", "诊断对手"), ("900003", "诊断管理")):
        game.create_user(uid, "金", "天灵根", 1000, "2026-11-11 00:00:00", name)
    game.conn.execute("UPDATE user_xiuxian SET stone=1000000, exp=100000, hp=1000, mp=1000, atk=100, user_stamina=100")
    game.conn.commit()
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import pet_system

    pet_system._ensure_pet_storage()
    from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_buff

    xiuxian_buff.asyncio = ImmediateTrainingWait()
    # Legacy JSON consumers keep their real implementation but write only fixtures.
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_Illusion.IllusionData import IllusionData
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_base import xiangyuan
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_buff.mentor_exp_cd import mentor_exp_cd
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_impart_pk.impart_pk import impart_pk

    legacy = tests._data_dir / "diagnostic-legacy"
    legacy.mkdir(exist_ok=True)
    IllusionData.DATA_PATH = legacy / "illusion"
    IllusionData.STATS_FILE = IllusionData.DATA_PATH / "illusion_stats.json"
    xiangyuan.XIANGYUAN_DATA_PATH = legacy / "xiangyuan"
    mentor_exp_cd.data_path = legacy / "mentor_exp_cd.json"
    impart_pk.data_path = legacy / "impart_pk.json"
    return context, (game, player, trade, impart)


def inventory():
    from nonebot.matcher import matchers
    from nonebot.rule import CommandRule, FullmatchRule, StartswithRule, RegexRule
    from nonebot_plugin_xiuxian_2.xiuxian.on_compat import _MATCHER_ROUTES, _PRIMARY_COMMAND_NAMES

    result = []
    for priority in sorted(matchers):
        for matcher in matchers[priority]:
            module = matcher.module_name or ""
            if "nonebot_plugin_xiuxian_2" not in module or matcher.type != "message":
                continue
            commands, patterns = set(), []
            for checker in matcher.rule.checkers:
                rule = checker.call
                if isinstance(rule, CommandRule):
                    commands.update(" ".join(parts) for parts in rule.cmds)
                elif isinstance(rule, (FullmatchRule, StartswithRule)):
                    commands.update(rule.msg)
                elif isinstance(rule, RegexRule):
                    patterns.append(rule.regex)
            meta = _MATCHER_ROUTES.get(matcher)
            primary = _PRIMARY_COMMAND_NAMES.get(matcher)
            if not primary and commands:
                primary = min(commands, key=lambda value: (len(value), value))
            if not primary and meta and meta.prefixes:
                primary = max(meta.prefixes, key=len)
            if not primary and patterns:
                primary = patterns[0]
            if not primary:
                continue  # Generic message/notice consumers are not commands.
            handlers = [inspect.unwrap(handler.call) for handler in matcher.handlers]
            line = handlers[0].__code__.co_firstlineno if handlers else 0
            result.append({
                "id": f"{module}:{line}", "command": primary,
                "aliases": sorted(commands - {primary}), "patterns": patterns,
                "module": module, "handlers": [function.__name__ for function in handlers],
                "line": line, "matcher": matcher,
            })
    return sorted(result, key=lambda item: item["id"])


class FixtureSnapshot:
    def __init__(self, root, managers):
        self.root, self.managers = root, managers
        self.databases = {}
        self.json_files = {path: path.read_bytes() for path in root.parent.rglob("*.json")}
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_buff.mentor_exp_cd import mentor_exp_cd
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_impart_pk.impart_pk import impart_pk

        self.json_stores = [(store, deepcopy(store.data)) for store in (mentor_exp_cd, impart_pk)]
        for path in root.rglob("*.db"):
            memory = sqlite3.connect(":memory:")
            with sqlite3.connect(path) as source:
                source.backup(memory)
            self.databases[path] = memory

    def restore(self):
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.lay_out import _shutdown_cooldown_runtimes, limit_all_data

        _shutdown_cooldown_runtimes()
        limit_all_data.clear()
        for path in self.root.parent.rglob("*.json"):
            if path not in self.json_files:
                path.unlink()
        for path, content in self.json_files.items():
            if not path.exists() or path.read_bytes() != content:
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(content)
        for store, data in self.json_stores:
            store.data = deepcopy(data)
        for manager in self.managers:
            manager.conn.rollback()
            for method in ("_clear_read_cache", "_clear_field_list_cache"):
                if callable(getattr(manager, method, None)):
                    getattr(manager, method)()
            for attribute in ("_ensured_tables", "_ensured_fields"):
                if hasattr(manager, attribute):
                    getattr(manager, attribute).clear()
        for path, memory in self.databases.items():
            with sqlite3.connect(path) as target:
                deadline = time.monotonic() + 2

                def progress(status, remaining, total):
                    if time.monotonic() > deadline:
                        raise AssertionError(f"fixture restore blocked: {path.name}")

                memory.backup(target, pages=256, progress=progress)


def make_event(text, uid, number):
    from nonebot.adapters.onebot.v11 import GroupMessageEvent, Message

    return GroupMessageEvent.model_validate({
        "time": 1794412800, "self_id": 900000, "post_type": "message", "sub_type": "normal",
        "message_type": "group", "group_id": 900100, "user_id": int(uid),
        "message_id": number, "message": Message(text), "original_message": Message(text),
        "raw_message": text, "font": 0, "to_me": True,
        "sender": {"user_id": int(uid), "nickname": "诊断道友", "role": "owner"},
    })


async def run_case(entry, scenario, number, *, input_text=None, actor=None, timeout=8):
    import nonebot
    from nonebot.adapters.onebot.v11 import Adapter, Bot
    from nonebot.exception import StopPropagation
    from nonebot.log import logger
    from nonebot.matcher import matchers
    from nonebot.message import check_and_run_matcher
    from nonebot.rule import TrieRule
    from nonebot_plugin_xiuxian_2.xiuxian.on_compat import _CURRENT_EVENT
    from nonebot_plugin_xiuxian_2.infrastructure.messaging import MessageGatewayAdapter

    uid = "900099" if scenario == "unregistered_player" else "900001"
    if scenario == "admin_missing":
        uid = "900003"
    uid = actor or uid
    argument = " 1" if scenario == "argument" else ""
    text = input_text if input_text is not None else entry["command"] + argument
    event = make_event(text, uid, number)
    bot = Bot(Adapter(nonebot.get_driver()), "900000")
    replies, errors, logs, blocked = [], [], [], []

    async def call_api(api, **data):
        if api.startswith("send_"):
            replies.append(str(data.get("message", data)))
            return {"message_id": number}
        if api in {"get_group_info", "get_group_member_info", "get_stranger_info"}:
            return {"group_id": 900100, "user_id": int(uid), "nickname": "诊断道友", "card": "", "role": "owner", "group_name": "诊断群"}
        blocked.append(f"Bot API fixture missing: {api}")
        raise ProbeBlocked(blocked[-1])

    def capture(message):
        record = message.record
        if record["level"].no >= 30:
            logs.append({"message": record["message"], "exception": str(record["exception"] or "")})
        if record["exception"]:
            exc = record["exception"].value
            if isinstance(exc, ProbeBlocked):
                blocked.append(str(exc))
            elif isinstance(exc, Exception):
                errors.append("".join(traceback.format_exception(type(exc), exc, exc.__traceback__)))
        elif "offline boundary:" in record["message"]:
            blocked.append(record["message"])
        elif record["level"].no >= 40 or "name '" in record["message"] and "is not defined" in record["message"]:
            errors.append("error log without traceback: " + record["message"])

    sink = logger.add(capture, level="WARNING")
    state = {}
    token = _CURRENT_EVENT.set(event)
    boundary_token = _boundary_denials.set(blocked)
    matched = False
    prior_tasks = asyncio.all_tasks()
    loop = asyncio.get_running_loop()
    previous_factory = loop.get_task_factory()
    created_tasks = []

    def track_task(loop, coroutine, **kwargs):
        task = previous_factory(loop, coroutine, **kwargs) if previous_factory else asyncio.Task(coroutine, loop=loop, **kwargs)
        created_tasks.append(task)
        return task

    loop.set_task_factory(track_task)

    async def transport(self, api, **data):
        return await call_api(api, **data)

    gateway_send = MessageGatewayAdapter.send

    async def checked_gateway(self, context, reply):
        result = await gateway_send(self, context, reply)
        if not result.ok:
            errors.append(f"message gateway rejected delivery: {result.error_code}; actor={uid}; command={entry['command']}")
        return result

    try:
        TrieRule.get_value(bot, event, state)
        candidates = matchers[entry["matcher"].priority]
        if entry["matcher"] not in candidates:
            return {"scenario": scenario, "status": "blocked", "reason": "route provider did not select matcher", "input": text}
        async with AsyncExitStack() as stack:
            # Keep the real permission, rule, dependencies, matcher and handler path.
            with patch.object(type(bot), "call_api", transport), patch.object(MessageGatewayAdapter, "send", checked_gateway):
                from nonebot.message import _check_matcher
                matched = await _check_matcher(entry["matcher"], bot, event, state, stack, {})
                if matched:
                    try:
                        await asyncio.wait_for(check_and_run_matcher(entry["matcher"], bot, event, state, stack, {}), timeout=timeout)
                    except StopPropagation:
                        pass
                pending = asyncio.all_tasks() - prior_tasks
                if pending:
                    _, unfinished = await asyncio.wait(pending, timeout=0.5)
                    if unfinished:
                        blocked.append("background dependency did not finish within 0.5 seconds")
                        for task in unfinished:
                            task.cancel()
                        await asyncio.gather(*unfinished, return_exceptions=True)
    except StopPropagation:
        pass
    except ProbeBlocked as exc:
        blocked.append(str(exc))
    except Exception as exc:
        errors.append("".join(traceback.format_exception(type(exc), exc, exc.__traceback__)))
    finally:
        loop.set_task_factory(previous_factory)
        for task in created_tasks:
            # asyncio clears this flag when an awaiter retrieves the exception.
            # Keep task references until here so detached failures cannot escape
            # this case through delayed garbage collection and lose attribution.
            if task.done() and not task.cancelled() and getattr(task, "_log_traceback", False):
                exc = task.exception()
                if isinstance(exc, ProbeBlocked):
                    blocked.append(str(exc))
                elif exc is not None:
                    from nonebot_plugin_xiuxian_2.xiuxian.command_guard import report_exception

                    report_exception(entry["command"], exc, phase="未捕获后台异常", event=event)
        _CURRENT_EVENT.reset(token)
        _boundary_denials.reset(boundary_token)
        logger.remove(sink)
    status = "exception" if errors else "blocked" if blocked or not matched or not replies else "pass"
    return {"scenario": scenario, "input": text, "actor": uid, "status": status,
            "reason": "permission/rule rejected" if not matched else "no outgoing reply" if not replies else "",
            "replies": replies, "errors": errors, "blocked": blocked, "logs": logs}


def command_key(entry):
    return entry["module"], entry["command"]


async def check_routes(entries):
    import nonebot
    from nonebot.adapters.onebot.v11 import Adapter, Bot
    from nonebot.matcher import matchers
    from nonebot.message import _check_matcher
    from nonebot.rule import TrieRule
    from nonebot_plugin_xiuxian_2.xiuxian.on_compat import _CURRENT_EVENT

    bot = Bot(Adapter(nonebot.get_driver()), "900000")
    rows = []
    for entry in entries:
        for name in [entry["command"], *entry["aliases"]]:
            if entry["patterns"]:
                continue  # Regex metadata is not a valid example input.
            event = make_event(name, "900003", 800001)
            token = _CURRENT_EVENT.set(event)
            try:
                state = {}
                TrieRule.get_value(bot, event, state)
                selected = entry["matcher"] in matchers[entry["matcher"].priority]
                rows.append({"module": entry["module"], "command": entry["command"], "input": name, "selected": selected})
            finally:
                _CURRENT_EVENT.reset(token)
    event = make_event("__xiuxian_unregistered_command_900099__", "900001", 800002)
    token = _CURRENT_EVENT.set(event)
    unknown = []
    try:
        state = {}
        TrieRule.get_value(bot, event, state)
        async with AsyncExitStack() as stack:
            for entry in entries:
                if entry["matcher"] in matchers[entry["matcher"].priority] and await _check_matcher(entry["matcher"], bot, event, state, stack, {}):
                    unknown.append({"module": entry["module"], "command": entry["command"]})
    finally:
        _CURRENT_EVENT.reset(token)
    return {"routes": rows, "unknown_command_matches": unknown}


async def probe(args, checkpoint):
    root = tests._data_dir.resolve()
    # The report lives inside this fixture until the process boundary is removed.
    context, managers = await prepare()
    entries = inventory()
    if args.routes_only:
        routes = await check_routes(entries)
        failures = sum(not row["selected"] for row in routes["routes"]) + len(routes["unknown_command_matches"])
        return {"commands": len(entries), "summary": {"exception": failures}, **routes}
    retries = {}
    if args.retry_from:
        previous = json.loads(args.retry_from.read_text(encoding="utf-8"))
        retries = {
            command_key(row): [case["scenario"] for case in row["cases"] if case["status"] == "exception" or case.get("blocked")]
            for row in previous["results"]
        }
        entries = [entry for entry in entries if retries.get(command_key(entry))]
    if args.command:
        entries = [entry for entry in entries if entry["command"] in args.command]
        if not entries:
            raise AssertionError("requested commands are absent from live registry")
    snapshot = FixtureSnapshot(root, managers)
    results = []
    for entry in entries:
        row = {key: value for key, value in entry.items() if key != "matcher"}
        row["cases"] = []
        if not args.inventory_only:
            for scenario in args.scenario or retries.get(command_key(entry)) or ("missing", "unregistered_player", "argument", "admin_missing"):
                snapshot.restore()
                row["cases"].append(await run_case(entry, scenario, len(results) * 10 + len(row["cases"]) + 1))
        results.append(row)
        checkpoint({"scope": "live registration/provider/permission/rule/DI/handler; mock OneBot transport; no driver jobs or event preprocessors", "commands": len(entries), "completed": len(results), "summary": dict(Counter(case["status"] for item in results for case in item["cases"])), "results": results})
        if len(results) % 25 == 0:
            print(f"completed {len(results)}/{len(entries)}", flush=True)
    summary = Counter(case["status"] for row in results for case in row["cases"])
    return {"scope": "live registration/provider/permission/rule/DI/handler; mock OneBot transport; no driver jobs or event preprocessors", "commands": len(results), "summary": dict(summary), "results": results}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--inventory-only", action="store_true")
    parser.add_argument("--routes-only", action="store_true")
    parser.add_argument("--command", action="append")
    parser.add_argument("--retry-from", type=Path)
    parser.add_argument("--scenario", action="append", choices=("missing", "unregistered_player", "argument", "admin_missing"))
    args = parser.parse_args()
    # Deny transports/processes before plugin import. Report output is opened first.
    with args.report.open("w", encoding="utf-8") as report:
        isolate_source()
        install_boundary(tests._data_dir)

        def checkpoint(value):
            report.seek(0)
            json.dump(value, report, ensure_ascii=False, indent=2)
            report.write("\n")
            report.truncate()
            report.flush()

        result = asyncio.run(probe(args, checkpoint))
        checkpoint(result)
        print(json.dumps({key: value for key, value in result.items() if key not in {"results", "routes"}}, ensure_ascii=False))
        return 1 if result["summary"].get("exception") else 0


if __name__ == "__main__":
    raise SystemExit(main())
