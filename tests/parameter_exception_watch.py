"""Observe blanket handler catches that would otherwise hide an exception."""
import ast
from contextlib import contextmanager
from functools import lru_cache
import inspect
from pathlib import Path
import sys
import traceback


@lru_cache(maxsize=128)
def blanket_catches(filename):
    tree = ast.parse(Path(filename).read_text(encoding="utf-8"), filename=filename)
    return {node.body[0].lineno for node in ast.walk(tree)
            if isinstance(node, ast.ExceptHandler) and node.body and (
                node.type is None or any(isinstance(child, ast.Name) and child.id in {"Exception", "BaseException"}
                                         for child in ast.walk(node.type)))}


@contextmanager
def watch_handler_catches(entry):
    from nonebot.exception import MatcherException, ProcessException
    from tests.command_probe import ProbeBlocked

    files = {inspect.unwrap(handler.call).__code__.co_filename for handler in entry["matcher"].handlers}
    lines = {filename: blanket_catches(filename) for filename in files}
    errors, pending, seen = [], {}, set()
    previous = sys.gettrace()

    def trace(frame, event, value):
        filename = frame.f_code.co_filename
        if filename not in files:
            return None
        if event == "exception":
            pending[id(frame)] = value[1]
        elif event == "return":
            pending.pop(id(frame), None)
        elif event == "line" and frame.f_lineno in lines[filename]:
            exc = pending.get(id(frame))
            if exc is not None and id(exc) not in seen and not isinstance(exc, (MatcherException, ProcessException, ProbeBlocked)):
                seen.add(id(exc))
                errors.append({"module": entry["module"], "function": frame.f_code.co_name,
                               "line": frame.f_lineno, "command": entry["command"],
                               "traceback": "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))})
        return trace

    sys.settrace(trace)
    try:
        yield errors
    finally:
        sys.settrace(previous)
