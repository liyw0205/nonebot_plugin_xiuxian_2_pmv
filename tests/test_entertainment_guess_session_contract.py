from __future__ import annotations

import ast
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
NUMBER = ROOT / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_entertainment/mod/guess_number.py"
PUZZLE = ROOT / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_entertainment/mod/guess_number_puzzle.py"


def _handler(path: Path, matcher: str) -> ast.AsyncFunctionDef:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    for node in tree.body:
        if not isinstance(node, ast.AsyncFunctionDef):
            continue
        for decorator in node.decorator_list:
            if (
                isinstance(decorator, ast.Call)
                and isinstance(decorator.func, ast.Attribute)
                and decorator.func.attr == "handle"
                and isinstance(decorator.func.value, ast.Name)
                and decorator.func.value.id == matcher
            ):
                return node
    raise AssertionError(f"handler for {matcher} was not found in {path.name}")


def _names(node: ast.AST) -> set[str]:
    return {child.id for child in ast.walk(node) if isinstance(child, ast.Name)}


def test_guess_handlers_use_feature_sessions_and_do_not_keep_session_dicts():
    number = NUMBER.read_text(encoding="utf-8")
    puzzle = PUZZLE.read_text(encoding="utf-8")
    assert "guess_number_sessions" not in number
    assert "guess_puzzle_sessions" not in puzzle
    assert "entertainment_application.guess_sessions" in number
    assert "entertainment_application.guess_sessions" in puzzle

    for matcher in (
        "guess_number_start_cmd",
        "guess_number_guess_cmd",
        "guess_number_info_cmd",
    ):
        names = _names(_handler(NUMBER, matcher))
        assert "_read_session" in names
    assert "entertainment_application" in _names(_handler(NUMBER, "guess_number_end_cmd"))

    puzzle_handler = _names(_handler(PUZZLE, "guess_puzzle_cmd"))
    start_handler = _names(_handler(PUZZLE, "guess_puzzle_start_cmd"))
    assert {"_show_status", "_start_game", "_handle_guess"} <= puzzle_handler
    assert "_start_game" in start_handler
    tree = ast.parse(PUZZLE.read_text(encoding="utf-8"))
    helpers = {
        item.name: item
        for item in tree.body
        if isinstance(item, ast.AsyncFunctionDef)
    }
    assert "run_blocking_io" in _names(helpers["_read_session"])
    assert "run_blocking_io" in _names(helpers["_start_game"])
    assert "_read_session" in _names(helpers["_show_status"])
    assert "run_blocking_io" in _names(helpers["_handle_guess"])
    assert "run_blocking_io" in _names(helpers["_reveal_answer"])
