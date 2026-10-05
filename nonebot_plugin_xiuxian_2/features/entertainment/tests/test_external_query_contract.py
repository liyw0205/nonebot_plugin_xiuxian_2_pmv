from __future__ import annotations

import ast
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[4]
ENTERTAINMENT = ROOT / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_entertainment"


def _function(source: str, name: str):
    tree = ast.parse(source)
    return next(
        node
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name == name
    )


def _called_attributes(node: ast.AST) -> set[str]:
    return {
        child.func.attr
        for child in ast.walk(node)
        if isinstance(child, ast.Call) and isinstance(child.func, ast.Attribute)
    }


def _called_names(node: ast.AST) -> set[str]:
    return {
        child.func.id
        for child in ast.walk(node)
        if isinstance(child, ast.Call) and isinstance(child.func, ast.Name)
    }


class EntertainmentExternalQueryContractTests(unittest.TestCase):
    def test_generic_query_helpers_delegate_to_feature_application(self):
        source = (ENTERTAINMENT / "command.py").read_text(encoding="utf-8")
        for name, method in (
            ("_get_json_api_sync", "external_json"),
            ("_get_text_api_sync", "external_text"),
            ("_get_media_url_api_sync", "external_media_url"),
        ):
            self.assertIn(method, _called_attributes(_function(source, name)))

    def test_calendar_fetch_is_offloaded_from_async_handlers(self):
        source = (ENTERTAINMENT / "mod/bangumi_calendar.py").read_text(
            encoding="utf-8"
        )
        self.assertIn(
            "bangumi_seasons_now",
            _called_attributes(_function(source, "fetch_bangumi_calendar")),
        )
        for name in ("today_bangumi_", "week_bangumi_"):
            self.assertIn(
                "run_blocking_io",
                _called_names(_function(source, name)),
            )

    def test_anime_reaction_uses_bounded_feature_owned_reads(self):
        source = (ENTERTAINMENT / "mod/anime_reaction.py").read_text(
            encoding="utf-8"
        )
        node = _function(source, "_fetch_nekos_sync")
        calls = _called_attributes(node)
        self.assertIn("external_json", calls)
        self.assertIn("external_bytes", calls)
        self.assertNotIn("http_client", calls)

    def test_random_video_failover_has_per_source_and_total_deadline(self):
        source = (ENTERTAINMENT / "mod/random_girl_video.py").read_text(
            encoding="utf-8"
        )
        node = _function(source, "_fetch_random_girl_video")
        calls = _called_attributes(node)
        self.assertIn("monotonic", calls)
        self.assertIn("wait_for", calls)
        constants = {
            child.value
            for child in ast.walk(node)
            if isinstance(child, ast.Constant) and isinstance(child.value, (int, float))
        }
        self.assertIn(30.0, constants)
        self.assertIn(8.0, constants)


if __name__ == "__main__":
    unittest.main()
