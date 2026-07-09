from __future__ import annotations

import ast
import unittest
from pathlib import Path


SOURCE_ROOT = Path(__file__).resolve().parents[1] / "nonebot_plugin_xiuxian_2"


class SourceQualityTests(unittest.TestCase):
    def test_python_sources_do_not_use_bare_except(self) -> None:
        violations: list[str] = []
        for path in SOURCE_ROOT.rglob("*.py"):
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            for node in ast.walk(tree):
                if isinstance(node, ast.ExceptHandler) and node.type is None:
                    violations.append(f"{path.relative_to(SOURCE_ROOT)}:{node.lineno}")

        self.assertEqual(violations, [], "Bare except clauses found: " + ", ".join(violations))

    def test_business_sources_use_centralized_data_paths(self) -> None:
        violations: list[str] = []
        forbidden_fragments = (
            'Path() / "data" / "xiuxian"',
            "Path() / 'data' / 'xiuxian'",
        )

        for path in SOURCE_ROOT.rglob("*.py"):
            relative_path = path.relative_to(SOURCE_ROOT)
            if relative_path == Path("paths.py") or "vendor" in relative_path.parts:
                continue
            source = path.read_text(encoding="utf-8")
            if any(fragment in source for fragment in forbidden_fragments):
                violations.append(str(relative_path))

        self.assertEqual(
            violations,
            [],
            "Direct data/xiuxian path construction found: " + ", ".join(violations),
        )

    def test_plugin_import_does_not_run_startup_maintenance(self) -> None:
        entrypoint = SOURCE_ROOT / "xiuxian" / "__init__.py"
        source = entrypoint.read_text(encoding="utf-8")
        forbidden_calls = (
            "ensure_plugin_dependencies()",
            "download_xiuxian_data()",
            "initialize_backend()",
            "_run_startup_database_maintenance()",
        )

        violations = [call for call in forbidden_calls if call in source]
        self.assertEqual(
            violations,
            [],
            "Import-time startup maintenance found: " + ", ".join(violations),
        )

    def test_web_server_is_not_started_during_import(self) -> None:
        web_entrypoint = SOURCE_ROOT / "xiuxian" / "xiuxian_web" / "__init__.py"
        tree = ast.parse(
            web_entrypoint.read_text(encoding="utf-8"),
            filename=str(web_entrypoint),
        )
        module_level_starts = [
            node.lineno
            for node in tree.body
            if isinstance(node, ast.Expr)
            and isinstance(node.value, ast.Call)
            and isinstance(node.value.func, ast.Attribute)
            and node.value.func.attr in {"run", "start", "serve_forever"}
        ]

        self.assertEqual(
            module_level_starts,
            [],
            f"Web server starts during import at lines: {module_level_starts}",
        )

    def test_web_modules_do_not_use_core_star_imports(self) -> None:
        web_root = SOURCE_ROOT / "xiuxian" / "xiuxian_web"
        violations = []
        for path in web_root.glob("*.py"):
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            if any(
                isinstance(node, ast.ImportFrom)
                and node.module == "core"
                and any(alias.name == "*" for alias in node.names)
                for node in tree.body
            ):
                violations.append(path.name)

        self.assertEqual(
            violations,
            [],
            "Web modules importing core with *: " + ", ".join(violations),
        )


if __name__ == "__main__":
    unittest.main()
