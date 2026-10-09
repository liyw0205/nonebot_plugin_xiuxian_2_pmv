"""Gate-accuracy regressions for ``scripts/check_architecture.py``.

These checks read source text only, so they run without NoneBot, Flask or SQLite.
"""

from __future__ import annotations

import ast
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

import check_architecture as gates  # noqa: E402

PACKAGE = ROOT / "nonebot_plugin_xiuxian_2"


def _function(source: str) -> ast.FunctionDef:
    tree = ast.parse(source)
    return next(node for node in tree.body if isinstance(node, ast.FunctionDef))


class ReadOnlyDeclarationTests(unittest.TestCase):
    def test_declarations_are_read_from_module_and_class_bodies(self) -> None:
        tree = ast.parse(
            "READ_ONLY_METHODS = ('a',)\n"
            "class App:\n"
            "    READ_ONLY_METHODS = ['c']\n"
        )
        self.assertEqual(gates._read_only_declarations(tree), {"a", "c"})

    def test_read_only_unit_of_work_is_not_mutating(self) -> None:
        node = _function(
            "def reconcile(self, operation_id):\n"
            "    with DatabaseUnitOfWork(self.database, read_only=True) as uow:\n"
            "        return uow.query_one('SELECT payload FROM receipts WHERE operation_id=?', (operation_id,))\n"
        )
        self.assertEqual(gates._mutating_evidence(node), [])

    def test_write_evidence_is_reported_for_declared_read_only_method(self) -> None:
        immediate = _function(
            "def reconcile(self):\n"
            "    with DatabaseUnitOfWork(self.database, immediate=True) as uow:\n"
            "        return uow\n"
        )
        self.assertEqual(gates._mutating_evidence(immediate), ["immediate unit of work"])

        ledger = _function(
            "def reconcile(self, uow):\n"
            "    self.ledger.begin(uow, 'op', 'action', {})\n"
        )
        self.assertEqual(gates._mutating_evidence(ledger), ["call begin"])

        sql = _function(
            "def reconcile(self, uow):\n"
            "    return uow.execute('UPDATE player SET stamina=? WHERE user_id=?', (1, 'u'))\n"
        )
        self.assertEqual(gates._mutating_evidence(sql), ["write sql"])


class FeatureConnectionRuleTests(unittest.TestCase):
    def test_feature_tree_opens_no_connection_and_imports_no_framework(self) -> None:
        # The last hand-rolled connections (arena candidates, message recall and
        # QQID batches) moved to the shared unit of work, so this rule is now a
        # tree-wide invariant rather than a list of still-visible owners.
        self.assertEqual(gates.check_feature_connections(), [])

    def test_connection_creation_and_framework_imports_are_still_reported(self) -> None:
        # The tree is clean, so the rule itself is proven on a scratch package:
        # opening a connection or importing a framework is reported, driver
        # constants alone are not.
        with tempfile.TemporaryDirectory(prefix="arch-connections-") as directory:
            root = Path(directory)
            feature = root / "features" / "demo"
            feature.mkdir(parents=True)
            (feature / "repository.py").write_text(
                "import sqlite3\n\nCONNECTION = sqlite3.connect('demo.db')\n", encoding="utf-8"
            )
            (feature / "matcher.py").write_text("from nonebot import on_command\n", encoding="utf-8")
            (feature / "cache.py").write_text(
                "import sqlite3\n\nMAX_BLOB = sqlite3.SQLITE_TOOBIG\n", encoding="utf-8"
            )
            with patch.object(gates, "PACKAGE", root), patch.object(gates, "ROOT", root):
                reported = gates.check_feature_connections()
        self.assertEqual(
            sorted(error.split(" ", 1)[0] for error in reported),
            ["features/demo/matcher.py", "features/demo/repository.py"],
        )

    def test_driver_exception_imports_are_not_reported(self) -> None:
        # These repositories only use sqlite3 error/limit constants and take their
        # connection from the shared unit of work.
        for relative in (
            "features/info/avatar_repository.py",
            "features/title/repository.py",
            "features/tasks/progress.py",
        ):
            reported = [error for error in gates.check_feature_connections() if relative in error]
            self.assertEqual(reported, [], relative)

    def test_feature_layer_does_not_import_the_message_framework(self) -> None:
        for path in (PACKAGE / "features").rglob("*.py"):
            if "tests" in path.parts:
                continue
            roots = {name.split(".", 1)[0] for name in gates._imports(path)}
            self.assertNotIn("nonebot", roots, str(path.relative_to(ROOT)))


class FacadeContractTests(unittest.TestCase):
    def test_read_only_facades_declare_their_methods(self) -> None:
        expectations = {
            "features/pet/application.py": "reconcile_travel_claim_operation",
            "features/map/application.py": "reconcile_mission_claim_operation",
            "features/boss/application.py": "weekly_purchases",
            "features/compensation/application.py": "compensation_claimed_data",
        }
        for relative, method in expectations.items():
            tree = ast.parse((PACKAGE / relative).read_text(encoding="utf-8"))
            self.assertIn(method, gates._read_only_declarations(tree), relative)

    def test_daily_reset_accepts_caller_supplied_operation_id(self) -> None:
        tree = ast.parse((PACKAGE / "features/beg/application.py").read_text(encoding="utf-8"))
        method = next(
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.FunctionDef) and node.name == "reset_daily_claim_flag"
        )
        names = {argument.arg for argument in method.args.args + method.args.kwonlyargs}
        self.assertIn("operation_id", names)

    def test_legacy_entrypoints_still_dispatch_through_applications(self) -> None:
        self.assertEqual(gates.check_migrated_legacy_entrypoints(), [])


class ActivityImportCycleTests(unittest.TestCase):
    def test_feature_migrations_import_before_framework_init(self) -> None:
        """A fresh interpreter must import the slice without touching NoneBot."""
        script = (
            "import sys;"
            f"sys.path.insert(0, {str(ROOT)!r});"
            "import nonebot_plugin_xiuxian_2.features.activity.migrations as migrations;"
            "print('ok', bool(migrations))"
        )
        completed = subprocess.run(
            [sys.executable, "-B", "-c", script],
            cwd=ROOT,
            capture_output=True,
            text=True,
            timeout=180,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr[-2000:])
        self.assertIn("ok True", completed.stdout)


if __name__ == "__main__":
    unittest.main()
