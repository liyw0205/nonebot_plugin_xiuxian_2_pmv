from __future__ import annotations

import importlib.util
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

MODULE_PATH = Path(__file__).resolve().parents[1] / "nonebot_plugin_xiuxian_2" / "paths.py"
SPEC = importlib.util.spec_from_file_location("xiuxian_paths", MODULE_PATH)
if SPEC is None or SPEC.loader is None:  # pragma: no cover
    raise RuntimeError(f"Unable to load {MODULE_PATH}")
paths_module = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = paths_module
SPEC.loader.exec_module(paths_module)

configure_paths = paths_module.configure_paths
configure_paths_from_nonebot = paths_module.configure_paths_from_nonebot
get_paths = paths_module.get_paths
reset_paths_for_test = paths_module.reset_paths_for_test


class XiuxianPathsTests(unittest.TestCase):
    def tearDown(self) -> None:
        reset_paths_for_test()

    def test_default_uses_current_working_directory(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            with patch.dict(os.environ, {}, clear=False), patch.object(Path, "cwd", return_value=Path(directory)):
                os.environ.pop("XIUXIAN_DATA_DIR", None)
                reset_paths_for_test()

                paths = get_paths()

        self.assertEqual(paths.data, Path(directory) / "data" / "xiuxian")

    def test_environment_override_is_resolved(self) -> None:
        with tempfile.TemporaryDirectory() as directory, patch.dict(
            os.environ,
            {"XIUXIAN_DATA_DIR": directory},
        ):
            reset_paths_for_test()

            paths = get_paths()

        self.assertEqual(paths.data, Path(directory).resolve())
        self.assertEqual(paths.game_db, Path(directory).resolve() / "xiuxian.db")
        self.assertEqual(paths.message_db, Path(directory).resolve() / "message.db")

    def test_explicit_configuration_wins_over_environment(self) -> None:
        with tempfile.TemporaryDirectory() as explicit, tempfile.TemporaryDirectory() as environment:
            with patch.dict(os.environ, {"XIUXIAN_DATA_DIR": environment}):
                paths = configure_paths(explicit)

        self.assertEqual(paths.data, Path(explicit).resolve())
        self.assertEqual(paths.backups, Path(explicit).resolve() / "backups")

    def test_nonebot_configuration_is_used(self) -> None:
        class Config:
            xiuxian_data_dir: str

            def __init__(self, data_dir: str) -> None:
                self.xiuxian_data_dir = data_dir

        with tempfile.TemporaryDirectory() as directory:
            paths = configure_paths_from_nonebot(Config(directory))

        self.assertEqual(paths.data, Path(directory).resolve())
        self.assertEqual(paths.players, Path(directory).resolve() / "players")
