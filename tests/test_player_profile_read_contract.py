from __future__ import annotations

import unittest
from pathlib import Path


class PlayerProfileReadContractTest(unittest.TestCase):
    def test_check_user_uses_feature_owned_profile_reader(self) -> None:
        root = Path(__file__).resolve().parents[1]
        source = (root / "nonebot_plugin_xiuxian_2" / "xiuxian" / "xiuxian_utils" / "utils.py").read_text(encoding="utf-8")
        start = source.index("def check_user(")
        end = source.index("def get_impersonating_target", start)
        boundary = source[start:end]
        self.assertIn("_player_profile().get_user_profile", boundary)
        self.assertNotIn("_sql_message().get_user_info_with_id", boundary)
        repository = (root / "nonebot_plugin_xiuxian_2" / "features" / "info" / "profile_repository.py").read_text(encoding="utf-8")
        self.assertIn("read_only=True", repository)


if __name__ == "__main__":
    unittest.main()
