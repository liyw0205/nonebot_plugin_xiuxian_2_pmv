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
        self.assertIn("get_user_profile(user_id_to_check)", boundary)
        self.assertNotIn("_sql_message().get_user_info_with_id", boundary)
        repository = (root / "nonebot_plugin_xiuxian_2" / "features" / "info" / "profile_repository.py").read_text(encoding="utf-8")
        self.assertIn("read_only=True", repository)

    def test_base_handlers_use_profile_reader_for_lookup_only(self) -> None:
        root = Path(__file__).resolve().parents[1]
        source = (root / "nonebot_plugin_xiuxian_2" / "xiuxian" / "xiuxian_base" / "__init__.py").read_text(encoding="utf-8")
        self.assertIn("get_user_profile_by_name", source)
        self.assertIn("get_user_profile(give_qq)", source)
        self.assertNotIn("_sql_message().get_user_info_with_id", source)
        self.assertNotIn("_sql_message().get_user_info_with_name", source)

    def test_info_projection_uses_profile_reader_for_identity_fields(self) -> None:
        root = Path(__file__).resolve().parents[1]
        source = (root / "nonebot_plugin_xiuxian_2" / "xiuxian" / "xiuxian_info" / "user_info.py").read_text(encoding="utf-8")
        self.assertGreaterEqual(source.count("get_user_profile("), 4)
        self.assertNotIn("_sql_message().get_user_real_info", source)
        self.assertIn("get_final_attributes(user_id)", source)


if __name__ == "__main__":
    unittest.main()
