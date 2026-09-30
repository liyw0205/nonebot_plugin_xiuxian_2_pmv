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
        self.assertIn("get_player_attributes(user_id)", source)

    def test_dynamic_attribute_reads_use_feature_application_boundary(self) -> None:
        root = Path(__file__).resolve().parents[1]
        sources = [
            root / "nonebot_plugin_xiuxian_2" / "xiuxian" / "xiuxian_info" / "user_info.py",
            root / "nonebot_plugin_xiuxian_2" / "xiuxian" / "xiuxian_buff" / "__init__.py",
            root / "nonebot_plugin_xiuxian_2" / "xiuxian" / "xiuxian_utils" / "player_fight.py",
            root / "nonebot_plugin_xiuxian_2" / "xiuxian" / "xiuxian_utils" / "xiuxian_json_config.py",
        ]
        for path in sources:
            source = path.read_text(encoding="utf-8")
            self.assertIn("get_player_attributes", source)
            self.assertNotIn("get_final_attributes", source)

        application = (
            root / "nonebot_plugin_xiuxian_2" / "features" / "info" / "attribute_application.py"
        ).read_text(encoding="utf-8")
        compatibility = (
            root / "nonebot_plugin_xiuxian_2" / "compatibility" / "legacy_player_attributes.py"
        ).read_text(encoding="utf-8")
        self.assertIn("PlayerAttributeApplication", application)
        self.assertIn("legacy_get_final_attributes", compatibility)

    def test_activity_timestamp_uses_feature_owned_application(self) -> None:
        root = Path(__file__).resolve().parents[1]
        source = (root / "nonebot_plugin_xiuxian_2" / "xiuxian" / "xiuxian_utils" / "utils.py").read_text(encoding="utf-8")
        start = source.index("def update_last_check_info_time(")
        end = source.index("def _player_data_manager", start)
        boundary = source[start:end]
        self.assertIn("_player_activity().update_last_check_info_time", boundary)
        self.assertIn("_player_activity().get_last_check_info_time", boundary)
        self.assertNotIn("_sql_message().update_last_check_info_time", boundary)


if __name__ == "__main__":
    unittest.main()
