from __future__ import annotations

import ast
import unittest
from pathlib import Path
from unittest.mock import Mock, patch


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

    def test_map_status_lookup_uses_read_only_boundary(self) -> None:
        root = Path(__file__).resolve().parents[1]
        repository = (
            root / "nonebot_plugin_xiuxian_2" / "features" / "map" / "repository.py"
        ).read_text(encoding="utf-8")
        start = repository.index("class MapStatusSqlQueryRepository:")
        end = repository.index("class MapStatusSqlWriteRepository:", start)
        self.assertIn(
            "DatabaseUnitOfWork(self.player_database, read_only=True)",
            repository[start:end],
        )

    def test_check_user_reads_blackhouse_once_for_same_effective_identity(self) -> None:
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import utils
        from nonebot_plugin_xiuxian_2.xiuxian import blackhouse

        avatar = Mock()
        avatar.get_active_user_id.return_value = "same-user"
        profile = {"user_id": "same-user", "is_ban": 0}
        with (
            patch.object(utils, "_player_avatar", return_value=avatar),
            patch.object(utils, "get_impersonating_target", return_value=None),
            patch.object(utils, "get_user_profile", return_value=profile),
            patch.object(blackhouse, "is_users_blackhoused", return_value={"same-user": False}) as lookup,
        ):
            result = utils.check_user("same-user")

        self.assertEqual(result, (True, profile, ""))
        lookup.assert_called_once_with(("same-user",))

    def test_check_user_still_checks_original_identity_when_avatar_differs(self) -> None:
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import utils
        from nonebot_plugin_xiuxian_2.xiuxian import blackhouse

        avatar = Mock()
        avatar.get_active_user_id.return_value = "active-avatar"
        profile = {"user_id": "active-avatar", "is_ban": 0}
        with (
            patch.object(utils, "_player_avatar", return_value=avatar),
            patch.object(utils, "get_impersonating_target", return_value=None),
            patch.object(utils, "get_user_profile", return_value=profile),
            patch.object(
                blackhouse,
                "is_users_blackhoused",
                return_value={"active-avatar": False, "main-account": True},
            ) as lookup,
        ):
            result = utils.check_user("main-account")

        self.assertFalse(result[0])
        self.assertIn("小黑屋", result[2])
        self.assertEqual(
            [call.args[0] for call in lookup.call_args_list],
            [("active-avatar", "main-account")],
        )

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
        tree = ast.parse(source)
        calls = [node for node in ast.walk(tree)
                 if isinstance(node, ast.Call) and ast.unparse(node.func) == "get_player_attributes"]
        self.assertEqual(len(calls), 1)
        self.assertEqual([ast.unparse(arg) for arg in calls[0].args], ["user_id"])
        self.assertEqual([(kw.arg, ast.unparse(kw.value)) for kw in calls[0].keywords],
                         [(None, "attribute_providers")])
        providers = next(node.value for node in ast.walk(tree)
                         if isinstance(node, ast.Assign)
                         and any(isinstance(target, ast.Name) and target.id == "attribute_providers"
                                 for target in node.targets))
        self.assertIsInstance(providers, ast.Dict)
        self.assertEqual({ast.literal_eval(key): ast.unparse(value)
                          for key, value in zip(providers.keys, providers.values)}, {
            "buff_data_provider": "user_buff_data",
            "base_provider": "lambda _user_id: _base_attributes_from_profile(user_info)",
        })

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
        buff_source = sources[1].read_text(encoding="utf-8")
        self.assertNotIn("get_user_real_info(", buff_source)

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

    def test_legacy_search_route_delegates_to_profile_application(self) -> None:
        root = Path(__file__).resolve().parents[1]
        source = (root / "nonebot_plugin_xiuxian_2" / "xiuxian" / "xiuxian_web" / "system.py").read_text(encoding="utf-8")
        start = source.index("def search_users():")
        end = source.index("@app.route('/download/", start)
        boundary = source[start:end]
        self.assertIn("search_users_application(query)", boundary)
        self.assertIn("return jsonify([])", boundary)
        self.assertNotIn("execute_sql(DATABASE", boundary)


if __name__ == "__main__":
    unittest.main()
