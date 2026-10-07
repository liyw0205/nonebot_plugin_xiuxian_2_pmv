from __future__ import annotations

import ast
import unittest
from pathlib import Path

from scripts.phase2_legacy_path_gate import load_phase2_scope_report


ROOT = Path(__file__).resolve().parents[1]
OWNER_TEST = "tests/test_info_frozen_command_owner.py"


def _handler_line(source: str, handler_name: str) -> int:
    tree = ast.parse((ROOT / source).read_text(encoding="utf-8"))
    return next(
        node.lineno
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name == handler_name
    )


class InfoFrozenCommandOwnerTests(unittest.TestCase):
    def test_frozen_commands_have_reviewed_owner_or_compatibility_edges(self) -> None:
        report = load_phase2_scope_report(include_items=True)
        items = {item["id"]: item for item in report["items"]}
        expected = {
            "command:info:我的修仙信息": (
                "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_info/user_info.py",
                "xiuxian_message_",
                "允许保留的兼容路径",
                "shared get_user_xiuxian_info projection -> PlayerProfileApplication, TitleApplication, and PlayerActivityApplication wrappers; legacy attribute/rank/root/sect/buff/relationship/natal readers remain",
            ),
            "command:info:我的修仙信息图片版": (
                "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_info/user_info.py",
                "xiuxian_message_img_",
                "允许保留的兼容路径",
                "shared get_user_xiuxian_info projection -> PlayerProfileApplication, TitleApplication, and PlayerActivityApplication wrappers; legacy attribute/rank/root/sect/buff/relationship/natal readers remain",
            ),
            "command:info:身外化身": (
                "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_info/avatar.py",
                "avatar_switch_cmd_",
                "已迁移",
                "get_user_profile/PlayerProfileApplication for main/avatar identity reads -> init_avatar_if_needed/toggle_avatar -> PlayerAvatarApplication",
            ),
            "command:info:更新日志": (
                "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_info/changelog_command.py",
                "changelog_",
                "允许保留的兼容路径",
                "get_commits/create_changelog_image via asyncio.to_thread -> handle_pic_send",
            ),
        }
        self.assertEqual(
            {item_id for item_id in items if item_id in expected},
            set(expected),
        )
        for item_id, (source, handler_name, status, owner_edge) in expected.items():
            with self.subTest(item_id=item_id):
                item = items[item_id]
                self.assertEqual(item["status"], status)
                self.assertNotIn("unknown_edge", item)
                location = f"{source}:{_handler_line(source, handler_name)}"
                edges = [
                    str(edge)
                    for edge in item["call_graph"]
                    if str(edge).startswith(f"{location} {handler_name} ->")
                ]
                self.assertEqual(len(edges), 1)
                self.assertIn(owner_edge, edges[0])
                self.assertNotIn("legacy downstream effect not closed", edges[0])
                evidence = {str(value) for value in item["evidence"]}
                self.assertIn(location, evidence)
                self.assertIn(OWNER_TEST, evidence)

    def test_text_and_image_handlers_share_the_same_projection(self) -> None:
        source = (
            ROOT / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_info/user_info.py"
        ).read_text(encoding="utf-8")
        text_handler = source[
            source.index("async def xiuxian_message_("):
            source.index("async def xiuxian_message_img_(")
        ]
        image_handler = source[source.index("async def xiuxian_message_img_("):]
        self.assertIn("get_user_xiuxian_info(user_info[\"user_id\"])", text_handler)
        self.assertIn("get_user_xiuxian_info(user_info['user_id'])", image_handler)
        self.assertIn("get_user_profile(user_id)", source)
        self.assertIn("get_player_attributes(user_id)", source)
        self.assertIn("update_last_check_info_time(user_id)", source)
        self.assertIn("get_equipped_title_display(user_id)", source)
        for legacy_reader in (
            "_sql_message().get_exp_rank(",
            "_sql_message().get_root_rate(",
            "UserBuffDate(user_id)",
            "load_partner(user_id)",
            "load_mentor(user_id)",
            "NatalTreasure(user_id)",
        ):
            self.assertIn(legacy_reader, source)

    def test_avatar_command_and_id_generation_use_profile_owner(self) -> None:
        source = (
            ROOT / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_info/avatar.py"
        ).read_text(encoding="utf-8")
        handler = source[
            source.index("async def avatar_switch_cmd_("):
            source.index("async def my_id_cmd_(")
        ]
        generator = source[source.index("def _generate_unique_avatar_id("):]
        self.assertIn("get_user_profile(main_id)", handler)
        self.assertIn("get_user_profile(str(avatar_id))", handler)
        self.assertIn("get_user_profile(new_id)", generator)
        self.assertNotIn("get_user_info_with_id", source)
        self.assertIn("_player_avatar_application().initialize(", source)
        self.assertIn("_player_avatar_application().toggle_active(", source)
        self.assertIn("_player_avatar_application().restore_active(", source)

    def test_changelog_handler_keeps_external_work_off_the_event_loop(self) -> None:
        source = (
            ROOT
            / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_info/changelog_command.py"
        ).read_text(encoding="utf-8")
        handler = source[source.index("async def changelog_("):]
        self.assertIn("asyncio.to_thread(get_commits, page)", handler)
        self.assertIn("asyncio.to_thread(create_changelog_image, commits, page)", handler)
        self.assertIn("await handle_pic_send(bot, event, img_buf)", handler)
        self.assertIn("_delete_generated_image", handler)


if __name__ == "__main__":
    unittest.main()
