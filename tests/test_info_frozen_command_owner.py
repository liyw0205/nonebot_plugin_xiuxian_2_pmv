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
        item_id = "command:info:我的修仙信息"
        source = "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_info/user_info.py"
        handler_name = "xiuxian_message_"
        item = items.get(item_id)
        if item is not None:
            location = f"{source}:{_handler_line(source, handler_name)}"
            edges = [
                str(edge)
                for edge in item["call_graph"]
                if str(edge).startswith(f"{location} {handler_name} ->")
            ]
            self.assertEqual(len(edges), 1)
            self.assertIn(
                "SectInfoSqlRepository.get_name_by_id (sect_name-only projection)",
                edges[0],
            )
            self.assertNotIn("legacy downstream effect not closed", edges[0])
            self.assertIn(location, {str(value) for value in item["evidence"]})
            self.assertIn(OWNER_TEST, {str(value) for value in item["evidence"]})
            return

        # The existing WIP deletes the frozen item artifact. Keep this changed
        # command's owner edge verified without restoring that user-owned file
        # or adding the unrelated frozen-command entries reported as absent.
        self.assertTrue(
            any("Frozen entry file is missing" in error for error in report["integrity_errors"]),
            report["integrity_errors"],
        )
        projection = (ROOT / source).read_text(encoding="utf-8")
        projection = projection[
            projection.index("async def get_user_xiuxian_info("):
            projection.index("@xiuxian_message.handle")
        ]
        self.assertIn(
            "SectInfoSqlRepository(get_paths().game_db).get_name_by_id(sect_id)",
            projection,
        )
        self.assertNotIn("get_sect_info(sect_id)", projection)
        repository = (
            ROOT / "nonebot_plugin_xiuxian_2/features/sect/sect_info_repository.py"
        ).read_text(encoding="utf-8")
        name_reader = repository[
            repository.index("def get_name_by_id("):
            repository.index("def get_id_by_name(")
        ]
        self.assertIn('"SELECT sect_name FROM sects WHERE sect_id=?"', name_reader)
        self.assertNotIn("normalize_sect_row", name_reader)

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
