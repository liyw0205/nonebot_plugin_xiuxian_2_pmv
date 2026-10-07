from __future__ import annotations

import ast
import unittest
from pathlib import Path

from scripts.phase2_legacy_path_gate import load_phase2_scope_report


ROOT = Path(__file__).resolve().parents[1]
SOURCE = "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_impart_pk/__init__.py"
OWNER_TEST = "tests/test_impart_pk_frozen_command_owner.py"


def _handler_line(handler_name: str) -> int:
    tree = ast.parse((ROOT / SOURCE).read_text(encoding="utf-8"))
    return next(
        node.lineno
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name == handler_name
    )


class ImpartPkFrozenCommandOwnerTests(unittest.TestCase):
    def test_frozen_commands_have_reviewed_owner_or_compatibility_edges(self) -> None:
        report = load_phase2_scope_report(include_items=True)
        items = {item["id"]: item for item in report["items"]}
        expected = {
            "command:impart_pk:投影虚神界": (
                "impart_pk_project_",
                "已迁移",
                "impart_pk_application.project_join -> ImpartPkRepository.project_join -> ImpartProjectJoinSqlRepository.join",
            ),
            "command:impart_pk:探索虚神界": (
                "impart_pk_go_",
                "已迁移",
                "impart_pk_application.explore_settle -> ImpartPkRepository.explore_settle -> ImpartExploreSqlRepository.settle",
            ),
            "command:impart_pk:虚神界信息": (
                "impart_pk_info_",
                "允许保留的兼容路径",
                "_daily_impart_state/impart_pk_check/UserBuffDate -> legacy-backed status projection",
            ),
            "command:impart_pk:虚神界修炼": (
                "impart_pk_exp_",
                "已迁移",
                "impart_pk_application.training_settle -> ImpartPkRepository.training_settle -> ImpartTrainingSqlRepository.settle",
            ),
            "command:impart_pk:虚神界出关": (
                "impart_pk_out_closing_",
                "已迁移",
                "impart_pk_application.closing_settle -> ImpartPkRepository.closing_settle -> ImpartClosingSettlementSqlRepository.settle",
            ),
            "command:impart_pk:虚神界列表": (
                "impart_pk_list_",
                "允许保留的兼容路径",
                "xu_world.all_xu_world_user/impart_pk.find_user_data/_sql_message.get_user_info_with_id -> legacy-backed member projection",
            ),
            "command:impart_pk:虚神界对决": (
                "impart_pk_now_",
                "已迁移",
                "impart_pk_application.battle_settle -> ImpartPkRepository.battle_settle -> ImpartBattleBatchSqlRepository.settle",
            ),
            "command:impart_pk:虚神界排行榜": (
                "impart_top_",
                "允许保留的兼容路径",
                "xiuxian_impart.get_impart_rank/_sql_message.get_user_info_with_id -> legacy-backed ranking projection",
            ),
            "command:impart_pk:虚神界闭关": (
                "impart_pk_in_closing_",
                "已迁移",
                "impart_pk_application.closing_enter -> ImpartPkRepository.closing_enter -> ImpartClosingEnterSqlRepository.enter",
            ),
        }
        self.assertEqual(
            {item_id for item_id in items if item_id.startswith("command:impart_pk:")},
            set(expected),
        )
        for item_id, (handler_name, status, owner_edge) in expected.items():
            with self.subTest(item_id=item_id):
                item = items[item_id]
                self.assertEqual(item["status"], status)
                self.assertNotIn("unknown_edge", item)
                location = f"{SOURCE}:{_handler_line(handler_name)}"
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


if __name__ == "__main__":
    unittest.main()
