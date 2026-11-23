from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import nonebot

nonebot.init()


def test_registered_map_dao_view_parameters_and_sqlite_read_recovery(map_parameter_probe_results):
    rows = [
        row for row in map_parameter_probe_results["results"]
        if row["group"] == "map-dao-view"
    ]
    assert {row["key"] for row in rows} == {
        "map-dao-view-records",
        "map-dao-view-target-read-recovery",
        "map-dao-view-record-read-recovery",
    }
    assert sorted(row["outcome"] for row in rows) == ["recovery", "recovery", "success"]


def test_dao_view_read_failure_reason_survives_onebot_qq_plain_markdown_and_fallback():
    import asyncio

    from nonebot_plugin_xiuxian_2.core.player_reasons import reason_text
    from nonebot_plugin_xiuxian_2.xiuxian.command_guard import result_failure
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import utils
    from tests.command_probe import make_event

    event = make_event("论道查看", "900001", 975899)
    onebot = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "OneBot V11"))
    qq = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "QQ"))
    cases = (
        ("map_nearby_target_read_failed", "nearby_target", "附近道友资料"),
        ("map_dao_record_read_failed", "dao_record", "论道战绩"),
    )
    for reason, subject, label in cases:
        message = result_failure(
            "论道查看",
            {"reason": reason, "subject": subject},
            subjects={"nearby_target": "附近道友资料", "dao_record": "论道战绩"},
        )
        for bot, markdown_enabled, fail_markdown in (
            (onebot, False, False),
            (qq, False, False),
            (qq, True, False),
            (qq, True, True),
        ):
            plain, markdown = AsyncMock(), AsyncMock()
            if fail_markdown:
                markdown.side_effect = RuntimeError("isolated dao view Markdown failure")
            config = SimpleNamespace(
                markdown_status=markdown_enabled, markdown_id="", markdown_button_status=False,
                reference_reply=False, message_optimization=False, img=False, at_sender=False,
            )
            with (
                patch.object(utils, "XiuConfig", return_value=config),
                patch.object(utils, "_allow_native_markdown", return_value=True),
                patch.object(utils, "handle_send2", plain),
                patch.object(utils, "handle_send_markdown", markdown),
            ):
                asyncio.run(utils.handle_send(bot, event, message))
            delivered = markdown if markdown_enabled and not fail_markdown else plain
            delivered.assert_awaited_once()
            rendered = str(delivered.await_args.args[2]).replace("\r", "")
            assert f"论道查看：{label}" in rendered, rendered
            assert reason_text(reason) in rendered, rendered
