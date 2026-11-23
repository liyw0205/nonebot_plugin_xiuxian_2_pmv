"""Real matcher coverage for the cave-dwelling help catalog."""

import html
import re

from tests.parameter_cases_dongfu_status_view import _snapshot


def register(case):
    @case(
        "dongfu-help-manifest-usage-readonly",
        "洞府帮助",
        "群聊与私聊重复查询",
        "运行期 dongfu 命令清单与隔离数据库快照",
        "命令/别名/参数提示与 manifest 一致，重复查询对全部数据库零写",
        group="dongfu-help",
    )
    async def help_catalog(probe):
        from nonebot_plugin_xiuxian_2.compatibility.command_inventory import commands_for

        before = _snapshot(probe)
        replies = []
        for index, event_type in enumerate(("group", "private")):
            reply = await probe.command(
                "洞府帮助",
                "洞府帮助",
                number=974500 + index,
                event_type=event_type,
                timeout=30,
            )
            replies.append(reply)

        required_forms = (
            "洞府种植 种子名 / 洞府种植 [灵田编号] 种子名",
            "洞府收获 [灵田编号]",
            "洞府施肥 灵田编号",
            "洞府催熟 灵田编号",
            "洞府扩建 [数量]",
            "拜访道友 道号",
            "潜入洞府 [道号]",
        )
        normalized = [
            re.sub(r"^\[CQ:at,qq=\d+\]", "", html.unescape(reply)).replace("\r", "")
            for reply in replies
        ]
        missing = []
        for index, text in enumerate(normalized):
            missing.extend(
                f"reply{index}:command:{spec.name}"
                for spec in commands_for("dongfu")
                if spec.name not in text
            )
            missing.extend(
                f"reply{index}:alias:{alias}"
                for spec in commands_for("dongfu")
                for alias in spec.aliases
                if not re.search(rf"(?<!\w){re.escape(alias)}(?!\w)", text)
            )
            missing.extend(
                f"reply{index}:usage:{form}"
                for form in required_forms
                if form not in text
            )
        after = _snapshot(probe)
        probe.evidence.update(
            replies=replies,
            missing=missing,
            same_reply=normalized[0] == normalized[1],
            database_unchanged=after == before,
        )
        assert not missing, {"missing_help_entries": missing}
        assert normalized[0] == normalized[1], "group/private help content differs"
        assert after == before, "dongfu help queries wrote to an isolated database"
