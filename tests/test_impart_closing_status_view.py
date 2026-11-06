"""虚神界闭关状态可见性：纯函数口径 + 信息命令接线合同。

`虚神界信息` 过去在 `user_cd.type=4` 时仍只显示修炼时间/次数，玩家无法确认
自己正在闭关、已入定多久、祝福余额与本境修为上限余量，只能盲发 `虚神界出关`。
本套件钉住新纯函数的口径，并用源合同保证命令确实在信息面板里调用它。
"""

from __future__ import annotations

import unittest
from datetime import datetime, timedelta
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.impart_pk.closing_status import (
    closing_status_lines,
)

ROOT = Path(__file__).resolve().parents[1]
INFO_MODULE = ROOT / "nonebot_plugin_xiuxian_2" / "xiuxian" / "xiuxian_impart_pk" / "__init__.py"
NOW = datetime(2026, 11, 7, 12, 0, 0)


def stamp(minutes_ago: int) -> str:
    return (NOW - timedelta(minutes=minutes_ago)).strftime("%Y-%m-%d %H:%M:%S")


class ClosingStatusLinesTests(unittest.TestCase):
    def test_non_closing_states_render_nothing(self) -> None:
        for cd_type in (0, 1, 2, 3, None, ""):
            cooldown = {} if cd_type is None else {"type": cd_type, "create_time": stamp(30)}
            self.assertEqual(
                closing_status_lines(
                    cooldown, exp_day=60, current_exp=0, max_exp=1000, now=NOW
                ),
                (),
                msg=f"type={cd_type!r}",
            )
        self.assertEqual(
            closing_status_lines(None, exp_day=60, current_exp=0, max_exp=1000, now=NOW),
            (),
        )

    def test_closing_reports_elapsed_blessing_headroom_and_next_action(self) -> None:
        lines = closing_status_lines(
            {"type": 4, "create_time": stamp(37)},
            exp_day=120,
            current_exp=400,
            max_exp=1000,
            now=NOW,
        )
        self.assertEqual(len(lines), 4)
        self.assertIn("虚神界闭关中", lines[0])
        self.assertIn("37 分钟", lines[0])
        self.assertIn("120 分钟", lines[1])
        self.assertIn("600", lines[2])
        self.assertIn("虚神界出关", lines[3])

    def test_bad_or_blank_create_time_counts_as_zero_minutes(self) -> None:
        for value in ("0", "", None, "not-a-time", "  "):
            lines = closing_status_lines(
                {"type": 4, "create_time": value},
                exp_day=10,
                current_exp=0,
                max_exp=100,
                now=NOW,
            )
            self.assertIn("0 分钟", lines[0], msg=repr(value))

    def test_negative_blessing_and_headroom_clamp_to_zero(self) -> None:
        lines = closing_status_lines(
            {"type": 4, "create_time": stamp(5)},
            exp_day=-3,
            current_exp=2_000,
            max_exp=1_000,
            now=NOW,
        )
        self.assertIn("0 分钟", lines[1])
        self.assertTrue(lines[2].endswith("0"))

    def test_unknown_realm_limit_omits_headroom_line(self) -> None:
        lines = closing_status_lines(
            {"type": 4, "create_time": stamp(1)},
            exp_day=10,
            current_exp=5,
            max_exp=0,
            now=NOW,
        )
        self.assertEqual(len(lines), 3)
        self.assertFalse(any("上限余量" in line for line in lines))

    def test_handler_wires_the_helper_into_the_info_panel(self) -> None:
        source = INFO_MODULE.read_text(encoding="utf-8")
        self.assertIn("closing_status_lines(", source)
        self.assertIn("_sql_message().get_user_cd(user_id)", source)
        start = source.index("async def impart_pk_info_")
        end = source.index("async def ", start + 10)
        block = source[start:end]
        self.assertIn("closing_status_lines(", block)
        self.assertIn("exp_day=impart_data_draw[\"exp_day\"]", block)
        self.assertIn("max_exp=max_exp_limit", block)


if __name__ == "__main__":
    unittest.main()
