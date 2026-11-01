r"""灵庄指令的空格容错回归（`invalid_argument` 误判）。

现象：玩家发送 `灵庄 信息`、`灵庄 存灵石 1000`（`灵庄` 与子指令之间有半角或全角空格）
时会收到「请输入正确的金额或指令！本次未结算。」，合法操作被判为
`invalid_argument`。

根因：早期正则为 `^灵庄(存灵石|取灵石|升级会员|信息|结算)?(.*)?`，要求子指令**紧跟**
`灵庄`。一旦中间有空格，`mode` 捕获组为 None，`BankCommandApplication.execute` 走
`action = "help"` 分支；而余下文本（如 `信息`、`存灵石 1000`）又不等于 `帮助`，
于是返回 `invalid_argument`。中文输入法下插入空格非常常见，全角空格（U+3000）同样命中。

修复：正则收敛为 `features/bank/command_application.py` 的单一常量
`BANK_COMMAND_PATTERN`，并在 `灵庄` 之后允许 `\s*`（Python 的空白类覆盖 Unicode
空白，半角与全角空格都能吸收）。本套件用**真实解析器**与**真实
`BankCommandApplication`** 钉住：带空格的合法指令必须落到正确动作，
真正非法的输入仍必须是 `invalid_argument`。
"""

from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.features.bank.command_application import (
    BANK_COMMAND_PATTERN,
    BANK_MODES,
    BankCommandApplication,
    parse_bank_command,
)
from nonebot_plugin_xiuxian_2.features.bank.command_replies import render_bank_reply
from nonebot_plugin_xiuxian_2.features.bank.migrations import apply_bank_accounts
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork

NOW = datetime(2026, 10, 7, 12, tzinfo=timezone.utc)
SAVED_AT = datetime(2026, 10, 7, 10, tzinfo=timezone.utc).isoformat()
LEVELS = {
    "1": {"level": "1", "savemax": 10000, "interest": 0.1, "levelup": 100},
    "2": {"level": "2", "savemax": 20000, "interest": 0.2, "levelup": 0},
}


class BankCommandWhitespaceTests(unittest.TestCase):
    def setUp(self) -> None:
        self._temp = tempfile.TemporaryDirectory()
        self.database = Path(self._temp.name) / "game.db"
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute(
                "CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER NOT NULL)"
            )
            uow.execute("INSERT INTO user_xiuxian VALUES('u',1000)")
            apply_bank_accounts(uow)
            uow.execute(
                "INSERT INTO bank_accounts VALUES('u',100,'1',?)", (SAVED_AT,)
            )
        self.app = BankCommandApplication(
            self.database, LEVELS, clock=SimpleNamespace(now=lambda: NOW)
        )

    def tearDown(self) -> None:
        self._temp.cleanup()

    def run_command(self, text: str, *, operation_id: str = "op") -> dict:
        """按 handler 的方式解析并执行一条灵庄消息。"""
        mode, argument = parse_bank_command(text)
        return self.app.execute(
            operation_id=operation_id, user_id="u", mode=mode, argument=argument
        )

    # ------------------------------------------------------- 空格容错
    def test_mode_is_parsed_across_half_and_full_width_spaces(self) -> None:
        for text in ("灵庄信息", "灵庄 信息", "灵庄\u3000信息", "灵庄  信息"):
            with self.subTest(text=text):
                mode, argument = parse_bank_command(text)
                self.assertEqual(mode, "信息", f"{text!r} 未解析出 信息")
                self.assertEqual(argument.strip(), "")

    def test_info_with_space_is_not_invalid_argument(self) -> None:
        result = self.run_command("灵庄 信息")
        self.assertEqual(result["action"], "info")
        self.assertNotEqual(result["status"], "invalid_argument")

    def test_deposit_with_spaces_actually_applies(self) -> None:
        for text in ("灵庄 存灵石 1000", "灵庄\u3000存灵石\u30001000", "灵庄  存灵石  1000"):
            with self.subTest(text=text):
                self.setUp()
                result = self.run_command(text, operation_id=f"dep-{text}")
                self.assertEqual(result["status"], "applied", f"{text!r} 未成功存款")
                self.assertEqual(result["action"], "deposit")
                self.assertEqual(result["deposited"], 1000)

    def test_withdrawal_with_spaces_actually_applies(self) -> None:
        result = self.run_command("灵庄 取灵石 10", operation_id="wd-1")
        self.assertEqual(result["status"], "applied")
        self.assertEqual(result["action"], "withdrawal")
        self.assertEqual(result["withdrawn"], 10)

    def test_upgrade_and_interest_with_spaces_resolve(self) -> None:
        upgrade = self.run_command("灵庄 升级会员", operation_id="up-1")
        self.assertEqual(upgrade["action"], "upgrade")
        self.assertNotEqual(upgrade["status"], "invalid_argument")
        interest = self.run_command("灵庄 结算", operation_id="in-1")
        self.assertEqual(interest["action"], "interest")
        self.assertNotEqual(interest["status"], "invalid_argument")

    # ------------------------------------------------------- 帮助不受影响
    def test_help_forms_still_return_help(self) -> None:
        for text in ("灵庄", "灵庄帮助", "灵庄 帮助", "灵庄\u3000帮助"):
            with self.subTest(text=text):
                result = self.run_command(text)
                self.assertEqual(result["action"], "help")
                self.assertEqual(result["status"], "help")

    # ------------------------------------------------------- 真非法输入
    def test_genuinely_invalid_amounts_stay_invalid_argument(self) -> None:
        for text in (
            "灵庄存灵石",
            "灵庄存灵石 0",
            "灵庄存灵石 -5",
            "灵庄存灵石 1,000",
            "灵庄存灵石 1000.5",
            "灵庄存灵石 abc",
            "灵庄存灵石 1000 灵石",
            "灵庄存灵石 \uff11\uff10\uff10\uff10",
        ):
            with self.subTest(text=text):
                result = self.run_command(text, operation_id=f"bad-{text}")
                self.assertEqual(
                    result["status"], "invalid_argument", f"{text!r} 应判为非法金额"
                )

    def test_unknown_subcommand_stays_invalid_argument(self) -> None:
        result = self.run_command("灵庄 乱写")
        self.assertEqual(result["status"], "invalid_argument")

    def test_invalid_argument_renders_actionable_message(self) -> None:
        message = render_bank_reply(
            {"action": "deposit", "status": "invalid_argument"}, LEVELS
        )
        self.assertIn("金额", message)

    # ------------------------------------------------------- 单一来源
    def test_pattern_and_modes_are_single_source_of_truth(self) -> None:
        """正则与动作表必须来自同一处，且覆盖全部子指令。"""
        import re

        self.assertIn(r"\s*", BANK_COMMAND_PATTERN)
        compiled = re.compile(BANK_COMMAND_PATTERN)
        for mode in BANK_MODES:
            with self.subTest(mode=mode):
                match = compiled.match(f"灵庄 {mode}")
                self.assertIsNotNone(match)
                self.assertEqual(match.group(1), mode)

    def test_legacy_package_uses_shared_pattern(self) -> None:
        """命令注册处必须引用共享常量，避免两份正则再次漂移。"""
        source = (
            Path(__file__).resolve().parents[1]
            / "nonebot_plugin_xiuxian_2"
            / "xiuxian"
            / "xiuxian_bank"
            / "__init__.py"
        ).read_text(encoding="utf-8")
        self.assertIn("BANK_COMMAND_PATTERN", source)
        self.assertNotIn(r"r'^灵庄(存灵石", source)

    def test_help_text_documents_space_tolerance(self) -> None:
        source = (
            Path(__file__).resolve().parents[1]
            / "nonebot_plugin_xiuxian_2"
            / "xiuxian"
            / "xiuxian_bank"
            / "__init__.py"
        ).read_text(encoding="utf-8")
        self.assertIn("灵庄与指令之间可加空格", source)
        self.assertIn("正整数", source)


if __name__ == "__main__":
    unittest.main()
