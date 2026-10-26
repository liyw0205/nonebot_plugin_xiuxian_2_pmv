"""Per-view delivery contract: every activity player view through the send seam.

`test_activity_center_delivery_contract.py` proves the shared `handle_send`
decision seam on the center card. The remaining player views — 活动阶段,
活动首领, 活动商店, 活动背包, 活动奖励, 活动玩法 — go through the same
`handle_send(native_markdown=True, fallback_msg=strip_md_command_links(text))`
seam (or silently skipped it), so this module pins each view on both
presentations against the real default configuration:

* OneBot V11 / QQ without markdown: the plain degradation arrives once, with
  no ``**`` and no ``mqqapi`` URL, and every key line still readable;
* QQ with markdown: the native message keeps ``](mqqapi`` blue links and
  normalises ``\\n`` to ``\\r`` (``utils.py``), since a single ``\\n`` swallows
  the line break in QQ markdown.

The handler source contract then pins all six handlers to the same seam.
Written as ``unittest.TestCase`` because the CI quality job collects tests
with ``python -m unittest discover``.
"""

from __future__ import annotations

import asyncio
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.features.activity.migrations import (
    apply_activity_state_schema,
)
from nonebot_plugin_xiuxian_2.features.activity.read_model_application import (
    ActivityReadModelApplication,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_activity as activity_module
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_activity import activity_config
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import utils
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.message_markdown import (
    strip_md_command_links,
)


ROOT = Path(__file__).resolve().parents[1]
HANDLERS_PATH = (
    ROOT
    / "nonebot_plugin_xiuxian_2"
    / "xiuxian"
    / "xiuxian_activity"
    / "__init__.py"
)


def _handler_body(source: str, command: str) -> str:
    """Text of one handler from its decorator to the next decorator."""

    start = source.index(f"@{command}.handle")
    following = source.find("@activity_", start + 1)
    return source[start : following if following != -1 else len(source)]


def _config() -> dict:
    """The shipped default configuration (three gameplay types at once)."""

    return activity_config._migrate_config(activity_config._load_default_config())[0]


def _event() -> SimpleNamespace:
    return SimpleNamespace(get_user_id=lambda: "u1", message_id="m1")


def _bot(*, onebot: bool) -> SimpleNamespace:
    name = "OneBot V11" if onebot else "QQ"
    return SimpleNamespace(
        self_id="bot-1",
        adapter=SimpleNamespace(get_name=lambda: name),
    )


def _config_stub(*, markdown: bool) -> type:
    class _XiuConfig:
        def __init__(self, *args, **kwargs) -> None:
            self.markdown_status = markdown
            self.img = False
            self.at_sender = False
            self.message_optimization = False
            self.markdown_button_status = False
            self.markdown_id2 = False

    return _XiuConfig


def _send_through_entry(bot, text: str, *, markdown: bool):
    """Run the shared handle_send decision seam, capturing the terminal send."""

    plain_sent: list[str] = []
    md_sent: list[str] = []

    async def capture_plain(*args, **kwargs):
        plain_sent.append(str(kwargs.get("message") or (args[-1] if args else "")))

    async def capture_markdown(*args, **kwargs):
        md_sent.append(str(kwargs.get("md_text") or (args[2] if len(args) > 2 else "")))

    with (
        patch.object(utils, "XiuConfig", _config_stub(markdown=markdown)),
        patch.object(utils, "get_chat_scene", lambda event: "group"),
        patch.object(utils, "is_group_event", lambda event: True),
        patch.object(utils, "get_real_id_async", AsyncMock(return_value="")),
        patch.object(utils, "_send_event_message", capture_plain),
        patch.object(utils, "_send_markdown_or_keyboard", capture_markdown),
    ):
        asyncio.run(
            utils.handle_send(
                bot,
                _event(),
                text,
                native_markdown=True,
                fallback_msg=strip_md_command_links(text),
            )
        )
    return plain_sent, md_sent


VIEWS = {
    # view name -> (handler command object, builder, key lines that must survive)
    "stage": (
        "activity_stage_cmd",
        lambda read_model: read_model.activity_stage_text("u1"),
        ("活动阶段", "正式期", "阶段日程"),
    ),
    "boss": (
        "activity_boss_cmd",
        lambda read_model: read_model.boss_status_text("u1"),
        ("庆典魔尊", "讨伐：活动讨伐", "全服进度奖励"),
    ),
    "shop": (
        "activity_shop_cmd",
        lambda read_model: read_model.point_shop_guidance_text("u1"),
        ("活动商店", "购买：活动购买 灵石补给", "可购商品"),
    ),
    "bag": (
        "activity_bag_cmd",
        lambda read_model: read_model.collect_bag_guidance_text("u1"),
        ("活动背包", "保底进度", "兑换：活动兑换 福运绵长"),
    ),
    "reward": (
        "activity_rewards_cmd",
        lambda read_model: read_model.reward_catalog_text("u1"),
        ("每日签到奖励", "每日任务奖励", "周常任务奖励"),
    ),
    "gameplay": (
        "activity_gameplay_cmd",
        lambda read_model: read_model.gameplay_personal_text("u1"),
        ("【我的玩法进度】", "字牌", "保底"),
    ),
}


class ActivityViewDeliveryContractTests(unittest.TestCase):
    def setUp(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.database = Path(tmp.name) / "game.db"
        # Real schema on an isolated database: no game writes, no real data.
        with DatabaseUnitOfWork(self.database) as uow:
            apply_activity_state_schema(uow)

    def _read_model(self) -> ActivityReadModelApplication:
        return ActivityReadModelApplication(self.database, config_loader=_config)

    def _render(self, name: str) -> str:
        _command, build, _key_lines = VIEWS[name]
        text = build(self._read_model())
        self.assertTrue(text.strip(), f"{name} rendered empty")
        return text

    # --- OneBot / QQ without markdown ---------------------------------

    def test_onebot_receives_the_plain_degradation_for_every_view(self) -> None:
        for name in VIEWS:
            with self.subTest(view=name):
                text = self._render(name)
                plain_sent, md_sent = _send_through_entry(
                    _bot(onebot=True), text, markdown=True
                )
                self.assertEqual([], md_sent)
                self.assertEqual([strip_md_command_links(text).rstrip()], [x.rstrip() for x in plain_sent])
                plain = plain_sent[0]
                self.assertNotIn("**", plain)
                self.assertNotIn("mqqapi", plain)
                for line in VIEWS[name][2]:
                    self.assertIn(line, plain)
                self.assertEqual(strip_md_command_links(plain).rstrip(), plain.rstrip())

    def test_qq_without_markdown_uses_the_same_plain_contract(self) -> None:
        for name in VIEWS:
            with self.subTest(view=name):
                text = self._render(name)
                plain_sent, md_sent = _send_through_entry(
                    _bot(onebot=False), text, markdown=False
                )
                self.assertEqual([], md_sent)
                self.assertEqual([strip_md_command_links(text).rstrip()], [x.rstrip() for x in plain_sent])

    # --- QQ native markdown -------------------------------------------

    def test_qq_with_markdown_keeps_native_layout_for_every_view(self) -> None:
        for name in VIEWS:
            with self.subTest(view=name):
                text = self._render(name)
                plain_sent, md_sent = _send_through_entry(
                    _bot(onebot=False), text, markdown=True
                )
                self.assertEqual([], plain_sent)
                self.assertEqual([text.replace("\n", "\r")], md_sent)
                native = md_sent[0]
                self.assertNotIn("\n", native)
                # 原生 markdown 通道按设计保留 `**标题**`（QQ 渲染加粗），
                # 只要求中心卡片同样的入口标签与行都在，且换行已归一。
                if name == "center":
                    self.assertIn("**", native)
                for line in VIEWS[name][2]:
                    self.assertIn(line, native)

    # --- multi-line layouts ------------------------------------------

    def test_playlist_layout_keeps_blank_lines_on_both_paths(self) -> None:
        """玩法个人状态段：段间空行不能被 QQ 原生 markdown 吞掉。"""

        text = self._render("gameplay")
        # 玩法层一行（今日进度）结束处必须有空行，否则 QQ 原生 \\r 单换行
        # 会把下一段粘到「今日进度」这一行上。
        lines = text.split("\n")
        index = next(
            i
            for i, line in enumerate(lines)
            if line.startswith("  今日进度：")
        )
        self.assertEqual("", lines[index + 1])
        plain_sent, md_sent = _send_through_entry(
            _bot(onebot=False), text, markdown=True
        )
        self.assertTrue(
            md_sent[0].endswith(lines[index].rstrip() + "\r"),
            "今日进度 段后必须留出空行（QQ 原生 \\r 单换行会吞行）",
        )
        # 个人玩法视图末尾保留段后空行，纯文本降级只比较去掉尾部空白后的内容。
        _, native_sent = _send_through_entry(_bot(onebot=False), text, markdown=False)
        self.assertEqual([], native_sent)

    def test_bag_groups_keep_one_blank_line_between_games(self) -> None:
        """多集字活动：段间恰一空行（纯文本与 QQ 原生都不粘连）。"""

        config = _config()
        base = next(
            activity
            for activity in config["gameplay_activities"]
            if activity.get("type") == "collect_words"
        )
        config["gameplay_activities"] = [
            base,
            {
                **base,
                "key": "second_collect",
                "template_key": "second_collect",
                "name": "第二集字",
                "letters": [{"char": "甲", "weight": 1}],
                "phrases": [{"phrase": "甲甲", "name": "甲甲", "reward": "灵石x1"}],
            },
        ]
        read_model = ActivityReadModelApplication(
            self.database, config_loader=lambda: config
        )
        text = read_model.collect_bag_guidance_text("u1")
        self.assertNotIn("\n\n\n", text.strip())
        self.assertIn("进行中\n字牌", text)
        self.assertIn("\n\n【第二集字】进行中", text)
        plain_sent, md_sent = _send_through_entry(
            _bot(onebot=False), text, markdown=True
        )
        self.assertEqual([], plain_sent)
        self.assertIn("【第二集字】进行中", md_sent[0])
        self.assertIn("\r\r【第二集字】进行中", md_sent[0])
        plain_sent, no_md = _send_through_entry(
            _bot(onebot=True), text, markdown=True
        )
        self.assertEqual([], no_md)
        self.assertEqual(strip_md_command_links(text).rstrip(), plain_sent[0].rstrip())

    # --- handler wiring -------------------------------------------------

    def test_every_player_view_handler_uses_the_shared_seam(self) -> None:
        """六视图 handler 都走中心卡片同一投递缝隙。

        `活动`/`活动奖励` 本次不在范围内，保留其既有 `fallback_msg=text`
        形态（它们不发命令链接），所以逐个视图只钉自己的 handler。
        """

        source = HANDLERS_PATH.read_text(encoding="utf-8")
        expected = {
            view: (command, "# --- seam: " + view + " ---\n", "seam-sentinel-" + view)
            for view in VIEWS
            for command in (VIEWS[view][0],)
        }
        for view, (command, _anchor, _sentinel) in expected.items():
            with self.subTest(handler=command):
                body = _handler_body(source, command)
                self.assertIn("native_markdown=True", body)
                self.assertIn(
                    "fallback_msg=strip_md_command_links(text)",
                    body,
                    f"{command} 未走剥链接纯文本降级",
                )

    def test_shop_and_bag_handlers_reach_the_same_seam_at_runtime(self) -> None:
        """运行期接线：商店/背包 handler 真的把提示文本交给共享缝隙。"""

        read_model = self._read_model()
        for command_name in ("activity_shop_cmd", "activity_bag_cmd"):
            with self.subTest(handler=command_name):
                command = getattr(activity_module, command_name)
                handler = command.handlers[0].call
                sent: list[dict] = []

                async def capture(*args, **kwargs):
                    sent.append(kwargs)

                with (
                    patch.object(
                        activity_module, "assign_bot", AsyncMock(return_value=None)
                    ),
                    patch.object(activity_module, "handle_send", capture),
                    patch.object(
                        activity_module,
                        "_ensure_user",
                        AsyncMock(
                            return_value=(True, {"user_id": "u1"}, "")
                        ),
                    ),
                    patch.object(
                        activity_module,
                        "activity_application",
                        SimpleNamespace(read_model=read_model),
                    ),
                ):
                    asyncio.run(handler(_bot(onebot=False), _event()))
                self.assertEqual(1, len(sent))
                self.assertIs(True, sent[0].get("native_markdown"))
                fallback = sent[0].get("fallback_msg")
                self.assertEqual(strip_md_command_links(fallback), fallback)
                self.assertNotIn("**", fallback)


if __name__ == "__main__":
    unittest.main()
