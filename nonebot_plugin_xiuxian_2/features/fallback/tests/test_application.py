from __future__ import annotations

import unittest
from types import SimpleNamespace

from ..application import EmptyFallbackApplication


class _Log:
    def __init__(self) -> None:
        self.warnings: list[str] = []

    def warning(self, message: str) -> None:
        self.warnings.append(message)


class EmptyFallbackApplicationTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.config = SimpleNamespace(
            empty_fallback=True,
            empty_msg="default reply",
            empty_fallback_image=False,
        )
        self.log = _Log()

    def application(self, **kwargs) -> EmptyFallbackApplication:
        return EmptyFallbackApplication(
            config_provider=lambda: self.config,
            event_kind_provider=lambda event: event.kind,
            full_message_group_provider=lambda group_id: group_id == "full",
            log=self.log,
            **kwargs,
        )

    def test_matcher_policy_preserves_private_group_create_and_full_group_rules(self) -> None:
        application = self.application()
        private = SimpleNamespace(kind="private")
        regular_group = SimpleNamespace(kind="group", type="group_message", to_me=False)
        group_create = SimpleNamespace(
            kind="group", type="GROUP_MESSAGE_CREATE", to_me=False
        )
        mentioned_group_create = SimpleNamespace(
            kind="group", type="GROUP_MESSAGE_CREATE", to_me=True
        )
        full_group_chat = SimpleNamespace(
            kind="group", group_id="full", to_me=False
        )

        self.assertTrue(application.should_respond(private))
        self.assertTrue(application.should_respond(regular_group))
        self.assertFalse(application.should_respond(group_create))
        self.assertTrue(application.should_respond(mentioned_group_create))
        self.assertFalse(application.should_respond(full_group_chat))

        self.config.empty_fallback = False
        self.assertFalse(application.should_respond(private))
        self.config.empty_fallback = True
        self.config.empty_msg = ""
        self.assertFalse(application.should_respond(private))
        self.assertFalse(
            self.application().should_respond(SimpleNamespace(kind="other"))
        )

    async def test_image_reply_preserves_legacy_image_then_text_sequence(self) -> None:
        calls: list[tuple] = []

        async def image_url_provider(*, timeout: int):
            calls.append(("fetch", timeout))
            return "https://example.invalid/image.jpg"

        async def image_sender(*args):
            calls.append(("image", *args))

        async def text_sender(*args):
            calls.append(("text", *args))

        self.config.empty_fallback_image = True
        application = self.application(
            image_url_provider=image_url_provider,
            image_sender=image_sender,
            text_sender=text_sender,
        )
        bot, event = object(), object()

        await application.respond(bot, event)

        self.assertEqual(calls[0], ("fetch", 3))
        self.assertEqual(
            calls[1],
            ("image", bot, event, "https://example.invalid/image.jpg", "default reply"),
        )
        self.assertEqual(calls[2], ("text", bot, event, "default reply"))
        self.assertEqual(len(calls), 3)

    async def test_image_send_failure_falls_back_to_text_and_swallows_text_failure(self) -> None:
        calls: list[str] = []

        async def image_url_provider(*, timeout: int):
            return "https://example.invalid/image.jpg"

        async def image_sender(*args):
            calls.append("image")
            raise RuntimeError("image unavailable")

        async def text_sender(*args):
            calls.append("text")
            raise RuntimeError("text unavailable")

        self.config.empty_fallback_image = True
        application = self.application(
            image_url_provider=image_url_provider,
            image_sender=image_sender,
            text_sender=text_sender,
        )

        await application.respond(object(), object())

        self.assertEqual(calls, ["image", "text"])
        self.assertEqual(len(self.log.warnings), 2)
        self.assertIn("图文发送失败", self.log.warnings[0])
        self.assertIn("纯文字发送失败", self.log.warnings[1])

    async def test_missing_image_sends_text_and_image_request_is_optional(self) -> None:
        calls: list[tuple] = []

        async def image_url_provider(**kwargs):
            calls.append(("fetch", kwargs))
            return None

        async def text_sender(*args):
            calls.append(("text", *args))

        application = self.application(
            image_url_provider=image_url_provider,
            text_sender=text_sender,
        )
        bot, event = object(), object()
        await application.respond(bot, event)
        self.assertEqual(calls, [("text", bot, event, "default reply")])

        self.config.empty_fallback_image = True
        calls.clear()
        await application.respond(bot, event)
        self.assertEqual(calls[0], ("fetch", {"timeout": 3}))
        self.assertEqual(calls[1], ("text", bot, event, "default reply"))


if __name__ == "__main__":
    unittest.main()
