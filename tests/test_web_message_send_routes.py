from __future__ import annotations

import asyncio
from contextlib import ExitStack
import unittest
from unittest.mock import AsyncMock, patch

import tests  # Keep web-module imports on the isolated test data directory.
import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.messaging import SendResult
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_web import core
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_web import messages as message_routes


class MessageReplyRepositoryFake:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []
        self.latest_candidates: list[dict] = []
        self.specific_candidate: dict | None = None
        self.reference_candidate: dict | None = None

    def get_latest_reply_candidates_for_qq(self, **kwargs):
        self.calls.append(("latest", kwargs))
        return self.latest_candidates

    def get_specific_reply_candidate_for_qq(self, **kwargs):
        self.calls.append(("specific", kwargs))
        return self.specific_candidate

    def get_specific_reference_candidate_for_qq(self, **kwargs):
        self.calls.append(("reference", kwargs))
        return self.reference_candidate


class MessageSendRouteTests(unittest.TestCase):
    csrf_token = "message-send-csrf"

    def setUp(self) -> None:
        core.app.config.update(TESTING=True, SECRET_KEY="message-send-routes")
        self.client = core.app.test_client()

    def _login(self) -> None:
        with self.client.session_transaction() as session:
            session["admin_id"] = "admin-1"
            session["_csrf_token"] = self.csrf_token

    def _post(self, data: dict, *, csrf: bool = True):
        headers = {"X-CSRF-Token": self.csrf_token} if csrf else {}
        return self.client.post("/api/messages/send", json=data, headers=headers)

    def _patch_send_runtime(self, repository, delivery_send):
        return (
            patch.object(core, "ADMIN_IDS", {"admin-1"}),
            patch.object(message_routes, "_message_reply_repository", return_value=repository),
            patch.object(
                message_routes,
                "get_message_db_connection",
                side_effect=AssertionError("send route must not use the legacy message DB connection"),
            ),
            patch.object(message_routes, "get_bot_by_adapter", return_value=object()),
            patch.object(message_routes, "get_bot_id", return_value="bot-1"),
            patch.object(message_routes, "build_web_message_segment", return_value="message"),
            patch.object(message_routes, "run_async", side_effect=lambda coro: asyncio.run(coro)),
            patch.object(message_routes.delivery_service, "send", delivery_send),
        )

    def test_send_requires_admin_and_csrf_before_resolving_candidates(self) -> None:
        repository = MessageReplyRepositoryFake()
        with patch.object(core, "ADMIN_IDS", {"admin-1"}), patch.object(
            message_routes, "_message_reply_repository", return_value=repository
        ) as repository_factory:
            anonymous = self._post({"adapter": "QQ"})
            self.assertEqual(anonymous.status_code, 401)
            self.assertEqual(anonymous.get_json(), {"success": False, "error": "未登录"})

            self._login()
            missing_csrf = self._post({"adapter": "QQ"}, csrf=False)
            self.assertEqual(missing_csrf.status_code, 403)
            self.assertEqual(
                missing_csrf.get_json(),
                {"success": False, "error": "CSRF 校验失败，请刷新页面后重试"},
            )

        repository_factory.assert_not_called()
        self.assertEqual(repository.calls, [])

    def test_active_send_uses_repository_for_reply_and_quote_candidates(self) -> None:
        self._login()
        repository = MessageReplyRepositoryFake()
        repository.specific_candidate = {"message_id": "source-22"}
        repository.reference_candidate = {"reference_id": "reference-11"}
        delivery_send = AsyncMock(return_value=SendResult("sent-1", "sent-ref", {}))
        patches = self._patch_send_runtime(repository, delivery_send)
        with ExitStack() as stack:
            stack.enter_context(patches[0])
            factory = stack.enter_context(patches[1])
            legacy_connection = stack.enter_context(patches[2])
            for patcher in patches[3:]:
                stack.enter_context(patcher)
            response = self._post(
                {
                    "adapter": "QQ",
                    "scene": "group",
                    "target_id": "group-7",
                    "content": "hello",
                    "active_send": True,
                    "reply_message_id": "source-22",
                    "quote_reference_id": "reference-11",
                }
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.get_json(),
            {
                "success": True,
                "message": "QQ 主动发送成功",
                "message_id": "sent-1",
                "reference_id": "sent-ref",
                "source_message_id": "source-22",
                "quote_reference_id": "reference-11",
            },
        )
        self.assertEqual(
            repository.calls,
            [
                ("reference", {"scene": "group", "target_id": "group-7", "reference_id": "reference-11"}),
                ("specific", {"scene": "group", "target_id": "group-7", "message_id": "source-22"}),
            ],
        )
        factory.assert_called_once_with()
        legacy_connection.assert_not_called()
        request = delivery_send.await_args.args[1]
        self.assertEqual(request.scene, "group")
        self.assertEqual(request.target_id, "group-7")
        self.assertEqual(request.reference_id, "reference-11")
        self.assertEqual(request.source_message_id, "source-22")

    def test_automatic_reply_keeps_candidate_order_and_waits_for_fallback_send(self) -> None:
        self._login()
        repository = MessageReplyRepositoryFake()
        repository.latest_candidates = [
            {"message_id": "first", "reference_id": "first-ref"},
            {"message_id": "second", "reference_id": "second-ref"},
        ]
        delivery_send = AsyncMock(
            side_effect=[RuntimeError("first candidate rejected"), SendResult("sent-2", "sent-ref-2", {})]
        )
        patches = self._patch_send_runtime(repository, delivery_send)
        with ExitStack() as stack:
            stack.enter_context(patches[0])
            factory = stack.enter_context(patches[1])
            legacy_connection = stack.enter_context(patches[2])
            for patcher in patches[3:]:
                stack.enter_context(patcher)
            response = self._post(
                {
                    "adapter": "QQ",
                    "scene": "group",
                    "target_id": "group-7",
                    "content": "hello",
                }
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.get_json(),
            {
                "success": True,
                "message": "发送成功",
                "message_id": "sent-2",
                "reference_id": "sent-ref-2",
                "source_message_id": "second",
                "source_reference_id": "second-ref",
                "quote_reference_id": "",
            },
        )
        self.assertEqual(
            repository.calls,
            [("latest", {"scene": "group", "target_id": "group-7", "limit": 3})],
        )
        factory.assert_called_once_with()
        legacy_connection.assert_not_called()
        self.assertEqual(delivery_send.await_count, 2)
        self.assertEqual(
            [call.args[1].source_message_id for call in delivery_send.await_args_list],
            ["first", "second"],
        )


if __name__ == "__main__":
    unittest.main()
