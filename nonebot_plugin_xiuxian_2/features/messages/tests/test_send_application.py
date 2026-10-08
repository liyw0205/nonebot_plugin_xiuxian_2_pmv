from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace

from nonebot_plugin_xiuxian_2.features.messages import WebMessageSendApplication
from nonebot_plugin_xiuxian_2.xiuxian.messaging import SendResult


class ReplyRepositoryFake:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []
        self.latest: list[dict] = []
        self.specific_reply: dict | None = None
        self.specific_reference: dict | None = None

    def get_latest_reply_candidates_for_qq(self, **kwargs):
        self.calls.append(("latest", kwargs))
        return self.latest

    def get_specific_reply_candidate_for_qq(self, **kwargs):
        self.calls.append(("reply", kwargs))
        return self.specific_reply

    def get_specific_reference_candidate_for_qq(self, **kwargs):
        self.calls.append(("reference", kwargs))
        return self.specific_reference


class StickerApplicationFake:
    def __init__(self, path: Path | None = Path("/stickers/2.webp")) -> None:
        self.path = path
        self.calls: list[str] = []

    def resolve_sticker_path(self, token: str):
        self.calls.append(token)
        return self.path


class TransportFake:
    def __init__(self, results=()) -> None:
        self.results = list(results)
        self.requests = []

    async def send(self, bot, request):
        self.requests.append((bot, request))
        result = self.results.pop(0) if self.results else SendResult("sent", "ref", {})
        if isinstance(result, Exception):
            raise result
        return result


def _application(
    *,
    repository=None,
    repository_factory=None,
    stickers=None,
    transport=None,
    bot=None,
    upload_saver=None,
    direct_api=None,
):
    built_messages = []
    direct_calls = []
    recorded = []
    selected_bot = bot or SimpleNamespace(self_id="bot-1")

    def message_builder(bot, **kwargs):
        result = {"bot": bot, **kwargs}
        built_messages.append(result)
        return result

    async def call_adapter_api(bot, api, **kwargs):
        direct_calls.append((bot, api, kwargs))
        return {"message_id": "forward-1"}

    application = WebMessageSendApplication(
        bot_resolver=lambda adapter: selected_bot,
        bot_id_resolver=lambda _bot: "bot-1",
        is_ob11_adapter=lambda adapter: adapter in {"OneBot V11", "OneBot V12"},
        message_builder=message_builder,
        reply_repository_factory=repository_factory or (lambda: repository or ReplyRepositoryFake()),
        sticker_application=stickers or StickerApplicationFake(),
        upload_saver=upload_saver or (lambda upload: Path("/uploads") / upload.filename),
        transport=transport or TransportFake(),
        message_id_extractor=lambda result: result.get("message_id", ""),
        web_send_recorder=lambda bot, **kwargs: recorded.append((bot, kwargs)),
        direct_api=direct_api or call_adapter_api,
    )
    return application, built_messages, direct_calls, recorded


def _send(application, data, *, upload_file=None):
    return asyncio.run(application.send(data, upload_file=upload_file))


def test_validation_and_unknown_adapter_preserve_legacy_json_bodies():
    application, _, _, _ = _application()

    assert _send(application, {}).to_dict() == {
        "success": False,
        "error": "缺少 adapter",
    }
    assert _send(
        application,
        {"adapter": "QQ", "scene": "group", "target_id": "g", "media_type": "bad"},
    ).to_dict() == {"success": False, "error": "无效 media_type"}

    unsupported = _send(
        application,
        {
            "adapter": "Other",
            "scene": "group",
            "target_id": "g",
            "content": "hello",
        },
    )
    assert unsupported.to_dict() == {
        "success": False,
        "error": "暂不支持适配器: Other",
    }


def test_ob11_markdown_uses_forward_api_and_records_legacy_result():
    application, _, direct_calls, recorded = _application()

    result = _send(
        application,
        {
            "adapter": "OneBot V11",
            "scene": "group",
            "target_id": "42",
            "content": "**hello**",
            "send_mode": "markdown",
        },
    )

    assert result.to_dict() == {
        "success": True,
        "message": "Markdown 已通过合并转发发送",
        "message_id": "forward-1",
    }
    bot, api, kwargs = direct_calls[0]
    assert api == "send_group_forward_msg"
    assert kwargs["group_id"] == 42
    assert kwargs["messages"][0] == {
        "type": "node",
        "data": {"name": "聊天记录", "uin": "bot-1", "content": "**hello**"},
    }
    assert recorded == [
        (
            bot,
            {
                "scene": "group",
                "message_id": "forward-1",
                "source_message_id": "",
                "group_id": "42",
                "user_id": "",
                "message": "**hello**",
            },
        )
    ]


def test_ob11_plain_send_uses_delivery_transport_without_opening_reply_repository():
    transport = TransportFake([SendResult("sent-2", "", {})])
    repository_calls = []
    application, built_messages, direct_calls, _ = _application(
        transport=transport,
        repository_factory=lambda: repository_calls.append(True),
    )

    result = _send(
        application,
        {
            "adapter": "OneBot V11",
            "scene": "group",
            "target_id": "42",
            "content": "hello",
        },
    )

    assert result.to_dict() == {
        "success": True,
        "message": "发送成功",
        "message_id": "sent-2",
    }
    assert built_messages[-1]["send_mode"] == "plain"
    assert transport.requests[0][1].scene == "group"
    assert transport.requests[0][1].target_id == "42"
    assert repository_calls == []
    assert direct_calls == []


def test_active_qq_send_resolves_reference_and_specific_reply():
    repository = ReplyRepositoryFake()
    repository.specific_reference = {"reference_id": "ref-quote"}
    repository.specific_reply = {"message_id": "source-22"}
    transport = TransportFake([SendResult("sent-1", "sent-ref", {})])
    application, _, _, _ = _application(repository=repository, transport=transport)

    result = _send(
        application,
        {
            "adapter": "QQ",
            "scene": "group",
            "target_id": "group-7",
            "content": "hello",
            "active_send": True,
            "reply_message_id": "source-22",
            "quote_reference_id": "ref-quote",
        },
    )

    assert result.to_dict() == {
        "success": True,
        "message": "QQ 主动发送成功",
        "message_id": "sent-1",
        "reference_id": "sent-ref",
        "source_message_id": "source-22",
        "quote_reference_id": "ref-quote",
    }
    assert [method for method, _ in repository.calls] == ["reference", "reply"]
    request = transport.requests[0][1]
    assert request.reference_id == "ref-quote"
    assert request.source_message_id == "source-22"


def test_qq_automatic_reply_keeps_candidate_order_and_falls_back():
    repository = ReplyRepositoryFake()
    repository.latest = [
        {"message_id": "first", "reference_id": "first-ref"},
        {"message_id": "second", "reference_id": "second-ref"},
    ]
    transport = TransportFake(
        [RuntimeError("first candidate rejected"), SendResult("sent-2", "sent-ref-2", {})]
    )
    application, _, _, _ = _application(repository=repository, transport=transport)

    result = _send(
        application,
        {
            "adapter": "QQ",
            "scene": "group",
            "target_id": "group-7",
            "content": "hello",
        },
    )

    assert result.to_dict() == {
        "success": True,
        "message": "发送成功",
        "message_id": "sent-2",
        "reference_id": "sent-ref-2",
        "source_message_id": "second",
        "source_reference_id": "second-ref",
        "quote_reference_id": "",
    }
    assert [request.source_message_id for _, request in transport.requests] == [
        "first",
        "second",
    ]
    assert repository.calls == [
        ("latest", {"scene": "group", "target_id": "group-7", "limit": 3})
    ]


def test_qq_sticker_resolves_through_existing_owner_and_preserves_record_token():
    stickers = StickerApplicationFake(Path("/stickers/2.webp"))
    transport = TransportFake()
    application, built_messages, _, _ = _application(
        stickers=stickers,
        transport=transport,
    )

    result = _send(
        application,
        {
            "adapter": "QQ",
            "scene": "group",
            "target_id": "group-7",
            "content": "ignored",
            "sticker": "pack-a/2",
            "active_send": True,
        },
    )

    assert result.to_dict()["success"] is True
    assert stickers.calls == ["pack-a/2"]
    assert built_messages[-1]["content"] == ""
    assert built_messages[-1]["media_type"] == "image"
    assert built_messages[-1]["media_input"] == Path("/stickers/2.webp")
    assert transport.requests[0][1].record_message == "<sticker[pack-a/2]>"


def test_uploaded_media_is_saved_by_port_and_markdown_downgrades_to_plain():
    upload_file = SimpleNamespace(filename="picture.png")
    saved_path = Path("/uploads/picture.png")
    saved = []
    transport = TransportFake()
    application, built_messages, _, _ = _application(
        upload_saver=lambda upload: saved.append(upload) or saved_path,
        transport=transport,
    )

    result = _send(
        application,
        {
            "adapter": "QQ",
            "scene": "group",
            "target_id": "group-7",
            "content": "caption",
            "send_mode": "markdown",
            "media_type": "image",
            "active_send": True,
        },
        upload_file=upload_file,
    )

    assert result.to_dict()["success"] is True
    assert saved == [upload_file]
    assert built_messages[-1]["send_mode"] == "plain"
    assert built_messages[-1]["media_input"] == saved_path


def test_bad_quote_and_missing_reply_candidates_do_not_send():
    repository = ReplyRepositoryFake()
    application, _, _, _ = _application(repository=repository)

    bad_quote = _send(
        application,
        {
            "adapter": "QQ",
            "scene": "group",
            "target_id": "group-7",
            "content": "hello",
            "quote_reference_id": "missing-ref",
        },
    )
    no_candidates = _send(
        application,
        {
            "adapter": "QQ",
            "scene": "group",
            "target_id": "group-7",
            "content": "hello",
        },
    )

    assert bad_quote.to_dict() == {
        "success": False,
        "error": "指定引用消息不可用：可能不属于当前会话，或消息记录已不存在",
    }
    assert no_candidates.to_dict()["error"].startswith("QQ 适配器无法发送：")
