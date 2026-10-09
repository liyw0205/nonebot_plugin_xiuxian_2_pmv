from __future__ import annotations

from typing import Any, Callable, Mapping

from ...xiuxian.messaging import SendRequest
from .repository import (
    MessageReplyLookupPort,
    MessageTransportPort,
    StickerPathResolverPort,
    WebSendRecorderPort,
)
# Every accepted request shape, the refusal strings and the response envelope are
# declared once in schemas.py; the legacy console keeps parsing that dict shape.
from .schemas import (
    ACTIVE_SEND_TRUE_VALUES,
    ALLOWED_MEDIA_TYPES,
    DEFAULT_SEND_MODE,
    SCENES,
    SEND_MODES,
    STICKER_MEDIA_TYPE,
    MessageSendResult,
)


class WebMessageSendApplication:
    """Own Web send policy while delegating adapter and storage effects to ports."""

    def __init__(
        self,
        *,
        bot_resolver: Callable[[str], Any],
        bot_id_resolver: Callable[[Any], Any],
        is_ob11_adapter: Callable[[str], bool],
        message_builder: Callable[..., Any],
        reply_repository_factory: Callable[[], MessageReplyLookupPort],
        sticker_application: StickerPathResolverPort,
        upload_saver: Callable[[Any], Any],
        transport: MessageTransportPort,
        message_id_extractor: Callable[[Any], Any],
        web_send_recorder: WebSendRecorderPort,
        direct_api: Callable[..., Any] | None = None,
        logger: Any = None,
    ) -> None:
        self._bot_resolver = bot_resolver
        self._bot_id_resolver = bot_id_resolver
        self._is_ob11_adapter = is_ob11_adapter
        self._message_builder = message_builder
        self._reply_repository_factory = reply_repository_factory
        self._sticker_application = sticker_application
        self._upload_saver = upload_saver
        self._transport = transport
        self._message_id_extractor = message_id_extractor
        self._web_send_recorder = web_send_recorder
        self._direct_api = direct_api or self._call_adapter_api
        self._logger = logger

    @staticmethod
    async def _call_adapter_api(bot: Any, api: str, **kwargs: Any) -> Any:
        return await bot.call_api(api, **kwargs)

    @staticmethod
    def _result(**body: Any) -> MessageSendResult:
        return MessageSendResult(body)

    async def send(
        self,
        data: Mapping[str, Any],
        *,
        upload_file: Any = None,
    ) -> MessageSendResult:
        try:
            return await self._send(data, upload_file=upload_file)
        except Exception as exc:  # Preserve the legacy catch-all JSON envelope.
            if self._logger is not None:
                self._logger.error(f"Web 消息发送失败: {exc}")
            return MessageSendResult.failure(f"发送失败: {exc}")

    async def _send(
        self,
        data: Mapping[str, Any],
        *,
        upload_file: Any,
    ) -> MessageSendResult:
        adapter = str(data.get("adapter", "")).strip()
        scene = str(data.get("scene", "")).strip()
        target_id = str(data.get("target_id", "")).strip()
        content = str(data.get("content", "") or "")
        send_mode = str(data.get("send_mode", DEFAULT_SEND_MODE) or DEFAULT_SEND_MODE).strip()
        media_type = str(data.get("media_type", "") or "").strip()
        media_url = str(data.get("media_url", "") or "").strip()
        sticker_token = str(
            data.get("sticker", "") or data.get("sticker_token", "") or ""
        ).strip()
        reply_message_id = str(data.get("reply_message_id", "") or "").strip()
        quote_message_id = str(data.get("quote_message_id", "") or "").strip()
        quote_reference_id = str(data.get("quote_reference_id", "") or "").strip()
        active_send = (
            str(data.get("active_send", "") or "").strip().lower() in ACTIVE_SEND_TRUE_VALUES
        )

        reply_from_quote_message_id = False
        if (
            quote_message_id
            and not quote_message_id.startswith("REFIDX")
            and not reply_message_id
            and not quote_reference_id
        ):
            reply_message_id = quote_message_id
            reply_from_quote_message_id = True

        if send_mode not in SEND_MODES:
            send_mode = DEFAULT_SEND_MODE

        if send_mode == "markdown":
            quote_message_id = ""
            quote_reference_id = ""
            if reply_from_quote_message_id:
                reply_message_id = ""

        if media_type and media_type not in ALLOWED_MEDIA_TYPES:
            return MessageSendResult.failure("无效 media_type")
        if not adapter:
            return MessageSendResult.failure("缺少 adapter")
        if scene not in SCENES:
            return MessageSendResult.failure("无效 scene")
        if not target_id:
            return MessageSendResult.failure("缺少 target_id")
        if not content and not media_url and not upload_file and not sticker_token:
            return MessageSendResult.failure("消息不能为空")

        bot = self._bot_resolver(adapter)
        if not bot:
            return MessageSendResult.failure(f"未找到在线 {adapter} Bot")
        bot_id = self._bot_id_resolver(bot)

        media_input = None
        if sticker_token:
            sticker_path = self._sticker_application.resolve_sticker_path(sticker_token)
            if sticker_path is None:
                return MessageSendResult.failure("表情包不存在或未安装")
            media_type = STICKER_MEDIA_TYPE
            media_input = sticker_path
            content = ""
            send_mode = "plain"
        elif media_url:
            media_input = media_url
        elif upload_file:
            media_input = self._upload_saver(upload_file)

        if send_mode == "markdown" and media_input is not None:
            send_mode = "plain"

        if self._is_ob11_adapter(adapter):
            if send_mode == "markdown":
                return await self._send_ob11_markdown(
                    bot,
                    bot_id=bot_id,
                    scene=scene,
                    target_id=target_id,
                    content=content,
                )

            message = self._message_builder(
                bot,
                content=content,
                send_mode="plain",
                media_type=media_type,
                media_input=media_input,
            )
            if scene in ("group", "private"):
                result = await self._transport.send(
                    bot, SendRequest(scene, target_id, message)
                )
                return self._result(
                    success=True,
                    message="发送成功",
                    message_id=result.message_id,
                )
            return MessageSendResult.failure(
                "OneBot V11 暂只支持 group/private 主动发送"
            )

        message = self._message_builder(
            bot,
            content=content,
            send_mode=send_mode,
            media_type=media_type,
            media_input=media_input,
            quote_message_id="" if adapter == "QQ" else quote_message_id,
        )
        if adapter != "QQ":
            return MessageSendResult.failure(f"暂不支持适配器: {adapter}")

        return await self._send_qq(
            bot,
            reply_repository=self._reply_repository_factory(),
            scene=scene,
            target_id=target_id,
            content=content,
            send_mode=send_mode,
            media_type=media_type,
            media_input=media_input,
            sticker_token=sticker_token,
            active_send=active_send,
            reply_message_id=reply_message_id,
            quote_message_id=quote_message_id,
            quote_reference_id=quote_reference_id,
        )

    async def _send_ob11_markdown(
        self,
        bot: Any,
        *,
        bot_id: Any,
        scene: str,
        target_id: str,
        content: str,
    ) -> MessageSendResult:
        if scene == "group":
            api = "send_group_forward_msg"
            kwargs = {"group_id": int(target_id)}
            success_message = "Markdown 已通过合并转发发送"
        elif scene == "private":
            api = "send_private_forward_msg"
            kwargs = {"user_id": int(target_id)}
            success_message = "Markdown 已通过私聊合并转发发送"
        else:
            return MessageSendResult.failure(
                "OneBot V11 Markdown 合并转发暂只支持 group/private"
            )

        node = {
            "type": "node",
            "data": {
                "name": "聊天记录",
                "uin": str(bot_id or getattr(bot, "self_id", "10000") or "10000"),
                "content": content or " ",
            },
        }
        result = await self._direct_api(bot, api, messages=[node], **kwargs)
        message_id = self._message_id_extractor(result)
        self._web_send_recorder(
            bot,
            scene=scene,
            message_id=message_id,
            source_message_id="",
            group_id=target_id if scene == "group" else "",
            user_id=target_id if scene == "private" else "",
            message=content,
        )
        return self._result(
            success=True,
            message=success_message,
            message_id=message_id,
        )

    def _resolve_qq_quote_reference_id(
        self,
        reply_repository: Any,
        *,
        scene: str,
        target_id: str,
        quote_message_id: str,
        quote_reference_id: str,
    ) -> tuple[str, str]:
        if quote_reference_id:
            candidate = reply_repository.get_specific_reference_candidate_for_qq(
                scene=scene,
                target_id=target_id,
                reference_id=quote_reference_id,
            )
            if candidate:
                return str(candidate.get("reference_id") or quote_reference_id), ""
            return "", "指定引用消息不可用：可能不属于当前会话，或消息记录已不存在"

        if not quote_message_id:
            return "", ""
        if quote_message_id.startswith("REFIDX"):
            return quote_message_id, ""

        candidate = reply_repository.get_specific_reference_candidate_for_qq(
            scene=scene,
            target_id=target_id,
            message_id=quote_message_id,
        )
        if candidate:
            reference_id = str(candidate.get("reference_id") or "")
            if reference_id:
                return reference_id, ""
            if scene in ("channel_group", "channel_private"):
                return str(candidate.get("message_id") or ""), ""

        if scene in ("channel_group", "channel_private"):
            return quote_message_id, ""
        return "", ""

    async def _send_qq(
        self,
        bot: Any,
        *,
        reply_repository: Any,
        scene: str,
        target_id: str,
        content: str,
        send_mode: str,
        media_type: str,
        media_input: Any,
        sticker_token: str,
        active_send: bool,
        reply_message_id: str,
        quote_message_id: str,
        quote_reference_id: str,
    ) -> MessageSendResult:
        message_reference_id, reference_error = self._resolve_qq_quote_reference_id(
            reply_repository=reply_repository,
            scene=scene,
            target_id=target_id,
            quote_message_id=quote_message_id,
            quote_reference_id=quote_reference_id,
        )
        if reference_error:
            return MessageSendResult.failure(reference_error)

        def build_message(reference_id: str = ""):
            return self._message_builder(
                bot,
                content=content,
                send_mode=send_mode,
                media_type=media_type,
                media_input=media_input,
                quote_message_id=reference_id,
            )

        if active_send:
            try:
                source_message_id = ""
                if reply_message_id:
                    candidate = reply_repository.get_specific_reply_candidate_for_qq(
                        scene=scene,
                        target_id=target_id,
                        message_id=reply_message_id,
                    )
                    if not candidate:
                        return MessageSendResult.failure(
                            "指定 msg_id 不可用：可能已过期、超过回复次数，或不属于当前会话"
                        )
                    source_message_id = str(candidate.get("message_id") or "")

                if scene not in SCENES:
                    return MessageSendResult.failure("无效 QQ scene")
                send_result = await self._transport.send(
                    bot,
                    SendRequest(
                        scene,
                        target_id,
                        build_message(message_reference_id),
                        reference_id=message_reference_id or None,
                        source_message_id=source_message_id or None,
                        record_message=(
                            f"<sticker[{sticker_token}]>" if sticker_token else None
                        ),
                    ),
                )
                return self._result(
                    success=True,
                    message="QQ 主动发送成功",
                    message_id=send_result.message_id,
                    reference_id=send_result.reference_id,
                    source_message_id=source_message_id,
                    quote_reference_id=message_reference_id,
                )
            except Exception as exc:
                if self._logger is not None:
                    self._logger.warning(
                        f"QQ Web 主动发送失败: scene={scene}, "
                        f"target_id={target_id}, error={exc}"
                    )
                return MessageSendResult.failure(f"QQ 主动发送失败：{exc}")

        if reply_message_id:
            candidate = reply_repository.get_specific_reply_candidate_for_qq(
                scene=scene,
                target_id=target_id,
                message_id=reply_message_id,
            )
            if not candidate:
                return MessageSendResult.failure(
                    "指定回复消息不可用：可能已过期、超过回复次数，或不属于当前会话"
                )
            candidates = [candidate]
        else:
            candidates = reply_repository.get_latest_reply_candidates_for_qq(
                scene=scene,
                target_id=target_id,
                limit=3,
            )

        if not candidates:
            return MessageSendResult.failure(
                "QQ 适配器无法发送：非主动发送需要 4 分钟内可用 msg_id，请选择“使用 msg_id”或开启主动发送"
            )

        last_error = ""
        for candidate in candidates:
            source_message_id = str(candidate.get("message_id", "") or "")
            if not source_message_id:
                continue
            try:
                source_reference_id = str(candidate.get("reference_id") or "")
                if scene not in SCENES:
                    return MessageSendResult.failure("无效 QQ scene")
                send_result = await self._transport.send(
                    bot,
                    SendRequest(
                        scene,
                        target_id,
                        build_message(message_reference_id),
                        reference_id=message_reference_id or None,
                        source_message_id=source_message_id,
                        record_message=(
                            f"<sticker[{sticker_token}]>" if sticker_token else None
                        ),
                    ),
                )
                return self._result(
                    success=True,
                    message="发送成功",
                    message_id=send_result.message_id,
                    reference_id=send_result.reference_id,
                    source_message_id=source_message_id,
                    source_reference_id=source_reference_id,
                    quote_reference_id=message_reference_id,
                )
            except Exception as exc:
                last_error = str(exc)
                if self._logger is not None:
                    self._logger.warning(
                        f"QQ Web 发送失败，尝试下一条候选: "
                        f"scene={scene}, target_id={target_id}, "
                        f"source_message_id={source_message_id}, error={exc}"
                    )

        return MessageSendResult.failure(f"QQ 回复式发送失败，最后错误: {last_error}")


__all__ = ["MessageSendResult", "WebMessageSendApplication"]
