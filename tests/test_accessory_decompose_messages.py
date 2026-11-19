import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import nonebot
import pytest

nonebot.init()

from nonebot_plugin_xiuxian_2.features.back.accessory_decompose_domain import AccessoryDecomposeChange
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_back import accessory
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import utils


@pytest.mark.parametrize("adapter,markdown,fails", [
    ("OneBot V11", True, False),
    ("QQ", False, False),
    ("QQ", True, False),
    ("QQ", True, True),
])
@pytest.mark.parametrize("reason,phrase", [
    ("accessory_decompose_stone_write_rejected", "分解所得洗练石未能保存"),
    ("accessory_decompose_stone_projection_mismatch", "分解所得洗练石与应得数量不一致"),
    ("accessory_decompose_state_write_rejected", "饰品分解状态保存失败"),
    ("accessory_decompose_state_projection_mismatch", "饰品分解状态与请求不一致"),
    ("accessory_decompose_receipt_write_rejected", "饰品分解操作回执保存失败"),
    ("accessory_decompose_receipt_projection_mismatch", "饰品分解操作回执不一致"),
])
def test_decompose_handler_keeps_reason_and_uid_across_adapter_delivery(adapter, markdown, fails, reason, phrase):
    uid = "adapter-accessory-1"
    target = {"uid": uid, "name": "烈阳戒指", "quality": 1}
    event = SimpleNamespace(get_user_id=lambda: "900001", message_id=970700)
    bot = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: adapter))
    config = SimpleNamespace(
        max_goods_num=999,
        markdown_status=markdown,
        markdown_id="",
        markdown_id2="",
        markdown_button_status=False,
        at_sender=False,
        message_optimization=False,
        img=False,
    )
    result = AccessoryDecomposeChange("failed", "decompose", "900001", reason=reason, subject=uid)
    application = SimpleNamespace(replay=Mock(return_value=None), decompose=Mock(return_value=result))
    plain = AsyncMock()
    markdown_send = AsyncMock(side_effect=RuntimeError("isolated decompose delivery failure") if fails else None)
    with (
        patch.object(accessory, "check_user", return_value=(True, {"user_id": "900001"}, "")),
        patch.object(accessory, "_get_data", return_value={"equipped": {}, "bag": [target]}),
        patch.object(accessory, "_decompose_application", return_value=application),
        patch.object(accessory, "XiuConfig", return_value=config),
        patch.object(accessory, "handle_send", utils.handle_send),
        patch.object(utils, "XiuConfig", return_value=config),
        patch.object(utils, "get_chat_scene", return_value="group"),
        patch.object(utils, "is_group_event", return_value=True),
        patch.object(utils, "get_real_id_async", new=AsyncMock(return_value="")),
        patch.object(utils, "_send_event_message", plain),
        patch.object(utils, "_send_markdown_or_keyboard", markdown_send),
    ):
        asyncio.run(accessory.decompose_accessory.handlers[0].call(
            bot, event, SimpleNamespace(extract_plain_text=lambda: uid),
        ))
    application.decompose.assert_called_once()
    if adapter == "OneBot V11" or not markdown or fails:
        plain.assert_awaited_once()
        reply = str(plain.call_args.kwargs["message"])
    else:
        markdown_send.assert_awaited_once()
        reply = markdown_send.call_args.args[2]
    assert phrase in reply and uid in reply
    assert "已分解：" not in reply
