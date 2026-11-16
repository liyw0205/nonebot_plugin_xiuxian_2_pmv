from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import tempfile
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
from zoneinfo import ZoneInfo

import nonebot
import pytest
from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from nonebot.exception import FinishedException

nonebot.init()
try:
    nonebot.load_plugin("nonebot_plugin_apscheduler")
except RuntimeError:
    pass

from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_trade as trade
from nonebot_plugin_xiuxian_2.features.trade.guishi_cancel_repository import GuishiOrderCancelResult
from nonebot_plugin_xiuxian_2.features.trade.guishi_expired_repository import GuishiExpiredOrderClearResult
from nonebot_plugin_xiuxian_2.features.trade.guishi_match_repository import GuishiMatchResult
from nonebot_plugin_xiuxian_2.compatibility.legacy_jobs import _TARGETS
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import utils
from nonebot_plugin_xiuxian_2.bootstrap import build_runtime_context
from nonebot_plugin_xiuxian_2.plugin import shutdown, startup


BUSINESS_TZ = ZoneInfo("Asia/Shanghai")


class FixedClock:
    def __init__(self, moment: datetime) -> None:
        self.moment = moment

    def now(self) -> datetime:
        return self.moment


def _utc(day: int, hour: int, minute: int = 0) -> datetime:
    return datetime(2026, 11, day, hour, minute, tzinfo=timezone.utc)


def test_guishi_jobs_are_registered_on_live_scheduler() -> None:
    bridge = trade.scheduler
    if hasattr(bridge, "activate"):
        bridge.activate()
    scheduler = getattr(bridge, "_scheduler", bridge)
    for job_id, handler in (
        ("auto_guishi_transactions", "auto_guishi_transactions_job"),
        ("clear_expired_baitan_orders", "clear_expired_baitan_orders_job"),
    ):
        assert _TARGETS[job_id] == ("xiuxian.xiuxian_trade", handler)
        assert scheduler.get_job(job_id) is not None
    trigger = scheduler.get_job("clear_expired_baitan_orders").trigger
    assert trigger.get_next_fire_time(None, _utc(17, 23, 59)) == _utc(18, 0)


def test_runtime_trade_application_receives_the_scheduler_business_timezone():
    async def run():
        with tempfile.TemporaryDirectory() as directory, patch(
            "nonebot_plugin_xiuxian_2.compatibility.scheduler.scheduler_bridge",
            return_value=SimpleNamespace(timezone=timezone.utc),
        ):
            state, _readiness, context, lifecycle = await startup(
                build_runtime_context(data_dir=directory, legacy_startup=False)
            )
            try:
                assert state.phase.value in {"ready", "not_ready"}
                application = context.services["trade"]
                assert application.clock is context.clock
                assert application.business_timezone is timezone.utc
                assert application.guishi_qiugou_expiry_service.business_timezone is timezone.utc
                assert application.guishi_order_status_service.business_timezone is timezone.utc
            finally:
                await shutdown(lifecycle)

    asyncio.run(run())


@pytest.mark.parametrize(
    ("now", "expected"),
    (
        (_utc(17, 12, 0), (20, True)),
        (_utc(17, 15, 59), (23, True)),
        (_utc(17, 16, 0), (0, True)),
        (_utc(17, 23, 59), (7, True)),
        (_utc(18, 0, 0), (8, False)),
    ),
)
def test_guishi_business_projection_uses_scheduler_timezone(now, expected) -> None:
    scheduler = SimpleNamespace(timezone=BUSINESS_TZ)
    with patch.object(trade, "scheduler", scheduler), patch.object(
        trade, "runtime_clock", FixedClock(now)
    ):
        projected = trade._trade_business_now()
        assert (projected.hour, projected.minute) == (expected[0], now.minute)
        assert (projected.hour < 8 or projected.hour >= 20) is expected[1]


def test_guishi_matching_job_does_not_swallow_failure() -> None:
    with patch.object(
        trade,
        "process_guishi_transactions",
        side_effect=RuntimeError("trade database unavailable"),
    ):
        with pytest.raises(RuntimeError, match="trade database unavailable"):
            asyncio.run(trade.auto_guishi_transactions_job())


@pytest.mark.parametrize(
    ("now", "open_window"),
    (
        (_utc(17, 11, 59), False),
        (_utc(17, 12), True),
        (_utc(17, 23, 59), True),
        (_utc(18, 0), False),
    ),
)
def test_stall_handler_checks_local_window_before_processing_order(now, open_window):
    sender = AsyncMock()
    with patch.object(trade, "scheduler", SimpleNamespace(timezone=BUSINESS_TZ)), patch.object(
        trade, "runtime_clock", FixedClock(now)
    ), patch.object(trade, "assign_bot", new=AsyncMock(return_value=(object(), None))), patch.object(
        trade, "check_user", return_value=(True, {"user_id": "seller"}, "")
    ), patch.object(trade, "handle_send", sender):
        with pytest.raises(FinishedException):
            asyncio.run(trade.guishi_baitan_(object(), object(), SimpleNamespace(extract_plain_text=lambda: "")))

    reply = sender.call_args.args[2]
    assert ("指令格式" in reply) is open_window
    assert ("下次可摆摊时间" in reply) is not open_window
    if not open_window:
        assert "20:00" in reply


@pytest.mark.parametrize(
    ("now", "weekend"),
    ((_utc(20, 15, 59), False), (_utc(20, 16), True), (_utc(22, 16), False)),
)
def test_take_handler_uses_local_weekend_boundary(now, weekend):
    sender = AsyncMock()
    with patch.object(trade, "scheduler", SimpleNamespace(timezone=BUSINESS_TZ)), patch.object(
        trade, "runtime_clock", FixedClock(now)
    ), patch.object(trade, "assign_bot", new=AsyncMock(return_value=(object(), None))), patch.object(
        trade, "check_user", return_value=(True, {"user_id": "buyer"}, "")
    ), patch.object(trade, "handle_send", sender):
        with pytest.raises(FinishedException):
            asyncio.run(trade.guishi_take_item_(object(), object(), SimpleNamespace(extract_plain_text=lambda: "")))
    reply = sender.call_args.args[2]
    assert ("请输入要取出" in reply) is weekend
    assert ("仅在周六和周日" in reply) is not weekend


@pytest.mark.parametrize("status", ("operation_conflict", "qiugou_invalid", "baitan_invalid"))
def test_matching_failure_result_reaches_scheduler_without_completion_log(status):
    want = {"id": "want", "user_id": "buyer", "item_name": "herb", "price": 20,
            "quantity": 1, "filled_quantity": 0}
    stall = {"id": "sell", "user_id": "seller", "price": 20}
    with patch.object(trade.trade_application, "guishi_get_orders", side_effect=[[want], [stall]]), patch.object(
        trade.trade_application, "guishi_match", return_value=GuishiMatchResult(status, "want", "sell")
    ), patch.object(trade, "logger") as logger:
        with pytest.raises(RuntimeError, match=status):
            asyncio.run(trade.auto_guishi_transactions_job())
    assert not any("处理完成" in str(call) for call in logger.info.call_args_list)


@pytest.mark.parametrize("status", ("order_missing", "baitan_completed", "not_matchable"))
def test_matching_expected_race_remains_a_normal_skip(status):
    want = {"id": "want", "user_id": "buyer", "item_name": "herb", "price": 20,
            "quantity": 1, "filled_quantity": 0}
    stall = {"id": "sell", "user_id": "seller", "price": 20}
    with patch.object(trade.trade_application, "guishi_get_orders", side_effect=[[want], [stall]]), patch.object(
        trade.trade_application, "guishi_match", return_value=GuishiMatchResult(status, "want", "sell")
    ):
        asyncio.run(trade.auto_guishi_transactions_job())


def test_expiry_cancel_failure_result_reaches_scheduler():
    failure = GuishiOrderCancelResult("operation_conflict", "want")
    with patch.object(trade.trade_application, "guishi_expire_qiugou", return_value=[failure]):
        with pytest.raises(RuntimeError, match="want.*operation_conflict"):
            asyncio.run(trade.clear_expired_baitan_orders_job())


def test_info_handler_reports_expiry_rejection_without_showing_a_normal_account():
    failure = GuishiOrderCancelResult("operation_conflict", "want")
    sender = AsyncMock()
    with patch.object(trade, "assign_bot", new=AsyncMock(return_value=(object(), None))), patch.object(
        trade, "check_user", return_value=(True, {"user_id": "buyer"}, "")
    ), patch.object(trade.trade_application, "guishi_expire_qiugou", return_value=[failure]), patch.object(
        trade.trade_application, "guishi_get_account"
    ) as account, patch.object(trade, "handle_send", sender):
        with pytest.raises(FinishedException):
            asyncio.run(trade.guishi_info_(object(), object(), SimpleNamespace(extract_plain_text=lambda: "")))
    reply = sender.call_args.args[2]
    assert "want" in reply and "到期处理未完成" in reply
    assert "不同内容的请求" in reply
    account.assert_not_called()


def test_stall_cleanup_failure_result_reaches_scheduler():
    order = {"id": "sell", "user_id": "seller", "item_name": "herb"}
    failure = GuishiExpiredOrderClearResult("operation_conflict", "sell")
    with patch.object(trade.trade_application, "guishi_expire_qiugou", return_value=[]), patch.object(
        trade.trade_application, "guishi_get_orders", return_value=[order]
    ), patch.object(trade.items, "get_data_by_item_name", return_value=(1001, {"type": "药材"})), patch.object(
        trade.trade_application, "guishi_clear_expired_baitan", return_value=failure
    ):
        with pytest.raises(RuntimeError, match="sell.*operation_conflict"):
            asyncio.run(trade.clear_expired_baitan_orders_job())


@pytest.mark.parametrize("fails", (False, True))
def test_actual_apscheduler_execution_reports_job_outcome(fails):
    async def run():
        scheduler = AsyncIOScheduler(timezone=BUSINESS_TZ)
        outcome = asyncio.get_running_loop().create_future()

        def completed(event):
            if not outcome.done():
                outcome.set_result(event)

        scheduler.add_listener(completed, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)
        scheduler.add_job(trade.auto_guishi_transactions_job, "date", id="auto_guishi_transactions")
        callback = AsyncMock(side_effect=RuntimeError("trade database unavailable") if fails else None)
        try:
            with patch.object(trade, "process_guishi_transactions", callback):
                scheduler.start()
                event = await asyncio.wait_for(outcome, timeout=3)
            assert event.job_id == "auto_guishi_transactions"
            assert event.code == (EVENT_JOB_ERROR if fails else EVENT_JOB_EXECUTED)
            if fails:
                assert str(event.exception) == "trade database unavailable"
            callback.assert_awaited_once_with()
        finally:
            if scheduler.running:
                scheduler.shutdown(wait=False)

    asyncio.run(run())


@pytest.mark.parametrize(
    ("onebot", "markdown", "send_fails"),
    ((True, True, False), (False, False, False), (False, True, False), (False, True, True)),
)
def test_window_rejection_keeps_its_state_across_message_adapters(onebot, markdown, send_fails):
    captured = AsyncMock()
    event = SimpleNamespace(get_user_id=lambda: "seller")
    with patch.object(trade, "scheduler", SimpleNamespace(timezone=BUSINESS_TZ)), patch.object(
        trade, "runtime_clock", FixedClock(_utc(18, 0))
    ), patch.object(trade, "assign_bot", new=AsyncMock(return_value=(object(), None))), patch.object(
        trade, "check_user", return_value=(True, {"user_id": "seller"}, "")
    ), patch.object(trade, "handle_send", captured):
        with pytest.raises(FinishedException):
            asyncio.run(trade.guishi_baitan_(object(), event, SimpleNamespace(extract_plain_text=lambda: "")))
    reply = captured.call_args.args[2]
    assert "下次可摆摊时间" in reply

    bot = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: "OneBot V11" if onebot else "QQ"))
    config = SimpleNamespace(markdown_status=markdown, markdown_id2="", markdown_button_status=False,
                             at_sender=False, message_optimization=False, img=False)
    plain_send = AsyncMock()
    markdown_send = AsyncMock(side_effect=RuntimeError("send failed") if send_fails else None)
    with patch.object(utils, "XiuConfig", return_value=config), patch.object(
        utils, "get_chat_scene", return_value="group"
    ), patch.object(utils, "is_group_event", return_value=True), patch.object(
        utils, "get_real_id_async", new=AsyncMock(return_value="")
    ), patch.object(utils, "_send_event_message", plain_send), patch.object(
        utils, "_send_markdown_or_keyboard", markdown_send
    ):
        asyncio.run(utils.handle_send(bot, event, reply, **captured.call_args.kwargs))

    if onebot or not markdown or send_fails:
        plain_send.assert_awaited_once()
        assert plain_send.call_args.kwargs["message"] == reply
    else:
        plain_send.assert_not_awaited()
        markdown_send.assert_awaited_once()
        rendered = markdown_send.call_args.args[2]
        assert "下次可摆摊时间" in rendered
        assert "鬼市摆摊" in rendered and "mqqapi://aio/inlinecmd" in rendered
