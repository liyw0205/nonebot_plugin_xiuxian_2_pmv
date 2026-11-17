"""Bulk clear reporting preserves partial commits and specific failures across adapters."""
import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import pytest
from nonebot.exception import FinishedException

from tests import test_guishi_schedule as runtime
from nonebot_plugin_xiuxian_2.features.trade.guishi_cancel_repository import GuishiOrderCancelResult

trade, utils = runtime.trade, runtime.utils
ORDERS = [dict(id='1001', user_id='buyer', item_type='qiugou', item_name='herb'),
          dict(id='1002', user_id='buyer', item_type='qiugou', item_name='herb')]


@pytest.mark.parametrize('reason', ('guishi_refund_balance_write_rejected', 'guishi_refund_order_delete_rejected',
                                   'guishi_refund_receipt_write_rejected', 'guishi_refund_order_invalid'))
@pytest.mark.parametrize('onebot,markdown,send_fails', ((True, True, False), (False, False, False),
                                                     (False, True, False), (False, True, True)))
def test_partial_clear_keeps_reason_and_successful_refund_in_adapter_reply(reason, onebot, markdown, send_fails):
    results = [GuishiOrderCancelResult('cancelled', '1001', 'qiugou', 'buyer', refunded_stone=30),
               GuishiOrderCancelResult('state_changed', '1002', 'qiugou', 'buyer', reason=reason, subject='1002')]
    captured = AsyncMock()
    event = SimpleNamespace(get_user_id=lambda: 'admin')
    with patch.object(trade, 'assign_bot', new=AsyncMock(return_value=(object(), None))), \
            patch.object(trade, 'SUPERUSER', new=AsyncMock(return_value=True)), \
            patch.object(trade.trade_application, 'guishi_get_orders', side_effect=[ORDERS, ORDERS[1:]]), \
            patch.object(trade.trade_application, 'guishi_cancel_qiugou', side_effect=results), \
            patch.object(trade, '_sql_message', return_value=SimpleNamespace(get_user_info_with_id=lambda uid: {'user_name': 'buyer'})), \
            patch.object(trade, 'handle_send', captured):
        with pytest.raises(FinishedException):
            asyncio.run(trade.clear_all_guishi_(object(), event))
    reply = captured.call_args.args[2]
    assert '本次处理 1 条' in reply and '当前剩余 1 条' in reply and 'buyer: 30灵石' in reply
    assert reason in reply and '订单1002' in reply and '成功清空鬼市' not in reply
    bot = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: 'OneBot V11' if onebot else 'QQ'))
    config = SimpleNamespace(markdown_status=markdown, markdown_id='', markdown_id2='', markdown_button_status=False,
                             at_sender=False, message_optimization=False, img=False)
    plain, md = AsyncMock(), AsyncMock(side_effect=RuntimeError('send failed') if send_fails else None)
    with patch.object(utils, 'XiuConfig', return_value=config), patch.object(utils, 'get_chat_scene', return_value='group'), \
            patch.object(utils, 'is_group_event', return_value=True), \
            patch.object(utils, 'get_real_id_async', new=AsyncMock(return_value='')), \
            patch.object(utils, '_send_event_message', plain), patch.object(utils, '_send_markdown_or_keyboard', md):
        asyncio.run(utils.handle_send(bot, event, reply, **captured.call_args.kwargs))
    if onebot or not markdown or send_fails:
        plain.assert_awaited_once()
        assert plain.call_args.kwargs['message'] == reply
    else:
        rendered = md.call_args.args[2]
        assert reason in rendered and '订单1002' in rendered and '当前剩余 1 条' in rendered


@pytest.mark.parametrize('stage', ('initial', 'remaining', 'name'))
def test_read_errors_are_reported_without_losing_committed_outcome(stage):
    fault = RuntimeError('isolated read fault')
    captured = AsyncMock()
    rows = [fault] if stage == 'initial' else [ORDERS[:1], fault if stage == 'remaining' else []]
    name_lookup = Mock(side_effect=fault if stage == 'name' else None, return_value=None)
    with patch.object(trade, 'assign_bot', new=AsyncMock(return_value=(object(), None))), \
            patch.object(trade, 'SUPERUSER', new=AsyncMock(return_value=True)), \
            patch.object(trade.trade_application, 'guishi_get_orders', side_effect=rows), \
            patch.object(trade.trade_application, 'guishi_cancel_qiugou',
                         return_value=GuishiOrderCancelResult('cancelled', '1001', 'qiugou', 'buyer', refunded_stone=30)) as cancel, \
            patch.object(trade, '_sql_message', return_value=SimpleNamespace(get_user_info_with_id=name_lookup)), \
            patch.object(trade, 'report_exception', return_value='定位号：read-test') as reported, \
            patch.object(trade, 'handle_send', captured):
        with pytest.raises(FinishedException):
            asyncio.run(trade.clear_all_guishi_(object(), object()))
    reply = captured.call_args.args[2]
    assert reported.call_args.args[1] is fault and '定位号：read-test' in reply
    if stage == 'initial':
        cancel.assert_not_called()
        assert '成功清空' not in reply
    else:
        assert '本次处理 1 条' in reply and 'buyer: 30灵石' in reply
        if stage == 'remaining':
            assert '未能核实' in reply and '成功清空' not in reply


def test_unchanged_order_after_duplicate_does_not_report_all_cleared_or_refunded_again():
    captured = AsyncMock()
    with patch.object(trade, 'assign_bot', new=AsyncMock(return_value=(object(), None))), \
            patch.object(trade, 'SUPERUSER', new=AsyncMock(return_value=True)), \
            patch.object(trade.trade_application, 'guishi_get_orders', return_value=ORDERS[:1]), \
            patch.object(trade.trade_application, 'guishi_cancel_qiugou',
                         return_value=GuishiOrderCancelResult('duplicate', '1001', 'qiugou', 'buyer', refunded_stone=30)), \
            patch.object(trade, 'handle_send', captured):
        with pytest.raises(FinishedException):
            asyncio.run(trade.clear_all_guishi_(object(), object()))
    reply = captured.call_args.args[2]
    assert '本次处理 0 条' in reply and '当前剩余 1 条' in reply and '灵石退还' not in reply and '成功清空' not in reply


def test_missing_catalogue_and_unknown_type_are_explicit_and_preserved():
    orders = [dict(ORDERS[0], item_type='baitan'), dict(ORDERS[1], item_type='invalid')]
    captured = AsyncMock()
    with patch.object(trade, 'assign_bot', new=AsyncMock(return_value=(object(), None))), \
            patch.object(trade, 'SUPERUSER', new=AsyncMock(return_value=True)), \
            patch.object(trade.trade_application, 'guishi_get_orders', return_value=orders), \
            patch.object(trade.items, 'get_data_by_item_name', return_value=(None, None)), \
            patch.object(trade.trade_application, 'guishi_clear_expired_baitan') as clear, \
            patch.object(trade, 'handle_send', captured):
        with pytest.raises(FinishedException):
            asyncio.run(trade.clear_all_guishi_(object(), object()))
    reply = captured.call_args.args[2]
    clear.assert_not_called()
    assert all(text in reply for text in ('item_missing', '1001', 'guishi_order_type_invalid', '1002', '当前剩余 2 条'))


def test_normal_control_flow_and_failed_delivery_are_never_swallowed():
    delivery = RuntimeError('delivery failed')
    captured = AsyncMock(side_effect=[None, delivery])
    with patch.object(trade, 'assign_bot', new=AsyncMock(return_value=(object(), None))), \
            patch.object(trade, 'SUPERUSER', new=AsyncMock(return_value=True)), \
            patch.object(trade.trade_application, 'guishi_get_orders', return_value=ORDERS[:1]), \
            patch.object(trade.trade_application, 'guishi_cancel_qiugou',
                         return_value=GuishiOrderCancelResult('state_changed', '1001', reason='guishi_refund_receipt_write_rejected')) as cancel, \
            patch.object(trade, 'report_exception') as reported, patch.object(trade, 'handle_send', captured):
        with pytest.raises(RuntimeError) as caught:
            asyncio.run(trade.clear_all_guishi_(object(), object()))
        assert caught.value is delivery
        captured.side_effect = None
        cancel.side_effect = FinishedException()
        with pytest.raises(FinishedException):
            asyncio.run(trade.clear_all_guishi_(object(), object()))
        reported.assert_not_called()
