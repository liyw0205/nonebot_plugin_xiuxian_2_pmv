"""Auction lifecycle errors survive command flow and adapter fallbacks."""
import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import pytest
from nonebot.exception import FinishedException

from tests.test_guishi_schedule import trade, utils
from nonebot_plugin_xiuxian_2.features.auction.domain import AuctionLifecycleError
from nonebot_plugin_xiuxian_2.features.auction.pending import AuctionPendingClaimResult
from nonebot_plugin_xiuxian_2.features.auction.queue_repository import AuctionQueueResult


@pytest.mark.parametrize('command,reason', (('start', 'auction_start_session_inactive'),
                                           ('start', 'auction_start_item_write_rejected'),
                                           ('start', 'auction_start_queue_write_rejected'),
                                           ('start', 'auction_start_session_write_rejected'),
                                           ('start', 'auction_start_receipt_write_rejected'),
                                           ('end', 'auction_session_missing'),
                                           ('end', 'auction_settlement_inventory_write_rejected'),
                                           ('end', 'auction_settlement_balance_write_rejected'),
                                           ('end', 'auction_settlement_pending_write_rejected'),
                                           ('end', 'auction_settlement_history_write_rejected'),
                                           ('end', 'auction_settlement_item_delete_rejected'),
                                           ('end', 'auction_settlement_session_write_rejected'),
                                           ('end', 'auction_settlement_receipt_write_rejected'),
                                           ('claim', 'auction_claim_inventory_write_rejected'),
                                           ('claim', 'auction_claim_pending_write_rejected'),
                                           ('claim', 'auction_claim_receipt_write_rejected'),
                                           ('add', 'auction_queue_inventory_write_rejected'),
                                           ('add', 'auction_queue_queue_write_rejected'),
                                           ('add', 'auction_queue_receipt_write_rejected'),
                                           ('remove', 'auction_queue_inventory_write_rejected'),
                                           ('remove', 'auction_queue_queue_write_rejected'),
                                           ('remove', 'auction_queue_receipt_write_rejected'),
                                           ('bid', 'auction_expired')))
@pytest.mark.parametrize('onebot,markdown,send_fails', ((True, True, False), (False, False, False),
                                                     (False, True, False), (False, True, True)))
def test_lifecycle_reason_and_subject_survive_adapters(command, reason, onebot, markdown, send_fails):
    captured = AsyncMock()
    session = SimpleNamespace(get_active_session=lambda: None if command == 'start' else {'session_id': 'session-test'})
    query = SimpleNamespace(count_current_auctions=lambda: 1,
                            get_current_auction=lambda aid: {'id': aid, 'name': 'herb', 'seller_id': 'seller',
                                                            'start_price': 100, 'current_price': 100, 'bids': {}})
    bid = SimpleNamespace(place_bid=lambda **kwargs: SimpleNamespace(
        code=reason, status='rejected', data={'status': reason, 'reason': reason, 'subject': 'session-test'}))
    event = SimpleNamespace(get_user_id=lambda: 'buyer')
    args = SimpleNamespace(extract_plain_text=lambda: {'claim': '领取 sale', 'add': 'herb 1000000',
                                                      'remove': 'herb'}.get(command, 'a001 100'))
    queue_result = AuctionQueueResult('state_changed', command, 'buyer', 1001, reason=reason, subject='session-test')
    queue = SimpleNamespace(enqueue=lambda *args, **kwargs: queue_result, dequeue=lambda *args, **kwargs: queue_result,
                            get_operation=lambda *args: None, get_player_items=lambda *args: [{'item_id': 1001, 'item_name': 'herb'}])
    rules = dict(trade.auction_config.get_auction_rules(), min_price=1, max_user_items=3)
    with patch.object(trade, 'assign_bot', new=AsyncMock(return_value=(object(), None))), \
            patch.object(trade, 'SUPERUSER', new=AsyncMock(return_value=True)), \
            patch.object(trade, 'check_user', return_value=(True, {'user_id': 'buyer', 'user_name': 'buyer'}, '')), \
            patch.object(trade, '_auction_queue_application', return_value=queue), \
            patch.object(trade.items, 'get_data_by_item_name', return_value=(1001, {'type': '药材'})), \
            patch.object(trade.items, 'get_data_by_item_id', return_value={'type': '药材'}), \
            patch.object(trade, 'get_trade_forbid_reason', return_value=''), \
            patch.object(trade.auction_config, 'get_auction_rules', return_value=rules), \
            patch.object(trade, '_auction_session_service', return_value=session), \
            patch.object(trade, '_auction_query_application', return_value=query), \
            patch.object(trade, '_auction_bid_application', return_value=bid), \
            patch.object(trade, '_auction_pending_application', return_value=SimpleNamespace(claim=lambda **kwargs:
                         AuctionPendingClaimResult('state_changed', reason=reason, subject='session-test'))), \
            patch.object(trade, 'get_auction_status', return_value={'active': command not in {'add', 'remove'}}), \
            patch.object(trade, '_sql_message', return_value=SimpleNamespace(get_user_info_with_id=lambda uid: {'stone': 1000})), \
            patch.object(trade, '_auction_session_operation_id', return_value='operation-test'), \
            patch.object(trade.auction_config, 'update_schedule'), \
            patch.object(trade, '_start_auction_with_application', side_effect=AuctionLifecycleError(reason, 'session-test')), \
            patch.object(trade, '_end_auction_with_application', new=AsyncMock(side_effect=AuctionLifecycleError(reason, 'session-test'))), \
            patch.object(trade, 'handle_send', captured):
        callback = trade.my_auction_ if command == 'claim' else getattr(trade, f'auction_{command}_')
        with pytest.raises(FinishedException):
            asyncio.run(callback(object(), event, args) if command in {'bid', 'claim', 'add', 'remove'} else callback(object(), event))
    reply = captured.call_args.args[2]
    assert reason in reply and 'session-test' in reply
    bot = SimpleNamespace(adapter=SimpleNamespace(get_name=lambda: 'OneBot V11' if onebot else 'QQ'))
    config = SimpleNamespace(markdown_status=markdown, markdown_id='', markdown_id2='', markdown_button_status=False,
                             at_sender=False, message_optimization=False, img=False)
    plain = AsyncMock()
    md = AsyncMock(side_effect=RuntimeError('isolated send failure') if send_fails else None)
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
        assert reason in rendered and 'session-test' in rendered


@pytest.mark.parametrize('command', ('start', 'end', 'bid', 'claim'))
def test_unknown_fault_flow_exception_and_failed_delivery_propagate(command):
    fault = RuntimeError('isolated business fault')
    delivery = RuntimeError('isolated delivery fault')
    captured = AsyncMock()
    operation = Mock(side_effect=fault) if command in {'start', 'claim'} else AsyncMock(side_effect=fault)
    target = {'start': '_start_auction_with_application', 'end': '_end_auction_with_application',
              'bid': '_place_auction_bid_with_application', 'claim': '_auction_pending_application'}[command]
    session = SimpleNamespace(get_active_session=lambda: None if command == 'start' else {'session_id': 'session-test'})
    with patch.object(trade, 'assign_bot', new=AsyncMock(return_value=(object(), None))), \
            patch.object(trade, 'SUPERUSER', new=AsyncMock(return_value=True)), \
            patch.object(trade, 'check_user', return_value=(True, {'user_id': 'buyer'}, '')), \
            patch.object(trade, '_auction_session_service', return_value=session), \
            patch.object(trade, '_auction_query_application', return_value=SimpleNamespace(count_current_auctions=lambda: 1)), \
            patch.object(trade, '_auction_session_operation_id', return_value='test-op'), \
            patch.object(trade.auction_config, 'update_schedule'), patch.object(trade, target, operation), \
            patch.object(trade, 'report_exception', return_value='定位号：auction-test') as reported, \
            patch.object(trade, 'handle_send', captured):
        callback = trade.my_auction_ if command == 'claim' else getattr(trade, f'auction_{command}_')
        async def invoke():
            if command in {'bid', 'claim'}:
                await callback(object(), object(), SimpleNamespace(extract_plain_text=lambda: '领取 sale' if command == 'claim' else 'a001 100'))
            else:
                await callback(object(), object())
        with pytest.raises(FinishedException):
            asyncio.run(invoke())
        assert reported.call_args.args[1] is fault and 'auction-test' in captured.call_args.args[2]
        captured.side_effect = delivery
        with pytest.raises(RuntimeError) as caught:
            asyncio.run(invoke())
        assert caught.value is delivery
        operation.side_effect = FinishedException()
        reported.reset_mock()
        captured.reset_mock()
        with pytest.raises(FinishedException):
            asyncio.run(invoke())
        reported.assert_not_called()
        captured.assert_not_called()


@pytest.mark.parametrize('phase', ('enqueue', 'dequeue', 'get_operation', 'get_player_items'))
def test_queue_unknown_fault_flow_and_failed_delivery_propagate(phase):
    fault, delivery = RuntimeError('isolated queue fault'), RuntimeError('isolated queue delivery')
    operation = Mock(side_effect=fault)
    captured = AsyncMock()
    queue = SimpleNamespace(enqueue=lambda *args, **kwargs: None, dequeue=lambda *args: None,
                            get_operation=lambda *args: None,
                            get_player_items=lambda *args: [{'item_name': 'herb', 'item_id': 1001}])
    setattr(queue, phase, operation)
    command = 'add' if phase == 'enqueue' else 'remove'
    event = SimpleNamespace(message_id='queue-test')
    args = SimpleNamespace(extract_plain_text=lambda: 'herb 1000000' if command == 'add' else 'herb')
    with patch.object(trade, 'assign_bot', new=AsyncMock(return_value=(object(), None))), \
            patch.object(trade, 'check_user', return_value=(True, {'user_id': 'seller', 'user_name': 'seller'}, '')), \
            patch.object(trade, 'get_auction_status', return_value={'active': False}), \
            patch.object(trade.items, 'get_data_by_item_name', return_value=(1001, {'type': '药材'})), \
            patch.object(trade.items, 'get_data_by_item_id', return_value={'type': '药材'}), \
            patch.object(trade, 'get_trade_forbid_reason', return_value=''), \
            patch.object(trade.auction_config, 'get_auction_rules', return_value={'min_price': 1, 'max_user_items': 3}), \
            patch.object(trade, '_auction_queue_application', return_value=queue), \
            patch.object(trade, 'report_exception', return_value='定位号：auction-queue-test') as reported, \
            patch.object(trade, 'record_trade_event') as recorded, \
            patch.object(trade, 'handle_send', captured):
        callback = getattr(trade, f'auction_{command}_')
        def invoke():
            return asyncio.run(callback(object(), event, args))
        with pytest.raises(FinishedException):
            invoke()
        assert reported.call_args.args[1] is fault
        assert 'auction-queue-test' in captured.call_args.args[2]
        recorded.assert_not_called()
        captured.side_effect = delivery
        with pytest.raises(RuntimeError) as caught:
            invoke()
        assert caught.value is delivery
        from nonebot.exception import StopPropagation
        for flow in (FinishedException(), StopPropagation()):
            operation.side_effect = flow
            captured.reset_mock()
            reported.reset_mock()
            with pytest.raises(type(flow)) as caught:
                invoke()
            assert caught.value is flow
            captured.assert_not_called()
            reported.assert_not_called()
