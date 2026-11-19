"""Task stamina charges and their permanent receipts must commit together."""
import sqlite3
import asyncio
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
import nonebot
from nonebot.exception import FinishedException

from nonebot_plugin_xiuxian_2.core.errors import DomainError
from nonebot_plugin_xiuxian_2.features.base.migrations import apply_base_stamina_operations
from nonebot_plugin_xiuxian_2.features.base.stamina_application import PlayerStaminaApplication
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork


@pytest.fixture
def stamina(tmp_path):
    database = tmp_path / 'stamina.db'
    with DatabaseUnitOfWork(database) as uow:
        uow.execute('CREATE TABLE user_xiuxian(user_id TEXT,user_stamina INTEGER)')
        uow.execute("INSERT INTO user_xiuxian VALUES('user',10)")
        uow.execute('CREATE TABLE isolated_effect(value TEXT)')
        apply_base_stamina_operations(uow)
    return SimpleNamespace(database=database, app=PlayerStaminaApplication(database))


def snapshot(stamina):
    with DatabaseUnitOfWork(stamina.database, read_only=True) as uow:
        return tuple(uow.query_all(f'SELECT * FROM {table} ORDER BY rowid') for table in
                     ('user_xiuxian', 'player_stamina_operations', 'isolated_effect'))


@pytest.mark.parametrize('step', ['user', 'receipt'])
@pytest.mark.parametrize('action', ['ignore', 'rewrite', 'late', 'abort'])
def test_charge_refusals_rollback_and_same_task_request_recovers(stamina, step, action):
    table, verb, rewrite = (
        ('user_xiuxian', 'UPDATE', 'UPDATE user_xiuxian SET user_stamina=0;') if step == 'user' else
        ('player_stamina_operations', 'INSERT', 'UPDATE player_stamina_operations SET stamina_after=99;')
    )
    if action == 'late':
        table, verb = 'player_stamina_operations', 'INSERT'
    body = ('SELECT RAISE(IGNORE);' if action == 'ignore' else
            "SELECT RAISE(ABORT,'isolated stamina abort');" if action == 'abort' else rewrite)
    timing = 'BEFORE' if action in {'ignore', 'abort'} else 'AFTER'
    with DatabaseUnitOfWork(stamina.database) as uow:
        uow.execute(f'CREATE TRIGGER refuse_stamina {timing} {verb} ON {table} '
                    "BEGIN INSERT INTO isolated_effect VALUES('effect'); " + body + ' END')
    before = snapshot(stamina)
    def charge(app):
        return app.consume('user', 3, expected_stamina=10, operation_id='sect:task:message')
    if action == 'abort':
        with pytest.raises(sqlite3.IntegrityError, match='isolated stamina abort'):
            charge(stamina.app)
    else:
        result = charge(stamina.app)
        assert result.get('reason') == f'stamina_{step}_write_rejected', result
        assert result['subject'] == ('user' if step == 'user' else 'sect:task:message')
    assert snapshot(stamina) == before
    with DatabaseUnitOfWork(stamina.database) as uow:
        uow.execute('DROP TRIGGER refuse_stamina')
    restarted = PlayerStaminaApplication(stamina.database)
    assert charge(restarted)['status'] == 'applied'
    saved = snapshot(stamina)
    assert saved[0][0]['user_stamina'] == 7 and len(saved[1]) == 1
    assert charge(restarted)['status'] == 'duplicate' and snapshot(stamina) == saved
    with DatabaseUnitOfWork(stamina.database, read_only=True) as uow:
        assert uow.query_one('PRAGMA integrity_check') == {'integrity_check': 'ok'}


def test_charge_without_receipt_also_rolls_back_ignored_write_effects(stamina):
    with DatabaseUnitOfWork(stamina.database) as uow:
        uow.execute("INSERT INTO user_xiuxian VALUES('user',50)")
        uow.execute("CREATE TRIGGER refuse_stamina BEFORE UPDATE ON user_xiuxian "
                    "BEGIN INSERT INTO isolated_effect VALUES('effect'); SELECT RAISE(IGNORE); END")
    before = snapshot(stamina)
    result = stamina.app.consume('user', 3, expected_stamina=10)
    assert result['reason'] == 'stamina_user_write_rejected' and snapshot(stamina) == before


def test_rewritten_receipt_identity_rolls_back_charge(stamina):
    with DatabaseUnitOfWork(stamina.database) as uow:
        uow.execute("CREATE TRIGGER refuse_stamina AFTER INSERT ON player_stamina_operations "
                    "BEGIN UPDATE player_stamina_operations SET payload='[\"other\",3]'; END")
    before = snapshot(stamina)
    result = stamina.app.consume('user', 3, operation_id='receipt')
    assert result['reason'] == 'stamina_receipt_write_rejected' and snapshot(stamina) == before


@pytest.mark.parametrize('value', ['broken', 1.5])
def test_invalid_current_stamina_has_specific_reason_and_zero_write(stamina, value):
    with DatabaseUnitOfWork(stamina.database) as uow:
        uow.execute('UPDATE user_xiuxian SET user_stamina=?', (value,))
    before = snapshot(stamina)
    with pytest.raises(DomainError) as caught:
        stamina.app.consume('user', 3, operation_id='new')
    assert caught.value.details['reason'] == 'user_stamina_invalid_numeric_value'
    assert caught.value.details['subject'] == 'user' and snapshot(stamina) == before


def test_historical_receipt_remains_authoritative_after_recovery_and_conflicts(stamina):
    assert stamina.app.consume('user', 3, expected_stamina=10, operation_id='old')['status'] == 'applied'
    assert stamina.app.recover(10, 3, batch_size=1)['updated'] == 1
    before = snapshot(stamina)
    assert stamina.app.consume('user', 3, expected_stamina=7, operation_id='old') == {'status': 'duplicate', 'stamina': 7}
    assert stamina.app.consume('user', 2, operation_id='old')['status'] == 'operation_conflict'
    assert stamina.app.consume('other', 3, operation_id='old')['status'] == 'operation_conflict'
    stale = stamina.app.consume('user', 3, expected_stamina=7, operation_id='new')
    assert stale['reason'] == 'stamina_changed' and stale['subject'] == 'user'
    assert stamina.app.consume('user', 11, operation_id='new')['status'] == 'stamina_insufficient'
    assert snapshot(stamina) == before


@pytest.mark.parametrize('value', ['broken', -1, 1.5])
def test_invalid_historical_stamina_receipt_is_specific_and_zero_write(stamina, value):
    assert stamina.app.consume('user', 3, operation_id='old')['status'] == 'applied'
    with DatabaseUnitOfWork(stamina.database) as uow:
        uow.execute('UPDATE player_stamina_operations SET stamina_after=?', (value,))
    before = snapshot(stamina)
    with pytest.raises(DomainError) as caught:
        stamina.app.consume('user', 3, operation_id='old')
    assert caught.value.code == 'stamina_receipt_invalid'
    assert caught.value.details['subject'] == 'old' and snapshot(stamina) == before


@pytest.mark.parametrize('same_key', [True, False])
def test_competing_task_charges_preserve_receipts_and_snapshot(stamina, same_key):
    def consume(index):
        return PlayerStaminaApplication(stamina.database).consume(
            'user', 3, expected_stamina=10, operation_id='same' if same_key else f'different-{index}')
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(consume, (1, 2)))
    assert sorted(row['status'] for row in results) == sorted(('applied', 'duplicate' if same_key else 'state_changed'))
    saved = snapshot(stamina)
    assert saved[0][0]['user_stamina'] == 7 and len(saved[1]) == 1


@pytest.mark.parametrize('reason', ['stamina_user_write_rejected', 'stamina_receipt_write_rejected',
                                  'stamina_receipt_invalid', 'schema_missing', 'operation_conflict'])
@pytest.mark.parametrize('delivery_fails', [True, False])
def test_shared_cooldown_preserves_specific_charge_refusal_and_failed_delivery(reason, delivery_fails):
    nonebot.init()
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import lay_out
    class PrivateEvent:
        user_id = 'user'
        def get_user_id(self):
            return self.user_id
    matcher = SimpleNamespace(plugin_name='', module_name='', commands=set(),
                              finish=AsyncMock(side_effect=FinishedException))
    dependency = lay_out.Cooldown(cd_time=0, stamina_cost=3).dependency
    sent = AsyncMock(side_effect=RuntimeError('isolated failed delivery') if delivery_fails else None)
    config = SimpleNamespace(at_response=False, admin_debug=False)
    result = {'status': 'state_changed', 'reason': reason, 'subject': 'request', 'stamina': 10}
    if reason in {'schema_missing', 'operation_conflict'}:
        result = {'status': reason, 'stamina': None}
    refusal = DomainError(reason, 'private sql', {'subject': 'request'}) if reason == 'stamina_receipt_invalid' else None
    with patch.object(lay_out, 'PrivateMessageEvent', PrivateEvent), \
            patch.object(lay_out, 'patch_context', side_effect=lambda bot, event: (bot, event)), \
            patch.object(lay_out, 'XiuConfig', return_value=config), \
            patch.object(lay_out, 'JsonConfig', return_value=SimpleNamespace(read_data=lambda: {'private': True})), \
            patch.object(lay_out, 'limit_all_run', return_value=None), patch.object(lay_out, 'ADMIN_IDS', set()), \
            patch.object(lay_out, 'check_user', return_value=(True, {'user_id': 'user', 'user_stamina': 10}, '')), \
            patch.object(lay_out, 'consume_player_stamina', return_value=result, side_effect=refusal), patch.object(lay_out, 'handle_send', sent):
        with pytest.raises(RuntimeError if delivery_fails else FinishedException):
            asyncio.run(dependency(bot=SimpleNamespace(), matcher=matcher, event=PrivateEvent()))
    text = sent.call_args.args[2]
    from nonebot_plugin_xiuxian_2.core.player_reasons import reason_text
    assert reason_text(reason) in text
    assert 'private sql' not in text and '状态已更新' not in text
    if reason.endswith('write_rejected'):
        assert '已撤销' in text
