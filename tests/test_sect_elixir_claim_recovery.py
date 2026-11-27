"""Verify daily elixir handoff, durable failure reasons and same-request recovery."""
import asyncio
import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from nonebot.exception import FinishedException, ProcessException

from tests.test_sect_elixir_claim_transaction import make_database
from nonebot_plugin_xiuxian_2.core.errors import DomainError
from nonebot_plugin_xiuxian_2.features.sect.application import SectApplication
from nonebot_plugin_xiuxian_2.features.sect.repository import SectRenameSqlRepository
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork, OperationLedger
from nonebot_plugin_xiuxian_2.compatibility.legacy_sect_elixir_claim import SectElixirClaimService

REWARDS = [(1999, 'first', 'pill', 1), (2001, 'second', 'pill', 2)]
REQUEST = dict(operation_id='claim', user_id='u1', sect_id=7, contribution_required=100,
               materials_required=100, rewards=REWARDS, max_goods_num=999)


@pytest.fixture
def database(tmp_path):
    path = tmp_path / 'claim.db'
    make_database(path)
    with DatabaseUnitOfWork(path) as uow:
        OperationLedger().ensure_schema(uow)
        uow.execute('CREATE TABLE sect_elixir_claim_operations(operation_id TEXT PRIMARY KEY,payload TEXT,rewards TEXT)')
        uow.execute('CREATE TABLE isolated_effect(value TEXT)')
        uow.execute("INSERT INTO back VALUES('u1',1999,'old','pill',4,'original','original',2)")
    return path


def snapshot(database):
    with DatabaseUnitOfWork(database, read_only=True) as uow:
        return tuple(uow.query_all(f'SELECT * FROM {table} ORDER BY rowid') for table in
                     ('user_xiuxian', 'sects', 'back', 'sect_elixir_claim_operations', 'isolated_effect'))


def install_fault(database, step, action):
    table, verb, condition, rewrite = {
        'item': ('back', 'INSERT', 'WHEN NEW.goods_id=2001', "UPDATE back SET goods_num=99 WHERE goods_id=2001;"),
        'existing_item': ('back', 'UPDATE', 'WHEN NEW.goods_id=1999', "UPDATE back SET bind_num=99 WHERE goods_id=1999;"),
        'user': ('user_xiuxian', 'UPDATE', '', 'UPDATE user_xiuxian SET sect_elixir_get=0;'),
        'receipt': ('sect_elixir_claim_operations', 'INSERT', '', "UPDATE sect_elixir_claim_operations SET rewards='[]';"),
        'eligibility': ('sect_elixir_claim_operations', 'INSERT', '', 'UPDATE sects SET sect_materials=0;'),
    }[step]
    if action == 'late':
        table, verb, condition = 'sect_elixir_claim_operations', 'INSERT', ''
    body = 'SELECT RAISE(IGNORE);' if action == 'ignore' else "SELECT RAISE(ABORT,'isolated claim abort');" if action == 'abort' else rewrite
    timing = 'BEFORE' if action in {'ignore', 'abort'} else 'AFTER'
    with DatabaseUnitOfWork(database) as uow:
        uow.execute(f'CREATE TRIGGER refuse_claim {timing} {verb} ON {table} {condition} '
                    "BEGIN INSERT INTO isolated_effect VALUES('effect'); " + body + ' END')


@pytest.mark.parametrize('step', ['item', 'existing_item', 'user', 'receipt'])
@pytest.mark.parametrize('action', ['ignore', 'rewrite', 'late', 'abort'])
def test_all_writes_rollback_and_same_request_recovers(database, step, action):
    install_fault(database, step, action)
    before = snapshot(database)
    app = SectApplication(database)
    reason = f"sect_elixir_{'item' if step == 'existing_item' else step}_write_rejected"
    if action == 'abort':
        with pytest.raises(sqlite3.IntegrityError, match='isolated claim abort'):
            app.claim_elixir(**REQUEST)
    else:
        result = app.claim_elixir(**REQUEST)
        assert not result.ok and result.status == 'failed' and result.code == reason
        assert result.data['subject'] == {'item': '2001', 'existing_item': '1999', 'user': 'u1', 'receipt': 'claim'}[step]
        with DatabaseUnitOfWork(database, read_only=True) as uow:
            ledger = uow.query_one('SELECT status,result_json FROM operation_ledger')
        assert ledger['status'] == 'failed' and json.loads(ledger['result_json'])['data']['reason'] == reason
    assert snapshot(database) == before
    with DatabaseUnitOfWork(database) as uow:
        uow.execute('DROP TRIGGER refuse_claim')
    restarted = SectApplication(database)
    assert restarted.claim_elixir(**REQUEST).ok
    saved = snapshot(database)
    assert [(r['goods_id'], r['goods_num'], r['bind_num']) for r in saved[2]] == [(1999, 5, 3), (2001, 2, 2)]
    assert saved[0][0]['sect_contribution'] == 500 and saved[1][0]['sect_materials'] == 500
    assert restarted.claim_elixir(**REQUEST).replayed and snapshot(database) == saved
    with DatabaseUnitOfWork(database, read_only=True) as uow:
        assert uow.query_one('PRAGMA integrity_check') == {'integrity_check': 'ok'}


def test_late_eligibility_change_is_concrete_and_atomic(database):
    install_fault(database, 'eligibility', 'late')
    before = snapshot(database)
    result = SectApplication(database).claim_elixir(**REQUEST)
    assert result.code == 'sect_elixir_eligibility_write_rejected' and result.data['subject'] == '7'
    assert snapshot(database) == before


@pytest.mark.parametrize('sql,reason', [
    ("UPDATE back SET goods_num='broken' WHERE goods_id=1999", 'sect_elixir_inventory_invalid_numeric_value'),
    ("UPDATE back SET bind_num='broken' WHERE goods_id=1999", 'sect_elixir_bound_invalid_numeric_value'),
    ("UPDATE back SET bind_num=5 WHERE goods_id=1999", 'sect_elixir_inventory_invalid'),
    ("UPDATE user_xiuxian SET sect_contribution='broken'", 'sect_contribution_invalid_numeric_value'),
    ("UPDATE user_xiuxian SET sect_elixir_get='broken'", 'sect_elixir_get_invalid_numeric_value'),
    ("UPDATE sects SET elixir_room_level='broken'", 'elixir_room_level_invalid_numeric_value'),
    ("UPDATE sects SET sect_materials='broken'", 'sect_materials_invalid_numeric_value'),
])
def test_bad_data_has_specific_reason_and_zero_business_writes(database, sql, reason):
    with DatabaseUnitOfWork(database) as uow:
        uow.execute(sql)
    before = snapshot(database)
    result = SectRenameSqlRepository(database).claim_elixir(**REQUEST)
    assert result['reason'] == reason and snapshot(database) == before


def test_inventory_boundary_includes_duplicate_reward_ids_and_exact_large_values(database):
    repo = SectRenameSqlRepository(database)
    before = snapshot(database)
    result = repo.claim_elixir(**{**REQUEST, 'rewards': [REWARDS[0], REWARDS[0]], 'max_goods_num': 5})
    assert result['reason'] == 'inventory_full' and snapshot(database) == before
    large = 2**64+7
    with DatabaseUnitOfWork(database) as uow:
        uow.execute('UPDATE back SET goods_num=xiuxian_int_store(?),bind_num=xiuxian_int_store(?)', (str(large), str(large-2)))
    assert repo.claim_elixir(**{**REQUEST, 'max_goods_num': large+1})['status'] == 'applied'
    saved = snapshot(database)[2][0]
    assert int(saved['goods_num']) == large+1 and int(saved['bind_num']) == large-1


def test_multi_reward_reads_are_batched_and_post_receipt_checks_stay_fresh(database):
    rewards = [(item, str(item), 'pill', 1) for item in range(3000, 3015)]
    execute = DatabaseUnitOfWork.execute
    reads = []

    def traced(uow, sql, params=()):
        if sql.startswith('SELECT') and 'LEFT JOIN back' in sql:
            reads.append((sql, params))
        return execute(uow, sql, params)

    with patch.object(DatabaseUnitOfWork, 'execute', traced):
        result = SectRenameSqlRepository(database).claim_elixir(**{**REQUEST, 'rewards': rewards})
    assert result['status'] == 'applied' and len(reads) == 2
    saved = snapshot(database)
    assert [(row['goods_num'], row['bind_num']) for row in saved[2][1:]] == [(1, 1)] * 15
    assert SectRenameSqlRepository(database).claim_elixir(**{**REQUEST, 'rewards': rewards})['status'] == 'duplicate'
    assert snapshot(database) == saved


def test_post_receipt_null_inventory_is_rejected_and_rolled_back(database):
    with DatabaseUnitOfWork(database) as uow:
        uow.execute("CREATE TRIGGER null_inventory AFTER INSERT ON sect_elixir_claim_operations "
                    "BEGIN UPDATE back SET goods_num=NULL WHERE goods_id=2001; END")
    before = snapshot(database)
    result = SectRenameSqlRepository(database).claim_elixir(**REQUEST)
    assert result['reason'] == 'sect_elixir_item_write_rejected'
    assert result['subject'] == '2001'
    assert snapshot(database) == before


def test_started_request_and_generated_rewards_can_resume_but_explicit_payload_conflicts(database):
    app = SectApplication(database)
    with DatabaseUnitOfWork(database) as uow:
        app.ledger.begin(uow, 'claim', 'sect.claim_elixir', {k: v for k, v in REQUEST.items() if k != 'operation_id'})
    assert app.claim_elixir(**REQUEST).ok
    before = snapshot(database)
    with DatabaseUnitOfWork(database, read_only=True) as uow:
        ledger_before = uow.query_all('SELECT * FROM operation_ledger')
        audit_before = uow.query_all('SELECT * FROM operation_audit')
    conflict = app.claim_elixir(**{**REQUEST, 'rewards': [REWARDS[0]]})
    assert not conflict.ok and not conflict.replayed
    assert (conflict.status, conflict.code) == ('rejected', 'operation_conflict')
    assert conflict.data == {'status': 'operation_conflict', 'reason': 'operation_conflict', 'subject': 'actor'}
    assert snapshot(database) == before
    with DatabaseUnitOfWork(database, read_only=True) as uow:
        assert uow.query_all('SELECT * FROM operation_ledger') == ledger_before
        assert uow.query_all('SELECT * FROM operation_audit') == audit_before
    with DatabaseUnitOfWork(database) as uow:
        uow.execute('UPDATE user_xiuxian SET sect_elixir_get=0')
    install_fault(database, 'receipt', 'ignore')
    args = {**REQUEST, 'operation_id': 'chat', 'generated_rewards': True}
    assert app.claim_elixir(**args).status == 'failed'
    with DatabaseUnitOfWork(database) as uow:
        uow.execute('DROP TRIGGER refuse_claim')
    assert SectApplication(database).claim_elixir(**{**args, 'rewards': [REWARDS[0]]}).ok


def test_historical_success_payload_and_receipt_survive_current_day_changes(database):
    repo = SectRenameSqlRepository(database)
    assert repo.claim_elixir(**REQUEST)['status'] == 'applied'
    with DatabaseUnitOfWork(database) as uow:
        uow.execute('UPDATE user_xiuxian SET sect_elixir_get=0,sect_id=NULL')
        uow.execute('DELETE FROM sects')
    before = snapshot(database)
    result = repo.claim_elixir(**{**REQUEST, 'rewards': [REWARDS[0]], 'sect_id': 999})
    assert result['status'] == 'duplicate' and result['rewards'] == tuple(REWARDS)
    assert repo.elixir_claim_receipt('claim', 'u1') == result and snapshot(database) == before
    with pytest.raises(DomainError, match='another player'):
        repo.elixir_claim_receipt('claim', 'other')
    with DatabaseUnitOfWork(database) as uow:
        uow.execute("UPDATE sect_elixir_claim_operations SET rewards='[{}]'")
    result = repo.claim_elixir(**REQUEST)
    assert result['reason'] == 'sect_elixir_receipt_invalid'


@pytest.mark.parametrize('step', ['item', 'existing_item', 'user', 'receipt'])
def test_legacy_service_keeps_dto_and_shares_atomic_checks(database, step):
    install_fault(database, step, 'ignore')
    before = snapshot(database)
    result = SectElixirClaimService(database).claim(**REQUEST)
    assert not result.succeeded and result.reason.endswith('_write_rejected')
    assert snapshot(database) == before


def test_legacy_null_position_and_item_metadata_rules_are_preserved(database):
    service = SectElixirClaimService(database)
    with DatabaseUnitOfWork(database) as uow:
        uow.execute('UPDATE user_xiuxian SET sect_position=NULL')
    assert service.claim(**REQUEST).status == 'position_ineligible'
    with DatabaseUnitOfWork(database) as uow:
        uow.execute('UPDATE user_xiuxian SET sect_position=1')
    assert service.claim(**REQUEST).succeeded
    assert snapshot(database)[2][0]['goods_name'] == 'first'


@pytest.mark.parametrize('known', [False, True])
def test_diagnostic_failure_keeps_original_error_and_started_request_recovers(database, known):
    install_fault(database, 'receipt', 'ignore' if known else 'abort')
    app = SectApplication(database)
    before = snapshot(database)
    diagnostic = RuntimeError('isolated diagnostics unavailable')
    method = 'finish' if known else 'record_failure'
    with patch.object(app.ledger, method, side_effect=diagnostic):
        with pytest.raises(DomainError if known else sqlite3.IntegrityError) as caught:
            app.claim_elixir(**REQUEST)
    assert caught.value.__cause__ is diagnostic and snapshot(database) == before
    if known:
        assert caught.value.code == 'sect_elixir_receipt_write_rejected'
    with DatabaseUnitOfWork(database) as uow:
        uow.execute('DROP TRIGGER refuse_claim')
    assert SectApplication(database).claim_elixir(**REQUEST).ok


def test_missing_schema_and_database_are_not_created(tmp_path, database):
    missing = tmp_path/'missing.db'
    assert SectRenameSqlRepository(missing).claim_elixir(**REQUEST)['reason'] == 'schema_missing'
    assert not missing.exists()
    with DatabaseUnitOfWork(database) as uow:
        uow.execute('DROP TABLE sect_elixir_claim_operations')
    assert SectRenameSqlRepository(database).claim_elixir(**REQUEST)['reason'] == 'schema_missing'


def test_committed_reward_before_diagnostic_failure_recovers_without_awarding_again(database):
    app = SectApplication(database)
    failure = RuntimeError('isolated completion failure')
    with patch.object(app.ledger, 'finish', side_effect=failure):
        with pytest.raises(RuntimeError):
            app.claim_elixir(**REQUEST)
    saved = snapshot(database)
    assert saved[0][0]['sect_elixir_get'] == 1 and len(saved[3]) == 1
    restarted = SectApplication(database)
    assert restarted.elixir_claim_receipt('claim', 'u1')['rewards'] == tuple(REWARDS)
    with DatabaseUnitOfWork(database, read_only=True) as uow:
        assert uow.query_one('SELECT status FROM operation_ledger')['status'] == 'applied'
        count = uow.query_one('SELECT COUNT(*) AS n FROM operation_audit')['n']
    assert snapshot(database) == saved
    assert restarted.elixir_claim_receipt('claim', 'u1')['status'] == 'duplicate'
    with DatabaseUnitOfWork(database, read_only=True) as uow:
        assert uow.query_one('SELECT COUNT(*) AS n FROM operation_audit')['n'] == count


@pytest.mark.parametrize('error', [DomainError('sect_elixir_user_write_rejected', 'private sql', {'subject': 'u1'}),
                                 RuntimeError('private sql'), DomainError('unmapped_test_code', 'private sql')])
def test_real_handler_known_unknown_reply_and_failed_delivery_propagation(error):
    from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_sect as module
    event = SimpleNamespace(get_user_id=lambda: 'u1')
    send = AsyncMock()
    with patch.object(module, '_claim_sect_elixir', AsyncMock(side_effect=error)), \
            patch.object(module, 'handle_send', send), patch.object(module.sect_elixir_get, 'finish', AsyncMock(side_effect=FinishedException)):
        with pytest.raises(FinishedException):
            asyncio.run(module.sect_elixir_get_(None, event))
    text = send.call_args.args[2]
    assert 'private sql' not in text
    assert ('已领标志' in text) if getattr(error, 'code', '') == 'sect_elixir_user_write_rejected' else ('定位号' in text)
    delivery = RuntimeError('isolated failed delivery')
    with patch.object(module, '_claim_sect_elixir', AsyncMock(side_effect=error)), \
            patch.object(module, 'handle_send', AsyncMock(side_effect=delivery)):
        with pytest.raises(RuntimeError) as caught:
            asyncio.run(module.sect_elixir_get_(None, event))
    assert caught.value is delivery


@pytest.mark.parametrize('flow', [FinishedException(), ProcessException()])
def test_normal_matcher_flow_is_not_converted_to_failure(flow):
    from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_sect as module
    with patch.object(module, '_claim_sect_elixir', AsyncMock(side_effect=flow)), patch.object(module, 'handle_send', AsyncMock()) as send:
        with pytest.raises(type(flow)) as caught:
            asyncio.run(module.sect_elixir_get_(None, None))
    assert caught.value is flow and not send.called


def test_web_claim_retains_specific_failure_and_recovers_same_key(database):
    from flask import Flask
    from nonebot_plugin_xiuxian_2.adapters.web.blueprints.sect import create_blueprint
    app = Flask(__name__)
    app.secret_key = 'isolated-test'
    app.register_blueprint(create_blueprint(application=SectApplication(database), permission=lambda _: True))
    client = app.test_client()
    with client.session_transaction() as session:
        session['_csrf_token'] = 'isolated-csrf'
    headers = {'X-CSRF-Token': 'isolated-csrf', 'Idempotency-Key': 'claim'}
    payload = {k: v for k, v in REQUEST.items() if k != 'operation_id'}
    before = snapshot(database)
    assert client.post('/api/v1/sect/elixir/claim', json=payload).status_code == 403
    install_fault(database, 'receipt', 'ignore')
    response = client.post('/api/v1/sect/elixir/claim', json=payload, headers=headers)
    data = response.get_json()['data']
    assert response.status_code == 409 and data['code'] == 'sect_elixir_receipt_write_rejected'
    assert data['data']['subject'] == 'claim' and snapshot(database) == before
    with DatabaseUnitOfWork(database) as uow:
        uow.execute('DROP TRIGGER refuse_claim')
    assert client.post('/api/v1/sect/elixir/claim', json=payload, headers=headers).status_code == 200
    saved = snapshot(database)
    assert client.post('/api/v1/sect/elixir/claim', json=payload, headers=headers).get_json()['data']['replayed']
    assert snapshot(database) == saved


@pytest.mark.parametrize('same_key', [True, False])
def test_concurrent_claims_award_exactly_once(database, same_key):
    def invoke(key):
        return SectApplication(database).claim_elixir(**{**REQUEST, 'operation_id': key})
    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(invoke, 'claim')
        second = pool.submit(invoke, 'claim' if same_key else 'other')
        results = [first.result(timeout=10), second.result(timeout=10)]
    assert any(r.ok for r in results)
    if not same_key:
        assert sorted(r.ok for r in results) == [False, True]
        assert next(r for r in results if not r.ok).data['reason'] == 'already_claimed'
    saved = snapshot(database)
    assert len(saved[3]) == 1 and saved[0][0]['sect_elixir_get'] == 1
    assert [(r['goods_num'], r['bind_num']) for r in saved[2]] == [(5, 3), (2, 2)]
