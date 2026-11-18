import json
import tempfile
import unittest
from datetime import date
from pathlib import Path

from ..repository import ArenaChallengePurchaseSqlRepository
from ..application import ArenaApplication
from ..domain import ArenaPurchaseRequest
from ..migrations import apply_arena_purchase_receipt
from ....infrastructure.database import DatabaseUnitOfWork, OperationLedger
from tests.test_db_backend import db_backend


class ArenaPurchaseSqlRepositoryTests(unittest.TestCase):
    def setUp(self):
        self.t=tempfile.TemporaryDirectory(); root=Path(self.t.name); self.game=root/'g.db'; self.player=root/'p.db'
        with db_backend.transaction(self.game) as c:
            c.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY)"); c.execute("INSERT INTO user_xiuxian VALUES('u')")
            c.execute("CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,goods_num INTEGER,create_time TEXT,update_time TEXT,bind_num INTEGER,UNIQUE(user_id,goods_id))")
            c.execute("CREATE TABLE arena_purchase_operations(operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,quantity INTEGER,cost INTEGER,honor_points INTEGER,purchased INTEGER,inventory INTEGER)")
        with DatabaseUnitOfWork(self.game) as uow:
            apply_arena_purchase_receipt(uow)
        with db_backend.transaction(self.player) as c:
            c.execute("CREATE TABLE arena(user_id TEXT PRIMARY KEY,honor_points INTEGER,weekly_purchases TEXT)")
            c.execute("INSERT INTO arena VALUES('u',100,?)",(json.dumps({'_last_reset':'2026-09-15','1':1}),))
        self.r=ArenaChallengePurchaseSqlRepository(self.game,self.player)
    def tearDown(self):self.t.cleanup()
    def buy(self,op='x',**kw):
        v=dict(item_id=1,item_name='item',item_type='type',quantity=2,unit_cost=10,weekly_limit=5,expected_honor=100,expected_weekly_purchases={'_last_reset':'2026-09-15','1':1},max_goods_num=99,bind_flag=1,today=date(2026,9,15));v.update(kw);return self.r.purchase(op,'u',**v)
    def state(self):
        with db_backend.connection(self.game) as c:item=c.execute("SELECT goods_num,bind_num FROM back").fetchone()
        with db_backend.connection(self.player) as c:a=c.execute("SELECT honor_points,weekly_purchases FROM arena").fetchone()
        return (int(a[0]),json.loads(a[1]),tuple(item) if item else None)

    @staticmethod
    def _trigger(database, sql):
        with db_backend.transaction(database) as c:
            c.execute(sql)

    @staticmethod
    def _drop_trigger(database, name):
        with db_backend.transaction(database) as c:
            c.execute(f"DROP TRIGGER {name}")
    def test_success_duplicate_conflict(self):
        a=self.buy();b=self.buy();c=self.buy(quantity=1);self.assertEqual((a['status'],b['status'],c['status']),('applied','duplicate','state_changed'));self.assertEqual(self.state(),(80,{'_last_reset':'2026-09-15','1':3},(2,2)))
    def test_rejections_and_rollback(self):
        self.assertEqual('limit_reached',self.buy('limit',quantity=5)['status']);self.assertEqual('honor_insufficient',self.buy('poor',unit_cost=60)['status'])
        with db_backend.transaction(self.game) as c:c.execute("CREATE TRIGGER fail_ap BEFORE INSERT ON arena_purchase_operations BEGIN SELECT RAISE(ABORT,'failed'); END")
        with self.assertRaises(Exception):self.buy('fail')
        self.assertEqual(self.state(),(100,{'_last_reset':'2026-09-15','1':1},None))

    def test_ignored_and_rewritten_purchase_writes_are_rejected_and_rolled_back(self):
        initial = self.state()
        cases = (
            (
                "honor_ignore", self.player,
                "CREATE TRIGGER honor_ignore BEFORE UPDATE OF honor_points ON arena BEGIN SELECT RAISE(IGNORE); END",
                "arena_purchase_honor_write_rejected", "u",
            ),
            (
                "honor_rewrite", self.player,
                "CREATE TRIGGER honor_rewrite AFTER UPDATE OF honor_points ON arena BEGIN UPDATE arena SET honor_points=honor_points+7 WHERE user_id=NEW.user_id; END",
                "arena_purchase_honor_write_rejected", "u",
            ),
            (
                "weekly_ignore", self.player,
                "CREATE TRIGGER weekly_ignore BEFORE UPDATE OF weekly_purchases ON arena BEGIN SELECT RAISE(IGNORE); END",
                "arena_purchase_honor_write_rejected", "u",
            ),
            (
                "weekly_rewrite", self.player,
                "CREATE TRIGGER weekly_rewrite AFTER UPDATE OF weekly_purchases ON arena BEGIN UPDATE arena SET weekly_purchases='{\"_last_reset\":\"2026-09-15\",\"1\":99}' WHERE user_id=NEW.user_id; END",
                "arena_purchase_weekly_write_rejected", "u",
            ),
            (
                "inventory_ignore", self.game,
                "CREATE TRIGGER inventory_ignore BEFORE INSERT ON back BEGIN SELECT RAISE(IGNORE); END",
                "arena_purchase_inventory_write_rejected", "u:1",
            ),
            (
                "inventory_rewrite", self.game,
                "CREATE TRIGGER inventory_rewrite AFTER INSERT ON back BEGIN UPDATE back SET goods_num=goods_num+7 WHERE user_id=NEW.user_id AND goods_id=NEW.goods_id; END",
                "arena_purchase_inventory_write_rejected", "u:1",
            ),
            (
                "receipt_ignore", self.game,
                "CREATE TRIGGER receipt_ignore BEFORE INSERT ON arena_purchase_operations BEGIN SELECT RAISE(IGNORE); END",
                "arena_purchase_receipt_write_rejected", "receipt",
            ),
            (
                "receipt_rewrite", self.game,
                "CREATE TRIGGER receipt_rewrite AFTER INSERT ON arena_purchase_operations BEGIN UPDATE arena_purchase_operations SET result_json='{}' WHERE operation_id=NEW.operation_id; END",
                "arena_purchase_receipt_write_rejected", "receipt",
            ),
        )
        for name, database, create_sql, reason, subject in cases:
            with self.subTest(name=name):
                self._trigger(database, create_sql)
                try:
                    result = self.buy(f"reject-{name}")
                finally:
                    self._drop_trigger(database, name)
                self.assertEqual((result["status"], result["reason"], result["subject"]), ("state_changed", reason, subject))
                self.assertEqual(self.state(), initial)
                with db_backend.connection(self.game) as c:
                    self.assertEqual(0, c.execute("SELECT COUNT(*) FROM arena_purchase_operations").fetchone()[0])

    def test_rejected_receipt_cannot_become_success_on_retry(self):
        for operation, options, status in (
            ('limit', {'quantity': 5}, 'limit_reached'),
            ('poor', {'unit_cost': 60}, 'honor_insufficient'),
            ('full', {'max_goods_num': 1}, 'inventory_full'),
        ):
            with self.subTest(operation=operation):
                self.assertEqual(status, self.buy(operation, **options)['status'])
                self.assertEqual(status, self.buy(operation, **options)['status'])
                self.assertEqual(status, self.r.purchase_result(operation, 'u', 1, options.get('quantity', 2))['status'])
        self.assertEqual(self.state()[0], 100)

    def test_clamp_receipt_binds_original_quantity_not_remaining_limit(self):
        first = self.buy(quantity=9, clamp_quantity=True)
        self.assertEqual(('applied', 4, 40), (first['status'], first['quantity'], first['cost']))
        replay = self.r.purchase_result('x', 'u', 1, 9)
        self.assertEqual(('duplicate', 4, 'item'), (replay['status'], replay['quantity'], replay['item_name']))
        for user, item, quantity in [('v', 1, 9), ('u', 2, 9), ('u', 1, 4)]:
            self.assertEqual('operation_conflict', self.r.purchase_result('x', user, item, quantity)['status'])
        self.assertEqual('duplicate', self.buy(quantity=9, clamp_quantity=True)['status'])
        self.assertEqual(self.state(), (60, {'_last_reset': '2026-09-15', '1': 5}, (4, 4)))

    def test_weekly_limit_preserves_same_iso_week_and_resets_next_week(self):
        self.assertEqual('applied', self.buy(today=date(2026, 9, 16))['status'])
        self.assertEqual({'_last_reset': '2026-09-15', '1': 3}, self.state()[1])
        result = self.buy('next-week', today=date(2026, 9, 21), expected_honor=80,
                          expected_weekly_purchases={'_last_reset': '2026-09-21'})
        self.assertEqual('applied', result['status'])
        self.assertEqual({'_last_reset': '2026-09-21', '1': 2}, self.state()[1])

    def test_free_purchase_replays_but_ambiguous_old_receipt_fails_closed(self):
        self.assertEqual('applied', self.buy(unit_cost=0)['status'])
        self.assertEqual('duplicate', self.buy(unit_cost=0)['status'])
        with DatabaseUnitOfWork(self.game) as uow:
            payload = uow.query_one('SELECT payload FROM arena_purchase_operations WHERE operation_id=?', ('x',))['payload']
            uow.execute('INSERT INTO arena_purchase_operations(operation_id,payload,quantity,cost,honor_points,purchased,inventory) VALUES(?,?,?,?,?,?,?)',
                        ('old', payload, 2, 0, 100, 3, 2))
            apply_arena_purchase_receipt(uow)
        self.assertEqual('needs_reconcile', self.r.purchase_result('old', 'u', 1, 2)['status'])
        self.assertEqual('needs_reconcile', self.buy('old', unit_cost=0)['status'])
        self.assertEqual((2, 2), self.state()[2])

    def test_corrupt_receipt_never_replays_success(self):
        self.buy()
        for encoded in ('{', 'null', '[]', '{}', '{"status":"honor_insufficient"}'):
            with DatabaseUnitOfWork(self.game) as uow:
                uow.execute('UPDATE arena_purchase_operations SET result_json=? WHERE operation_id=?', (encoded, 'x'))
            self.assertEqual('operation_conflict', self.r.purchase_result('x', 'u', 1, 2)['status'])

    def test_started_sql_application_recovers_without_receipt(self):
        app, request = self.started_purchase('started')
        result = app.purchase(**request, today=date(2026, 9, 15))
        self.assertTrue(result.ok)
        self.assertEqual((2, 2), self.state()[2])
        self.assertTrue(app.purchase(**request, today=date(2026, 9, 15)).replayed)
        self.assertEqual((2, 2), self.state()[2])

    def test_started_sql_application_recovers_applied_and_rejected_receipts(self):
        app, request = self.started_purchase('poor', unit_cost=60)
        self.assertEqual('honor_insufficient', self.buy('poor', unit_cost=60)['status'])
        result = app.purchase(**request, today=date(2026, 9, 15))
        self.assertFalse(result.ok)
        self.assertEqual('honor_insufficient', result.code)
        app, request = self.started_purchase('done')
        self.buy('done')
        result = app.purchase(**request, today=date(2026, 9, 15))
        self.assertTrue(result.ok)
        self.assertEqual('duplicate', result.data['status'])
        self.assertEqual((2, 2), self.state()[2])

    def test_started_application_payload_conflict_still_rejects(self):
        from ....core.errors import ConflictError
        app, request = self.started_purchase('conflict')
        request['quantity'] = 3
        with self.assertRaises(ConflictError):
            app.purchase(**request, today=date(2026, 9, 15))
        self.assertIsNone(self.state()[2])

    def started_purchase(self, operation, **overrides):
        request = dict(operation_id=operation, user_id='u', item_id=1,
                       item_name='item', item_type='type', quantity=2, unit_cost=10,
                       weekly_limit=5, expected_honor=100,
                       expected_weekly_purchases={'_last_reset': '2026-09-15', '1': 1},
                       max_goods_num=99, bind_flag=1)
        request.update(overrides)
        ledger = OperationLedger()
        with DatabaseUnitOfWork(self.game) as uow:
            ledger.ensure_schema(uow)
            ledger.begin(uow, operation, 'arena.purchase', ArenaPurchaseRequest(**request).payload())
        return ArenaApplication(self.game, self.player), request

if __name__=='__main__':unittest.main()
