import tempfile
import unittest
from pathlib import Path
from ..buff_search_repository import SectBuffSearchSqlRepository
from tests.test_db_backend import db_backend
from .fixtures import migrate_sect

class SectBuffSearchRepositoryTests(unittest.TestCase):
    def _snapshot(self, db):
        with db_backend.connection(db) as c:
            sect = tuple(c.execute(
                'SELECT mainbuff,secbuff,sect_used_stone,sect_materials FROM sects'
            ).fetchone())
            receipt = c.execute('SELECT COUNT(*) FROM sect_buff_search_operations').fetchone()[0]
            effect = c.execute('SELECT COUNT(*) FROM isolated_effect').fetchone()[0]
        return sect, receipt, effect

    def _with_effect_table(self, db):
        with db_backend.transaction(db) as c:
            c.execute('CREATE TABLE isolated_effect(n INTEGER NOT NULL)')
            c.execute('INSERT INTO isolated_effect VALUES (0)')

    def test_search_is_idempotent_and_deducts_assets(self):
        with tempfile.TemporaryDirectory() as temp:
            db=Path(temp)/'sect.db'
            migrate_sect(db)
            with db_backend.transaction(db) as c:
                c.execute('CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,sect_id INTEGER,sect_position INTEGER)'); c.execute("INSERT INTO user_xiuxian VALUES('owner',1,0)")
                # 真实 sects 列是 mainbuff/secbuff（无 sect_mainbuff/sect_secbuff）。
                c.execute('CREATE TABLE sects(sect_id INTEGER PRIMARY KEY,sect_owner TEXT,mainbuff TEXT,secbuff TEXT,sect_used_stone INTEGER,sect_materials INTEGER)'); c.execute("INSERT INTO sects VALUES(1,'owner','[]','[]',1000,2000)")
            repo=SectBuffSearchSqlRepository(db)
            first=repo.apply('op','owner',1,'main','[]','[1,2]',100,200)
            duplicate=repo.apply('op','owner',1,'main','[]','[3]',999,999)
            self.assertEqual((first['status'],duplicate['status']),('applied','duplicate'))
            with db_backend.connection(db) as c:self.assertEqual(tuple(c.execute('SELECT mainbuff,sect_used_stone,sect_materials FROM sects').fetchone()),('[1,2]',900,1800))

    def test_owner_position_is_honoured_instead_of_a_hardcoded_zero(self):
        with tempfile.TemporaryDirectory() as temp:
            db=Path(temp)/'sect.db'
            migrate_sect(db)
            with db_backend.transaction(db) as c:
                c.execute('CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,sect_id INTEGER,sect_position INTEGER)'); c.execute("INSERT INTO user_xiuxian VALUES('owner',1,3)")
                c.execute('CREATE TABLE sects(sect_id INTEGER PRIMARY KEY,sect_owner TEXT,mainbuff TEXT,secbuff TEXT,sect_used_stone INTEGER,sect_materials INTEGER)'); c.execute("INSERT INTO sects VALUES(1,'owner','[]','[]',1000,2000)")
            repo=SectBuffSearchSqlRepository(db)
            # 宗主位是 3 时也必须通过（旧实现对 owner_position 硬编码 0）。
            result=repo.apply('op','owner',1,'secondary','[]','[9]',10,20,owner_position=3)
            self.assertEqual(result['status'],'applied')
            # position 与传入值不符 -> not_owner 且零变化。
            denied=repo.apply('op2','owner',1,'secondary','[9]','[7]',10,20,owner_position=0)
            self.assertEqual(denied['status'],'not_owner')
            with db_backend.connection(db) as c:self.assertEqual(tuple(c.execute('SELECT secbuff,sect_used_stone,sect_materials FROM sects').fetchone()),('[9]',990,1980))

    def test_receipt_ignore_is_a_specific_rejection_and_rolls_back_side_effects(self):
        with tempfile.TemporaryDirectory() as temp:
            db=Path(temp)/'sect.db'
            migrate_sect(db)
            with db_backend.transaction(db) as c:
                c.execute('CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,sect_id INTEGER,sect_position INTEGER)')
                c.execute("INSERT INTO user_xiuxian VALUES('owner',1,0)")
                c.execute('CREATE TABLE sects(sect_id INTEGER PRIMARY KEY,sect_owner TEXT,mainbuff TEXT,secbuff TEXT,sect_used_stone INTEGER,sect_materials INTEGER)')
                c.execute("INSERT INTO sects VALUES(1,'owner','[]','[]',1000,2000)")
            self._with_effect_table(db)
            with db_backend.transaction(db) as c:
                c.execute("CREATE TRIGGER search_effect AFTER UPDATE OF mainbuff ON sects BEGIN UPDATE isolated_effect SET n=n+1; END")
                c.execute("CREATE TRIGGER ignore_search_receipt BEFORE INSERT ON sect_buff_search_operations BEGIN SELECT RAISE(IGNORE); END")
            before = self._snapshot(db)
            result = SectBuffSearchSqlRepository(db).apply('receipt-ignore','owner',1,'main','[]','[1]',100,200)
            self.assertEqual((result['status'], result['reason'], result['subject']), ('state_changed','sect_buff_search_receipt_write_rejected','receipt-ignore'))
            self.assertEqual(self._snapshot(db), before)
            with db_backend.transaction(db) as c: c.execute('DROP TRIGGER ignore_search_receipt')
            recovered = SectBuffSearchSqlRepository(db).apply('receipt-ignore','owner',1,'main','[]','[1]',100,200)
            self.assertEqual(recovered['status'], 'applied')
            self.assertEqual(self._snapshot(db), (('[1]','[]',900,1800),1,1))

    def test_catalog_ignore_and_rewrite_are_specific_rejections(self):
        for trigger in ('ignore', 'rewrite'):
            with self.subTest(trigger=trigger), tempfile.TemporaryDirectory() as temp:
                db=Path(temp)/'sect.db'
                migrate_sect(db)
                with db_backend.transaction(db) as c:
                    c.execute('CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,sect_id INTEGER,sect_position INTEGER)')
                    c.execute("INSERT INTO user_xiuxian VALUES('owner',1,0)")
                    c.execute('CREATE TABLE sects(sect_id INTEGER PRIMARY KEY,sect_owner TEXT,mainbuff TEXT,secbuff TEXT,sect_used_stone INTEGER,sect_materials INTEGER)')
                    c.execute("INSERT INTO sects VALUES(1,'owner','[]','[]',1000,2000)")
                    c.execute('CREATE TABLE isolated_effect(n INTEGER NOT NULL)')
                    c.execute('INSERT INTO isolated_effect VALUES (0)')
                    if trigger == 'ignore':
                        c.execute("CREATE TRIGGER ignore_search_state BEFORE UPDATE ON sects BEGIN UPDATE isolated_effect SET n=n+1; SELECT RAISE(IGNORE); END")
                    else:
                        c.execute("CREATE TRIGGER rewrite_search_state AFTER UPDATE OF mainbuff ON sects WHEN NEW.mainbuff <> '[tampered]' BEGIN UPDATE sects SET mainbuff='[tampered]' WHERE sect_id=NEW.sect_id; END")
                before = self._snapshot(db)
                result = SectBuffSearchSqlRepository(db).apply('state-'+trigger,'owner',1,'main','[]','[1]',100,200)
                self.assertEqual((result['status'], result['reason'], result['subject']), ('state_changed','sect_buff_search_state_write_rejected','1'))
                self.assertEqual(self._snapshot(db), before)

    def test_receipt_rewrite_and_abort_are_not_success(self):
        for mode in ('rewrite', 'abort'):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as temp:
                db=Path(temp)/'sect.db'
                migrate_sect(db)
                with db_backend.transaction(db) as c:
                    c.execute('CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,sect_id INTEGER,sect_position INTEGER)')
                    c.execute("INSERT INTO user_xiuxian VALUES('owner',1,0)")
                    c.execute('CREATE TABLE sects(sect_id INTEGER PRIMARY KEY,sect_owner TEXT,mainbuff TEXT,secbuff TEXT,sect_used_stone INTEGER,sect_materials INTEGER)')
                    c.execute("INSERT INTO sects VALUES(1,'owner','[]','[]',1000,2000)")
                    c.execute('CREATE TABLE isolated_effect(n INTEGER NOT NULL)')
                    c.execute('INSERT INTO isolated_effect VALUES (0)')
                    if mode == 'rewrite':
                        c.execute("CREATE TRIGGER rewrite_search_receipt AFTER INSERT ON sect_buff_search_operations BEGIN UPDATE sect_buff_search_operations SET new_value='[tampered]' WHERE operation_id=NEW.operation_id; END")
                    else:
                        c.execute("CREATE TRIGGER abort_search_receipt BEFORE INSERT ON sect_buff_search_operations BEGIN SELECT RAISE(ABORT, 'search receipt failed'); END")
                before = self._snapshot(db)
                if mode == 'abort':
                    with self.assertRaises(db_backend.IntegrityError):
                        SectBuffSearchSqlRepository(db).apply('receipt-abort','owner',1,'main','[]','[1]',100,200)
                else:
                    result = SectBuffSearchSqlRepository(db).apply('receipt-rewrite','owner',1,'main','[]','[1]',100,200)
                    self.assertEqual((result['status'], result['reason'], result['subject']), ('state_changed','sect_buff_search_receipt_write_rejected','receipt-rewrite'))
                self.assertEqual(self._snapshot(db), before)

    def test_stale_catalog_is_rejected_before_any_charge(self):
        with tempfile.TemporaryDirectory() as temp:
            db=Path(temp)/'sect.db'
            migrate_sect(db)
            with db_backend.transaction(db) as c:
                c.execute('CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,sect_id INTEGER,sect_position INTEGER)')
                c.execute("INSERT INTO user_xiuxian VALUES('owner',1,0)")
                c.execute('CREATE TABLE sects(sect_id INTEGER PRIMARY KEY,sect_owner TEXT,mainbuff TEXT,secbuff TEXT,sect_used_stone INTEGER,sect_materials INTEGER)')
                c.execute("INSERT INTO sects VALUES(1,'owner','[2]','[]',1000,2000)")
            result = SectBuffSearchSqlRepository(db).apply('stale','owner',1,'main','[]','[1]',100,200)
            self.assertEqual((result['status'], result['reason'], result['subject']), ('catalog_changed','sect_catalog_changed','1'))
            with db_backend.connection(db) as c:
                self.assertEqual(tuple(c.execute('SELECT mainbuff,sect_used_stone,sect_materials FROM sects').fetchone()), ('[2]',1000,2000))
