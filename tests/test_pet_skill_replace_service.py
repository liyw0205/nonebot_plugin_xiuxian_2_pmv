import nonebot; nonebot.init()
import tempfile, unittest
from pathlib import Path
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_pet.transaction_service import PetSkillReplaceService
from tests.test_db_backend import db_backend
class T(unittest.TestCase):
 def test_pet_facade_defers_skill_replace_service_construction(self):
  from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_pet
  self.assertIsNone(xiuxian_pet._pet_skill_replace_service_instance)

 def setUp(self):
  self.t=tempfile.TemporaryDirectory(); self.d=Path(self.t.name)/'d'
  with db_backend.transaction(self.d) as c: c.execute('CREATE TABLE player_pet_item(user_id TEXT,uid TEXT,skill_id TEXT,updated_at INTEGER)'); c.execute("INSERT INTO player_pet_item VALUES ('u','x','old',0)")
  self.s=PetSkillReplaceService(self.d)
 def tearDown(self): self.t.cleanup()
 def test_replace(self): self.assertEqual(self.s.replace('o','u','x','old','new').status,'applied'); self.assertEqual(self.s.replace('o','u','x','old','new').status,'duplicate')
 def test_stale(self): self.assertEqual(self.s.replace('x','u','x','bad','new').status,'state_changed')
