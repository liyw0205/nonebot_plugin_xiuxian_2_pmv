import nonebot; nonebot.init()
import tempfile, unittest
from pathlib import Path
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_pet.transaction_service import PetSkillReplaceService
from tests.test_db_backend import db_backend
class T(unittest.TestCase):
 def test_pet_facade_defers_skill_replace_service_construction(self):
  from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_pet
  self.assertIsNone(xiuxian_pet._pet_skill_replace_service_instance)

 def test_pet_facade_defers_sql_manager_construction(self):
  from pathlib import Path
  source = Path("nonebot_plugin_xiuxian_2/xiuxian/xiuxian_pet/__init__.py").read_text(encoding="utf-8")
  self.assertIn("_sql_message_instance = None", source)
  self.assertIn("def _sql_message(", source)
  self.assertIn("_sql_message().goods_num(", source)
  self.assertNotIn("sql_message = XiuxianDateManage()", source)
  pet_source = Path("nonebot_plugin_xiuxian_2/xiuxian/xiuxian_utils/pet_system.py").read_text(encoding="utf-8")
  self.assertIn("_player_data_manager_instance = None", pet_source)
  self.assertIn("def _player_data_manager(", pet_source)
  self.assertNotIn("player_data_manager = PlayerDataManager()", pet_source)
  self.assertIn("_player_data_manager().get_doc(", pet_source)
  self.assertIn("_player_data_manager().save_doc(", pet_source)
  self.assertIn("_player_data_manager().get_all_records(", pet_source)

 def setUp(self):
  self.t=tempfile.TemporaryDirectory(); self.d=Path(self.t.name)/'d'
  with db_backend.transaction(self.d) as c: c.execute('CREATE TABLE player_pet_item(user_id TEXT,uid TEXT,skill_id TEXT,updated_at INTEGER)'); c.execute("INSERT INTO player_pet_item VALUES ('u','x','old',0)")
  self.s=PetSkillReplaceService(self.d)
 def tearDown(self): self.t.cleanup()
 def test_replace(self): self.assertEqual(self.s.replace('o','u','x','old','new').status,'applied'); self.assertEqual(self.s.replace('o','u','x','old','new').status,'duplicate')
 def test_stale(self): self.assertEqual(self.s.replace('x','u','x','bad','new').status,'state_changed')
