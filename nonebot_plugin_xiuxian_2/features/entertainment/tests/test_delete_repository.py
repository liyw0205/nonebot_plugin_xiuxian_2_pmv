from pathlib import Path
import tempfile
import unittest
from ..repository import EntertainmentRepository
from tests.test_db_backend import db_backend

class EntertainmentDeleteRepositoryTests(unittest.TestCase):
    def test_delete_selected_accounts_keeps_other_rows_and_secrets(self):
        with tempfile.TemporaryDirectory() as temp:
            state=Path(temp)/'accounts.json'
            state.write_text('[{"api_user_id":"11","secret":"a"},{"api_user_id":"22","secret":"b"}]',encoding='utf-8')
            repo=EntertainmentRepository(Path(temp)/'game.db')
            result=repo.delete_accounts(str(state), [1])
            self.assertEqual(result['status'],'applied')
            self.assertEqual(state.read_text(encoding='utf-8').count('22'),1)
            self.assertNotIn('11',state.read_text(encoding='utf-8'))
    def test_delete_rejects_invalid_without_write(self):
        with tempfile.TemporaryDirectory() as temp:
            state=Path(temp)/'accounts.json'; original='[{"api_user_id":"11"}]'; state.write_text(original,encoding='utf-8')
            repo=EntertainmentRepository(Path(temp)/'game.db')
            self.assertEqual(repo.delete_accounts(str(state), [2])['status'],'rejected')
            self.assertEqual(state.read_text(encoding='utf-8'),original)
