from pathlib import Path
import os

from ...paths import get_paths
from ..xiuxian_utils import db_backend
from ..xiuxian_utils.json_store import load_json_file, save_json_file


class TWO_EXP_CD:
    def __init__(self):
        self.dir_path = Path(__file__).parent
        self.data_path = os.path.join(self.dir_path, "two_exp_cd.json")
        self.data = load_json_file(self.data_path, {"two_exp_cd": {}})

    def __save(self):
        save_json_file(self.data_path, self.data)

    def find_user(self, user_id):
        user_id = str(user_id)
        with db_backend.transaction(get_paths().player_db) as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS partner_two_exp_usage ("
                "user_id TEXT PRIMARY KEY,used_count INTEGER NOT NULL DEFAULT 0,"
                "updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
            )
            row = conn.execute(
                "SELECT used_count FROM partner_two_exp_usage WHERE user_id=%s", (user_id,)
            ).fetchone()
            if row is None:
                legacy_count = int(self.data["two_exp_cd"].get(user_id, 0) or 0)
                conn.execute(
                    "INSERT INTO partner_two_exp_usage(user_id,used_count) VALUES(%s,%s)",
                    (user_id, legacy_count),
                )
                return legacy_count
            return int(row[0])

    def add_user(self, user_id) -> bool:
        user_id = str(user_id)
        current = self.find_user(user_id)
        with db_backend.transaction(get_paths().player_db) as conn:
            conn.execute(
                "UPDATE partner_two_exp_usage SET used_count=%s,updated_at=CURRENT_TIMESTAMP WHERE user_id=%s",
                (current + 1, user_id),
            )
        return True

    def re_data(self):
        self.data = {"two_exp_cd": {}}
        self.__save()
        with db_backend.transaction(get_paths().player_db) as conn:
            conn.execute("DROP TABLE IF EXISTS partner_two_exp_usage")

    def remove_user(self, user_id, count=1):
        user_id = str(user_id)
        current_count = self.find_user(user_id)
        new_count = max(0, current_count - count)
        with db_backend.transaction(get_paths().player_db) as conn:
            conn.execute(
                "UPDATE partner_two_exp_usage SET used_count=%s,updated_at=CURRENT_TIMESTAMP WHERE user_id=%s",
                (new_count, user_id),
            )
        return True


two_exp_cd = TWO_EXP_CD()
