import json
from pathlib import Path

from ...paths import get_paths
from ..xiuxian_utils import db_backend
from ..xiuxian_utils.json_store import load_json_file, update_json_file


GLOBAL_BOSS_KEY = "global"


class OLD_BOSS_INFO(object):
    def __init__(self, data_path=None):
        self.data_path = Path(data_path) if data_path else Path(__file__).parent / "boss_info.json"
        self.data = self._load_data()

    def _load_data(self):
        """加载 Boss 状态；格式损坏时由中央 Store 备份并重置。"""
        if not self.data_path.exists():
            return {}
        return load_json_file(self.data_path, {}, dict)

    def save_boss(self, boss_data):
        """Persist administrative boss changes to the database and legacy mirror."""
        if boss_data is None:
            return False

        def merge(current):
            current.update(boss_data)
            return current

        self.data = update_json_file(
            self.data_path,
            {},
            merge,
            expected_type=dict,
        )
        bosses = self.data.get(GLOBAL_BOSS_KEY, [])
        conn = db_backend.connect(get_paths().player_db)
        try:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS world_boss_state ("
                "state_key TEXT PRIMARY KEY, bosses TEXT NOT NULL, updated_at TEXT NOT NULL)"
            )
            conn.execute(
                "INSERT INTO world_boss_state(state_key,bosses,updated_at) VALUES ('global',%s,CURRENT_TIMESTAMP) "
                "ON CONFLICT(state_key) DO UPDATE SET bosses=excluded.bosses,updated_at=excluded.updated_at",
                (json.dumps(bosses, ensure_ascii=False, sort_keys=True, separators=(",", ":")),),
            )
            conn.commit()
        finally:
            conn.close()
        return True

    def read_boss_info(self):
        """Read the database truth, migrating the legacy JSON snapshot once."""
        legacy = self._load_data()
        conn = db_backend.connect(get_paths().player_db)
        try:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS world_boss_state ("
                "state_key TEXT PRIMARY KEY, bosses TEXT NOT NULL, updated_at TEXT NOT NULL)"
            )
            row = conn.execute("SELECT bosses FROM world_boss_state WHERE state_key='global'").fetchone()
            if row is None:
                bosses = legacy.get(GLOBAL_BOSS_KEY, [])
                conn.execute(
                    "INSERT INTO world_boss_state(state_key,bosses,updated_at) VALUES ('global',%s,CURRENT_TIMESTAMP)",
                    (json.dumps(bosses, ensure_ascii=False, sort_keys=True, separators=(",", ":")),),
                )
                conn.commit()
            else:
                try:
                    bosses = json.loads(row[0])
                except (TypeError, ValueError):
                    bosses = []
            self.data = dict(legacy)
            self.data[GLOBAL_BOSS_KEY] = bosses if isinstance(bosses, list) else []
        finally:
            conn.close()
        return self.data

old_boss_info = OLD_BOSS_INFO()
