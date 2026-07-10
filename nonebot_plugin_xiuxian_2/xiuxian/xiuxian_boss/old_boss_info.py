from pathlib import Path

from ..xiuxian_utils.json_store import load_json_file, update_json_file


GLOBAL_BOSS_KEY = "global"


class OLD_BOSS_INFO(object):
    def __init__(self, data_path=None):
        self.data_path = Path(data_path) if data_path else Path(__file__).parent / "boss_info.json"
        self.data = self._load_data()

    def _load_data(self):
        """加载 Boss 状态；格式损坏时由中央 Store 备份并重置。"""
        return load_json_file(self.data_path, {}, dict)

    def save_boss(self, boss_data):
        """串行合并并原子保存 Boss 状态。"""
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
        return True

    def read_boss_info(self):
        """读取boss信息"""
        self.data = self._load_data()
        return self.data

old_boss_info = OLD_BOSS_INFO()
