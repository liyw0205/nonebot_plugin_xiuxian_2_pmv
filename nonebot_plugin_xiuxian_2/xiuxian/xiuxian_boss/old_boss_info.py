try:
    import ujson as json
except ImportError:
    import json
from pathlib import Path
import os


class OLD_BOSS_INFO(object):
    def __init__(self):
        self.dir_path = Path(__file__).parent
        self.data_path = self.dir_path / "boss_info.json"
        self.data = self._load_data()

    def _load_data(self):
        """加载数据，失败时返回空字典但不覆盖文件"""
        if os.path.exists(self.data_path):
            try:
                with open(self.data_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    return data if data is not None else {}
            except Exception as e:
                print(f"警告: 读取 {self.data_path} 失败: {e}")
                return {}
        return {}

    def __save(self):
        """保存数据"""
        try:
            with open(self.data_path, 'w', encoding='utf-8') as f:
                json.dump(self.data, f, ensure_ascii=False, indent=4)
            return True
        except Exception as e:
            print(f"错误: 保存 {self.data_path} 失败: {e}")
            return False

    def save_boss(self, boss_data):
        """保存boss数据，不清空已有数据"""
        if boss_data is not None:
            self.data.update(boss_data)  # 仅更新，不清空
            return self.__save()
        return False

    def read_boss_info(self):
        """读取boss信息"""
        return self.data

old_boss_info = OLD_BOSS_INFO()