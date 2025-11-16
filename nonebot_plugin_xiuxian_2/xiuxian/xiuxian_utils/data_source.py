try:
    import ujson as json
except ImportError:
    import json
import tomllib
from pathlib import Path

DATABASE = Path() / "data" / "xiuxian"


class JsonData:
    """处理基础配置 JSON数据"""

    def __init__(self):
        """基础配置 文件路径"""
        self.root_jsonpath = DATABASE / "灵根.json"
        self.level_rate_jsonpath = DATABASE / "突破概率.json"
        self.level_jsonpath = DATABASE / "境界.json"
        self.sect_json_pth = DATABASE / "宗门玩法配置.json"
        self.BACKGROUND_FILE = DATABASE / "image" / "background.png"
        self.BOSS_IMG = DATABASE / "boss_img" 
        self.BANNER_FILE = DATABASE / "image" / "banner.png"
        self.FONT_FILE = DATABASE / "font" / "SarasaMonoSC-Bold.ttf"

    def _load(self, path:Path):
        with open(path.with_suffix(".toml"), "rb") as f:
            return tomllib.load(f)

    def level_data(self):
        """境界数据"""
        return self._load(self.level_jsonpath)

    def sect_config_data(self):
        """宗门玩法配置"""
        return self._load(self.sect_json_pth)

    def root_data(self):
        """获取灵根数据"""
        return self._load(self.root_jsonpath)

    def level_rate_data(self):
        """获取境界突破概率"""
        return self._load(self.level_rate_jsonpath)

    def exercises_level_data(self):
        """获取炼体境界数据"""
        return self._load(self.exercises_level_jsonpath)


jsondata = JsonData()
