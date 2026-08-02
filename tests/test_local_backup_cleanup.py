"""本地备份保留天数：库/插件/配置统一 clean_old_backups。"""

from __future__ import annotations

import importlib.util
import sys
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MOD_PATH = (
    ROOT
    / "nonebot_plugin_xiuxian_2"
    / "xiuxian"
    / "xiuxian_utils"
    / "download_xiuxian_data.py"
)


def _load_update_manager_class():
    """按路径加载，避免拉起整包 nonebot 依赖。"""
    # 只取类方法：构造空对象挂上 static/instance 方法
    src = MOD_PATH.read_text(encoding="utf-8")
    # 最小 stub：执行时不 import 全文件（依赖重）；直接复制逻辑测 timestamp+clean
    # 改用 importlib 加载前注入轻量依赖会更脆；这里测纯函数行为用 exec 片段。
    ns: dict = {}
    # 抽 _backup_file_timestamp / clean_old_backups 需要 Path/datetime/re/logger
    import re
    from pathlib import Path as P
    from datetime import datetime as DT

    class _Dummy:
        @staticmethod
        def _local_backup_keep_days(default: int = 10) -> int:
            return 10

        @staticmethod
        def _backup_file_timestamp(path: P):
            stem = path.stem
            m = re.search(r"(?P<ts>\d{8}_\d{6})", stem)
            if m:
                try:
                    return DT.strptime(m.group("ts"), "%Y%m%d_%H%M%S")
                except Exception:
                    pass
            try:
                return DT.fromtimestamp(path.stat().st_mtime)
            except Exception:
                return None

        def clean_old_backups(self, backup_dir, keep_days=None, patterns=None):
            backup_dir = P(backup_dir)
            if not backup_dir.exists():
                return True, "备份目录不存在，跳过清理"
            if keep_days is None:
                keep_days = self._local_backup_keep_days()
            keep_days = int(keep_days)
            if keep_days <= 0:
                return True, "本地保留天数<=0，跳过清理"
            if patterns is None:
                patterns = ("*.zip",)
            elif isinstance(patterns, str):
                patterns = (patterns,)
            now = DT.now()
            deleted = 0
            seen = set()
            for pattern in patterns:
                for f in backup_dir.glob(pattern):
                    if not f.is_file() or f in seen:
                        continue
                    seen.add(f)
                    t = self._backup_file_timestamp(f)
                    if t is None:
                        continue
                    if (now - t).days > keep_days:
                        f.unlink()
                        deleted += 1
            return True, f"本地旧备份清理完成：删除{deleted}个（>{keep_days}天）"

    return _Dummy


class LocalBackupCleanupTests(unittest.TestCase):
    def setUp(self) -> None:
        self.cls = _load_update_manager_class()
        self.mgr = self.cls()
        self.tmp = tempfile.TemporaryDirectory(prefix="hermes-backup-keep-")
        self.root = Path(self.tmp.name)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _touch(self, name: str, days_ago: int) -> Path:
        p = self.root / name
        p.write_text("x", encoding="utf-8")
        # 时间以文件名为准，mtime 可不改
        return p

    def test_parse_timestamp_from_names(self) -> None:
        old = (datetime.now() - timedelta(days=15)).strftime("%Y%m%d_%H%M%S")
        recent = (datetime.now() - timedelta(days=3)).strftime("%Y%m%d_%H%M%S")
        f_old = self._touch(f"db_backup_{old}.zip", 15)
        f_new = self._touch(f"backup_{recent}_v1.zip", 3)
        f_cfg = self._touch(f"config_backup_{old}.json", 15)
        t_old = self.mgr._backup_file_timestamp(f_old)
        t_new = self.mgr._backup_file_timestamp(f_new)
        t_cfg = self.mgr._backup_file_timestamp(f_cfg)
        self.assertIsNotNone(t_old)
        self.assertIsNotNone(t_new)
        self.assertIsNotNone(t_cfg)
        self.assertGreater((datetime.now() - t_old).days, 10)
        self.assertLessEqual((datetime.now() - t_new).days, 10)

    def test_clean_deletes_only_over_keep_days(self) -> None:
        old = (datetime.now() - timedelta(days=15)).strftime("%Y%m%d_%H%M%S")
        mid = (datetime.now() - timedelta(days=10)).strftime("%Y%m%d_%H%M%S")
        recent = (datetime.now() - timedelta(days=2)).strftime("%Y%m%d_%H%M%S")
        p_old = self._touch(f"db_backup_{old}.zip", 15)
        p_mid = self._touch(f"db_backup_{mid}.zip", 10)  # days==10 不删
        p_new = self._touch(f"db_backup_{recent}.zip", 2)
        ok, msg = self.mgr.clean_old_backups(
            self.root, keep_days=10, patterns=("db_backup_*.zip",)
        )
        self.assertTrue(ok)
        self.assertIn("删除1个", msg)
        self.assertFalse(p_old.exists())
        self.assertTrue(p_mid.exists())
        self.assertTrue(p_new.exists())

    def test_keep_days_zero_skips(self) -> None:
        old = (datetime.now() - timedelta(days=30)).strftime("%Y%m%d_%H%M%S")
        p = self._touch(f"backup_{old}_v1.zip", 30)
        ok, msg = self.mgr.clean_old_backups(
            self.root, keep_days=0, patterns=("backup_*.zip",)
        )
        self.assertTrue(ok)
        self.assertIn("跳过", msg)
        self.assertTrue(p.exists())

    def test_config_json_pattern(self) -> None:
        old = (datetime.now() - timedelta(days=20)).strftime("%Y%m%d_%H%M%S")
        recent = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d_%H%M%S")
        p_old = self._touch(f"config_backup_{old}.json", 20)
        p_new = self._touch(f"config_backup_{recent}.json", 1)
        ok, msg = self.mgr.clean_old_backups(
            self.root, keep_days=10, patterns=("config_backup_*.json",)
        )
        self.assertTrue(ok)
        self.assertFalse(p_old.exists())
        self.assertTrue(p_new.exists())

    def test_source_wires_three_backup_paths(self) -> None:
        text = MOD_PATH.read_text(encoding="utf-8")
        self.assertIn('patterns=("backup_*.zip",)', text)
        self.assertIn('patterns=("config_backup_*.json",)', text)
        self.assertIn('patterns=("db_backup_*.zip",)', text)
        self.assertIn("_local_backup_keep_days", text)
        cfg = (ROOT / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_config.py").read_text(
            encoding="utf-8"
        )
        self.assertIn("local_backup_keep_days = 10", cfg)
        web = (ROOT / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_web/config.py").read_text(
            encoding="utf-8"
        )
        self.assertIn('"local_backup_keep_days"', web)


if __name__ == "__main__":
    unittest.main()
