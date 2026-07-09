"""从 GitHub 拉取 astrbot_plugin_media_parser 的 core 包并修补 AstrBot 专用依赖。"""
from __future__ import annotations

import io
import re
import sys
import tempfile
import zipfile
from pathlib import Path
from urllib.request import Request, urlopen

from nonebot.log import logger
from nonebot_plugin_xiuxian_2.paths import get_paths

from ...xiuxian_utils.download_xiuxian_data import UpdateManager

_REPO_ZIP = (
    "https://github.com/drdon1234/astrbot_plugin_media_parser/archive/refs/heads/main.zip"
)
_ZIP_PREFIX = "astrbot_plugin_media_parser-main/"
_VENDOR_ROOT = Path(__file__).resolve().parent / "vendor"
_CORE_MARKER = _VENDOR_ROOT / "core" / "parser" / "manager.py"


def vendor_core_ready() -> bool:
    return _CORE_MARKER.is_file()


def _download_zip_direct(timeout: int = 120) -> bytes:
    req = Request(_REPO_ZIP, headers={"User-Agent": "nonebot-xiuxian-media-parser/1.0"})
    with urlopen(req, timeout=timeout) as resp:
        return resp.read()


def _download_zip_via_proxies() -> bytes:
    """与修仙资源更新相同：get_proxy_list → 测速 → 代理下载 main.zip。"""
    manager = UpdateManager()
    proxy_list = manager.get_proxy_list()
    working = manager.test_proxies(proxy_list, _REPO_ZIP)
    top = sorted(working, key=lambda x: x.get("delay", 9999))[:5]
    if not top:
        logger.info("娱乐媒体解析：无可用 GitHub 代理，将尝试直连下载 core …")
        return _download_zip_direct()

    logger.info(
        f"娱乐媒体解析：将经 {len(top)} 个 GitHub 代理拉取 core（最快 {top[0].get('delay', '?')}ms）"
    )
    errors: list[str] = []
    with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp:
        tmp_path = Path(tmp.name)

    try:
        for proxy in top:
            try:
                ok, msg = manager.download_with_proxy(_REPO_ZIP, tmp_path, proxy)
                if ok and tmp_path.is_file() and tmp_path.stat().st_size > 1024:
                    data = tmp_path.read_bytes()
                    if data[:2] == b"PK":
                        logger.info(
                            f"娱乐媒体解析：经代理 {proxy.get('name', proxy.get('url'))} "
                            f"下载 core 成功（{len(data) // 1024} KB）"
                        )
                        return data
                errors.append(f"{proxy.get('name')}: {msg}")
            except Exception as e:
                errors.append(f"{proxy.get('name')}: {e}")
        logger.warning(f"娱乐媒体解析：代理均失败，回退直连。{'; '.join(errors[:3])}")
        return _download_zip_direct()
    finally:
        try:
            tmp_path.unlink(missing_ok=True)
        except OSError:
            pass


def _download_zip() -> bytes:
    try:
        return _download_zip_via_proxies()
    except Exception as e:
        logger.warning(f"娱乐媒体解析：代理下载流程异常，最后尝试直连: {e}")
        return _download_zip_direct()


def _patch_config_manager_text(src: str, cache_dir: str) -> str:
    """去掉 AstrBot 导入，缓存目录改成本地 data。"""
    src = re.sub(
        r"try:\s*\n\s*from astrbot\.api import.*?\nexcept ImportError:\s*\n\s*astrbot_config = None\s*\n",
        "astrbot_config = None\n",
        src,
        flags=re.S,
    )
    src = re.sub(
        r"try:\s*\n\s*from astrbot\.core\.utils\.io import get_astrbot_data_path.*?\nexcept ImportError:\s*\n\s*get_astrbot_data_path = None\s*\n",
        "get_astrbot_data_path = None\n",
        flags=re.S,
    )

    def _replace_cache_fn(m: re.Match) -> str:
        return (
            "def _get_astrbot_plugin_cache_dir() -> str:\n"
            f"    return {cache_dir!r}\n"
        )

    src = re.sub(
        r"def _get_astrbot_plugin_cache_dir\(\)[^:]*:.*?(?=\n\n|\ndef )",
        _replace_cache_fn,
        src,
        count=1,
        flags=re.S,
    )
    return src


def ensure_vendor_core(*, force: bool = False) -> Path:
    """
    解压 core/ 到 vendor/core，并把 vendor 加入 sys.path。
    返回 vendor 目录。
    """
    if vendor_core_ready() and not force:
        _ensure_on_path(_VENDOR_ROOT)
        return _VENDOR_ROOT

    _VENDOR_ROOT.mkdir(parents=True, exist_ok=True)
    logger.info("娱乐媒体解析：正在拉取 astrbot_plugin_media_parser core …")
    try:
        data = _download_zip()
    except Exception as e:
        raise RuntimeError(
            f"无法下载媒体解析核心包（需网络）：{e}\n"
            f"也可手动将仓库 core 目录放到：{_VENDOR_ROOT / 'core'}"
        ) from e

    zf = zipfile.ZipFile(io.BytesIO(data))
    cache_dir = str((get_paths().data / "media_parser_cache").resolve())
    for name in zf.namelist():
        if not name.startswith(f"{_ZIP_PREFIX}core/"):
            continue
        rel = name[len(_ZIP_PREFIX) :]
        if rel.endswith("/"):
            (_VENDOR_ROOT / rel).mkdir(parents=True, exist_ok=True)
            continue
        content = zf.read(name)
        out = _VENDOR_ROOT / rel
        out.parent.mkdir(parents=True, exist_ok=True)
        if rel == "core/config_manager.py":
            text = content.decode("utf-8", errors="replace")
            text = _patch_config_manager_text(text, cache_dir)
            out.write_text(text, encoding="utf-8")
        else:
            out.write_bytes(content)

    if not _CORE_MARKER.is_file():
        raise RuntimeError(f"解压后缺少核心文件：{_CORE_MARKER}")

    (_VENDOR_ROOT / "core" / "__init__.py").write_text(
        '"""Vendored from astrbot_plugin_media_parser."""\n',
        encoding="utf-8",
    )
    logger.info(f"娱乐媒体解析：core 已就绪 -> {_VENDOR_ROOT}")
    _ensure_on_path(_VENDOR_ROOT)
    return _VENDOR_ROOT


def _ensure_on_path(vendor: Path) -> None:
    p = str(vendor.resolve())
    if p not in sys.path:
        sys.path.insert(0, p)
