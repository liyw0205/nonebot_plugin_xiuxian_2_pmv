import os
import re
import zipfile
import tarfile
import wget
import json
import requests
import sqlite3
import subprocess
from urllib.parse import quote
import tempfile
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path, PurePosixPath
from nonebot.log import logger
from . import db_backend
from ..xiuxian_config import XiuConfig, Xiu_Plugin


def _safe_leaf_name(filename) -> str:
    name = Path(str(filename or "")).name
    if not name or name in {".", ".."} or "\x00" in name:
        raise ValueError("无效文件名")
    return name


def _path_under(base_dir, *parts) -> Path:
    base = Path(base_dir).resolve()
    candidate = base.joinpath(*[str(part) for part in parts]).resolve()
    try:
        candidate.relative_to(base)
    except ValueError as exc:
        raise ValueError("路径不在允许目录内") from exc
    return candidate


def _safe_archive_member_name(member_name) -> str:
    name = str(member_name or "").replace("\\", "/")
    while name.startswith("./"):
        name = name[2:]
    if not name or name in {".", "/"}:
        return ""
    if name.startswith("/") or re.match(r"^[A-Za-z]:", name) or "\x00" in name:
        raise ValueError(f"压缩包成员路径非法: {member_name}")
    parts = PurePosixPath(name).parts
    if any(part == ".." for part in parts):
        raise ValueError(f"压缩包成员路径非法: {member_name}")
    return PurePosixPath(*parts).as_posix()


def _safe_extract_zip(zip_ref: zipfile.ZipFile, target_dir):
    target = Path(target_dir)
    target.mkdir(parents=True, exist_ok=True)
    for info in zip_ref.infolist():
        safe_name = _safe_archive_member_name(info.filename)
        if not safe_name:
            continue
        output_path = _path_under(target, safe_name)
        if info.is_dir():
            output_path.mkdir(parents=True, exist_ok=True)
            continue
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with zip_ref.open(info) as src, open(output_path, "wb") as dst:
            shutil.copyfileobj(src, dst)


def _safe_extract_tar(tar_ref: tarfile.TarFile, target_dir):
    target = Path(target_dir)
    target.mkdir(parents=True, exist_ok=True)
    for member in tar_ref.getmembers():
        safe_name = _safe_archive_member_name(member.name)
        if not safe_name:
            continue
        if member.issym() or member.islnk() or member.isdev():
            raise ValueError(f"压缩包包含不允许的链接或设备文件: {member.name}")
        output_path = _path_under(target, safe_name)
        if member.isdir():
            output_path.mkdir(parents=True, exist_ok=True)
            continue
        if not member.isfile():
            continue
        output_path.parent.mkdir(parents=True, exist_ok=True)
        src = tar_ref.extractfile(member)
        if src is None:
            continue
        with src, open(output_path, "wb") as dst:
            shutil.copyfileobj(src, dst)


def download_xiuxian_data():
    """
    检测修仙插件必要文件是否存在（如字体文件），
    如不存在则自动下载最新的 xiuxian.zip
    """
    FONT_FILE = Path() / "data" / "xiuxian" / "font" / "SarasaMonoSC-Bold.ttf"
    XIUXIAN_ZIP_URL = "https://github.com/liyw0205/nonebot_plugin_xiuxian_2_pmv_file/releases/download/v0/xiuxian.zip"
    XIUXIAN_TEMP_ZIP_PATH = Path() / "data" / "xiuxian_data_temp.zip"
    path_xiuxian = Path() / "data" / "xiuxian"

    if FONT_FILE.exists():
        return True

    logger.opt(colors=True).info(f"<yellow>未检测到修仙插件资源文件（字体/图片），开始自动下载更新...</yellow>")    
    path_xiuxian.mkdir(parents=True, exist_ok=True)

    try:
        manager = UpdateManager()
        proxy_list = manager.get_proxy_list()

        # 测试代理延迟并排序
        working_proxies = manager.test_proxies(proxy_list, XIUXIAN_ZIP_URL)
        working_proxies_sorted = sorted(working_proxies, key=lambda x: x.get('delay', 9999))[:3]

        logger.info(f"检测到 {len(working_proxies_sorted)} 个可用代理，将尝试使用代理下载...")

        success = False
        error_msgs = []

        # 尝试使用代理下载
        for proxy in working_proxies_sorted:
            proxy_url = proxy['url']
            logger.info(f"尝试通过代理 {proxy_url} 下载...")
            try:
                success, message = manager.download_with_proxy(XIUXIAN_ZIP_URL, XIUXIAN_TEMP_ZIP_PATH, proxy)
                if success:
                    logger.opt(colors=True).info(f"<green>使用代理 {proxy_url} 下载成功！</green>")
                    break
                else:
                    error_msgs.append(f"代理 {proxy_url} 下载失败: {message}")

            except Exception as e:
                err_msg = f"代理 {proxy_url} 下载失败: {str(e)}"
                error_msgs.append(err_msg)
                logger.warning(err_msg)
                continue

        # 如果所有代理都失败，尝试直连下载
        if not success:
            logger.info(f"<yellow>所有代理下载失败，尝试直连下载...</yellow>")
            try:
                response = requests.get(XIUXIAN_ZIP_URL, stream=True, timeout=30)
                response.raise_for_status()

                total_size = int(response.headers.get('content-length', 0))
                downloaded = 0

                with open(XIUXIAN_TEMP_ZIP_PATH, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                            downloaded += len(chunk)
                            if total_size > 0:
                                percent = (downloaded / total_size) * 100
                                logger.info(f"直连下载进度: {percent:.1f}%")

                logger.opt(colors=True).info(f"<green>直连下载成功！</green>")
                success = True

            except Exception as e:
                logger.opt(colors=True).error(f"<red>直连下载也失败: {str(e)}</red>")
                raise RuntimeError(f"所有下载方式均失败，请手动下载：{XIUXIAN_ZIP_URL}")

        if not success or not XIUXIAN_TEMP_ZIP_PATH.exists():
            raise RuntimeError("未能成功下载更新包。")

        logger.opt(colors=True).info(f"<green>开始解压到：{path_xiuxian}</green>")
        with zipfile.ZipFile(XIUXIAN_TEMP_ZIP_PATH, 'r') as zip_ref:
            _safe_extract_zip(zip_ref, path_xiuxian)

        logger.opt(colors=True).info(f"<green>修仙插件资源更新完成！</green>")

        try:
            XIUXIAN_TEMP_ZIP_PATH.unlink()
            logger.opt(colors=True).info(f"<green>临时下载文件已删除</green>")
        except Exception as e:
            logger.warning(f"<yellow>删除临时文件失败：{e}</yellow>")

    except Exception as e:
        logger.opt(colors=True).error(f"<red>下载或解压过程中出错: {e}</red>")
        raise

class UpdateManager:
    def __init__(self):
        self.repo_owner = "liyw0205"
        self.repo_name = "nonebot_plugin_xiuxian_2_pmv"
        self.api_url = f"https://api.github.com/repos/{self.repo_owner}/{self.repo_name}/releases"
        self.current_version = self.get_current_version()

    # =========================
    # 基础版本/更新
    # =========================
    def get_current_version(self):
        """获取当前版本"""
        version_file = Path() / "data" / "xiuxian" / "version.txt"
        if version_file.exists():
            try:
                with open(version_file, 'r', encoding='utf-8') as f:
                    return f.read().strip()
            except Exception:
                pass
        return "unknown"

    def get_latest_releases(self, count=10):
        """获取最近的release信息"""
        try:
            response = requests.get(self.api_url, timeout=10)
            response.raise_for_status()
            releases = response.json()

            recent_releases = []
            for release in releases[:count]:
                recent_releases.append({
                    'tag_name': release.get('tag_name', ''),
                    'name': release.get('name', ''),
                    'published_at': release.get('published_at', ''),
                    'body': release.get('body', ''),
                    'assets': [
                        {
                            'name': asset.get('name', ''),
                            'browser_download_url': asset.get('browser_download_url', ''),
                            'size': asset.get('size', 0)
                        }
                        for asset in release.get('assets', [])
                    ]
                })
            return recent_releases
        except Exception as e:
            logger.error(f"获取GitHub release信息失败: {e}")
            return []

    def check_update(self):
        """检查更新"""
        releases = self.get_latest_releases(1)
        if not releases:
            return None, "无法获取更新信息"

        latest_release = releases[0]
        latest_version = latest_release['tag_name']

        if latest_version != self.current_version:
            return latest_release, f"发现新版本 {latest_version}，当前版本 {self.current_version}"
        else:
            return None, "当前已是最新版本"

    def download_release(self, release_tag, asset_name="project.tar.gz"):
        """下载指定的release资源，使用代理加速"""
        try:
            release_url = f"{self.api_url}/tags/{release_tag}"
            response = requests.get(release_url, timeout=10)
            response.raise_for_status()
            release_data = response.json()

            target_asset = None
            for asset in release_data.get('assets', []):
                if asset.get('name') == asset_name:
                    target_asset = asset
                    break

            if not target_asset:
                return False, f"未找到 {asset_name} 资源文件"

            temp_dir = Path(tempfile.mkdtemp())
            download_path = temp_dir / asset_name

            proxy_list = self.get_proxy_list()
            working_proxies = self.test_proxies(proxy_list, target_asset['browser_download_url'])

            success = False
            error_messages = []

            if working_proxies:
                logger.info(f"找到 {len(working_proxies)} 个可用代理，尝试使用代理下载...")
                for proxy in working_proxies[:3]:
                    try:
                        success, message = self.download_with_proxy(
                            target_asset['browser_download_url'],
                            str(download_path),
                            proxy
                        )
                        if success:
                            logger.info(f"使用代理 {proxy['url']} 下载成功")
                            return True, download_path
                        else:
                            error_messages.append(f"代理 {proxy['url']} 下载失败: {message}")
                    except Exception as e:
                        error_messages.append(f"代理 {proxy['url']} 下载错误: {str(e)}")

            if not success:
                logger.info("代理下载失败，尝试直接下载...")
                try:
                    wget.download(target_asset['browser_download_url'], out=str(download_path))
                    logger.info(f"\n直接下载完成: {download_path}")
                    return True, download_path
                except Exception as e:
                    error_messages.append(f"直接下载失败: {str(e)}")
                    return False, f"下载失败: {'; '.join(error_messages)}"

        except Exception as e:
            return False, f"下载失败: {str(e)}"

    # =========================
    # 代理
    # =========================
    def get_proxy_list(self):
        """代理列表"""
        proxies = [
            {"url": "https://gh-proxy.com/", "name": "gh-proxy.com"},
            {"url": "https://gh.jasonzeng.dev/", "name": "gh.jasonzeng.dev"},
            {"url": "https://ghproxy.net/", "name": "ghproxy.net"},
            {"url": "https://tvv.tw/", "name": "tvv.tw"},
            {"url": "https://git.yylx.win/", "name": "git.yylx.win"},
            {"url": "https://wget.la/", "name": "wget.la"},
            {"url": "https://github.dpik.top/", "name": "github.dpik.top"},
            {"url": "https://ghproxy.imciel.com/", "name": "ghproxy.imciel.com"}
        ]
        return proxies

    def _looks_like_expected_download(self, original_url, chunk, content_type=""):
        """判断代理返回内容是否像预期文件，避免把错误页当更新包。"""
        if not chunk:
            return False

        head = chunk[:512].lower()
        if b"<html" in head or b"<!doctype html" in head:
            return False

        url_path = str(original_url or "").split("?", 1)[0].lower()
        if url_path.endswith((".tar.gz", ".tgz")):
            return chunk.startswith(b"\x1f\x8b")
        if url_path.endswith(".zip"):
            return chunk.startswith((b"PK\x03\x04", b"PK\x05\x06", b"PK\x07\x08"))

        content_type = str(content_type or "").lower()
        if "text/html" in content_type:
            return False
        return True

    def test_proxies(self, proxy_list, test_url=None):
        """按实际目标链接测试代理，确认支持当前拼接下载方式。"""
        import time
        from concurrent.futures import ThreadPoolExecutor, as_completed

        target_url = test_url or "https://github.com/robots.txt"

        def test_proxy(proxy):
            try:
                start_time = time.time()
                proxy_test_url = f"{proxy['url']}{target_url}"
                headers = {
                    "User-Agent": "xiuxian-update-proxy-test",
                    "Range": "bytes=0-4095",
                    "Accept": "*/*"
                }
                with requests.get(proxy_test_url, headers=headers, stream=True, timeout=(5, 15)) as response:
                    if response.status_code not in (200, 206):
                        return None

                    first_chunk = b""
                    for chunk in response.iter_content(chunk_size=4096):
                        if chunk:
                            first_chunk = chunk
                            break

                    content_type = response.headers.get("content-type", "")
                    if not self._looks_like_expected_download(target_url, first_chunk, content_type):
                        return None

                    delay = int((time.time() - start_time) * 1000)
                    result = dict(proxy)
                    result['delay'] = delay
                    return result
            except Exception as e:
                logger.debug(f"代理 {proxy['url']} 测试失败: {str(e)}")
            return None

        working_proxies = []
        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_proxy = {executor.submit(test_proxy, proxy): proxy for proxy in proxy_list}
            for future in as_completed(future_to_proxy):
                try:
                    result = future.result()
                    if result:
                        working_proxies.append(result)
                except Exception as e:
                    logger.debug(f"代理测试异常: {str(e)}")

        working_proxies.sort(key=lambda x: x.get('delay', 9999))
        logger.info(
            f"找到 {len(working_proxies)} 个可用代理，最低延迟: "
            f"{[(p['name'], p.get('delay', '未知')) for p in working_proxies[:3]]}"
        )
        return working_proxies

    def download_with_proxy(self, original_url, download_path, proxy):
        """代理下载"""
        try:
            proxy_url = f"{proxy['url']}{original_url}"
            logger.info(f"尝试使用代理 {proxy['name']} 下载: {proxy_url}")

            response = requests.get(proxy_url, stream=True, timeout=30)
            response.raise_for_status()

            total_size = int(response.headers.get('content-length', 0))
            downloaded_size = 0

            chunks = response.iter_content(chunk_size=8192)
            first_chunk = b""
            for chunk in chunks:
                if chunk:
                    first_chunk = chunk
                    break

            content_type = response.headers.get("content-type", "")
            if not self._looks_like_expected_download(original_url, first_chunk, content_type):
                return False, "代理返回内容不是预期下载文件"

            with open(download_path, 'wb') as file:
                file.write(first_chunk)
                downloaded_size += len(first_chunk)
                if total_size > 0:
                    percent = (downloaded_size / total_size) * 100
                    print(f"\r下载进度: {percent:.1f}% ({downloaded_size}/{total_size} bytes)", end='')

                for chunk in chunks:
                    if chunk:
                        file.write(chunk)
                        downloaded_size += len(chunk)
                        if total_size > 0:
                            percent = (downloaded_size / total_size) * 100
                            print(f"\r下载进度: {percent:.1f}% ({downloaded_size}/{total_size} bytes)", end='')
            print()
            return True, "下载成功"
        except requests.exceptions.Timeout:
            return False, "下载超时"
        except requests.exceptions.ConnectionError:
            return False, "连接错误"
        except Exception as e:
            return False, f"下载错误: {str(e)}"

    # =========================
    # 解压/更新
    # =========================
    def _merge_directories(self, source_dir, target_dir):
        """安全合并目录（覆盖同名，不删额外）"""
        if not target_dir.exists():
            target_dir.mkdir(parents=True, exist_ok=True)

        for item in source_dir.iterdir():
            target_item = target_dir / item.name
            if item.is_dir():
                self._merge_directories(item, target_item)
            else:
                if self._is_sqlite_sidecar_name(item.name):
                    continue
                if item.name in self._sqlite_db_names():
                    self._restore_sqlite_file(item, target_item, item.name)
                    continue
                target_item.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(item, target_item)

    def extract_update(self, archive_path, backup=True):
        """解压更新文件"""
        extract_temp = None
        try:
            if backup:
                self.backup_current_version()

            extract_temp = Path(tempfile.mkdtemp())
            logger.info(f"开始解压文件: {archive_path}")
            with tarfile.open(archive_path, 'r:gz') as tar:
                _safe_extract_tar(tar, extract_temp)

            target_data_dir = Path() / "data"
            target_plugin_dir = Xiu_Plugin

            target_data_dir.mkdir(parents=True, exist_ok=True)
            target_plugin_dir.parent.mkdir(parents=True, exist_ok=True)

            self.update_version_file()

            logger.info("开始覆盖更新文件...")

            data_source = extract_temp / "." / "data"
            if data_source.exists():
                logger.info(f"合并更新data目录: {data_source} -> {target_data_dir}")
                self._merge_directories(data_source, target_data_dir)
            else:
                return False, "压缩包中缺少data目录"

            plugin_source = extract_temp / "." / "nonebot_plugin_xiuxian_2"
            if plugin_source.exists():
                logger.info(f"合并更新插件目录: {plugin_source} -> {target_plugin_dir}")
                self._merge_directories(plugin_source, target_plugin_dir)
            else:
                return False, "压缩包中缺少插件目录"

            return True, "更新成功"

        except Exception as e:
            return False, f"解压失败: {str(e)}"
        finally:
            try:
                if extract_temp and extract_temp.exists():
                    shutil.rmtree(extract_temp)
            except Exception:
                pass

    def update_version_file(self):
        """更新版本文件"""
        try:
            releases = self.get_latest_releases(1)
            if releases:
                latest_version = releases[0]['tag_name']
                version_file = Path() / "data" / "xiuxian" / "version.txt"
                version_file.parent.mkdir(parents=True, exist_ok=True)
                with open(version_file, 'w', encoding='utf-8') as f:
                    f.write(latest_version)
                logger.info(f"版本文件更新为: {latest_version}")
        except Exception as e:
            logger.error(f"更新版本文件失败: {e}")

    # =========================
    # WebDAV 基础工具
    # =========================
    def _gmt_to_cst_str(self, t: str) -> str:
        """WebDAV/GMT -> UTC+8 字符串"""
        if not t:
            return "未知"
        dt = self._parse_webdav_time(t)
        if not dt:
            return t
        dt_utc = dt.replace(tzinfo=timezone.utc)
        dt_cst = dt_utc.astimezone(timezone(timedelta(hours=8)))
        return dt_cst.strftime("%Y/%m/%d %H:%M:%S")

    def _webdav_encode_path(self, path: str) -> str:
        return "/".join(quote(p, safe="") for p in path.split("/"))

    def _webdav_join_url(self, base_url: str, rel_path: str) -> str:
        base = base_url.rstrip("/")
        enc = self._webdav_encode_path(rel_path.strip("/"))
        return f"{base}/{enc}" if enc else base

    def _webdav_remote_exists(self, url: str, auth: tuple) -> bool:
        try:
            r = requests.head(url, auth=auth, timeout=15, allow_redirects=False)
            return r.status_code in (200, 204, 206, 301, 302)
        except Exception:
            return False

    def _webdav_mkcol_recursive(self, base_url: str, target_subdir: str, rel_dir: str, auth: tuple):
        """
        递归创建目录：
        full_dir = target_subdir/rel_dir
        """
        try:
            full_dir = "/".join(x for x in [target_subdir.strip("/"), rel_dir.strip("/")] if x)
            if not full_dir:
                return True, "ok"

            parts = [p for p in full_dir.split("/") if p]
            current = ""
            for p in parts:
                current = f"{current}/{p}" if current else p
                url = self._webdav_join_url(base_url, current)
                r = requests.request("MKCOL", url, auth=auth, timeout=15, allow_redirects=False)
                if r.status_code not in (200, 201, 204, 301, 302, 405):
                    return False, f"MKCOL失败: {current} HTTP {r.status_code}"
            return True, "ok"
        except Exception as e:
            return False, f"创建目录异常: {e}"

    def _parse_webdav_time(self, t: str):
        if not t:
            return None
        try:
            return datetime.strptime(t.strip(), "%a, %d %b %Y %H:%M:%S GMT")
        except Exception:
            return None

    # =========================
    # 统一 WebDAV 路径（关键）
    # =========================
    def _get_webdav_paths(self):
        """
        统一返回 WebDAV 各目录：
        plugin_dir: .../{target_subdir}/{backup_folder}
        db_dir:     .../{target_subdir}/{backup_folder}/db_backup
        config_dir: .../{target_subdir}/{backup_folder}/config_backups
        """
        cfg = XiuConfig()

        if not cfg.webdav_url or not cfg.webdav_user or not cfg.webdav_pass:
            return False, "WebDAV 配置不完整（url/user/pass）", None

        base_url = cfg.webdav_url.strip().rstrip("/")
        target_subdir = (cfg.webdav_target_subdir or "").strip("/")
        backup_folder = (getattr(cfg, "webdav_backup_folder", "backups") or "backups").strip("/")

        plugin_rel = "/".join(x for x in [target_subdir, backup_folder] if x)
        db_rel = "/".join(x for x in [plugin_rel, "db_backup"] if x)
        config_rel = "/".join(x for x in [plugin_rel, "config_backups"] if x)

        return True, "ok", {
            "base_url": base_url,
            "auth": (cfg.webdav_user, cfg.webdav_pass),
            "plugin_rel": plugin_rel,
            "db_rel": db_rel,
            "config_rel": config_rel,
            "plugin_url": self._webdav_join_url(base_url, plugin_rel),
            "db_url": self._webdav_join_url(base_url, db_rel),
            "config_url": self._webdav_join_url(base_url, config_rel),
        }

    # =========================
    # 插件备份云端
    # =========================
    def upload_backup_to_webdav(self, local_file: Path):
        """上传备份文件到WebDAV（保留 backups 下相对结构）"""
        try:
            cfg = XiuConfig()
            if not getattr(cfg, "cloud_backup_enabled", False):
                return False, "云备份未开启"

            ok, msg, paths = self._get_webdav_paths()
            if not ok:
                return False, msg

            if not local_file.exists():
                return False, f"本地文件不存在: {local_file}"

            auth = paths["auth"]

            backup_root = Path() / "data" / "xiuxian" / "backups"
            try:
                rel = local_file.relative_to(backup_root).as_posix()
            except Exception:
                rel = local_file.name

            cloud_rel = "/".join(x for x in [paths["plugin_rel"], rel] if x)

            rel_dir = str(Path(cloud_rel).parent).replace("\\", "/")
            if rel_dir == ".":
                rel_dir = ""

            if self._webdav_remote_exists(self._webdav_join_url(paths["base_url"], cloud_rel), auth):
                return True, f"远端已存在，跳过上传: {cloud_rel}"

            mk_ok, mk_msg = self._webdav_mkcol_recursive(paths["base_url"], "", rel_dir, auth)
            if not mk_ok:
                return False, mk_msg

            remote_file_url = self._webdav_join_url(paths["base_url"], cloud_rel)
            with open(local_file, "rb") as f:
                r = requests.put(remote_file_url, data=f, auth=auth, timeout=120)

            if r.status_code in (200, 201, 204):
                return True, f"上传成功: {cloud_rel}"
            return False, f"上传失败 HTTP {r.status_code}: {cloud_rel}"

        except Exception as e:
            return False, f"WebDAV上传异常: {e}"

    def cleanup_webdav_old_backups(self):
        """清理云端旧备份（按 webdav_delete_days）"""
        try:
            cfg = XiuConfig()
            days_raw = getattr(cfg, "webdav_delete_days", 0)

            if days_raw in ("", None):
                return True, "未设置删除天数，跳过"

            try:
                days = int(days_raw)
            except Exception:
                return False, f"webdav_delete_days 非法: {days_raw}"

            if days <= 0:
                return True, "删除天数<=0，跳过"

            ok, msg, paths = self._get_webdav_paths()
            if not ok:
                return False, msg

            base_url = paths["base_url"]
            auth = paths["auth"]
            root_url = paths["plugin_url"]

            headers = {"Depth": "infinity"}
            body = """<?xml version="1.0" encoding="utf-8" ?>
<d:propfind xmlns:d="DAV:">
  <d:prop>
    <d:getlastmodified />
    <d:resourcetype />
  </d:prop>
</d:propfind>"""

            r = requests.request(
                "PROPFIND",
                root_url,
                data=body.encode("utf-8"),
                headers=headers,
                auth=auth,
                timeout=30
            )
            if r.status_code not in (207, 200):
                return False, f"PROPFIND失败 HTTP {r.status_code}"

            import xml.etree.ElementTree as ET
            ns = {"d": "DAV:"}
            root = ET.fromstring(r.text)

            deadline = datetime.utcnow() - timedelta(days=days)
            deleted = 0
            checked = 0

            for resp in root.findall("d:response", ns):
                href_el = resp.find("d:href", ns)
                if href_el is None or not href_el.text:
                    continue
                href = href_el.text

                rt = resp.find(".//d:resourcetype", ns)
                is_collection = (rt is not None and rt.find("d:collection", ns) is not None)
                if is_collection:
                    continue

                lm_el = resp.find(".//d:getlastmodified", ns)
                lm = self._parse_webdav_time(lm_el.text if lm_el is not None else "")
                if lm is None:
                    continue

                checked += 1
                if lm < deadline:
                    file_url = href if href.startswith(("http://", "https://")) else f"{base_url.rstrip('/')}/{href.lstrip('/')}"
                    dr = requests.delete(file_url, auth=auth, timeout=20)
                    if dr.status_code in (200, 202, 204):
                        deleted += 1

            return True, f"云端清理完成：检查{checked}个文件，删除{deleted}个（>{days}天）"

        except Exception as e:
            return False, f"云端清理异常: {e}"

    def list_webdav_backups(self):
        """列出云端插件备份"""
        try:
            ok, msg, paths = self._get_webdav_paths()
            if not ok:
                return False, "未配置 WebDAV 信息。请前往配置管理设置。"

            root_url = paths["plugin_url"]
            auth = paths["auth"]
            backup_folder = Path(paths["plugin_rel"]).name if paths["plugin_rel"] else "backups"

            headers = {"Depth": "1"}
            r = requests.request("PROPFIND", root_url, auth=auth, timeout=15, headers=headers)

            if r.status_code not in (207, 200):
                return False, f"无法连接到 WebDAV (HTTP {r.status_code})。"

            import xml.etree.ElementTree as ET
            ns = {"d": "DAV:"}
            root = ET.fromstring(r.text)

            cloud_files = []
            for resp in root.findall("d:response", ns):
                href_el = resp.find("d:href", ns)
                if href_el is None or not href_el.text:
                    continue
                href = href_el.text
                name = href.rstrip('/').split('/')[-1]

                if not name or name in (backup_folder,):
                    continue

                # 过滤目录
                rt = resp.find(".//d:resourcetype", ns)
                is_collection = (rt is not None and rt.find("d:collection", ns) is not None)
                if is_collection:
                    continue

                size_el = resp.find(".//d:getcontentlength", ns)
                time_el = resp.find(".//d:getlastmodified", ns)
                raw_modified = time_el.text if time_el is not None else ""

                cloud_files.append({
                    "filename": name,
                    "size": int(size_el.text) if size_el is not None and str(size_el.text).isdigit() else 0,
                    "modified": self._gmt_to_cst_str(raw_modified)
                })

            return True, sorted(cloud_files, key=lambda x: x['modified'], reverse=True)
        except Exception as e:
            return False, f"WebDAV 访问异常: {str(e)}"

    def download_from_webdav(self, cloud_filename):
        """下载云端插件备份到本地 backups"""
        try:
            cloud_filename = _safe_leaf_name(cloud_filename)
            ok, msg, paths = self._get_webdav_paths()
            if not ok:
                return False, msg

            auth = paths["auth"]
            remote_rel = "/".join(x for x in [paths["plugin_rel"], cloud_filename] if x)
            remote_url = self._webdav_join_url(paths["base_url"], remote_rel)

            local_path = Path() / "data" / "xiuxian" / "backups" / cloud_filename
            local_path.parent.mkdir(parents=True, exist_ok=True)

            r = requests.get(remote_url, auth=auth, timeout=300, stream=True)
            if r.status_code == 200:
                with open(local_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=16384):
                        if chunk:
                            f.write(chunk)
                return True, local_path
            return False, f"下载失败: HTTP {r.status_code}"
        except Exception as e:
            return False, f"下载过程中出错: {str(e)}"

    # =========================
    # 配置备份云端（统一到 backups/config_backups）
    # =========================
    def upload_config_backup_to_webdav(self, local_file_path):
        """上传配置备份到云端 config_backups（统一路径）"""
        try:
            ok, msg, paths = self._get_webdav_paths()
            if not ok:
                return False, msg

            file_path = Path(local_file_path)
            if not file_path.exists():
                return False, f"本地文件不存在: {file_path}"

            auth = paths["auth"]

            mk_ok, mk_msg = self._webdav_mkcol_recursive(paths["base_url"], "", paths["config_rel"], auth)
            if not mk_ok:
                return False, mk_msg

            remote_url = self._webdav_join_url(paths["base_url"], f"{paths['config_rel']}/{file_path.name}")
            with open(file_path, "rb") as f:
                resp = requests.put(remote_url, data=f, auth=auth, timeout=60)

            if resp.status_code in (200, 201, 204):
                return True, f"配置备份已上传: {file_path.name}"
            return False, f"上传失败，HTTP {resp.status_code}: {resp.text[:200]}"
        except Exception as e:
            return False, f"上传配置备份失败: {e}"

    def list_webdav_config_backups(self):
        """列出云端 config_backups 目录中的配置备份（统一路径）"""
        try:
            ok, msg, paths = self._get_webdav_paths()
            if not ok:
                return False, msg

            auth = paths["auth"]
            self._webdav_mkcol_recursive(paths["base_url"], "", paths["config_rel"], auth)

            headers = {"Depth": "1"}
            body = """<?xml version="1.0" encoding="utf-8" ?>
                <d:propfind xmlns:d="DAV:">
                    <d:prop>
                        <d:getlastmodified />
                        <d:getcontentlength />
                        <d:displayname />
                        <d:resourcetype />
                    </d:prop>
                </d:propfind>
            """
            resp = requests.request(
                "PROPFIND",
                paths["config_url"],
                data=body.encode("utf-8"),
                headers=headers,
                auth=auth,
                timeout=30
            )

            if resp.status_code not in (207, 200):
                return False, f"读取云端配置备份失败，HTTP {resp.status_code}"

            import xml.etree.ElementTree as ET
            ns = {"d": "DAV:"}
            root = ET.fromstring(resp.text)

            backups = []
            for r in root.findall("d:response", ns):
                propstat = r.find("d:propstat", ns)
                if propstat is None:
                    continue
                prop = propstat.find("d:prop", ns)
                if prop is None:
                    continue

                displayname = prop.find("d:displayname", ns)
                resourcetype = prop.find("d:resourcetype", ns)
                contentlength = prop.find("d:getcontentlength", ns)
                lastmodified = prop.find("d:getlastmodified", ns)

                name = displayname.text if displayname is not None and displayname.text else ""
                if not name or name == "config_backups":
                    continue

                is_dir = (resourcetype is not None and resourcetype.find("d:collection", ns) is not None)
                if is_dir:
                    continue

                if not name.endswith(".json"):
                    continue

                size = int(contentlength.text) if contentlength is not None and contentlength.text and contentlength.text.isdigit() else 0
                raw_modified = lastmodified.text if lastmodified is not None and lastmodified.text else ""
                modified = self._gmt_to_cst_str(raw_modified)

                backups.append({
                    "filename": name,
                    "size": size,
                    "modified": modified
                })

            backups.sort(key=lambda x: x["modified"], reverse=True)
            return True, backups

        except Exception as e:
            return False, f"获取云端配置备份失败: {e}"

    def download_config_backup_from_webdav(self, filename, overwrite=False):
        """从云端下载配置备份到本地 config_backups（统一路径）"""
        try:
            filename = _safe_leaf_name(filename)
            ok, msg, paths = self._get_webdav_paths()
            if not ok:
                return False, msg

            auth = paths["auth"]

            local_dir = Path() / "data" / "xiuxian" / "backups" / "config_backups"
            local_dir.mkdir(parents=True, exist_ok=True)
            local_path = local_dir / filename

            if local_path.exists() and not overwrite:
                return False, "FILE_EXISTS"

            remote_url = self._webdav_join_url(paths["base_url"], f"{paths['config_rel']}/{filename}")
            resp = requests.get(remote_url, auth=auth, timeout=60)
            if resp.status_code != 200:
                return False, f"下载失败，HTTP {resp.status_code}"

            with open(local_path, "wb") as f:
                f.write(resp.content)

            return True, local_path
        except Exception as e:
            return False, f"下载配置备份失败: {e}"

    def upload_config_to_cloud(self, local_path):
        """
        兼容旧函数：统一转发到 upload_config_backup_to_webdav
        """
        return self.upload_config_backup_to_webdav(local_path)

    def cloud_restore_config_backup(self, filename):
        """
        云端配置恢复：
        1) 本地有则直接读取
        2) 本地无则先下载
        3) 返回配置 dict
        """
        try:
            filename = _safe_leaf_name(filename)
            local_path = _path_under(Path() / "data" / "xiuxian" / "backups" / "config_backups", filename)
            if not local_path.exists():
                ok, result = self.download_config_backup_from_webdav(filename, overwrite=False)
                if not ok and result != "FILE_EXISTS":
                    return False, f"云端下载失败: {result}"

            if not local_path.exists():
                return False, "本地配置备份文件不存在"

            with open(local_path, "r", encoding="utf-8") as f:
                backup_data = json.load(f)

            metadata = backup_data.get("_metadata", {})
            if "_metadata" in backup_data:
                del backup_data["_metadata"]

            return True, {
                "data": backup_data,
                "metadata": metadata,
                "local_path": str(local_path)
            }
        except Exception as e:
            return False, f"云端恢复配置失败: {e}"

    # =========================
    # 备份/恢复（插件）
    # =========================
    def enhanced_backup_current_version(self):
        """备份当前版本"""
        try:
            backup_dir = Path() / "data" / "xiuxian" / "backups"
            backup_dir.mkdir(parents=True, exist_ok=True)

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_path = backup_dir / f"backup_{timestamp}_{self.current_version}.zip"

            skip_dirs = {"backups", "config_backups", "db_backup", "cache", "boss_img", "font", "卡图", "__pycache__"}

            with zipfile.ZipFile(backup_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                data_dir = Path() / "data" / "xiuxian"
                if data_dir.exists():
                    for root, dirs, files in os.walk(data_dir):
                        root_path = Path(root)
                        if any(skip_dir in root_path.parts for skip_dir in skip_dirs):
                            continue
                        for file in files:
                            file_path = Path(root) / file
                            try:
                                arcname = file_path.relative_to(data_dir.parent.parent)
                                zipf.write(file_path, arcname)
                            except Exception as e:
                                logger.warning(f"备份文件跳过: {file_path}, 错误: {e}")

                plugin_dir = Xiu_Plugin
                if plugin_dir.exists():
                    for root, dirs, files in os.walk(plugin_dir):
                        root_path = Path(root)
                        if any(skip_dir in root_path.parts for skip_dir in skip_dirs):
                            continue
                        for file in files:
                            file_path = Path(root) / file
                            try:
                                arcname = file_path.relative_to(plugin_dir.parent.parent.parent)
                                zipf.write(file_path, arcname)
                            except Exception as e:
                                logger.warning(f"备份文件跳过: {file_path}, 错误: {e}")

            logger.info(f"备份完成: {backup_path}")

            try:
                cfg = XiuConfig()
                if getattr(cfg, "cloud_backup_enabled", False):
                    up_ok, up_msg = self.upload_backup_to_webdav(backup_path)
                    if up_ok:
                        logger.info(f"云备份结果: {up_msg}")
                        clean_ok, clean_msg = self.cleanup_webdav_old_backups()
                        if clean_ok:
                            logger.info(clean_msg)
                        else:
                            logger.warning(clean_msg)
                    else:
                        logger.warning(f"云备份失败: {up_msg}")
            except Exception as e:
                logger.warning(f"云备份执行异常: {e}")

            return True, backup_path
        except Exception as e:
            logger.error(f"备份失败: {e}")
            return False, str(e)

    def backup_all_configs(self):
        """备份所有配置"""
        try:
            config = XiuConfig()
            config_values = {}
            from ..xiuxian_web import CONFIG_EDITABLE_FIELDS

            for field_name in CONFIG_EDITABLE_FIELDS.keys():
                if hasattr(config, field_name):
                    config_values[field_name] = getattr(config, field_name)

            backup_dir = Path() / "data" / "xiuxian" / "backups" / "config_backups"
            backup_dir.mkdir(parents=True, exist_ok=True)

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_filename = f"config_backup_{timestamp}.json"
            backup_path = backup_dir / backup_filename

            config_values['_metadata'] = {
                'backup_time': datetime.now().isoformat(),
                'backup_fields': list(config_values.keys()),
                'version': self.current_version,
                'type': 'config_backup',
                'backup_type': 'full'
            }

            with open(backup_path, 'w', encoding='utf-8') as f:
                json.dump(config_values, f, ensure_ascii=False, indent=2)

            logger.info(f"全选配置备份完成: {backup_filename}")

            try:
                cfg = XiuConfig()
                if getattr(cfg, "cloud_backup_enabled", False):
                    up_ok, up_msg = self.upload_config_backup_to_webdav(backup_path)
                    if up_ok:
                        logger.info(f"配置云备份结果: {up_msg}")
                        clean_ok, clean_msg = self.cleanup_webdav_old_backups()
                        if clean_ok:
                            logger.info(clean_msg)
                        else:
                            logger.warning(clean_msg)
                    else:
                        logger.warning(f"配置云备份失败: {up_msg}")
            except Exception as e:
                logger.warning(f"配置云备份执行异常: {e}")

            return True, backup_path
        except Exception as e:
            logger.error(f"配置备份失败: {str(e)}")
            return False, f"配置备份失败: {str(e)}"

    def perform_update_with_backup(self, release_tag):
        """执行完整更新流程"""
        try:
            logger.info(f"开始更新到版本: {release_tag}")

            logger.info("创建自动插件备份...")
            plugin_backup_success, plugin_backup_result = self.enhanced_backup_current_version()
            if not plugin_backup_success:
                return False, f"插件备份失败: {plugin_backup_result}"

            logger.info("创建自动数据库备份...")
            db_backup_success, db_backup_result = self.backup_db_files()
            if not db_backup_success:
                return False, f"数据库备份失败: {db_backup_result}"

            logger.info("创建自动配置备份...")
            backup_success, backup_result = self.backup_all_configs()
            if not backup_success:
                return False, f"配置备份失败: {backup_result}"

            config_backup_path = backup_result

            logger.info("下载更新包...")
            success, result = self.download_release(release_tag)
            if not success:
                return False, result

            archive_path = result
            logger.info(f"下载完成: {archive_path}")

            logger.info("解压更新包...")
            success, result = self.extract_update(archive_path, backup=False)

            if success and backup_success:
                logger.info("开始恢复配置...")
                restore_success, restore_message = self.restore_config_from_backup(config_backup_path)
                if not restore_success:
                    logger.warning(f"配置恢复失败: {restore_message}")
                else:
                    logger.info("配置恢复成功")

            try:
                if archive_path.exists():
                    temp_dir = archive_path.parent
                    if temp_dir.exists():
                        shutil.rmtree(temp_dir)
            except Exception as e:
                logger.warning(f"清理临时文件失败: {e}")

            if success:
                logger.info("更新成功完成")
            else:
                logger.error(f"更新失败: {result}")

            return success, result

        except Exception as e:
            logger.error(f"更新过程中出现错误: {str(e)}")
            return False, f"更新过程中出现错误: {str(e)}"

    def restore_config_from_backup(self, backup_path):
        """从配置备份恢复"""
        try:
            if not backup_path.exists():
                return False, "备份文件不存在"

            with open(backup_path, 'r', encoding='utf-8') as f:
                backup_data = json.load(f)

            if '_metadata' in backup_data:
                del backup_data['_metadata']

            success, message = self.save_config_values(backup_data)
            if not success:
                return False, f"保存配置失败: {message}"

            return True, "配置恢复成功"
        except Exception as e:
            return False, f"恢复配置失败: {str(e)}"

    def save_config_values(self, new_values):
        """保存配置到文件"""
        config_file_path = Xiu_Plugin / "xiuxian" / "xiuxian_config.py"
        from ..xiuxian_web import CONFIG_EDITABLE_FIELDS

        if not config_file_path.exists():
            return False, "配置文件不存在"

        try:
            with open(config_file_path, 'r', encoding='utf-8') as f:
                content = f.read()

            for field_name, new_value in new_values.items():
                if field_name in CONFIG_EDITABLE_FIELDS:
                    field_type = CONFIG_EDITABLE_FIELDS[field_name]["type"]

                    if field_type == "list[int]":
                        if isinstance(new_value, str):
                            cleaned_value = re.sub(r'[\[\]\'"\s]', '', new_value)
                            if cleaned_value:
                                try:
                                    int_list = [int(x.strip()) for x in cleaned_value.split(',') if x.strip()]
                                    formatted_value = f"[{', '.join(map(str, int_list))}]"
                                except ValueError:
                                    formatted_value = "[]"
                            else:
                                formatted_value = "[]"
                        else:
                            formatted_value = str(new_value)

                    elif field_type == "list[str]":
                        if isinstance(new_value, str):
                            cleaned_value = re.sub(r'[\[\]]', '', new_value)
                            str_list = []
                            for item in cleaned_value.split(','):
                                item = item.strip()
                                item = re.sub(r'^[\'"]|[\'"]$', '', item)
                                if item:
                                    str_list.append(f'"{item}"')
                            formatted_value = f"[{', '.join(str_list)}]"
                        else:
                            formatted_value = str(new_value)

                    elif field_type == "bool":
                        formatted_value = "True" if str(new_value).lower() in ('true', '1', 'yes') else "False"

                    elif field_type == "select":
                        formatted_value = f'"{new_value}"'

                    elif field_type == "int":
                        try:
                            formatted_value = str(int(new_value))
                        except (ValueError, TypeError):
                            formatted_value = "0"

                    elif field_type == "float":
                        try:
                            formatted_value = str(float(new_value))
                        except (ValueError, TypeError):
                            formatted_value = "0.0"

                    else:
                        if not (isinstance(new_value, str) and (
                            (new_value.startswith('"') and new_value.endswith('"')) or
                            (new_value.startswith("'") and new_value.endswith("'"))
                        )):
                            formatted_value = f'"{new_value}"'
                        else:
                            formatted_value = new_value

                    pattern = rf"(self\.{re.escape(field_name)}\s*=\s*).+"
                    if re.search(pattern, content):
                        content = re.sub(
                            pattern,
                            lambda m: f"{m.group(1)}{formatted_value}",
                            content
                        )

            with open(config_file_path, 'w', encoding='utf-8') as f:
                f.write(content)

            return True, "配置保存成功"
        except Exception as e:
            return False, f"保存配置时出错: {str(e)}"

    def _restore_files_from_backup(self, backup_root):
        """从备份根目录恢复文件"""
        data_backup_path = backup_root / "data"
        if data_backup_path.exists():
            target_data_dir = Path() / "data"
            self._merge_directories(data_backup_path, target_data_dir)

        plugin_backup_path = backup_root / "src" / "plugins" / "nonebot_plugin_xiuxian_2"
        if plugin_backup_path.exists():
            target_plugin_dir = Xiu_Plugin
            self._merge_directories(plugin_backup_path, target_plugin_dir)

    def get_backups(self):
        """获取所有插件备份"""
        backup_dir = Path() / "data" / "xiuxian" / "backups"
        backups = []

        if backup_dir.exists():
            for file in backup_dir.glob("backup_*.zip"):
                filename = file.name
                parts = file.stem.split('_')
                if len(parts) >= 4:
                    timestamp = f"{parts[1]}_{parts[2]}"
                    version = '_'.join(parts[3:])
                    backups.append({
                        'filename': filename,
                        'path': str(file),
                        'timestamp': timestamp,
                        'version': version,
                        'size': file.stat().st_size,
                        'created_at': datetime.fromtimestamp(file.stat().st_ctime).isoformat()
                    })

        backups.sort(key=lambda x: x['created_at'], reverse=True)
        return backups

    def restore_backup(self, backup_filename):
        """从插件备份恢复"""
        temp_dir = None
        try:
            backup_filename = _safe_leaf_name(backup_filename)
            backup_dir = Path() / "data" / "xiuxian" / "backups"
            backup_path = _path_under(backup_dir, backup_filename)

            if not backup_path.exists():
                return False, f"备份文件不存在: {backup_filename}"
            if not backup_path.is_file():
                return False, f"无效备份文件: {backup_filename}"

            logger.info(f"开始从备份恢复: {backup_filename}")

            temp_dir = Path(tempfile.mkdtemp())
            with zipfile.ZipFile(backup_path, 'r') as zipf:
                names = {
                    _safe_archive_member_name(info.filename)
                    for info in zipf.infolist()
                    if _safe_archive_member_name(info.filename)
                }
                _safe_extract_zip(zipf, temp_dir)

            self._restore_files_from_backup(temp_dir)
            shutil.rmtree(temp_dir)
            temp_dir = None

            restored_dbs = [
                name
                for name in self._sqlite_db_names()
                if name in names or f"data/xiuxian/{name}" in names
            ]
            if restored_dbs:
                self._reload_restored_database_handles(restored_dbs)
                self._compact_pet_storage_after_database_restore()

            version_match = re.search(r'backup_.*_(v?[\d.]+)\.zip', backup_filename)
            if version_match:
                version = version_match.group(1)
                version_file = Path() / "data" / "xiuxian" / "version.txt"
                version_file.parent.mkdir(parents=True, exist_ok=True)
                with open(version_file, 'w', encoding='utf-8') as f:
                    f.write(version)

            logger.info(f"备份恢复完成: {backup_filename}")
            return True, f"成功从备份 {backup_filename} 恢复"

        except Exception as e:
            logger.error(f"恢复备份失败: {str(e)}")
            return False, f"恢复备份失败: {str(e)}"
        finally:
            if temp_dir is not None and temp_dir.exists():
                shutil.rmtree(temp_dir, ignore_errors=True)

    # =========================
    # 数据库备份/恢复 + 云端
    # =========================
    def _compact_pet_storage_after_database_restore(self):
        try:
            from .pet_system import migrate_all_legacy_pet_docs, reset_pet_storage_state

            reset_pet_storage_state()
            migrated = migrate_all_legacy_pet_docs()
            if migrated:
                logger.info(f"数据库恢复后已整理宠物旧表数据：{migrated} 条")
        except Exception as e:
            logger.warning(f"数据库恢复后整理宠物旧表失败: {e}")

    def _sqlite_db_names(self):
        return ["xiuxian.db", "xiuxian_impart.db", "player.db", "trade.db"]

    def _validate_sqlite_file(self, db_path: Path):
        """校验 SQLite 文件可读，避免把损坏库恢复到线上。"""
        conn = None
        try:
            conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
            row = conn.execute("PRAGMA quick_check").fetchone()
            if row and str(row[0]).lower() == "ok":
                return True, ""
            return False, f"quick_check={row[0] if row else '无结果'}"
        except sqlite3.DatabaseError as e:
            return False, str(e)
        finally:
            if conn is not None:
                conn.close()

    def _sqlite_sidecar_paths(self, db_path: Path):
        return [db_path.with_name(f"{db_path.name}-wal"), db_path.with_name(f"{db_path.name}-shm")]

    def _is_sqlite_sidecar_name(self, filename: str) -> bool:
        return any(
            filename == f"{db_name}-wal" or filename == f"{db_name}-shm"
            for db_name in self._sqlite_db_names()
        )

    def _remove_sqlite_sidecars(self, db_path: Path):
        for sidecar in self._sqlite_sidecar_paths(db_path):
            try:
                if sidecar.exists():
                    sidecar.unlink()
            except Exception as e:
                logger.warning(f"[DB恢复] 清理 WAL 边车失败 {sidecar}: {e}")

    def _copy_salvage_table_rows(self, src_conn, dst_conn, table_name: str, columns: list[str]):
        q_table = db_backend.quote_ident(table_name)
        col_sql = ", ".join(db_backend.quote_ident(col) for col in columns)
        insert_sql = (
            f"INSERT OR IGNORE INTO {q_table} "
            f"({col_sql}) VALUES ({', '.join(['?'] * len(columns))})"
        )

        def insert_rows(rows):
            rows = list(rows)
            if not rows:
                return 0
            before = dst_conn.total_changes
            dst_conn.executemany(insert_sql, ([row[col] for col in columns] for row in rows))
            return dst_conn.total_changes - before

        try:
            return insert_rows(src_conn.execute(f"SELECT {col_sql} FROM {q_table}").fetchall()), 0
        except Exception:
            pass

        try:
            row = src_conn.execute(f"SELECT min(rowid), max(rowid) FROM {q_table}").fetchone()
            min_rowid, max_rowid = row[0], row[1]
        except Exception as e:
            logger.warning(f"[DB恢复] {table_name} 无法按 rowid 抢救: {e}")
            return 0, 1

        if min_rowid is None or max_rowid is None:
            return 0, 0

        def copy_range(start: int, end: int):
            try:
                rows = src_conn.execute(
                    f"SELECT {col_sql} FROM {q_table} WHERE rowid BETWEEN ? AND ? ORDER BY rowid",
                    (start, end),
                ).fetchall()
                return insert_rows(rows), 0
            except Exception:
                if start == end:
                    return 0, 1
                mid = (start + end) // 2
                left_count, left_skipped = copy_range(start, mid)
                right_count, right_skipped = copy_range(mid + 1, end)
                return left_count + right_count, left_skipped + right_skipped

        copied = 0
        skipped = 0
        chunk_size = 512
        current = int(min_rowid)
        max_rowid = int(max_rowid)
        while current <= max_rowid:
            end = min(current + chunk_size - 1, max_rowid)
            chunk_copied, chunk_skipped = copy_range(current, end)
            copied += chunk_copied
            skipped += chunk_skipped
            current = end + 1
        return copied, skipped

    def _python_salvage_sqlite_db(self, src_path: Path, dst_path: Path):
        """纯 Python 逐表抢救：用于 sqlite3 CLI 不可用或 .recover 失败时。"""
        src_conn = None
        dst_conn = None
        temp_dir = Path(tempfile.mkdtemp())
        temp_db = temp_dir / dst_path.name
        table_stats = []
        failed_objects = []
        try:
            src_conn = sqlite3.connect(src_path, timeout=30)
            src_conn.row_factory = sqlite3.Row
            dst_conn = sqlite3.connect(temp_db, timeout=30)
            dst_conn.execute("PRAGMA journal_mode=DELETE")
            dst_conn.execute("PRAGMA synchronous=OFF")
            dst_conn.execute("PRAGMA foreign_keys=OFF")

            objects = src_conn.execute(
                """
                SELECT type, name, tbl_name, sql
                FROM sqlite_master
                WHERE sql IS NOT NULL
                ORDER BY CASE type
                    WHEN 'table' THEN 0
                    WHEN 'index' THEN 1
                    WHEN 'trigger' THEN 2
                    WHEN 'view' THEN 3
                    ELSE 4
                END, name
                """
            ).fetchall()

            tables = []
            deferred_objects = []
            for row in objects:
                obj_type = row["type"]
                name = row["name"]
                sql = row["sql"]
                if obj_type == "table":
                    if str(name).startswith("sqlite_"):
                        continue
                    dst_conn.execute(sql)
                    tables.append(str(name))
                else:
                    deferred_objects.append((obj_type, str(name), sql))
            dst_conn.commit()

            for table_name in tables:
                try:
                    columns = [
                        row[1]
                        for row in src_conn.execute(
                            f"PRAGMA table_info({db_backend.quote_ident(table_name)})"
                        ).fetchall()
                    ]
                    copied, skipped = self._copy_salvage_table_rows(src_conn, dst_conn, table_name, columns)
                    table_stats.append((table_name, copied, skipped))
                    if skipped:
                        logger.warning(f"[DB恢复] {src_path.name}.{table_name} 抢救跳过疑似坏行/空洞 {skipped} 个")
                    dst_conn.commit()
                except Exception as e:
                    failed_objects.append(f"table {table_name}: {e}")
                    logger.warning(f"[DB恢复] {src_path.name}.{table_name} 抢救失败: {e}")

            for obj_type, name, sql in deferred_objects:
                try:
                    dst_conn.execute(sql)
                except Exception as e:
                    failed_objects.append(f"{obj_type} {name}: {e}")
                    logger.warning(f"[DB恢复] 重建 {obj_type} {name} 失败，已跳过: {e}")
            dst_conn.commit()

            ok, msg = self._validate_sqlite_file(temp_db)
            if not ok:
                return False, f"Python 抢救后校验失败: {msg}"

            if dst_conn is not None:
                dst_conn.close()
                dst_conn = None
            dst_path.parent.mkdir(parents=True, exist_ok=True)
            os.replace(temp_db, dst_path)

            skipped_summary = {
                table: skipped
                for table, _copied, skipped in table_stats
                if skipped
            }
            if skipped_summary:
                logger.warning(f"[DB恢复] {src_path.name} 抢救完成但有不可读行/空洞: {skipped_summary}")
            if failed_objects:
                logger.warning(f"[DB恢复] {src_path.name} 抢救时跳过对象: {'; '.join(failed_objects)}")
            return True, ""
        except Exception as e:
            return False, f"Python 抢救失败: {e}"
        finally:
            if dst_conn is not None:
                dst_conn.close()
            if src_conn is not None:
                src_conn.close()
            shutil.rmtree(temp_dir, ignore_errors=True)

    def _recover_sqlite_db(self, src_path: Path, dst_path: Path):
        """使用 sqlite3 CLI 的 .recover 尽量抢救损坏库。"""
        sqlite_bin = shutil.which("sqlite3")
        if not sqlite_bin:
            logger.warning("[DB恢复] 未找到 sqlite3 命令，改用 Python 逐表抢救")
            return self._python_salvage_sqlite_db(src_path, dst_path)

        temp_dir = Path(tempfile.mkdtemp())
        try:
            sql_path = temp_dir / "recover.sql"
            dst_path.parent.mkdir(parents=True, exist_ok=True)
            dst_path.unlink(missing_ok=True)

            recover_errors = []
            for recover_cmd in (".recover --ignore-freelist", ".recover"):
                with open(sql_path, "w", encoding="utf-8") as sql_file:
                    result = subprocess.run(
                        [sqlite_bin, str(src_path), recover_cmd],
                        stdout=sql_file,
                        stderr=subprocess.PIPE,
                        text=True,
                        timeout=300,
                    )
                if sql_path.exists() and sql_path.stat().st_size > 0:
                    break
                recover_errors.append(result.stderr.strip() or f"{recover_cmd} 无输出")
            else:
                msg = "; ".join(recover_errors) or ".recover 未产生可导入 SQL"
                logger.warning(f"[DB恢复] {msg}，改用 Python 逐表抢救")
                return self._python_salvage_sqlite_db(src_path, dst_path)

            with open(sql_path, "r", encoding="utf-8") as sql_file:
                load_result = subprocess.run(
                    [sqlite_bin, str(dst_path)],
                    stdin=sql_file,
                    stderr=subprocess.PIPE,
                    text=True,
                    timeout=300,
            )
            if load_result.returncode != 0:
                msg = load_result.stderr.strip() or ".recover SQL 导入失败"
                logger.warning(f"[DB恢复] {msg}，改用 Python 逐表抢救")
                return self._python_salvage_sqlite_db(src_path, dst_path)

            ok, msg = self._validate_sqlite_file(dst_path)
            if not ok:
                logger.warning(f"[DB恢复] .recover 后校验失败，改用 Python 逐表抢救: {msg}")
                return self._python_salvage_sqlite_db(src_path, dst_path)
            return True, ""
        except subprocess.TimeoutExpired:
            logger.warning("[DB恢复] .recover 执行超时，改用 Python 逐表抢救")
            return self._python_salvage_sqlite_db(src_path, dst_path)
        except Exception as e:
            logger.warning(f"[DB恢复] .recover 执行失败，改用 Python 逐表抢救: {e}")
            return self._python_salvage_sqlite_db(src_path, dst_path)
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def _snapshot_sqlite_db(self, src_path: Path, dst_path: Path):
        """使用 SQLite backup API 生成一致性快照，兼容 WAL 写入中的数据库。"""
        src_conn = None
        dst_conn = None
        try:
            dst_path.parent.mkdir(parents=True, exist_ok=True)
            src_conn = sqlite3.connect(src_path, timeout=30)
            dst_conn = sqlite3.connect(dst_path, timeout=30)
            src_conn.backup(dst_conn)
            dst_conn.commit()
        except sqlite3.DatabaseError as e:
            logger.warning(f"[DB恢复] {src_path.name} 快照失败，尝试 .recover 抢救: {e}")
            return self._recover_sqlite_db(src_path, dst_path)
        finally:
            if dst_conn is not None:
                dst_conn.close()
            if src_conn is not None:
                src_conn.close()

        ok, msg = self._validate_sqlite_file(dst_path)
        if not ok:
            logger.warning(f"[DB恢复] {src_path.name} 快照校验失败，尝试 .recover 抢救: {msg}")
            return self._recover_sqlite_db(src_path, dst_path)
        return True, ""

    def _close_database_handles(self, db_names: list[str]):
        try:
            from .xiuxian2_handle import (
                XIUXIAN_IMPART_BUFF,
                PlayerDataManager,
                TradeDataManager,
                XiuxianDateManage,
            )

            manager_classes = {
                "xiuxian.db": XiuxianDateManage,
                "player.db": PlayerDataManager,
                "trade.db": TradeDataManager,
                "xiuxian_impart.db": XIUXIAN_IMPART_BUFF,
            }
            for db_name in db_names:
                try:
                    manager_cls = manager_classes.get(db_name)
                    manager = manager_cls() if manager_cls else None
                    if manager and hasattr(manager, "close"):
                        manager.close()
                except Exception as e:
                    logger.warning(f"[DB恢复] 关闭 {db_name} 旧连接失败: {e}")
        except Exception as e:
            logger.warning(f"[DB恢复] 加载数据库管理器失败: {e}")

    def _restore_sqlite_file(self, src_path: Path, dst_path: Path, db_name: str):
        temp_dir = Path(tempfile.mkdtemp())
        try:
            clean_path = temp_dir / db_name
            ok, msg = self._snapshot_sqlite_db(src_path, clean_path)
            if not ok:
                raise RuntimeError(msg)

            dst_path.parent.mkdir(parents=True, exist_ok=True)
            self._backup_current_db_before_restore(dst_path, db_name)
            self._close_database_handles([db_name])
            self._remove_sqlite_sidecars(dst_path)
            os.replace(clean_path, dst_path)
            self._remove_sqlite_sidecars(dst_path)
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def _backup_current_db_before_restore(self, dst_path: Path, db_name: str):
        if not dst_path.exists():
            return
        backup_dir = Path() / "data" / "xiuxian" / "backups" / "db_restore_before"
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = backup_dir / f"{timestamp}_{db_name}"
        ok, msg = self._snapshot_sqlite_db(dst_path, backup_path)
        if ok:
            logger.info(f"[DB恢复] 已备份当前数据库: {backup_path}")
            return
        backup_dir.mkdir(parents=True, exist_ok=True)
        raw_path = backup_dir / f"{timestamp}_{db_name}.raw"
        try:
            shutil.copy2(dst_path, raw_path)
            logger.warning(f"[DB恢复] 当前库快照失败，已保留原始副本 {raw_path}: {msg}")
        except Exception as e:
            logger.warning(f"[DB恢复] 当前库备份失败 {dst_path}: {e}")

    def _reload_restored_database_handles(self, restored_dbs: list[str]):
        try:
            from .xiuxian2_handle import (
                XIUXIAN_IMPART_BUFF,
                PlayerDataManager,
                TradeDataManager,
                XiuxianDateManage,
            )

            manager_classes = {
                "xiuxian.db": XiuxianDateManage,
                "player.db": PlayerDataManager,
                "trade.db": TradeDataManager,
                "xiuxian_impart.db": XIUXIAN_IMPART_BUFF,
            }
            for db_name in restored_dbs:
                try:
                    manager_cls = manager_classes.get(db_name)
                    manager = manager_cls() if manager_cls else None
                    if manager and hasattr(manager, "reconnect"):
                        manager.reconnect()
                except Exception as e:
                    logger.warning(f"[DB恢复] 重建 {db_name} 数据库连接失败: {e}")
        except Exception as e:
            logger.warning(f"[DB恢复] 加载数据库管理器失败: {e}")

    def backup_db_files(self):
        """备份本地 SQLite 数据库到 data/xiuxian/backups/db_backup/"""
        try:
            backup_dir = Path() / "data" / "xiuxian" / "backups" / "db_backup"
            backup_dir.mkdir(parents=True, exist_ok=True)

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            zip_name = f"db_backup_{timestamp}.zip"
            zip_path = backup_dir / zip_name

            added = []
            temp_dir = Path(tempfile.mkdtemp())
            try:
                with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
                    for db_name in self._sqlite_db_names():
                        db_path = Path() / "data" / "xiuxian" / db_name
                        if not db_path.exists():
                            continue
                        snapshot_path = temp_dir / db_name
                        ok, msg = self._snapshot_sqlite_db(db_path, snapshot_path)
                        if not ok:
                            raise RuntimeError(msg)
                        zf.write(snapshot_path, db_name)
                        added.append(db_name)
            finally:
                shutil.rmtree(temp_dir, ignore_errors=True)

            if not added:
                zip_path.unlink(missing_ok=True)
                return False, "未找到可备份的 SQLite 数据库文件"

            try:
                cfg = XiuConfig()
                if getattr(cfg, "cloud_backup_enabled", False):
                    ok, msg = self.upload_backup_to_webdav(zip_path)
                    if ok:
                        logger.info(f"[DB云备份] {msg}")
                        c_ok, c_msg = self.cleanup_webdav_old_backups()
                        if c_ok:
                            logger.info(c_msg)
                        else:
                            logger.warning(c_msg)
                    else:
                        logger.warning(f"[DB云备份] 失败: {msg}")
            except Exception as e:
                logger.warning(f"[DB云备份] 执行异常: {e}")

            self.clean_old_backups(backup_dir, keep_days=10)
            return True, f"数据库备份完成: {zip_name}，已备份: {', '.join(added)}"
        except Exception as e:
            return False, f"数据库备份失败: {e}"

    def clean_old_backups(self, backup_dir, keep_days=10):
        """清理本地旧备份"""
        try:
            backup_dir = Path(backup_dir)
            now = datetime.now()
            for f in backup_dir.glob("*.zip"):
                parts = f.stem.split('_')
                if len(parts) >= 4:
                    try:
                        ts = f"{parts[-2]}_{parts[-1]}"
                        t = datetime.strptime(ts, "%Y%m%d_%H%M%S")
                        if (now - t).days > keep_days:
                            f.unlink()
                            logger.info(f"[DB备份] 清理旧文件: {f.name}")
                    except Exception:
                        continue
            return True, "本地旧备份清理完成"
        except Exception as e:
            return False, f"清理旧备份失败: {e}"

    def get_db_backups(self):
        """获取本地数据库备份列表"""
        backup_dir = Path() / "data" / "xiuxian" / "backups" / "db_backup"
        backups = []
        if backup_dir.exists():
            for f in backup_dir.glob("db_backup_*.zip"):
                backups.append({
                    "filename": f.name,
                    "size": f.stat().st_size,
                    "created_at": datetime.fromtimestamp(f.stat().st_ctime).isoformat(),
                    "path": str(f),
                    "type": "sqlite",
                })
        backups.sort(key=lambda x: x["created_at"], reverse=True)
        return backups

    def _normalize_selected_db_names(self, selected_dbs: list):
        aliases = {
            "xiuxian": "xiuxian.db",
            "xiuxian.db": "xiuxian.db",
            "xiuxian_impart": "xiuxian_impart.db",
            "xiuxian_impart.db": "xiuxian_impart.db",
            "player": "player.db",
            "player.db": "player.db",
            "trade": "trade.db",
            "trade.db": "trade.db",
        }
        normalized = []
        seen = set()
        for db_name in selected_dbs or []:
            safe_name = Path(str(db_name)).name
            normalized_name = aliases.get(safe_name)
            if not normalized_name:
                continue
            if normalized_name not in seen:
                seen.add(normalized_name)
                normalized.append(normalized_name)
        return normalized

    def _db_path_for_name(self, db_name: str):
        safe_name = Path(str(db_name)).name
        return Path() / "data" / "xiuxian" / safe_name

    def restore_db_files(self, backup_filename: str, selected_dbs: list):
        """从本地 db_backup zip 恢复指定数据库"""
        temp_dir = None
        try:
            backup_filename = _safe_leaf_name(backup_filename)
            selected_dbs = self._normalize_selected_db_names(selected_dbs)
            if not selected_dbs:
                return False, "至少选择一个数据库进行恢复"

            backup_path = _path_under(Path() / "data" / "xiuxian" / "backups" / "db_backup", backup_filename)
            if not backup_path.exists():
                return False, f"备份文件不存在: {backup_filename}"
            if not backup_path.is_file():
                return False, f"无效备份文件: {backup_filename}"

            temp_dir = Path(tempfile.mkdtemp())
            staged = {}
            with zipfile.ZipFile(backup_path, 'r') as zf:
                names = set(zf.namelist())
                skipped = []
                failed = []
                for db_name in selected_dbs:
                    member_name = None
                    for candidate in (db_name, f"data/xiuxian/{db_name}"):
                        if candidate in names:
                            member_name = candidate
                            break

                    if member_name:
                        staged_path = temp_dir / db_name
                        with zf.open(member_name) as src, open(staged_path, "wb") as dst:
                            shutil.copyfileobj(src, dst)
                        for suffix in ("-wal", "-shm"):
                            for sidecar_member in (f"{member_name}{suffix}", f"{db_name}{suffix}"):
                                if sidecar_member in names:
                                    sidecar_path = temp_dir / f"{db_name}{suffix}"
                                    with zf.open(sidecar_member) as src, open(sidecar_path, "wb") as dst:
                                        shutil.copyfileobj(src, dst)
                                    break
                        clean_path = temp_dir / f"{db_name}.clean"
                        ok, check_msg = self._snapshot_sqlite_db(staged_path, clean_path)
                        if ok:
                            staged[db_name] = clean_path
                        else:
                            failed.append(f"{db_name}: {check_msg}")
                    else:
                        skipped.append(db_name)

            if failed:
                return False, f"数据库恢复失败，备份库校验未通过: {'; '.join(failed)}"

            restored = []
            for db_name, staged_path in staged.items():
                dst = self._db_path_for_name(db_name)
                self._restore_sqlite_file(staged_path, dst, db_name)
                restored.append(db_name)

            if restored:
                self._reload_restored_database_handles(restored)
                self._compact_pet_storage_after_database_restore()

            msg = f"恢复完成，已恢复: {restored}"
            if skipped:
                msg += f"，备份中不存在: {skipped}"
            return True, msg
        except Exception as e:
            return False, f"数据库恢复失败: {e}"
        finally:
            if temp_dir is not None:
                shutil.rmtree(temp_dir, ignore_errors=True)

    def list_webdav_db_backups(self):
        """列出云端 db_backup/*.zip"""
        try:
            ok, msg, paths = self._get_webdav_paths()
            if not ok:
                return False, "未配置 WebDAV 信息"

            base_url = paths["base_url"]
            auth = paths["auth"]
            root_url = paths["db_url"]

            headers = {"Depth": "1"}
            r = requests.request("PROPFIND", root_url, auth=auth, timeout=20, headers=headers)
            if r.status_code not in (207, 200):
                return False, f"读取云端目录失败 HTTP {r.status_code}"

            import xml.etree.ElementTree as ET
            ns = {"d": "DAV:"}
            root = ET.fromstring(r.text)

            files = []
            for resp in root.findall("d:response", ns):
                href_el = resp.find("d:href", ns)
                if href_el is None or not href_el.text:
                    continue
                name = href_el.text.rstrip('/').split('/')[-1]
                if not name or not name.endswith(".zip"):
                    continue
                if not name.startswith("db_backup_"):
                    continue

                rt = resp.find(".//d:resourcetype", ns)
                is_collection = (rt is not None and rt.find("d:collection", ns) is not None)
                if is_collection:
                    continue

                size_el = resp.find(".//d:getcontentlength", ns)
                time_el = resp.find(".//d:getlastmodified", ns)
                raw_modified = time_el.text if time_el is not None else ""

                files.append({
                    "filename": name,
                    "size": int(size_el.text) if size_el is not None and str(size_el.text).isdigit() else 0,
                    "modified": self._gmt_to_cst_str(raw_modified)
                })

            files.sort(key=lambda x: x["modified"], reverse=True)
            return True, files
        except Exception as e:
            return False, f"读取云端数据库备份失败: {e}"

    def download_db_backup_from_webdav(self, filename, overwrite=False):
        """从云端 db_backup 下载到本地"""
        try:
            filename = _safe_leaf_name(filename)
            ok, msg, paths = self._get_webdav_paths()
            if not ok:
                return False, "未配置 WebDAV 信息"

            local_dir = Path() / "data" / "xiuxian" / "backups" / "db_backup"
            local_dir.mkdir(parents=True, exist_ok=True)
            local_path = _path_under(local_dir, filename)

            if local_path.exists() and not overwrite:
                return False, "FILE_EXISTS"

            auth = paths["auth"]
            remote_url = self._webdav_join_url(paths["base_url"], f"{paths['db_rel']}/{filename}")

            r = requests.get(remote_url, auth=auth, timeout=120, stream=True)
            if r.status_code != 200:
                return False, f"下载失败 HTTP {r.status_code}"

            tmp_path = local_path.with_name(f"{local_path.name}.tmp")
            try:
                with open(tmp_path, "wb") as f:
                    for chunk in r.iter_content(16384):
                        if chunk:
                            f.write(chunk)
                if not zipfile.is_zipfile(tmp_path):
                    return False, "下载完成但文件不是有效 zip"
                os.replace(tmp_path, local_path)
            finally:
                if tmp_path.exists():
                    tmp_path.unlink(missing_ok=True)

            return True, local_path
        except Exception as e:
            return False, f"下载数据库备份失败: {e}"

    def cloud_restore_db_files(self, filename: str, selected_dbs: list):
        """云端数据库恢复：本地无则先下，再恢复"""
        try:
            filename = _safe_leaf_name(filename)
            local_path = _path_under(Path() / "data" / "xiuxian" / "backups" / "db_backup", filename)
            ok, msg = self.download_db_backup_from_webdav(filename, overwrite=True)
            if not ok:
                if not local_path.exists():
                    return False, msg
                logger.warning(f"[DB云恢复] 重新下载失败，尝试使用本地已有备份 {filename}: {msg}")
            return self.restore_db_files(filename, selected_dbs)
        except Exception as e:
            return False, f"云端数据库恢复失败: {e}"

    # =========================
    # 云端删除
    # =========================
    def delete_webdav_backup(self, filename: str):
        """删除云端插件备份"""
        try:
            filename = _safe_leaf_name(filename)
            ok, msg, paths = self._get_webdav_paths()
            if not ok:
                return False, "未配置 WebDAV 信息"

            auth = paths["auth"]
            remote_url = self._webdav_join_url(paths["base_url"], f"{paths['plugin_rel']}/{filename}")

            r = requests.delete(remote_url, auth=auth, timeout=20)
            if r.status_code in (200, 202, 204):
                return True, f"已删除云端文件: {filename}"
            return False, f"删除失败 HTTP {r.status_code}"
        except Exception as e:
            return False, f"删除云端文件失败: {e}"

    def delete_webdav_db_backup(self, filename: str):
        """删除云端数据库备份"""
        try:
            filename = _safe_leaf_name(filename)
            ok, msg, paths = self._get_webdav_paths()
            if not ok:
                return False, "未配置 WebDAV 信息"

            auth = paths["auth"]
            remote_url = self._webdav_join_url(paths["base_url"], f"{paths['db_rel']}/{filename}")

            r = requests.delete(remote_url, auth=auth, timeout=20)
            if r.status_code in (200, 202, 204):
                return True, f"已删除云端数据库备份: {filename}"
            return False, f"删除失败 HTTP {r.status_code}"
        except Exception as e:
            return False, f"删除云端数据库备份失败: {e}"
