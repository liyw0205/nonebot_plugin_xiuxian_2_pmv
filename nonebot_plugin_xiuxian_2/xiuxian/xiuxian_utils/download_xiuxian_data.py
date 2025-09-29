import os
import zipfile
import tarfile
import wget
import json
import requests
import tempfile
import shutil
from datetime import datetime
from pathlib import Path
from nonebot.log import logger
from ..xiuxian_config import XiuConfig

def download_xiuxian_data():
    path_data = Path() / "data"
    zipPath = str(path_data / "xiuxian_data_temp.zip")  # 压缩包的绝对路径
    version = "xiuxian_version.txt"
    URL = "https://huggingface.co/xiaonana/xiuxian/resolve/main/xiuxian.zip"

    def get_data():
        wget.download(URL, out=zipPath)  # 获取内容

    def _main_():
        if not os.path.exists(path_data):
            os.makedirs(path_data)
        version_path = path_data / "xiuxian" / version
        data = None
        try:
            with open(version_path, 'r', encoding='utf-8') as f:
                data = f.read()
                f.close()
        except:
            pass
        if str(data) == str(XiuConfig().version):
            logger.opt(colors=True).info(f"<green>修仙配置校核完成！</green>")
        else:
            logger.opt(colors=True).info(f"<green>正在更新修仙配置文件，请等待！</green>")
            try:
                get_data()  # data为byte字节
                logger.opt(colors=True).info(f"<green>正在解压修仙配置文件！</green>")
                with zipfile.ZipFile(file=zipPath, mode='r') as zf:
                    for old_name in zf.namelist():
                        # 获取文件大小，目的是区分文件夹还是文件，如果是空文件应该不好用。
                        file_size = zf.getinfo(old_name).file_size
                        new_name = old_name.encode('cp437').decode('gbk')
                        new_path = os.path.join(path_data, new_name)
                        if file_size > 0:
                            with open(file=new_path, mode='wb') as f:
                                f.write(zf.read(old_name))
                                f.close()
                        else:
                            if not os.path.exists(new_path):
                                os.makedirs(new_path)
                zf.close()
            except Exception as e:
                logger.opt(colors=True).info(f"<red>修仙配置文件下载失败，原因{e}，一直失败请前往网址手动下载{URL}</red>")
            finally:
                try:
                    os.remove(zipPath)
                    logger.opt(colors=True).info(f"<red>原始压缩包已删除！</red>")
                except:
                    logger.opt(colors=True).info(f"<red>原始压缩包删除失败，请手动删除，路径{zipPath}!</red>")
    return _main_()

class UpdateManager:
    def __init__(self):
        self.repo_owner = "liyw0205"
        self.repo_name = "nonebot_plugin_xiuxian_2_pmv"
        self.api_url = f"https://api.github.com/repos/{self.repo_owner}/{self.repo_name}/releases"
        self.current_version = self.get_current_version()
        
    def get_current_version(self):
        """获取当前版本"""
        version_file = Path() / "data" / "xiuxian" / "version.txt"
        if version_file.exists():
            try:
                with open(version_file, 'r', encoding='utf-8') as f:
                    return f.read().strip()
            except:
                pass
        return "unknown"
    
    def get_latest_releases(self, count=5):
        """获取最近的release信息"""
        try:
            response = requests.get(self.api_url, timeout=10)
            response.raise_for_status()
            releases = response.json()
            
            # 获取前count个release
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
        """下载指定的release资源"""
        try:
            # 获取特定release的assets
            release_url = f"{self.api_url}/tags/{release_tag}"
            response = requests.get(release_url, timeout=10)
            response.raise_for_status()
            release_data = response.json()
            
            # 查找指定的asset
            target_asset = None
            for asset in release_data.get('assets', []):
                if asset.get('name') == asset_name:
                    target_asset = asset
                    break
            
            if not target_asset:
                return False, f"未找到 {asset_name} 资源文件"
            
            # 创建临时目录
            temp_dir = Path(tempfile.mkdtemp())
            download_path = temp_dir / asset_name
            
            # 使用wget下载文件
            logger.info(f"开始下载 {asset_name}...")
            try:
                wget.download(target_asset['browser_download_url'], out=str(download_path))
                logger.info(f"\n下载完成: {download_path}")
                return True, download_path
            except Exception as e:
                return False, f"wget下载失败: {str(e)}"
            
        except Exception as e:
            return False, f"下载失败: {str(e)}"
    
    def _merge_directories(self, source_dir, target_dir):
        """合并两个目录，保留目标目录中的现有文件，用源目录中的文件覆盖同名文件"""
        if not target_dir.exists():
            target_dir.mkdir(parents=True, exist_ok=True)
        
        for item in source_dir.iterdir():
            target_item = target_dir / item.name
            if item.is_dir():
                # 递归处理子目录
                self._merge_directories(item, target_item)
            else:
                # 确保目标目录存在
                target_item.parent.mkdir(parents=True, exist_ok=True)
                # 覆盖文件
                if target_item.exists():
                    os.remove(target_item)
                shutil.copy2(item, target_item)
    
    def extract_update(self, archive_path, backup=True):
        """解压更新文件"""
        extract_temp = None
        try:
            # 备份当前文件
            if backup:
                self.backup_current_version()
            
            # 创建临时解压目录
            extract_temp = Path(tempfile.mkdtemp())
            
            # 解压.tar.gz文件
            logger.info(f"开始解压文件: {archive_path}")
            with tarfile.open(archive_path, 'r:gz') as tar:
                tar.extractall(extract_temp)
            
            # 目标目录
            target_data_dir = Path() / "data"
            target_plugin_dir = Path() / "src" / "plugins" / "nonebot_plugin_xiuxian_2"
            
            # 确保目标目录存在
            target_data_dir.mkdir(parents=True, exist_ok=True)
            target_plugin_dir.parent.mkdir(parents=True, exist_ok=True)
            
            # 更新版本信息
            self.update_version_file()
            
            # 直接使用解压后的目录进行覆盖更新
            logger.info("开始覆盖更新文件...")
            
            # 覆盖更新data目录（从./data）
            data_source = extract_temp / "." / "data"
            if data_source.exists():
                logger.info(f"合并更新data目录: {data_source} -> {target_data_dir}")
                self._merge_directories(data_source, target_data_dir)
            else:
                return False, "压缩包中缺少data目录"
            
            # 覆盖更新插件目录（从./nonebot_plugin_xiuxian_2）
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
            # 清理临时文件
            try:
                if extract_temp and extract_temp.exists():
                    shutil.rmtree(extract_temp)
            except:
                pass
    
    def backup_current_version(self):
        """备份当前版本"""
        try:
            backup_dir = Path() / "data" / "backups"
            backup_dir.mkdir(parents=True, exist_ok=True)
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_path = backup_dir / f"backup_{timestamp}_{self.current_version}.zip"
            
            with zipfile.ZipFile(backup_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                # 备份data目录
                data_dir = Path() / "data"
                if data_dir.exists():
                    for root, dirs, files in os.walk(data_dir):
                        for file in files:
                            if "backups" not in root:  # 不备份备份文件本身
                                file_path = Path(root) / file
                                arcname = file_path.relative_to(data_dir.parent)
                                zipf.write(file_path, arcname)
                
                # 备份插件目录
                plugin_dir = Path() / "src" / "plugins" / "nonebot_plugin_xiuxian_2"
                if plugin_dir.exists():
                    for root, dirs, files in os.walk(plugin_dir):
                        for file in files:
                            file_path = Path(root) / file
                            arcname = file_path.relative_to(plugin_dir.parent.parent.parent)
                            zipf.write(file_path, arcname)
            
            logger.info(f"备份完成: {backup_path}")
            return True, backup_path
        except Exception as e:
            logger.error(f"备份失败: {e}")
            return False, str(e)
    
    def update_version_file(self):
        """更新版本文件"""
        try:
            # 从GitHub获取最新版本号
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
    
    def perform_update(self, release_tag):
        """执行完整的更新流程"""
        try:
            logger.info(f"开始更新到版本: {release_tag}")
            
            # 1. 下载release
            logger.info("下载更新包...")
            success, result = self.download_release(release_tag)
            if not success:
                return False, result
            
            archive_path = result
            logger.info(f"下载完成: {archive_path}")
            
            # 2. 解压更新
            logger.info("解压更新包...")
            success, result = self.extract_update(archive_path)
            
            # 3. 清理临时文件
            try:
                if archive_path.exists():
                    # 删除临时目录及其内容
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