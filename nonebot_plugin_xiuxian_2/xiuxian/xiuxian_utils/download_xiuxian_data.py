import os
import re
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
from .utils import get_plugin_dir

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
            
            # 获取代理列表并测试延迟
            proxy_list = self.get_proxy_list()
            working_proxies = self.test_proxies(proxy_list)
            
            # 如果找到可用的代理，使用延迟最低的3个代理进行下载尝试
            success = False
            error_messages = []
            
            if working_proxies:
                logger.info(f"找到 {len(working_proxies)} 个可用代理，尝试使用代理下载...")
                for proxy in working_proxies[:3]:  # 只尝试延迟最低的3个代理
                    try:
                        success, message = self.download_with_proxy(target_asset['browser_download_url'], 
                                                                   str(download_path), proxy)
                        if success:
                            logger.info(f"使用代理 {proxy['url']} 下载成功")
                            return True, download_path
                        else:
                            error_messages.append(f"代理 {proxy['url']} 下载失败: {message}")
                    except Exception as e:
                        error_messages.append(f"代理 {proxy['url']} 下载错误: {str(e)}")
            
            # 如果所有代理都失败或没有可用代理，使用直接下载
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

    def get_proxy_list(self):
        """代理列表"""
        proxies = [
            {"url": "https://gh.llkk.cc/", "name": "gh.llkk.cc"},
            {"url": "https://j.1lin.dpdns.org/", "name": "j.1lin.dpdns.org"},
            {"url": "https://ghproxy.net/", "name": "ghproxy.net"},
            {"url": "https://gh-proxy.net/", "name": "gh-proxy.net"},
            {"url": "https://j.1win.ggff.net/", "name": "j.1win.ggff.net"},
            {"url": "https://tvv.tw/", "name": "tvv.tw"},
            {"url": "https://ghf.xn--eqrr82bzpe.top/", "name": "ghf.xn--eqrr82bzpe.top"},
            {"url": "https://ghproxy.vansour.top/", "name": "ghproxy.vansour.top"},
            {"url": "https://gh.catmak.name/", "name": "gh.catmak.name"},
            {"url": "https://gitproxy.127731.xyz/", "name": "gitproxy.127731.xyz"},
            {"url": "https://gitproxy.click/", "name": "gitproxy.click"},
            {"url": "https://jiashu.1win.eu.org/", "name": "jiashu.1win.eu.org"},
            {"url": "https://github.dpik.top/", "name": "github.dpik.top"},
            {"url": "https://github.tbedu.top/", "name": "github.tbedu.top"},
            {"url": "https://ghm.078465.xyz/", "name": "ghm.078465.xyz"},
            {"url": "https://hub.gitmirror.com/", "name": "hub.gitmirror.com"},
            {"url": "https://ghfile.geekertao.top/", "name": "ghfile.geekertao.top"},
            {"url": "https://gh.dpik.top/", "name": "gh.dpik.top"},
            {"url": "https://git.yylx.win/", "name": "git.yylx.win"}
        ]
        return proxies

    def test_proxies(self, proxy_list):
        """测试代理的延迟，返回可用的代理列表（按延迟排序）"""
        import time
        import threading
        from concurrent.futures import ThreadPoolExecutor, as_completed
        
        def test_proxy(proxy):
            """测试单个代理的延迟"""
            try:
                start_time = time.time()
                # 简单的连接测试，使用代理的根路径
                test_url = f"{proxy['url']}https://github.com/robots.txt"
                response = requests.get(test_url, timeout=5)
                if response.status_code == 200:
                    delay = int((time.time() - start_time) * 1000)  # 转换为毫秒
                    proxy['delay'] = delay
                    return proxy
            except Exception as e:
                logger.debug(f"代理 {proxy['url']} 测试失败: {str(e)}")
            return None
        
        working_proxies = []
        
        # 使用线程池并发测试代理
        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_proxy = {executor.submit(test_proxy, proxy): proxy for proxy in proxy_list}
            
            for future in as_completed(future_to_proxy):
                proxy = future_to_proxy[future]
                try:
                    result = future.result()
                    if result:
                        working_proxies.append(result)
                except Exception as e:
                    logger.debug(f"代理测试异常: {str(e)}")
        
        # 按延迟排序
        working_proxies.sort(key=lambda x: x.get('delay', 9999))
        
        logger.info(f"找到 {len(working_proxies)} 个可用代理，延迟最低的3个: {[(p['name'], p.get('delay', '未知')) for p in working_proxies[:3]]}")
        
        return working_proxies

    def download_with_proxy(self, original_url, download_path, proxy):
        """使用代理下载文件"""
        try:
            # 构建代理下载URL
            proxy_url = f"{proxy['url']}{original_url}"
            
            logger.info(f"尝试使用代理 {proxy['name']} 下载: {proxy_url}")
            
            # 使用requests下载，支持进度显示
            response = requests.get(proxy_url, stream=True, timeout=30)
            response.raise_for_status()
            
            total_size = int(response.headers.get('content-length', 0))
            downloaded_size = 0
            
            with open(download_path, 'wb') as file:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        file.write(chunk)
                        downloaded_size += len(chunk)
                        
                        # 显示下载进度
                        if total_size > 0:
                            percent = (downloaded_size / total_size) * 100
                            print(f"\r下载进度: {percent:.1f}% ({downloaded_size}/{total_size} bytes)", end='')
            
            print()  # 换行
            return True, "下载成功"
            
        except requests.exceptions.Timeout:
            return False, "下载超时"
        except requests.exceptions.ConnectionError:
            return False, "连接错误"
        except Exception as e:
            return False, f"下载错误: {str(e)}"

    def _merge_directories(self, source_dir, target_dir):
        """安全的目录合并 - 只覆盖同名文件，不删除额外文件"""
        if not target_dir.exists():
            target_dir.mkdir(parents=True, exist_ok=True)
        
        for item in source_dir.iterdir():
            target_item = target_dir / item.name
            if item.is_dir():
                # 递归处理子目录，但确保目标目录存在
                self._merge_directories(item, target_item)
            else:
                # 确保目标目录存在
                target_item.parent.mkdir(parents=True, exist_ok=True)
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
            target_plugin_dir = get_plugin_dir()
            
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

    def enhanced_backup_current_version(self):
        """备份当前版本"""
        try:
            backup_dir = Path() / "data" / "backups"
            backup_dir.mkdir(parents=True, exist_ok=True)
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_path = backup_dir / f"backup_{timestamp}_{self.current_version}.zip"
            
            with zipfile.ZipFile(backup_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                # 备份data目录（排除backups目录本身）
                data_dir = Path() / "data"
                if data_dir.exists():
                    for root, dirs, files in os.walk(data_dir):
                        if any(x in root.split(os.sep) for x in ["backups", "config_backups", "db_backup", "boss_img", "font", "卡图"]):
                            continue
                        
                        for file in files:
                            file_path = Path(root) / file
                            try:
                                arcname = file_path.relative_to(data_dir.parent)
                                zipf.write(file_path, arcname)
                            except Exception as e:
                                logger.warning(f"备份文件跳过: {file_path}, 错误: {e}")
                
                # 备份插件目录
                plugin_dir = Path() / "src" / "plugins" / "nonebot_plugin_xiuxian_2"
                if plugin_dir.exists():
                    for root, dirs, files in os.walk(plugin_dir):
                        for file in files:
                            file_path = Path(root) / file
                            try:
                                arcname = file_path.relative_to(plugin_dir.parent.parent.parent)
                                zipf.write(file_path, arcname)
                            except Exception as e:
                                logger.warning(f"备份文件跳过: {file_path}, 错误: {e}")
            
            logger.info(f"备份完成: {backup_path}")
            return True, backup_path
        except Exception as e:
            logger.error(f"备份失败: {e}")
            return False, str(e)

    def backup_all_configs(self):
        """备份所有配置（全选）"""
        try:
            
            # 获取当前配置值
            config = XiuConfig()
            config_values = {}
            from ..xiuxian_web import CONFIG_EDITABLE_FIELDS
            
            # 获取所有可编辑配置字段
            for field_name in CONFIG_EDITABLE_FIELDS.keys():
                if hasattr(config, field_name):
                    value = getattr(config, field_name)
                    config_values[field_name] = value
            
            # 创建备份目录
            backup_dir = Path() / "data" / "config_backups"
            backup_dir.mkdir(parents=True, exist_ok=True)
            
            # 生成备份文件名
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_filename = f"config_backup_{timestamp}.json"
            backup_path = backup_dir / backup_filename
            
            # 添加元数据
            config_values['_metadata'] = {
                'backup_time': datetime.now().isoformat(),
                'backup_fields': list(config_values.keys()),
                'version': self.current_version,
                'type': 'config_backup',
                'backup_type': 'full'  # 标记为全选备份
            }
            
            # 保存备份文件
            with open(backup_path, 'w', encoding='utf-8') as f:
                json.dump(config_values, f, ensure_ascii=False, indent=2)
            
            logger.info(f"全选配置备份完成: {backup_filename}")
            return True, backup_path
            
        except Exception as e:
            logger.error(f"配置备份失败: {str(e)}")
            return False, f"配置备份失败: {str(e)}"

    def perform_update_with_backup(self, release_tag):
        """执行完整的更新流程（带自动备份和恢复配置）"""
        try:
            logger.info(f"开始更新到版本: {release_tag}")
            
            # 自动创建插件备份
            logger.info("创建自动插件备份...")
            self.enhanced_backup_current_version()

            # 自动创建配置备份
            logger.info("创建自动配置备份...")
            backup_success, backup_result = self.backup_all_configs()
            if not backup_success:
                return False, f"配置备份失败: {backup_result}"
            
            config_backup_path = backup_result

            # 下载release
            logger.info("下载更新包...")
            success, result = self.download_release(release_tag)
            if not success:
                return False, result
            
            archive_path = result
            logger.info(f"下载完成: {archive_path}")
            
            # 解压更新
            logger.info("解压更新包...")
            success, result = self.extract_update(archive_path, backup=False)
            
            # 恢复配置
            if success and backup_success:
                logger.info("开始恢复配置...")
                restore_success, restore_message = self.restore_config_from_backup(config_backup_path)
                if not restore_success:
                    logger.warning(f"配置恢复失败: {restore_message}")
                else:
                    logger.info("配置恢复成功")
            
            # 清理临时文件
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
        """从配置备份文件恢复配置"""
        try:
            if not backup_path.exists():
                return False, "备份文件不存在"
            
            # 读取备份文件
            with open(backup_path, 'r', encoding='utf-8') as f:
                backup_data = json.load(f)
            
            # 移除元数据字段
            if '_metadata' in backup_data:
                del backup_data['_metadata']
            
            # 保存配置到文件
            success, message = self.save_config_values(backup_data)
            if not success:
                return False, f"保存配置失败: {message}"
            
            return True, "配置恢复成功"
            
        except Exception as e:
            return False, f"恢复配置失败: {str(e)}"

    def save_config_values(self, new_values):
        """保存配置到文件"""
        config_file_path = get_plugin_dir() / "xiuxian" / "xiuxian_config.py"
        from ..xiuxian_web import CONFIG_EDITABLE_FIELDS
        
        if not config_file_path.exists():
            return False, "配置文件不存在"
        
        try:
            # 读取原文件内容
            with open(config_file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # 更新配置值
            for field_name, new_value in new_values.items():
                if field_name in CONFIG_EDITABLE_FIELDS:
                    field_type = CONFIG_EDITABLE_FIELDS[field_name]["type"]
                    
                    # 根据类型格式化值
                    if field_type == "list[int]":
                        # 处理整数列表
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
                        # 处理字符串列表
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
                        # 字符串类型
                        if not (isinstance(new_value, str) and (
                            (new_value.startswith('"') and new_value.endswith('"')) or 
                            (new_value.startswith("'") and new_value.endswith("'"))
                        )):
                            formatted_value = f'"{new_value}"'
                        else:
                            formatted_value = new_value
                    
                    # 在文件中查找并替换配置项
                    pattern = rf"self\.{field_name}\s*=\s*.+"
                    replacement = f"self.{field_name} = {formatted_value}"
                    content = re.sub(pattern, replacement, content)
            
            # 写入新内容
            with open(config_file_path, 'w', encoding='utf-8') as f:
                f.write(content)
            
            return True, "配置保存成功"
        
        except Exception as e:
            return False, f"保存配置时出错: {str(e)}"

    def _restore_files_from_backup(self, backup_root):
        """从备份根目录恢复文件"""
        # 恢复data目录
        data_backup_path = backup_root / "data"
        if data_backup_path.exists():
            target_data_dir = Path() / "data"
            self._merge_directories(data_backup_path, target_data_dir)
        
        # 恢复插件目录
        plugin_backup_path = backup_root / "src" / "plugins" / "nonebot_plugin_xiuxian_2"
        if plugin_backup_path.exists():
            target_plugin_dir = Path() / "src" / "plugins" / "nonebot_plugin_xiuxian_2"
            self._merge_directories(plugin_backup_path, target_plugin_dir)

    def get_backups(self):
        """获取所有备份文件"""
        backup_dir = Path() / "data" / "backups"
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
        """从备份恢复"""
        try:
            backup_dir = Path() / "data" / "backups"
            backup_path = backup_dir / backup_filename
            
            if not backup_path.exists():
                return False, f"备份文件不存在: {backup_filename}"
            
            logger.info(f"开始从备份恢复: {backup_filename}")
            
            # 创建临时目录用于解压
            temp_dir = Path(tempfile.mkdtemp())
            
            # 解压备份文件
            with zipfile.ZipFile(backup_path, 'r') as zipf:
                zipf.extractall(temp_dir)
            
            # 恢复文件
            self._restore_files_from_backup(temp_dir)
            
            # 清理临时文件
            shutil.rmtree(temp_dir)
            
            # 更新版本信息
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
