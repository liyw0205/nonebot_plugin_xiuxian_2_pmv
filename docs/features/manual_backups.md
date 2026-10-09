# 手动整包备份

## 用户流程

管理员点一次“手动备份”，系统先做插件整包快照，再做配置快照；任一子任务产生了云端上传，就触发一次统一的云端清理，最后把两个结果和合并后的错误信息一次性返回。已有手动备份在执行时，后来的请求不会排队，而是直接得到“未执行”的结果。

## 命令与别名

无命令，`commands.py` 声明 `COMMANDS = ()`。手动备份只由 Web 控制台触发。

## Web API

`ROUTES` 为空；`POST /manual_backup` 目前仍由旧模块 `xiuxian/xiuxian_web/backups.py`（管理员会话 + CSRF）注册，转发到 `ManualBackupApplication.create_backup`，返回体沿用旧字段。

## 数据模型与迁移

无表、无迁移，`migrations.py` 声明 `MIGRATIONS = ()`。本切片不写任何文件：插件快照由 `plugin_backups` 创建，配置快照由 `config_backups` 创建。`schemas.py` 定义唯一结果契约 `ManualBackupResult(success, plugin_backup, config_backup, error)`；`repository.py` 用 `PluginBackupCreationPort` 与 `ConfigBackupProvider` 两个协议描述依赖边界，应用不再直接导入兄弟切片的具体类。

## 事务与失败回滚

`_backup_lock` 是进程内非阻塞单飞锁，抢锁失败立即返回“未执行/已有手动备份任务正在执行”，不会与正在进行的备份交叉写目录。两个子任务都以 `defer_cloud_cleanup=True` 执行，把云端清理推迟到两步都结束之后，只做一次；任一子任务失败都会记录到 `error` 并把 `success` 置假，但不会回滚已经成功的另一份子快照（备份是追加产物，不做删除式补偿）。

## 定时任务

无任务，`jobs.py` 声明 `JOBS = ()`。保留期清理由被调用切片在各自的备份动作内完成。

## 配置项

本切片不拥有配置键，也不读环境变量；保留天数与云备份开关由被调用的 `plugin_backups`、`config_backups` 切片各自经端口获取。

## 适配器差异

应用只依赖两个端口协议，不导入 NoneBot、Flask、`sqlite3` 或具体仓储实现，测试用假对象即可覆盖成功、单飞锁竞争、部分失败和云端清理触发条件。旧 Web 层负责鉴权与响应封装。

## 测试与手工验收

注意本目录混用两种写法：函数式（pytest）用例不会被 `unittest discover` 收集，必须用 `python -m pytest -p no:cacheprovider <目录>` 运行；`unittest discover` 只收集到 unittest 写法的契约用例。函数式用例是 `test_application.py`，unittest 写法的只有 `test_slice_contract.py`。

`python -m pytest -p no:cacheprovider nonebot_plugin_xiuxian_2/features/manual_backups/tests` 覆盖调用顺序、`defer_cloud_cleanup` 传递、云端清理只触发一次、单飞锁竞争与错误合并；`tests/test_backup_slice_manifests.py` 校验切片契约与注册表登记。手工验收：连续发两次 `POST /manual_backup`，确认第二次返回“未执行”，且目录里只出现一次新的插件与配置快照。
