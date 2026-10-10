# 插件配置管理

## 用户流程

管理员打开 `/config` 查看按类别分组的可编辑配置，通过 `/save_config` 提交修改。应用只负责读取配置对象、格式化展示值并调用注入的写入端口，写入失败返回原错误，不伪造成功。

## 命令与别名

无聊天命令；`commands.py` 声明 `COMMANDS = ()`。配置面板的权限由旧 Web 会话和 CSRF 链承担。

## Web API

`ROUTES` 为空，旧模块 `xiuxian/xiuxian_web/config.py` 保留两条适配器入口：`GET /config` → `PluginConfigApplication.config_by_category`，`POST /save_config` → `PluginConfigApplication.save_values`。旧适配器继续负责会话、CSRF 和 JSON 响应。

## 数据模型与迁移

本 owner 不拥有 SQLite 表，`MIGRATIONS = ()`。配置值落在既有 `xiuxian_config.py` 文件，字段白名单和类型来自 `schema.py`，`schemas.py` 只提供稳定导入面。

## 事务与失败回滚

配置文件写入是独立文件操作，不承诺多字段整体原子性；写入端口返回失败时应用原样返回。快照和恢复由备份 owner 负责，本切片不创建临时数据库或内存缓存。

## 定时任务

无定时任务，`JOBS = ()`。配置生效需要按页面提示重启实例。

## 配置项

字段契约由 `CONFIG_EDITABLE_FIELDS` 集中定义；`web_secret_key` 等敏感字段仍沿用既有 Web 配置策略，不新增 secret 导出。

## 适配器差异

application 不依赖 Flask；NoneBot/Flask 适配器负责构造配置工厂、写入端口和会话权限。列表值展示使用安全的 `ast.literal_eval`，不执行任意表达式。

## 测试与手工验收

运行 `python -B -m unittest discover -s nonebot_plugin_xiuxian_2/features/plugin_config/tests -t . -q`，并执行 `scripts/check_architecture.py`。手工检查配置读取不创建数据库，提交非法字段不会越过白名单，写入失败响应为失败状态。
