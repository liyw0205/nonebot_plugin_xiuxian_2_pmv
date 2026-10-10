# 运维日志查询

## 用户流程

管理员打开日志页，先查询消息用户候选，再按用户和筛选条件分页查看消息；也可浏览受限日志文件目录、读取分页内容或读取增量 tail。日志读取只投影现有数据，不初始化运行库。

## 命令与别名

无命令。`commands.py` 的 `COMMANDS = ()` 明确该 owner 只提供管理 Web 读模型。

## Web API

旧 Flask 数据入口保留兼容鉴权和响应 envelope，并委托 `LogsApplication`：

- `GET /api/logs/users` → `LogsApplication.users`
- `GET /api/logs/user_messages` → `LogsApplication.user_messages`
- `GET /api/logs/files` → `LogsApplication.files`
- `GET /api/logs/read` → `LogsApplication.read`
- `GET /api/logs/tail` → `LogsApplication.tail`

`GET /logs` 仅渲染兼容模板，不伪造第二份 application 路径；运行时 `/api/v1/logs` 是独立的系统审计接口。

## 数据模型与迁移

无 feature-owned 表和迁移，`migrations.py` 的 `MIGRATIONS = ()`。消息历史、回复标记和文件目录分别通过既有 `MessageHistoryRepository`、`MessageReplyRepository`、`MessageRecallRepository` 与 `LogFileRepository` 读取；请求不建库、不补列。

## 事务与失败回滚

消息和资料使用只读 game/message UoW；撤回由显式 `MessageRecallApplication` 委托已有 repository，发送失败不会伪造日志成功。文件 read/tail 限制扫描、单行和响应大小，路径越界或轮转返回明确错误。

## 定时任务

无任务，`jobs.py` 的 `JOBS = ()`。日志轮转和限速等其他 job 仍由 compatibility scheduler 负责，不在本 owner 重复注册。

## 配置项

无 feature 配置项。日志根目录、消息库路径和 Web 管理员权限由 runtime context/adapter 注入；本切片不导出凭据或修改配置文件。

## 适配器差异

`LogsApplication` 只编排 repository 与 DTO；旧 Flask 模块保留登录、参数解析、presenter 和 JSON envelope，文件读取 adapter 负责路径边界与有界内存。OneBot/QQ 消息展示差异由既有 presenter 处理。

## 测试与手工验收

运行 `.venv/bin/pytest -q tests/test_logs_routes.py nonebot_plugin_xiuxian_2/features/logs/tests -p no:cacheprovider`，应覆盖匿名拒绝、参数透传、用户合并、消息历史、文件目录/read/tail 与撤回。手工确认管理员页面可加载、缺少日志文件时返回空结果而不创建数据库。
