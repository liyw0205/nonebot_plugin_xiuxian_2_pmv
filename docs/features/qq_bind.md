# QQ 扫码绑定

## 用户流程

管理员在配置页发起扫码绑定，服务端生成一次性任务和二维码；轮询确认后解密凭据并安全合并到环境文件，最后按运行能力决定是否提交重启。过期任务不会写配置。

## 命令与别名

无聊天命令，`commands.py` 声明 `COMMANDS = ()`。所有入口使用旧 Web 管理会话权限。

## Web API

`ROUTES` 为空，旧 `xiuxian/xiuxian_web/qq_bind_routes.py` 委托以下 application 方法：`POST /api/config/qq-bind/start` → `QqBindApplication.start`；`GET /api/config/qq-bind/qr/<task_id>` → `QqBindApplication.qr_png`；`POST /api/config/qq-bind/poll` → `QqBindApplication.poll`；`GET /api/config/qq-bind/restart-capability` → `QqBindApplication.restart_capability`；`POST /api/config/qq-bind/restart` → `QqBindApplication.restart`。

## 数据模型与迁移

无 SQLite 表和迁移，任务暂存由旧 Web 适配器注入的 `QqBindTaskStore` 管理；`schemas.py` 固化 `QqBindResponse`，`repository.py` 固化存储协议。

## 事务与失败回滚

任务凭据只在确认成功后消费；缺失任务、错误状态、解密或环境写入异常均返回明确失败，不调用重启。环境合并由注入端口完成，application 不保留 secret。

## 定时任务

无定时任务，`JOBS = ()`。任务 TTL 和清理由 `BindTaskStore` 的既有实现负责。

## 配置项

不新增配置键。QQ_BOTS 环境文件路径、绑定页地址、加密密钥和重启策略均由适配器端口注入，避免把 secret 纳入配置面板导出。

## 适配器差异

application 只使用异步 callable、任务存储和文件端口；Flask 适配器负责鉴权、状态码、PNG 响应和异步桥接，NoneBot 不直接参与扫码流程。

## 测试与手工验收

运行 `python -B -m unittest discover -s nonebot_plugin_xiuxian_2/features/qq_bind/tests -t . -q` 与架构门禁。隔离环境中验证成功轮询、过期任务、缺字段、解密失败和重启确认不会写入真实 `.env`。
