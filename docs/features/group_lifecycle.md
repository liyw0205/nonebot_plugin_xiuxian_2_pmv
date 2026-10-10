# 群生命周期通知

## 用户流程

NoneBot 收到机器人入群、退群或成员加入事件后，application 统一分类事件、读取群欢迎策略并发送欢迎或离群通知；重复预处理事件复用已有结果，避免状态重复应用。

## 命令与别名

本 owner 不注册命令，`commands.py` 声明 `COMMANDS = ()`。欢迎开关命令仍由既有管理员适配器负责解析。

## Web API

没有 Web API，`ROUTES = ()`、`LEGACY_ROUTES = ()`。生命周期事件只从 NoneBot 事件适配器进入。

## 数据模型与迁移

本切片不拥有新表，`MIGRATIONS = ()`。群欢迎禁用列表继续通过注入的管理员配置仓储读取，生命周期状态更新委托现有 `apply_lifecycle_event` 适配器。

## 事务与失败回滚

通知发送是最佳努力副作用；分类失败或发送失败不会伪造成功，也不会重复写状态。application 不建立长连接、不持有事件缓存，消息失败只记录日志并返回决策对象。

## 定时任务

无定时任务，`JOBS = ()`。事件驱动通知不会创建每事件 sleeper。

## 配置项

`group_welcome`、`group_welcome_msg`、`group_bot_join_msg` 等配置仍由 plugin_config owner 管理，本切片只读取注入 settings provider；群级关闭状态由管理员配置仓储管理。

## 适配器差异

application 通过端口接收 bot/event、消息 segment 和发送函数，NoneBot 适配器负责实际事件类型与平台消息格式；业务分类和策略不依赖 Flask。

## 测试与手工验收

运行 `python -B -m unittest discover -s nonebot_plugin_xiuxian_2/features/group_lifecycle/tests -t . -q` 与架构门禁。隔离事件对象验证机器人入群、成员加入、退群、屏蔽群和重复预处理不会产生重复通知。
