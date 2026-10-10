# 空消息兜底回复

## 用户流程

消息适配器在没有匹配到指令时调用 `EmptyFallbackApplication.should_respond`。
符合配置和场景策略后由 `respond` 先尝试图片回复，再发送纯文字兜底；图片失败不会阻断文字回复。

## 命令与别名

本功能不是命令，不声明 `CommandSpec` 或别名。它由兼容层
`xiuxian/xiuxian_admin/empty_fallback.py` 的未匹配消息入口触发。

## Web API

无 Web API。`ROUTES` 与 `LEGACY_ROUTES` 均为空，管理配置由 `plugin_config` owner 提供。

## 数据模型与迁移

功能不持久化状态，`migrations.MIGRATIONS` 为空。外部随机图片只作为一次请求期输入，不能写入运行数据。

## 事务与失败回滚

没有资产事务。图片请求、图文发送和文字发送均是独立的注入端口；图片或文字发送异常被记录后结束本次兜底，不重试或重复发送资产。

## 定时任务

无后台任务，`jobs.JOBS` 为空。

## 配置项

读取 `empty_fallback`、`empty_msg` 和 `empty_fallback_image`。配置由 `plugin_config` 统一校验和写入，本功能只消费只读快照。

## 适配器差异

事件分类和消息发送由 NoneBot 兼容适配器注入。QQ 官方群创建事件要求显式 `to_me`，OneBot 普通群消息沿用原有群屏蔽策略。

## 测试与手工验收

`features/fallback/tests/test_application.py` 覆盖私聊、普通群、QQ 群创建、满消息群、图片降级和发送异常。验收时使用假的图片提供器与发送器，禁止请求真实图片接口。
