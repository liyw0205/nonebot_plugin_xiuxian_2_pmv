# 宗门炼体堂修行

## 用户流程

用户在加入宗门且满足当日修行条件时执行宗门炼体堂领取。旧命令仍由兼容入口接收，业务动作统一转发到 `SectFairylandApplication`。同一个操作号重复提交只返回第一次结果，不会重复增加炼体气血。

## 命令与别名

- `宗门淬体修行`
- `淬体修行`
- `宗门炼体堂修行`
- `炼体堂修行`
- `宗门炼体堂领取`

命令权限为普通用户；事件上下文负责提供用户、宗门、日期、境界和修行时长。

## Web API

`POST /api/v1/sect/fairyland/claim`，权限为 `user`，请求体至少包含：

```json
{
  "operation_id": "fairy-20260912-u1",
  "user_id": "u1",
  "sect_id": "sect-1",
  "day": "2026-09-12",
  "level": 2,
  "minutes": 30
}
```

成功响应使用统一 API envelope，`data` 包含 `status`、`sect_id` 和 `detail`；`operation_id` 是幂等键。参数错误、权限失败、重复领取和状态变更均返回结构化错误或拒绝结果，不暴露数据库异常。

## 数据模型与迁移

`sect_fairyland.001` 在 `game_db` 写入功能迁移标记。历史宗门状态仍由兼容仓储从 `player_db` 的宗门/炼体表读取，现阶段不搬迁玩家资产字段。

## 事务与失败回滚

应用层先在 `player_db.operation_ledger` 记录请求，再调用惰性旧仓储。旧仓储在同一个事务中校验宗门状态、日期和修行时长并更新炼体气血；拒绝或异常会回滚资产，应用层将拒绝/失败写入 ledger 和审计记录。

## 定时任务

无新增定时任务。旧调度仍由兼容生命周期管理。

## 配置项

- `sect_fairyland_enabled` / `XIUXIAN_SECT_FAIRYLAND_ENABLED`：默认启用，可热切换灰度；关闭后保留旧兼容入口。

## 适配器差异

NoneBot 命令适配器负责解析事件和生成 `ReplyPlan`；Flask blueprint 负责 DTO、权限、CSRF 和统一 JSON。领域与 application 不依赖 NoneBot、Flask 或 SQL。

## 测试与手工验收

- `python -m unittest nonebot_plugin_xiuxian_2.features.sect_fairyland.tests.test_sect_fairyland_application -q`
- `python scripts/check_architecture.py`
- 使用隔离数据目录调用 `POST /api/v1/sect/fairyland/claim`，重复相同 `operation_id` 应返回 replay，不重复调用仓储。

## 灰度开关、回滚和已知限制

关闭 `sect_fairyland_enabled` 即停止新 application 的组合根接入，可恢复旧处理路径。回滚前先备份数据库和 operation ledger；跨旧表的历史数据仍依赖兼容仓储，待完整发布周期和运行数据满足 P7 后再删除 shim。
