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

`sect_fairyland.001` 在 `game_db` 写入功能迁移标记。`sect_fairyland.002` 只路由到 `player_db`，预建领取回执与按用户/宗门规范化的每日领取标记，并从旧 `sect_fairyland_claim.last_claim_{sect_id}` 回填日标记。历史炼体档案继续保存在 `tianti_info`，本切片不搬迁玩家资产字段。

## 事务与失败回滚

默认 application 调用 feature-owned SQL repository。repository 在同一 `player_db` immediate UoW 中校验幂等回执和当日标记、计算炼体收益、更新 `tianti_info`、写每日标记及操作回执；异常会同时回滚气血与领取状态，请求路径不执行 DDL。重复操作从 repository 回执重放，不另起一个可能卡在 `started` 的外层 operation ledger。显式 `LegacySectFairylandRepository` 仍保留作回滚边界，旧 service 在发现新规范化日标记时也会拒绝当天重复领取。

计算使用运行时 Clock、炼体配置和天降灵脉倍率；过期药浴清理、窍穴加成、炼体堂加成、气血上限及旧 `last_claim_{sect_id}` 投影均保持兼容。

## 定时任务

无新增定时任务。旧调度仍由兼容生命周期管理。

## 配置项

- `sect_fairyland_enabled` / `XIUXIAN_SECT_FAIRYLAND_ENABLED`：控制 feature registry 中的 Web/API 接入。NoneBot 宗门 matcher 由旧插件模块注册，关闭该开关不会自动把命令重接到 legacy repository；命令回滚需显式将 application 注入 `LegacySectFairylandRepository`。

## 适配器差异

NoneBot 命令适配器负责解析事件和生成 `ReplyPlan`；Flask blueprint 负责 DTO、权限、CSRF 和统一 JSON。领域与 application 不依赖 NoneBot、Flask 或 SQL。

## 测试与手工验收

- `python -m unittest nonebot_plugin_xiuxian_2.features.sect_fairyland.tests.test_sect_fairyland_application -q`
- `python scripts/check_architecture.py`
- `pytest -q nonebot_plugin_xiuxian_2/features/sect_fairyland/tests tests/test_fairyland_claim_service.py tests/test_sect_fairyland_claim_cutover.py`
- 使用隔离数据目录调用 `POST /api/v1/sect/fairyland/claim`；重复相同 `operation_id` 应从 feature receipt 重放且不重复发放气血。

## 灰度开关、回滚和已知限制

回滚前先备份 player DB；迁移回填、规范化标记读取和旧列投影共同保持新旧实现兼容。`.002` 只新增表并回填，不删除旧列或回执。真实发布迁移和 P7 验证完成前，不删除 compatibility shim。
