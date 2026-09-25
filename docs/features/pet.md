# 宠物资产操作

## 用户流程

宠物游历派遣、游历领取、喂食、砸蛋和技能替换五项资产动作通过 `PetApplication` 统一处理。砸蛋与技能替换命令默认直接调用 feature application，兼容门面只保留显式回滚入口；结果适配仍保留原有字段和提示语。

## 命令与别名

- `宠物游历`、`灵宠游历`：派遣宠物。
- `领取宠物游历`、`领取游历奖励`：领取游历奖励。
- `宠物喂食`：消耗背包物品提升宠物经验。
- `砸蛋`、`砸宠物蛋`：消耗灵石孵化宠物。
- `替换宠物技能`、`确认替换宠物技能`：确认替换升星候选技能。
- `保留宠物技能`、`放弃替换宠物技能`：放弃候选技能并保留当前技能。

## Web API

- `POST /api/v1/pet/travel/start`
- `POST /api/v1/pet/travel/claim`
- `POST /api/v1/pet/feed`
- `POST /api/v1/pet/hatch`

所有接口权限为 `user`，写请求支持 `Idempotency-Key`，统一返回 `OperationOutcome`。

## 数据模型与迁移

`pet.001` 在 `game_db` 创建 `pet_feature_migrations`；`pet.002` 在 `game_db` 创建 `pet_hatch_operations`；`pet.003` 在 `player_db` 创建 `pet_skill_replace_operations`。孵化和技能替换 repository 只使用已迁移 schema，不在请求路径建表；宠物历史表仍由既有 player 数据边界管理。

## 事务与失败回滚

application 先调用带 operation replay/CAS 的 feature-owned repository；重复请求只重放首次结果，宠物快照、背包和灵石不匹配时返回拒绝且不修改资产。缺少 `pet.002` 或 `pet.003` 时请求失败并保持 schema 不变。

## 配置项与回滚

`pet_enabled`（`XIUXIAN_PET_ENABLED`）控制灰度。关闭后可以继续使用旧入口，待融合、放生和技能动作迁移完成、兼容命中归零后再删除门面。

## 定时任务

游历到期由兼容调度器处理，任务使用稳定 ID 和幂等领取操作。

## 适配器差异

application 不导入 NoneBot、Flask 或数据库驱动；旧命令和 Web blueprint 分别承担协议转换。

## 测试与手工验收

覆盖派遣、领取、喂食、孵化的成功、库存拒绝、异常回滚和重放，并执行 Web CSRF/权限测试。

## 灰度开关、回滚和已知限制

`XIUXIAN_PET_ENABLED` 关闭后回到旧兼容实现；融合、放生以及查询/展示中的历史 JSON 边界仍保留显式兼容路径。
