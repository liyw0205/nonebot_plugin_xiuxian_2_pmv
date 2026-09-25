# 裂隙世界
## 用户流程
世界生成、进入、终止、事件奖励、加速和普通结算均有稳定操作号。秘境斩妖令 `20018` 的战斗仍由
NoneBot handler 预滚；资产、探索次数、统计和奖励结算经 `RiftApplication.settle_demon_token_battle`。
## 命令与别名
`裂隙`、`进入裂隙`、`裂隙探索`。
## Web API
`POST /api/v1/rift/{generate,enter,terminate,event_settle,speedup,settle}`，权限 `user`。
## 数据模型与迁移
迁移 `rift.001` 保留既有裂隙 feature marker；`rift.002` 在 game DB 建斩妖令兼容 operation 表，
`rift.003` 在 player DB 预建/补齐探索计数和统计列；`rift.004` 在 game DB 预建加速 replay 表，并补齐旧表缺列；
`rift.005` 在 game DB 预建世界状态与生成 replay 表；`rift.006` 在 game DB 预建秘境终止 replay 表；
`rift.007` 在 game DB 预建秘境钥匙事件 replay 表；`rift.008` 在 game DB 预建普通结算 replay 表并补齐历史 `message` 列。
`rift.009` 在 game DB 预建/补齐秘境 entry、entry count 和 entry replay 表。
迁移使用 `IF NOT EXISTS` 与缺列探测保留现有行。
秘境世界生成、当前世界读取和历史 JSON 首次导入经 `RiftApplication` 与 SQL repository；玩家 entry JSON 兼容读取仍保留旧仓储。
## 事务与失败回滚
斩妖令在 attached game/player UoW 内校验秘境、角色资源、道具数量与探索快照，再提交背包、战斗资产、奖励、
探索次数、统计、秘境状态和旧格式 operation payload；异常回滚两库。默认 handler 不走旧
`RiftDemonTokenBattleSettlementService`，该 service 保留给兼容对照。其他裂隙兼容边界仍需分片迁移。
加速默认经 `RiftSpeedupSqlRepository` 原子消耗加速券、缩短秘境与冷却时间，并保留旧 payload replay；
缺少 `rift.004` 时返回 `schema_missing`，请求不建表。世界生成使用稳定 operation ID、递增 revision 与旧 payload replay；
缺少 `rift.005` 时拒绝生成，generation/current-world/bootstrap 请求路径均不建表。
秘境终止经 `RiftApplication -> RiftTerminationSqlRepository` 原子结束 active entry、释放 cooldown 并记录旧格式 replay payload；
缺少 `rift.006` 时返回 `schema_missing`，终止和 replay 请求路径不建表。
秘境钥匙事件经 `RiftApplication -> RiftKeyEventSqlRepository` 在 attached game/player UoW 内原子扣除钥匙、结算预滚事件、更新探索次数/统计和奖励；
缺少 `rift.007` 或 player schema 时返回 `schema_missing`，请求和 replay 路径不建表，并兼容旧 key-event payload。
普通秘境结算经 `RiftApplication -> RiftSettlementSqlRepository` 注入 Clock，在事务内校验结算时间窗口、资源/快照和探索次数，原子写入奖励/统计、
结束 entry、释放 cooldown 与 replay；缺少 `rift.008` 或 player schema 时返回 `schema_missing`，不在请求路径建表或补列。
秘境进入经 `RiftApplication -> RiftEntrySqlRepository` 校验 generation/revision、参与者、体力和秘藏令快照，在 game DB 单事务内更新 world participants、
entry、cooldown、entry count 和 replay；缺少 `rift.009` 时返回 `schema_missing`，entry 请求不建表或补列。
玩家 active entry 只读查询经 `RiftEntrySqlRepository.read_entry` 使用 read-only UoW；表或历史列缺失时不执行 DDL，才回退旧玩家 JSON 投影。
## 定时任务
世界生成继续由兼容 scheduler 触发。
## 配置项
`rift_enabled`。
## 适配器差异
业务层不依赖 NoneBot/Flask。
## 测试与手工验收
覆盖世界版本冲突、重复进入、道具不足、普通结算重放，以及斩妖令胜负结算、十次奖励、库存限制、统计异常回滚、
旧 payload replay 和真实注册 `道具使用` matcher 的 `20018` dispatch。
## 灰度开关、回滚和已知限制
关闭开关回退旧裂隙入口；生成计划仍由旧配置提供。

## Manifest 清单
- `route: POST /api/v1/rift/generate`
- `route: POST /api/v1/rift/enter`
- `route: POST /api/v1/rift/terminate`
- `route: POST /api/v1/rift/event_settle`
- `route: POST /api/v1/rift/speedup`
- `route: POST /api/v1/rift/settle`
