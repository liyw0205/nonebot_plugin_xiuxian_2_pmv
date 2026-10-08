# 功法与洞天福地
## 用户流程
购买、开垦、改名、闭关和结算操作都通过应用服务提交。
## 命令与别名
`功法`、`洞天福地购买`、`洞天福地查看`。
## Web API
`POST /api/v1/buff/{open,upgrade_field,rename,training_start,training_complete,stone_training,pvp_settle}`，权限 `user`，要求幂等键。
## 数据模型与迁移
迁移 `buff.001` 至 `buff.013`；双修令牌 operation 在 game DB，次数 projection 在 player DB，分别由 `buff.002`/`buff.003` 创建。双修结算 operation 由 game DB `buff.004` 创建；`buff.005` 在 player DB 补齐关系、保护、邀请与统计 schema。普通切磋回执由 game DB `buff.006` 预建，切磋胜负统计列由 player DB `buff.007` 预建；闭关结算及 effects 使用 `buff.008`/`buff.009`，普通修炼 lifecycle 使用 `buff.010`/`buff.011`，普通闭关进入使用 game receipt `buff.012` 与 player statistics `buff.013`。

### Phase 3 普通闭关 scope（已冻结，2026-10-09）
`scope_id=phase3-player-lifecycle-v1` 只覆盖默认 `闭关` 命令，不覆盖 `出关`、
`虚神界闭关` 或其它功法命令。当前旧入口在
`xiuxian/xiuxian_buff/__init__.py::in_closing_`，同时改写 game DB 的 `user_cd.type`
和 player-side `statistics.闭关次数`。现已由 `BuffApplication.closing_enter` 承载一次
可重放的 enter operation：attached game/player UoW 内先记 ledger，再以 `user_cd.type=0`
CAS 写入 type 1 和 started_at，原子 upsert 统计并写 game receipt；请求路径不得执行
DDL，缺 schema、用户、伪灵根、忙状态或 operation 冲突均 fail closed。迁移路由为 game
receipt `buff.012` 与 player statistics `buff.013`；handler 只负责身份、事件稳定 operation
ID 和兼容回复。进度 checker 的 `closing_enter_*` source contract 是该 scope 的静态证据，
application/repository/handler/source 聚焦回归为 `29 passed`；NoneBot composition-root
迁移导入不纳入该数字，避免既有 Activity 循环导入掩盖业务结果。

### Phase 3 普通出关收益计算 scope（v2，收益 adapter 子切片）
`scope_id=phase3-player-lifecycle-v2`、`stable_id=command:buff:出关` 只覆盖普通
`出关` 及其 `灵石出关` alias 的收益快照读取适配和纯计算。handler 先用
`BuffApplication.closing_replay` 做 replay-first 检查，再把一次 legacy read snapshot
交给 `BuffApplication.calculate_closing_reward`，由 `ClosingRewardCalculator` 返回不可变
`ClosingRewardSnapshot`；结算 mutation 仍由既有 `BuffApplication.closing_settle` 承担。
operation ID 使用独立的 `buff-closing-settle:` 前缀，两个命令走同一 application path。
本 scope 不覆盖 game CAS/receipt、outbox effects、player 统计/任务/活动投影、跨事务
恢复，也不覆盖 `虚神界出关`、`虚神界闭关` 或 impart_pk 其它命令；这些保留 backlog。
Phase 2 的 `496` 项 membership/hash 不变。

### Phase 3 普通出关 effects reconcile scope（v3）
`scope_id=phase3-player-lifecycle-v3`、`stable_id=command:buff:出关:effects-reconcile`
只处理普通 `出关` core settlement 成功后的 `buff.closing.effects` outbox dispatch、统计、
日志、任务、活动投影编排和 CLI/runtime reconcile。默认 `BuffApplication` 现在注入
feature-owned `ClosingEffectsApplication`；每个 projection 都使用稳定的 event/operation
ID，可在 `pending -> failed -> retry -> sent` 和部分失败后重放。旧
`compatibility.buff_closing_effects.LegacyBuffClosingEffects` 只保留兼容名称，不再是默认
owner；任务和活动底层 legacy sink 仍是显式兼容依赖，不宣称其存储实现已整体迁移。
本 scope 不覆盖 v2 的 reward calculator，不覆盖 game CAS/receipt mutation、请求期隐式
初始化、跨库恢复，也不覆盖 `虚神界出关`、`虚神界闭关` 或 impart_pk 其它命令；这些继续
进入 backlog。Phase 2 `496` 项 membership/hash 不变。
## 事务与失败回滚
统一 operation ledger 和审计，旧事务异常可重试。
## 定时任务
无。
## 配置项
`buff_enabled`。
## 适配器差异
命令/Web 仅组装 DTO，不直接访问数据库。
## 测试与手工验收
覆盖 fake repository 成功、拒绝、重复和异常路径。
## 灰度开关、回滚和已知限制
关闭开关后使用旧功法入口；双修令牌默认经 `PartnerTokenUseApplication` 原子消费库存并更新次数。双修结算默认经 `PartnerCultivationApplication`，把修为、属性、次数、统计、亲密度和邀请状态置于同一 attached UoW；随机计算仍由领域 handler 预滚。普通切磋默认经 `BuffApplication -> NormalPvpSqlRepository`，在 attached UoW 内完成双方 HP/MP/体力 CAS、胜负统计和回执，缺 schema、用户或快照冲突时 fail closed；请求路径不执行 DDL。切磋 handler 的动态属性读取也经 `PlayerAttributeApplication`，不再直接读取 `get_user_real_info`。旧 `NormalPvpSettlementService` 仅保留显式兼容用途，`pvp_battle` 只承载纯战斗计算适配。双修/师徒默认 handler 的姓名、境界、修为和存在性读取统一经 `PlayerProfileApplication`；信息页、功法状态和默认玩家战斗辅助的动态属性读取统一经 `PlayerAttributeApplication`，旧 `get_final_attributes` 仅作为显式兼容公式 provider。

## Manifest 清单
- `route: POST /api/v1/buff/open`
- `route: POST /api/v1/buff/upgrade_field`
- `route: POST /api/v1/buff/rename`
- `route: POST /api/v1/buff/training_start`
- `route: POST /api/v1/buff/training_complete`
- `route: POST /api/v1/buff/stone_training`
- `route: POST /api/v1/buff/pvp_settle`
