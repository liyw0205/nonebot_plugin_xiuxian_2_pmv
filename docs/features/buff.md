# 功法与洞天福地
## 用户流程
购买、开垦、改名、闭关和结算操作都通过应用服务提交。
## 命令与别名
`功法`、`洞天福地购买`、`洞天福地查看`。
## Web API
`POST /api/v1/buff/{open,upgrade_field,rename,training_start,training_complete,stone_training,pvp_settle}`，权限 `user`，要求幂等键。
## 数据模型与迁移
迁移 `buff.001` 至 `buff.015`；双修令牌 operation 在 game DB，次数 projection 在 player DB，分别由 `buff.002`/`buff.003` 创建。双修结算 operation 由 game DB `buff.004` 创建；`buff.005` 在 player DB 补齐关系、保护、邀请与统计 schema。普通切磋回执由 game DB `buff.006` 预建，切磋胜负统计列由 player DB `buff.007` 预建；闭关结算及 effects 使用 `buff.008`/`buff.009`，普通修炼 lifecycle 使用 `buff.010`/`buff.011`，普通闭关进入使用 game receipt `buff.012` 与 player statistics `buff.013`。操作回执 `blessed_spot_operations` 由 game DB `buff.014` 创建，`stone_training_operations` 由 game DB `buff.015` 创建。

### 操作回执的 schema 归属

| 库 | 表 | 迁移 | 契约列 |
| --- | --- | --- | --- |
| game_db | `blessed_spot_operations` | `buff.014` | `operation_id`(PK)、`action`、`payload`、`result_json`、`created_at` |
| game_db | `stone_training_operations` | `buff.015` | `operation_id`(PK)、`payload`、`result_json`、`created_at` |

两条迁移均为 `IF NOT EXISTS`：旧库中由请求期惰性建出的回执行与历史记录原样保留，重复执行幂等。
请求路径 `features/buff/rename_repository.py`、`upgrade_repository.py`、
`stone_training_repository.py` 只经 `operation_schema.operations_ready()` /
`stone_training_ready()` 校验表与列；缺表或缺列返回 `schema_missing`，不建表、不补列，
也不改 `user_xiuxian`、`mix_elixir_info` 或回执行。`灵田开垦` handler 现在传
`stone_cost`（与 `BuffApplication.upgrade_field` 签名一致），福地开垦/改名在
`schema_missing` 时提示检查启动迁移。
legacy `BlessedSpotService` 与 `StoneTrainingSettlementService` 保留自身事务内建表，
仅作为显式兼容/回滚入口，不再是默认写路径。
定向测试：`python -m pytest tests/test_blessed_spot_schema_ownership.py -q`。

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

### 师徒解除清算（120% 权威传功修为）

解除师徒关系必须同时结清该徒弟**通过本段师徒获得**的修为，否则
`拜师 → 传功 → 解除 → 再拜入同一师父` 是无成本收益循环。

- 权威账目：`mentor_transmission_operations` 中该**配对**（`json_extract(payload,'$[0]')=mentor`、
  `'$[1]'=apprentice`）的 `reward_exp` 之和；调用方传入的数值一律不采信，事务内重算。
  表/列缺失或无 JSON1 时按有界回退扫描（上限 5000 行）。
  **账目读不出来（缺表、缺 `reward_exp`、回退扫描失败）一律 fail-closed**：
  返回 `ledger_unavailable`、零写入、关系保持——绝不按 0 处理，
  否则“先弄坏账目再解除”就是免费解除。
- 应收 = 该配对**尚未结清**收益的 120%，**向上取整**（`-(-gained * 6 // 5)`）；
  已经结清过的收益在后续解除中不再重复收取（已结清额度记在回执表 `gained_exp` 里）。
  `gained <= 0` 或坏值 → 0（历史关系、确实没有权威传功时不收费）。
- 原子性：单个 `BEGIN IMMEDIATE`（`AttachedDatabaseUnitOfWork`，attached game+player）事务内完成——
  回执重放判定 → 权威账目重算 → 配对/境界 CAS → 余额判定 →
  扣减修为（`exp = exp - charge`，带 `exp >= charge` 守卫，永不为负）→
  清关系字段、写冷却/重绑定、统计、历史 → 写回执。任一步失败整体回滚。
- 幂等：同 operation_id 重放且稳定输入一致 → `duplicate` 并返回首次实际扣减额；
  输入不同 → `operation_conflict`。重复解除第二次必然 `state_changed`（关系已不在），
  不会二次扣减；并发传功按事务内最新权威账目判定。
- 拒绝语义：修为不足 → `insufficient_cultivation`，**不解除关系、零资产变化**，
  回复应收修为、当前修为与缺口；绝不扣成负数，也绝不补修为凑数。
- 缺表/缺列 → `schema_missing` 零写入。请求路径**不执行 DDL**：回执表
  `mentor_dissolution_operations` 由 `buff.016` 预建，玩家统计列 `statistics."师徒清退修为"`
  由 `buff.017` 预建；`feature` 只经基础设施 UoW 读写，不自行打开连接、不 import 驱动层。
- 防刷保证：每一步解除都先付出 120% 且余额不足时关系保持，因此关系循环不产生净修为；
  `逐出师门` 与徒弟自行 `解除师徒` 共用同一清算事务（分别记 `expel`/`leave`）。
- 范围：出师（`MentorGraduationService`）路径不变，其判定与灵石奖励不参与本清算。

## 定时任务
无。
## 配置项
`buff_enabled`。
## 适配器差异
命令/Web 仅组装 DTO，不直接访问数据库。
## 测试与手工验收
覆盖 fake repository 成功、拒绝、重复和异常路径。

双修亲密度快照与写入守卫均沿用 `core.numeric` 的整数解释，兼容旧库
`partner.affection TEXT`；不会把相同的数字文本误判为关系变化，也不容忍真实
数值变化。亲密度写入被拒绝时，以 `partner_affection_write_rejected` 和发起方/
目标方返回具体原因，整笔 attached UoW 回滚。定向回归包含旧表结算、回执重放、
真实变化拒绝及两库完整回滚，不需要迁移或改写生产亲密度。
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
