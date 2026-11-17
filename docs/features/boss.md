# 世界BOSS资产结算

## 用户流程

世界BOSS兑换和讨伐结算均通过 application 生成唯一 operation_id。旧 NoneBot 命令通过兼容门面调用新 application，返回字段保持兼容。

`世界BOSS兑换` 由 `BossPurchaseCommandApplication` 负责。命令先读取 ledger/旧回执和用户 profile，再读取商品配置、物品目录、周限购与积分快照；原始请求数量写入 operation payload，实际数量只在事务内按剩余周限购裁剪。成功回放不依赖当前配置、目录、积分或周限购，拒绝和不明确回执不会渲染为成功。回执仓储只读、不执行请求期 DDL；`started`、`needs_reconcile`、旧表缺少明确 status 和坏 JSON 均 fail closed。

## 命令与别名

- `世界BOSS兑换`：积分商店兑换。
- `世界BOSS商店 [关键词|页码]`：商店列表，可按物品名/ID/描述筛选；筛选与翻页续查走
  `docs/features/base.md` 的共享合同（第 2 页仍是筛选结果，无命中与空商店分开提示），
  只读，不改积分扣减、周限购或兑换回执。
- `讨伐世界BOSS`：战斗结果结算。

## Web API

- `POST /api/v1/boss/purchase`，权限 `user`，支持 `Idempotency-Key`。
- `POST /api/v1/boss/settle`，权限 `user`，支持 `Idempotency-Key`。

响应使用统一 `OperationOutcome`。业务拒绝返回 HTTP 409，输入错误返回 HTTP 400。

## 数据模型与迁移

`boss.001` 在 `game_db` 创建 `boss_feature_migrations`，`boss.004` 与 `boss.005` 在 `player_db` 启动时创建世界BOSS生命周期、限额和全量刷新回执表。积分、背包和战斗结算继续复用既有跨库表，但默认通过 `BossApplication` 的 feature-owned repository 执行；手动生成、全量刷新与每日限额重置运行期只读校验 schema，缺失时 fail closed，不执行 DDL。

### 惩罚/状态表的 schema 归属（`boss.007`）

`WorldBossPunishmentSqlRepository` 原先在 `snapshot`、`get_result`、`punish` 共四处请求期
`CREATE TABLE IF NOT EXISTS` 建 `world_boss_state`（`state_key`/`bosses`/`updated_at`/`revision`）
与 `world_boss_punishment_operations`（`operation_id`/`payload`/`result_json`/`created_at`）。
现在 `boss.007` 在 game DB 统一建这两张表，`features/boss/schema.py` 是唯一契约来源；
请求路径只校验「表存在 + 本次要读的列齐全」：

- 写路径 `punish`：缺表或缺列返回 `schema_missing`，不改变首领状态、不写回执；
- 读路径 `snapshot` 降级为 `([], 0)`，`get_result` 返回 `None`；
- 半成品 `world_boss_state` 不会被补列。

## 事务与失败回滚

资产 application 先写 `operation_ledger`，再执行结算；手动生成使用版本 CAS 与回执幂等，每日限额重置冻结目标并按 chunk 恢复。相同 operation/日期重试只返回首次结果；异常在当前事务回滚并保留可恢复进度。旧 transaction service 仅作为显式兼容/回滚路径。

兑换 writer 复用 `BossApplication.purchase -> BossPurchaseSqlRepository.purchase`，在附加 player DB 的 immediate SQLite 事务中校验积分/周限购/背包容量并同时更新积分、周限购、背包和业务回执；ledger 与业务回执仍保留原 operation identity。已确认的 SQL 回滚记录为 `internal_error`，同 payload 可重试；无法确认的 started/needs_reconcile 不盲目重放。

世界BOSS战斗和“世界BOSS信息”通过 `BossApplication.daily_limit_snapshot` 读取每日讨伐次数、积分和灵石。读取使用只读 UoW，每次最多取一行，不缓存；缺数据库、表、用户行或旧字段时返回零，不创建数据库/表、不补字段，也不插入默认用户行。战斗沿用该快照作为结算 CAS 的期望值；显式限额写入与每日重置仍由各自 mutation 路径负责。

世界BOSS积分排行榜通过 `BossIntegralApplication.top_integrals` 读取长期 `boss_limit.integral` 投影，只返回最多 50 项且不在 Python 中物化全量积分表。重复 user_id 保持与积分发放相同的首 rowid 语义；缺数据库/schema 返回空列表，不建库、不建表。旧积分榜曾读取错误的 `integral.boss_integral` 表，不能作为当前数据源。榜外玩家另见 `BossIntegralApplication.integral_rank`：并列名次 + 有分人数 + 本人积分，行追加在前 50 名之后，缺库缺表返回空 `PersonalRank`；共享渲染与口径见 `docs/features/base.md` 的「排行榜本人名次（共享合同）」。

### 商店每周限购行
`世界BOSS商店` 的商品行改走共享 `shop_limit_line`，渲染 `每周限购：{剩余}/{总量}个`、
售罄写 `已售罄（N/N个）`、不限购写 `不限购`；口径见 `docs/features/base.md` 的
「商店每周限购行（共享合同）」。只读渲染，不改兑换与周限购判定。

### 每日额度与剩余次数

每日讨伐次数、积分上限、灵石上限原先在结算与 `世界BOSS信息` 各写死一遍
（`12000` / `300000000` / `DAILY_BATTLE_COUNT`），视图与结算可能各自漂移。现在三者
集中在 `xiuxian/xiuxian_boss/boss_limit.py`：

- `DAILY_BATTLE_COUNT = 30`、`DAILY_INTEGRAL_LIMIT = 12000`、`DAILY_STONE_LIMIT = 300000000`；
- `daily_quota_line(battle_count, limit=DAILY_BATTLE_COUNT)` 为纯函数，输出
  `今日讨伐：X/30（剩余 N 次）`，最后一次显示 `已用尽`，超额/坏输入按上限截断；
- 结算与 `世界BOSS信息` 都引用同一常量，战斗成功回执末尾追加该行，玩家不必再另发
  `世界BOSS信息` 才能知道剩余次数。

数值本身未调整，仍是既有上限；本条只消除重复字面量与信息缺口。

### 讨伐伤害上限说明
`actual_damage = min(total_damage, boss_all_hp*0.2, boss_old_hp)` 一直生效，但回执
原本只写「受天道限制，实际削减…」；现在被截断时由
`features/boss/schemas.py::boss_damage_note` 追加一句原因（未截断时零输出）：

- 单次上限是最紧的一项 → `说明：单次讨伐最多削减世界BOSS最大血量的20%（本次上限 N），超出的伤害不结算。`；
- 剩余血量是最紧的一项 → `说明：世界BOSS剩余血量只剩 N，超出的伤害无处结算。`。

上限与剩余血量同分时优先报单次上限（固定规则）。只读渲染，不改伤害公式、
`damage_ratio`、积分/灵石/修为奖励与结算 CAS。由 `tests/test_boss_damage_note.py` 钉住。

### 全量刷新回执的紧凑编码

`world_boss_full_refresh_operations.payload` 原先写入 25 个 BOSS 的**完整定义**（`bosses` 与
`expected_bosses` 各一份）加配置快照。该列在代码里只用于判等（`operation_conflict`），
可回放的 BOSS 列表另存在 `result_json`。线上实测该列 47.5 MB（`player.db` 的 41%，
0.40 MB/日，单行 12,842 B）。

现改为 `features/base/receipt_payload.py::build_payload` 的紧凑回执
（`kind="full_refresh"`、`trigger`、`expected_revision`、`boss_count`、`digest`）：

- `payload_matches` 对 v2 比 digest，对旧行按原文算 sha256，升级后重放旧 operation 仍为
  `duplicate`；不同 `trigger`/`expected_revision` 仍是 `operation_conflict`。
- 回放内容不变：`get_result` 继续从 `result_json` 返回 BOSS 列表与 revision。
- 取舍：回执不再保存当时的期望 BOSS 定义（当前 BOSS 仍在 `world_boss_state`，回放用
  `result_json`）。既有旧行不重写（单行 12,842 → 168 B）。

## 定时任务

世界BOSS刷新和天罚仍由显式兼容生命周期注册，手动生成、全量刷新与每日重置的持久化由 feature-owned repository 承担；手动与定时全量刷新共享 operation receipt，未在 feature 导入时重复注册。

### 全量刷新故障恢复

`set_boss_generation` 的注册异常透传启动生命周期；调度桥尚未激活时保留声明，激活
失败可重试，不能凭“已配置”日志认定任务已注册。小时/分钟统一接受非负整数或整数
字符串，均为0时正常禁用，负数/小数/布尔/缺键明确 `world_boss_schedule_invalid`。
间隔的幂等槽使用注入业务时钟的 Unix 时间戳，同一时刻转换时区不改变操作号；整点
相邻时间槽分别刷新。同槽回放读取当前状态同步缓存，不将历史回执中的满血状态复活。

状态 UPSERT 与永久回执 INSERT 在同一 player DB immediate 事务中逐步校验行数，
全部写入后再回读两行；IGNORE、trigger改写或后一步篡改早一步均整笔回滚（含副作用）。
`world_boss_refresh_state_write_rejected/global` 与
`world_boss_refresh_receipt_write_rejected/operation_id` 经应用到实际任务/管理员回复。
任务拒绝产生 `EVENT_JOB_ERROR`；配置变化、场次变化、缺迁移、生成数据无效各保留
具体原因/主体。SQLite未知异常保留原链，管理员收到安全定位；正常流程和失败回复的
投递异常透传。成功回执和旧紧凑/完整payload继续回放，拒写无回执，同请求可恢复。
状态或回执JSON损坏明确拒绝，不过滤坏条目或当空列表覆盖。

玩家查询、列表与讨伐只接受完整正整数编号/页码（查询空参数列全部，列表默认1）。
负号、多余参数、非数字和超长整数明确拒绝；全量生成不接收参数。讨伐CAS通过feature
仓储读取冷却原文，保留旧本地/带时区时间，不使用展示工具函数重新格式化；首次NULL
和空串均是未记录时间的合法期望值，实际结算时间和日/周期间仍必填。奖励、20%伤害
上限与额度规则不变，缺用户行仍拒绝。全量生成回复沿原有纯文本消息门面，不强制QQ
Markdown；定时通知失败可能发生在提交后，重放不重复刷新/通知，未新增通知补发队列。

可重放入口为 `tests/test_world_boss_refresh_recovery.py` 和
`tests/test_world_boss_parameter_dispatch.py`：实际注册/调度事件、隔离迁移SQLite、
真实dispatch/DI/handler贯穿刷新→查询/分页→讨伐→回放→下一槽，核对余额/体力/
扣血/回执/统计/缓存及三库完整性。战斗、图片渲染和发送为mock，不代表真实QQ展示。

每日限额重置的逐用户明细（`world_boss_daily_limit_reset_targets`）由启动维护
`features/maintenance/reset_target_retention.py` 保留 30 天：只清理「日期规范、状态为
`applied/skipped`、且该业务日父操作行已 `completed`」的行；`pending`/未知状态/非 ISO 日期/
缺父行一律保留，父操作行（含总计与变更数）与 `boss.boss_*` 永不改动。
详见 `docs/features/maintenance.md`「每日重置明细保留」。

### 每日额度重置故障恢复

`daily_reset_boss_limits` 保持调度时区的每日00:01:40、coalesce、单实例及300秒
misfire宽限。默认日期用注入时钟转换调度时区，避免上海零点使用UTC前一天；显式日期
仍用于恢复指定业务日，仓储独立默认Asia/Shanghai。未新增自动补跑历史日期的策略。

冻结任务/名单独立事务，处理每个分块另用 immediate 事务。登记、冻结名单、额度清零、
逐用户回执及汇总进度均校验行数，最后回读本块额度/完整名单/汇总；IGNORE、改写或
末步篡改前步均拒绝并回滚当前事务及trigger副作用。已有完成分块保留，失败名单仍pending，
故障解除后重建应用继续；不再次清掉已完成玩家的新额度。已删除玩家记skipped，冻结后
新增玩家不加入该日。completed汇总仍是永久幂等依据，兼容30天明细清理与历史回放。

具体 `world_boss_reset_{operation,freeze,limit,target,progress}_write_rejected` 分别携带
业务日或玩家主体，坏进度明确 `world_boss_reset_progress_invalid`，坏数值保留字段原因，
拒绝退出事务后才转结果。失败结果即使task_status仍running也停止循环，不空转；任务
产生 `EVENT_JOB_ERROR` 并保留原因/主体。SQLite未知异常保留原链，管理员收到安全定位，
正常流程和失败回复投递异常透传。管理员重置无参数，多余参数零写；旧兼容服务只保留
原有schema准备，写入委派同一feature校验，不新增迁移或改永久积分/经济规则。

`tests/test_world_boss_daily_reset_recovery.py` 覆盖实际声明注册/持续失败pending恢复、
cron边界/真实触发、隔离SQLite五步拒写/改写/ABORT/后步篡改与恢复、整块回滚/跳过/
旧完成回执和坏数据；两条 `world-boss-daily-reset` 实际dispatch/DI/handler覆盖权限、
玩家额度读取、跨本地日期、同日新额度保留、拒写/未知异常同消息恢复与三库完整性。
OneBot/QQ文本、Markdown及降级仅mock，未做真实QQ展示。

## 配置项

`boss_enabled`（`XIUXIAN_BOSS_ENABLED`）控制新资产边界，默认启用。

## 灰度开关、回滚和已知限制

关闭开关即可停止新 manifest/API 并保留旧命令。活动联动奖励和读模型仍由旧适配器提供，完整发布周期后再删除兼容层。

## 适配器差异

application 保持框架无关；命令适配器负责战斗快照解析，Web 适配器负责 DTO、权限和 CSRF。

## 测试与手工验收

覆盖兑换、讨伐结算的成功、拒绝、异常回滚和重放，并在隔离数据目录执行 Web client 验收。
