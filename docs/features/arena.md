# 竞技场兑换与挑战

## 本组范围

本组收口冻结清单中的两个入口：`竞技场兑换`、`竞技场挑战`。`竞技场查看` 仅随共享对手缓存接入同一 owner，未新增冻结条目。排行榜、购买挑战次数、挑战券和调度任务不在本组新增改动范围。

## 默认调用链

- 兑换：真实 matcher -> `arena_buy_` -> `ArenaApplication.purchase_result/purchase` -> `ArenaChallengePurchaseSqlRepository`。
- 挑战：真实 matcher -> `arena_challenge_` -> `ArenaApplication.settlement_result/settle` -> 同一 SQL repository。
- 玩家快照：`PlayerProfileApplication`；竞技场状态：`ArenaStateApplication`。本组 handler 不为读取资料初始化旧 `XiuxianDateManage` 或 `PlayerDataManager`。
- 匹配与查看：`ArenaOpponentApplication` -> `ArenaOpponentRepository`。旧 `find_arena_opponent` 和缓存 set/get/clear 函数只作委托，不保留第二份字典。
- 战斗继续使用已有 `BattleSystem`、属性和增益处理，不另写战斗算法。

## 兑换与回放

兑换仅接受有界长度的数字商品编号与正整数数量。handler 将原始请求数量交给应用，显式设置 `clamp_quantity=True`；剩余额度的裁剪在事务内完成，不改变幂等请求身份。其他调用方默认不启用裁剪，原有语义保持。

同一事件先读取持久化回执，再检查当前商品目录、段位、荣誉和限购。回执验证玩家、商品和原始数量；成功重放不再次扣除荣誉或添加物品，拒绝重放不能变成成功。周限购复用 `ArenaStateRepository._weekly` 的 ISO 周规则，与状态读取保持一致。

`arena.010` 的 `apply_arena_purchase_receipt` 在启动阶段为 `game_db.arena_purchase_operations` 补充 `status`、`result_json`。历史记录没有可靠状态，不按数量或费用猜测成功，保留 `needs_reconcile` 待核查。请求路径不创建表。

## 挑战与失败

挑战先查结算回执，已处理请求不重新匹配或战斗。新请求才读取双方状态并运行战斗，结算继续通过既有事务写入积分、次数、体力与双方最终生命/真元快照。

缺少玩家或选定对手、数据库缺失、查询失败、冲突和战斗异常均明确停止。只有正常查询确认没有有效候选，才允许既有 `no_match` 分支；存储异常不能获得安慰积分。

应用对 `started` 操作的继续执行仅开放给同 payload 的实际 SQL owner 的 `arena.purchase`、`arena.settle`，依靠与效果同事务保存的业务回执恢复。其他 action 或注入的旧服务不自动放开这条恢复路径；挑战重试若尚无业务回执且参数已变化，仍按冲突拒绝。沿用的 attached SQLite/WAL UOW 不新增断电级跨库原子保证。

## 对手与缓存

候选通过现有 `player_db.arena` 与 `game_db.user_xiuxian` 的只读 JOIN 查询，排除自己、无玩家资料及非法积分记录，避免旧的全量列表加逐人查询。两个数据库必须预先存在；查询不创建缺失数据库。

匹配优先从积分差不超过 200 的候选中按 operation seed 随机选择，否则选择最近者。查看展示最多三个最近目标。缓存保存 180 秒，每人最多三个目标，全进程默认最多 2048 个查看者，使用锁、过期清理、LRU 淘汰及脱离内部状态的返回副本。查看和带编号挑战消费同一缓存；失效玩家会重新生成一致的显示顺序。

该缓存不持久化，也不提供跨进程一致性。候选查询仍会物化有效候选列表，不声称已经获得线上性能提升或恒定内存查询。

## 保留边界

`compatibility/legacy_arena_transactions.py` 保留历史服务和结果类型，但默认兑换/挑战不回退到这些写者。排行榜与已有奖励调度的独立迁移成果不因本组而重新计算；冻结清单的旧别名描述也不扩展本组范围。

Web API 保留 `/api/v1/arena/purchase`、`/challenge-purchase`、`/settle`，认证、CSRF 和 `Idempotency-Key` 按原框架处理。功能开关不应被视为自动撤销已提交数据或恢复旧写者的保证。

## 验证状态

本组新增 `tests/test_arena_command_ingress.py`、`features/arena/tests/test_opponent_application.py` 和 `tests/test_arena_progress_contract.py`，并补充回执、迁移及恢复测试。进度门禁从真实源码验证 owner、重放先行、原始数量、共享缓存和恢复范围，并以变异用例检查回退。

主线程隔离验收：聚焦 67 项、扩大回归 197 项通过，另有 38 个 subtest；arena 源码门禁全 true、冻结完整性错误为空。具体耗时、首轮失败原因及后续队列见 `docs/full_refactor_progress.md`。没有线上性能实测。

## 用户流程

玩家兑换荣誉商品或购买挑战次数，再发起竞技场挑战并取回结算回复。兑换与挑战先查持久回执，只有未处理过的新请求才进入匹配、战斗与结算；查看对手只走候选只读查询和进程内有界缓存，不写玩家资产。

## 命令与别名

`竞技场挑战`（别名 `竞技场购买次数`）与 `竞技场兑换`（别名 `竞技场商店`）保持历史触发词；matcher 仍由 `xiuxian_arena` 注册，资格、限购和结算规则由 `ArenaApplication` 承担。`我的竞技场战绩`（别名 `竞技场战绩`、`竞技场记录`）是只读个人历史入口，不改挑战、兑换或结算规则。

## Web API

- `POST /api/v1/arena/purchase`
- `POST /api/v1/arena/challenge-purchase`
- `POST /api/v1/arena/settle`

三条路由由 `adapters/web/blueprints/arena.py` 注册，`features/arena/web.py` 只负责转发；沿用统一 write guard：`user` 权限、CSRF 与 `Idempotency-Key` 幂等回放。

## 数据模型与迁移

`arena.001` 在启动迁移中登记 `arena_feature_migrations` 版本标记，`arena.010` 为 game DB 的 `arena_purchase_operations` 补 `status` 与 `result_json`；历史无状态记录保持 `needs_reconcile`，不按数量或费用猜测成功。请求路径不执行 DDL，缺表或缺列一律拒绝。

## 事务与失败回滚

兑换与结算在 game DB immediate UoW 内完成 ledger 登记、资产写入、业务回执与审计，异常整体回滚。`started` 维持处理中、`needs_reconcile` 维持待核查；只有同 payload 的 `arena.purchase`、`arena.settle` 才允许沿业务回执继续，已拒绝回执不会被重放成成功。

## 荣誉结算回执与个人历史

每日 `reset_daily` 在 `arena_season_reward_operations` 保存 `season_key`、本次荣誉、
结算后荣誉/累计荣誉和完整 `payload`（基础荣誉、排名奖励、结算时段位与名次）。玩家侧只读
展示复用该表，不改变自动结算：

- `ArenaSeasonRewardRepository.recent_settlements(user_id, limit=5)` 在
  `read_only=True` 的 `DatabaseUnitOfWork` 中按 `season_key DESC, rowid DESC` 返回本人
  最近 N 条；缺数据库文件、缺表或读失败返回空元组，不建库、不建表、不补列。
- 正常 `payload` 解析基础/排名/总计、结算时段位与名次；旧行缺 `payload`、坏 JSON 或
  `base + bonus != honor` 时只显示总计，`details_available=False`，不猜测拆分。
- 纯模块 `features/arena/honor_settlement_view.py` 渲染【最近荣誉结算】：正常行显示
  `日期：基础 N + 排名 N = 本次 N（结算后荣誉 N）`，旧行显示
  `日期：本次 +N 荣誉（结算后 N；明细不可用）`；无记录显示“暂无荣誉结算记录”。
- `我的荣誉` 保留原有当前段位/排名/荣誉与“明日预计奖励”，在其后追加该段；最多展示
  最近 5 次。只读；不新增命令、别名、路由、配置键、表、列、迁移或索引。
- 结算时区沿用 `SystemClock` 的 UTC 业务日，展示 `season_key` 原值；不改变每日重置
  时刻、段位/排名/荣誉公式或任何奖励写入。

## 排行榜本人名次
`竞技场排行榜` 的前 10 名徽章列表不变，末尾追加本人名次行，取数走
`ArenaLimit.rank_of` -> `ArenaStateApplication.rank_of`（只读 UoW，缺库缺表返回空
`PersonalRank`，不建库、不补列）。名次与空态口径见 `docs/features/base.md` 的
「排行榜本人名次（共享合同）」。

## 商店每周限购行
`竞技场商店` 的商品行改走共享 `shop_limit_line`（口径见 `docs/features/base.md` 的
「商店每周限购行（共享合同）」）；已购数量仍取 `arena_limit.get_weekly_purchases`。
只读渲染，不改兑换与周限购判定。

## 个人挑战战绩

`我的竞技场战绩`（别名 `竞技场战绩`、`竞技场记录`）补齐玩家侧每一场挑战的可见性：
`我的竞技场` 只有累计场次/胜率，`我的荣誉` 只有每日荣誉结算，均无法回答“最近和谁打、
输赢、积分怎么变”。

- 数据源是真实结算回执 `arena_challenge_settlement_operations`：`payload` 提供
  对手、结果与挑战时间，`result_json` 提供结算状态、积分变化与结算后积分/段位；
  不新增表、列、迁移、索引或写入路径。
- 只统计 `result_json.status == "applied"` 的记录；`limit_reached`、`stamina_insufficient`、
  `state_changed` 等被拒绝回执不计入战绩。缺 `status` 的历史行按已结算处理，
  但 outcome 不在 `win/loss/draw/no_match` 内的行跳过。
- 时间窗为近 7 天且只用下界（`created_at >= start`），与其它个人流水一致；回执
  由真实挑战写入，玩家隔离条件为 `challenger_id = 当前会话 user_id`，不接受参数查询他人。
- `ArenaMatchHistoryApplication` 在 `read_only=True` 的 `DatabaseUnitOfWork` 中查询；
  缺库、缺表、缺列或读失败返回空态，不建表、不补列。对手名通过注入的
  `PlayerProfileApplication` 解析，解析失败回退 user_id，不影响聚合。
- 渲染为“近7天竞技场战绩 / 挑战 N 场（胜/负/平/未匹配）/ 积分净变化 / 最近 5 场明细”；
  空态显示“近7天没有竞技场挑战记录。”。纯渲染函数不做 I/O。
- 帮助与发现性：`竞技场帮助` 增补条目与按钮，`修仙帮助`「流水与总览」登记命令；
  manifest 增加 `CommandSpec`（含别名），命令仍由 `xiuxian_arena` 注册。

## 定时任务

本 feature 无自有任务，`features/arena/jobs.py` 不导出 job；排行榜与赛季任务仍由兼容调度器执行。

### schema 归属（全域）

竞技场的持久化全部由启动迁移建立，请求路径（命令、调度、Web、回执读取）只做校验。
`features/arena/schema.py` 是唯一契约来源：

| 库 | 表 | 迁移 |
| --- | --- | --- |
| game | `arena_challenge_purchase_operations` | `arena.002` |
| game | `arena_purchase_operations`（含 `status`/`result_json`） | `arena.003` + `arena.010` |
| game | `arena_challenge_ticket_operations` | `arena.004` |
| game | `arena_challenge_settlement_operations` | `arena.005` |
| game | `arena_season_reward_operations` | `arena.007` |
| player | `arena_weekly_rank_reduction_operations` / `..._targets` | `arena.006` |
| player | `arena`、`arena_state_operations` | `arena.008` / `arena.009` |

`features/arena/` 与 legacy `xiuxian/xiuxian_arena/` 中除 `migrations.py` 外的任何文件都不得出现
`CREATE TABLE`/`ALTER TABLE`（由定向测试钉住）。校验口径是「表存在 + 本次要读的列齐全」，
缺表或缺任一列即 fail-closed，不建表、不补列、不写资产：

- 写路径 `purchase` / `purchase_challenges` / `use_challenge_ticket` / `settle` /
  赛季 `claim` / 周降段 `reduce` 返回 `schema_missing`，不写回执、不动 `arena`、`back`、`user_xiuxian`。
- 读路径 `purchase_result` / `settlement_result` 返回 `None`，`ranking` 与 `candidates` 返回空，
  `recent_settlements` 返回空。
- 状态初始化 `ArenaStateRepository.initialize` 抛 `ArenaSchemaMissing`（`DomainError`，
  `code="schema_missing"`），命令层用 `except DomainError` 直接展示 `exc.message`。
- 调度侧：`reduce_arena_rank` 在续跑循环前短路，`reset_arena_daily_challenges` 在
  `schema_ready()` 为假时记 warning 并跳过，不再打印「发放 0 名玩家」的误导日志。

### 每周降段的 schema 归属

`竞技场每周降段` 的持久化由 `ArenaWeeklyRankApplication -> ArenaWeeklyRankRepository` 承担，
但仓储**不再在降段事务内建表或补列**。三张表全部归启动迁移：

- `arena.006` 建 `arena_weekly_rank_reduction_operations`（业务周主键的进度行）与
  `arena_weekly_rank_reduction_targets`（冻结的逐用户目标）。
- `arena.008` / `arena.009` 建 `arena` 并补齐 `score`、`rank`、`win_streak` 等列。

`reduce()` 第一步只做 `_schema_ready` 校验：`arena` 与两张回执表必须存在，且 `arena` 具备
`user_id/score/rank/win_streak`。缺表**或缺任一列**返回 `schema_missing` 且零写入，不建表、不补列。
调度入口 `reduce_arena_rank` 在进入 `while` 续跑循环之前短路该状态并记录一条 warning，
不会把 `schema_missing` 当成 `applied` 反复空转。

降段数值不变：段位表 `青铜→王者`、初始分数 `1000/1500/1900/2300/2700/3200`、
`_target()` 的下标回退、分块 `chunk_size`、CAS 条件（`score/rank/win_streak` 三元组）
与回执字段语义都与迁移前逐字一致。

## 配置项

`arena_enabled` 控制新 application 与 Web 路由是否注册，默认开启且可热重载；关闭开关不会撤销已提交的结算数据。

## 适配器差异

战斗继续复用既有 `BattleSystem` 与属性/增益处理，旧跨库事务保留在 `compatibility/legacy_arena_transactions.py`；HTTP 适配在 `adapters/web`，命令适配在 `xiuxian_arena`，feature 层不自行打开数据库连接。

## 测试与手工验收

`python -m unittest nonebot_plugin_xiuxian_2.features.arena.tests.test_arena_application nonebot_plugin_xiuxian_2.features.arena.tests.test_opponent_application -q`；`python -m unittest tests.test_arena_command_ingress tests.test_arena_progress_contract -q`。手工验收覆盖兑换幂等重放、剩余额度裁剪、无有效候选拒绝与结算回执回放。

每周降段切片：`python -m unittest tests.test_arena_weekly_rank_schema tests.test_arena_weekly_rank_reduction -q`
—— 覆盖三张表的启动建表形状与列集、未迁移库与半成品 `arena` 上 `reduce()` 返回 `schema_missing`
且零写入（含「不得 `ALTER` 补列」）、缺一张回执表即整体拒绝、历史回执行保留、
迁移后分块续跑至 `completed` 并重放、以及段位/初始分数常量未被改动。

schema 归属切片：`python -m unittest tests.test_arena_schema_ownership -q`
—— 覆盖 `arena.002`~`arena.010` 建出 `GAME_TABLES`/`PLAYER_TABLES` 的全部表列且可重跑、
请求路径无 DDL（源码合同）、四个写路径与赛季 `claim`/周降段在未迁移库上 `schema_missing`
并保持 `arena`/`back`/`user_xiuxian`/回执零变化、半成品 `arena` 不被补列、
状态初始化抛 `ArenaSchemaMissing`、读路径降级为空/`None`、应用层映射为 rejected 结果。
