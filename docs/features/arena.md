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
