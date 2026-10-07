# 新手机缘领取与帮助

## 本组范围

本组只收口冻结清单中的三个真实命令：`仙途奇缘`、`仙途奇缘帮助`、`新手礼包`。不新增命令、Web route、领取规则或冻结成员，不重复计算已经迁移的每日重置。

## 默认调用链

三个 matcher -> `beg_stone_` / `beg_help_` / `novice_` -> `BegCommandApplication.execute` -> `render_beg_reply`。适配器只提取身份、构造操作 ID 并发送结果，不再初始化旧 `XiuxianDateManage`、读取资格、随机发奖或解析礼包。

默认应用惰性接收 `XiuConfig`、等级目录和礼包 `18052` 的 provider。模块导入不实例化物品目录，不提前生成配置或帮助时间快照。帮助调用只读取当前配置和时钟，不要求玩家或操作 ID，也不读取数据库；显示期限来自 `beg_max_days`，不再写死 24 小时。

## 回执与执行次序

写命令保留原操作身份：`beg-daily:{event}:{user}` 和 `novice-gift:{event}:{user}`；没有事件 ID 时仍使用 `{prefix}:{uuid}:{user}`，不承诺不同消息自动去重。

新命令先通过 `BegCommandRepository.receipt` 只读检查平台 ledger 和既有业务回执，再读取玩家快照。已完成回放不检查当前资格、目录或配置，也不重新读取时钟、抽奖或更新活动时间。ledger 校验原玩家、action、请求摘要及结果状态；历史业务回执验证原玩家和奖励字段。冲突、无效回执或缺失 schema 不当作成功，也不在请求路径建表。

新请求在玩家快照和创建时间、当前时间有效后继续。仅每日机缘调用已有 `PlayerActivityApplication.update_last_check_info_time`，然后读取配置、等级目录和奖励随机数；礼包调用读取礼包目录并生成奖励计划。最后交给现有 `BegApplication.execute`，回复只消费返回结果。回放没有物品明细的旧礼包只显示已保存的灵石和已处理状态，不从当前目录补造奖励。

## 事务与失败边界

不重写 `BegRepository.settle_daily` / `claim_novice`。现有应用在同一 game DB immediate 事务内登记 ledger、核对玩家与领取标志、写灵石和礼包库存、保存业务回执并完成 ledger/audit；中途异常回滚领取效果。

活动时间是独立的 feature 事务，不与领取奖励原子提交。每日机缘在配置失效、资格拒绝或后续结算失败时，可能已经更新活动时间；回放不会再次更新。这里不承诺所有副作用共同回滚。

已拒绝 ledger 回放仍然拒绝。`started` 保持处理中，`needs_reconcile` 保持待核查，不推测成功或自动重新结算。只有严格验证为原玩家、原 action、原请求摘要且为 `internal_error` 的 `failed` 记录，才允许调用原 writer 重试：该失败记录由原事务回滚后单独保存。此类重试会重读当前输入，并可重新抽取尚未提交的奖励，不等同于完成回执的只读重放。无效配置、无效资料等尚未进入领取事务的拒绝不是持久领取回执，条件修复后可以重试。

读取或服务异常停止当前流程，不回退旧 writer，不显示领取成功；异常日志只记录类型。消息发送不属于数据库事务，发送失败后可按原操作 ID 查回执确认结果。

## 时间与资格

保留原 writer 的两种不同日边界：每日机缘按 `(settled_at - create_time).days > max_age_days` 判断过期；礼包按 `claimed_at > create_time + timedelta(days=max_age_days)` 判断过期。这里没有新增独立 24 小时规则。

历史无时区创建时间继续按进程本地 naive 时间解释；有时区的当前时钟先转换到本地再去掉时区，与历史值比较。有时区的创建时间则按其时区比较。未来创建时间和无法解析的资料拒绝，不借迁移改写存量时间。等级上界仍为排他边界，轮回灵根、宗门限制及库存上限仍由现有 writer 验证。

## 迁移与每日重置

`beg.001` 在启动阶段创建两类领取业务回执，平台启动迁移提供 ledger/audit。只读命令 repository 校验所需结构，不执行 DDL；默认请求不通过旧事务 facade 创建表。

既有 `daily_reset_beg` -> `BegApplication.reset_daily_claim_flag` -> `BegDailyResetSqlRepository` 保持不变。按业务日期生成操作 ID，同日重放不清除新领取标志，次日使用新的重置身份。本组没有更换 scheduler、领取标志存储或业务日期来源。

## 验证范围

新增命令入口、只读 repository 和进度契约测试；覆盖回放先于当前输入、拒绝不误报成功、缺 schema 不建库/建表、回滚后重试、动态帮助和旧操作 ID。进度门禁绑定三个真实 handler、owner、只读回执/快照、原事务 writer 及 renderer，并用定向源码变异防止绕过。

本说明不预报测试通过数量或线上性能收益；实际结果由主线程统一验收记录。配置开关或代码回退不能撤销已提交奖励，也不替代数据库备份和对账。

## Manifest 清单
- `command: 仙途奇缘`
- `command: 仙途奇缘帮助`
- `command: 新手礼包`
