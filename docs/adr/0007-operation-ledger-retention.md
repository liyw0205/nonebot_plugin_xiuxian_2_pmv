# ADR-0007 Operation ledger 与审计流水保留周期
日期：2026-09-12
状态：已接受

## 背景

资产动作需要幂等重放、失败对账和审计追踪；过早清理会让恢复和兼容周期失去证据。

## 选项

1. 固定短期 TTL 自动删除流水。
2. 至少保留到对应数据库备份过期，并在对账完成后由运维归档；插件不自动删除。

## 决策

采用选项 2。`operation_ledger`、`operation_audit` 和 `domain_outbox` 与数据库备份
一起纳入恢复范围。归档必须保留 operation/action、状态、请求摘要、结果、审计分类
和时间，不保留秘密或未脱敏用户标识。

Dufang 的 `dufang_bets`、bet/payout operation、`dufang_bet_resolutions`、share
operation/progress、player projection receipts 和 `dufang_player_outbox` 全部属于
在线幂等或恢复状态，默认不设 TTL、不自动删除。`sent` outbox 仍保留投影 payload；
player receipt 是防止重复统计的 tombstone；已完成 share progress 仍提供历史 replay
结果。只有独立归档格式同时保留恢复与查询能力，并经审批验证后，才允许把终态数据移出
在线库；pending/dead outbox、pending bets、非终态 ledger 和未完成 share progress
始终禁止归档/删除。`economy_log` 与 `operation_audit` 在外部消费者和审计期限得到确认
前同样在线保留。

新建的 `avatar_operation_receipts` 也是身份切换的幂等 tombstone，与 `player.db.avatar`
同库提交；默认不设 TTL 或自动清理，归档前必须证明旧命令重放窗口已关闭。

容量不足时，写入前预检应拒绝新的备份/批量迁移并报告所需空间，不得隐式删旧备份、清理
业务回执、checkpoint 或运行 `VACUUM`。运营方应先扩容/调整备份目标；确需归档时，走本 ADR
规定的备份、clean reconcile、审批、校验清单和恢复演练流程。

通用备份与恢复在创建目标目录或替换目标文件前检查目标文件系统容量，估算数据库快照使用
`page_count * page_size`，普通附加文件使用文件长度；最低保留 `max(64 MiB, 估算写入量的
10%)` 空间。容量探测失败按拒绝写入处理。该预检不能消除并发写入造成的容量竞态，逐文件原子
替换及失败清理仍须保留；恢复必须先验证整份 manifest 和所有校验和，再统一做容量检查。

## 代价与风险

流水会持续增长，需要监控容量并由运维执行经过审批的归档；归档失败不能阻塞业务写入。

## 迁移和回滚

归档前先备份并确认 `ReconcileService` 报告 clean；恢复时连同流水和 outbox 一起
恢复，禁止手工删除失败记录。

Dufang game/player 数据必须作为一个恢复集合验证；在没有可审计的配对备份和投影修复
证据前，不得仅归档其中一侧的 receipt/outbox。

## 影响的 feature / 数据 / API

影响所有资产 application、`game_db` 公共治理表、对账 CLI/API 和备份清单；不改变
业务响应字段。
