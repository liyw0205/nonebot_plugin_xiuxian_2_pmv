# ADR-0003 多 SQLite 数据库的迁移边界
日期：2026-09-12
状态：已接受

## 背景

现有安装把玩家、交易、传承和消息数据分在多个 SQLite 文件中。一次合并会扩大
锁竞争和恢复风险，也会迫使兼容代码同时改变。

## 选项

1. 本轮立即合并为单库。
2. 保留文件边界，由 `DatabaseCatalog` 声明所有权，并以 operation ledger、outbox
   和 reconcile 处理跨库动作。

## 决策

采用选项 2。新 repository 只能写自己拥有的数据库；跨库动作必须携带同一
`operation_id`，主库先登记流水，再执行从属库步骤。失败保留
`needs_reconcile`，禁止用手工 SQL 静默修复。

## 代价与风险

跨库提交不是原子事务，需要补偿和对账；合并数据库的收益和锁模型留到有运行数据
后重新评估。

## 迁移和回滚

迁移前创建带校验和的全量备份，先 dry-run，再按库执行。异常时停止写入、恢复备份、
运行对账并保留失败流水。

## 影响的 feature / 数据 / API

影响 `game_db`、`player_db`、`trade_db`、`impart_db`、`message_db` 的 repository
和所有跨库资产 feature；不改变公开 API 路径。

## schema 归属（2026-11 补充）

每个库的表只由该库的启动迁移创建，请求路径只校验形状、不建表补列：

| 库 | 迁移 | 表 |
| --- | --- | --- |
| `message_db` | `message.001` | `messages`、`user_nicknames` 及其 14 个索引 |
| `game_db` | `season_rank.001` | `season_rank` 及 `idx_season_rank_query` |
| `game_db` | `economy_ledger.003` | `economy_operations` 及 `operation_id` 主键 |

`messages` / `user_nicknames` 此前只由 `xiuxian_utils/message_db.py::_ensure_message_db_schema`
在每次连接消息库时创建，并对历史库补 `messages.reference_id`；`season_rank` 此前只在
`xiuxian_utils/season_rank_service.py::ensure_season_rank_table` 里按需创建。两条迁移现在
是唯一创建者，均为 `CREATE TABLE/INDEX IF NOT EXISTS` 且可重复执行；`message.001` 先补
`reference_id` 再建索引，历史库保留全部既有消息行。

`season_rank` 需要 JOIN `game_db` 的 `user_xiuxian`/`sects`，因此归属 `game_db` 而不是
`message_db`。请求路径在未迁移时降级：`_ensure_message_db_schema` 抛
`RuntimeError("消息库数据尚未就绪，请检查启动迁移。")` 且零建表；`ensure_season_rank_table()`
返回 `False`，写路径返回 `None`、读路径返回 `[]`/`None`。

`economy_operations`（`xiuxian_utils/repositories.py::transfer_stones` 的转账幂等回执）
归 `game_db`：它和 `user_xiuxian` 在同一个 legacy 主库事务内写入，迁移 `economy_ledger.003`
用 `CREATE TABLE IF NOT EXISTS` 建出 `operation_id` 主键、`operation_type` 与 `created_at`。
请求路径只经 `infrastructure/database/economy_operations.operations_ready()` 校验；
未迁移或缺列时 `transfer_stones` 返回 `False`，零扣款、零入账、零回执，也不建表。
