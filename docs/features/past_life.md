# Past life

## 用户流程
The compatibility command remains available while the new application boundary is enabled.

## 命令与别名
The historical package owns command names during the compatibility release. New names are added only through this feature manifest.

## Web API
No new HTTP route is exposed in this migration slice. Existing URLs remain served by the legacy web adapter.

## 数据模型与迁移
Migration `legacy.past_life.001` records the slice in `game_db`; the feature-owned repository and explicit service port now own the operation boundary while legacy tables remain the compatibility data source.

### schema 归属（`past_life.002` / `past_life.003`）

前世的状态表与回执表原先由四份仓储在请求事务内 `CREATE TABLE` / `ALTER TABLE` 惰性创建。
现在启动迁移是唯一写入者，`features/past_life/schema.py` 是唯一契约来源：

| 库 | 表 | 迁移 |
| --- | --- | --- |
| player | `past_life`（`user_id` + 19 个业务字段） | `past_life.002` |
| player | `statistics."前尘往事次数"`（共享表，只补列） | `past_life.002` |
| game | `past_life_start_operations` | `past_life.003` |
| game | `past_life_choice_operations` | `past_life.003` |
| game | `past_life_final_operations` | `past_life.003` |
| game | `past_life_reset_operations` | `past_life.003` |
| game | `past_life_reset_targets` | `past_life.003` |

回执表位于 game 库，因为仓储的写事务主库是 game，只把 player 作为 `player_data` 附库读取状态；
`past_life` 与 `statistics` 在 player 库。`economy_log` 是共享账本，由 `tasks`/`dufang`/`auction`/
`admin_asset`/`sign_in` 的启动迁移各自 `IF NOT EXISTS` 建立，前世只校验、不重复声明所有权。

请求路径只校验「表存在 + 本次要读的列齐全」，缺表或缺任一列即返回 `schema_missing`
且零写入（不动 `past_life`、`statistics`、任何回执表与 `economy_log`）：

- `PastLifeStartSqlRepository.start`、`PastLifeChoiceSqlRepository.advance`、
  `PastLifeFinalSettlementSqlRepository.settle`、`PastLifeResetSqlRepository.reset_one` /
  `reset_all_create` / `reset_all_batch` 全部 fail-closed。
- 命令层对 `schema_missing` 给出「前尘数据尚未就绪，请检查启动迁移。」，不再误报
  「已有不同模式的任务」或「operation 冲突」。
- 兼容 `xiuxian/xiuxian_past_life/transaction_service.py` 保留为**显式回滚入口**，仍在自身事务内建表；
  定向测试钉住 legacy 包内只有它允许出现 DDL。

## 事务与失败回滚
Mutating calls carry an `operation_id` and are recorded in the operation ledger. Disable the feature flag or restore the pre-migration backup to roll back.

Lifecycle writes for `投胎`、`前尘选择`、终局奖励和 `重置前尘` are owned by `PastLifeApplication` repositories. Choice replay reads also use the feature repository in the default path and do not create the legacy receipt table; an explicitly injected legacy service remains compatibility-only. `前尘回忆`、`前尘往事` and static help remain read-only compatibility paths. `前尘排行` uses a read-only schema-aware projection and does not create missing tables or columns.

奖励物品目录只在实际生成终局奖励时按需构造，导入前尘模块不会预加载全量 `Items` 缓存；共享目录不在 feature 内复制或主动清空。

### 回执 payload 的紧凑编码

`past_life_choice_operations` / `past_life_final_operations` / `past_life_start_operations` 的
`payload` 列原先写入整份内部快照（选择前/后的完整状态、开局计划与事件快照）。代码里对该列的
唯一用途是**判等**（重放/冲突），玩家可见文案另存在 `response_json`。线上实测这三列合计
92.6 MB（`xiuxian.db` 的 31.9%，0.93 + 0.08 + 0.07 MB/日）。

现改为 `features/base/receipt_payload.py::build_payload` 产出的紧凑回执：
`{"v":2,"kind":...,"user_id":...,"digest":<sha256>, stage/revision/choice_idx/...}`。

- 判等由 `payload_matches` 完成：v2 比 `digest`；**旧行按原文算 sha256 比对**，
  因此升级后重放旧 operation 仍返回 `duplicate` 而不是 `operation_conflict`。
- 身份（`user_id`/`kind`）仍在回执内，`payload_identity` 供旧行 `user_id` 反解回退。
- 回放内容不变：`response_json` / `result_json` 原样保留，玩家看到的文案逐字相同。
- 取舍：不再能从旧回执恢复某一步的完整内部状态（当前进度仍在 `past_life` 表，叙事仍在
  `response_json`）。既有旧行不重写；收益只体现在后续新增行（单行 60,943 → 184 B）。

## 定时任务
No new scheduled jobs. Legacy jobs stay registered through the compatibility scheduler.

## 配置项
`past_life_enabled` controls the application boundary and defaults to true.

## 适配器差异
Command and web adapters translate transport input into the application DTO; business code does not import NoneBot or Flask.

## 测试与手工验收
Run the feature application test and the full architecture gate. Repeat the same operation ID to verify replay.

Focused lifecycle, replay, rank-read and final-history regression tests: `29 passed`.

## 灰度开关、回滚和已知限制
Legacy algorithms and schemas remain behind the repository adapter for one complete release cycle; the compatibility hit counter determines when removal is safe.

## Manifest 清单
- `command: 前尘回忆`
- `alias: 前世回忆`
- `command: 前尘帮助`
- `command: 前尘往事`
- `alias: 前世今生`
- `command: 前尘排行`
- `alias: 前世排行`
- `command: 前尘选择`
- `alias: 前世选择`
- `command: 投胎`
- `command: 重置前尘`
