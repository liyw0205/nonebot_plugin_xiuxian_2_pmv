# Body cultivation compatibility

## 用户流程
The compatibility command remains available while the new application boundary is enabled.

## 命令与别名
The historical package owns command names during the compatibility release. New names are added only through this feature manifest.

本包命令：`炼体帮助`、`炼体结算`、`灵石炼体`、`炼体突破`、`我的炼体`（别名 `炼体状态`）、
`冲窍`、`我的体窍`、`炼体境界`、`炼体药浴`（别名 `药浴`）、`我的药浴记录`（别名 `药浴记录`）。

### 炼体药浴生命周期（时段 / 剩余 / 记录）
`炼体药浴` 的完整生命周期由 `features/tianti_training/presentation.py` 的纯函数承载：

- `bath_schedule(config, now)` / `bath_slot_remaining(slot, now)`：读取 `MEDICINE_BATH_TIME_CONFIG`
  全部时段并标注当前时段（跨零点时段如 23→5 正确环绕），`炼体药浴` 无参数时列出时段表与
  本时段剩余时间；`bath_remaining_text(end_time, now)` 用于开启回执的剩余分钟。
- `medicine_bath_active(data, now)`：**生效判定的唯一来源**，被 `tianti_training` 的
  `apply_bath`（`bath_active` 拒绝）、`tianti_settlement` 结算与 `sect_fairyland` 领取三处共用；
  四处此前各自内联 `end_time/effect > 1` 判定，现已合并。
- `bath_status(data, now)` / `render_bath_status`：`我的炼体` 的三态显示——生效中（名称、效果、
  剩余分钟）／已过期（上次药浴名称与到期时刻）／从未开启（沿用「药浴：无」逐字）。
  `render_bath_remaining_suffix` 给 `bath_active` 拒绝文案追加当前剩余分钟。
- `我的药浴记录`（`features/tianti_training/bath_history.py`，只读
  `tianti_medicine_bath_operations`）：近 7 天（只用下界）本人药浴次数、累计消耗药材份数与
  种类数、最高结算效果，最近 10 次含消耗清单与有效至；缺库/缺表/缺列/坏 JSON 返回空态，
  不建表不补列。不改药浴消耗、效果公式、结算或任何写路径。

## Web API
No new HTTP route is exposed in this migration slice. Existing URLs remain served by the legacy web adapter.

## 数据模型与迁移
Migration `legacy.tianti.001` records the slice in `game_db`; the feature-owned repository and explicit service port now own the operation boundary while legacy tables remain the compatibility data source.

## 事务与失败回滚
Mutating calls carry an `operation_id` and are recorded in the operation ledger. Disable the feature flag or restore the pre-migration backup to roll back.

## 定时任务
No new scheduled jobs. Legacy jobs stay registered through the compatibility scheduler.

## 配置项
`tianti_enabled` controls the application boundary and defaults to true.

## 适配器差异
Command and web adapters translate transport input into the application DTO; business code does not import NoneBot or Flask.

## 测试与手工验收
Run the feature application test and the full architecture gate. Repeat the same operation ID to verify replay.

## 灰度开关、回滚和已知限制
Legacy algorithms and schemas remain behind the repository adapter for one complete release cycle; the compatibility hit counter determines when removal is safe.

## Manifest 清单
- `command: 我的体窍`
- `command: 我的炼体`
- `alias: 炼体状态`
- `command: 我的药浴记录`
- `alias: 药浴记录`
- `command: 炼体境界`
- `command: 炼体帮助`
