# Impart cards

## 用户流程
The compatibility command remains available while the new application boundary is enabled.

## 命令与别名
The historical package owns command names during the compatibility release. New names are added only through this feature manifest.

## Web API
No new HTTP route is exposed in this migration slice. Existing URLs remain served by the legacy web adapter.

## 数据模型与迁移
`legacy.impart.001` records the feature boundary in `game_db`. The application ledger stays in `game_db`, while `ImpartRepository` is wired to the actual `xiuxian_impart.db`. `impart.002` creates the prayer replay table in `game_db`, and `impart.003` prepares the three prayer statistics columns in `player_db`. For love sand, `impart.004` creates the replay table in `game_db`, and `impart.005` prepares its two statistics columns in `player_db`. The `20005` use handler keeps random card selection at the adapter boundary; `ImpartPrayerSqlRepository` atomically consumes the item, updates cards and bonuses, increments those statistics, and records replay state across the attached game/impart/player databases. The `20016` handler keeps reward rolls at the adapter boundary; `LoveSandSqlRepository` consumes the item, grants crystals, increments statistics, and stores replay state across the same databases. Legacy rows remain the compatibility data source.

## 事务与失败回滚
Prayer and love-sand requests carry an `operation_id`; asset changes, player statistics, and replay data commit or roll back together. Replays return the first result without incrementing statistics again. The request path does not create or alter schema and rejects requests while required startup migrations are missing. Disable the feature flag or restore the pre-migration backup to roll back.

### 思恋结晶精确存储

`impart.010` 为 game DB 中的流沙/管理员回执补齐精确字段；`impart.011` 为 impart DB
中的余额、祈愿/分解回执补齐精确字段，并建立旧工具操作回执。`stone_num_exact TEXT`
是权威余额；旧 `stone_num` 仅作兼容镜像，不能因其 INTEGER affinity 把超 int64 十进制
文本转换为 REAL，就把镜像误当精确值。UoW 与 legacy backend 均注册统一整数比较/加减
UDF。全部结晶写入者同步写 shadow；仅更新旧镜像的写入会被数据库触发器拒绝。

迁移保留原始余额，不删除或合并重复行。旧 INTEGER、可解析为整数的科学计数法 TEXT，
以及绝对值小于 `2**53` 的整数 REAL 可回填精确十进制 TEXT。达到 `±2**53` 的 REAL
无法证明未发生历史舍入，标为 `precision_lost_real`；非整数或无效值标为
`invalid_numeric_value`。这些记录结算时分别保留 `crystal_precision_lost_real` /
`crystal_invalid_numeric_value` 与主体，不扣费、不发奖励、不写成功回执，不自动覆盖。

同角色历史多行是镜像，不是可累加余额。一致镜像在同事务中全部更新；真实余额分歧仍
保留 `impart_rows_stone_num` 并完整回滚。祈愿的永久 operation 回执与扣费、卡片、统计
同事务提交；相同请求只回放，不再次扣费。旧兑换工具的背包授予、余额清零及回执也已
移入同一 UoW；可显式传入 operation ID 重放。管理员批量汇总使用精确 JSON 回执，
不从旧 REAL 镜像统计总额，并保留被拒绝目标的 reason/subject。

无法恢复精度或真实分歧的生产记录需要管理员另行核对：先按部署规范停机/冷备份，
在隔离副本逐条对照可证明原值的备份或流水，区分角色身份与重复镜像，不相加、不任选
一条覆盖、不重新领取。没有可信原值时保持拒绝。本版本不提供猜测余额或自动修账。

## 定时任务
No new scheduled jobs. Legacy jobs stay registered through the compatibility scheduler.

## 配置项
`impart_enabled` controls the application boundary and defaults to true.

## 适配器差异
Command and web adapters translate transport input into the application DTO; business code does not import NoneBot or Flask.

## 测试与手工验收
Run the feature application test and the full architecture gate. Repeat the same operation ID to verify replay.

## 灰度开关、回滚和已知限制
Legacy algorithms and schemas remain behind the repository adapter for one complete release cycle; the compatibility hit counter determines when removal is safe.

## 列表检索
`传承背包 [关键词|页码]`：卡片列表按卡名筛选（如 `传承背包 画屏春`），第 1 页的种类/总数
与「筛选：「关键词」」行按命中集合统计，翻页提示与按钮携带关键词（`传承背包 画屏春 2`）。
解析与续查走 `docs/features/base.md` 的「列表检索与翻页续查（共享合同）」；卡名排序沿用
既有 `(-数量, 卡名)` 规则，不改抽卡、合成、分解或思恋结晶数值。

## Manifest 清单
- `command: 传承信息`
- `command: 传承分解`
- `command: 传承卡图`
- `alias: 传承卡片`
- `command: 传承合成`
- `command: 传承帮助`
- `command: 传承抽卡`
- `command: 传承祈愿`
- `command: 传承背包`
- `command: 加载传承数据`
- `command: 虚神界帮助`
