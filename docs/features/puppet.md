# 灵田傀儡资产操作

## 用户流程

购买灵田傀儡后可升级傀儡等级。购买、升级、开启/关闭状态写入、状态查询和 scheduler 用户分页读取由 `PuppetApplication` 承接；自动收取经 `PuppetApplication.harvest -> PuppetHarvestSqlRepository` 在跨库单事务内完成，命令层只负责渲染结果。

自动收取由每小时 `auto_harvest` job 触发：它以 `rowid` 高水位分页遍历 `user_xiuxian.puppet_status=1` 的玩家，逐个结算成熟灵田。收取成功会按灵田产出发放药材、扣除当级驱动灵石、推进 `收取时间` 并写入幂等回执；灵石不足时把 `puppet_status` 置 0（与旧实现一致，避免每小时空转）；其余未完成状态在玩家日志与返回值中都有明确文案，不再静默丢弃。

## 命令与别名

旧命令保持不变：`购买灵田傀儡`、`购买傀儡`、`灵田傀儡购买`、`灵田傀儡升级`、`傀儡升级`、`升级傀儡`。

## Web API

- `POST /api/v1/puppet/purchase`：字段 `user_id`、`stone_cost`。
- `POST /api/v1/puppet/upgrade`：字段 `user_id`、`upgrade_costs`、`max_level`。

写接口要求 `user` 权限、CSRF 和 `Idempotency-Key`，返回统一 operation outcome；重复请求只回放首次结果。

## 数据模型与迁移

`puppet.001` 写入 `game_db.puppet_feature_migrations`；`puppet.002` 在 game DB 启动迁移中为 `user_xiuxian.puppet_status` 补列，保留既有状态且不在请求路径执行 DDL。开关状态与 operation ledger/audit 在同一 game DB immediate UoW 提交。scheduler 先固定 `rowid` 高水位，再按最多 200 行分页读取启用用户，避免一次性将全体 user ID 装入内存；每个页面查询结束后关闭只读连接。

自动收取的列合同（`mix_elixir_info`，player DB）：

- `收取时间`：上次收取时刻，`%Y-%m-%d %H:%M:%S` 文本，与 `SystemClock` 同口径（UTC）。**它是唯一的时间锚点**，缺失或不可解析时拒绝收取，不补算、不猜测。
- `灵田傀儡`：**傀儡等级**（1/2/3；0 表示未拥有）。它不是开关——开关在 `user_xiuxian.puppet_status`。仓库曾误读一个不存在的 `傀儡等级` 列，SQLite 把该标识符解析成字符串字面量并导致收取抛错；`tests/test_puppet_harvest_production_path.py` 用真实 schema 钉住这一点。
- `灵田数量`、`收取等级`、`药材速度`：分别参与产出数量与成熟窗口（见下）。

历史傀儡等级/收取时间仍由 player DB `mix_elixir_info` 承载；`puppet_harvest_operations` 保存 operation ID、玩家、奖励 JSON 与灵石消耗，作为幂等回执。

## 事务与失败回滚

自动收取在 game DB 的单个 `BEGIN IMMEDIATE` 事务内 `ATTACH` player DB，校验玩家、洞天福地、开关、成熟窗口、驱动灵石与背包容量后，原子推进 `收取时间`、扣灵石、入包并写回执；任何一步失败整体回滚，不留半笔资产变化。

口径与 `我的灵田` 展示及兼容实现同源，三处不得漂移：

- 成熟窗口：`interval = max(时间成本 × (1 - 加速基数 × 药材速度), 0)`，`elapsed >= interval` 才算成熟；未成熟返回剩余小时数，不抛错。
- 产出数量：`灵田数量 + 收取等级 + 收获加成`（加成来自传承与功法）。
- 驱动灵石：按 `灵田傀儡` **自己的等级**取 `PUPPET_CONFIG[level]["harvest_cost"]`；等级无配置时拒绝收取而不是免费发放。
- 时间基准：`收取时间` 是无偏移文本而运行时钟是 aware UTC，仓储在比较前把 aware 实例归一为 naive UTC；naive 调用方的语义不变。

时间基准与状态码：未完成状态（`not_ready`、`puppet_disabled`、`blessed_spot_missing`、`inventory_full`、`player_info_missing`、`invalid_harvest_time`、`invalid_puppet_level`、`harvest_time_changed`、`reward_missing`、`stone_insufficient`、`user_missing`）都会在命令层渲染成明确结果；除 `puppet_disabled`（每小时遍历的常态，不打扰玩家）外，其余都会写入玩家日志。`duplicate` 回执附带"已经处理"提示，且不重复发放。

若玩家库在最终提交后才发生故障，operation 行与资产同事务，不会出现"扣了灵石没给药材"或反之。

## 定时任务

`auto_harvest` 仍注册在兼容调度器上（job id 与摘要不变），拥有 `coalesce=True`、`max_instances=1` 和 `misfire_grace_time=300`；启用用户分页有上界。收取的事务边界未因本片扩展，也不新增 job。

## 配置、灰度与回滚

`puppet_enabled` / `XIUXIAN_PUPPET_ENABLED` 默认开启。关闭后新 application/Web 路由不注册，旧命令兼容入口可按发布策略继续提供回滚路径。

## 测试与验收

- `python -m unittest nonebot_plugin_xiuxian_2.features.puppet.tests.test_puppet_application -q`
- `python -m unittest tests.test_puppet_operation_service tests.test_source_quality -q`
- `python -m unittest tests.test_puppet_harvest_production_path -q`
- `python -m compileall -q nonebot_plugin_xiuxian_2 tests`

## 配置项

`puppet_enabled` / `XIUXIAN_PUPPET_ENABLED` 控制新 application、命令和 Web 边界，默认开启。

## 适配器差异

跨库事务只存在于 feature repository；命令和 Web 层不直接访问数据库。`LegacyPuppetRepository` 仅是显式兼容回滚边界，默认组合根不构造它。

## 测试与手工验收

覆盖购买、升级、余额拒绝、状态冲突、幂等重放，以及真实 player schema + 生产奖励工厂 + aware UTC 时钟组合下的自动收取（成功、未成熟、未开启、灵石不足关闭傀儡、背包满拒绝、等级 2/3 计价、重复 operation 不重发）。使用 Flask client、窄屏页面和恢复演练验收。

## 灰度开关、回滚和已知限制

关闭开关后旧命令兼容入口继续工作；灵田读模型仍待后续迁移。自动收取的旧实现（`PuppetHarvestService`）保留在 compatibility 模块作为显式回滚路径，其入包语句带 `create_time/update_time` 而 feature 仓储只维护数量与名称——两实现在玩家可见结果上一致，列级差异记录在此而不是当作缺陷。收取不做跨库 WAL 崩溃原子性承诺（与既有 attached SQLite 事务边界相同）。

## Manifest 清单

- `command: 购买灵田傀儡`
- `alias: 灵田傀儡升级`
- `route: POST /api/v1/puppet/purchase`
- `route: POST /api/v1/puppet/upgrade`
- `config: puppet_enabled`
- `migration: puppet.002`
