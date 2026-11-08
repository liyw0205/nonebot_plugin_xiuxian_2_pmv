# 启动期存储整理

启动钩子 `runtime._maintain_database`（受 `xiuxian_startup_database_maintenance`
控制）在迁移完成后，把 v1.2.10 / v1.2.11 之前写入的**历史回执快照**就地改写为紧凑格式。
本 feature 无玩家命令、无 Web 路由、无定时任务、无表结构。

## 用户流程

玩家无感知：该步骤只在启动时运行，改写的列玩家看不到，可回放内容
（`response_json` 与前世/世界BOSS 的 `result_json`）逐字保留。

启动日志依次输出：各任务本批 `转换/跳过/失败` 与 watermark、总览与用时、
以及两个库的 `文件字节 → 文件字节（freelist 页 → 页）`。空间回收结果单独一行，
状态为 `reclaimed` / `no_free_pages` / `no_backup` / `no_headroom` / `failed` / `already_done`。

**不会把「新增写入变小」说成「文件变小」**：日志里只有真实测得的
`文件字节 → 文件字节`；没有回收时该行数字相同，且 `vacuum_done` 保持 `false` 以便下次重试。

## 命令与别名

无。压缩只由启动钩子触发（`features/maintenance/commands.py` 显式声明 `COMMANDS = ()`）。

## Web API

无。运维通过启动日志与 `data/maintenance/receipt_compaction.json` 观测
（`features/maintenance/web.py` 显式声明 `ROUTES = ()`）。

## 数据模型与迁移

本 feature **不拥有任何表**（`maintenance.001` 是显式空迁移）；进度写在 JSON 状态文件
`data/maintenance/receipt_compaction.json`，原子写入，含每任务 `watermark/done/converted/
skipped/failed` 与一次性 `vacuum_done` 标记。

改写的列（每个任务只改一列，永不删除行）：

| 任务 | 库 | 表 | 列 |
| --- | --- | --- | --- |
| `pet_hatch` | game | `pet_hatch_operations` | `result_json` |
| `past_life_choice` | game | `past_life_choice_operations` | `payload` |
| `past_life_final` | game | `past_life_final_operations` | `payload` |
| `past_life_start` | game | `past_life_start_operations` | `payload` |
| `world_boss_refresh` | player | `world_boss_full_refresh_operations` | `payload` |

缺少表或列时该任务直接标记完成，**不在请求期或启动期建表**。

## 事务与失败回滚

每批最多 `DEFAULT_BATCH_SIZE`（200）行，在单个 `BEGIN IMMEDIATE` 中执行；`UPDATE` 以
`rowid` 加原值做条件，行被并发改写时跳过。批次提交后才推进 watermark 并原子保存状态文件，
因此进程中断只会重扫一批；转换本身幂等（已是 v2 的行跳过）。

`corrupt/unknown` 行（非法 JSON、未知版本、坏行）一律**保持字节不变**，计入 `skipped`
并输出日志，绝不删除、绝不猜测内容。

整轮有 `DEFAULT_BUDGET_SECONDS`（20s）时间预算；未完成的任务在下次启动从 watermark 继续。

启动顺序：`初始化数据库后端 → 宠物存储迁移 → 本步骤`，此时后台任务尚未启动，
写事务窗口最短；整步骤异常被 `runtime` 捕获并警告，不阻断启动。

## 定时任务

无（`features/maintenance/jobs.py` 显式声明 `JOBS = ()`）。收敛后每次启动只读状态文件即返回。

## 配置项

不新增配置键，复用既有 `xiuxian_startup_database_maintenance` 开关。批量、预算、
回收余量系数（2.2×，下限 256 MB）为模块常量。

空间回收额外遵守既有备份规则：只有 `data/backups/db_backup` 已存在至少一个 `.zip`
快照时才执行 `VACUUM`（否则状态 `no_backup`、保持待重试）。启动顺序是
「初始化后端 → 宠物迁移 → 本步骤」，此时后台任务未启动，`VACUUM` 不在任何事务内、
不与业务写并发；`VACUUM` 失败只记录 `failed` 与原因，不影响启动。

## 适配器差异

与适配器无关：只读写本地 SQLite 文件与状态文件，不投递消息。

## 测试与手工验收

`tests/test_receipt_compaction_startup.py`（13 项，隔离库）：旧行全部转换且
`response_json`/`result_json` 字节不变、玩家状态不变、操作号经真实仓储/服务仍判
`duplicate`；新旧混合只转旧行；损坏行原样保留且计入 skipped；预算为 0 时中断、
再次运行从 watermark 补齐；第二次启动 0 转换；缺表任务标记完成且不建表；
空间测量（freelist/reclaimable）、无备份拒绝回收、无余量拒绝回收、有余量回收后文件变小、
未转换不触发回收、一次性回收标记、缺备份时保持待重试。

`features/maintenance/tests/test_receipt_compaction_units.py`（9 项）：转换器只返回
快照列、compact 行跳过、损坏/未知版本/非字符串跳过、各任务保留身份字段。

启动端到端（隔离 data 目录，非生产）实测两轮：

    有备份+余量：game 102400 B → 98304 B（reclaimed）
    无备份：    game 102400 B → 102400 B（no_backup，vacuum_done=false，下次重试）
    第二轮：    两个库均 0 转换，文件不变

手工验收（不得在生产直接执行）：在数据副本上启动一次，确认日志出现
`回执整理 … 转换 N` 与 `回执整理文件大小 … → …`，重启后任务显示已完成。
