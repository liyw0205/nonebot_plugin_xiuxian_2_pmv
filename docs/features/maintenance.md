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

### 每日重置明细保留（`reset_target_retention`）

`work_daily_refresh_reset_targets`（99,032 行 / 12.3 MB）与
`world_boss_daily_limit_reset_targets`（39,231 行 / 4.4 MB）都是「每个业务日 × 每个玩家」一行，
由分块每日重置写入，只服务于**当次**运行：重置按日期读取 pending 行续跑，而父操作行
（`*_reset_operations`，`business_date` + `status`）一旦是 `completed`，feature 仓储与
legacy 兼容服务都会先返回 `duplicate`（总计/已处理/变更来自父行），根本不会读 target 行。

因此只允许「很久以前、已完成运行」的明细过期，且必须同时满足：

1. `status IN ('applied','skipped')` —— `pending` 与任何未知状态一律保留（中断可续跑）；
2. `business_date` 是规范 `YYYY-MM-DD`（`GLOB` 校验）—— 非 ISO 的历史行无法与 cutoff 安全比较，保留；
3. 该日期存在父操作行且 `status='completed'` —— 缺父行（历史遗留）按「未完成」处理，保留。

父操作行（含 `total/completed/changed/skipped` 与 `reset_count`）**永不删除**，
玩家状态（`user_xiuxian.work_num`、`boss.boss_*`）不触碰。启动日志输出 cutoff、
每批删除行数、总览与用时；进度写 `data/maintenance/reset_target_retention.json`。

保留窗口固定为 **30 天**（模块常量 `DEFAULT_RETENTION_DAYS`，无新配置键）。只读实测证据
（部署库，2026-11-09）：work 保留 30 天可清理 71,544 / 99,032 行（72.2%），
boss 28,716 / 39,231（73.2%）；两者全部行都是 `applied` + 父 `completed`，非 ISO 日期 0 行，
pending 0 行。同一批数据下 14/60/90 天分别对应 86.5%/46.0%/20.4%（work）与
87.0%/47.5%/21.9%（boss），30 天是在「保留一个月人工重跑余量」与「回收约七成」之间的保守取值。

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

### 终态组队邀请保留期（`terminal_invite_retention`）

`dungeon_team_invites` 每发一次邀请留一行，终态（`expired`/`joined` 等）行只由
`UPDATE ... SET consumed_at=CURRENT_TIMESTAMP` 产生，从不删除，因此无限累积。所有业务
读者都只取 `status='pending'`：`due_invites`/`pending_for_user` 带 pending 条件；`join`
对终态行与缺失行都返回 `invite_invalid`；60 秒通知任务在 `invite_by_id` 为 None 时静默
返回，而终态行让 `expire()` 返回非 `applied`，同样静默。

因此只回收同时满足「终态」（`status <> 'pending'` 或 `consumed_at IS NOT NULL`）、
「`expires_at` 为数值」且「早于 30 天窗口」的行：

- `pending` 行永久保留（即便已过期，交给既有到期/过期流程）；
- `dungeon_team_operations` 幂等回执、`economy_log`、`messages` 与玩家可见数据一律不动；
- 缺表或缺列（旧库）直接返回 `schema_missing`，**不建表、不补列**。

事务与限制：每批 `DEFAULT_BATCH_SIZE`（500）行在单个 `BEGIN IMMEDIATE` 中删除，
整轮 `DEFAULT_BUDGET_SECONDS`（10s）预算。谓词稳定且幂等，因此**不写进度/状态文件**：
预算中断后下次启动重新选中同一批剩余可删行即可。日志只输出一行聚合口径
（cutoff_epoch、清理行数、批数、是否「下次启动继续」与用时）。保留窗口为模块常量，
不新增配置键、不新增表结构。

生命周期依据：邀请创建时 `expires_at = now + 60`（`xiuxian_dungeon/__init__.py:491`），
`join` 要求 `expires_at > now`，过期通知任务在 60 秒后运行——单条邀请可操作期最多 60 秒，
30 天窗口约为其 43,200 倍，任何真实重放/通知路径都不可能再关心该行。

测试：`tests/test_terminal_invite_retention.py`（13 项）钉住只删过期终态行、pending
永久保留、回执逐字不变、缺表/缺列不建表、二次运行零删除、批大小与预算中断后补齐、
`due_invites` 不变，以及删除前后 `invite_by_id`/`expire()` 结果等价。
