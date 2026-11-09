# 当前项目状态

最后核验：2026-11-10。本文是唯一活跃进度入口；旧重构文档仅作为历史记录，不再定义当前分支或禁止事项。

## 已完成修复：宠物指令失败不再静默（v1.2.16）

同一份日志（`xiu2.log-20261103-033001.gz`）在第 7 次撞号之外还留下 7 条
`Running Matcher(type='message', module=...xiuxian_pet, lineno=93) failed`——`lineno=93`
即 `砸蛋`；玩家**一条回复都没收到**。`d64ce230` 只搬掉「撞号」这一个诱因，
`xiuxian_pet/__init__.py` 的 12 个 `pet_application.*` 调用点（10 个 handler）仍无任何兜底。

- 新增 `xiuxian/xiuxian_pet/failure_reply.py`：`failure_text()` 把
  `ConflictError`/`ValidationError`/`DomainError`/数据繁忙/其他异常映射成四种固定文案
  （繁忙判定在 `is_database_busy` 之外补了驱动消息兜底，因为调用方重新抛出的
  `OperationalError` 没有 `sqlite_errorcode`）；`guard_pet_handler(action)` 在异常时
  用 `logger.opt(exception=exc)` 留下栈并**只回一条**不含栈/SQL/路径的提示，
  不回抛、不吞 `BaseException`（`CancelledError` 照常传播）。
- 10 个 handler（游历/游历结算/砸蛋/出战/放生/一键放生/喂食/融合/技能洗练/技能替换）
  全部套上守卫，成功路径逐字不变；`functools.wraps` 保证 nonebot 依赖注入不变
  （已用真实 `get_typed_signature` 验证 `bot/event/args` 与 `DependsInner(_command_arg)` 未变）。
- `xiuxian_back` 的批量孵蛋本就有 try/except 与「未消耗道具」文案，未改动。
- 测试：新 `tests/test_pet_failure_reply.py` 9 项（单次回复且不泄漏 `sqlite3`/`.db` 等、
  繁忙与冲突文案、成功路径零额外发送、投递失败不掩盖原异常、签名与 DI 契约、
  真实仓储异常端到端到达玩家且灵石不变、AST 合约钉住 10 个 handler/12 个调用点）。
  受影响合计 85 项通过。门禁：architecture、inventory `--check` exit 0。
- 未部署、未写生产库/日志、未打标签。

## 已完成修复：砸蛋宠物 UID 冲突（v1.2.15）

（版本说明：`v1.2.14` 标签指向 buff 自愈修复 `2ced9de4`；本项未打标签。）

只读审计 `/root/xiu2/logs/xiu2.log-20261103-033001.gz`（窗口内唯一日志，覆盖 10-26～11-03）。
1184 个 traceback 中，877 个 `FileNotFoundError`、171 个 `TypeError: 'str' object is not
a mapping`、126 个 `GroupMessageCreateEvent` pydantic 校验错误全部来自
`nonebot/adapters/qq/adapter.py`（上游 QQ 适配器），与本插件无关；插件自有帧只有两类：

- `on_compat.py:727` 空消息 `IndexError`（3 次，10-31/11-01）：`nonebot/rule.py:101`
  取 `message[0]`。该守卫（`_message_is_empty`，`57153918`）已在 develop 落地并有
  `tests/test_impart_card_bonus_and_empty_message.py` 覆盖，线上日志早于该提交。
- `player_pet_item` 的 `UNIQUE constraint failed: player_pet_item.user_id, player_pet_item.uid`
  （7 次，11-01 连续 `砸蛋10`，栈为 `xiuxian_pet/transaction_service.py:461` →
  `db_backend.py:249`）：本轮修复对象。

根因：新宠物 UID 为 `pet_<epoch_seconds>_<randint(1000,9999)>`，同一秒内砸 10 颗蛋按
生日问题约 0.5% 概率撞号；撞号的 INSERT 让整个跨库事务回滚，玩家既没扣灵石也收不到任何回复。

- `xiuxian_utils/pet_system.py` 新增 `new_pet_uid()`：保持 `pet_<数字>_<数字>` 外形，
  改为「每进程固定 token + 每秒单调序号」，同秒内不可能重复；`create_pet_instance`
  与 `_normalize_pet` 的缺省 UID 均改用它（砸蛋、批量宠物蛋、`grant_pet` 同源）。
- 三处 `player_pet_item` 写入（`features/pet/repository.py` 现代仓储、
  `compatibility/legacy_pet_transactions.py` 旧服务、`features/back/pet_egg_repository.py`
  批量孵蛋）在残余撞号时 fail-closed：只对 `UNIQUE(user_id, uid)` 返回
  `uid_conflict` 并回滚（现代仓储用 savepoint），不扣灵石、不消耗道具、不写回执；
  其他完整性错误继续抛出。
- 文案：`砸蛋` 与 `_back_op_fail_msg` 增加 `uid_conflict` 说明（「宠物编号冲突，
  本次不会扣除灵石／道具未消耗」），不再是无回复的 500。
- 测试：新 `tests/test_pet_uid_collision.py` 6 项（冻结时钟下 5000 次分配零重复、
  旧公式在同一时钟下确实重复、同一秒砸 10 颗蛋全部落库、现代仓储/旧服务/批量孵蛋
  三条写入路径各自 `uid_conflict` 且灵石与道具不变、无回执行）。
- 门禁：`check_architecture.py`、`export_refactor_inventory.py --check` exit 0；
  受影响套件 62 项 + 新套件 6 项通过。未部署、未写生产库、未读生产数据。

## 已完成优化：每日重置明细保留 + 启动期回收（v1.2.13）

仅清理「很久前已完成」的逐用户重置明细（不清理父行、pending/未知/非 ISO/缺父行保留）。

- `work_daily_refresh_reset_targets`（99,032 行 / 12.3 MB）与
  `world_boss_daily_limit_reset_targets`（39,231 行 / 4.4 MB）的每行只服务于一次分块重置；
  一旦父操作行 `status=completed`，feature 仓储与 legacy 服务均先返回
  `duplicate`（总计/已处理/变更来自父行），不会读 target 行 —— 因此很久前已完成运行的明细
  属纯死重。
- 新模块 `features/maintenance/reset_target_retention.py`，在启动维护中、回执整理后独立 try：
  只删除同时满足「`YYYY-MM-DD` 规范日期且早于 30 天窗口」、「`status IN ('applied','skipped')」、
  「该日期存在父操作行且 `status='completed'」的行；其余（`pending`/未知状态/非 ISO/缺父行）
  一律保留；父操作行（含总计/变更/`reset_count`）与玩家状态（`user_xiuxian.work_num`、
  `boss.boss_*`）永不改动。
- 500 行单事务 `BEGIN IMMEDIATE`、10s 预算、`rowid IN (SELECT ... LIMIT)` 逐批删除；
  进度写 `data/maintenance/reset_target_retention.json`（原子写）；收敛的 cutoff 下次启动
  经一次索引探针跳过，而之后才完成的日期仍被拾取。
- 只读证据（生产库，2026-11-09）：两表各行均为 `applied` + 父 `completed`，pending 0，
  非 ISO 0；30 天窗口 work 72.2%（71,544 / 99,032）可清理、boss 73.2%（28,716 / 39,231）；
  14 天 86.5%/87.0%、60 天 46.0%/47.5%。30 天是「留一个月人工重跑余量」与「回收约七成」
  之间的保守取值。
- 测试：`tests/test_reset_target_retention.py` 11 项（守卫保持/重放对等/中断续跑/第二次启动
  0 删除/缺表不建表/feature 与 legacy 重放响应逐字段不变/替换测试日期确保 never expired）。
- 并发修复了一个「过窗即永久失败」的既有测试（`test_spirit_vein_timeframe.py`：
  `_patched` 现在也钉住 `_now()`，断言强度不变）。
- 门禁：architecture（`ok=True`）、inventory `--check`、progress `--json`（`failures: None`）、
  compileall、`git diff --check` 全 exit 0；337 + 71 + 56 项通过。
- 交付：`9f7daf02` 推送 develop（run `42539333241` success）→ `--ff-only` 合入 main
  （run `42543522335` success）。未部署、未写生产库、未删父行或玩家数据。

## 已完成优化：启动期历史回执整理 + 受控一次性空间回收（v1.2.12）

v1.2.10/v1.2.11 只改变**新写入**；线上既有旧行不会自己变小，且仅改写行并不会让文件变小
（就地更新先释放页内空间）。本阶段把历史整理放进既有启动维护钩子，并给出一条有守卫的
一次性回收路径。

- 实测旧行（只读审计）：`pet_hatch_operations.result_json` 94.2 MB / 4,600 行、
  `past_life_choice_operations.payload` 79.8 MB / 1,276 行、`past_life_final` 6.8 MB、
  `past_life_start` 6.0 MB、`world_boss_full_refresh_operations.payload` 47.5 MB。
- 新 feature `features/maintenance/`（manifest/application/repository/schemas/migrations/
  commands/web/jobs + tests + `docs/features/maintenance.md`）；`runtime._maintain_database()`
  在宠物存储迁移后调用 `compact_receipts`，受既有 `xiuxian_startup_database_maintenance`
  控制；**零新增配置键、零表结构**（进度写 `data/maintenance/receipt_compaction.json`，
  原子写）。
- 五个任务各只改一列；**绝不改写** `response_json` 与前世/世界Boss 的 `result_json`，
  操作号保留，因此重放/冲突语义不变。每批 200 行单事务，`rowid` + 原值 CAS（并发写入的行
  跳过）；批次提交后才推进 watermark，进程中断只重扫一批；每轮 20s 预算，未完成的任务
  下次启动继续；已收敛的任务不再扫描。
- 损坏/未知版本/非字符串行**字节不变**，计入 `skipped` 并记日志，绝不删除、绝不猜测。
- 宠物存储迁移与回执整理互相隔离：任一步失败只警告，不阻断另一步。
- 空间回收 `infrastructure/database/maintenance.py`：`database_space()` 报文件/页/freelist/
  dbstat unused；`reclaim_space()` 仅在**本轮确有转换**、`freelist>0`、
  `data/backups/db_backup` 已有 `.zip` 备份、剩余空间 ≥ max(256 MB, 2.2×库) 时执行
  `VACUUM`；一次性 `vacuum_done` 标记；`no_backup`/`no_headroom` 保持待重试；
  `VACUUM` 在启动早段、后台任务启动前、不在事务内执行。日志输出每批转换/跳过/失败与
  watermark、总览与用时、每库`文件字节 → 文件字节（freelist 页 → 页）`、回收状态；
  **没有回收就不声称缩小**。
- 测试：`tests/test_receipt_compaction_startup.py` 13 项（旧行全转换且回放列字节不变、
  玩家状态不变、操作号经真实仓储/服务仍 `duplicate`；新旧混合；损坏行不动；预算 0 中断后
  补齐；第二次启动 0 转换；缺表不建表；空间测量；无备份/无余量拒绝；有余量回收后文件变小；
  未转换不回收；一次性标记）；`features/maintenance/tests/test_receipt_compaction_units.py`
  9 项（转换器只返回快照列、跳过 compact/损坏/未知版本、保留身份字段）。
  受影响集合：326 + 55 + 56 项通过。
- 启动端到端（隔离 data 目录，未触碰 `/root/xiu2`）：有备份+余量
  `102400 B → 98304 B (reclaimed)`；无备份 `102400 → 102400 B (no_backup,
  vacuum_done=false)`；第二轮两库均 0 转换、文件不变。
- 门禁：architecture（含新 feature 契约与文档，`ok=True`）、inventory `--check`、
  progress `--json`（`failures: None`）真实 exit 0；`compileall`、`git diff --check` 干净。
- 交付：`f9029fb8` 推送 develop（CI run `42532505693` success），`--ff-only` 合入 main，
  main CI `42532842499` success。未部署、未写生产库、未在生产 VACUUM、未删除任何回执或日志、
  未发真实消息；用户删除的四个文件保持未暂存，`.codex-*` 未提交。

## 已完成优化：存储审计第二阶段——幂等回执摘要化（v1.2.11）

只读审计（`mode=ro` + dbstat，含 `unused` 列；字节一律 UTF-8，`LENGTH(CAST(x AS BLOB))`）
之后的第二项存储修复。未写生产库、未 checkpoint、未 VACUUM、未删除任何行、未部署。

- 页级事实：`xiuxian.db` 290.1 MB（页内碎片 22.2 MB / 7.65%）、`player.db` 115.9 MB
  （4.3 MB / 3.75%）、`message.db` 23.4 MB（3.7 MB / 15.87%），全部 `freelist_count=0`
  —— 没有可回收空页，只有页内碎片（只能靠 VACUUM/REINDEX，属维护动作）。
- 按 UTF-8 字节与增长排序（宠物回执已在上阶段修完）：`past_life_choice_operations`
  79.8 MB / 0.93 MB/日、`world_boss_full_refresh_operations` 68.2 MB / 0.40 MB/日、
  `economy_log`+6 索引 27.2 MB、`past_life_final` 6.8 MB、`dungeon_explore` 10.1 MB、
  `past_life_start` 6.0 MB、`work_daily_refresh_reset_targets` 12.3 MB、
  `messages`+13 索引 22.8 MB。
- **读写归属决定分类**：四个 `payload` 列（前尘选择/开局/终局、世界BOSS全量刷新）在代码里
  只用于**判等**（重放/冲突），玩家可见文案在 `response_json`、可回放 BOSS 列表在
  `result_json`；`economy_log` 是无界流水、`messages` 是可重建缓存、两张
  `*_daily_reset_targets` 只按当日查询 —— 后三者属「保留 + 需要保留期」，本轮不改。
- 新增 `features/base/receipt_payload.py`（纯函数）：`build_payload` 写
  `{"v":2,"kind","user_id","digest":sha256(既有 canonical 快照), 少量身份字段}`；
  `payload_matches` 对 v2 比 digest、**对旧行按原文算 sha256 再比**，因此升级后重放旧
  operation 仍是 `duplicate`，不同输入仍是 `operation_conflict`；`payload_identity`
  供旧行 `user_id` 反解回退。
- 接线（每条路径的写方与读方同时改，避免两条写路径写出不同格式）：
  `xiuxian/xiuxian_past_life/transaction_service.py`（advance/start/final + `record_result`
  判等 + final 旧 user_id 反解）、`features/past_life/choice_repository.py`、
  `features/boss/world_boss_repository.py`、`xiuxian/xiuxian_boss/transaction_service.py`。
- 回放内容零变更：`response_json` / `result_json` 逐字保留；零 schema/迁移/命令/配置键。
- 实测单行：选择 60,943 → 184 B、开局 30,597 → 205 B、终局 34,804 → 253 B、
  BOSS 刷新 12,842 → 168 B；合计增长 ~1.48 MB/日 → ~0.03 MB/日。既有旧行不重写。
- 取舍（已写入功能文档）：不再能从旧回执恢复某一步的完整内部状态；当前进度仍在
  `past_life` 表，玩家可见叙事仍在 `response_json`。
- 测试：新 `tests/test_receipt_payload_compaction.py` 11 项（digest 稳定性与不含快照、
  v2 判等、旧行判等、旧候选保字节相等、身份读取；真实 `PastLifeChoiceService`
  应用/重放/冲突/旧行重放/字节上限；真实 `WorldBossFullRefreshSqlRepository`
  刷新/重放返回 bosses/冲突/旧行重放/字节上限）。受影响套件：`test_past_life*` 55、
  `test_boss*` 56、source_quality+refactor_progress+新套件 280 均通过。
- 门禁：architecture、inventory `--check`、progress `--json`（`failures: None`；
  `exit_ready: False` 仍由用户删除的冻结文件造成）真实 exit 0；`compileall`、
  `git diff --check` 干净。
- 交付：`ad2d7ccb` 推送 develop（CI run `42527316070` success）。因 main 上已有 v1.2.10
  状态记录提交、而 develop 有功能提交，两者各多 1 个提交；按「不重写历史、不强推」用一次
  普通 merge 收口 `4ca5c0ba`（仅带入状态记录），develop CI `42527671157` success 后
  `--ff-only` 合入 main，main CI `42528083863` success。
- 只报告不执行的维护建议（与上阶段一致）：`VACUUM` 可回收 22.2 MB 页内碎片（需停机+冷备份+
  ≥1× 库空间，当前 3.8 GB 余量可行）；`economy_log` 建议 180 天保留/按月归档；两张
  `*_daily_reset_targets` 的旧日期可安全清除；`messages` 建议 30–90 天保留；`backups/`
  3.0 GB 按用户策略不动。

## 已完成优化：数据库体积诊断 + 砸蛋回执冻结 ID 存储（v1.2.10）

只读诊断（`mode=ro` + dbstat，未写生产库、未 VACUUM、未部署）后实施的一项存储修复。
字节口径一律为 **UTF-8 字节**（`LENGTH(CAST(x AS BLOB))`），不用 `LENGTH(text)` 的字符数。

- 部署实际数据库：`xiuxian.db` 290,131,968 B / 70,833 页、`player.db` 115,892,224 B、
  `message.db` 23,334,912 B、`xiuxian_impart.db` 1,507,328 B、`activity.db` 925,696 B、
  `trade.db` 458,752 B；全部 `freelist_count=0`（体积全是活数据，不是删除残留），
  `journal=wal`，侧车 `message.db-wal` 3.57 MB、其余 4–62 KB。`backups/` 3.0 GB 另行保留。
- 最大对象：`pet_hatch_operations` 94.2 MB（32.5%）、`past_life_choice_operations` 79.8 MB
  （27.5%）、`economy_log`+4 索引 27.2 MB、`dungeon_explore_operations` 10.1 MB、
  `work_daily_refresh_reset_targets` 12.3 MB；`player.db` 的
  `world_boss_full_refresh_operations` 68.2 MB（58.8%）。
- **ID-only 物品存储被否**：`back.goods_name` 仅 458,192 B、`goods_type` 248,703 B
  （38,334 行，合计 0.71 MB = `xiuxian.db` 的 0.24%）。为 0.24% 引入启动期全局
  `items.json` 缓存依赖与「改名/删除/未知 ID」语义不划算，保留实例冻结名。
- 真正的冗余是**回执逐行重写整份定义**：`_normalize_pet` 设置 `pet["skill"] = pet["skills"][0]`，
  于是每宠 1.84 KB 中有 1.40 KB（37.9%）是同一份 33 键技能定义的第二份拷贝。
  修复形态：静态目录定义改为按内容去重 + 按 id 引用；`past_life` 的 `event_snapshots`
  属**必需的冻结结算快照**（历史叙事不可由目录重算），只有 `expected`/`final` 之间的
  逐字节重复（45%）是表示层冗余，留待独立阶段。
- 新增 `features/pet/receipt_codec.py`（纯函数）：每条回执 = `[stored_pet_dict, active,
  skill_ref, skills_refs, flags]` + 一张去重定义表；**保留全部宠物键**（未知字段、多技能、
  `skill != skills[0]`、非映射条目、显式 `None` 均逐字还原），只冻结技能的展示子集
  （`skill_id/name/type/scope/category/desc`），`base_power/effect/...` 由 id 重解析；
  `ensure_ascii=False` 让中文按 3 字节而非 6 字节转义；`v: 2` 带版本，旧行按 v1 读。
- **损坏回执 fail-closed**：非法 JSON、未知版本、坏行、越界引用一律 `ReceiptDecodeError`
  → 仓储/兼容门面返回 `receipt_unreadable`（`succeeded=False`，零扣费、零宠物、零新回执），
  `砸蛋` 给出明确文案「砸蛋回执损坏，本次不会重复扣除灵石；请联系管理员核对数据。」，
  绝不伪装成「成功但零宠物」，也不会因此再扣一次灵石。
- 接线：`features/pet/repository.py`（`hatch`/`get_result`/duplicate）、
  `compatibility/legacy_pet_transactions.py`（`_result_from_row`/`hatch`）、
  `xiuxian/xiuxian_pet/__init__.py`（仅新增一条失败映射）。`payload`（CAS 令牌）、扣费、
  落库与 `state_changed` 语义未改；零 schema/迁移/命令/配置键变更。
- 实测收益（真实 200 行、只读、不落库）：18,427 B/行 → 3,993 B/行（**-78.3%**），
  键集完全保留；4,600 行等效 84.8 MB → 18.4 MB；活跃日写入 ~7.06 MB → ~1.53 MB。
  既有旧行不重写（不做后台 UPDATE/VACUUM），空间只体现在后续新增行。
- 测试：新 `tests/test_pet_hatch_receipt_codec.py` 24 项（往返/多技能/未知字段/去重/
  字节缩减/损坏与未知版本显式报错/仓储 fail-closed 不二次扣费/v1 旧行/兼容门面双形态）。
  定向合计 304 项 + `features/pet/tests` 30 项全绿。
- 门禁：`check_architecture.py`、`export_refactor_inventory.py --check`、
  `check_full_refactor_progress.py --json`（`failures: None`；`exit_ready: False` 仍由用户
  删除的冻结文件造成，11 条既有 blocker）真实 exit 0；`compileall`、`git diff --check` 干净。
- 交付：`b2cf19d6` 推送 develop，CI run `42509288094` success（`quality` success、
  `build-and-release` 按分支规则 skipped），随后一次性 `--ff-only` 合入 main。未部署、
  未写生产库、未 VACUUM、未发真实 QQ 消息；用户删除的四个文件保持未暂存，
  `.codex-next-goal.md` 未提交。
- 只报告不执行的维护建议：`VACUUM`（需停机+备份+≥1× 库大小临时空间，当前 3.8 GB 余量
  对 406 MB 库可行）；`economy_log`（48,934 行/149 天）与两张 `*_daily_reset_targets`
  （138k 行）无保留期；`backups/` 3.0 GB（62 个 zip）是磁盘占用最大单项。

## 已完成修复：签到 QQ 投递走真实 Markdown + 表格日历

线上 `/root/xiu2` 只读确认：迁移后的签到走 `NoneBotMessageGateway → delivery_service.reply(reply.content)`，
`ReplyPlan.content` 里的 Markdown 被当纯文本发出（`**`、`---` 原样可见，空格补齐的日历列散开）。

- 显式 intent：`ReplyPlan` 新增 `intent`（默认 `plain`，既有调用逐字不变）；签到与补签声明
  `intent=markdown`。`adapters/nonebot/gateway.py` 对 markdown intent 的字符串计划走既有
  Markdown 投递 `reply_enhanced`（按 `xiuxian_qq_capabilities` 能力判定），失败/非 QQ Bot
  回退同一文案的纯文本（复用 `strip_md_command_links`）；媒体段与 `plain` 计划仍走 `reply`。
- 日历：`features/sign_in/calendar.py` 改为真实七列表格（表头 + 分隔行 + 按当月生成的周行），
  保留 ✓已签/·今日/—非本月 与 本月已签/连续天数；不再用空格或全角竖线拼列。
- 文案：签到/补签的 `---` 前后留空行；`docs/message_channel.md` 修正「禁止 `#` 标题」「不解析 Markdown」
  等与实际不符的说法，并链接新增的 `docs/qq-markdown-reference.md`（随仓库维护的支持语法清单：
  表格、代码块、公式、行内命令）。
- 能力配置核对：部署 `.env` 未配置 `xiuxian_qq_capabilities`，`QQCapabilityRegistry` 默认
  `markdown=True`，该投递路径默认生效；按 Bot AppID 可单独关闭（测试覆盖两种情形）。
- 测试：新 `tests/test_qq_markdown_delivery.py` `12`（markdown intent 走 enhanced、群/私聊、
  最终 Markdown 消息段含真表格、OneBot 纯文本回退、无 markdown 能力回退、发送异常回退、
  媒体段不被改写、能力默认/按 Bot 覆盖、纯度回退保留表格）；签到日历与补签套件同步改为表格断言。
  定向合计 `47`，含静态契约与 mentor 的回归合计 `338` 全绿。
- 交付：`f6863b2b` 推送 develop，CI run `42493438120`。

## 已完成目标：师徒解除清算（120% 权威传功修为，五包阶段）

缺口证据：`拜师 → 传功 → 解除 → 再拜` 无成本，解除路径不结清任何传功收益。

- P1 纯规则 `features/buff/mentor_dissolution.py`：应收 = 该配对**尚未结清**传功收益 ×120%，
  向上取整（`-(-gained*6//5)`），坏值/负数按 0；`render_dissolution_line` 输出应收、当前修为与缺口。
- P2 原子清算 `features/buff/mentor_dissolution_repository.py`：经 `AttachedDatabaseUnitOfWork`
  （attached game+player）单事务完成回执重放判定 → 权威账目重算 → 配对/境界 CAS →
  `exp >= charge` 守卫扣减 → 清关系/写冷却、重绑定、统计、历史 → 写回执；请求路径不执行 DDL，
  回执表由 `buff.016`、`statistics."师徒清退修为"` 由 `buff.017` 预建（player allowlist 已登记）。
  架构门禁未放宽：feature 不再 import 驱动层，`check_architecture.py` `ok=True`。
- 防刷不加价：账目**读不出来**（缺表/缺列/回退扫描失败）→ `ledger_unavailable`，零写入、
  关系保持，绝不按 0 放行；重复解除 `state_changed`、重放 `duplicate`、输入不同
  `operation_conflict`，均不二次扣费。
- 接线：`逐出师门` 与 `解除师徒` 共用同一清算；拒绝语义回文给出应收/当前/缺口或账目暂不可读。
- 测试：`tests/test_mentor_dissolution.py` `22`（120% 取整、配对隔离、余额不足、扣不成负数、
  重放/冲突/二次解除/境界漂移、缺表缺列、**缺账目与坏账目 fail-closed**、统计与历史、
  迁列幂等、整循环不产生净修为）。
- 交付：`9bafdf40` 推送 develop，CI run `42492407071` success。

## 已完成目标：炼体记录与突破/冲窍门控单一来源（四包阶段）

新功能（非修复）。目标卡 `.codex-next-goal.md`（local-only，未提交）。承接
`docs/features/tianti.md` 与上一阶段「炼体药浴生命周期」。

**缺口证据**：`tianti_breakthrough_operations`/`tianti_qiaoxue_operations`（player DB）与
`tianti_stone_training_operations`（game DB）三张回执行只被写入与幂等重放查询使用，
**没有任何玩家面**——玩家错过一次性回执后无法回看突破成败、冲窍开出的窍穴与灵石炼体消耗；
突破门控在 handler 与仓储各解析一遍（且「所需修仙境界」分别走 `convert_rank` 硬编码表与
`境界.json` 键序）；冲窍上限在命令辅助、仓储与 `我的体窍` 三处独立计算，
`炼体帮助` 的说明（解锁 1 个）与代码（`×3`）不一致；`我的炼体` 只列数字，不回答
「现在能不能突破、还差多少」。

- P1 `我的炼体记录`（别名 `炼体记录`，priority 10，注册在 `xiuxian_tianti`）：新增只读
  `features/tianti_training/progress_history.py`，近 7 天（只用下界）分三段读取上述三表
  （突破与冲窍在 player DB、灵石炼体在 game DB），聚合突破次数/成功/失败/累计气血消耗、
  冲窍次数/累计气血/已开数、灵石炼体次数/累计灵石/累计气血，各取最近 10 次明细；
  缺库/缺表/半迁移表/坏 JSON/读失败只让该段消失，**不建表不补列**；`user_id` 只取会话身份，
  handler 不解析 `args`。
- P2 突破门控单一来源：`presentation.breakthrough_progress(...)` 一次产出下一境界、需求气血、
  当前气血、缺口与可达性，`我的炼体` 与 `TiantiBreakthroughSqlRepository` 共用；
  `need_hp` 不可解析时 **fail-closed**（绝不显示可突破），淡出错误的缺口数字。
- P3 冲窍上限单一来源：`presentation.qiaoxue_unlock_limit(rank)` = `min(rank*3, 108)`
  （常量 `QIAOXUE_PER_RANK`/`QIAOXUE_TOTAL`）；命令辅助 `_get_qiaoxue_unlock_limit`、
  仓储 `open` 与 `我的体窍` 共用同一函数；`炼体帮助` 的冲窍说明同步为
  「每提升1个炼体小境界，累计解锁3个可开窍数量（上限108个）」（只改文案，数值未动）。
- P4 `我的炼体` 可执行下一步：`presentation.render_breakthrough_status(progress)` 在突破需求段
  后追加一行结论（`现在可以突破，发送【炼体突破】`／`暂不可突破：还差 N 炼体气血`／
  `暂不可突破：修仙境界不足（下一境需【X】对应境界）`／沿用「已达炼体最高境界」），
  既有各行逐字保留。
- 零 schema/迁移/配置键变更：不新增表、列、索引、迁移版本、配置键或 Web 路由；不改药材与
  灵石消耗、奖励数值、结算、统计与任何资产写路径。
- 帮助与文档：`炼体帮助` 登记 `我的炼体记录`（带一行说明）；`docs/features/tianti.md`
  新增「炼体记录与门控单一来源」段，命令清单与 Manifest 清单各补两条
  （`我的炼体记录`/`炼体记录`）。
- 测试：新 `tests/test_tianti_progress_history.py` `17`（三段聚合、缺段为 None、最近 10 次
  倒序与上限、7 天窗外排除与未来行计入、玩家隔离、坏 JSON 容错、缺表/缺库不建文件、
  半迁移表跳过、空 `user_id`、渲染与不变量、handler/manifest/帮助源合同；
  另有 P2/P3/P4 纯函数用例：`qiaoxue_unlock_limit` 边界（0/1/36/1000/负/None/坏值）、
  `breakthrough_progress` 四种 reason 与坏数据 fail-closed、`render_breakthrough_status`
  四态）。
- 派生计数 659→660、345→346；`tests/test_phase2_legacy_path_gate.py` 同步期望值并记录 delta
  注释，**冻结成员文件与哈希保持不变**（`git archive HEAD` 隔离快照实测
  `frozen_membership_valid: True`、`integrity_errors: []`、47/47 通过）。
- 三门禁真实 exit 0（progress `failures: None`；`exit_ready: False` 仍由用户删除的冻结文件
  造成，11 条既有 blocker）；`compileall` 与 `git diff --check` 干净。定向套件：新套件 17、
  `test_source_quality`+`test_refactor_progress`+三个 tianti 套件合计 320、
  `features/tianti_training/tests` 隔离 43、含冻结清单门禁 57 均通过。
- 交付与 CI：功能提交 `d470a584` 的 develop run `42429077343` success（`quality` success、
  `build-and-release` 按分支规则 skipped）；状态记录 `c26d4b49` 的 develop run `42429546764`
  success，随后一次性 `--ff-only` 合入 main，main CI run `42429887262` success。据此发布
  **v1.2.6**：tag workflow run `42436536309` success（`quality` + `build-and-release` 均 success），
  Release `targetCommitish` `c26d4b49dc20859232431a7b14614a4c3b389b34`，资产 `project.tar.gz`
  7,000,152 字节、sha256 `c1cf161498c0918b47bb5c219e9a28abce2c07a31efc821b78034bf5a2e9507d`、
  3,642 条目，含本轮三个新文件且不含本地目标卡/数据库/`.env`；既有 tag 未移动。
  未部署、未读写真实游戏库、未发真实 QQ 消息；用户删除的四个文件保持未暂存，
  `.codex-next-goal.md` 未提交。

## 已完成目标：炼体药浴生命周期闭环（时段 / 剩余 / 记录，五包阶段）

新功能（非修复）。目标卡 `.codex-next-goal.md`（local-only，未提交）。承接 `docs/features/tianti.md`。

**缺口证据**：`tianti_medicine_bath_operations` 由真实写路径落行并被重放查询，但
**没有任何玩家面读取它**；`MEDICINE_BATH_TIME_CONFIG` 每个时段的 `range` 字段从未被读取；
`炼体药浴` 无参数只打印当前时段前 10 味药材（无时段表、无剩余时间）；两条过期路径都会把
`medicine_end_time/_effect/_name` 静默清空，`我的炼体` 的「药浴：无」因此无法区分
「从未使用」与「上次药浴已过期」；「是否生效」判定在 presentation / `tianti_training` 仓储 /
`tianti_settlement` 仓储 / `sect_fairyland` 领取共四处各写一份。

交付内容：

- P1 时段表可见：`features/tianti_training/presentation.py` 新增纯函数 `bath_schedule` /
  `bath_slot_remaining` / `render_bath_schedule`（跨零点时段如 23→5 正确环绕）；`炼体药浴`
  无参数时列出全部时段、起止小时、有效药材（各前 10），用 `◇` 标注当前时段并给本时段剩余时间。
- P2 开启回执：新增 `bath_remaining_text`，成功回执在「有效至」后追加「剩余 N 分钟」，
  既有消耗/效果/上限提示行逐字不变。
- P3 三态状态：新增 `bath_status` / `render_bath_status`，`我的炼体` 区分 生效中（名称、
  效果、剩余分钟）／已过期（「上次药浴已结束」+ 名称与到期时刻）／从未开启（沿用
  「药浴：无」逐字）。`render_bath_remaining_suffix` 给 `bath_active` 拒绝文案追加剩余分钟。
- P4 `我的药浴记录`（别名 `药浴记录`，priority 10）注册在 `xiuxian_tianti`；新增只读
  `features/tianti_training/bath_history.py`：近 7 天（只用下界）本人药浴的次数、累计消耗
  药材份数与种类数、最高结算效果，最近 10 次含消耗清单与有效至；缺库/缺表/缺列/坏 JSON
  返回空态，不建表不补列；`user_id` 只取会话身份，handler 不解析 `args`。
- P5 生效判定单一来源：`medicine_bath_active` 被 `tianti_training.apply_bath`（`bath_active`
  拒绝）、`tianti_settlement` 结算与 `sect_fairyland` 领取三处共用，四处内联判定删除。
- 帮助与文档：`炼体帮助` 补两条条目（药浴无参数说明 + `我的药浴记录`）；
  `docs/features/tianti.md` 新增「炼体药浴生命周期（时段 / 剩余 / 记录）」段与 Manifest 清单两条。
- 零 schema/命令名之外的变更：不新增表、列、索引、迁移版本、配置键或 Web 路由；不改药材消耗、
  效果公式、抽取、结算、统计与任何资产写路径。
- 测试：新 `tests/test_tianti_bath_lifecycle.py` `18`、`tests/test_tianti_bath_history.py` `16`
  （时段边界与跨零点环绕、脏配置容错、三态状态、四处共享谓词参数化、真实回执表读写与
  玩家隔离、只用下界含未来行、缺表/缺库/缺列/坏 JSON 不建表、`request_json.plan` 回退、
  渲染与不变量、handler/manifest/帮助源合同）；`tests/test_help_discoverability.py` 的
  `EXPECTED` 扩到第九条并新增 `炼体帮助` 块（`使用炼体神物` 是既有描述行，显式列入
  `DESCRIPTIVE_ENTRIES`，不放宽反向防漂移）；`tests/test_tianti_frozen_entry_owner.py` 与
  `scripts/check_full_refactor_progress.py` 的 tianti 静态契约改钉共享 helper
  （`bath_status`/`medicine_bath_active`），并把 `tianti_level_help_handler` 切片终点收紧到
  下一个 `@tianti_` 装饰器，行号耦合改为路径级证据。
- 派生计数 658→659、344→345；`tests/test_phase2_legacy_path_gate.py` 按既有先例同步期望值，
  `test_tianti_display_commands_share_existing_read_owners` 的证据断言改为按文件路径判定
  （不钉绝对行号），**冻结成员文件与哈希保持不变**（`git archive HEAD` 隔离快照实测
  `659/345`、`frozen_membership_valid: True`、`integrity_errors: []`、`failures: None`、47/47 通过）。
- 三门禁真实 exit 0（progress `failures: None`；`exit_ready: False` 仍由用户删除的冻结文件
  造成，11 条既有 blocker）；`compileall` 与 `git diff --check` 干净。
- 交付与 CI：功能提交 `d9a02944` 的 develop run `42422642102` **failure**——仅两个静态契约
  断言（`test_progress_report_tracks_tianti_frozen_display_owners`、`test_tianti_profile_read`）
  仍钉改动前的 `_get_active_medicine_bath(data, now_t)` 与四处内联共享函数名；修正提交
  `db4a441e` 的 develop run `42423838542` success。随后一次性 `--ff-only` 合入 main。
  未部署、未读写真实游戏库、未发真实 QQ 消息；用户删除的四个文件保持未暂存，
  `.codex-next-goal.md` 未提交。

## 已完成目标：排行榜本人名次 / 商店限购行 / 出关祝福回执 / BOSS 伤害上限说明（四包阶段）

本轮沿上一阶段基线 `18115743` 连续交付四个包，全部先在 develop 实施、定向验证并推送，
再一次性普通快进合入 main。未部署、未读写真实游戏库、未发真实 QQ 消息、未打 tag。

- P1 排行榜本人名次（`8c9675b2`，develop CI run `42388880659` success）：新增共享
  `features/base/ranking_position.py`（`PersonalRank` + `personal_rank_line`）；
  四个 feature 仓储补齐 `rank_of`/`integral_rank`（并列名次 = 1 + 严格高于本人的人数、
  有有效记录去重人数、本人数值；缺库/缺表返回空 `PersonalRank`，不建表不补列）；
  六个视图（世界BOSS积分、通天塔层数与积分、历练完成与积分、竞技场积分）在前 N 名后
  追加本人名次行，零/无记录输出「你还没有…记录。」。前 N 名输出逐字不变。
  聚焦测试 `tests/test_ranking_position.py` 24 项，受影响套件 270 项通过。
- P2 商店每周限购行统一（`c439222c`，develop CI run `42394319564` success）：新增共享
  `features/base/shop_limit.py`（`shop_limit_line`/`shop_limit_parts`）：剩余/总量、
  售罄 `已售罄（N/N个）`、`limit<=0` → `不限购`；五个商店（宗门、世界BOSS、通天塔、
  历练、竞技场）全部改调该 helper。宗门商店补齐已购数量：新增只读
  `SectApplication.weekly_shop_progress`（同一张 `sect_shop_weekly_purchases` 与
  同一 `%G-W%V` 周键，缺库/缺表/跨周返回空，不建表不补列），旧库回退迁移前个人 JSON
  记录（`get_sect_weekly_purchases_snapshot`，与购买路径 `legacy_purchased` 同序）。
  不改购买校验、周限购判定、扣费、周键写入与任何回执。
  聚焦测试 `tests/test_shop_limit_line.py` 16 项，受影响套件 349 项通过。
- P3 虚神界出关祝福回执（`98e173ee`，develop CI run `42399404175` success）：新增纯渲染
  `features/impart_pk/closing_status.py::closing_settlement_lines`——有消耗渲染
  `本次消耗虚神界祝福 N 分钟，剩余 M 分钟`（applied 追加本次闭关分钟数），无消耗渲染
  `本次未消耗虚神界祝福，剩余 M 分钟`；applied 与 duplicate（重放）两条回执路径都追加
  该段，数值取自 `ImpartClosingSettlementResult` 同一字段。零 schema/数学变更：扣减、
  修为、概率、statistics 投影与 `result_json`（仍写 [exp_gain, blessing_cost,
  exp_day_remaining]）均未改，旧行重放照常渲染。
  聚焦测试 `tests/test_impart_closing_receipt.py` 14 项，受影响套件 324 项通过。
- P4 世界BOSS 伤害上限说明（`8e1106f0`，develop CI run `42399561025` success）：在
  `features/boss/schemas.py` 新增纯函数 `boss_damage_note(total, actual, max_single,
  hp_left)`——未被截断（`actual >= total`）返回空串；单次上限最紧（与剩余血量同分时
  优先）命名「单次讨伐最多削减世界BOSS最大血量的20%（本次上限 N），超出的伤害不结算」；
  否则命名「世界BOSS剩余血量只剩 N，超出的伤害无处结算」；两个上限缺失或值无法解析
  返回空串。讨伐回执三个分支（击杀、战胜化身、战败逃遁）在「实际削减」行后插入该说明，
  空说明时输出逐字不变。只读渲染：不改 `actual_damage` 公式、`max_single_damage`、
  `damage_ratio`、积分/灵石/修为奖励与结算 CAS。
  聚焦测试 `tests/test_boss_damage_note.py` 13 项，受影响套件 324 项通过。
- 文档：`docs/features/base.md` 新增「排行榜本人名次」「商店每周限购行」两段共享合同；
  `boss.md`、`tower.md`、`training.md`、`arena.md`、`sect.md`、`impart_pk.md` 各自补
  对应段落。零 schema/命令/迁移/配置键变更，派生计数保持 `645 / 331` 不变。
- 门禁与验证：architecture、inventory `--check`、progress `--json`（`failures: None`）
  在开发中与提交后均真实 exit 0（`exit_ready: False` 仍由用户删除的冻结文件导致，
  blocker 属既有）；`compileall` 与 `git diff --check` 干净；冻结 Phase-2 门禁在
  `git archive HEAD` 隔离快照 + 本轮改动中 47/47 通过。用户删除的四个文件保持未暂存，
  `.codex-next-goal.md` 与 `docs/goals/**` 未提交。
- 交付与 CI：四个功能提交（`8c9675b2`/`c439222c`/`98e173ee`/`8e1106f0`）的 develop run
  依次 `42388880659`/`42394319564`/`42399404175`/`42399561025` 全部 success；整阶段完成
  后一次性快进合入 main，main CI 与本节状态记录提交随后完成。

## CI 稳定化（2026-10-12）

- develop/main 已同步至 `0c55b1fa`：quality job 通过 `fetch-depth: 0` + `fetch-tags: true` 取得真实 tag，`test_repository_has_no_release_after_the_only_tag` 在 run 1497/1498 语境下不再复现。
- run 1498（main）暴露第二个共享缺陷：`DatabaseUnitOfWork.__enter__` 先执行 `PRAGMA journal_mode=WAL` 后才设 `busy_timeout`，且 WAL 模式转换不等锁、直接抛 SQLITE_BUSY，两个并发首次打开同一库会在进入 UOW 时互锁失败（sect 周常进度并发测试复现）。修复：`__enter__` 先设 `busy_timeout`；新增 `_ensure_wal` 只在库头不是 wal 时转换一次，并在 timeout 预算内对 busy 重试；回归测试 `tests/test_uow_attach.py::test_concurrent_first_opens_converge_on_wal_without_busy_error`。
- 工作区 `docs/refactor_phase2_legacy_path_items.json` 已由用户明确删除，保持未暂存、不提交、不推送。

## 活动中心切片进展（2026-10-14）

- 第二切片 `9ad7ee8c`（CI run 38505422761 success）：状态入口映射 `commands_for_status`、单卡聚合+玩法一行汇总、只读个人进度行、QQ 蓝字/降级合同，均限冻结清单范围。
- 时间提示切片：目标文档 `docs/goals/activity-time-hints.md`；`center.time_hint` 配置驱动倒计时（进行中"剩余 N 天/小时"、未开始"N 天后开启"、无限/已结束不显示），渲染在玩法行后、奖励行前；聚焦测试 `tests/test_activity_center.py` 6/6。非目标（阶段倒计时/提醒推送/新配置键）见目标文档。
- 签到状态行切片：目标文档 `docs/goals/activity-center-sign-line.md`；`personal_progress_summary` 复用 `overview_snapshot` 的 `sign.last_sign_date` 与结算侧同一"今日已签"语义，在进度行行首追加"签到 已签/未签"，只读、无新查询/配置键；聚焦测试 `tests/test_activity_center.py` 7/7（历史 6 项断言未改）。
- 投递合同切片：目标文档 `docs/goals/activity-center-delivery-contract.md`；`tests/test_activity_center_delivery_contract.py` 5/5 覆盖 OneBot V11 纯文本降级、QQ markdown 关闭同合同、QQ markdown 开启保留 `mqqapi://aio/inlinecmd` 原生蓝字（换行按既有约定归一为 `\r`）与降级幂等，另含 handler 接线源合同；生产代码零改动，仅修正一处测试期望；受影响 `tests/test_activity_center.py` 7/7。
- 来源级领奖入口完整目标（本轮已完成）：活动中心在同一只读 overview snapshot 上按真实资格追加战令、首领进度、首领排行快捷入口，并把阶段/排行入口收紧为实际可领；五类来源保持一键领取首项、稳定顺序和跨适配器降级合同。无新表/迁移/配置键，缺少可选状态表时保持旧库兼容；`tests/test_activity_center_claim_sources.py` 覆盖单来源、组合去重、缺少状态、旧库兼容和参数化蓝字。正式说明已写入 `docs/features/activity.md`；develop CI run `38883608353`、main CI run `38884198589` 均成功。
- 活动详情 Web 交互闭环：配置驱动的 `detail` 视图复用既有只读读模型；活动页可切换中心/详情/阶段/排行/首领视图并按需输入玩家 ID，个人状态要求现有管理权限。不新增表、迁移、配置或奖励写路径；精简旧库和缺失数据库均保持只读回退。聚焦测试 `tests/test_activity_web_state.py` 13/13；develop CI run `38941319083`、main CI run `38942373734` 均成功。正式 API 与交互合同已写入 `docs/features/activity.md`；本轮不部署。

## 主线与已交付

- `main` 是安装/正式 Release 分支。本轮开始时 `main`、本地 `develop`、`origin/main`、`origin/develop` 均指向 `2fd77d84`；该基线已包含 `c7a9d45b` 阶段/排行入口及来源级领取、活动详情交互，develop/main CI 分别由 run `38941319083`、`38942373734` 验证成功，不重做这些功能。后续合并策略：先在 develop HEAD 完成功能、CI 全绿并通过审查后才普通快进合入 main。
- `develop` 是唯一活动开发分支（`efcad54f` 起自 main 创建并推送）。合并历史：`0c55b1fa`（CI tag 抓取）、`3d7fcb84`（UOW WAL 锁）、`a21649b3`（installer tests 误报修复）已按 CI 门逐段快进。
- C1-C4 与既定重构收口已经合入 main，不重开约 2,000 个旧提交的重构盘点。
- 活动中心第一切片 `910aefbb` 及适配器降级修正 `592fdf7a` 都是 main 的祖先；字体修复 `4344796b` 已在 main。
- 旧 `refactor/full-bottom-layer` 和其他历史分支/worktree 保留，不删除或盲目合并。

## Release 与部署实况

- GitHub 当前真实 latest Release 为 `v1.0.0`，资产 `project.tar.gz`，目标 `97f43acba8dd185111d998c48d4e1cf5a069b117`。
- `v1.1.0` tag 指向 `4083da45ce865ad2e3deb8bad965668fa72d3fba`，`v1.2.0` tag 指向 `efcad54f444a593aab364ce0a0472496a99dc1d3`，两者 Release API 均返回 404（tag 存在但无对应 Release）。全部旧 tag 均不可移动；包含当前修复的新 Release 尚未发布。
- `/root/xiu2` 是目标实例。`/bin/xiu2_start` 使用 `/root/myenv` 并在 `/root/xiu2` 执行 `nb run --reload`；当前控制器是旧脚本，不具备新版安装器的 Release 参数。
- 已安装插件无 `.git` 元数据，不能证明精确 commit/tag。`card.py` 文件时间为 2026-08-20，SHA256 为 `140700ca2fbf67fbd23c7ccc9082d55bd10194d27af391c07cee3b5587fe9a8e`，与 main 字体修复不同。运行中的 screen 会话曾被观察到；部署更新/重启及当前加载版本待验。
- 数据目录 `/root/xiu2/data/xiuxian` 有五个根目录库：`message.db`、`player.db`、`trade.db`、`xiuxian.db`、`xiuxian_impart.db`，另有活动中心的独立 `activity/activity.db`。配置文件为 `.env`、`.env.dev` 和 `data/xiuxian/config.json`；`.env.dev` 含 `QQ_BOTS` 与 PostgreSQL DSN 键。值不读取、不输出。尚未做停机一致备份，因此禁止覆盖部署。
- QQ 官方机器人配置键存在不等于平台连接通过；QQ 连接状态需从本轮启动证据单独核验。官方 QQ 消息呈现/权限未做真机验收，不发送真机消息，也不将其宣称通过。
- 本轮先核实 Release 质量门和实例的真实加载/数据路径；停机冷备份并验证复制一致后，才可下载 Release 覆盖插件代码。不得运行会改写共享 `/root/myenv` 的依赖升级。

## 已完成目标：仙肆行情与关注（仙肆行情 / 仙肆关注）

新功能（非修复）。目标卡 `.codex-next-goal.md`（local-only，未提交）。

**缺口证据**：仙肆是全服唯一玩家交易市场，成交账本 `xianshi_operations` 与在售投影
`xianshi_item` 一直在真实写路径上产生数据（`xianshi_purchase_repository.py:125-138` 以
`price * quantity` 结算），但玩家侧只有逐条翻页的 `仙肆查看` 与只看自己的 `我的仙肆`；
`grep -rn "行情\|均价\|价格参考"` 在玩家模块零命中——没有按物品聚合的成交价，
也没有「等某件物品」的持久入口。

交付内容：

- 迁移 `trade.014`（game DB，启动期）：`xianshi_watchlist(user_id,goods_name COLLATE NOCASE,
  created_at, PRIMARY KEY(user_id,goods_name))` 与 `xianshi_watch_operations(operation_id
  PRIMARY KEY, user_id, action, goods_name, result, created_at)`；请求期不建表、不补列。
- 新模块 `features/trade/`：`xianshi_market_schemas.py`（窗口/上限常量、名称归一化）、
  `xianshi_watch_repository.py`（关注写路径与回执）、`xianshi_market_repository.py`
  （只读行情聚合）、`xianshi_market_application.py`（用例编排）、
  `xianshi_market_view.py`（纯渲染）。
- 命令 `仙肆关注`（别名 `关注仙肆物品`）与 `仙肆行情`（别名 `仙肆价格`），priority 5，
  `block=True`，注册在 `xiuxian/xiuxian_trade/__init__.py`；`仙肆关注` 无参数看关注列表、
  `<物品名>` 添加、`取消/取关 <物品名>` 取消；`仙肆行情 <物品名>` 查单件行情。
- 成交口径：按物品名大小写不敏感匹配；时间窗**只用下界** `created_at >= now-7d`
  （UTC/本地混用，上界会丢弃「刚刚发生」的行）；行内单价 `total_cost/quantity` 四舍五入，
  加权均价 `Σtotal_cost/Σquantity`；跳过 `quantity<=0`、`total_cost<0`、空名、空时间。
- 在售口径：`xianshi_item.price` 是单价；统计摊位行数、卖家去重（`user_id='0'` 为系统）、
  件数与最低单价；跳过 `price<=0`、`quantity<=0`。
- 失败与幂等：`added`/`removed` 写关注行与回执；`duplicate` 为同操作号重放或重复关注
  （仅写回执）；`absent` 为取消不存在或同一取消操作重放（不写回执）；`limit_reached`
  在 10 个上限时不写回执、腾出名额后同一操作号可重试；`invalid_name`/`invalid_user`/
  `operation_conflict`/`schema_missing` 均零写入。上限判定与插入是同一条
  `INSERT ... SELECT ... WHERE COUNT(*) < 10`，并发不超限；拒绝分支全部在写入前返回
  （正常返回即 commit），晚期异常整体回滚。
- 降级：缺库/缺表/读异常时按来源提示「成交数据暂不可用」/「在售数据暂不可用」，
  两者都不可用显示「行情数据暂不可用」，不建库、不建表。
- 帮助与文档：`交易帮助` 仙肆段登记两条命令（沿用该文件的 `- 命令 …：说明` 单行格式）；
  `docs/features/trade.md` 新增「仙肆行情与关注」；`docs/gameplay_notes.md` 补玩家口径。
- `features/trade/manifest.py` 追加两条 `CommandSpec`；`scripts/export_refactor_inventory.py`
  使 `docs/refactor_inventory.json` 新增 2 条 command 与 2 张表（+18 行）。
- 派生计数 650→652、336→338；`tests/test_phase2_legacy_path_gate.py` 同步期望值并记录
  delta 注释，**冻结成员文件与哈希保持不变**（隔离 worktree 实测 47/47 通过）。
- `tests/test_help_discoverability.py` 新增 `仙肆帮助` 块与两条 `EXPECTED`；
  一行说明断言放宽为「`>` 下一行或 `：` 后文本」，命令名仍要求逐字命中。
- 测试：新 `tests/test_xianshi_market_watch.py` `39 passed, 13 subtests passed`
  （`unittest discover -s tests -p 'test_xianshi_market*.py'` 39 OK）：迁移与重复迁移、
  添加/重放/重复关注/上限与解除后重试/非法名称/取消命中与未命中/取消重放/操作号冲突/
  名称归一化与大小写/玩家隔离/缺 schema 不建表；行情加权均价与最低最高、7 天窗外排除、
  只有下界（未来时间行计入）、脏行跳过、在售多卖家与系统卖家、大小写匹配、未知物品、
  缺表与缺库降级、关注列表编排与仓储异常降级；渲染全文/空态/分页外状态/九种回执文案；
  handler 源合同与 manifest 合同。受影响套件：help 10、source quality 246、
  trade 121、隔离 worktree 内 phase2 gate 47 均通过。
- 三门禁真实 exit 0（progress `failures: None`；本地 `exit_ready: False` 仍由用户删除的
  冻结文件导致，11 条 blocker 属既有），`compileall` 与 `git diff --check` 干净。
- 交付与 CI：功能提交 `a1846b66` 的 develop run `41434310002` success；`--ff-only` 合入 main
  后 main run `41435476640` success（headSha 均 `a1846b66`；`build-and-release` 按分支规则
  skipped）。未部署、未读写真实游戏库、未发真实 QQ 消息、未打 tag。
  用户删除的四个文件保持未暂存，`.codex-next-goal.md` 未提交。

## 已完成目标：签到补签闭环（修仙补签）

签到日历与连续天数早已由 `sign_in_daily` 持久化，但漏签一天只能永久断签，文档也明确写
「没有补签入口」。本目标补上完整业务动作，不改签到奖励、鸿运与签到任务。

- 新增迁移 `sign_in.005`（game DB）：`sign_in_daily` 增加 `source TEXT NOT NULL DEFAULT 'claim'`，
  `PRAGMA table_info` 探测缺列才 `ALTER TABLE`，旧行默认 claim，不重建表、不回填历史日期。
- 仓储新增 `makeup_schema_ready` / `stone_balance` / `charge_stone` / `record_makeup_daily`：
  补签回执 `stone=0`、`source='makeup'`，唯一键 `(user_id,business_date)` 兜底同日重复；
  扣款用 `stone >= cost` 守卫，不会出现负余额。
- `SignInApplication.makeup`（action `sign_in.makeup`）：只允许本月内、早于今天的日期；
  今日/未来/跨月/非法日期、已有回执、余额不足、角色缺失、缺迁移分别返回
  `today_not_allowed` / `future_date` / `out_of_month` / `invalid_date` / `already_signed` /
  `stone_insufficient` / `user_missing` / `schema_missing`，失败均不改资产；扣款与回执同事务，
  异常回滚并 `record_failure`。同一操作号重放返回首次结果、不重复扣款。
- `SignInEffects.on_makeup` 只写签到统计投影，`occurred_at` 取补签日 12:00，
  不触发鸿运与签到任务，`我的签到统计` 与日历口径一致；补签不改 `is_sign`，当天仍可正常签到。
- 命令 `修仙补签`（别名 `补签`、`签到补签`，priority 1）注册在迁移后 matcher 组：
  空参数补昨天，支持 `YYYY-MM-DD` 与本月第 `D` 天；成功回复含日期、成本、余额与本月日历。
- 配置 `sign_in_makeup_cost`（`XIUXIAN_SIGN_IN_MAKEUP_COST`，默认 `100000`）：XiuConfig、
  SettingDefinition、插件配置 schema、manifest ConfigSpec 与组合根注入同步；
  `修仙帮助` 基础段、`docs/features/sign_in.md`、`docs/gameplay_notes.md` 已更新；
  `docs/refactor_inventory.json` 增加 1 条 command 记录（+9 行）。
- 派生计数保持 650/336：新命令与 `修仙签到` 一样由迁移后 matcher 注册，不计入
  `xiuxian/` 下的 legacy 声明；`test_phase2_legacy_path_gate` 只在注释记录该原因、期望值未改。
  为实测该套件，临时从 HEAD 还原被用户删除的冻结文件后 47/47 通过，随后再次删除（保持未暂存）。
- 测试：`tests/test_sign_in_makeup.py` 17 项（成功扣款与回执 source、日历/连续天数、统计目标日、
  同操作重放、不同操作同日拒绝、今日/未来/跨月/非法日期、余额不足、无角色、缺表/缺列
  fail-closed 且不建表、`is_sign` 不被改写、同日仍可签到）。受影响套件：help 10、
  签到统计 14、legacy switch 1、source quality 246 均通过；三门禁 exit 0
  （progress `failures: None`；`exit_ready: False` 仍由用户删除的冻结文件导致）。
  本地单独 discover `features/sign_in/tests` 时 `test_sign_in_adapter` 因未初始化 NoneBot
  报 1 error，属既有隔离行为（该文件未改动，CI 全量发现通过），不计为本目标回归。
- CI：功能提交 `4ae67919` 的 develop run `41405172148` success；`--ff-only` 合入 main 后
  main run `41406084291` success（headSha 均 `4ae67919`；`build-and-release` 按分支规则 skipped）。
  未部署、未读写真实游戏库、未发真实 QQ 消息、未打 tag。

## 已完成目标：列表检索翻页续查（含宗门成员查看收口）

已交付的关键词筛选只作用于当次请求：翻页提示、MD 下一页链接与底部按钮仍只拼页码，总页数却按
筛选结果计算，于是第 2 页渲染的是**未筛选**集合的第 2 页；`宗门成员查看` 还是全包最后一处
`int(args)` 静默回退，且完全不支持关键词。

- 新增 `features/base/keyword_page.py::page_command`：有关键词时生成 `命令 关键词 页码`，
  与 `parse_page_arg` 往返一致；无关键词时输出与改动前逐字相同。
- 接入九个视图的全部翻页出口：宠物背包（`_format_pet_bag` 与 MD/纯文本构建器共 3 处）、
  饰品背包（MD/回落/文本/按钮 4 处）、我的背包、丹药背包、药材背包（各 MD 两处 `next_cmd`
  + 文本提示 + 按钮）、我的装备与药材背包详情（文本提示 + 按钮）、宗门商店（提示 + 按钮）。
- 宗门成员查看：改用 `parse_page_arg`，新增 `sect_member_utils.filter_sect_member_rows`
  按道号/职位名/境界/UID 过滤；首页职位统计只统计命中成员并标注「筛选「kw」命中 N 人」；
  MD/纯文本/footer/按钮都保留关键词；`empty_notice(keyword, "宗门成员")` 与
  「宗门目前没有成员！」分开；职位权限判定、踢人/变更快捷操作语义未改。
  `宗门管理`「成员」段改为 `宗门成员查看 [页码|关键词]`。
- 零 schema/命令变更：不新增表、列、迁移、配置键、命令或别名；派生计数保持 650/336，
  `test_phase2_legacy_path_gate` 期望不变。
- 文档：`docs/features/pet.md`、`back.md`、`accessory_package.md`、`sect.md` 记录翻页续查口径。
- 测试：新增 `tests/test_list_search_pagination.py` 25 项（解析往返、宠物/饰品真实渲染的第 1/2 页、
  九个视图源合同、成员筛选与统计、全包零静默回退）；`test_pet_accessory_search.py` 的
  `KNOWN_PENDING` 收紧为空集。受影响套件 `74 passed`（`test_list_search_pagination` 25、
  `test_pet_accessory_search` 12、`test_backpack_search` 17、`test_sect_shop_search` 10、
  `test_help_discoverability` 10）；`test_progression_source_table` 9/9。
  三门禁 exit 0（progress `failures: None`；本地 `exit_ready: False` 仍因用户删除的冻结文件）；
  `compileall` 与 `git diff --check` 干净。
- CI：功能提交 `f7974fcd` 的 develop run `41394825763` **failure** —— 宗门切片使
  `percent_exp_reward` 两个调用点从 2033/2116 漂到 2048/2131，而
  `test_progression_source_table` 仍按 (文件, 绝对行号) 作契约键（4 failures / 2 errors）。
  修复提交 `61a27e76` 把该表改为「文件 → 按源码出现顺序的契约列表」，契约（anchor/÷3/
  压制/调用点数量）未放宽，行号只留在文档（已同步 2048/2131）；develop run `41395742298`
  success，`--ff-only` 合入 main 后 main run `41396549661` success（headSha 均 `61a27e76`；
  `build-and-release` 按分支规则 skipped）。未部署、未读写真实游戏库、未发真实 QQ 消息、未打 tag。

## 已完成目标：宠物背包与饰品背包关键词筛选

`宠物背包`、`饰品背包` 是包内最后两处 `int(args)` 失败即静默回退第 1 页的分页入口（输入宠物名或饰品名只看到首页）。本目标补齐这两处，并加了全包防回归断言。

- 宠物背包：接入 `parse_page_arg`，新增 `_filter_pet_rows` 按宠物名/品种/稀有度/类型/UID 过滤，MD、纯文本与分页文本三条渲染路径共用同一过滤结果；无命中提示与空背包区分。
- 饰品背包：接入 `parse_page_arg` 与 `filter_sections`，按饰品名过滤并丢弃空段，保持原有分页与容量提示。
- 两条帮助文本补 `[页码|关键词]`；`docs/features/pet.md`、`docs/features/accessory_package.md` 记录口径。
- `tests/test_pet_accessory_search.py` 12 项通过：页码/关键词/组合/非法页码、宠物多字段命中、饰品段过滤、空关键词保序、无命中、两种提示文案、两条 handler 源合同，以及全包防回归（除已知待迁移的宗门成员列表外不得再有 `int(args)` 静默回退，并断言该待迁移项未扩散）。
- 受影响套件：pet 62、accessory 63、help 10 通过；三门禁 exit 0 / `failures: None`；`compileall` 与 `git diff --check` 干净。
- 提交 `0b32abb3` 推送 develop，CI run `41315800446` success；随后 `--ff-only` 合入 main 并核验 main CI。未部署、未读写真实游戏库、未打 tag。

## 已完成目标：宗门商店关键词筛选 + 防漂移测试去行号耦合

宗门商店只做 `int(page_input)`，非数字一律静默回退第 1 页（`宗门商店 灵芝` 显示首页商品）；同时该切片暴露了防漂移测试的脆弱设计。

- 新增通用 `features/base/keyword_page.py`：`parse_page_arg` 区分页码与关键词（纯数字才是页码，支持 `关键词 页码`），`matches_keyword`/`filter_rows`/`filter_lines` 做大小写不敏感匹配，`empty_notice` 区分「筛选无结果」与「本来就为空」。
- 宗门商店接入：按商品名/描述/编号匹配，命中后只渲染命中商品并保持每页 5 条分页；无命中给出专门提示，与「宗门商店暂无商品」分开；兑换仍走 `宗门兑换 编号`，编号语义未改；`宗门帮助`「任务与商店」段补 `[页码|关键词]`。
- `tests/test_sect_shop_search.py` 10 项：页码/关键词/组合/非法页码、名称/描述/编号命中、无命中、空关键词、两种提示文案、`filter_rows` 辅助、handler 源合同与防回归断言。受影响套件：sect 248、help 10、back+help search 27 通过。
- **CI 红灯与根因修复**：该切片给 `xiuxian_sect` 增加 5 行 import，两个 `percent_exp_reward` 调用点从 2028/2111 漂到 2033/2116，而 `test_progression_source_table` 按绝对行号断言，导致 develop run `41310099374` failure（4 failures / 2 errors，全部集中在该套件）。改为「文件 → 按源码出现顺序的契约列表」并新增调用点数量断言，行号只留在 `docs/features/xiuxian_progression.md` 供人查阅（已同步 2033/2116）；契约未放宽（anchor/÷3/压制、`current` 禁用、高频玩法必须 ÷3 全部保留）。
- 提交 `b3a2dc17`（宗门商店）、`396a528d`（测试根因修复）；develop run `41310806511` success；随后 `--ff-only` 合入 main 并核验 main CI。计数保持 650/336；未部署、未读写真实游戏库、未打 tag。

## 已完成目标：背包类视图统一关键词筛选

`我的背包` 修好后，同一类静默回退仍在四处兄弟路径上：`丹药背包`、`药材背包`、`我的装备`、`药材背包详细` 都是 `int(args)` 失败即当第 1 页，关键词被无声吞掉。本目标按「修一类而不是一处」处理。

- 五处背包视图统一走 `features/back/backpack_search.py::parse_backpack_arg`：纯数字才是页码，其余按关键词筛选，支持 `关键词 页码`；`filter_sections`/`filter_lines` 保持原排序与分页、空段整段丢弃。
- `empty_notice` 增加 `bag_name` 参数：各背包保留原有空文案（如「道友的丹药背包空空如也！」），同时把「筛选无结果」与「背包为空」区分开，不误报。
- `背包帮助`「查看」段为四条命令补 `[页码|关键词]` 用法。
- `tests/test_backpack_search.py` 17 项通过，新增三项：命名背包文案、**五处视图都接入同一解析器**、**防回归断言（不得再出现 `int(args)` 失败静默当第 1 页）**。受影响套件：back 64、help 10 通过；三门禁 exit 0 / `failures: None`；compile 与 diff check 干净。
- 未新增命令，派生计数保持 650/336；含冻结文件隔离 worktree 实跑 `test_phase2_legacy_path_gate` + 新套件 64/64（本仓因用户删除该文件无法本地评估，未恢复）。
- 提交 `e544ec11` 推送 develop，CI run `41307415995` success；随后 `--ff-only` 合入 main 并核验 main CI。未部署、未读写真实游戏库、未打 tag。

## 已完成目标：我的背包关键词筛选

`我的背包` 只做 `int(args)`，解析失败即静默当第 1 页：玩家输入 `我的背包 破境丹` 看到的是背包首页，参数被无声吞掉。本目标修正该缺陷并补上按名称/类型筛选。

- 新增 `features/back/backpack_search.py`：`parse_backpack_arg` 区分「页码」与「关键词（可带页码）」——纯数字才是页码，`0`/`-1` 之类不当页码；`filter_sections`/`filter_lines` 按名称或类型做大小写不敏感包含匹配，保持原有排序与分页、空段整段丢弃；`empty_notice` 把「筛选无结果」与「背包为空」分开，不误报。
- markdown 与非 markdown 两条渲染路径都接入筛选；`背包帮助`「查看」段补充用法（`我的背包 关键词 [页码]`）。
- `tests/test_backpack_search.py` 14 项通过：空参数、页码、关键词、关键词+页码、非法页码、名称/类型命中、大小写、空关键词保序、空段丢弃、无命中、行过滤、两种提示文案、handler 源合同。受影响套件：back 61、help 10、help search 10 通过；三门禁 exit 0 / `failures: None`；compile 与 diff check 干净。
- 未新增命令，派生计数保持 650/336；含冻结文件隔离 worktree 实跑 `test_phase2_legacy_path_gate` + 新套件 61/61（本仓因用户删除该文件无法本地评估，未恢复）。
- 提交 `0ff3ef24` 推送 develop，CI run `41304532901` success；随后 `--ff-only` 合入 main 并核验 main CI。未部署、未读写真实游戏库、未打 tag。

## 已完成目标：修仙帮助关键词检索

`修仙帮助` 早已声明 `args` 却从未使用，而仓库登记了 650+ 条命令/别名，玩家只能逐行翻整页静态文本。本目标让它支持关键词检索，不改任何命令行为、不新增命令。

- 新增 `features/base/help_search.py`：扫描 `bootstrap/platform_manifest.py` 与 `features/*/manifest.py` 的 `FEATURE.commands` 构建索引（命令名 + 别名），首次检索时缓存；匹配强度「完全相等 → 前缀 → 包含」，大小写与首尾空白归一化，最多返回 10 条。
- `修仙帮助 关键词` 走检索分支并提示收窄或改用完整菜单；不带参数时输出与原来完全一致；帮助页首行补充用法提示。
- 纯内存只读：不读库、不写文件、不改 matcher 与权限。
- `tests/test_help_search.py` 10 项通过：匹配强度排序、别名命中（`道具流水`→`我的物品流水`）、归一化、空关键词提示、无命中提示、截断提示、索引非空、关键命令可检索（修行周报/战力拆解/背包容量/修仙签到）、handler 源合同，以及**与 `plugin.build_registry()` 登记集合逐条比对**（manifest 新增命令而索引漏扫即失败）。
- 受影响套件：help discoverability 10 通过；三门禁 exit 0 / `failures: None`；compile 与 diff check 干净；派生计数保持 650/336（未新增命令），含冻结文件隔离 worktree 实跑 57/57。
- 提交 `1c6bbbcc` 推送 develop，CI run `41301498808` success；随后 `--ff-only` 合入 main 并核验 main CI。未部署、未读写真实游戏库、未打 tag。

## 已完成目标：背包容量体检（单件口径）

背包上限 `max_goods_num` 在写入前按**单件物品件数**判定（`当前件数 + 本次发放 > 上限` 即 `inventory_full`），但 `我的背包` 只列物品，玩家往往在领奖被拒后才知道装不下。本目标新增只读的 `背包容量`，不改上限、判定与写路径。

- 新增 `features/back/capacity_view.py::BackpackCapacityApplication`：只读 `back` 表按 `user_id` 聚合，输出上限、条目数、总件数、最接近上限的前 3 件（当前件数/剩余可容纳件数/占用百分比）与达到 80% 的警示条目；剩余只与该件当前件数相关，不被其他物品影响。
- 口径与 `inventory_full` 同源：任务领奖（`features/tasks/claim_repository.py`）、洞府收获/巡逻/潜入（`features/dongfu/`）、战斗结算（`features/combat_settlement/repository.py`）、宠物出行（`features/pet/repository.py`）。
- 上限由调用方注入 `XiuConfig().max_goods_num`；`PRAGMA` 探测 `back` 表与 `goods_num` 列，缺表/缺列明确降级，不建表、不补列、不写库，也不改任何 `inventory_full` 判定。
- 命令 `背包容量`（别名 `我的背包容量`、`背包空间`，priority 7）注册于 `xiuxian_back`，只取登录会话 `user_id`；`背包帮助`「查看」段与 `features/back/manifest.py` 同步登记。
- `tests/test_backpack_capacity_view.py` 10 项通过：单件口径、与 `inventory_full` 判定同值、排序与并列、80% 边界（79%/80%）、空背包、零/负行忽略、他人隔离、缺表不建表、缺列降级、命令/帮助/manifest 合同。受影响套件：help 10、back 47 通过；三门禁 exit 0 / `failures: None`；compile 与 diff check 干净。
- 派生计数 649/335 → 650/336；`test_phase2_legacy_path_gate.py` 期望同步更新，并在**含冻结文件的隔离 worktree** 中实跑 57/57 通过（本仓因用户删除该文件无法本地评估，未恢复）。
- 提交 `a5e59663` 推送 develop，CI run `41293640452` success；随后 `--ff-only` 合入 main 并核验 main CI。未部署、未读写真实游戏库、未打 tag。

## 已完成目标：战力拆解（三因子与升境预估）

战力是闭式三因子乘积（`xiuxian2_handle.py::update_power2`：`round(exp × root_rate × level_spend, 0)`），但玩家侧只有一个数字，无从判断提升优先级。本目标新增只读的 `战力拆解`，不改公式、灵根/境界数据与任何写路径。

- 新增 `features/base/power_breakdown.py::PowerBreakdownApplication`：只读 `user_xiuxian` 单行，输出修为、灵根倍率、境界系数、计算战力、账面战力与差异、距下一境所需修为、同修为突破后的战力预估，以及三项提升手段中相对增幅最大的一项。
- 舍入与 SQLite `round(x,0)` 对齐（.5 远离零），避免与账面值出现 +1 的假差异；账面与计算不一致时如实标注（提示下次刷新会校正），不改数、不写库。
- `root_rate`/`level_data` 可注入（`命运道果` 阶梯倍率走 `compute_fate_root_rate`）；`PRAGMA table_info` 探测真实列，缺 `power` 列仍可计算；未知境界、倍率不可用、缺表一律 fail-closed，不伪造战力、不建表。
- 命令 `战力拆解`（别名 `我的战力`、`战力明细`，priority 7）注册于 `xiuxian_base`，只取登录会话 `user_id`；`修仙帮助`「常用入口」与 `features/base/manifest.py` 同步登记。
- `tests/test_power_breakdown_view.py` 11 项通过：公式一致与 .5 舍入、灵根倍率线性、命运道果阶梯注入、未知境界 fail-closed、缺 power 列、缺表不建表、账面差异、升境预估与末境无预估、主导项、命令/帮助/manifest 合同。受影响套件：help 10 通过；三门禁 exit 0 / `failures: None`；compile 与 diff check 干净。
- 派生计数 648/334 → 649/335；`test_phase2_legacy_path_gate.py` 期望同步更新，并在**含冻结文件的隔离 worktree** 中实跑 58/58 通过（本仓因用户删除该文件无法本地评估，未恢复）。
- 提交 `45a2deb5` 推送 develop，CI run `41288611849` success；`--ff-only` 合入 main 后 main CI run `41289386745` success。未部署、未读写真实游戏库、未打 tag。

## 已完成目标：修行周报（跨玩法 7 天核算）

玩家此前只能单口径查看：灵石流水、物品流水、宗门贡献流水、资产总览与累计统计，缺少跨来源的周期汇总。本目标新增只读的 `修行周报`，不改 schema、写入路径与既有命令输出。

- 新增 `features/economy_ledger/weekly_report.py::WeeklyReportApplication`：单次只读 `economy_log`，汇总近 7 天修为/灵石/宗门贡献的获得、消耗、净额与笔数，物品获得/消耗件数与前 3 名（同名合并、按件数降序），活跃天数、最活跃一天与主要玩法前 3；已知动作映射中文，未知动作回退 `source·action`。
- 时间窗只用下界：`created_at` 由各写入方提供、UTC 与本地混用，加上界会误删「刚刚发生」的行（灵石流水切片已实测该陷阱）；`PRAGMA table_info` 探测真实列，旧库缺列该段为 0、缺表直接降级，不建表、不补列、不写库；`item_delta` 脏数据逐行跳过。
- 命令 `修行周报`（别名 `我的周报`、`本周修行`，priority 7）注册于 `xiuxian_base`，只取登录会话 `user_id`；`修仙帮助`「流水与总览」分组与 `features/base/manifest.py` 同步登记。
- `tests/test_weekly_report_view.py` 10 项通过：收支拆分与净额、本人隔离、未来时间行不被上界误删、窗外行排除、脏 JSON 逐行降级、缺表不建表、缺列逐段降级、零记录文案、活跃天数去重与最活跃一天、玩法分布排序、命令与帮助合同。受影响套件：help 10、economy 17、flow 52、source_quality 246 全通过。
- 派生计数 647/333 → 648/334；`test_phase2_legacy_path_gate.py` 期望同步更新，并在**含冻结文件的隔离 worktree** 中实跑 47/47 通过（本仓因用户删除该文件无法本地评估，未恢复）。
- 三门禁：architecture exit 0、inventory `--check` exit 0（已重生成 9 行）、progress `failures: None`；`compileall` 与 `git diff --check` 干净。
- 提交 `0034ecb2` 推送 develop，CI run `41284807435` success；随后 `--ff-only` 合入 main 并核验 main CI。未部署、未读写真实游戏库、未打 tag。

## 已完成目标：修为奖励口径校准（gap 锚定 + 境界压制）

核对 `docs/features/xiuxian_progression.md` 声称的 2026-10 全项目盘点是否在代码落地，按实测偏差分批修复；不改任何百分比数值、境界数据或奖励文案。

- 定位唯一真实偏差：世界BOSS 讨伐（`xiuxian_boss/__init__.py`）手写 `自身累计修为 × exp_buff/10000 × min(0.1×rank//3,1)`，属 `current` 锚定，单次奖励会随累计修为膨胀。改为 `features/boss/exp_reward.py::boss_exp_reward` 纯函数，复用 `percent_exp_reward(anchor="gap", divide_by_three=True)`，百分比与压制口径不变；新增 fail-closed 守卫，境界名未知时返回 0 而非回退 `current`。三个零奖励分支（`exp_buff<=0`、`root=="凡人"`、未获胜）保持不变。
- 历练惩罚侧（`features/training/event_resolver.py:542`）补齐 `divide_by_three=True`，与该文件奖励侧、规划侧一致。
- 按当前代码重新核验全部 16 个 `percent_exp_reward` 调用点（爬塔 4 处、宗门任务 2 处、历练 3 处、幻境、前尘轮回、魔修入侵、双修、传功、突破成功、世界BOSS），逐项记录 anchor/÷3/压制开关；另登记固定值与间接来源：闭关与灵石/修为修炼（乘法增益 + cap）、悬赏令（任务固定数值）、秘境扣减（`apply_rank_suppress=False`，扣减语义）与结算（gap + `max_exp` 上限）、签到（仅灵石）。
- 重写 `docs/features/xiuxian_progression.md` 总表并同步 `docs/gameplay_notes.md`；纠正文档事实错误：境界为「初期/中期/圆满」三段共 77 境，77 境 `stage_power_gap` 全部 > 0。
- 新增 `tests/test_boss_exp_reward_anchor.py` 9 项（gap 成正比、改变累计 exp 不影响奖励、÷3 压制、未知境界 fail-closed、三个零分支、历练两处源合同）与 `tests/test_progression_source_table.py` 8 项（未登记调用点、登记位置缺失、anchor/÷3/压制不符、`current` 锚定发放点、高频玩法缺 ÷3、文档同步），共 17/17 通过。受影响套件：boss 31、training 17、source_quality 246 全通过。
- 目标提交 `b4421bb3`、`6f4e1105` 已推送 develop，develop CI run `41275688373`、`41276510275` 均成功，随后快进合入 main 并验证 main CI。未部署、未读写真实游戏库、未打 tag。

## 已完成目标：灵庄空格容错与活动中心注入时钟

修复玩家反馈的灵庄误报与 develop CI 的时间驱动红灯，两段均先取证再改。

- 灵庄：正则 `^灵庄(存灵石|取灵石|升级会员|信息|结算)?(.*)?` 要求子指令紧跟 `灵庄`，中间有空格/全角空格时 `mode` 为 None → 走 help 分支 → 余文非「帮助」→ `invalid_argument`。收敛为 `BANK_COMMAND_PATTERN` + `parse_bank_command()`（`灵庄\s*`），matcher 引用同一常量，帮助文本补充空格与正整数金额规则；金额校验未放宽（小数点、千分位、单位、全角数字仍拒绝）。
- 活动中心：`gameplay_status_text(activity)` 丢弃传入的 `now`，内部 `activity_state` 用 `datetime.now()`，导致固定时钟测试随真实日期翻转，且同一张卡片可能出现主活动「进行中」而玩法「已结束」。新增 `at_time` 参数贯通 `activity_state`/`gameplay_status_text`/`build_activity_center_items`/阶段视图与两处玩法摘要。失败先于本次改动存在（未修改的 `b1679cf4` 上同节点同失败、实际值逐字相同）。
- 新增 `tests/test_bank_command_whitespace.py`（12 tests + 24 subtests）与两项不依赖真实日期的活动时钟合同测试；受影响套件 295 与 99 全通过。
- 提交 `4763733d`、`690e1c91`；develop CI run `41272314565`、main CI run `41272887413` 成功。未部署、未打 tag。

## 已完成目标：活动签到 Web 代办

本轮沿活动中心路线图推进到受保护的签到写交互，不重做来源级领奖入口、阶段/排行快捷入口、详情视图或已通过的 QQ/OneBot 投递合同。只允许管理员对已选玩家 ID 发起代签到；不引入未绑定 Web 身份的玩家自助写入口。

- `POST /api/v1/activity/sign-in` 以 CSRF 和 `Idempotency-Key` 保护，复用现有 `ActivityApplication -> claim_sign`。活动开关/阶段、每日唯一、奖励配置、用户存在和背包上限仍以现有业务事务为准。
- `activity_sign_settlement_operations` 与签到状态/奖励写入保持既有事务；外层 `activity.claim_sign` 留在 `started` 而签到回执已提交时可恢复为 duplicate。仅扩展该动作的恢复白名单，不放宽其他动作。
- 请求前只读检查签到、玩法事件及 operation/audit schema；缺失文件或旧 schema 返回 503，不请求期建库、建表、补迁移或写独立旧活动库。不改数据模型、奖励语义或配置。
- 管理员活动面板要求显式玩家 ID，复用幂等键重试可恢复错误，完成后刷新玩家状态。正式接口与状态合同在 `docs/features/activity.md`。
- 新聚焦套件 `tests/test_activity_sign_web.py` 7/7 通过，包含真实隔离库结算、奖励只发一次、外层收据恢复、权限/CSRF 和旧库失败关闭；ES module 语法、目标 Python 编译及 `git diff --check` 通过。
- 目标提交 `e9ebc4d4` 已推送 develop 并快进合入 main；develop CI run `38947837210`、main CI run `38948727873` 均成功。未部署、未触碰真实游戏数据。

## 已完成目标：活动中心 Web 一键代领

沿活动中心路线图推进到管理员活动面板的一键代领闭环；阶段/排行入口、来源资格、详情视图、签到 Web 代办与消息投递合同均是已完成基线，不重做。玩家本人仍通过既有 bot `活动领取` 命令领奖；本目标不新增 Web 玩家身份模型。

- Web 领奖路由与 feature manifest 收紧为 `admin`，要求 CSRF、显式玩家 ID 和稳定 operation ID；仅接受管理员代办，修正既有 `user` 权限可由普通请求任意指定 `user_id` 的边界。
- 管理员活动页复用当前玩家 ID 调用既有 `ActivityRewardApplication` 六步协调器，保持来源顺序、child operation ID、逐步回执、旧四步回执兼容回填及 bot 命令路径；同键重放稳定，部分失败续跑时不重发已完成子操作。
- 请求前只读检查 game DB 文件、`operation_ledger`、`operation_audit` 与六步协调器 schema；缺库、读错误或旧 schema 返回 503，不请求期创建文件、表或迁移；不新增表、版本、奖励语义或配置键。
- 面板提供空玩家禁用、操作忙碌时锁定玩家/视图、保持同一玩家和键的可恢复重试、业务结果反馈及操作后状态刷新。接口、权限、错误 envelope 与交互写入 `docs/features/activity.md`；`docs/refactor_inventory.json` 同步管理员权限。
- `tests/test_activity_reward_web.py` 与 `tests/test_activity_reward_application.py` 聚焦套件 `15/15` 通过，覆盖权限/CSRF、参数、缺库旧 schema 无 DDL、六步顺序、成功/replay、异玩家 operation 冲突、部分失败续跑、旧 body operation ID、manifest 与面板合同。JS/Python 语法检查、inventory 与 architecture 门禁通过；diff check 在提交前复核。
- 本轮只改本目标白名单中的 API、管理员面板、manifest、测试与正式文档。目标提交 `a2214a29` 已推送 develop 并快进合入 main；develop CI run `39018775656`、main CI run `39019137012` 均成功（quality job 通过，build-and-release 按无 Release 跳过）。未部署，未读取或写入真实游戏库。

## 已完成目标：活动详情视图行动建议对齐一键领取来源

沿活动中心路线图推进到活动详情 Web 视图（`view=detail`）的行动建议段；中心卡片、
一键代领、签到代办、详情视图与投递合同均是已完成基线，不重做。

- 详情视图带 `user_id` 时按配置请求完整领取状态快照（结束阶段窗口、排行来源含 boss 板、
  首领活动 key），行动建议段复用中心 `detail_action_tips` 判定，覆盖任务、战令与追赶、
  集字保底、阶段、排行、首领进度、首领排行，与活动中心一键领取来源集合一致。
- 修复既有首领排行名次读取缺陷：boss 板位此前用主活动 key 查 `activity_boss_damage`，
  生产配置中首领活动 key 不同（如 `festival_world_boss`），名次恒为 0，中心与详情均无法
  提示首领排行可领；现按首领玩法活动自身 key 读取。
- 只读复用既有 `overview_snapshot`，同一单次快照，不新增表、迁移、配置键、查询次数或
  写路径；无 `user_id` 或快照缺来源键时详情输出不变。
- `tests/test_activity_detail_actions.py` 聚焦套件 `4/4` 通过，覆盖四来源单独可领、
  中心/详情一致性、无资格不误报、首领 key 修复；受影响活动套件（web_state、center、
  claim_sources、claimable、reward_web、sign_web）合计 `58/58` 通过。architecture、
  inventory 与 progress 门禁真实 exit 0；`git diff --check` 干净。
- 本轮只改 `center.py`、`read_model_application.py`、`read_model_repository.py`、
  `stage_participation.py`、新聚焦测试与正式文档。目标提交 `d5a25d32` 与快照单单元修正
  `e64ee107` 已推送 develop 并快进合入 main；develop CI run `39054068758`（首推
  `39042548289` 因快照多单元失败，已修正）、main CI run `39054522726` 均成功。
  未部署，未读取或写入真实游戏库。

## 已完成目标：NewAPI 签到历史写入收口

沿 Phase 2 backlog 项 `newapi-checkin-remote-history-window` 收口真实风险：远端签到
POST 与本地三条历史写入无法原子提交，进程在远端接受后、历史写入前崩溃会留下
"远端已签、本地无记录"的永久不一致；原实现历史写入失败只降级为无类别提示。

- `_run_checkin_for_target` 的历史写入抽为 `_record_checkin_history_with_recovery`：
  首次失败后同进程补偿重试一次（仅本地历史写入，不重放远端 POST——远端同日重复
  签到返回明确"已签"消息，但补偿丢失历史行只需本地写入）；重试仍失败才降级为带
  原因类别（数据库不可用/历史不可写）的 `_history_warning`，签到结果本身不变。
- 自动签到调度逐账号异常隔离行为不变；不改远端调用次数、历史表结构、3 条上限、
  展示格式或命令合同；不新增配置键或后台任务。
- `tests/test_entertainment_newapi_history_recovery.py` 聚焦套件 `6/6` 通过，覆盖
  重试成功、双失败分类警告、非数据库失败类别、成功不重试、远端错误仍记失败摘要、
  补偿不重放远端；受影响合同套件（checkin/auto/client/owner）合计 `20/20` 通过。
  architecture/inventory/progress 门禁真实 exit 0；`git diff --check` 干净。
- 本轮只改 `newapi_commands.py`、新聚焦测试、`tests/test_entertainment_checkin_contract.py`
  （AST 合同随委托更新）与正式文档。目标提交 `ed7413ed` 已推送 develop 并快进合入 main；
  develop CI run `39080592491`、main CI run `39080959210` 均成功。未部署，未读取或写入
  真实游戏库，不发送真实消息。

## 已完成目标：多首领排行名次按首领活动区分

沿活动中心路线图收口多首领配置下的排行名次缺陷：快照 `board_positions["boss"]`
只用第一个首领活动的 key 计算，而 `_claimable_boss_rank_count` 用这一个共享名次
匹配所有首领的档位——玩家在首领 A 上榜、首领 B 未上榜时，B 仍可能被误报"排行可领"。

- `overview_snapshot` 为每个首领活动 key 计算名次，新增可选快照键
  `boss_board_positions`（同一只读快照单元，无第二次往返）；旧
  `board_positions["boss"]` 保留为单首领/旧快照回退语义。
- `_claimable_boss_rank_count` 与奖励目录 `reward_catalog_text` 的首领排行段改为
  按各首领活动自己的名次匹配自己的档位；缺键时回退共享名次，现有断言不变。
- 不改领取写路径、`claim_boss_rank_reward` 的 query 语义、表结构或配置键。
- `tests/test_activity_boss_rank_positions.py` 聚焦套件 `6/6` 通过，覆盖 per-activity
  名次快照、A 上榜 B 不误报、双首领独立名次、旧快照回退、已领档不重复计数与单首领
  中心渲染；受影响活动套件合计 `98 passed + 4 subtests`。architecture/inventory/progress
  门禁真实 exit 0；`git diff --check` 干净。
- 本轮只改 `read_model_repository.py`、`center.py`、`read_model_application.py`、
  新聚焦测试与正式文档。目标提交 `44eecf85` 已推送 develop 并快进合入 main；
  develop CI run `39093663915`、main CI run `39094053406` 均成功。未部署，未读取或
  写入真实游戏库，不发送真实消息。

## 已完成目标：多首领领取状态按首领活动区分

沿活动中心路线图继续收口多首领配置缺陷：`_boss_claim_state` 把所有首领的
`milestones_unlocked/claimed` 与 `rank_claimed` 并成无命名空间集合，而领取回执按
`(activity_key, user, key)` 独立——首领 A 已领同名 `m1` 会漏报首领 B 的可领进度，
A 已解锁会冒充 B 未达标的进度，A 已领 `1-3` 排行档会漏报 B 的同档。

- 新增 `_boss_claim_state_by_activity`：按 `boss_progress` 的 activity key 归位
  各首领的 unlocked/claimed/rank_claimed；无 `boss_progress` 的旧 flat 快照回退
  共享投影（单首领语义不变）。
- `_claimable_boss_milestone_count` 逐首领用自己的 unlocked/claimed与自己配置的
  `server_milestones` 求交；`_claimable_boss_rank_count` 逐首领用自己的
  `rank_claimed` 判档（名次沿用 `boss_board_positions`）；奖励目录
  `reward_catalog_text` 的首领进度/排行段已领标记按当前首领自己的状态判定。
- 不改领取写路径、回执结构、表结构或配置键；只读复用同一快照。
- `tests/test_activity_boss_claim_scoping.py` 聚焦套件 `6/6` 通过，覆盖 A 已领不
  抑制 B、A 已解锁不冒充 B、排行档独立、flat 回退、helper 归位与奖励目录分段
  标记；上一切片 `tests/test_activity_boss_rank_positions.py` 的已领档断言随
  per-activity 表示更新。受影响活动套件合计 `104 passed + 4 subtests`。
  architecture/inventory/progress 门禁真实 exit 0；`git diff --check` 干净。
- 本轮只改 `center.py`、`read_model_application.py`、新聚焦测试、既有聚焦断言与
  正式文档。目标提交 `56391d37` 已推送 develop 并快进合入 main；develop CI run
  `39141994905`、main CI run `39142399243` 均成功。未部署，未读取或写入真实游戏库，
  不发送真实消息。

## 已完成目标：活动中心完整玩家流程——阶段时间线视图

活动中心各玩家流程（发现/详情/签到/任务/战令/阶段/排行/首领/集字/积分/奖励
目录/一键领取/Web 管理面）均已交付并有聚焦测试；本切片补全 `活动阶段` 视图
缺失的完整阶段日程，玩家一次查看全部阶段的进程与本人领奖状态。

- `activity_stage_text` 末尾新增【阶段日程】段：按配置顺序列出全部阶段的状态
  标记（进行中/已结束/未开始）、时间窗口与剩余倒计时（沿用 `remaining_text`）；
  已结束且配置奖励的阶段按本人参与证据与领取记录标注 `已领取` /
  `可领取｜活动阶段领取` / `未领取`，复用"可领阶段奖励"同一查询路径，无第二
  往返；匿名查看只列公共日程（已配置奖励显示"奖励：已配置"）；无阶段配置或
  活动未开启时输出与现状一致。
- 只读复用既有配置与快照；不新增命令、路由、表、迁移、配置键或写路径。
- `tests/test_activity_stage_timeline.py` 新聚焦套件 `6/6` 通过（多阶段顺序、
  参与证据标注、已领标注、匿名无标注、单阶段/无奖励阶段、关闭与无阶段回归）；
  受影响套件（web_state/stage/center/read_model/detail_actions/claim_sources/
  claimable）合计 `71 passed`。architecture/inventory/progress 门禁真实 exit 0；
  `git diff --check` 干净。
- 本轮只改 `read_model_application.py`、新聚焦测试与正式文档。目标提交 `70465227`
  已推送 develop 并快进合入 main；develop CI run `39244610916`、main CI run
  `39245281531` 均成功。未部署，未读取或写入真实游戏库，不发送真实消息。

## 已完成目标：积分事件每日上限进度闭环

活动中心积分玩法的事件结算带每规则 `daily_limit`（结算按
`activity_point_event_log` 当日累计截断），但玩家侧完全不可见——每日上限是黑盒。
本切片把同一累计渲染为进度。

- `overview_snapshot` 在同一只读单元内按 activity+user+date 有界聚合当日事件积分，
  新增可选快照键 `point_event_daily_totals`（事件日志表缺失时为 `None`，旧库兼容）。
- 共享纯 helper `point_event_daily_parts`（`gameplay_progress.py`）：每个配置
  `daily_limit` 的事件规则产出 `{label} 今日 {earned}/{limit}`（达上限追加
  `已达上限`）；中心进度行积分段在余额后追加 `（今日 …）`。
- `活动积分` 视图（`points_text`）与旧 `build_activity_points_text` 逐字 parity
  锁定，不携带个人进度——进度只在中心卡片渲染（skill 规则：parity-pinned 方法
  不能长数据，用新位置承载）。
- 不改积分结算、每日上限截断、商店购买或任何写路径；不新增命令、表、迁移或
  配置键。
- `tests/test_activity_point_daily_progress.py` 新聚焦套件 `7/7` 通过（进度渲染、
  达上限、跨日/跨用户不计、匿名无进度、旧库缺表、无限制规则、helper 单测）；
  受影响活动套件合计 `121 passed + 4 subtests`（含 read_model parity）。
  architecture/inventory/progress 门禁真实 exit 0；`git diff --check` 干净。
- 本轮只改 `gameplay_progress.py`、`read_model_repository.py`、
  `read_model_application.py`（仅 snapshot 传参）、`center.py`、新聚焦测试与正式
  文档。目标提交 `dc1d842c` 已推送 develop 并快进合入 main；develop CI run
  `39266368432`、main CI run `39267090259` 均成功。未部署，未读取或写入真实游戏库，
  不发送真实消息。

## 已完成目标：活动中心玩法入口完整覆盖

配置三类玩法（集字+积分+首领）时，`gameplay_entry_commands` 与配置覆盖路径在
映射层硬截断 `[:4]`——首领入口（活动首领/活动首领排行）整体从卡片消失，玩家
只能靠记住命令。入口数量约束属于投递层（消息长度），不该在映射层静默丢入口。

- 移除映射层两处 `[:4]` 截断：默认入口与配置 `commands` 覆盖都完整映射。
- 新增 `gameplay_entry_groups` 与 `ActivityCenterItem.gameplay_entry_groups`：
  按玩法分组的入口（组间按命令去重，主状态入口优先）；渲染层改为紧凑分组行
  ——主状态入口一行，之后每个进行中的玩法一行（`{玩法名}：入口 | 入口`），
  每行条目数只随该组增长，不随玩法总数无界增长。
- 投递载荷核验：最大配置（三类玩法+阶段+榜单+首领里程碑）整卡渲染字节数
  断言远低于 64 KiB（本插件 md模板输入自有上限）；QQ 原生 markdown 与
  OneBot 纯文本降级都完整覆盖全部入口（蓝字按 command 参数断言，纯文本按
  label 断言）。
- 不改投递层 `handle_send` 合同、不新增命令/路由/表/迁移/配置键、不改写路径。
- `tests/test_activity_gameplay_entry_coverage.py` 新聚焦套件 `8/8` 通过（三类
  玩法 6 入口、配置覆盖完整渲染、去重、未开启玩法、非进行中状态、最大配置
  分组行、载荷上限、纯文本降级）；受影响活动套件合计 `89 passed + 4 subtests`
  （含 delivery contract）。architecture/inventory/progress 门禁真实 exit 0；
  `git diff --check` 干净。
- 本轮只改 `center.py`、`read_model_application.py`、新聚焦测试与正式文档。
  目标提交 `edfeb9bf` 已推送 develop 并快进合入 main；develop CI run
  `39345702933`、main CI run `39346383377` 均成功。未部署，未读取或写入真实
  游戏库，不发送真实消息。

## 已完成目标：G1 积分商店购买引导闭环（roadmap-index G1）

`point_shop_text` 被 parity 锁定（与旧 `build_activity_shop_text` 逐字相等），
商店视图没有购买命令引导，玩家看到商品不知道发送 `活动购买 <商品名>`；达
个人上限/售罄也无终态标注。

- 新增 sibling `point_shop_guidance_text`：每商品行追加
  `购买：活动购买 {商品名}`；`bought>=limit>0` 标 `已兑换完（x/y）`、
  `stock_limit>0 且 sold>=stock_limit` 标 `已售罄`，两者不再引导；
  `limit<=0` 不限商品始终可购；尾部显示 `可购商品 N 件` 汇总。
- `活动商店` handler（`__init__.py:395`）改调 sibling；`point_shop_text`
  字节不变（parity 回归锁定）。
- 不改购买结算、上限/库存校验、拒绝语义；不新增命令/表/迁移/配置键。
- `tests/test_activity_shop_guidance.py` 新聚焦套件 `6/6` 通过（可购引导、
  个人上限终态、售罄终态、不限商品、parity 不变、汇总行）；受影响套件
  （point_shop_purchase/read_model/center/point_daily_progress/gameplay_consistency）
  合计 `51 passed + 4 subtests`。三门禁真实 exit 0；`git diff --check` 干净。
- 本轮只改 `read_model_application.py`（新增 sibling）、`__init__.py`（handler
  改调）、新聚焦测试与正式文档。目标提交 `7c68ffce` 与门禁字面量修正
  `5f8bc2f1` 已推送 develop；develop CI run `39393218002`（首推因
  activity_points_shop matcher 字面量翻转失败）、`39394735034` 均已核验，
  后者 success。

## 已完成目标：G2 集字兑换就绪状态与缺口明细闭环（roadmap-index G2）

`collect_bag_text` 被 parity 锁定（与旧 `build_collect_bag_text` 逐字相等），
背包视图无就绪标记、无缺字明细，达上限仍引导兑换。

- 新增 sibling `collect_bag_guidance_text`：字牌齐+未达上限标注
  `可兑换｜活动兑换 {词组}`；缺字列出 `还缺 {char}x{count}`（≤4 项，其余
  `等 N 项`），与结算侧 `tokens_insufficient` 计算同源；达上限标注
  `已兑换完` 且不再显示兑换命令；`limit<=0` 不限词组永不显示已兑换完。
- `活动背包` handler（`__init__.py:353`）改调 sibling（主视图 + parity 回退
  表达式保留 gate 字面量）；`collect_bag_text` 字节不变（parity 回归锁定）。
- 不改兑换结算事务、幂等回执、拒绝语义；不新增命令/表/迁移/配置键。
- `tests/test_activity_exchange_readiness.py` 新聚焦套件 `6/6` 通过（就绪
  标注、缺字明细、达上限终态、不限词组、parity 不变）；受影响套件
  （collect_exchange/read_model/gameplay_consistency/center）合计
  `47 passed + 4 subtests`。三门禁真实 exit 0；`git diff --check` 干净。
- 本轮只改 `read_model_application.py`（新增 sibling）、`__init__.py`（handler
  改调）、新聚焦测试与正式文档。目标提交 `42b4a1e4` 已推送 develop 并快进合入
  main；develop CI run `39421168893`、main CI run `39423466337` 均成功。未部署，
  未读取或写入真实游戏库，不发送真实消息。

## 当前完整目标：G3 阶段领取引导与时间线标注闭环（roadmap-index G3）

阶段时间线已交付，但 `未领取`（无参与证据）阶段没有提示参与记录来源，玩家
不知道为什么领不了；`活动阶段领取` 无 query 的语义（领取全部可领阶段）已
核验但未在文档固定。

- 时间线 `未领取` 标注细化为
  `未领取（参与记录来自活动期间的签到/任务/玩法记录）`；`可领取`/`已领取`
  标注不变。
- `活动阶段领取` 无 query 语义（无 query = 领取全部可领阶段）由行为测试固定
  （`claim(uid, query="")` 实际领取后时间线翻转已领取）。
- 只读渲染改动；不改 claim 过滤语义、领取写路径、参与证据判定或分状态拒绝
  文案；不新增命令/表/迁移/配置键。
- `tests/test_activity_stage_claim_guidance.py` 新聚焦套件 `5/5` 通过（未领取
  参与提示、可领取不变、已领取不变、匿名无标注、无 query 领取行为对照）；
  受影响套件（stage/stage_timeline/stage_gate/center/web_state）合计
  `48 passed`。三门禁真实 exit 0；`git diff --check` 干净。
- 本轮只改 `read_model_application.py`（时间线标注）、新聚焦测试与正式文档；
  develop 推送/CI、审查与 main 快进/CI 按发布门继续完成。未部署，未读取或
  写入真实游戏库，不发送真实消息。

## 已完成目标：G5 活动玩法个人进度视图（roadmap-index G5）

`gameplay_text` 被 parity 锁定、无 `user_id` 参数，玩家看不到本人集字保底、
积分当日进度与首领伤害；目标文档 `docs/goals/goal-gameplay-personal-state.md`。

- 新增 sibling `gameplay_personal_text(user_id)`：复用 `gameplay_text` 的
  配置/阶段/玩法摘要，按玩法追加本人状态段（集字复用
  `collect_pity_entries`/`closest_pity_entry`；积分余额+今日进度复用
  `point_event_daily_parts` 与快照键 `point_event_daily_totals`；首领复用
  `boss_snapshot`）；数据来自一次 `overview_snapshot`（传 `now`），无
  `user_id` 时输出等价 `gameplay_text()`。
- `活动玩法` handler 改调新方法并传入登录用户 id；`gameplay_text` 字节不变
  （parity 回归锁定）；旧库缺可选表时对应段省略；只读，无新命令/路由/表/
  迁移/配置键，不改写路径。
- `tests/test_activity_gameplay_personal.py` 新聚焦套件 `4/4` 通过（三玩法
  本人状态段、单快照、parity、handler 源合同；时钟经
  `patch.object(application.clock, "now", …)` 固定为与插入记录同日的 aware
  时间戳，测试与服务器日期无关）；受影响套件 `test_activity_read_model.py`
  `17/17`（含两个 G5 测试 `test_personal_gameplay_uses_one_snapshot_for_all_progress`
  与 `test_personal_gameplay_ignores_missing_legacy_inventory_table`）、
  `test_activity_gameplay_consistency.py` `9/9`、
  `test_activity_point_daily_progress.py` `7/7` 全绿。三门禁真实 exit 0；
  `git diff --check` 干净。
- 本轮只改 `read_model_application.py`、`read_model_repository.py`、
  `__init__.py`（handler 改调）、新聚焦测试与 `test_activity_read_model.py`
  两个 G5 测试。目标提交 `bf85e010` 已推送 develop 并快进合入 main；
  develop CI run `39998595819`、main CI run `40001772187` 均成功（headSha
  均为 `bf85e010`）。未部署，未读取或写入真实游戏库，不发送真实消息。

## 已完成目标：G6 活动视图投递合同全覆盖（roadmap-index G6）

`test_activity_center_delivery_contract.py` 只通过共享 `handle_send` 缝隙证明过中心
卡片；其余玩家视图没有投递层测试，实测确实存在合同缺口。目标文档
`docs/goals/goal-view-delivery-contracts.md`。

- 新增 `tests/test_activity_view_delivery_contracts.py`（`7/7`）：以随包默认配置
  （集字+积分+首领三玩法、阶段日程、首领行动提示、购买引导、就绪标注）渲染
  stage/boss/shop/bag/reward/gameplay 六视图，逐个走
  `handle_send(native_markdown=True, fallback_msg=strip_md_command_links(text))`：
  OneBot 与关闭 markdown 的 QQ 只收到一次纯文本（无 `**`、无 `mqqapi`、入口 label
  与关键行完整、降级幂等）；QQ 原生 markdown 收到 `\n`→`\r` 归一后消息、保留
  `](mqqapi`；多行布局（玩法段之间、多集字段之间）两条路径都保留空行。handler 源
  合同逐个钉住该视图自己的 handler，另有商店/背包运行期接线合同。
- 合同暴露并修复的缺口（只读渲染/接线）：`活动首领` 曾先 `_ensure_user`，未注册
  用户拿到「请先修仙」而不是状态块，且从未走原生缝隙 → 改为按事件用户 id 渲染并
  走共享缝隙；`活动商店`/`活动背包` 只发纯文本（QQ 原生下购买/兑换引导无蓝字）→
  改走共享缝隙；`活动玩法`/`活动奖励` 的 `fallback_msg` 曾是未剥链接的 `text` →
  统一 `strip_md_command_links(text)`；`活动玩法` 玩法段与「今日进度」后缺空行、
  `活动背包` 多集字时相邻段出现两个空行 → 新增 `_tighten_blank_lines` 折叠为
  「段后恰一空行、段间不连空行」。
- 不改投递层 `handle_send` 的换行归一行为、不改领取/购买/兑换/讨伐写路径与
  `claim_*` query 语义；不新增命令/路由/表/迁移/配置键。
- 受影响 `tests/test_activity_*` 合计 `352 passed`，加上 `test_refactor_progress`
  `23/23` 共 `375` 通过；architecture/inventory/progress 三门禁真实 exit 0；
  `git diff --check` 干净；`compileall` 通过。按用户指示未跑全量测试、未部署、
  未读写真实游戏库、未发真实消息。
- 本轮只改 `read_model_application.py`、`xiuxian_activity/__init__.py`、新聚焦测试与
  正式文档。目标提交 `5c88cd3a` 已推送 develop 并快进合入 main；develop CI run
  `40260273593`、main CI run `40261122013` 均成功（headSha 均为 `5c88cd3a`，
  build-and-release 按分支规则 skipped）。未部署，未读取或写入真实游戏库，不发送
  真实消息。

## 已完成目标：活动中心收口大目标（G4→G5→G6，roadmap-index）

目标文档 `docs/goals/goal-activity-completion.md`（local-only，未提交）。审计确认三个阶段
均已有实现提交；本轮补齐 G4 多首领边界缺口，并把三阶段的空态/多首领/parity/投递合同
补成可回归断言。G4 此前没有独立完成记录，本段一并收口。

- 阶段提交：G4 首领行动引导 `8682acec`（develop CI run `39781507524` success）。该提交
  当时未单独快进 main，随后随 G5 一起进入 main，其 main 门禁由包含该代码的
  `bf85e010` main run `40001772187` 覆盖（历史 sha 不补跑）。G5 玩法个人状态 `bf85e010`
  （develop `39998595819`、main `40001772187` success）；G6 六视图投递合同 `5c88cd3a`
  （develop `40260273593`、main `40261122013` success）。
- 本轮 G4 修正：`boss_status_text` 在“按名查看单个首领”分支未传 `multiple_active`，
  多活跃首领下仍输出不带首领名的 `可用 活动首领领奖 排行 领取` /
  `活动首领领奖 进度 领取已解锁的进度奖励`，而 `活动首领领奖` 在多首领下会拒绝无名称
  query——该提示不可执行。现按活跃首领数在聚合与单首领两条渲染路径统一带名；新增
  真实视图边界测试（双首领聚合、双首领按名、单首领、无首领空态）。`docs/features/activity.md`
  同步该口径。
- 测试补强：G4 `test_activity_boss_action_hints.py` 17 项；G5
  `test_activity_gameplay_personal.py` 5 项（新增无记录玩家：集字零保底、积分零余额与
  零当日进度、首领无状态行省略段）；G6 `test_activity_view_delivery_contracts.py` 9 项
  （新增多阶段日程逐行在 OneBot 纯文本与 QQ 原生 `\r` 两条路径完整，以及命令链接抽取
  为键盘按钮后不得同时留在正文的非重复断言）。受影响 `test_activity*.py` 合计 `393`
  通过；三门禁真实 exit 0（progress `failures: None`；`exit_ready: False` 仍由用户删除的
  冻结文件导致，11 条 blocker 属既有）；`compileall` 与 `git diff --check` 干净。
- 边界：只读渲染与测试改动；不改讨伐结算、领奖写路径与 `claim_boss_*_reward` 的 query
  语义；不新增命令/路由/表/迁移/配置键。未部署、未读写真实游戏库、未发真实 QQ 消息、
  未创建 Release。
- 交付与 CI：修复提交 `3a15815e` 的 develop run `41457736488` success；`--ff-only` 合入
  main 后 main run `41459087854` success（headSha 均为 `3a15815e`；`build-and-release`
  按分支规则 skipped）。用户删除的四个文件保持未暂存；`docs/goals/**` 与
  `.codex-next-goal.md` 未提交。

## 已完成目标：活动一键领取中断重试回执可见性

活动中心 roadmap（G1-G6）已全部交付，本轮改从仓库证据另选切片：`docs/features/activity_reward.md`
承诺 `retryable_failure`「保留已完成步骤、重试只补跑未完成步骤」，持久化行与重试语义确实成立
（实测中途失败后重试只跑剩下的 3/6 步），但玩家文案只写「活动奖励领取未完成，请重试 + 失败步骤」，
从不说明前面几步已经发放——玩家无法判断奖励是否已到账，只能靠客服或补偿核对。

- 旧兼容协调器 `ActivityClaimAllService.run` 与新仓储 `ActivityClaimAllRepository.run` 共用同一
  文案规则：失败行后追加 `已发放：任务、战令、…（重试不会重复发放）`；无步骤落发放时写
  `暂无已发放步骤，重试从第一步开始`。文案只列步骤标签，不回显奖励明细（明细在重试结算后按
  已落账数据重出）。
- 不改六步固定计划、步骤顺序、`retryable_failure` 状态码、外层 ledger/审计、重试补跑语义、
  表结构、迁移或配置键；不改 `apply/duplicate/operation_conflict` 分支与其他结果文案。
- 新聚焦套件 `tests/test_activity_claim_all_retry_message.py` `8/8`（旧/新两条路径各覆盖：多步成功
  后失败列出全部已发放步骤、首步即失败写无已发放提示、已完成段按固定六步顺序、单步成功不列
  未执行步骤、文案变更不影响重试只补跑未完成步骤）；受影响 claim-all/reward 套件 `24 passed`。
  三门禁真实 exit 0，`git diff --check` 干净。未跑全量测试、未部署、未读写真实游戏库。

## 已完成目标：文档权限与 manifest 一致性门禁（架构门禁补强）

`check_manifest_documentation()` 只要求文档**提到**路由/命令，从不校验文档写下的权限等级。
`activity_reward` 因而长期把 `POST /api/v1/activity/rewards/claim` 写成 `权限 user`，而 manifest 与
蓝图实际都按 `admin` 执行（`adapters/web/static/pages/admin.js` 是唯一调用方），运维按文档
配置权限会直接踩空。

- `scripts/check_architecture.py` 新增 `_documented_permission_drift()` 与
  `check_documented_permission_levels()`：逐行读取功能文档里 `权限 \`<level>\`` 的声明，
  与 manifest 的 `RouteSpec`/`CommandSpec.permission` 比对，不一致即报错；不写权限的
  文档行不受影响（存在性仍由原检查负责）。
- 修正唯一一处真实偏差：`docs/features/activity_reward.md` 的 Web API 段改为 `admin`，
  并写明该端点只供管理员面板代玩家领取、玩家本人走 bot 命令。
- 不改任何执行层权限、路由、命令、表或迁移；只加门禁与文档更正。
- `tests/architecture/test_architecture_gate_checks.py` 新增 `ManifestPermissionDriftTests`
  `5/5`（路由漂移上报、命令漂移上报、一致的声明不报、未写权限的行不报、真实仓库保持零错误）；
  反射对照确认把文档改回 `user` 时门禁确实报错。
- 三门禁真实 exit 0；`git diff --check` 干净；`compileall` 通过。未部署、未读写真实游戏库、
  不发送真实消息。

## 已完成目标：活动 Web 玩家视图与 bot 六视图对齐

bot 侧六个玩家视图（`活动阶段`/`活动首领`/`活动商店`/`活动背包`/`活动奖励`/`活动玩法`）
在 G6 已有统一投递合同，但 Web 的 `GET /api/v1/activity/state` 与管理员面板下拉都只有
`center|detail|stage|rank|boss`；实测 `?view=shop|bag|reward|gameplay` 一律
`400 unknown view`，运营无法在面板查看商店购买引导、背包就绪标注、奖励目录与玩法个人状态。
四个读模型本身可直接渲染，能力已存在、Web 未暴露。

- `adapters/web/blueprints/activity.py` 视图映射补 4 项：`shop`/`bag`/`reward`/`gameplay`
  分别指向 `point_shop_guidance_text`/`collect_bag_guidance_text`/`reward_catalog_text`/
  `gameplay_personal_text`；路由直接返回读模型文本，不重渲染。
- `adapters/web/templates/pages/admin.html` 的 `data-activity-view` 补齐同名四个选项。
  权限语义（无 `user_id` 公共、带 `user_id` 要求 `admin` 否则 403）、未知视图 400、缺
  `application` 的兼容响应、缺可选表的 `schema_missing`（503）失败关闭合同全部沿用既有视图；
  `center`/`detail` 在缺可选表时本就返回 503，新视图行为与其一致。
- 只读对齐：不新增路由、命令、表、迁移或配置键；不改写路径；未部署、未读写真实游戏库、
  不发送真实消息。
- 新聚焦套件 `tests/test_activity_web_views.py` `12/12`（四视图公共/个人两态、逐字等于读模型
  文本、九视图枚举、面板选项与路由映射一致、旧库只读不建文件、缺表失败关闭）；受影响
  `test_activity_web_state`/`test_activity_center_delivery_contract`/
  `test_activity_view_delivery_contracts`/`test_activity_center`/`test_activity_read_model`
  合计 `53 passed`。三门禁真实 exit 0；`git diff --check` 干净；`compileall` 通过。目标提交
  `52f62c09` 已推送 develop 并快进合入 main；develop CI run `40371340167`、main CI run
  `40372486961` 均成功（headSha 均为 `52f62c09`）。未部署、未读写真实游戏库、不发送真实消息。

## 已完成目标：娱乐房间重启后超时恢复

三个娱乐房间在重启后仍然存在（快照持久化于 `entertainment_game_rooms`，
启动时 `load_rooms()` 恢复），但**超时任务不随进程保留**：一个等待对手的
五子棋房间（`ROOM_TIMEOUT=180s`）重启后不再自动关闭，创建者永远
「你已在其它棋局中」，无法开新局；进行中的五子棋/扫雷/十点半同样不再超时收敛。

- 新增只读纯规则模块 `features/entertainment/room_lifecycle.py`：
  `timeout_seconds`/`room_deadline`/`is_expired`/`remaining_seconds`/
  `expired_rooms`。按类型取时间锚点（五子棋 `playing` 用落子时钟、否则用加入
  时钟；十点半用创建时钟；扫雷用操作时钟），`waiting`/`playing` 之外的状态一律
  不参与，缺字段或坏时间戳按「不清理」处理。到期时刻本身不算过期（运行期任务
  睡满整段才动作），剩余时间返回余量，过期返回 0。
- `room_repository.py` 增加只读批量能力：`list_all_states()` 在**单个只读 UoW**
  内取回三类快照（`list_states` 语义、行/字节预算与 fail-closed 检查不变，抽出的
  `_select_rows` 两者共用）；`EntertainmentApplication.room_states_all()` 暴露该
  能力。
- 新增 `mod/room_restore.py`：`restore_entertainment_rooms()` 用一次批量快照依次
  调用三个 manager 的 `load_rooms(states)`，再执行过期清理与剩余时间重建；进程内
  只跑一次（三个启动钩子中的第一个真正执行，其余为空操作），供测试重置的
  `reset_restore_state()`。
- 三个 `restore_*_rooms()` 改为 async 并委派给该协调器；`load_rooms(states=None)`
  增加可选入参（不传时仍按原路径逐类型读取，既有调用语义不变）。过期房间走**与
  运行期相同的清理分支**（仅抑制通知）：五子棋 `_close_waiting_room`/
  `_finish_stale_move`、十点半 `_close_waiting_room`（人数不足关闭 vs 自动结算）、
  扫雷 `_expire_game`（`status="closed"` + 落盘），其余房间按**剩余时间**重建
  计时器；重复重建先取消旧任务，不叠加。`_finish_stale_move` 接受可选 `moment`，
  使恢复用与过期判定同一时刻比较而非墙钟。
- `start_*_timeout` 增加关键字参数 `delay`（默认 None = 原完整时长，在线行为
  不变）并允许 `bot/event` 为 None（仅清理与落盘，不投递）。运行期超时文案与
  触发条件未改。
- 不改写路径、表结构、迁移或配置键；未新增命令/路由。
- 新聚焦套件 `nonebot_plugin_xiuxian_2/features/entertainment/tests/test_room_lifecycle.py`
  `20/20`（锚点选择、非候选状态、缺/坏时间戳、到期边界、余量、按类型分组）、
  `tests/test_entertainment_room_restore_timeout.py` `15/15`（过期释放并可
  重新开局、走真实关闭分支、存活房间保留且重建计时器、无投递上下文不发送消息、
  重复重建不叠加、启动钩子可 await）与 `tests/test_entertainment_room_batch_restore.py`
  `9/9`（批量读只开一个只读 UoW、覆盖三类且空类型仍在、保留预算与 fail-closed、
  恢复只跑一次且不逐类型读、无 bot 不发消息）；`test_room_repository.py` 新增
  3 项批量读断言（`9 passed`）。受影响套件 `test_entertainment_room_managers`/
  `test_half_ten_providers`/`test_minesweeper_providers`/`test_source_quality`
  与全部 entertainment feature 测试（129 项）合计 `270 passed`。三门禁真实
  exit 0；`git diff --check` 干净；`compileall` 通过。
- 交付提交：`d814465d`（纯规则模块 + 首次接入，develop CI run `40447332383`
  success，已快进 main，main CI run `40450117536` success）、`5ab2db4e`（文档，
  run `40448212171` success）、`060ca806`（回到 develop/CI 记录，run
  `40449259010` success）、`fdb8ae8f`（单快照批量读 + 协调器，run `40461069946`
  success）、`bcaf80a5`（文档，run `40461855992` success）、`80ba1cb9`（过期房间
  走真实关闭分支，run `40468586654` success）、`42401305`（最终记录，develop CI
  run `40472172908` success，已快进 main，main CI run `40473193062` success）；
  全部 develop run 的 `quality` job 均 success、`build-and-release` skipped。
  整卡验收通过前未把 `fdb8ae8f` 之后的提交合入 main。未部署、未读写真实游戏库、
  不发送真实消息。

## 已完成目标：鬼市订单生命周期闭环（N1→N2→N3）

鬼市三个入口（存/取灵石、求购、摆摊）此前已迁到 feature application，但订单从创建
到成交、到期、退款从未闭环：`鬼市求购` 的回执明写「次日 8 点自动取消订单，并退还
未购得物品的灵石」，而仓库里没有任何求购到期路径（guishi 仓储零 `created_at`/
`expire` 读取），冻结灵石只在手动取消或管理员 `清空鬼市` 时才回来；`鬼市摆摊`
创建后不触发撮合（对照求购创建后立即调用 `process_guishi_transactions`）；
撮合按摆摊价结算而买家按求购价冻结，差价既不入卖家也不退买家。目标文档
`docs/goals/next-full-feature.md`（local-only，未提交）。

- N1 求购到期自动取消退款：新增纯规则 `features/trade/guishi_lifecycle.py`
  （创建日次日 `GUISHI_BAITAN_END_HOUR` 点到期、`None` 锚点永不过期、混合时区
  不抛错）与 `GuishiQiugouExpiryService`（单次只读 UoW 取候选 + 既有
  `cancel_qiugou` 退款，操作号 `guishi-expire-qiugou:{order_id}` 稳定可重放；
  缺锚点表/缺库返回空且不建表）。锚点复用既有
  `guishi_order_create_operations.created_at`，**未新增表/列/迁移**。接线：
  `clear_expired_baitan_orders_job` 先做全服到期再做摆摊清理（job id 与 cron 不变），
  `鬼市信息` 做本人访问期收敛。
- N2 摆摊即时撮合与差价退还：新增 `GuishiListingFillService`（同物品、求购价不
  低于摆摊价、非本人、按创建时刻 FIFO，操作号 `guishi-fill:{摆摊}:{求购}`），
  `鬼市摆摊` 创建后立即撮合并把成交摘要与 `鬼市成交` 统计写回卖家回执
  （抽 `_record_guishi_match_event` 与既有定时撮合共用）。`guishi_match` 新增
  `price_refund`（`数量 × (求购价 - 摆摊价)`）在同事务退回买家，`GuishiMatchResult`
  默认值保证旧 operation 行重放兼容；价格闸门与部分成交语义不变。
- N3 玩家可见状态与文案：新增纯渲染 `guishi_order_view.py` 与只读
  `GuishiOrderStatusService`，`鬼市信息` 每笔订单显示到期时刻与剩余时长，
  无锚点历史订单显示 `到期：未知（历史订单）`，段尾给出可执行提示；摆摊回执
  「物品不退还」改为与代码一致的「未售出物品退回背包」；`trade_help.py` 补齐
  `鬼市取消求购`/`鬼市取物品` 与到期规则。正式文档 `docs/features/trade.md`、
  `docs/gameplay_notes.md` 已更新。
- 测试：`tests/test_trade_guishi_qiugou_expiry.py` `21/21`、
  `tests/test_trade_guishi_listing_fill.py` `13/13`、
  `tests/test_trade_guishi_order_status.py` `13/13`；
  `tests/test_trade_guishi_match_repository.py` 按新语义原地更新（买家余额
  99→159、回滚不留退款、旧行 `price_refund=0`）；三阶段受影响套件
  `362 passed`、unittest 收集 `293 tests OK`。三门禁真实 exit 0；
  `compileall` 与 `git diff --check` 干净。另用一次性探针跑真实 application
  路径验证价值守恒：1000 → 冻结 120 → 成交净付 80 并退差价 40 → 到期退回 100
  → 买家 920、卖家 80，重复清理不再退款。
- 交付提交：`f020f62d`（N1，develop CI run `40517570714` success）、
  `5319b209`（N2，run `40519162723` success）、`235d9912`（N3，run
  `40520461812` success）、`66e4710b`（状态记录，develop CI run `40521587683`
  success）。三阶段与记录提交齐备后一次性快进合入 main（`main` 与 `develop`
  均为 `66e4710b`），main CI run `40522703643` success（headSha `66e4710b`，
  `build-and-release` 按分支规则 skipped）。未部署、未读写真实游戏库、
  不发送真实消息。

## 已完成目标：拍卖结算物品投递闭环（A1→A2→A3）

拍卖的竞价、上架、下架、场次开始与结算早已由 feature application 承担，但结算的容量
口径在生产接线里恒为 `1`：`AuctionSettlementApplication` 只传 database，
`AuctionSettlementSqlRepository(database)` 的 `max(int(max_goods_num or 1), 1)`
把「未传参」当成单件上限。真实仓储探针（生产接线，非配置默认）实测
`repo.max_goods_num = 1`，因此卖家流拍时只要背包里还有同名物品就判「背包满」：
`auction_current` 行被删、物品不退回、`auction_history` 记 `卖家背包满流拍`，
而玩家日志写的是「已退回背包」；中标者已持有该物品 1 件则 `inventory_full`
拒绝整场结算，所有人的拍品与卖家收入一起卡住，管理员重试也必然失败。目标文档
`docs/goals/next-full-feature.md`（local-only，未提交）。

- A1 结算容量口径修正（`bc404a9d`）：新增 `DEFAULT_MAX_GOODS_NUM = 1000` 与
  `configured_max_goods_num()`（惰性读 `XiuConfig().max_goods_num`，配置不可读才
  回落默认值），`AuctionSettlementSqlRepository` 在未显式注入时走该解析；
  `AuctionSettlementApplication` 可透传 `max_goods_num`。实测修复后
  `max_goods_num = 1000`：卖家持有 2 件时流拍物品退回成 3 件、中标者已持有 1 件
  时正常入包。不改结算价、手续费、卖家收入与败者退款口径。
- A2 未送达入库（`621e558e`）：新迁移 `auction.009`（game DB）创建
  `auction_pending_items`/`auction_pending_claim_operations`。结算在同一事务内
  按 `backpack`/`pending` 投递：卖家流拍且背包满、中标者背包满都写入待领取行
  （`role`+`reason`），历史状态 `待领取`，**不再丢物品、也不再阻塞整场**；
  其余拍品、卖家入账、败者退款照常完成。`item_missing`/`participant_missing`
  仍整事务拒绝；旧库缺表时插入抛错→整个结算回滚、场次保持 active，绝不静默降级
  为销毁。中标/流拍日志改为按 `delivery` 分支，不再谎称「已退回背包」。
- A3 领取闭环（`0d9d378b`）：新增 `AuctionPendingApplication`——`list_for` 只读
  列本人未领取物品，`claim` 在单个 immediate UoW 内逐行做容量校验、原子入包
  （`goods_num/bind_num +1`）、标记 `claimed_at/claim_operation_id`，背包满的
  跳过并保持待领取，同 operation_id 重放不重复发放，缺表 `schema_missing` 失败关闭。
  命令面复用既有 `我的拍卖`（不新增命令名/路由/配置键）：无参数同时列出等待区与
  待领取并给出指令提示，`我的拍卖 领取 [拍卖ID]` 领取全部可领取项或指定一件；
  `trade_help.py` 拍卖帮助补该指令与待领取说明。
- 测试：`tests/test_auction_settlement_delivery.py`（A1 容量 4 项 + A2 入库 5 项 +
  迁移路由）`10/10`、`tests/test_auction_pending_claim.py`（领取/重放/跨用户/
  容量/缺表 + handler 源合同）`15/15`；既有断言按新语义**原地更新**：
  `test_auction_settlement_repository.py` 由「inventory_full 保持 pending」改为
  「settled + 待领取 + 待领取行」，`test_auction_session_finish.py` 同，
  `test_source_quality.py` 的结算默认路径合同改钉
  `self.repository = repository or self._build_repository()` +
  `max_goods_num=self.max_goods_num`。受影响拍卖/交易/源码质量套件 `335 passed`，
  unittest 收集 `271 tests OK`；三门禁真实 exit 0；`compileall` 与
  `git diff --check` 干净；`docs/refactor_inventory.json` 已重新导出（仅新增两张表）。
- 端到端探针（真实 application + 真实仓储，cap=1 复现满包）：结算 `待领取`、
  场次 `settled`、卖家 +630、败者退 500、中标者待领取 1 件；满包时领取 `blocked`
  且仍待领取，腾出空间后领取成功入包（`goods_num=1,bind_num=1`），同 operation_id
  重放返回 `duplicate`、不重复发放。
- 交付与 CI：A1 `bc404a9d` 的 develop run `40529415381` **failure**——唯一失败项是
  `test_source_quality` 仍钉旧字面量 `self.repository = repository or
  AuctionSettlementSqlRepository`，该合同在 A2 提交就地更新（功能代码本身由 A2 的
  绿色 run 覆盖），这里如实记录而不宣称通过；A2 `621e558e` run `40531739039`
  success；A3 `0d9d378b` run `40534016731` success；状态记录 `9993f451` 的
  develop run `40535126193` success，随后一次性快进合入 main（`main` 与 `develop`
  均为 `9993f451`），main CI run `40536296188` success（headSha `9993f451`，
  `build-and-release` 按分支规则 skipped）。未部署、未读写真实游戏库、
  不发送真实消息。

## 已完成目标：拍卖竞价可见性与被超价通知（B1→B2）

竞价资产一直是对的：`AuctionBidSqlRepository` 在同一事务锁定新出价并退回上一位领先者，
`refunded_bidder`/`refunded_amount` 落进 `auction_bid_operations`。但玩家侧完全看不到这条链：
`on_bid` 效果签名里没有被超价者与退款额，被超越的人只会发现灵石悄悄回到余额；
`我的拍卖` 只列等待区与待领取，看不到自己参与的竞价。目标文档
`docs/goals/next-full-feature.md`（local-only，未提交）。

- B1 我的竞价视图：`AuctionQuerySqlRepository.my_bids(user_id)` 只读解析
  `auction_current` 与 `auction_bid_operations`——竞价仓储在玩家被超越时会把它从
  `auction_current.bids` 移除，因此**出价只能从操作账本恢复**；返回
  `auction_id/item_name/my_price/current_price/leading/bid_count`，缺库缺表返回空且不建表。
  `AuctionQueryApplication.my_bids` 直接委派。`我的拍卖` 在等待区与待领取之后追加
  `【我的竞价】` 段（领先/已被超价 + `拍卖竞拍 <ID> <价格>` 加价提示），
  竞价投影读取异常时该段按空处理，不影响整条指令。
- B2 被超价通知：`AuctionBidEffects.on_bid`/`NullAuctionBidEffects` 增加带默认值的
  `refunded_bidder`/`refunded_amount`；`_append_effect_event` 把两者写进 outbox payload，
  `reconcile_outbox_event` 读出并透传；`LegacyAuctionBidEffects.on_bid` 在出价行之后，
  若被超价者非空且退款额大于 0，写
  `[拍卖被超价] {物品} 已被超越，原出价 {N} 灵石已退回，拍卖ID:{id}`，
  `event_id=f"auction.outbid:{operation_id}"`（与出价行 `auction.bid:{operation_id}` 分开，
  outbox 重放各自去重）。`reconcile_event` 同步透传两字段。
- 不改扣款/退款金额、竞价校验、operation 与 outbox 结构；不新增表、迁移、命令、路由或配置键。
- 测试：新增 `tests/test_auction_bid_visibility.py` `13/13`（领先/被超价、账本恢复出价、
  只返回本人拍品、`current_price` 不低于最高出价、空玩家、缺库缺表不建表、超价通知两行/
  无超价一行/退款额为 0 不写、事件号稳定可去重、application payload 源合同、
  handler 渲染源合同）；受影响拍卖/交易/源码质量套件 `333 passed`，unittest 收集
  `274 tests OK`；三门禁真实 exit 0；`compileall` 与 `git diff --check` 干净。
- 端到端探针（真实 `AuctionBidSqlRepository` + `AuctionQueryApplication` +
  `LegacyAuctionBidEffects` + 真实日志写入到临时 players 目录）：先由 me 出价 500、
  再由 rival 出价 800 → 回执 `refunded_bidder=me/500`；`my_bids("me")` 返回
  `my_price=500,current_price=800,leading=False`；me 的日志恰好一条
  `[拍卖被超价] 灵剑 已被超越，原出价 500 灵石已退回，拍卖ID:a1`，rival 只有出价行。
- 交付与 CI：`b05c665e` 已推送 develop，develop CI run `40620682579` success；状态记录
  `a2fba70e` 的 develop run `40621226265` success，随后一次性快进合入 main（`main` 与
  `develop` 均为 `a2fba70e`），main CI run `40621911086` success（headSha `a2fba70e`，
  `build-and-release` 按分支规则 skipped）。未部署、未读写真实游戏库、不发送真实消息。

## 已完成目标：我的拍卖——本场拍品与个人成交记录闭环

`我的拍卖` 只读等待区/待领取/我的竞价；开场把等待区整体搬进 `auction_current` 并清空
`auction_player_upload` 后，卖家拍品从该命令消失，三源全空时还会误报「没有上架任何
拍卖物品」。结算结果只写一次性私聊日志，玩家错过 `[拍卖成交]`/`[拍卖流拍]` 后无法回查。
目标文档 `.codex-next-goal.md`（local-only，未提交）。

- 新增只读 `AuctionQuerySqlRepository.my_auction_snapshot(user_id)`（单个只读 UoW）：
  `lots` 返回本人非系统在拍品——当前价取列值与最高出价的较大者，出价次数/人数来自
  `auction_bid_operations`（缺表回退 `bids` 长度），剩余时间取 active `auction_sessions`
  的 `end_time` 差额（缺表为 `None`）；`records` 按 `seller_id OR winner_id` 取最近 5 条
  `auction_history`，由 SQL `ORDER BY end_time DESC LIMIT` 截断。缺库/缺表返回空且不建表。
- 新增纯渲染 `features/auction/my_auction_view.py`；`我的拍卖` 段序固定为
  等待区 →【本场拍品】→【拍卖待领取】→【我的竞价】→【最近拍卖记录】，五源全空才进空态。
  卖出行显示成交价/手续费/实收/买家，拍得行显示成交价/卖家；流拍写「未售出，物品已退回
  背包」，待领取写「物品在拍卖待领取」，不谎称已售出。
- 只读；不改上架/下架/竞价/结算/投递/领取写路径；不新增表、迁移、命令、路由或配置键。
- 测试：`tests/test_auction_my_lots.py` `24/24`（仓储投影、开拍交接回归、双角色记录、
  limit/排序、缺库缺表不建文件、纯渲染、handler/帮助源合同）；受影响拍卖与源码质量套件
  `324 passed`；unittest 收集 `162 tests OK`。architecture/inventory/progress 门禁真实
  exit 0；`compileall` 与 `git diff --check` 干净。
- 目标提交 `ac8cfdc3` 已推送 develop，develop CI run `40680683571` success；快进合入
  main，main CI run `40681683532` success（headSha 均为 `ac8cfdc3`，`build-and-release`
  按分支规则 skipped）。未部署、未读写真实游戏库、不发送真实消息。

## 已完成目标：签到业务日回执与签到日历

`修仙签到` 只依赖可被每日重置清掉的 `user_xiuxian.is_sign`：重置任务若延迟到玩家当天签到
之后执行，会把当天标记清零，玩家可再次签到并重复领灵石；迁移前的 `is_sign=1` 也没有业务
日期回执，无法区分新旧签到，玩家侧也没有签到历史可查。目标文档 `.codex-next-goal.md`
（local-only，未提交）。

- 新增 `sign_in.004`（game DB）创建 `sign_in_daily`（`user_id`、`business_date`、`stone`、
  `operation_id`，主键 `(user_id,business_date)`）与 `business_date` 索引；请求路径不建表。
- `SignInApplication.claim` 在事务内用注入时钟固定业务日：当天已有回执时返回既有
  `already_signed` 拒绝（即使 `is_sign` 被清成 0），成功路径在同一 UoW 写入日回执；
  `sign_in_daily` 缺失的旧库跳过回执、保持原领取语义。
- `SignInDailyResetSqlRepository` 在表存在时只清没有当天回执的 `is_sign`，审计记录
  `protected_rows`/`protected_count`；表缺失时保持旧全表清零。同日 operation 重放不重复清零。
- 新增纯渲染 `features/sign_in/calendar.py`：`signed_streak` 与
  `render_sign_in_calendar`（本月网格、已签 `✓`、今天 `·`、`本月已签 N 天｜连续 M 天`）；
  `修仙签到` 成功与已签到回复都追加日历，表缺失或读取异常时静默省略。
- 只读展示，不发额外奖励；不改灵石区间、随机源、ledger/effects/outbox、任务/统计投影或
  Web 响应；不新增命令、别名、路由或配置键。无历史回填（旧 `is_sign` 无法推断日期）。
- 测试：`tests/test_sign_in_daily_calendar.py` `18/18`（日回执、同日重复拒绝、延迟重置保护、
  旧标记清零、重放、缺表兼容、日历/连签、迁移与接线合同）；受影响签到与源码质量套件
  `277 passed`；unittest 签到组 `20 + 19 tests OK`。architecture/inventory/progress 门禁
  真实 exit 0；`compileall` 与 `git diff --check` 干净。
- 交付与 CI：功能提交 `03c5a1b6` 的 develop run `40710128801` **failure**——唯一失败项是
  `test_refactor_progress` 仍要求已知限制保留“迁移前/首次重置延迟”原文，文档改写时移除了
  该表述；修正文档 `be0e82b7` 的 develop run `40711177457` success，随后一次性快进合入
  main，main run `40712724666` success（headSha `be0e82b7`，`build-and-release` 按分支规则
  skipped）。未部署、未读写真实游戏库、不发送真实消息。

## 已完成目标：我的仙肆成交记录与收支汇总

仙肆成交事实早已写入 `xianshi_operations`（买家、卖家、物品、数量、成交总额、时间），但
`我的仙肆` 只读取 `xianshi_item` 当前挂单；物品售罄后命令直接回“没有上架任何物品”，玩家
只能从一次性 `仙肆售出`/`仙肆购买` 日志回忆成交。目标文档 `.codex-next-goal.md`
（local-only，未提交）。

- `XianshiQuerySqlRepository.my_trades(user_id, limit=5)` 在单个只读 UoW 内按
  `buyer_id OR seller_id` 取最近 5 笔，`rowid DESC` 排序；字段含 `operation_id、
  listing_id、name、goods_type、quantity、total_cost、role、counterparty、created_at`。
  对手方优先取 `user_xiuxian.user_name`（同名多行取最小 rowid）；缺表时为空，系统卖家
  显示“系统”。缺 `xianshi_operations`/数据库返回空且不建表。
- `TradeApplication.xianshi_my_trades` 直接委托；新增纯渲染
  `features/trade/xianshi_history_view.py`，逐行输出卖出/买入、数量、成交总额、对手方与
  时间，末尾给出最近 N 笔的卖出收入/买入支出汇总。
- `我的仙肆 记录`（也接受 `成交`/`历史`）展示记录；无记录时明确提示。原上架列表、20 条
  分页和 `send_msg_handler` 行为不变，尾部追加记录入口；空上架提示同样带入口。
- 只读；不改购买扣款、卖家到账、上架手续费、挂单数量、系统商品或 operation 回执；不新增
  表、列、迁移、索引、配置键、命令名或路由。
- 测试：`tests/test_trade_xianshi_history.py` `14/14`（角色/对手方、系统卖家、排序/limit、
  缺表缺库不建文件、纯渲染、handler/帮助源合同）；受影响交易与源码质量套件 `263 passed`；
  unittest 交易组 `121 tests OK`。architecture/inventory/progress 门禁真实 exit 0；
  `compileall` 与 `git diff --check` 干净。
- 目标提交 `6ef35416` 已推送 develop，develop CI run `40731686802` success；快进合入 main，
  main CI run `40732798556` success（headSha 均为 `6ef35416`，`build-and-release` 按分支规则
  skipped）。未部署、未读写真实游戏库、不发送真实消息。

## 已完成目标：魔修入侵个人战功与领奖状态

魔修入侵的个人战功已持久化在 `world_event_state.participants/claimed`，但状态页此前只显示
本境界本波前五名，榜外玩家看不到自己的出手、伤害、名次与领奖状态。目标文档
`.codex-next-goal.md`（local-only，未提交）。

- 新增纯渲染 `features/world_events/contribution_view.py`：`find_current_record` 兼容
  `realm:wave:user`、`realm:user` 与仅字段匹配的旧记录；`latest_wave` 取本人该境界最新
  波次；`render_personal_contribution` 汇总本期出手/真实伤害、当前波次名次与伤害占比，
  并按 `claimed`（用户 key、记录 key、`:*user` 后缀）和 `reward_ready` 输出已领取/
  可领取/进行中/已结束未领取/未参与。
- `xiuxian_world_events/__init__.py::_build_state_message` 在原有排行后追加【你的战功】段；
  `_personal_panel_lines` 仅在有 `user_info` 时渲染，无当前境界 Boss 时按本人最新波次降级。
  不改 `讨伐魔修`、`领取魔修奖励`、Boss/波次刷新、奖励计算或 claimed 写入；不新增命令、
  别名、路由、表、列、迁移、索引、配置键或查询次数。
- 测试：`tests/test_world_events_personal_contribution.py` `13/13`（含 3 subtests），覆盖
  榜外名次、波次汇总、三种 claimed 旧格式、四种奖励文案、坏字段降级与状态接线源合同。
  Phase2 门禁 `test_world_events_demon_claim_command_owns_atomic_claim_statistic` 改为校验
  证据路径与 `claim_demon_reward_` 调用图，不再硬编码源代码行号；该单测在 HEAD 快照中
  `OK`。`docs/refactor_phase2_legacy_path_items.json` 保持用户删除、未暂存。
- 门禁：architecture、inventory `--check`、progress `--json`（`failures: []`）真实 exit 0；
  `compileall` 与 `git diff --check` 干净。正式说明写入 `docs/features/world_events.md`
  与 `docs/gameplay_notes.md`。
- 交付与 CI：功能提交 `1b96bc04` 的 develop run `40738987652` failure——唯一失败项是
  Phase2 门禁仍断言旧行号 `__init__.py:191`；修正提交 `fc7f3d9f`（自然 import 布局 +
  路径级证据断言）的 develop run `40742318934` success，快进合入 main 后 main run
  `40743346266` success（headSha 均为 `fc7f3d9f`，`build-and-release` 按分支规则 skipped）。
  未部署、未读写真实游戏库、不发送真实消息；用户删除的四个文件保持未暂存。

## 已完成目标：竞技场荣誉结算回执与个人历史

竞技场每日荣誉由 `ArenaSeasonRewardApplication.reset_daily` 自动发放，结算快照已写入
`arena_season_reward_operations`，但 `我的荣誉` 只显示当前荣誉与“明日预计奖励”，玩家
看不到今天实际结算了多少、最近几次是否到账。目标文档 `.codex-next-goal.md`
（local-only，未提交）。

- `ArenaSeasonRewardRepository.recent_settlements(user_id, limit=5)` 在只读 UoW 中按
  `season_key DESC, rowid DESC` 取本人最近 N 条；缺 game DB 文件、缺表或读失败返回空，
  不建库、不建表、不补列。新增冻结 `ArenaHonorSettlement`，解析 `payload` 中的基础/
  排名/段位/名次；坏 JSON、缺字段或 `base + bonus != honor` 时只保留总计，不猜测拆分。
- `ArenaSeasonRewardApplication.recent_settlements` 只读委托；纯渲染
  `features/arena/honor_settlement_view.py` 输出【最近荣誉结算】，空记录给出每日 0 点
  结算提示，旧记录标注“明细不可用”。
- `我的荣誉` 保留既有段位/排名/荣誉与“明日预计奖励”，在其后追加最近最多 5 次结算；
  不新增命令、别名、路由、配置键、表、列、迁移或索引，不改自动结算、荣誉公式或资产。
- 测试：`tests/test_arena_honor_settlement_history.py` `10/10`，覆盖真实 claim 写入后
  读回、倒序/limit、用户隔离、缺表/缺文件不建库、坏 payload 降级、应用委托、纯渲染与
  handler 源合同；受影响竞技场/源码质量套件 `283 passed + 10 subtests`。architecture、
  inventory `--check`、progress `--json`（`failures: []`）真实 exit 0；`compileall` 与
  `git diff --check` 干净。
- 交付与 CI：功能提交 `695f1243` 的 develop run `40755596698` success；快进合入 main 后
  main run `40756949336` success（headSha 均为 `695f1243`，`build-and-release` 按分支规则
  skipped）。未部署、未读写真实游戏库、不发送真实消息；用户删除的四个文件保持未暂存。

## 已完成目标：组队邀请待处理面板

组队邀请已持久化到 `dungeon_team_invites` 并有到期清理，但 `查看队伍` 在玩家不在队伍时
直接回复“你不在任何队伍中”，错过的私聊邀请没有主动查询入口。目标文档
`.codex-next-goal.md`（local-only，未提交）。

- `features/dungeon/team_presentation.py` 新增 `TeamPendingInviteView` 与
  `render_pending_invite`：展示队伍名、邀请人道号、发起群、剩余分钟和同意/拒绝指引；
  空名称回退 `未知队伍`/`未知用户`，`remaining_seconds <= 0` 输出过期文案。
- `xiuxian_dungeon/__init__.py::view_team_handler` 的无队伍分支先调用
  `DungeonTeamApplication.pending_invite(user_id, now)`；命中未过期邀请时读取
  `team_info` 与邀请人道号并渲染，按钮为同意/拒绝/队伍帮助；未命中保持原创建队伍提示。
  新 helper 定义在文件末尾，既有 Phase2 证据锚点 `343/346/1126/1223` 未漂移。
- 只读；复用既有 `dungeon_team_invites` 与 `pending_invite`，不新增表、列、迁移、索引、
  配置键、命令、别名或路由；不改邀请 TTL、私聊通知、同意/拒绝或到期清理。
- 测试：`tests/test_dungeon_pending_invite_view.py` `7/7`，覆盖真实 pending/过期/已消费/
  他人邀请、缺表/缺库不建表、应用委托、纯渲染降级与 handler 源合同；受影响组队/源码
  质量套件 `275 passed`。`test_dungeon_frozen_command_owner` 在本地因用户删除
  `docs/refactor_phase2_legacy_path_items.json` 报 KeyError，在 HEAD 快照中单测 `OK`。
  architecture、inventory `--check`、progress `--json`（`failures: []`）真实 exit 0；
  `compileall` 与 `git diff --check` 干净。
- 交付与 CI：功能提交 `6c14cb7e` 的 develop run `40787111157` success；快进合入 main 后
  main run `40788335192` success（headSha 均为 `6c14cb7e`，`build-and-release` 按分支规则
  skipped）。未部署、未读写真实游戏库、不发送真实消息；用户删除的四个文件保持未暂存。

## 已完成目标：通天塔个人进度与里程碑总览

`通天塔信息` 此前只显示当前层数、历史最高、累计积分和挑战提示；玩家需要记住帮助里的
每 10 层奖励，也看不到本周兑换总进度。目标文档 `.codex-next-goal.md`（local-only，未提交）。

- 新增纯展示 `features/tower/progress_view.py`：`next_milestone` 返回严格大于历史最高的
  下一个 10 的倍数；`weekly_purchase_summary` 汇总有效商品的已兑/上限/已兑完项数，
  忽略 `_last_reset`、非法数量、缺失/非正上限；`render_tower_progress` 输出下一里程碑、
  剩余层数、首通积分/灵石/修为预览、本周兑换与重置提示，空配置仍保留里程碑段。
- `通天塔信息` 使用同一 `tower_limit.get_user_tower_info` 快照与 `tower_data.config`
  追加【进度与里程碑】；保留原有当前层数/历史最高/累计积分/挑战提示。零新增查询、
  表、列、迁移、索引、配置键、命令、别名或路由，不写回 `weekly_purchases`。
- 测试：`tests/test_tower_progress_view.py` `9/9`，覆盖里程碑边界、正常/坏/整数键
  `weekly_purchases`、空商品配置、可重复渲染与输入不变、handler 源合同；受影响
  通天塔/源码质量套件 `266 passed`。architecture、inventory `--check`、progress
  `--json`（`failures: []`）真实 exit 0；`compileall` 与 `git diff --check` 干净。
- 交付与 CI：功能提交 `fb40e07f` 的 develop run `40801441031` success；快进合入 main 后
  main run `40802372687` success（headSha 均为 `fb40e07f`，`build-and-release` 按分支规则
  skipped）。未部署、未读写真实游戏库、不发送真实消息；用户删除的四个文件保持未暂存。

## 已完成目标：灵庄流水与最近操作

`灵庄信息` 此前只显示当前存款、会员等级和上限；`bank_account_operations` 已保存每次
存入/取出/结息/升级，但玩家没有读取面。目标文档 `.codex-next-goal.md`（local-only，未提交）。

- 新增纯展示 `features/bank/history_view.py`：`BankOperationView` 与
  `render_bank_history` 输出【最近灵庄记录】，含存入/取出/结息/会员升级的时间、金额或
  利息、操作后存款；空记录显示“暂无记录”，不导入 NoneBot/Flask。
- `BankAccountInfoApplication.recent_operations(user_id, limit=5)` 在只读 UoW 中按
  `created_at DESC,rowid DESC` 取本人最近 N 条；从 payload 的 `withdraw`/`interest`/
  `upgrade` 前缀识别类型，缺失/损坏时按金额符号与利息降级；缺库/缺表/schema 不完整
  返回空，不请求期 DDL。`get_info` 既有字段不变。
- `BankCommandApplication.execute` 的 `info` 结果增加 `operations`；`render_bank_reply`
  仅在该键存在时追加流水，旧调用方与其它 action 输出不变。未改存取、结息、升级、回执、
  余额或资产写路径；无新表、列、迁移、索引、配置键、命令、别名或路由。
- 测试：`tests/test_bank_history_view.py` `8/8`，覆盖真实流水排序/limit/用户隔离、
  四类操作识别、坏 payload、缺表/缺库不建表、info 结果接线、纯渲染与命令源合同；
  受影响 bank/源码质量套件 `340 passed`。architecture、inventory `--check`、progress
  `--json`（`failures: []`）真实 exit 0；`compileall` 与 `git diff --check` 干净。
- 交付与 CI：功能提交 `ddfec137` 的 develop run `40828075324` success；快进合入 main 后
  main run `40829025235` success（headSha 均为 `ddfec137`，`build-and-release` 按分支规则
  skipped）。未部署、未读写真实游戏库、不发送真实消息；用户删除的四个文件保持未暂存。

## 已完成目标：活动中心最近一键领取回执

`活动领取` 的六步回执已写入 `activity_claim_all_operations` / `activity_claim_all_steps`，
但 `活动中心` 只渲染当前卡片，玩家错过后无法回看哪些来源已发放、哪些步骤失败。目标文档
`.codex-next-goal.md`（local-only，未提交）。

- 新增纯展示 `features/activity/claim_receipt_view.py`：`ActivityClaimReceiptView` 与
  `render_claim_receipts` 输出【最近一键领取】，按状态显示已领取来源、未完成步骤或
  “处理中（可重试）”；空输入不输出段。
- `ActivityReadModelSqlRepository.recent_claim_receipts(user_id, limit=5)` 在只读 UoW 中按
  `COALESCE(updated_at,created_at) DESC,rowid DESC` 取本人最近 N 条；解析 `result_json`
  的 `ok/text/steps`，坏 JSON 只保留状态/时间；缺库/缺表返回空，不请求期 DDL。
- `activity_center_text(user_id)` 仅在本人回执非空时追加该段；匿名或没有回执时输出与
  当前一致。未改六步领取计划、`retryable_failure` 文案、回执写入、ledger/审计或资产；
  无新表、列、迁移、索引、配置键、命令、别名或路由。
- 测试：`tests/test_activity_claim_receipt_view.py` `6/6`，覆盖真实回执排序/limit/用户
  隔离、成功/部分失败/未领取/处理中/坏 JSON、缺表/缺库不建表、纯渲染和中心源合同；
  受影响活动/源码质量套件 `292 passed`。architecture、inventory `--check`、progress
  `--json`（`failures: []`）真实 exit 0；`compileall` 与 `git diff --check` 干净。
- 交付与 CI：功能提交 `3db163c0` 的 develop run `40842248247` success；快进合入 main 后
  main run `40843463241`（headSha 均为 `3db163c0`；`build-and-release` 按分支规则
  skipped）。未部署、未读写真实游戏库、不发送真实消息；用户删除的四个文件保持未暂存。

## 已完成目标：活动中心最近活动签到

`活动签到` 的逐日记录已写入 `activity_sign_log`，但 `活动` 只显示累计/上次日期，
`活动中心` 没有逐日回看。目标文档 `.codex-next-goal.md`（local-only，未提交）。

- 新增纯展示 `features/activity/sign_history_view.py`：`ActivitySignDayView` 与
  `render_sign_calendar` 以今天为终点输出最近 7 天 `MM-DD 已签/未签`；空记录或全部
  坏日期不输出段，坏日期跳过。
- `ActivityReadModelSqlRepository.recent_signs(user_id, limit=7)` 在只读 UoW 中按
  `sign_date DESC` 取本人最近 N 条；缺库/缺表/读失败返回空，不请求期 DDL。
- `activity_center_text(user_id)` 仅在本人存在有效签到记录时追加【最近活动签到】；
  匿名或无记录输出与当前一致。未改 `活动签到` 的写入、奖励、去重、累计天数或事务；
  无新表、列、迁移、索引、配置键、命令、别名或路由。
- 测试：`tests/test_activity_sign_history_view.py` `5/5`，覆盖排序/limit/用户隔离、
  缺表/缺库不建表、7 天窗口补缺/坏日期、中心接线与源合同；受影响活动/源码质量套件
  `289 passed`。architecture、inventory `--check`、progress `--json`（`failures: []`）
  真实 exit 0；`compileall` 与 `git diff --check` 干净。
- 交付与 CI：功能提交 `c65c20a2` 的 develop run `40848021717` success；快进合入 main 后
  main run `40849339731`（headSha 均为 `c65c20a2`；`build-and-release` 按分支规则
  skipped）。未部署、未读写真实游戏库、不发送真实消息；用户删除的四个文件保持未暂存。

## 已完成目标：灵田傀儡自动收取生产路径修复

`灵田傀儡自动收取`（每小时 `auto_harvest` job → `check_and_harvest` →
`PuppetApplication.harvest` → `PuppetHarvestSqlRepository`）在生产形态下**必然失败**，
且失败被命令层 `return None` 静默吞掉，玩家与日志都收不到任何结果。目标文档
`.codex-next-goal.md`（local-only，未提交）。

- 读取不存在的列：`harvest_repository.py` 的 SELECT 引用 `mix_elixir_info."傀儡等级"`，
  而真实列是 `灵田傀儡`（= 等级，0 表示未拥有）；SQLite 把未解析标识符当**字符串
  字面量**，`int('傀儡等级')` 抛 `ValueError`。生产探针实测 row 为
  `('2026-01-01 00:00:00','1','傀儡等级')`。既有测试自建了带 `傀儡等级` 的表，
  所以本地绿、生产必炸。
- 奖励工厂类型不匹配：仓库按 4 元组解包，生产 `create_rewards` 返回
  `PuppetHarvestReward` 数据类 → `TypeError: cannot unpack non-iterable ...`。
- 时间基准不一致：`收取时间` 是无偏移文本，`SystemClock.now()` 是 aware UTC，
  裸减抛 `TypeError: can't subtract offset-naive and offset-aware datetimes`。
- 口径漂移：成熟窗口应为 `max(时间成本 × (1 - 加速基数 × 药材速度), 0)`、产出数量为
  `灵田数量 + 收取等级 + 收获加成`、驱动灵石按 `灵田傀儡` 自身等级取配置；旧实现用
  `elapsed × (1 + speed_base)`、`remaining // time_cost_hours` 且把不存在的列当等级，
  等级 2/3 的玩家被误判为未拥有。
- 状态码不可用：仓库产出 `not_mature`/`puppet_missing`，facade 只识别兼容实现的
  `not_ready`/`blessed_spot_missing`/`inventory_full`/`stone_insufficient`，未知状态
  直接 `return None`；灵石不足也不回写关闭。

修复内容：

- `PuppetHarvestSqlRepository` 重写：读真实列；`normalize_now` 把 aware 实例归一为
  naive UTC、`parse_harvest_time` 解析锚点；成熟窗口/产出数量/驱动灵石与
  `我的灵田` 展示及兼容实现同源；等级无配置时拒绝收取而不是免费发放；灵石不足把
  `puppet_status` 置 0 并提交；返回冻结数据类 `PuppetHarvestResult` 与 `HarvestReward`
  （替代 `type(...)` 动态类）；奖励工厂同时接受 4 元组与数据类；`_coerce_reward`/
  `as_int` 容错坏字段。保留 `puppet_harvest_operations` 幂等语义。
- facade `check_and_harvest` 补齐状态分支，未完成状态写出可读文案并记日志
  （`puppet_disabled` 是每小时遍历常态，保持静默）；`duplicate` 附"已经处理"提示。
- 零 schema 变更：复用 `mix_elixir_info`/`back`/`puppet_status`/
  `puppet_harvest_operations`；未改已应用的 `puppet.001`/`puppet.002` 函数；无新表、列、
  迁移、索引、配置键、命令名、别名或路由；job 注册与摘要不变。
- 测试：新 `tests/test_puppet_harvest_production_path.py` `20/20`，用**真实 player
  schema + 生产奖励工厂 + aware UTC 时钟**复现并钉住上述缺陷，覆盖收取成功、数量口径、
  `药材速度` 缩短窗口、未成熟剩余时间、naive 时钟兼容、未开启、灵石不足关闭傀儡、
  等级 3 计价、背包满 fail-closed、重复 operation 不重发、缺行、缺洞天福地、
  helper 单测、handler 源合同（不再静默 `return None`）。feature 侧
  `test_harvest_repository.py` 的表结构随真实列原地更新。聚焦套件合计 `330 passed`。
  architecture、inventory `--check`、progress `--json`（`failures: []`）真实 exit 0；
  `compileall` 与 `git diff --check` 干净；Phase-2 冻结门禁在 `git archive HEAD`
  快照中 `47 OK`。
- 交付与 CI：功能提交 `fd795c82` 的 develop run `40979221766` success；快进合入 main 后
  main run `40979718077` success（headSha 均为 `fd795c82`；`build-and-release` 按分支
  规则 skipped）。未部署、未读写真实游戏库、不发送真实消息；用户删除的四个文件保持
  未暂存。

## 已完成目标：炼丹升级闭环修复（升级收取等级 / 升级丹药控火）

`炼丹升级` 两条玩家命令在生产形态下都不可用，且都被自建 schema 的旧测试掩盖。
两条缺陷都源自同一重构提交 `00768565`。目标文档 `.codex-next-goal.md`（local-only，未提交）。

- **`升级丹药控火` 必然抛错**：`features/mixelixir/fire_control_upgrade_repository.py`
  读写 `mix_elixir_info."控火等级"`，而真实列是 `丹药控火`（权威来源
  `xiuxian2_handle.mix_elixir_infoconfigkey`，**没有 `控火等级`**）。SELECT 里 SQLite
  把未解析标识符当**字符串字面量**（探针 row 为 `'控火等级'`，`CAST` 后 0），UPDATE
  直接抛 `OperationalError: no such column: 控火等级`。探针在 `丹药控火=0/1/2` 三种
  等级下全部复现，等级与经验零变化；handler 无 try/except，玩家收不到任何回复。
  既有测试 `features/mixelixir/tests/test_fire_control_upgrade_repository.py` 自建了
  带 `控火等级` 的表，所以本地绿、生产必炸。
- **`升级收取等级` 对钱包非 0 的玩家恒失败**：`features/mixelixir/application.py` 的
  `harvest_level_upgrade` 默认分支把 `expected_stone=0` 硬编码传给仓储，而仓储把它当
  钱包并发快照，`current_stone != expected_stone` 即返回 `state_changed`。探针（真实
  application）实测 `stone=0` → applied、`stone=1`/`stone=5000` → state_changed；
  即只有钱包恰好为 0 的玩家能升级。handler 也从未透传钱包快照（兄弟
  `升级丹药控火` 传了）。旧测试的钱包值恰好等于硬编码快照，才得以通过。

修复内容：

- 两个升级仓储把列名收在类常量（`_LEVEL_FIELD = "丹药控火"` /
  `"收取等级"`，`_EXPERIENCE_FIELD = "炼丹经验"`），SELECT 与 UPDATE 共用，读写不再
  可能漂移；删除全部 `控火等级` 引用。
- `harvest_level_upgrade` 新增 `expected_stone` 参数并原样透传仓储；删除写死
  `expected_stone=0` 与「显式 repository」分支三重复的嵌套三元表达式，改为与
  `fire_control_upgrade` 同形的单一 `call`。`expected_stone=None` 语义保留
  （显式跳过钱包校验）。
- `升级收取等级` handler 传 `expected_stone=user_info['stone']`，与 `升级丹药控火`
  一致。两条命令都只消耗 `炼丹经验`，都不扣灵石。
- 零 schema 变更：复用 `mix_elixir_info`、`user_xiuxian.stone` 与两张既有
  operation 表；未改已应用迁移函数；无新表、列、迁移、索引、配置键、命令名、别名或
  路由；兼容 service 保留为显式回滚路径且列名常量与 feature 仓储一致。
- 测试：新 `tests/test_mixelixir_upgrade_production_path.py` `16/16`（含 7 subtests），
  用**真实 `mix_elixir_info` 列集 + 真实 `MixelixirApplication` + `OperationLedger`**
  复现并钉住两条缺陷，覆盖 `丹药控火=0/1/2`、duplicate 重放不重扣、operation_conflict、
  经验不足、钱包快照过期、钱包 `0/1/5000/极大值` 全通过、`None` 跳过快照、缺表/缺行
  fail-closed、操作行写入失败整体回滚、列名常量与 handler 源合同。feature 侧
  `test_fire_control_upgrade_repository.py` / `test_harvest_level_upgrade_repository.py`
  的表结构与断言同步改为真实列。聚焦套件合计 `334 passed + 9 subtests`。
  architecture、inventory `--check`、progress `--json`（`failures: []`）真实 exit 0；
  `compileall` 与 `git diff --check` 干净；`unittest discover -s tests -p 'test_mixelixir*'`
  `46 OK`。
- 交付与 CI：功能提交 `37e87326` 的 develop run `41008196196` success；快进合入 main 后
  main run `41008883828` success（headSha 均为 `37e87326`；`build-and-release` 按分支规则
  skipped）。未部署、未读写真实游戏库、不发送真实消息；用户删除的四个文件保持未暂存。

## 已完成目标：宗门命令契约闭环修复（搜寻/创建/加入/传位/任务结算）

宗门有 8 条玩家命令在生产数据上不可用或把成功渲染成失败。全部源自同一重构接线漂移，
且都被自建 schema / 自建 stub 的旧测试掩盖。目标文档 `.codex-next-goal.md`（local-only，未提交）。
证据方法：用插件自身的迁移目录把 `game_db`/`player_db` 迁到临时库得到真实 schema，
再按 `xiuxian2_handle` 的基础 DDL + `xiuxian_config.py` 的 `sql_sects`/`sql_user_xiuxian`
ALTER 列表得到真实旧表列集，最后调用真实 `SectApplication` 与真实 feature 仓储逐条实测。

- **宗门功法搜寻 / 宗门神通搜寻 必然抛错（三重缺陷）**：
  1. `xiuxian_sect/__init__.py` 以 `owner_position=owner_position` 调用
     `apply_buff_search`，而该方法签名无此关键字 → 实测
     `TypeError: got an unexpected keyword argument 'owner_position'`；
     同文件兄弟方法（`close_mountain`/`inherit_owner`/`open_join`/`disband`/`upgrade_*`）
     都接受 `*, owner_position: int = 0`，唯它漏了。handler 无 try/except，玩家收不到回复。
  2. `buff_search_repository.py` 读写**不存在的列** `sect_mainbuff` / `sect_secbuff`；
     真实 `sects` 列是 `mainbuff` / `secbuff`（权威来源 `xiuxian_config.py` 的 `sql_sects`）。
     实测 `UPDATE sects SET sect_mainbuff=...` 抛
     `OperationalError: no such column: sect_mainbuff`，而
     `SELECT "sect_mainbuff"` 返回字符串字面量 `'sect_mainbuff'`
     （与已修 `控火等级`、`傀儡等级` 同一 SQLite 语义陷阱）。修好 (1) 后命令依然必炸。
     对照：兼容实现 `legacy_sect_membership.py` 的 `{"main": "mainbuff", "secondary": "secbuff"}`
     与 `features/sect/repository.py` 都正确。
  3. 仓储把宗主校验写成 `int(user['sect_position'] or 0) != 0`，硬编码 0；
     兼容实现比较调用方传入的 `owner_position`，故非 0 宗主位的宗门配置永远拒绝。
- **`SectMutationResult.applied` 漏掉真实成功状态**：只认
  `claimed/closed/disbanded/duplicate/granted/inherited/upgraded`，漏掉
  `applied`/`charged`/`created`/`opened`/`transferred`/`settled`。实测
  `创建宗门`(`created`)、`宗门改名`计费(`charged`)、`开放宗门加入`(`opened`)、
  `宗主传位`(`transferred`)、`宗门任务完成`(`settled`)、功法/神通搜寻(`applied`)
  全部 `applied=False`，成功被渲染成失败并要求玩家重试。
- **`宗主传位` 读取不存在的属性**：handler 读 `result.succeeded`，而 `transfer_owner`
  返回 `SectMutationResult`（dict 子类，只有 `.applied`），
  `.succeeded` 走 `__getattr__` 抛 `AttributeError`（实测确认）。

修复内容：

- `features/sect/buff_search_repository.py`：列名收进
  `_BUFF_COLUMNS = {"main": "mainbuff", "secondary": "secbuff"}`，SELECT/UPDATE 共用；
  `apply(...)` 增加 `*, owner_position: int = 0` 并用于宗主校验，去掉硬编码 0。
- `features/sect/application.py`：`apply_buff_search` 增加 `*, owner_position: int = 0`
  并原样透传；`SectMutationResult.applied` 补齐六个真实成功状态。
- `xiuxian/xiuxian_sect/__init__.py`：`宗主传位` 成功判定由 `result.succeeded` 改为
  `result.applied`。
- 零 schema 变更：复用 `sects`、`user_xiuxian`、`sect_buff_search_operations` 及既有
  operation 表；未改已应用迁移函数；无新表、列、迁移、索引、配置键、命令名、别名或路由。
- 测试：新 `tests/test_sect_command_contract_production_path.py` `25/25`（含 35 subtests），
  用**真实旧表列集**（含 `mainbuff`/`secbuff`、**无** `sect_mainbuff`/`sect_secbuff`）
  + 真实 `SectApplication` + 真实 feature 仓储 + 真实任务迁移表，复现并钉住上述缺陷，
  覆盖成功/失败状态词表、搜寻 main/secondary 扣减与幂等重放不二次扣减、非宗主与资材不足
  零变化、`owner_position` 生效与不匹配被拒、创建/改名/开放/关闭/传位/任务结算各状态、
  传位结果无 `succeeded` 属性、handler 源合同。feature 侧
  `test_buff_search_repository.py` 表结构改为真实列并新增 `owner_position` 用例。
  聚焦套件合计 `386 passed + 37 subtests`；`unittest discover -s tests -p 'test_sect*'`
  `222 OK`、`-p 'test_source_quality*'` `246 OK`。
  architecture、inventory `--check`、progress `--json`（`failures: []`）真实 exit 0；
  `compileall` 与 `git diff --check` 干净。全量 `unittest discover -s tests` 在改动前后
  的失败集合逐名比对**完全一致**（37 个 distinct，全部源于用户有意删除的
  `docs/refactor_phase2_legacy_path_items.json`），确认零回归。
- 交付与 CI：功能提交 `72229709` 的 develop run `41027959408` success（全部步骤 success）；
  快进合入 main 后 main run `41029044007` success（headSha 均 `72229709`；
  `build-and-release` 按分支规则 skipped）。未部署、未读写真实游戏库、不发送真实消息；
  用户删除的四个文件保持未暂存。

## 已完成目标：砸蛋命令闭环修复（hatch_result keyword-only 调用）

`砸蛋` / `砸宠物蛋` 在生产形态下 100% 不可用。目标文档 `.codex-next-goal.md`
（local-only，未提交）。

- **位置参数调用 keyword-only 方法**：`xiuxian/xiuxian_pet/__init__.py:1064` 的前置回放
  检查以 `pet_application.hatch_result(operation_id)` 传位置参数，而
  `features/pet/application.py:152` 定义为 `def hatch_result(self, *, operation_id: str)`
  （keyword-only）。实测 `TypeError: hatch_result() takes 1 positional argument but
  2 were given`。
- **致命位置**：该调用位于 `pet_egg` handler 的灵石判断、容量判断、孵蛋与落库**之前**，
  且 handler 无 try/except，因此**任何**玩家每次发送 `砸蛋` 都在任何业务逻辑执行前抛错、
  收不到任何回复，孵化功能整体失效。
- **对照**：同文件 1185 行（同一次孵化的后置回放）与兼容门面
  `compatibility/pet.py:69` 都用正确的 `hatch_result(operation_id=operation_id)`；
  唯一破损点就是这一处位置参数调用。
- **测试为何未捕获**：既有 `features/pet/tests/test_pet_application.py:34` 的 stub
  写作 `def hatch_result(self, *args, **kwargs)`，接受任意位置参数，故本地绿、生产必炸。
  属已修的签名/语义漂移家族（`控火等级`、`傀儡等级`、`owner_position`、
  `sect_mainbuff`）的**位置参数 vs keyword-only** 变体。

修复内容：

- `xiuxian/xiuxian_pet/__init__.py`：第 1064 行改为
  `pet_application.hatch_result(operation_id=operation_id)`，与同文件后置回放及兼容门面一致。
- `features/pet/tests/test_pet_application.py`：stub 的 `hatch_result` 收紧为与生产一致的
  keyword-only 形态（`def hatch_result(self, *, operation_id)`），使同类漂移可被捕获。
- 零 schema 变更：复用 `pet_hatch_operations` 与既有宠物表；未改已应用迁移函数；
  无新表、列、迁移、索引、配置键、命令名、别名或路由；未改命令名/别名/priority/冷却/文案。
- 测试：新 `tests/test_pet_hatch_production_path.py` `5/5`，用**真实 `PetApplication`**
  （非 stub）+ 真实 `pet_hatch_operations` 表钉住缺陷：位置调用抛 `TypeError`、
  关键字调用返回 `None`、真实落库后经前置回放读出 `duplicate` 且
  `pets`/`updated_meta`/`bag_limit` 与写入一致（`pets` 为冻结元组）、
  缺表时抛错且**不建表**（不写请求期 DDL）、handler 源合同（所有 `hatch_result(` 调用
  均带 `operation_id=` 且无位置参数）。聚焦套件合计 `292 passed`；
  `unittest discover -s tests -p 'test_pet*'` `50 OK`。
  architecture、inventory `--check`、progress `--json`（`failures: []`）真实 exit 0；
  `compileall` 与 `git diff --check` 干净。
- 交付与 CI：功能提交 `40fbbc74` 的 develop run `41033528555` success；
  快进合入 main 后 main run `41034346130` success（headSha 均 `40fbbc74`；
  `build-and-release` 按分支规则 skipped）。未部署、未读写真实游戏库、不发送真实消息；
  用户删除的四个文件保持未暂存。

## 已完成目标：新增玩家「我的灵石流水」个人经济流水视图

新功能（非修复）。目标文档 `.codex-next-goal.md`（local-only，未提交）。

**缺口证据**：`economy_log` 是全服唯一经济流水账，由真实玩法持续写入
（`features/tasks/claim_repository.py`、`features/dufang/share_repository.py`、
`features/past_life/final_repository.py`、`features/admin_asset/*`，以及
`xiuxian/xiuxian_utils/economy_log.py` 的 `safe_log_economy_change` 7 处调用点），
但玩家完全看不到自己的流水：`features/economy_ledger/manifest.py` 的 `FEATURE`
没有 `commands`；`features/economy_ledger/commands.py` 明确声明只暴露管理员 Web
读取（`COMMANDS = ()`）；唯一入口是需管理员会话与 CSRF 守卫的 Web 蓝图。
数据在、聚合已实现、只有管理员能看，属纯可见性缺口。

真实 schema（用插件自身迁移目录实测，`game_db` 225 条迁移）：`economy_log` 15 列
`id,user_id,sect_id,source,action,stone_delta,exp_delta,sect_contribution_delta,
sect_scale_delta,sect_materials_delta,item_delta,detail,trace_id,created_at,event_id`，
已有 `idx_economy_log_user_source_time(user_id,source,created_at)` 支撑按玩家查询。

交付内容：

- 新玩家命令 `我的灵石流水`（别名 `灵石流水`、`灵石收支`，priority 7，`block=True`），
  注册在 `xiuxian/xiuxian_base/__init__.py`；展示本人近 7 天灵石收支总览
  （收入/支出/净额/笔数）、主要收入来源前 3、主要支出方向前 3、最近 5 笔明细。
- `EconomyLedgerApplication.stone_flow_summary(user_id)` 复用既有 `query_page`
  （同一仓储、同一 SQL），玩家视图与管理员台账不可能出现口径分歧；
  `user_id` 只由调用方注入，handler 不解析命令参数，**不能查询他人流水**。
- 新纯函数 `format_stone_flow(payload, *, format_number=None)`（无 I/O、无 NoneBot）
  承载文案与省略规则：无收入省略收入段、无支出省略支出段、无记录输出
  `近7天没有灵石收支记录。`、表缺失透传仓储 notice。
- 时间窗**只用下界**：`economy_log.created_at` 由各写入方提供（UTC 与本地时区混用），
  加上界会把「刚刚发生」的本地时间行误判为未来而丢弃；实测该陷阱会把 6 笔近 7 天
  流水错算成 4 笔、收入 9200 错算成 1200。
- 常量集中于 `features/economy_ledger/schemas.py`：
  `STONE_FLOW_DAYS=7`、`STONE_FLOW_TOP_N=3`、`STONE_FLOW_RECENT=5`，无配置键。
- `features/base/manifest.py` 的 `FEATURE.commands` 追加该命令的 `CommandSpec`；
  `docs/features/base.md` 的「命令与别名」与「Manifest 清单」两段登记
  （`check_architecture.py` 要求 feature 文档点名每条命令）。
- 零 schema 变更：只读复用 `economy_log` 与既有索引；不改任何写路径、经济数值、
  Web 路由或已应用迁移函数。
- 清单再生成：`scripts/export_refactor_inventory.py` 使
  `docs/refactor_inventory.json` 新增 1 条 command 记录（+9 行）。
- 新增命令使 Phase-2 派生计数 641→642、backlog 327→328；
  `tests/test_phase2_legacy_path_gate.py` 按既有活动切片先例同步该期望值
  （活动切片曾 638→641、324→327），**冻结成员文件与其哈希保持不变**
  （实测 `frozen_membership_valid: True`、`integrity_errors: []`、`failures: None`）。
- 测试：新 `tests/test_economy_stone_flow_view.py` `13/13`（`unittest.TestCase`），
  用**真实 `economy_log` 列集** + 真实 `EconomyLedgerApplication` 钉住：汇总口径
  （收入 6200/支出 10000/净额 -3800/4 笔）、**玩家隔离**（他人流水不进任何字段）、
  7 天窗口（窗外行排除）、收入/支出各前 3 排序、最近 5 笔上限、缺表返回 notice 且
  **不建表**、空 `user_id` 拒绝、四种渲染形态（完整/仅收入/仅支出/空）、
  handler 源合同（会话 `user_id` + 不出现 `args`/`extract_plain_text`）。
  聚焦套件合计 `270 passed`；`unittest discover -s tests -p 'test_economy*'` `17 OK`、
  `-p 'test_source_quality*'` `246 OK`、`-p 'test_phase2*'` `47 OK`（冻结文件在位时）。
  architecture、inventory `--check`、progress `--json`（`failures: None`）真实 exit 0；
  `compileall` 与 `git diff --check` 干净。全量 `unittest discover -s tests` 在改动前后
  的失败集合逐名比对**完全一致**（37 个 distinct，全部源于用户有意删除的
  `docs/refactor_phase2_legacy_path_items.json`），确认零回归；测试数 3482→3500。
- 交付与 CI：功能提交 `b0b5f3f4` 的 develop run `41043300120` success（全部步骤 success）；
  快进合入 main 后 main run `41044079519` success（headSha 均 `b0b5f3f4`；
  `build-and-release` 按分支规则 skipped）。未部署、未读写真实游戏库、不发送真实消息；
  用户删除的四个文件保持未暂存。

## 已完成目标：新增玩家「我的资产总览」跨库资产一览视图

新功能（非修复）。目标文档 `.codex-next-goal.md`（local-only，未提交）。

**缺口证据**：玩家「身家」分散在 **两个库、五张表**——灵石（`game_db.user_xiuxian.stone`）、
灵庄存款（`game_db.bank_accounts.saved_stone`）、宗门归属与贡献（`game_db.sects.sect_name` +
`user_xiuxian.sect_contribution`）、背包规模（`game_db.back`）、竞技场段位/积分/荣誉
（`player_db.arena.rank/score/honor_points`）。此前只能靠 `灵石`/`灵庄`/`我的宗门`/
`我的背包`/`我的竞技场` 五条命令各自拼，没有任何合计视图：
`grep -rn "资产|总览|一览" --include=*.py nonebot_plugin_xiuxian_2/xiuxian/ | grep on_command|on_regex`
**零匹配**，`docs/refactor_inventory.json` 的 command 清单也无对应条目。数据齐全、
每条命令都在、只有合计视角缺失，属纯可见性缺口，无需任何新数据。

真实 schema（插件自身迁移 + `xiuxian2_handle` 基础 DDL + `XiuConfig` ALTER 实测）：
`user_xiuxian` 33 列含 `stone/sect_id/sect_contribution`；`sects` 13 列含 `sect_name`；
`back` 13 列含 `goods_id/goods_num/bind_num`；`bank_accounts` 4 列
`user_id,saved_stone,bank_level,updated_at`；`arena` 含 `score/rank/honor_points`。

交付内容：

- 新玩家命令 `我的资产总览`（别名 `资产总览`、`我的资产`，priority 7，`block=True`），
  注册在 `xiuxian/xiuxian_base/__init__.py`；一次展示灵石、灵庄存款（含等级）、
  宗门（含贡献）、背包（种类/总数）、竞技场（段位·积分·荣誉）。
- 新模块 `features/base/assets_overview.py`：
  `PlayerAssetsOverviewApplication(game_db, player_db).overview(user_id)` 以**只读**
  `DatabaseUnitOfWork(..., read_only=True)` 打开主库并 `attach_database(player_db,
  "player_data")`，每段查询前用 `sqlite_master` + `PRAGMA table_info` 探测表/列，
  旧库缺表缺列自动降级；纯函数 `format_assets_overview(payload, *, format_number=None)`
  （无 I/O、无 NoneBot）承载文案与省略规则。
- **分段省略**：无灵庄开户行、无宗门、背包为空、无竞技场记录时各自独立消失，
  任一段缺失不会让整条回复失败；全部可选段缺失时仍输出标题与「灵石」行。
- **错误响应**：主库不可读或角色不存在 → `未找到角色数据，请先发送【我要修仙】。`；
  `player_db` 文件缺失/无 `arena` 表 → 只省略竞技场段、不抛错；
  `bank_accounts`/`sects`/`back` 缺表 → 只省略对应段，且**只读路径不建任何表**
  （实测 `sqlite_master` 仍无该表）。
- **玩家隔离**：`user_id` 只取 `check_user` 的会话信息，handler 不解析 `args`/
  `extract_plain_text`，无法查询他人资产；测试写入第二名玩家的巨额资产（灵石
  999,999、存款 888,888、贡献 777,777、段位王者）验证零泄漏。
- 零 schema 变更：只读复用上述五张既有表与索引；不新增表、列、索引、迁移版本、
  配置键或 Web 路由；不改任何写路径与经济数值。
- `features/base/manifest.py` 追加该命令的 `CommandSpec`；`docs/features/base.md`
  的「命令与别名」与「Manifest 清单」两段登记（`check_architecture.py` 要求
  feature 文档点名每条命令）；`scripts/export_refactor_inventory.py` 使
  `docs/refactor_inventory.json` 新增 1 条 command 记录（+9 行）。
- 新增命令使 Phase-2 派生计数 642→643、backlog 328→329；
  `tests/test_phase2_legacy_path_gate.py` 按既有先例同步期望值，
  **冻结成员文件与其哈希保持不变**（临时还原 `git show HEAD:` 版本实测
  `exit_ready: True`、`integrity_errors` 无、`exit_blockers` 0 条，随后再次删除）。
- 测试：新 `tests/test_assets_overview_view.py` `17/17`（`unittest.TestCase`），
  用**真实列集**（`user_xiuxian`/`sects`/`back`/`bank_accounts`/`arena`）与真实
  `PlayerAssetsOverviewApplication` 钉住：五段取数（灵石 123,456、存款 500,000 黄金、
  青云宗贡献 8,800、背包 2 种/17 个且 `goods_num=0` 不计、白银 1,520 分/荣誉 3,300）、
  完整渲染五行、**玩家隔离**、四种分段省略、全段缺失仅剩灵石、
  未知/空 `user_id` 引导文案、缺 `bank_accounts`/`arena`/`sects`+`back` 表与
  整个 `player_db` 缺失均降级且不建表、纯函数边界（空等级、无贡献）、
  handler 源合同（命令名/别名/`overview(str(user_info["user_id"]))`、无 `args`）。
  聚焦套件 `tests/test_assets_overview_view.py` + `test_source_quality.py` +
  `test_phase2_legacy_path_gate.py` + `features/base/tests/` 合计 `401 passed`
  （冻结文件在位时）；`unittest discover -s tests -p 'test_assets*'` `17 OK`、
  `-p 'test_source_quality*'` `246 OK`。architecture、inventory `--check`、
  progress `--json`（`failures: None`）真实 exit 0；`compileall` 与
  `git diff --check` 干净。改动前 stash 比对：`test_phase2_legacy_path_gate.py`
  在干净 HEAD 上同样 `86 failed`（全部源于用户有意删除的冻结文件），确认零回归。
- 交付与 CI：功能提交 `7ac9ea89` 的 develop run `41057871515` success；
  快进合入 main 后 main run `41058553519` success（headSha 均 `7ac9ea89`；
  `build-and-release` 按分支规则 skipped）。未部署、未读写真实游戏库、
  不发送真实消息；用户删除的四个文件保持未暂存，`.codex-next-goal.md` 与
  `docs/goals/**` 未提交。

## 已完成目标：虚神界统计列契约修复（虚神界闭关 / 投影虚神界）

修复（非新功能）。目标文档 `.codex-next-goal.md`（local-only，未提交；本目标由人工命名，
自动命名辅助能力遇 HTTP 400 不参与，未构成阻塞）。

**缺口证据（真实 schema 实测）**：用插件自身迁移 + `xiuxian2_handle` DDL +
`XiuConfig` ALTER 物化真实库（`game_db` 337 表 / `player_db` 82 表），
`player_data.statistics` 实有 59 列，含 `闭关次数`/`闭关时长`/`闭关修为`/
`闭关灵石消耗`/`虚神界时间获取` 等，但**不含** `虚神界闭关次数`、`虚神界投影次数`。
对真实 schema 直接调用生产仓储，两条玩家命令必然抛错：

- `ImpartClosingEnterSqlRepository.enter()` →
  `OperationalError: no such column: 虚神界闭关次数`
- `ImpartProjectJoinSqlRepository.join()` →
  `OperationalError: table statistics has no column named 虚神界投影次数`

**根因（同一缺陷类）**：两处都用
`CREATE TABLE IF NOT EXISTS statistics(user_id TEXT PRIMARY KEY, "<列>" INTEGER DEFAULT 0)`
把业务列内联进建表语句；而 `statistics` 已被 `features/base`、`training`、`auction`、
`boss` 等迁移以 `CREATE TABLE IF NOT EXISTS statistics(user_id TEXT PRIMARY KEY)` 先建好，
`IF NOT EXISTS` 分支是 **no-op**，内联列永远不会被创建。两个 handler 在调用处
（`xiuxian_impart_pk/__init__.py:984`、`:236`）都没有 `try/except`，异常直接冒泡，
**玩家收不到任何回复**。既有测试自建带目标列的表，绕开了真实 schema，故本地一直绿。

对照先例（同子系统与其他切片都已做对）：`closing_settlement_repository`（虚神界出关）
先 `PRAGMA` 取列、缺列才 `ALTER`、`UPDATE rowcount==0` 时 `INSERT`；
`past_life/start_repository`（前尘往事次数）同样先 `ALTER`；
旧路径 `_increment_entry_stat` 亦为 `PRAGMA` + `ALTER` + 补行。

交付内容：

- 新模块 `features/impart_pk/statistics_columns.py`：
  `ensure_statistics_column()` 用 `PRAGMA table_info(statistics)` 取真实列，
  缺列才 `ALTER TABLE ... ADD COLUMN "<列>" INTEGER DEFAULT 0`（不 `DROP`、不改已有列类型）；
  `increment_statistics()` 先 `UPDATE ... COALESCE("<列>",0)+? WHERE user_id=?`，
  `rowcount==0` 时 `INSERT` 补行，返回累加后的真实值。两者支持 `schema=""`
  （UoW 主库即 player_db、未 attach 的仓储）。
- `closing_enter_repository.py` 与 `project_join_repository.py` 的统计写入改走该辅助，
  都在既有写事务内执行，事务边界与回滚语义不变。
- 零迁移：不新增迁移版本、不改已应用迁移函数（checksum 不变）、不新增表/索引/配置键/
  命令/路由；不新增或修改任何玩家文案。
- 兼容性：旧库已有该列 → `PRAGMA` 判定存在，跳过 `ALTER` 直接读写；
  干净库首次写入自动补列补行（计数从 1 开始，不再像旧实现那样在无统计行时静默丢失）；
  `statistics` 表完全不存在时兜底建最小表再补列。
- 文档：`docs/features/impart_pk.md` 新增「`player_data.statistics` 统计列契约」一节。
- 测试：新 `tests/test_impart_statistics_column_contract.py` `19/19`（`unittest.TestCase`），
  刻意用**生产形态** `statistics`（只有 `user_id`，绝不内联目标列）钉住：
  两命令 `applied` 且计数 1；闭关两次累加到 2、删除统计行后仍从 1 正确补行；
  重复 `operation_id` 返回 `duplicate` 且计数不增；`busy`/`ineligible`/
  `already_joined`/`pk_exhausted`/`capacity_full` 分支计数不变；投影按玩家隔离；
  旧库预置列（值 5）累加到 6；`statistics` 表缺失时自动创建；
  以及回归防线——断言两个仓储**不得**再把业务列内联进 `CREATE TABLE ... statistics`。
  聚焦套件 `test_impart_statistics_column_contract.py` + `test_impart_closing_enter.py` +
  `features/impart_pk/tests/` + `test_source_quality.py` 合计 `261 passed`；
  `test_phase2_legacy_path_gate.py` `47 passed`（冻结文件在位时）；
  `unittest discover -s tests -p 'test_impart_statistics*'` `19 OK`。
  architecture、inventory `--check`、progress `--json`（`failures: None`）真实 exit 0；
  `compileall` 与 `git diff --check` 干净。
- 门禁计数：本切片不新增命令，派生计数按预期**保持** `643 / 329` 不变；
  临时还原冻结文件实测 `exit_ready: True`、`exit_blockers` 0 条、无 `integrity_errors`，
  随后再次删除。
- 交付与 CI：功能提交 `c0753bd0` 的 develop run `41071870790` success；
  快进合入 main 后 main run `41072988353` success（headSha 均 `c0753bd0`；
  `build-and-release` 按分支规则 skipped）。未部署、未读写真实游戏库、不发送真实消息；
  用户删除的四个文件保持未暂存，`.codex-next-goal.md` 与 `docs/goals/**` 未提交。

## 已完成目标：新增玩家「我的宗门贡献流水」个人贡献收支视图

新功能（非修复）。目标文档 `.codex-next-goal.md`（local-only，未提交）。

**缺口证据**：宗门贡献是玩家用灵石换来的个人额度，`economy_log.sect_contribution_delta`
由真实写路径持续记录——`宗门捐献`（`action="donate"`，贡献 +）、`宗门商店兑换`
（`action="shop_exchange"`，贡献 −）、宗门洞天/炼丹房升级、以及宗门任务结算
（`action="task_complete"`，贡献 +），均位于 `xiuxian/xiuxian_sect/__init__.py`。
但玩家只能看到 `user_xiuxian.sect_contribution` 这一个当前余额，
`grep -rn "贡献流水" --include=*.py` **零匹配**，没有任何命令说明贡献"最近怎么变的"。
数据在、写入点齐全、只有玩家视角缺失，属纯可见性缺口，无需任何新数据。

真实 schema（插件自身迁移实测）：`economy_log` 15 列，含
`sect_contribution_delta`、`source`、`action`、`created_at`；
`user_xiuxian` 33 列含 `sect_contribution`、`sect_id`；`sects` 含 `sect_name`。

交付内容：

- 新玩家命令 `我的宗门贡献流水`（别名 `宗门贡献流水`、`贡献流水`，priority 7，`block=True`），
  注册在 `xiuxian/xiuxian_sect/__init__.py`；一次展示近 7 天获得/消耗/净变化/笔数、
  主要来源与主要去向各前 3、最近 5 笔明细，以及当前贡献余额与所属宗门。
- 新模块 `features/economy_ledger/contribution_flow.py`：
  `ContributionFlowApplication(game_db, clock=...)` 以只读 UoW 聚合
  `sect_contribution_delta`；`format_contribution_flow()` 为纯函数（无 I/O、无 NoneBot），
  把已知宗门动作映射为中文（捐献/宗门任务/商店兑换/洞天升级/炼丹房升级），
  未知动作回退 `source·action`。
- 口径细节：`COALESCE(sect_contribution_delta,0)<>0` 过滤零变化行，使"笔数"反映真实变动；
  前 3 各按金额降序、笔数次之；最近 5 笔按 `created_at DESC, id DESC`。
- **时间窗只用下界**：`economy_log.created_at` 由各写入方提供、UTC 与本地时区混用，
  加上界会把「刚刚发生」的本地时间行误判为未来而丢弃（灵石流水切片已实测该陷阱）。
  常量与灵石流水同形，集中在 `features/economy_ledger/schemas.py`
  （`CONTRIBUTION_FLOW_DAYS=7`、`CONTRIBUTION_FLOW_TOP_N=3`、`CONTRIBUTION_FLOW_RECENT=5`）。
- **玩家隔离**：`user_id` 只取 `check_user` 会话，handler 不解析 `args`/
  `extract_plain_text`，无法查询他人贡献流水。
- **错误/降级响应**：`economy_log` 缺失、缺 `sect_contribution_delta` 列、
  `user_xiuxian` 缺 `sect_contribution` 列、`sects` 表缺失、库不可读——各自只丢失
  它覆盖的部分，均不抛异常；空 `user_id` 返回「缺少玩家身份，无法查询宗门贡献流水。」
  只读路径**不建任何表**（测试断言 `sqlite_master` 前后一致）。
- 零 schema 变更：不新增表、列、索引、迁移版本、配置键或 Web 路由；
  不改任何写路径与经济数值；不新增或修改既有玩家文案。
- `features/sect/manifest.py` 追加该命令的 `CommandSpec`；`docs/features/sect.md`
  的「命令与 Web API」与「命令与别名」两段登记；`scripts/export_refactor_inventory.py`
  使 `docs/refactor_inventory.json` 新增 1 条 command 记录（+9 行）。
- 新增命令使 Phase-2 派生计数 643→644、backlog 329→330；
  `tests/test_phase2_legacy_path_gate.py` 按既有先例同步期望值，
  **冻结成员文件与其哈希保持不变**（临时还原 `git show HEAD:` 版本实测，
  随后再次删除）。
- 测试：新 `tests/test_sect_contribution_flow.py` `16/16`（`unittest.TestCase`），
  用**真实 15 列 `economy_log`** 与真实 `ContributionFlowApplication`（注入固定时钟）钉住：
  汇总口径（获得 1,620 / 消耗 500 / 净 1,120 / 5 笔）、前 3 排序与金额、
  最近 5 笔倒序与上限、零变化行不计笔数、7 天窗外行排除、
  **玩家隔离**（第二名玩家 9,999 零泄漏且各自独立）、中文动作标签与未知动作回退、
  无记录/无角色文案、空 `user_id`、四条降级路径（缺表/缺列/缺宗门表/极旧库）、
  只读不建表，以及 handler 源合同（命令名/别名、`summary(str(user_info["user_id"]))`、
  无 `args`）。聚焦套件 `test_sect_contribution_flow.py` + `test_phase2_legacy_path_gate.py`
  + `test_source_quality.py` + `test_economy_stone_flow_view.py` + `features/sect/tests/`
  合计 `404 passed`（冻结文件在位时）；
  `unittest discover -s tests -p 'test_sect_contribution*'` `16 OK`。
  architecture、inventory `--check`、progress `--json`（`failures: None`）真实 exit 0；
  `compileall` 与 `git diff --check` 干净。
- 交付与 CI：功能提交 `ccd2f6f3` 的 develop run `41077385695` success；
  快进合入 main 后 main run `41078349901` success（headSha 均 `ccd2f6f3`；
  `build-and-release` 按分支规则 skipped）。未部署、未读写真实游戏库、不发送真实消息；
  用户删除的四个文件保持未暂存，`.codex-next-goal.md` 与 `docs/goals/**` 未提交。

## 已完成目标：新增玩家「我的物品流水」个人物品收支视图

新功能（非修复）。目标文档 `.codex-next-goal.md`（local-only，未提交；本目标由人工命名，
自动命名辅助 HTTP 400 属辅助能力故障，未构成阻塞）。

**缺口证据**：物品增减已全量记进共享 `economy_log.item_delta`，写入点齐备且语义统一——
背包加减（`xiuxian2_handle.py:2485` 的 `item_add` 与 `:2570` 的 `item_cost`）、
奖励统一发放（`reward_service.py:248`）、宗门商店兑换（`xiuxian_sect/__init__.py:3531`）、
仙肆购买（`xiuxian_trade/__init__.py:642`），元素形状均为
`{"id","name","type","amount",...}`，加物品 `amount` 为正、扣物品为负。
但 `grep -rn "物品流水|道具流水" --include=*.py` **零匹配**，`docs/refactor_inventory.json`
无对应命令；玩家只能翻 `我的背包` 看当前持有，无法回答"今天的丹药/材料怎么来的、花到哪去了"。
属纯可见性缺口，不需要任何新数据。

真实 schema（插件自身迁移实测）：`economy_log` 15 列，`item_delta` 为 **NOT NULL TEXT**，
另有 `source`、`action`、`created_at`、`id`、`user_id`。

交付内容：

- 新玩家命令 `我的物品流水`（别名 `物品流水`、`道具流水`，priority 7，`block=True`），
  注册在 `xiuxian/xiuxian_back/__init__.py`；一次展示近 7 天获得/消耗件数与种类数、
  变动笔数、主要获得与主要消耗各前 3、最近 5 笔明细。
- 新模块 `features/economy_ledger/item_flow.py`：
  `ItemFlowApplication(game_db, clock=...)` 以只读 UoW 取该玩家近 7 天
  `item_delta` 非空的行，按物品名聚合；`format_item_flow()` 为纯函数
  （无 I/O、无 NoneBot），已知动作映射中文（宗门商店兑换/仙肆购买/物品获得/物品消耗/
  宗门捐献/宗门任务），未知动作回退 `source·action`，最近明细按元素正负标注「获得/失去」。
- 口径细节：`''`/`'[]'`/`'{}'`/`'null'`/`None` 视为无变动、不计笔数，与
  `features/economy_ledger/repository.py` 既有 `item_change` 判定一致；两个方向各按
  累计件数降序、笔数次之取前 3；最近 5 笔按 `created_at DESC, id DESC`。
- **时间窗只用下界**：`economy_log.created_at` 由各写入方提供、UTC 与本地时区混用，
  加上界会把「刚刚发生」的本地时间行误判为未来而丢弃（灵石流水切片已实测该陷阱）。
  常量与灵石/贡献流水同形，集中在 `features/economy_ledger/schemas.py`
  （`ITEM_FLOW_DAYS=7`、`ITEM_FLOW_TOP_N=3`、`ITEM_FLOW_RECENT=5`）。
- **脏数据容错**：非法 JSON、非 list、元素非 dict、`amount` 非数字——逐行/逐元素跳过，
  其余照常展示；缺 `name` 回退 `物品#<id>`，缺 `type` 记「其他」；早期把单个物品
  直接写成对象的行也兼容。任何异常都在应用层捕获并降级为提示文案，绝不冒泡到 handler。
- **玩家隔离**：`user_id` 只取 `check_user` 会话，handler 不解析 `args`/
  `extract_plain_text`，无法查询他人物品流水。
- **降级响应**：空 `user_id` → 「缺少玩家身份，无法查询物品流水。」；
  缺 `economy_log` 表或缺 `item_delta` 列 → 「暂无物品流水记录。」；无记录 →
  「近7天没有物品收支记录。」；只读路径**不建表、不补列**（测试断言 `sqlite_master`
  与 `PRAGMA table_info` 前后一致）。
- 零 schema 变更：不新增表、列、索引、迁移版本、配置键或 Web 路由；
  不改任何物品增减逻辑、经济数值与既有玩家文案。
- `features/back/manifest.py` 追加该命令 `CommandSpec`；`docs/features/back.md`
  的「命令与别名」段登记；`scripts/export_refactor_inventory.py` 使
  `docs/refactor_inventory.json` 新增 1 条 command 记录（+9 行）。
- 新增命令使 Phase-2 派生计数 644→645、backlog 330→331；
  `tests/test_phase2_legacy_path_gate.py` 按既有先例同步期望值，
  **冻结成员文件与其哈希保持不变**（临时还原 `git show HEAD:` 版本实测后再次删除）。
- 测试：新 `tests/test_item_flow_view.py` `19/19`（`unittest.TestCase`），
  用**真实 15 列 `economy_log`**（`item_delta` NOT NULL）与真实 `ItemFlowApplication`
  （注入固定时钟）钉住：汇总口径（获得 112 件/2 种、消耗 20 件/1 种、5 笔）、
  四种空形状不计笔数、前 3 排序与笔数、最近 5 笔倒序与上限、7 天窗外排除、
  **玩家隔离**（他人 999 零泄漏且各自独立）、完整渲染、混合方向行双标注、
  未知动作回退、四种脏数据形状、dict 包装遗留行、缺 type 默认「其他」，
  以及无记录/空 `user_id`/缺表/缺列（不补列）/只读不建表五条降级路径，
  外加 handler 源合同（命令名/别名、`summary(str(user_info["user_id"]))`、无 `args`）。
  聚焦套件 `test_item_flow_view.py` + `test_phase2_legacy_path_gate.py` +
  `test_source_quality.py` + `test_sect_contribution_flow.py` + `features/back/tests/`
  合计 `332 passed`（冻结文件在位时）；
  `unittest discover -s tests -p 'test_item_flow*'` `19 OK`。
  architecture、inventory `--check`、progress `--json`（`failures: None`）真实 exit 0；
  `compileall` 与 `git diff --check` 干净。
- 交付与 CI：功能提交 `6485cbfb` 的 develop run `41095115054` success；
  快进合入 main 后 main run `41095961222` success（headSha 均 `6485cbfb`；
  `build-and-release` 按分支规则 skipped）。未部署、未读写真实游戏库、不发送真实消息；
  用户删除的四个文件保持未暂存，`.codex-next-goal.md` 与 `docs/goals/**` 未提交。

## 已完成目标：帮助文本补齐已上线玩家命令（发现性修复）

修复（非新功能）。目标文档 `.codex-next-goal.md`（local-only，未提交；本目标由人工命名，
自动命名辅助 HTTP 400 属辅助能力故障，未构成阻塞）。

**缺口证据**：近四个切片新增的四条玩家命令**上线即隐身**——`grep` 三个帮助文本块零命中，
只存在于 matcher 定义与 handler docstring：

| 命令 | 应有帮助入口 | 修复前 |
| --- | --- | --- |
| `我的灵石流水` | 修仙帮助 | 未列出 |
| `我的资产总览` | 修仙帮助 | 未列出 |
| `我的宗门贡献流水` | 宗门帮助 | 未列出 |
| `我的物品流水` | 背包帮助 | 未列出 |

复核命令：
`grep -rn "我的物品流水\|我的灵石流水\|我的资产总览\|我的宗门贡献流水" --include=*.py nonebot_plugin_xiuxian_2/xiuxian/ | grep -v on_command`
仅命中四条 handler docstring。即功能做了但玩家没有任何途径发现，属半个功能缺失；
既有测试只覆盖命令行为，**没有一条**断言"已上线命令必须出现在对应帮助文本里"。

交付内容：

- `修仙帮助`（`help_in_`）新增 `**流水与总览**` 分组（置于 `**功能入口**` 之前），
  列出 `我的灵石流水`（近7天灵石收支与主要来源去向）与
  `我的资产总览`（灵石、灵庄、宗门、背包与竞技场一览）。
- `宗门帮助`（`__sect_help__`）在 `**任务与商店**` 段 `宗门兑换` 之后追加
  `我的宗门贡献流水`（查看本人近7天贡献收支）。
- `背包帮助`（`back_help_`）在 `**查看**` 段 `灵石` 之后追加
  `我的物品流水`（查看本人近7天物品获得与消耗）。
- 统一沿用既有「`- 命令名` + `> 一行说明`」格式；**纯追加**，不改任何既有条目、
  分组名、快捷按钮（`k1..k4` / `button_id`）或 handler 行为；`宗门帮助` 仍走
  `paginate_text_blocks(..., per_page=3)`，不写死页数。
- 零 schema/命令变更：不新增表、列、索引、迁移版本、配置键、命令或 Web 路由；
  派生计数按预期**保持** `645 / 331`（临时还原冻结文件实测 `exit_ready: True`、
  `exit_blockers` 0 条，随后再次删除）。
- 文档：`docs/features/base.md`、`docs/features/sect.md`、`docs/features/back.md`
  各补一句"已登记在对应帮助入口"，保持文档与玩家入口一致。
- 测试：新 `tests/test_help_discoverability.py` `10/10`（`unittest.TestCase`），
  把这件事变成合同：
  - **正向**：四条命令既已注册，又逐字出现在所属帮助块中，且每条带 `>` 一行说明
    （不接受只甩命令名）。
  - **不倒退**：三个帮助块的原有分组名（`**基础指令**`/`**任务与商店**`/
    `**使用与整理**` 等）与抽样既有条目仍在；帮助快捷按钮未改。
  - **反向防漂移**：帮助块里每个 `- ` 条目都必须能落到真实命令上
    （等于主名/别名，或是某个主名/别名的口语简称），因此帮助**不可能**再宣传
    一条不存在的命令。
  - **分页**：`宗门帮助` 首页仍非空，且新条目可经翻页到达。
  - 收集器覆盖整个包（命令也注册在 `adapters/nonebot/commands.py`），并能从
    `"修仙签到" if _legacy_sign_in_enabled else "__legacy_sign_in_disabled__"`
    这类条件首参里取出真实命令名字面量、过滤 `__` 占位符。
  聚焦套件 `test_help_discoverability.py` + `test_source_quality.py` +
  `test_phase2_legacy_path_gate.py` + `features/back/tests/` + `features/sect/tests/`
  合计 `389 passed`（冻结文件在位时）；
  `unittest discover -s tests -p 'test_help_discoverability*'` `10 OK`。
  architecture、inventory `--check`、progress `--json`（`failures: None`）真实 exit 0；
  `compileall` 与 `git diff --check` 干净。
- 交付与 CI：功能提交 `6f461149` 的 develop run `41172607668` success；
  快进合入 main 后 main run `41172947130` success（headSha 均 `6f461149`；
  `build-and-release` 按分支规则 skipped）。未部署、未读写真实游戏库、不发送真实消息；
  用户删除的四个文件保持未暂存，`.codex-next-goal.md` 与 `docs/goals/**` 未提交。

## 已完成目标：新增玩家「我的签到统计」签到累计视图

新功能（非修复）。目标文档 `.codex-next-goal.md`（local-only，未提交；本目标由人工命名，
自动命名辅助 HTTP 400 属辅助能力故障，未构成阻塞）。

**缺口证据**：每次成功签到都会把统计写进投影，写入点在真实生产路径上——
`features/sign_in/application_effects.py:54` 的
`self.statistics.record(user_id=..., operation_id=f"statistics:{operation_id}",
event_key="修仙签到", occurred_at=now)`，其 effects 由 `plugin.py:1009` 以
`SignInApplicationEffects(..., statistics=SignInStatisticsRepository(str(game_db)))` 装配。
但 `grep -rn "签到统计\|签到记录"` 只命中 newapi 的无关命令，**没有任何玩家命令读它**；
`SignInStatisticsRepository.value(user_id, event_key)`（`features/sign_in/statistics.py:44`）
本就是为这个视图准备的读接口，长期只被测试与写入侧使用。数据在、读接口在、玩家入口完全没有。

真实 schema（插件自身迁移实测，两表都在 **game_db**）：
`sign_in_statistics_events(operation_id,user_id,event_key,created_at)`、
`sign_in_statistics_projection(user_id,event_key,value)`。

交付内容：

- 新玩家命令 `我的签到统计`（别名 `签到统计`、`签到记录`，priority 7，`block=True`，
  注册在 `xiuxian/xiuxian_base/__init__.py` 与 `修仙签到` 同文件）；展示累计签到次数、
  本月签到天数、最近一次签到时间。
- 新模块 `features/sign_in/statistics_view.py`：
  `SignInStatisticsViewApplication(game_db, clock=...)` 以只读 UoW 读投影表取累计次数、
  聚合事件表取本月天数与最近时间；`format_sign_in_statistics()` 为纯函数
  （无 I/O、无 NoneBot）。
- **口径合同**：累计次数与 `SignInStatisticsRepository.value(user_id, "修仙签到")`
  使用同一 SQL、同一 `event_key`；测试在同一夹具上断言两者相等，玩家视图与写入侧
  不可能分歧。
- **本月口径**：按 `created_at` 的 `YYYY-MM` 前缀匹配（不用 `strftime`，避免毫秒/时区
  后缀解析失败），天数用 `substr(created_at,1,10)` 去重——同一天两条签到只算一天。
- **分段降级**：两表都缺 → `暂无签到统计数据。`；只有投影表 → 显示累计、省略本月与最近；
  只有事件表 → 累计按事件行数兜底；`created_at` 为空的行不计天数、不污染最近时间；
  任何异常在应用层捕获并降级为提示文案。只读路径**不建表、不补列**。
- **玩家隔离**：`user_id` 只取 `check_user` 会话，handler 不解析 `args`/
  `extract_plain_text`，无法查询他人签到统计。
- 零 schema 变更：不新增表、列、索引、迁移版本、配置键或 Web 路由；不改签到写路径、
  奖励、连签规则或 `is_sign` 标记。
- 帮助与文档：`修仙帮助` 的「流水与总览」段追加 `我的签到统计`；
  `docs/features/sign_in.md` 登记命令与口径；既有
  `tests/test_help_discoverability.py` 的 `EXPECTED` 扩到第五条，反向防漂移合同
  继续生效。
- `features/sign_in/manifest.py` 追加该命令 `CommandSpec`；
  `scripts/export_refactor_inventory.py` 使 `docs/refactor_inventory.json` 新增 1 条
  command 记录（+9 行）。
- 新增命令使 Phase-2 派生计数 645→646、backlog 331→332；
  `tests/test_phase2_legacy_path_gate.py` 按既有先例同步期望值，
  **冻结成员文件与其哈希保持不变**（临时还原后再次删除）。
- 测试：新 `tests/test_sign_in_statistics_view.py` `14/14`（`unittest.TestCase`），
  用**真实两表列集**与真实应用（固定时钟）钉住：汇总口径（累计 5 次、本月 3 天、
  最近 10-30 09:12）、同日去重、跨月排除、其他 `event_key` 忽略、
  **与 `SignInStatisticsRepository.value()` 取值一致**、玩家隔离、
  三种降级形态（双表缺/只有投影/只有事件兜底）、空 `user_id`、脏 `created_at`、
  从未签到引导文案、只读不建表，以及 handler 源合同（命令名/别名、
  `summary(str(user_info["user_id"]))`、无 `args`）。
  聚焦套件 `test_sign_in_statistics_view.py` + `test_help_discoverability.py` +
  `test_phase2_legacy_path_gate.py` + `test_source_quality.py` +
  `features/sign_in/tests/` 合计 `326 passed`（冻结文件在位时）；
  `unittest discover -s tests -p 'test_sign_in_statistics*'` `16 OK`。
  architecture、inventory `--check`、progress `--json`（`failures: None`）真实 exit 0；
  `compileall` 与 `git diff --check` 干净。
- 交付与 CI：功能提交 `14f9e346` 的 develop run `41180959755` success；
  快进合入 main 后 main run `41181330664` success（headSha 均 `14f9e346`；
  `build-and-release` 按分支规则 skipped）。未部署、未读写真实游戏库、不发送真实消息；
  用户删除的四个文件保持未暂存，`.codex-next-goal.md` 与 `docs/goals/**` 未提交。

## 已完成目标：新增玩家「我的突破统计」派生统计视图

新功能（非修复）。目标文档 `.codex-next-goal.md`（local-only，未提交；本目标由人工命名，
自动命名辅助 HTTP 400 属辅助能力故障，未构成阻塞）。

**缺口证据**：突破的四个原始计数一直在累积（`player_data.statistics` 实有 59 列，实测含
`突破次数`/`突破成功`/`突破失败`/`突破损失修为`/`师父突破返修`/`徒弟突破回馈`），
但玩家侧**没有任何派生视图**——唯一入口 `修仙数据`（`view_data_`，
`xiuxian_base/__init__.py:1807`）只是把 55 个字段原样裸罗列
（`:1824` `stats_message += f"- {key}：{number_to(formatted_value)}\n"`），
没有成功率、没有累计损失修为的汇总，玩家要自己在 55 行里挑 4 个数字做除法。
`grep -rn "突破统计\|突破记录\|成功率"` 在玩家侧零匹配（竞技场胜率是竞技场自己的字段）。

交付内容：

- 新玩家命令 `我的突破统计`（别名 `突破统计`、`突破记录`，priority 7，`block=True`，
  注册在 `xiuxian/xiuxian_base/breakthrough_tribulation.py`，与 `突破` 同文件）；
  展示突破次数、成功、失败、成功率、累计损失修为。
- 新模块 `features/base/breakthrough_stats.py`：
  `BreakthroughStatsApplication(player_db)` 以只读 UoW 读 `statistics` 单行并派生；
  `format_breakthrough_stats()` 为纯函数（无 I/O、无 NoneBot）。
- **口径不臆造**：成功率分母恒为 `突破次数`（`成功/次数`，保留 1 位小数）。
  当 `成功+失败 != 次数`（历史脏数据/旧版本只记部分计数）时置 `inconsistent`
  并在输出追加「（成功+失败 与 突破次数 不一致，成功率按突破次数计）」，
  **不静默改数、不用"成功+失败"顶替次数**。
- `突破次数 == 0` → 不计算、不显示成功率（避免除零与"0%"误导）；任一计数为负或非数字
  一律按 0 展示，不出现负成功率。
- **分段降级**：缺 `statistics` 表 / 四列全缺 / 库不可读 → `暂无突破统计数据。`；
  缺单列（如旧库无 `突破损失修为`）→ 该行按 0 展示、其余照常；无该玩家行 →
  `还没有突破记录，发送【突破】试试。`；空 `user_id` → `缺少玩家身份，无法查询突破统计。`。
  只读路径**不建表、不补列**。
- **玩家隔离**：`user_id` 只取 `check_user` 会话，handler 不解析 `args`/
  `extract_plain_text`，无法查询他人突破统计。
- 零 schema 变更：不新增表、列、索引、迁移版本、配置键或 Web 路由；不改突破写路径、
  成功率规则、境界数据或奖励；**不替换 `修仙数据` 的裸罗列视图**（既有合同）。
- 帮助与文档：`修仙帮助` 的「流水与总览」段追加该命令；`docs/features/base.md` 登记口径；
  既有 `tests/test_help_discoverability.py` 的 `EXPECTED` 扩到第六条。
- `features/base/manifest.py` 追加该命令 `CommandSpec`；
  `scripts/export_refactor_inventory.py` 使 `docs/refactor_inventory.json` 新增 1 条
  command 记录（+9 行）。
- 新增命令使 Phase-2 派生计数 646→647、backlog 332→333；
  `tests/test_phase2_legacy_path_gate.py` 按既有先例同步期望值，
  **冻结成员文件与其哈希保持不变**（临时还原后再次删除）。
- 测试：新 `tests/test_breakthrough_stats_view.py` `16/16`（`unittest.TestCase`），
  用**真实 `statistics` 形状**（player_db）与真实应用钉住：汇总取值与成功率
  （12/5/7 → 41.7%）、全成功 100.0%、零次数不显示成功率、
  **不一致行分母仍取次数（10/3/3 → 30.0%）且渲染含说明**、一致时不出现说明、
  玩家隔离、缺表/四列全缺/缺单列/无行/空 `user_id` 五条降级、
  负数钳制、千分位渲染、只读不建表，以及 handler 源合同
  （命令名/别名、`summary(str(user_info["user_id"]))`、无 `args`）。
  聚焦套件 `test_breakthrough_stats_view.py` + `test_help_discoverability.py` +
  `test_phase2_legacy_path_gate.py` + `test_source_quality.py` +
  `features/base/tests/` 合计 `410 passed`（冻结文件在位时）；
  `unittest discover -s tests -p 'test_breakthrough_stats*'` `16 OK`。
  architecture、inventory `--check`、progress `--json`（`failures: None`）真实 exit 0；
  `compileall` 与 `git diff --check` 干净。
- 交付与 CI：功能提交 `b7292a94` 的 develop run `41195694321` success；
  快进合入 main 后 main run `41196290315` success（headSha 均 `b7292a94`；
  `build-and-release` 按分支规则 skipped）。未部署、未读写真实游戏库、不发送真实消息；
  用户删除的四个文件保持未暂存，`.codex-next-goal.md` 与 `docs/goals/**` 未提交。

## 已完成目标：剩余六处列表检索与翻页续查

新功能（非修复，承接「列表检索翻页续查」与背包/宠物/宗门检索切片）。目标文档
`.codex-next-goal.md`（local-only，未提交）。

**缺口证据**：`世界BOSS商店`、`通天塔商店`、`历练商店`、`传承背包`、`仙肆查看`、
`查看修仙界物品` 是包内最后六处只认页码的列表——前四处写 `int(args)` 失败即静默回退第 1 页，
后两处用前缀循环只识别「类型紧贴页码」，玩家输入物品名/卡名时参数被无声吞掉；即便命中筛选，
翻页提示与按钮也只拼页码，第 2 页会退回未筛选集合。

- `features/base/keyword_page.py` 新增 `parse_typed_page_arg(raw, types)`：
  `命令 类型 [关键词] [页码]`，类型按最长优先（`辅修功法`/`合成丹药` 不被 `功法`/`丹药`
  抢先），兼容旧紧贴写法（`神通2`、`药材2`）；未知类型返回空串，沿用各 handler 原错误文案，
  因此不带关键词的旧输入逐字保持原行为。
- 六个 handler 接入共享 `matches_keyword` / `page_command`：商店按物品名/ID/描述筛选，
  `传承背包` 按卡名，`仙肆查看` 与 `查看修仙界物品` 按物品名/ID；MD `next_cmd`、
  纯文本提示、底部按钮全部携带关键词（`仙肆查看药材 灵芝 2`、`传承背包 画屏春 2`）。
- 总页数按命中集合计算；`empty_notice` 区分「没有包含该关键词的内容」与「本来就空」；
  `传承背包` 第 1 页的种类/总数与「筛选：「关键词」」行按命中集合统计，排序仍是
  `(-数量, 卡名)`。
- 零 schema/命令变更：不新增表、列、索引、迁移版本、配置键、命令或别名；派生计数
  `652 / 338` 与冻结清单不变（`test_phase2_legacy_path_gate` 临时还原冻结文件后 47/47）。
- 帮助与文档：`交易帮助` 改 `仙肆查看 类型 [关键词] [页码]`，`背包帮助` 改
  `查看修仙界物品+类型 [关键词] [页码]`，`世界BOSS帮助`/`通天塔帮助`/`历练帮助`/
  `传承帮助` 的商店与背包条目加 `[关键词|页码]` 与一行说明；`docs/features/base.md`
  新增「列表检索与翻页续查（共享合同）」，trade/back/boss/tower/training/impart
  各补一段，`docs/gameplay_notes.md` 新增玩家口径「列表筛选与翻页」。
- 测试：新 `tests/test_list_search_remaining_views.py` `27/27`（`unittest.TestCase`），
  驱动六个真实注册 handler（patch 数据源与发送缝隙）：筛选命中、第 2 页仍是筛选结果、
  翻页命令携带关键词、无关键词保持旧页码形式、旧紧贴写法兼容、无命中与空集合分开提示、
  MD 下一页链接编码、`parse_typed_page_arg` 与 `page_command` 往返、帮助文本与源合同
  （六视图必须共用 `keyword_page`，不得再拼字面量页码命令）。
  定向套件：`test_list_search_remaining_views` 27、`test_list_search_pagination`、
  `test_source_quality`、`test_boss_purchase_command_ingress`、
  `test_help_discoverability`、`test_impart_command_owner_contract` 与
  `features/{trade,boss,tower,training,back}/tests` 全绿；
  architecture、inventory `--check` 真实 exit 0；`compileall` 与 `git diff --check` 干净。
- 交付与 CI：功能提交 `bcc0745c` 的 develop run `41493961692` success；
  `--ff-only` 合入 main 后 main run `41494905967` success（headSha 均 `bcc0745c`；
  `build-and-release` 按分支规则 skipped）。未部署、未读写真实游戏库、未发真实 QQ 消息、
  未打 tag、未新建 Release；用户删除的四个文件保持未暂存，`.codex-next-goal.md` 未提交。

## 已完成目标：灵田读模型迁移与傀儡回执启动迁移

重构收敛（非新玩法）。目标文档 `docs/goals/goal-puppet-field-read-model.md`（local-only，未提交）。
承接 `docs/features/puppet.md` 的既有遗留项「灵田读模型仍待后续迁移」。

**缺口证据**：`我的灵田`（`puppet_info_handler`）仍走 `get_player_info(user_id, "mix_elixir_info")`，
在 `if not mix_elixir_info` 之前直接读 `['收取时间']` / `['灵田傀儡']`——缺行玩家抛 `KeyError`、
脏时间抛 `ValueError`；成熟窗口在 handler 内硬编码 `23 / 0.05`，与 `PuppetHarvestSqlRepository`
的注入参数各写一份。购买/收取仓储还在请求事务内 `CREATE TABLE IF NOT EXISTS`
`puppet_operations` / `puppet_harvest_operations`，与「请求路径不执行 DDL」的既有合同冲突；
`migrations.py` 只有 `puppet.001/.002`。

交付内容：

- 新启动迁移 `puppet.003`（game DB）：`CREATE TABLE IF NOT EXISTS` 预建两张回执表并登记版本；
  旧库已有表/回执原样保留。`plugin.py` 迁移目录注册，manifest 版本同步 `puppet.003`。
- `PuppetPurchaseSqlRepository` 与 `PuppetHarvestSqlRepository` 改为 schema 检查：
  缺表/缺列返回 `schema_missing`，零写入、不建表；正常路径的回执、CAS、幂等重放不变。
- 新 `features/puppet/field_rules.py`：`maturity_interval_hours` / `maturity_state` /
  `normalize_now` / `parse_harvest_time`；收获仓储与读模型共用同一成熟规则与时间归一。
- 新 `features/puppet/field_view.py`：`PuppetFieldApplication` 一次只读快照读
  `user_xiuxian`（level/stone/blessed_spot_flag/puppet_status）与 `mix_elixir_info` 真实列集；
  可选列缺失按 0，缺表/缺行/脏时间/库不可读分别降级；`puppet_status` 经注入的
  `puppet_application.get_status` 读取，保留 enable 开关的 feature owner。
  有 `puppet_harvest_operations` 时追加「最近7天收取」（笔数、灵石消耗、最近一次、上次收获），
  时间窗只用下界，脏 JSON 逐行跳过；`format_puppet_field` 为纯函数，保留既有五行文案。
- `我的灵田` handler 改调新读应用，不再 `get_player_info`、不再裸 `strptime`；
  `schema_missing` 在购买/升级与自动收取命令层都有明确文案。
- 兼容：`puppet.003` 幂等；旧库已有表不被重建；缺新迁移时写路径 fail closed、读路径省略
  最近收取但仍给基础信息；只读路径不建表、不补列。
- 文档：`docs/features/puppet.md` 新增「灵田读模型（我的灵田）」与 `puppet.003`/`schema_missing`
  说明，`docs/gameplay_notes.md` 补玩家口径；无新命令/路由/配置键，派生计数保持 `652 / 338`。

**测试**：新 `tests/test_puppet_field_view.py` 12/12（迁移幂等与回执保留、购买/收取缺表
`schema_missing` 且不建表、真实列集、成熟边界、7 天窗口下界与玩家隔离、五条降级、
status_reader 归属、渲染与 handler 源合同）。受影响套件：
`test_puppet_harvest_production_path` + `test_puppet_application_purchase` +
`test_puppet_operation_service` + `test_source_quality` 274 passed；
`features/puppet/tests` 6 passed；`test_puppet_field_view` + `test_puppet_harvest_service` +
`test_refactor_progress` 42 passed；`test_phase2_legacy_path_gate` 47/47（临时还原冻结文件后再次删除）。
三门禁 exit 0（progress `failures: None`，`exit_ready: False` 仍由用户删除的冻结文件导致）；
`compileall` 与 `git diff --check` 干净。
- 交付与 CI：功能提交 `bce21448` 的 develop run `41520619713` **failure**——两处既有源码/进度
  合同仍要求 `puppet_application.get_status(` 出现在 facade（`test_puppet_harvest_service`、
  `test_refactor_progress`）。修复提交 `f2fd56e1` 把 enable 开关读取经注入 `status_reader` 交回
  application，develop run `41521920609` success、`--ff-only` 合入 main 后 main run
  `41523252106` success（headSha 均 `f2fd56e1`；`quality` success、`build-and-release`
  skipped）。未部署、未读写真实游戏库、未发真实 QQ 消息、未打 tag；用户删除的四个文件保持
  未暂存，`docs/goals/**` 与 `.codex-next-goal.md` 未提交。

## 已完成目标：魔修领奖奖励结算域收敛

重构收敛（非新玩法）。目标文档 `docs/goals/goal-world-events-reward-domain.md`（local-only，未提交）。
承接 `docs/features/world_events.md` 既有遗留项「奖励随机池和贡献计算暂未迁移到新 domain，
仍由兼容命令提供」。

**缺口证据**：`xiuxian/xiuxian_world_events/__init__.py` 内联七个奖励常量
（灵石上限 5000 万、修为比例 5%、贡献下限 1%、灵签 id 20010/档位 0.10/上限 3、
随机池门槛 0.50）与九个奖励/贡献助手（贡献压档、BOSS/记录倍率、贡献文案、灵签档位、
无上判定、池筛选、抽取、记录分母/贡献、汇总），并在 `claim_demon_reward_` 里逐行拼装
贡献、灵石、修为比例、灵签与随机奖励；规则与领奖写入、消息文案混在同一文件，无法单独回归。

交付内容：

- 新 `features/world_events/reward_rules.py`（纯函数、无 I/O）：上述常量、
  `reward_contribution`（压入 `[0.01, 1.0]`）、`stone_reward`（贡献 × 上限并封顶）、
  `exp_reward_rate`（`0.05 × 贡献`，结算仍由 `percent_exp_reward(anchor="gap")` 承担）、
  `talisman_reward_count`（每 0.10 贡献 1 个、上限 3）、`is_random_reward_item`
  （特殊物品恒可抽；功法/辅修功法/神通需 rank>0；身法/瞳术与法器/防具需贡献 ≥ 0.50 或 rank>0；
  带 `fusion` 的装备排除）、`pick_random_reward`（无上仅 1% 保留，否则改抽非无上）、
  记录级 `record_wave`/`record_reward_base_hp`/`record_reward_contribution` 与
  `sum_reward_contribution`。
- 新 `features/world_events/reward_settlement.py`：`DemonRewardPlan` 与
  `build_demon_reward_plan(...)` 一次产出贡献/灵石/修为比例/灵签数量/随机奖励 id、物品与文案/
  奖励物品列表；随机奖励只在 `rng.random() <= 贡献` 时抽取（与历史一致）。
- facade 改为委派：奖励常量从 feature 域导入；`_reward_contribution`、
  `_format_contribution_percent`、`_demon_talisman_reward_count`、`_is_wushang_reward_item`、
  `_is_demon_random_reward_item`、`_get_demon_random_reward_pool`、`_pick_demon_random_reward`、
  `_record_reward_base_hp`、`_record_reward_contribution`、`_record_wave`、
  `_sum_reward_contribution` 全部保留历史名称但只调用 feature 规则；
  `claim_demon_reward_` 只做快照、调用组装、写回执与发送消息。
- 兼容：`world_event_state` 是历史 JSON 投影，字段缺失/为文本按默认值回退；已冻结的
  `reward_contribution` 优先于按伤害重算；本次不改奖励数值、领奖写入、operation/统计语义、
  迁移版本与消息格式，不新增表/命令/路由/配置键。
- 文档：`docs/features/world_events.md` 新增「奖励结算归属（feature 域）」段并改写已知限制。
- 测试：新 `tests/test_world_events_reward_domain.py` `21/21`（贡献/灵石/修为比例/灵签边界、
  池筛选含 `fusion` 排除与 rank 门、无上 1% 保留、结算组装与脏数据结构容错、
  facade 委派与规则模块纯净性源合同）；`tests/test_demon_wushang_reward_gate.py` 合同改钉
  feature 域所有权并新增真实规则用例；定向套件 `test_world_events_reward_domain` +
  `test_demon_wushang_reward_gate` + `test_demon_claim_service` + `test_demon_claim_repository` +
  `test_world_events_personal_contribution` + `test_world_events_claim_web` +
  `test_demon_event_lifecycle_application` + `test_demon_wave_refresh_application` 合计
  `45 passed`；`test_source_quality` + `test_progression_source_table` 等合计 `279 passed`；
  `features/world_events/tests` 隔离运行 `2 passed`。
  三门禁真实 exit 0（progress `failures: None`；`exit_ready: False` 仍由用户删除的冻结文件导致，
  11 条既有 blocker）；`compileall` 与 `git diff --check` 干净。
- 交付与 CI：功能提交 `7b4fe0b8` 的 develop run `41549499847` success；`--ff-only` 合入 main 后
  main run `41550561041` success（headSha 均 `7b4fe0b8`；`quality` success、
  `build-and-release` skipped）。未部署、未读写真实游戏库、未发真实 QQ 消息、未打 tag；
  用户删除的四个文件保持未暂存，`docs/goals/**` 与 `.codex-next-goal.md` 未提交。

## 已完成目标：灵田收取域收敛（成熟窗口/抽取规则 + 回执表启动迁移）

重构收敛（非新玩法）。目标文档 `docs/goals/goal-mixelixir-harvest-domain.md`（local-only，未提交）。
承接 `docs/features/mixelixir.md` 的两处遗留：命令侧内联成熟窗口/抽取，以及四张炼丹回执表
在请求事务内 `CREATE TABLE IF NOT EXISTS`（与仓库级「请求路径不执行 DDL」合同冲突）。

**缺口证据**：`xiuxian/xiuxian_mixelixir/__init__.py` 的 `yaocai_get_` 内联
`{"time_cost": 23, "加速基数": 0.05}` 与裸 `strptime`；`last_time != 0` 对历史文本列上的 `'0'`
成立，新号直接 `ValueError`，缺列/缺行则 `KeyError`，窗口常量还与收获仓储各写一份。数量合计
（`灵田数量 + 收取等级 + 传承 + 功法`）、逐次 `choice` 去重与「恒心草」兜底也全部内联。
四张回执表（harvest / settlement / harvest_level_upgrade / fire_control_upgrade）在四个 feature
仓储与 legacy `transaction_service` 的请求事务里建表。

交付内容：

- 新 `features/mixelixir/harvest_rules.py`（纯函数、无 I/O）：`maturity_state()` 三态
  （`ready`/`waiting` 含剩余小时/`invalid`），窗口 `round(23 × (1 − 0.05 × 药材速度), 2)`；
  `is_never_harvested()` 把 `0`/`'0'`/空串/非法时间统一视作「从未收取」而不抛错；
  `harvest_count()` 数量合计（下限 0）；`roll_rewards()` 逐次抽取按 id 去重保持首次顺序、
  候选为空时兜底 `(3001, "恒心草", 数量)`；`remaining_text()` 保持 `X小时Y分钟` 文案；
  `normalize_rewards()` 兼容 mapping/元组两种 payload 形状。
- 新 `features/mixelixir/harvest_view.py`：`MixelixirHarvestFieldApplication.snapshot()` 单只读 UoW +
  player 附库读真实列集，缺 `user_xiuxian`/缺 `mix_elixir_info`/缺可选列一律 fail-soft，
  不建表不补列；无行时按「首次收取」返回 `invalid`。
- `migrations.py` 新增 `apply_mixelixir_operations`（含 `OPERATIONS_TABLE_DDL`/`OPERATIONS_TABLE_COLUMNS`），
  `plugin.py` 注册 `Migration("mixelixir.004", "mixelixir_operations", apply_mixelixir_operations)`。
- 四个 feature 仓储改为 `_operations_ready` schema 检查，缺表/缺列返回 `schema_missing` 且零变化；
  请求路径不再建表。迁移语句保持 `IF NOT EXISTS`，已按惰性路径建表的历史库保留全部回执行。
- 命令入口改调 feature 规则与只读快照；消息状态分支保持历史形态并新增 `schema_missing` 提示。
- 兼容：`收取时间` 历史文本列、`'0'`/空串/脏值等价于首次收取；重放语义不变；不改奖励数值、
  抽取分布、写入语义与 operation 语义；不新增表/命令/路由/配置键。
- 文档：`docs/features/mixelixir.md` 新增「灵田收取（成熟窗口 / 数量 / 抽取）」段，改写「数据模型与迁移」
  与「灰度开关、回滚和已知限制」，补定向测试命令；`docs/refactor_inventory.json` 重新导出（新增 `migrated_command_aliases` 等为空，表清单无漂移）。
- 测试：新 `tests/test_mixelixir_harvest_domain.py` `25/25`（窗口边界、脏时间/缺行/缺列容错、
  数量合计、抽取去重与兜底、请求路径无 DDL 与 `schema_missing`、启动迁移建四表并复用历史表、
  facade 委派与模块纯净性源合同）；4 个 feature 仓储夹具与 `tests/test_mixelixir_upgrade_production_path.py`
  夹具补 `mixelixir.004`（后者移除了自建回执表改为用启动迁移）。定向套件
  `test_mixelixir_harvest_domain` + `test_mixelixir_upgrade_production_path` + `test_source_quality` +
  `test_mixelixir_harvest_service` + `test_mixelixir_settlement_service` + `test_mixelixir_refine_claim_boundary` +
  `test_refactor_progress` + 4 个 feature 仓储测试合计 `329 passed`；扩大集（含
  `test_mixelixir_refine_cost_service`/`_refine_reward_service`/`_fire_control_upgrade`/`_harvest_level_upgrade`/`_recipe_service`）
  合计 `340 passed`；`features/mixelixir/tests` 隔离运行 `8 passed`。
  三门禁真实 exit 0（progress `failures: None`；`exit_ready: False` 仍仅由用户删除的冻结文件造成，
  11 条既有 blocker）；`test_phase2_legacy_path_gate` `47/47`（临时还原后删除，未暂存）；
  `compileall` 与 `git diff --check` 干净。
- 交付与 CI：功能提交 `d1920603` 的 develop run `41562018713` success；`--ff-only` 合入 main 后
  main run `41562844978` success（headSha 均 `d1920603`；`quality` success、`build-and-release` skipped）。
  未部署、未读写真实游戏库、未发真实 QQ 消息、未打 tag；用户删除的四个文件保持未暂存，
  `docs/goals/**` 与 `.codex-next-goal.md` 未提交。

## 已完成目标：本命法宝回执表启动迁移

重构收敛（非新玩法）。目标文档 `docs/goals/goal-natal-treasure-operations-schema.md`（local-only，未提交）。
承接 `docs/features/natal_treasure.md` 的「历史表仍由旧服务维护」与仓库级「请求路径不执行 DDL」合同。

**缺口证据**：六张回执表都在请求事务内 `CREATE TABLE IF NOT EXISTS` ——
`natal_awaken_operations`（player DB immediate）、`natal_reawaken_operations` /
`natal_training_operations` / `natal_effect_upgrade_operations` / `natal_engraving_operations` /
`natal_forget_operations`（game DB + ATTACH player_data）。此前只有 `natal_treasure.001` 一个版本标记，
没有任何启动迁移承载这些表，缺表时仓储顺手建表，把 DDL 混进资产事务。

交付内容：

- `features/natal_treasure/migrations.py` 新增 `GAME_OPERATIONS_TABLE_DDL/COLUMNS`（五表）、
  `PLAYER_OPERATIONS_TABLE_DDL/COLUMNS`（觉醒表）与合并视图，以及
  `apply_natal_treasure_operations`（game，`natal_treasure.002`）与
  `apply_natal_treasure_player_operations`（player，`natal_treasure.003`）。
- `plugin.py` 注册 `Migration("natal_treasure.002"/"natal_treasure.003", ...)`，并把
  `natal_treasure.003` 加入 player 库迁移白名单与 game 库排除集（与既有 `mixelixir.003` 同处理）。
- 六个 feature 仓储改为 `OPERATION_TABLE/OPERATION_COLUMNS` + `_operations_ready` schema 检查，
  缺表**或缺任一读取列**都返回 `schema_missing`；移除请求期 `CREATE TABLE`。
- 命令入口四个 handler 的 `failure_reasons` 补 `schema_missing` 提示，成功路径与文案形态不变。
- 兼容：迁移语句保持 `IF NOT EXISTS`，已按惰性路径建表的历史库保留全部回执行；不改数值规则、
  随机种子派生、返还/消耗口径与 operation 语义；不新增表/命令/路由/配置键；不动 `natal_treasure`
  本表与旧 `transaction_service` 的六个兼容 service。
- 文档：`docs/features/natal_treasure.md` 改写「数据模型与迁移」「事务与失败回滚」「测试与手工验收」
  「灰度开关、回滚和已知限制」，补 Manifest 命令/别名/config/migration 清单；
  `docs/refactor_inventory.json` 重新导出无漂移。
- 测试：新 `tests/test_natal_operations_schema.py` `15/15`（迁移建表形状与列集、请求路径无 DDL 且
  六条动作各自 `schema_missing` 零资产变化、半成品旧表被拒、历史回执行保留、迁移后同一调用
  成功/重放可用）；5 个 feature 仓储夹具 + `tests/test_natal_awaken_service.py` 夹具补对应迁移。
  定向套件（含 `test_source_quality`/`test_refactor_progress`/`test_rift_boss_battle_source_contract`/
  全部 natal 服务与仓储测试）`343 passed`，唯一失败是 `test_info_frozen_command_owner` ——
  临时还原用户删除的 `docs/refactor_phase2_legacy_path_items.json` 后 51 项全绿（该文件已立即删除、
  未暂存），确认非本切片回归。三门禁真实 exit 0（progress `failures: None`；`exit_ready: False` 仍仅由
  该冻结文件缺失造成，11 条既有 blocker）；`compileall` 与 `git diff --check` 干净。
- 交付与 CI：功能提交 `49f3b7de` 的 develop run `41572948906` success；`--ff-only` 合入 main 后
  main run `41573740823` success（headSha 均 `49f3b7de`；`quality` success、
  `build-and-release` skipped）。未部署、未读写真实游戏库、未发真实 QQ 消息、未打 tag；
  用户删除的四个文件保持未暂存，`docs/goals/**` 与 `.codex-next-goal.md` 未提交。

## 已完成目标：世界首领惩罚/状态表的启动迁移收敛

重构收敛（非新玩法），承接 `docs/features/boss.md` 与仓库级「请求路径不执行 DDL」合同。

**缺口证据**：`features/boss/punishment_repository.py` 在 `snapshot`、`get_result`、`punish`
共四处请求期 `CREATE TABLE IF NOT EXISTS` 建 `world_boss_state`
（`state_key`/`bosses`/`updated_at`/`revision`）与 `world_boss_punishment_operations`
（`operation_id`/`payload`/`result_json`/`created_at`）；`boss.001`~`boss.006` 只覆盖生命周期、
限额、全量刷新与战斗回执，从未建这两张表。

交付内容：

- 新增 `features/boss/schema.py`：`SCHEMA_MISSING`、`STATE_COLUMNS`、
  `PUNISHMENT_OPERATION_COLUMNS`、`TABLE_COLUMNS`、`columns()`/`table_ready()`。
- `features/boss/migrations.py` 新增 `apply_boss_punishment_operations`；
  `plugin.py` 按版本序注册 `boss.007`（game 库；catalog 308→309 条且单调）。
- `punishment_repository.py` 删除四处请求期 DDL：`punish` 缺表/缺列返回 `schema_missing`
  且不写首领状态与回执；`snapshot` 降级为 `([], 0)`；`get_result` 返回 `None`。
- 测试：新增 `tests/test_boss_punishment_schema_ownership.py`（130 行，6 项）：两张表由启动迁移
  建出且幂等、仓储源码无 DDL、未迁移库写路径 `schema_missing` 零建表、半成品
  `world_boss_state` 被拒且不被补列、迁移后 punish 正常 applied 并重放 `duplicate`。
  `features/boss/tests/test_punishment_repository.py` 夹具改为先跑 `boss.007`。
- 定向测试：新套件 6/6；`features/boss/tests` 隔离 6/6；合并集 280/280；
  含冻结清单门禁 53/53（临时还原后立即删除）。
- 门禁：`check_architecture.py`、`export_refactor_inventory.py --check`、
  `check_full_refactor_progress.py --json` 均 exit 0（`failures: None`）；`compileall`、
  `git diff --check` 干净。
- 兼容：不新增表/命令/路由/配置键；首领惩罚数值与 revision CAS 语义未改；迁移一律
  `IF NOT EXISTS`，历史库回执行保留。
- 交付与 CI：功能提交 `6c6830ce` 的 develop run `41735661436` success、
  main run `41736799935` success（headSha 均 `6c6830ce`；`quality` success、
  `build-and-release` skipped）。未部署、未读写真实游戏库、未发真实 QQ 消息、未打 tag；
  用户删除的四个文件保持未暂存，`docs/goals/**` 与 `.codex-next-goal.md` 未提交。

## 已完成目标：宗门（sect）操作回执表的启动迁移收敛

重构收敛（非新玩法），承接 `docs/features/sect.md` 与仓库级「请求路径不执行 DDL」合同。

**缺口证据**：11 份仓储在**业务方法写事务内**各自 `CREATE TABLE IF NOT EXISTS` 建回执表，
共 12 张（入宗开关是动态的 `sect_{open,close}_join_operations`）：`sect_creation_operations`、
`sect_name_refresh_operations`、`sect_operations`、`sect_inactive_disband_operations`、
`sect_buff_search_operations`、`sect_practice_operations`、`sect_elixir_room_operations`、
`sect_open_join_operations`/`sect_close_join_operations`、`sect_owner_inherit_operations`、
`sect_close_mountain_operations`、`sect_daily_reset_operations`。

交付内容：

- 新增 `features/sect/schema.py`：`SCHEMA_MISSING`、11 组列契约、`OPERATION_TABLES`（12 张表）、
  `JOIN_STATE_TABLES`、`columns()`/`table_ready()`。
- `features/sect/migrations.py` 新增 `_SECT_OPERATION_DDL`（12 条显式 DDL）与
  `apply_sect_operations`；`plugin.py` 按版本序注册 `sect.019`（game 库；catalog 307→308 条且单调）。
- 11 份仓储删除请求期 DDL，改为方法内布尔守卫 `_ready(uow)`；缺表或缺本次要读的列返回
  `schema_missing`，不回写宗门资产、成员、贡献或任何回执行。三个原有 `-> None` 辅助方法
  （`disband_repository._ensure`、`close_mountain_repository.ensure_schema`、
  `owner_inherit_repository.ensure_schema`）改为布尔守卫，避免返回值被调用方丢弃。
- 测试：新增 `tests/test_sect_schema_ownership.py`（197 行，6 项）：12 张表由启动迁移建出且幂等、
  仓储源码无 DDL、五个仓储都引用共享契约、未迁移库上 11 条写路径全部 `schema_missing` 且零建表、
  半成品回执表被拒且不被补列。
  `features/sect/tests/fixtures.py` 新增 `migrate_sect`，11 个仓储测试夹具接入；
  `tests/test_sect_command_contract_production_path.py` 的真实 schema 夹具补跑 `sect.019`。
- 定向测试：新套件 6/6；`features/sect/tests` 隔离 82/82；生产路径契约套件 25/25；
  两者合并 31/31；含冻结清单门禁 53/53（临时还原后立即删除）。
- 门禁：`check_architecture.py`、`export_refactor_inventory.py --check`、
  `check_full_refactor_progress.py --json` 均 exit 0（`failures: None`）；`compileall`、
  `git diff --check` 干净。
- 兼容：不新增表/命令/路由/配置键；宗门贡献、建设度、洗练成本等数值未改；迁移一律
  `IF NOT EXISTS`，历史库回执行保留。
- 交付与 CI：功能提交 `b21de1ba` 的 develop run `41730486737` success、
  main run `41731333440` success（headSha 均 `b21de1ba`；`quality` success、
  `build-and-release` skipped）。未部署、未读写真实游戏库、未发真实 QQ 消息、未打 tag；
  用户删除的四个文件保持未暂存，`docs/goals/**` 与 `.codex-next-goal.md` 未提交。

## 已完成目标：共享经济账本 economy_log 的 schema 归属

重构收敛（非新玩法），承接仓库级「请求路径不执行 DDL」合同与经济/奖励结算边界。

**缺口证据**：`xiuxian/xiuxian_utils/economy_log.py::ensure_economy_log_table()` 在**每次**
`log_economy_change()` 写入时建表、逐列 `ALTER` 并建五个索引；而
`tasks`/`dufang`/`auction`/`admin_asset`/`sign_in` 的迁移各自只建了精简表。真正被写入的
权威列集（`sect_id`、`sect_contribution_delta`、`sect_scale_delta`、`sect_materials_delta`、
`event_id`）只存在于这段请求期 DDL 里 —— 启动迁移建的是另一份形状，请求期再补齐。

交付内容：

- 新增 `infrastructure/database/economy.py`：`ECONOMY_LOG_COLUMNS`、`ECONOMY_LOG_INDEXES`、
  `ECONOMY_LOG_DDL`、`apply_economy_log()`、`columns()`/`indexes()`/`schema_ready()`，
  作为共享账本形状的唯一来源。
- `features/economy_ledger/migrations.py` 新增 `apply_economy_log_schema` 并登记
  `economy_ledger.002`；`plugin.py` 按版本序注册（`economy_ledger.001` 之后、`fusion.002` 之前），
  catalog 306→307 条且保持单调。
- `ensure_economy_log_table()` 改为「已就绪即返回，不执行任何 DDL」；仅在表整体缺失时
  用同一份 `ECONOMY_LOG_DDL` 兼容兜底（供未跑启动迁移的库/测试夹具）。
  奖励口径未变：`stone_delta`/`exp_delta`/道具增量与 `event_id` 幂等语义逐字保留。
- 测试：新增 `tests/test_economy_log_schema_ownership.py`（112 行，6 项）：启动迁移建出
  权威列集与全部索引、幂等、写入口列集与迁移一致、就绪分支不含 `ALTER TABLE`、
  就绪探测零写入、奖励列与 `event_id` 唯一索引冻结。
  `features/economy_ledger/tests/test_slice_contract.py` 的迁移声明契约同步为
  「清单版本为首个声明 + 固定顺序声明」，并断言新迁移可调用。
- 定向测试：新套件 6/6；economy 相关合并集 355/355；`features/economy_ledger/tests` 隔离 11/11；
  含冻结清单门禁合并集 53/53（临时还原后立即删除）。
- 门禁：`check_architecture.py`、`export_refactor_inventory.py --check`、
  `check_full_refactor_progress.py --json` 均 exit 0（`failures: None`）；`compileall`、
  `git diff --check` 干净。
- 交付与 CI：功能提交 `426386e3` 的 develop run `41682316741` **failure**（`economy_ledger`
  slice 契约钉住「迁移元组只有一版」）→ 契约同步提交 `c24c9c65` 的 develop run
  `41683764690` success、main run `41685097231` success（headSha `c24c9c65`；`quality` success、
  `build-and-release` skipped）。未部署、未读写真实游戏库、未发真实 QQ 消息、未打 tag；
  用户删除的四个文件保持未暂存，`docs/goals/**` 与 `.codex-next-goal.md` 未提交。

## 已完成目标：宠物（pet）回执表启动迁移收敛

重构收敛（非新玩法），承接 `docs/features/pet.md` 与仓库级「请求路径不执行 DDL」合同。

**缺口证据**：`features/pet/migrations.py` 只有 `apply_pet`/`apply_pet_hatch`/`apply_pet_skill_replace`/
`apply_pet_travel_claim` 四条迁移；`features/pet/repository.py` 仍在六个写路径里各自
`CREATE TABLE IF NOT EXISTS <回执行>` 惰性建表：`pet_active_switch_operations`（出战切换）、
`pet_feed_operations`（喂养）、`pet_travel_start_operations`（派遣出发）、
`pet_release_operations`（放生）、`pet_fusion_breakthrough_operations`（融合突破）、
`pet_skill_reroll_operations`（技能洗练）。

交付内容：

- 新增 `features/pet/schema.py`：`SCHEMA_MISSING`、六张表的列契约、
  `PLAYER_OPERATION_TABLES`/`GAME_OPERATION_TABLES`、`columns()`/`operations_ready()`、
  `PetSchemaMissing(DomainError)`。
- `migrations.py` 新增两条真实迁移，按仓储实际主库拆分：`pet.005`（player 库：
  出战切换/派遣出发/融合突破）、`pet.006`（game 库：喂养/放生/技能洗练）。
  `plugin.py` 按版本序注册（`pet.004` 之后、`platform.001` 之前），`pet.005` 加入
  player 白名单与 game 排除集；catalog 304→306 条且保持单调。
- `repository.py` 六处请求期 `CREATE TABLE` 全部删除，改为方法首步 `operations_ready` 校验；
  缺表或缺本次要读的列返回 `schema_missing`，不改动宠物、背包与任何回执行。
- 测试：新增 `tests/test_pet_schema_ownership.py`（198 行，9 项）：两条迁移的建表形状与幂等、
  player/game 归属不串库、仓储源码无 DDL、六条写路径未迁移时 `schema_missing` 且零建表、
  半成品回执表被拒且不被补列、迁移后出战切换与派遣出发正常 applied 并重放 `duplicate`。
  feature 夹具新增 `migrate_player`/`migrate_game`，六个仓储测试与 `test_pet_application` 改用它。
- 定向测试：新套件 9/9；`features/pet/tests` 隔离 30/30；pet 相关与门禁合并集 318/318；
  含冻结清单门禁的合并集 96/96（临时还原后立即删除）。
- 门禁：`check_architecture.py`、`export_refactor_inventory.py --check`、
  `check_full_refactor_progress.py --json` 均 exit 0（`failures: None`）；`compileall`、
  `git diff --check` 干净。
- 兼容：不新增表/命令/路由/配置键；宠物数值、抽取概率与技能池未改；迁移一律
  `IF NOT EXISTS`，历史库回执行保留。
- 交付与 CI：功能提交 `c5559667` 的 develop run `41671742605` success、
  main run `41672978406` success（headSha 均 `c5559667`；`quality` success、
  `build-and-release` skipped）。未部署、未读写真实游戏库、未发真实 QQ 消息、未打 tag；
  用户删除的四个文件保持未暂存，`docs/goals/**` 与 `.codex-next-goal.md` 未提交。

## 已完成目标：前世（past_life）全域 schema 归属与 fail-closed

重构收敛（非新玩法），承接 `docs/features/past_life.md` 与仓库级「请求路径不执行 DDL」合同。

**缺口证据**：`features/past_life/migrations.py` 只有标记迁移 `legacy.past_life.001`（建
`past_life_feature_migrations` 并写一行），**没有任何真实建表**；四份仓储在请求事务内惰性建表并补列：
`start_repository`（`player_data.past_life` 全列、`player_data.statistics."前尘往事次数"`、
`past_life_start_operations`）、`choice_repository`（同表 + `past_life_choice_operations`）、
`final_repository`（同表 + `past_life_final_operations` 及补列 + 共享 `economy_log`）、
`reset_repository`（同表 + `past_life_reset_operations` 及补列 + `past_life_reset_targets`）。
文档也明写「legacy tables remain the compatibility data source」。

交付内容：

- 新增 `features/past_life/schema.py`：`SCHEMA_MISSING`、`PAST_LIFE_COLUMNS`（`user_id` + 19 字段）、
  五张回执表列集、`PLAYER_TABLES`/`GAME_TABLES` 形状、`columns()`/`table_ready()`/`tables_ready()`、
  `PastLifeSchemaMissing(DomainError)`。
- `migrations.py` 拆成两条真实迁移：`past_life.002`（player 库：`past_life` 全列 +
  通过共享 `ensure_statistics_column` 补 `statistics."前尘往事次数"`）、
  `past_life.003`（game 库：五张回执表）。`plugin.py` 按版本序注册，`past_life.002` 进 player 白名单
  与 game 排除集，`past_life.003` 留在 game 库。catalog 303→304 条且保持单调。
- 四份仓储删除请求期 `CREATE TABLE`/`ALTER TABLE`，改 `table_ready` 门禁；回执表按主库（game）校验、
  `past_life` 按附库（`player_data.`）校验。缺表或缺本次要读的列返回 `schema_missing`：
  `start`/`advance`/`settle`/`reset_one`/`reset_all_create`/`reset_all_batch` 全部零写入。
- `economy_log` 明确不由前世声明所有权（`tasks`/`dufang`/`auction`/`admin_asset`/`sign_in`
  的迁移各自 `IF NOT EXISTS` 建），前世 `settle` 在它缺失时也 fail-closed。
- 命令层：全服重置与单用户重置的失败分支区分 `schema_missing`，不再误报
  「已有不同模式的任务」/「operation 冲突」。
- 兼容：`xiuxian/xiuxian_past_life/transaction_service.py` 保留为显式回滚入口（仍在自身事务内建表），
  定向测试钉住 legacy 包内只有它允许出现 DDL；数值/奖励/评分/抽取口径未改。
- 测试：新增 `tests/test_past_life_schema_ownership.py`（283 行，14 项）：两条迁移的建表形状与幂等、
  `economy_log` 不归前世、请求路径无 DDL（源码合同）、四个写路径未迁移时 `schema_missing` 且零建表、
  半成品 `past_life` 被拒且不被补列、缺 `economy_log` 阻断终局结算、迁移后 reset/final/choice
  正常 applied 并重放 `duplicate`。feature 测试夹具改为先跑 `past_life.002`+`past_life.003`。
- 定向测试：新套件 14/14；past_life 9 个既有套件 41/41；`features/past_life/tests` 隔离 8/8；
  合并集 329/329；含冻结清单门禁的合并集 87/87（临时还原后立即删除）。
- 门禁：`check_architecture.py`、`export_refactor_inventory.py --check`、
  `check_full_refactor_progress.py --json` 均 exit 0（`failures: None`）；`compileall`、
  `git diff --check` 干净。
- 交付与 CI：功能提交 `87b10a10` 的 develop run `41659267095` success、
  main run `41660823344` success（headSha 均 `87b10a10`；`quality` success、
  `build-and-release` skipped）。未部署、未读写真实游戏库、未发真实 QQ 消息、未打 tag；
  用户删除的四个文件保持未暂存，`docs/goals/**` 与 `.codex-next-goal.md` 未提交。

## 已完成目标：竞技场全域 schema 归属与 fail-closed

重构收敛（非新玩法），承接 `docs/features/arena.md` 与仓库级「请求路径不执行 DDL」合同。

**审计结论**：`features/arena/` 与 legacy `xiuxian/xiuxian_arena/` 的请求路径**已无任何 DDL**
（`CREATE TABLE`/`ALTER TABLE` 只在 `features/arena/migrations.py`；上一轮已删掉
`weekly_rank_repository._ensure_schema`）。剩余缺口是同一合同的其余部分未覆盖：
`repository.py`（四个写路径 + 两个回执读）、`state_repository.py`、`season_reward_repository.py`、
`opponent_repository.py` 均无 schema 校验，未迁移库上直接抛 `sqlite3.OperationalError`；
`season_reward_repository.claim` 用 `PRAGMA` 查到缺列后返回 `state_changed`，把 schema 缺失误报成状态变更。

交付内容：

- 新增 `features/arena/schema.py`：`SCHEMA_MISSING`、`GAME_TABLES`/`PLAYER_TABLES` 的完整形状
  （7 张表，对齐 `arena.002`~`arena.010`）、`columns()`/`table_ready()`/`tables_ready()`、
  `ArenaSchemaMissing(DomainError)`。校验口径是「表存在 + 本次要读的列齐全」。
- `repository.py`：`purchase` / `purchase_challenges` / `use_challenge_ticket` / `settle` 四个写路径
  与 `purchase_result`/`settlement_result` 两个回执读路径接契约；缺表或缺列返回
  `schema_missing`（读路径 `None`），不写回执、不动 `arena`/`back`/`user_xiuxian`。
- `state_repository.py`：`initialize` 抛 `ArenaSchemaMissing`，`ranking` 返回空；
  `season_reward_repository.py`：`claim` 返回 `schema_missing`（不再误报 `state_changed`），
  新增 `schema_ready()`，`daily_candidates` 缺表返回空；
  `opponent_repository.py`：`candidates` 缺表返回空；`weekly_rank_repository.py` 改用共享 `table_ready`。
- facade：挑战、挑战次数购买、挑战券、兑换补 `schema_missing` 文案；三个 handler 增加
  `except DomainError` 直接展示 `exc.message`；`reset_arena_daily_challenges` 在 `schema_ready()`
  为假时记 warning 并跳过（不再打印「发放 0 名玩家」）。
- 测试：新增 `tests/test_arena_schema_ownership.py`（383 行，21 项）：迁移建出全部表列且可重跑、
  请求路径无 DDL（源码合同 + 五个仓储都引用共享契约）、四个写路径与赛季 `claim`/周降段在未迁移库上
  `schema_missing` 且零写入、半成品 `arena` 不被补列、状态初始化抛 `ArenaSchemaMissing`、
  读路径降级、应用层映射为 rejected（`code=schema_missing`）、迁移后降段仍正常。
  `tests/test_arena_weekly_rank_schema.py` 的仓储源码合同改钉共享契约。
- 定向测试：新套件 21/21；arena 15 个套件 90/90；`features/arena/tests` 隔离 21/21；
  `test_source_quality` + `test_refactor_progress` 269/269；含冻结清单门禁的合并集 114/114
  （临时还原 `docs/refactor_phase2_legacy_path_items.json` 后立即删除）。
- 门禁：`check_architecture.py`、`export_refactor_inventory.py --check`、
  `check_full_refactor_progress.py --json` 均 exit 0（`failures: None`，11 条既有 blocker 仍只由
  用户删除的冻结清单造成）；`compileall` 与 `git diff --check` 干净。
- 兼容：不新增表/命令/路由/配置键；段位表、初始分数、`_target()` 下标、`chunk_size`、CAS 三元组、
  荣誉/降段数值与 `duplicate`/`completed` 语义均未改动；迁移保持 `IF NOT EXISTS`，历史库回执行保留。
- 交付与 CI：功能提交 `39ad5f7f` 的 develop run `41650174237` success、
  main run `41651473877` success（headSha 均 `39ad5f7f`；`quality` success、
  `build-and-release` skipped）。未部署、未读写真实游戏库、未发真实 QQ 消息、未打 tag；
  用户删除的四个文件保持未暂存，`docs/goals/**` 与 `.codex-next-goal.md` 未提交。

## 已完成目标：竞技场每周降段的启动迁移收口

重构收敛（非新玩法），承接 `docs/features/arena.md` 与仓库级「请求路径不执行 DDL」合同。

**缺口证据**：`features/arena/weekly_rank_repository.py` 的 `_ensure_schema()` 在 `reduce()`
的降段/调度事务内执行 `CREATE TABLE IF NOT EXISTS arena(...)`、`ALTER TABLE arena ADD COLUMN`
补 `score/rank/win_streak`，以及 `CREATE TABLE IF NOT EXISTS arena_weekly_rank_reduction_operations`
与 `..._targets`。三张表其实已由启动迁移 `arena.006`（两张降段回执表）与
`arena.008`/`arena.009`（`arena` 及列）持有，请求路径重复一次启动迁移已拥有的 DDL。

交付内容：

- `weekly_rank_repository.py`：`_ensure_schema` 换成 `_schema_ready`（只查 `sqlite_master` 与
  `PRAGMA table_info("arena")`），`reduce()` 首步校验 `arena` + 两张回执表存在且
  `arena` 具备 `user_id/score/rank/win_streak`，缺表或缺列返回 `schema_missing` 且零写入；
  请求期 `CREATE TABLE`/`ALTER TABLE` 全部删除。新增 `OPERATION_TABLES`/`ARENA_COLUMNS` 常量。
- `xiuxian/xiuxian_arena/__init__.py`：调度入口 `reduce_arena_rank` 在进入 `while` 续跑循环
  **之前**短路 `schema_missing` 并记一条 warning，避免把它当 `applied` 反复空转。
- `tests/test_arena_weekly_rank_schema.py`（新，242 行，13 测试）：三条迁移的建表形状与列集、
  未迁移库 `schema_missing` 且零建表、半成品 `arena` 被拒且**不被 `ALTER` 补列**、
  缺一张回执表即整体拒绝且段位零变化、历史回执行在重跑迁移后保留、迁移后分块续跑至
  `completed` 与重放 `duplicate`、空榜 `completed`、冻结目标行、仓储源无 DDL、
  调度入口在 `await asyncio.sleep(0)` 之前短路、段位表与初始分数常量冻结。
- `tests/test_arena_weekly_rank_reduction.py`：夹具改为先跑 `apply_arena_state` +
  `apply_arena_weekly_rank_reduction` 再插 `arena` 行（此前靠请求期惰性建表），
  插入语句改为显式列名。
- `docs/features/arena.md`：新增「每周降段的 schema 归属」段与定向测试命令。
- 兼容：迁移语句保持 `IF NOT EXISTS`，已按旧惰性路径建表的历史库保留全部回执行；
  不改段位表、初始分数、`_target()` 下标、`chunk_size`、CAS 三元组与回执语义；
  不新增表/命令/路由/配置键；`arena_enabled` 灰度与 Web 路由行为不变。
- 定向测试：新套件 `13/13`；`tests.test_arena_weekly_rank_reduction` `11/11`；
  arena 相关 12 个套件 `45/45`；`features/arena/tests` 隔离 `21/21`；
  合并集（含 `test_platform_ledger_migration`/`test_source_quality`/`test_refactor_progress`/
  `test_refactor_architecture`，临时还原冻结清单后即删）`321/321`。
  三门禁 exit 0（progress `failures: None`、11 条既有 blocker 仍只由用户删除的冻结文件造成）；
  `compileall` 与 `git diff --check` 干净。
- 交付与 CI：功能提交 `37e2c706` 的 develop run `41621695411` success、
  main run `41623005648` success（headSha 均 `37e2c706`；`quality` success、
  `build-and-release` skipped）。未部署、未读写真实游戏库、未发真实 QQ 消息、未打 tag；
  用户删除的四个文件保持未暂存，`docs/goals/**` 与 `.codex-next-goal.md` 未提交。

## 已完成目标：虚神界回执/投影表启动迁移

重构收敛（非新玩法）。目标文档 `.codex-next-goal.md`（local-only，未提交）。
承接 `docs/features/impart_pk.md` 与仓库级「请求路径不执行 DDL」合同。

**缺口证据**：六条虚神界动作都在请求事务内 `CREATE TABLE IF NOT EXISTS` ——
`impart_closing_enter_operations` / `impart_closing_operations` / `impart_explore_operations` /
`impart_training_operations`（game 主库事务）、`impart_battle_batch_operations`（impart 主库事务）、
`impart_pk_state` / `impart_project_members` / `impart_project_meta` /
`impart_project_join_operations`（player 主库事务）。此前只有 `legacy.impart_pk.001` 一个版本标记，
没有任何启动迁移承载这些表。

交付内容：

- `features/impart_pk/migrations.py` 新增 `operations_ready(uow, table, schema)` 与按库拆分的
  三组 DDL/列集：`apply_impart_pk_operations`（game 四张回执表，`impart_pk.002`）、
  `apply_impart_pk_player_operations`（player 四张投影表，`impart_pk.003`）、
  `apply_impart_pk_impart_operations`（impart 战斗批回执，`impart_pk.004`）。
- `plugin.py` 把三条 Migration 按版本序插入 `build_migrations()`（必须在
  `LEGACY_MIGRATIONS` 展开之前，否则 catalog 不再单调、`MigrationRunner` 会拒绝构造），
  并把 `impart_pk.003` 加入 player 白名单、`impart_pk.004` 加入 impart 白名单与 game 排除集。
- 六个仓储改为 `operations_ready` 门禁：缺表**或缺任一读取列**返回 `schema_missing`
  且零资产/零统计变化；移除请求期 `CREATE TABLE`，并删除 `project_join` 里对
  `statistics` 的 no-op 建表（`ensure_statistics_column` 仍是唯一列写入者，契约不变）。
- 命令入口五处补 `schema_missing` 提示（含闭关的通用兜底分支），成功路径与文案形态不变。
- 兼容：迁移语句保持 `IF NOT EXISTS`，已按惰性路径建表的历史库保留全部回执行；不改数值口径、
  日限/统计计数、随机派生与 operation 语义；不新增表/命令/路由/配置键。
  旧 `transaction_service` 六个兼容 service 保留为显式回滚入口。
- 文档：`docs/features/impart_pk.md` 新增「回执/投影表的启动迁移（请求路径不执行 DDL）」段，
  补定向测试命令与回滚说明。
- 测试：新 `tests/test_impart_pk_operations_schema.py` `19/19`（三条迁移各自的建表形状与列集、
  六条动作在未迁移库上 `schema_missing` 且零资产/零统计变化、半成品旧表被拒、
  投影需四表齐全、历史回执行保留、迁移后同一操作号成功并重放、facade 源合同）；
  6 个 feature 仓储夹具与 `tests/test_impart_statistics_column_contract.py` 夹具补启动迁移。
  定向套件 `352 passed`；含 `test_impart_*` 与残余面 `376 passed`；feature 目录隔离运行 `9 passed`。
  三门禁真实 exit 0（progress `failures: None`；`exit_ready: False` 仍仅由用户删除的冻结文件造成，
  11 条既有 blocker）；`test_phase2_legacy_path_gate`/`test_info_frozen_command_owner`/
  `test_impart_pk_frozen_command_owner`（临时还原冻结文件后删除，未暂存）全绿；
  `compileall` 与 `git diff --check` 干净。
- 交付与 CI：功能提交 `394f36bd`（develop run `41584263513` **failure**：三条迁移原先注册在
  `LEGACY_MIGRATIONS` 之后，导致 catalog 非单调、`MigrationRunner` 拒绝构造）→ 修复提交
  `d02d4ff0`（改到 `impart.008` 之后、`LEGACY_MIGRATIONS` 之前）的 develop run `41586325225`
  success；`--ff-only` 合入 main 后 main run `41587210983` success（headSha 均 `d02d4ff0`；
  `quality` success、`build-and-release` skipped）。未部署、未读写真实游戏库、未发真实 QQ 消息、
  未打 tag；用户删除的四个文件保持未暂存，`docs/goals/**` 与 `.codex-next-goal.md` 未提交。

## 冻结清单与停止条件

冻结范围仅包括活动 Web 六来源一键代领 API、权限/错误合同、管理员面板、兼容性保护、聚焦测试、路由清单和正式文档。不得新增奖励经济、schema/数据迁移、玩家身份模型、后台活动编辑器、通用 UI 框架或 unrelated refactor。

成功条件：本轮目标的状态、持久化兼容、交互、聚焦测试和正式文档均完成；diff 检查通过；只提交目标代码、测试和正式文档到 develop，CI 成功且审查通过后普通合入 main 并验证 main CI。本轮按用户指示不部署、不停机、不读取或写入真实游戏数据；已知部署验收事实与本次功能发布门禁分开记录。

停止条件：Release 质量门不能在不违反“无全量测试”要求时完成、发现共享环境/数据冲突、冷备份 hash 不一致、迁移试验需要触碰真实库、活动能力缺少既有入口/查询、或验收需扩大到冻结范围之外。此时保留现场并报告具体阻塞，不绕过门禁或推测通过。

安装/更新/重启/测试串行取得 `flock -w 600 /tmp/nonebot-project-validation.lock`；短工作持锁，结束后释放。运行数据、凭据和 `boss_info.json` 永不暂存。

## 已完成目标：地图玩家状态表的启动迁移归属

重构收敛（非新玩法）。目标文档 `.codex-next-goal.md`（local-only，未提交）。
承接 `docs/features/map.md` 与仓库级「请求路径不执行 DDL」合同。

**缺口证据**：AST 扫描 `features/map/repository.py` 得 5 处请求期
`ALTER TABLE ... ADD COLUMN`，覆盖 `map_daily_limit` / `map_cooldown` / `map_status` /
`map_explore_status` / `map_mission`。全仓搜索确认：`map_daily_limit`、`map_status`、
`map_mission` **没有任何启动迁移创建过**（只有测试自建），`map_cooldown`、`map_explore_status`
由 `map.003`/`map.005` 建出但列不全 —— 归属完全缺失而非重复。

交付内容：

- 新增 `features/map/schema.py`：`SCHEMA_MISSING`、五张表的列契约
  （`DAILY_LIMIT_COLUMNS`/`COOLDOWN_COLUMNS`/`STATUS_COLUMNS`/`EXPLORE_STATUS_COLUMNS`/
  `MISSION_COLUMNS`）、`PLAYER_TABLES`、`columns()` 与 `table_ready()`（只读 `PRAGMA table_info`）。
- `features/map/migrations.py` 新增 `apply_map_player_state_schema`：`map.018`（player 库）建出
  五张表的完整列集，并对历史库补列（`CREATE TABLE IF NOT EXISTS` + 受 `PRAGMA` 保护的
  `ALTER`，保留既有行，重复执行幂等）。`plugin.py` 按版本序插入 `map.018`
  （catalog 309 → 310 单调），并同时登记 player 白名单与 game 排除集。
- `features/map/repository.py` 五处请求期 DDL 全部改门禁：`save_daily_limit`/`upsert`/
  explore `save`/mission `save` 在缺表或缺本次回读列时返回 `{"status": "schema_missing"}`
  且零写入；`set_cooldown` 返回 `None`；源码已无 `CREATE TABLE`/`ALTER TABLE`。保留
  `map_explore_status.reward_plan` 兼容分支（仅在契约列齐全时读取该遗留列）。
- 测试：四个 feature 写仓储夹具改用 `apply_map_player_state_schema` 建立受支持形状，并把
  原先钉住「请求期自动补列」的 7 个用例改钉新契约（历史/缺失 schema 返回 `schema_missing`
  且行数为 0、列未被添加）；新增 `tests/test_map_schema_ownership.py` `6/6`（迁移建表形状、
  幂等、历史库补列保留数据、仓储无 DDL 源合同、缺表零写入、迁移后正常写入）。
  定向 `features/map/tests` `115/115`；含架构与启动迁移契约的合并集 `34/34`。
  三门禁真实 exit 0（progress `failures: None`；`exit_ready: False` 仍仅由用户删除的冻结文件
  造成，11 条既有 blocker）；`test_phase2_legacy_path_gate`（临时还原冻结文件后删除，未暂存）
  `47/47`；`compileall` 与 `git diff --check` 干净。
- 兼容：迁移只搬 DDL，不改任何数值常量、算法或 operation 语义；不新增表、命令、路由或配置键。
  回退时先恢复代码版本并保留 `map.018` 已建出的表，不删除迁移数据。
- 文档：`docs/features/map.md` 新增五表归属表（归属迁移 + 契约列）与请求路径 fail-closed 说明。

## 已完成目标：消息库与赛季榜 schema 的启动迁移归属

重构收敛（非新玩法）。承接 `docs/adr/0003-multiple-databases.md` 与仓库级「请求路径不执行 DDL」合同。

**缺口证据**：`messages`、`user_nicknames` 只由
`xiuxian_utils/message_db.py::_ensure_message_db_schema` 在每次连接消息库时创建
（含 14 个索引与历史库 `messages.reference_id` 补列）；`season_rank` 只由
`xiuxian_utils/season_rank_service.py::ensure_season_rank_table` 在赛季榜每次读/写前创建。
两者都没有启动迁移，而 `message_db` 虽在 `DatabaseCatalog.KEYS` 中注册，
`migrations_for_database()` 却只给它 `platform.001` —— 归属完全缺失。

交付内容：

- 新增 `infrastructure/database/message_store.py`：三张表的权威 DDL/索引、列契约
  （`MESSAGES_COLUMNS` 17 列、`USER_NICKNAMES_COLUMNS` 8 列、`SEASON_RANK_COLUMNS` 9 列）、
  `apply_message_store_schema`（`message_db`）与 `apply_season_rank_schema`（`game_db`），
  以及 `table_columns`/`table_ready`/`schema_ready` 只读校验。
- `message.001` 只路由 `message_db`；`season_rank.001` 只路由 `game_db`
  （赛季榜需 JOIN 主库 `user_xiuxian`/`sects`，因此不属于消息库）。catalog 311 → 312 单调，
  `message.001` 列入 game 排除集、`season_rank.001` 列入 player/trade/impart/message 排除集。
- `message_db._ensure_message_db_schema` 改为只校验：未就绪抛
  `RuntimeError("消息库数据尚未就绪，请检查启动迁移。")` 且零建表。
- `season_rank_service.ensure_season_rank_table()` 改为返回 `bool`：未就绪返回 `False`，
  写路径返回 `None`、读路径返回 `[]`/`None`，均不建表。
- `message.001` 先补 `messages.reference_id` 再建引用该列的索引，历史库保留全部既有消息行；
  两条迁移均为 `IF NOT EXISTS`，可重复执行且不改变列类型/默认值/查询语义。
- 测试：新增 `tests/test_message_store_schema_ownership.py` `12/12`（双库路由、建表与索引、
  幂等、历史库补列保数据、无请求期 DDL 源合同、未迁移降级零建表、迁移后赛季榜与消息读写）；
  `tests/test_message_db_migration.py` 与 `tests/test_auction_settlement_projection_ids.py`
  夹具改为显式应用迁移。缺口相关合并集 `61/61`。
- 文档：`docs/adr/0003-multiple-databases.md` 新增 schema 归属表与降级/回滚说明。
- 明确排除：`economy_operations`（`repositories.py::transfer_stones` 在请求事务内创建）
  属于主玩家库的转账幂等回执，不在本目标范围，留作下一个独立切片。
- 三门禁真实 exit 0（progress `failures: None`；`exit_ready: False` 仍仅由用户删除的冻结文件
  造成，11 条既有 blocker）；`test_phase2_legacy_path_gate`（临时还原冻结文件后删除，未暂存）
  `47/47`；`compileall` 与 `git diff --check` 干净。

## 已完成目标：洞天福地与灵石修炼操作回执表的启动迁移归属

重构收敛（非新玩法）。目标文档 `.codex-next-goal.md`（local-only，未提交）。
承接 `docs/features/buff.md` 与仓库级「请求路径不执行 DDL」合同。

**缺口证据**：`features/buff/rename_repository.py` 与 `upgrade_repository.py` 在请求事务内
`CREATE TABLE IF NOT EXISTS blessed_spot_operations`，`stone_training_repository.py` 同样内联
`stone_training_operations`；`buff.001`~`buff.013` 从未创建这两张表。福地开垦、福地改名与
`灵石修炼` 都是真实玩家命令。实测 `BuffApplication.upgrade_field(..., cost=200, ...)` 抛
`ValidationError: expected_level and stone_cost are required`——handler 自 `08eeef62` 起传
`cost`，应用只接受 `stone_cost`，开垦命令在参数校验阶段必然失败。

交付内容：

- 新增 `features/buff/operation_schema.py`：`SCHEMA_MISSING`、两张表的列契约与 DDL、
  `operations_ready`/`stone_training_ready`（表存在 + 全列 + `operation_id` 主键）。
- `features/buff/migrations.py` 新增 `apply_blessed_spot_operations`（`buff.014`）与
  `apply_stone_training_operations`（`buff.015`），均 game DB；`plugin.py` 按版本序注册，
  catalog 312→314 单调；两版本只出现在 game 路由，player/trade/impart/message 均不包含。
- 三个仓储删除请求期 `CREATE TABLE`，事务首步校验契约；缺表/缺列返回 `schema_missing`，
  零资产与零回执写入，不建表、不补列。
- `灵田开垦` handler 改传 `stone_cost=cost`；福地开垦/改名在 `outcome.code == "schema_missing"`
  时提示「洞天福地数据尚未就绪，请检查启动迁移。」。
- 兼容：两条迁移均为 `IF NOT EXISTS`，旧库惰性建出的回执行与历史记录保留；
  `BlessedSpotService`/`StoneTrainingSettlementService` 仍是显式回滚入口（保留自身事务内建表）。
- 文档：`docs/features/buff.md` 数据模型改为 `buff.001` 至 `buff.015`，新增操作回执归属表、
  fail-closed 与回滚说明。
- 测试：新增 `tests/test_blessed_spot_schema_ownership.py` 11 项（两迁移建表形状与幂等、
  game-only 路由、旧回执行保留、未迁移/半成品表 `schema_missing` 且零写入零建表、
  真实 `BuffApplication` 的开垦/改名 applied-duplicate-state_changed、灵石修炼 applied/duplicate、
  请求路径无 DDL 与共享契约源合同、handler 参数与降级文案合同）。
  定向 `test_blessed_spot_schema_ownership.py` + 三个仓储套件 + `test_blessed_spot_service.py`
  `25 passed`；`test_source_quality.py` + `features/buff/tests` `261 passed`；
  `test_phase2_legacy_path_gate.py` + `test_refactor_closeout_recovery.py`
  `48 passed, 91 subtests passed`（临时还原冻结文件后删除，未暂存）。
- 三门禁真实 exit 0（progress `failures: None`；`exit_ready: False` 仍仅由用户删除的冻结文件
  造成，11 条既有 blocker）；`compileall` 与 `git diff --check` 干净。
- 交付与 CI：功能提交 `3c464252` 的 develop run `41906862840` success；
  `--ff-only` 合入 main 后 main run `41907846475` success（headSha 均 `3c464252`；
  `quality` success、`build-and-release` skipped）。未部署、未读写真实游戏库、未发真实 QQ 消息、
  未打 tag；用户删除的四个文件保持未暂存，`.codex-next-goal.md` 未提交。
- 明确排除：`economy_operations`（`xiuxian_utils/repositories.py::transfer_stones` 请求期建表，
  无 in-repo 生产调用方）留作下一独立切片。

## 已完成目标：`economy_operations` 转账幂等回执的启动迁移归属

重构收敛（非新玩法）。目标文档 `.codex-next-goal.md`（local-only，未提交）。
承接 `docs/adr/0003-multiple-databases.md` 与仓库级「请求路径不执行 DDL」合同。

**缺口证据**：`xiuxian_utils/repositories.py::EconomyRepository.transfer_stones` 在请求事务内
`CREATE TABLE IF NOT EXISTS economy_operations(operation_id, operation_type, created_at)`；
`economy_ledger.001/.002` 从未创建该表，全仓仅该文件 3 处引用。调用链为
`XiuxianDateManage.transfer_ls` 的 legacy 主库连接（`game_db`）；`repositories.py` 还会被
`tests/test_repositories.py` 以文件路径独立加载，不能引入顶层包级相对导入。

交付内容：

- 新增纯标准库 `infrastructure/database/economy_operations.py`：
  `ECONOMY_OPERATIONS_COLUMNS`、`ECONOMY_OPERATIONS_DDL`、`operations_ready(connection)`
  （raw legacy connection，兼容 tuple/Row），迁移与仓储共用同一字面 DDL。
- `features/economy_ledger/migrations.py` 新增 `apply_economy_operations`，
  `MIGRATIONS` 追加 `economy_ledger.003`；`plugin.py` 按版本序注册，catalog 314→315 单调；
  该版本只出现在 `game_db`，player/trade/impart/message 均不包含。
- `transfer_stones` 删除请求期 `CREATE TABLE`，改由 `_economy_operations_bind()` 取契约：
  包内相对导入失败时按文件路径加载 `economy_operations.py`（与 `_numeric_bind()` 同模式），
  保持独立文件加载测试可用。未迁移或缺列返回 `False`，零扣款、零入账、零回执，不建表。
- 兼容：迁移 `IF NOT EXISTS`，旧库惰性建出的回执行与历史记录保留；转账金额/事务语义、
  `update_stones` 等方法与回执 `operation_type='stone_transfer'` 未改。
- 文档：`docs/features/economy_ledger.md` 登记 `economy_ledger.002/.003` 与 fail-closed 口径；
  `docs/adr/0003-multiple-databases.md` 的 schema 归属表补 `economy_operations` 一行，
  并删除“留作下一独立切片”的过时说明。
- 测试：新增 `tests/test_economy_operations_schema_ownership.py` `7/7`（迁移建表形状与幂等、
  game-only 路由、旧回执行保留、未迁移 `False` 且零写入零建表、迁移后转账/重放/余额不足/
  收款方缺失回滚、独立文件加载 fallback、仓储源无 DDL）；`tests/test_repositories.py` 夹具
  改为显式应用迁移。定向 `test_economy_operations_schema_ownership.py` + `test_repositories.py`
  + `test_slice_contract.py` `15 passed`；含 `test_source_quality.py`、
  `test_refactor_architecture.py`、`test_dependency_lock.py` 的合并集 `281 passed`；
  `test_phase2_legacy_path_gate.py` + `test_refactor_progress.py`
  `70 passed, 91 subtests passed`（临时还原冻结文件后删除，未暂存）。
- 三门禁真实 exit 0（progress `failures: None`；`exit_ready: False` 仍仅由用户删除的冻结文件
  造成，11 条既有 blocker）；`compileall` 与 `git diff --check` 干净。
- 交付与 CI：功能提交 `6392b589` 的 develop run `41913786901` success；
  `--ff-only` 合入 main 后 main run `41915105030` success（headSha 均 `6392b589`；
  `quality` success、`build-and-release` skipped）。未部署、未读写真实游戏库、未发真实 QQ 消息、
  未打 tag；用户删除的四个文件保持未暂存，`.codex-next-goal.md` 未提交。

## 已完成目标：虚神界出关统计列的启动迁移归属

重构收敛（非新玩法）。目标文档 `.codex-next-goal.md`（local-only，未提交）。
承接 `docs/features/impart_pk.md` 与仓库级「请求路径不执行 DDL」合同。

**缺口证据**：`features/impart_pk/closing_settlement_repository.py::settle` 在真实写事务内
`PRAGMA player_data.table_info(statistics)` 后，对 `虚神界闭关时长`/`虚神界闭关修为`/
`虚神界闭关祝福时长` 逐列 `ALTER TABLE player_data.statistics ADD COLUMN ... INTEGER DEFAULT 0`；
`impart_pk.002~.004` 均未创建这三列。上一轮 impart_pk 切片只处理回执/投影表，
`statistics` 业务列仍留在请求路径。

交付内容：

- `features/impart_pk/statistics_columns.py` 增加 `CLOSING_SETTLEMENT_FIELDS` 与只读
  `statistics_columns_ready(uow, fields, *, schema)`（`PRAGMA` 取列，空列集视为缺表）。
- `features/impart_pk/migrations.py` 新增 `apply_impart_pk_closing_statistics`（`impart_pk.005`，
  player DB）：经共享 `ensure_statistics_column(schema="")` 建表并补三列，幂等；
  `plugin.py` 按版本序注册，catalog 315→316 单调；列入 player 白名单与 game 排除集，
  trade/impart/message 均不包含。
- `settle` 删除逐列 `ALTER TABLE`，在幂等重放检查之后、任何资产写入之前调用
  `statistics_columns_ready`；缺表或缺任一列返回 `schema_missing`，`user_xiuxian`/`user_cd`/
  `impart_data.xiuxian_impart` 与回执行零变化，不建表、不补列。
- 兼容：迁移 `IF NOT EXISTS`，旧库已有三列与历史行不变；成功结算仍按
  `closing_minutes/exp_gain/blessing_cost` 累加，重放 `duplicate` 不重复累加；
  legacy `xiuxian_impart_pk/transaction_service.py::_increment_stat` 保留为显式回滚入口。
- 文档：`docs/features/impart_pk.md` 的启动迁移清单补 `impart_pk.005`，统计列契约段说明
  三列归迁移所有、请求路径 fail-closed 与回滚口径。
- 测试：新增 `tests/test_impart_closing_statistics_ownership.py` `7/7`（迁移建表/补列幂等、
  player-only 路由、既有行保留、缺表与半成品表 `schema_missing` 且零写入零补列、
  迁移后 applied/duplicate/state_changed、请求路径无 DDL 与共享契约源合同）；
  `features/impart_pk/tests/test_closing_settlement_repository.py` 与
  `tests/test_impart_pk_operations_schema.py` 夹具改为显式应用迁移。
  合并集（新套件 + `test_impart_pk_operations_schema` + `test_impart_statistics_column_contract`
  + `test_impart_closing_enter` + `test_impart_closing_settlement` + `features/impart_pk/tests`
  + `test_source_quality`）`310 passed`；`test_phase2_legacy_path_gate`（临时还原冻结文件后删除）
  `47 passed, 91 subtests passed`。
- 已知既有失败（非本目标回归）：`tests/test_refactor_closeout_recovery.py::
  test_closeout_five_database_migration_backup_restore_and_reconcile` 在
  `tests/test_dependency_lock.py` 之后运行时，临时 data 目录缺 `功法/功法概率设置.json`，
  `state.phase` 为 `not_ready`；在 HEAD 隔离 worktree 同序复现，单独运行通过，属既有测试顺序问题。
- 三门禁真实 exit 0（progress `failures: None`；`exit_ready: False` 仍仅由用户删除的冻结文件
  造成，11 条既有 blocker）；`compileall` 与 `git diff --check` 干净。
- 交付与 CI：功能提交 `ed6c50b1` 的 develop run `41921231649` success；
  `--ff-only` 合入 main 后 main run `41922652223` success（headSha 均 `ed6c50b1`；
  `quality` success、`build-and-release` skipped）。未部署、未读写真实游戏库、未发真实 QQ 消息、
  未打 tag；用户删除的四个文件保持未暂存，`.codex-next-goal.md` 未提交。

## 已完成目标：虚神界剩余统计列（`impart_pk.006`）的启动迁移归属

重构收敛（非新玩法）。承接 `docs/features/impart_pk.md` 与仓库级「请求路径不执行 DDL」合同。

**缺口证据**：`features/impart_pk/closing_enter_repository.py` 成功进入闭关时经
`increment_statistics()` 补 `虚神界闭关次数`；`features/impart_pk/project_join_repository.py`
成功投影加入时经 `ensure_statistics_column()` 补 `虚神界投影次数`。两者都在请求事务内
`CREATE TABLE`/`ALTER TABLE`，而 `impart_pk.002~.005` 未创建这两列。

交付内容：

- `features/impart_pk/statistics_columns.py` 增加 `IMPART_STATISTICS_FIELDS`（两列）与
  无 DDL 的 `increment_statistics_verified()`；`increment_statistics()` 保留给迁移与显式回滚入口。
- `features/impart_pk/migrations.py` 新增 `apply_impart_pk_impart_statistics`（`impart_pk.006`，
  player DB），经 `ensure_statistics_column(schema="")` 建表补列；`plugin.py` 按版本序注册，
  catalog 316→317 单调；列入 player 白名单与 game 排除集，trade/impart/message 均不包含。
- 两个仓储删除请求期建表/补列：`closing_enter` 在重放检查之后、`project_join` 在
  legacy 成员导入之前调用 `statistics_columns_ready`（前者 `player_data`，后者主库无前缀）；
  缺表或缺列返回 `schema_missing`，零资产/零成员/零回执写入。
- 写口径不变：成功进入闭关 `虚神界闭关次数` +1、成功投影加入 `虚神界投影次数` +1；
  无统计行时插入；operation 重放 `duplicate` 不重复累加；`busy`/`ineligible`/`already_joined`/
  `pk_exhausted`/`capacity_full` 分支零累加。
- 兼容：迁移 `IF NOT EXISTS`，旧库已有列与历史行保留；legacy
  `xiuxian_impart_pk/transaction_service.py` 的 `_increment_stat`/`_increment_entry_stat`/
  `_increment_projection_stat` 保留为显式回滚入口。
- 文档：`docs/features/impart_pk.md` 迁移清单补 `impart_pk.006`，统计列契约改为五列全部归迁移、
  请求路径无 DDL 累加。
- 测试：新增 `tests/test_impart_statistics_migration_ownership.py` `7/7`（迁移补列幂等、
  player-only 路由、既有行保留、两个仓储缺迁移 `schema_missing` 且零写入零建表、
  迁移后 enter/join applied+duplicate、请求路径无 DDL 源合同）；
  `tests/test_impart_statistics_column_contract.py` 的 superseded 断言改为迁移后契约，
  夹具显式应用 `impart_pk.006`。定向合并集 `311 passed`；
  `test_phase2_legacy_path_gate`（临时还原冻结文件后删除）`47 passed, 91 subtests passed`；
  `test_refactor_progress`/`test_dependency_lock`/`test_refactor_architecture` `43 passed`。
- 三门禁真实 exit 0（progress `failures: None`；`exit_ready: False` 仍仅由用户删除的冻结文件
  造成，11 条既有 blocker）；`compileall` 与 `git diff --check` 干净。
- 交付与 CI：功能提交 `8b88e76c` 的 develop run `41933024471` success；
  `--ff-only` 合入 main 后 main run `41933829744` success（headSha 均 `8b88e76c`；
  `quality` success、`build-and-release` skipped）。未部署、未读写真实游戏库、未发真实 QQ 消息、
  未打 tag；用户删除的四个文件保持未暂存，`.codex-next-goal.md` 未提交。

## 已完成目标：灵石赠送请求路径的 DDL 收口

重构收敛（非新玩法）。承接 `docs/features/stone_gift.md` 与仓库级「请求路径不执行 DDL」合同。

**缺口证据**：`features/stone_gift/repository.py::limits` 与 `::transfer` 在请求事务内调用
`ensure_limit_schema(uow)`，执行 `CREATE TABLE IF NOT EXISTS stone_gift_limits`；而
`stone_gift.001`/`stone_gift.002` 已经创建 `stone_gift_operations`/`stone_gift_limits`，
`plugin.py` 也已注册，属重复 DDL。

交付内容：

- 新增 `features/stone_gift/schema.py`：两张表的 DDL、列契约、`SCHEMA_MISSING` 与只读
  `table_ready(uow, table, required)`；`migrations.py` 改为从该模块取 DDL。
- `repository.py` 删除 `ensure_schema`/`ensure_limit_schema`：`operation()` 缺表返回 `None`、
  `limits()` 缺表返回全零（读路径降级）；`transfer()` 在 `stone_gift_operations` 或
  `stone_gift_limits` 未就绪时返回 `schema_missing`，在任何资产/额度/回执写入之前短路。
- `application.py` 把 `schema_missing` 映射为 code `schema_missing`、文案
  「赠送数据尚未就绪，请检查启动迁移。」。
- 兼容：迁移 `IF NOT EXISTS`；`compatibility/legacy_stone_gift.py` 的建表保留为显式回滚入口。
- 文档：`docs/features/stone_gift.md` 记录 DDL 归属、读降级与写 fail-closed。
- 测试：新增 `tests/test_stone_gift_schema_ownership.py` `6/6`（两表建表幂等、读降级不建表、
  未迁移写 `schema_missing` 且零写入、应用层映射、迁移后转账/重放、仓储源无 DDL）；
  `features/stone_gift/tests/test_stone_gift_service.py` 与 `test_stone_gift_adapter.py`
  夹具显式应用 `.002`。定向合并集 `259 passed`；
  `test_refactor_progress`/`test_dependency_lock`/`test_refactor_architecture` `43 passed`。
- 三门禁真实 exit 0（progress `failures: None`；`exit_ready: False` 仍仅由用户删除的冻结文件
  造成，11 条既有 blocker）；`compileall` 与 `git diff --check` 干净。
- 交付与 CI：功能提交 `42256588` 的 develop run `41937991440` success；
  `--ff-only` 合入 main 后 main run `41938949357` success（headSha 均 `42256588`；
  `quality` success、`build-and-release` skipped）。未部署、未读写真实游戏库、未发真实 QQ 消息、
  未打 tag；用户删除的四个文件保持未暂存，`.codex-next-goal.md` 未提交。

## 已完成目标：玩家「我的赠送流水」个人赠送收支视图

新功能（非修复）。目标文档 `.codex-next-goal.md`（local-only，未提交）。

**缺口证据**：`stone_gift_operations` 在每次成功赠送时由真实写路径落行
（`features/stone_gift/repository.py::transfer`，含发送方/接收方/总额/净额/手续费/时间），
但玩家侧只有 `送灵石` 与 Web 写入，`grep -rn "赠送流水|赠送记录"` 在玩家模块零命中，
无法查看自己送出/收到过哪些灵石。

交付内容：

- 新增 `features/stone_gift/history.py`：`StoneGiftHistoryApplication(database, clock=...)`
  只读聚合近 7 天记录（窗口只用下界 `created_at >= start`，未来时间行计入）；
  发送侧统计笔数/总额/手续费/净额，接收侧统计笔数/净额；主要对象按净额降序取前 3；
  最近 5 笔按 `created_at DESC` 标注送出/收到；对方显示 `user_xiuxian.user_name`，缺失回退 user_id。
  缺表/缺列/读异常返回空快照，不建表、不补列；`format_stone_gift_history` 为纯函数。
- 新玩家命令 `我的赠送流水`（别名 `赠送流水`、`灵石赠送流水`，priority 7，`block=True`），
  注册在 `adapters/nonebot/commands.py`；`user_id` 只取会话身份，handler 不解析 `args`。
- 帮助：`修仙帮助` 的「流水与总览」分组登记命令与一行说明。
- 清单：`features/stone_gift/manifest.py` 追加 `CommandSpec`（含别名）；
  `docs/refactor_inventory.json` 新增 1 条 command（+9 行）；该命令由迁移后 matcher 注册，
  Phase-2 legacy 派生计数不变。
- 文档：`docs/features/stone_gift.md` 的命令与别名段登记命令、口径与只读边界。
- 测试：新增 `tests/test_stone_gift_history.py` `5/5`（汇总口径与前 3、最近 5 笔排序、
  7 天窗外排除与未来行计入、玩家隔离、缺表降级且零建表、全文/空态渲染、handler/manifest/帮助合同）；
  `tests/test_migrated_matcher_wiring.py` 的 matcher priority 列表同步为 `[1,1,1,7]`。
  定向合并集 `298 passed`；`test_phase2_legacy_path_gate` + `test_dependency_lock` +
  `test_refactor_architecture` `67 passed, 91 subtests passed`（临时还原冻结文件后删除）。
- 三门禁真实 exit 0（progress `failures: None`；`exit_ready: False` 仍仅由用户删除的冻结文件
  造成，11 条既有 blocker）；`compileall` 与 `git diff --check` 干净。
- 交付与 CI：功能提交 `aa174fa2` 的 develop run `42223184181` success；
  `--ff-only` 合入 main 后 main run `42224408701` success（headSha 均 `aa174fa2`；
  `quality` success、`build-and-release` skipped）。未部署、未读写真实游戏库、未发真实 QQ 消息、
  未打 tag；用户删除的四个文件保持未暂存，`.codex-next-goal.md` 未提交。

## 已完成目标：活动中心复核 + 竞技场个人挑战战绩

先审计被人为标记为“半成品”的活动中心，再按证据交付下一项真实缺口。

**活动中心审计（完整，不重复实现）**：`docs/goals/goal-activity-completion.md` 的 G1–G6
均已关闭；`xiuxian_activity` 实际注册 26 条玩家命令，覆盖中心/详情/阶段/排行/首领/商店/
背包/奖励/玩法/签到/任务/战令/一键领取；Web 为 `/api/v1/activity`（审计）、
`/api/v1/activity/state`（九视图）与 `POST /api/v1/activity/sign-in`，admin 面板九视图与路由
一致；`activity_state.001/.002` 启动迁移与 `activity_claim_all_operations/steps` 分步回执
承担持久化与幂等，请求路径无 DDL。本机实跑 `tests/test_activity_*.py`：
`413 passed, 112 subtests passed`。未发现命令、路由、奖励或持久化缺口。

**下一缺口（竞技场个人战绩）**：`arena_challenge_settlement_operations` 每次结算均由真实
写路径落行（`challenger_id`、`payload` 对手/结果/时间、`result_json` 状态/积分变化/结算后
积分段位），但 `我的竞技场` 只有累计场次与胜率、`我的荣誉` 只有荣誉结算，玩家无法查看
逐场战绩。

交付内容：

- 新增 `features/arena/match_history.py`：`ArenaMatchHistoryApplication` 只读聚合近 7 天
  本人回执（窗口只用下界）；只统计 `result_json.status == "applied"`，拒绝回执不计入；
  outcome 限 `win/loss/draw/no_match`；输出总场次、胜/负/平/未匹配、积分净变化与最近 5 场
  （时间、对手、结果、积分变化、结算后积分/段位）。缺库/缺表/缺列/坏 JSON 降级为空态，
  不建表、不补列；对手名通过注入 resolver 解析，失败回退 user_id。
- 新命令 `我的竞技场战绩`（别名 `竞技场战绩`、`竞技场记录`，priority 10）注册在
  `xiuxian_arena`；`竞技场帮助` 增条目与按钮，`修仙帮助`「流水与总览」登记；
  `features/arena/manifest.py` 增 `CommandSpec`，inventory 新增 1 条 command（+9 行）。
- `docs/features/arena.md` 增「个人挑战战绩」章节与命令说明，明确只读边界与拒绝回执过滤。
- 测试：新增 `tests/test_arena_match_history.py` `7 passed`（窗口/排序、拒绝回执过滤、
  玩家隔离、缺表降级不建表、坏 JSON 跳过、resolver 失败回退、渲染与帮助/manifest 合同）；
  定向集 `284 passed`；`test_phase2_legacy_path_gate` `47 passed, 91 subtests`，
  派生计数按新增 legacy 命令更新为 653/339（冻结文件未动）。
- 三门禁真实 exit 0（progress `failures: None`，11 条既有 blocker）；`compileall` 与
  `git diff --check` 干净。既有 4 项 pytest-only arena 失败在 stash 本切片后同样复现
  （CI 的 unittest discover 不收集），未修改。
- 交付与 CI：功能提交 `56b3bd45` 的 develop run `42235432858` success；
  `--ff-only` 合入 main 后 main run `42236407388` success（headSha 均 `56b3bd45`）。
  未部署、未读写真实游戏库、未发真实 QQ 消息、未打 tag；用户删除的四个文件保持未暂存，
  `.codex-next-goal.md` 未提交。

## 已完成目标：通天塔个人爬塔记录

**缺口证据**：`tower_settlement_operations` 每次结算落行（`payload` 为
`[user_id, floor, stamina_cost]`，`result_json` 含积分/灵石/修为/胜负/体力/掉落，
`created_at` 可排序），但玩家只有 `通天塔信息` 的当前层数/历史最高/累计积分，
看不到最近爬塔结果。

交付内容：

- 新增 `features/tower/history.py`：`TowerHistoryApplication` 只读近 7 天本人回执；
  用 `json_extract(payload,'$[0]')` 定位玩家并保留 `floor > 0` 的真实结算行（拒绝回执
  写 floor 0），聚合次数/成功/失败/最高层/累计积分/灵石/修为，最近 10 次含掉落摘要；
  缺库/缺表/缺列/坏 JSON 降级空态，不建表不补列；旧 SQLite 无 JSON1 时有界扫描回退。
- 新命令 `我的通天塔记录`（别名 `通天塔记录`、`爬塔记录`，priority 5）注册在
  `xiuxian_tower`；`通天塔帮助` 与 `修仙帮助`「流水与总览」登记；manifest 增
  `CommandSpec`，inventory +9 行；`docs/features/tower.md` 增「个人爬塔记录」章节。
- 测试：新增 `tests/test_tower_history.py` `6 passed`（窗口/排序、拒绝行过滤、玩家隔离、
  缺表降级不建表、坏 JSON、渲染与帮助/manifest 合同）；定向集 `309 passed, 91 subtests`；
  Phase-2 派生计数 653→654、339→340。
- 三门禁 exit 0；compileall/diff-check 干净。
- 交付与 CI：功能提交 `4eb421c2` 的 develop run `42244338818` success；
  `--ff-only` 合入 main 后 main run `42245637720` success。

## 已完成目标：宗门任务结算记录

**缺口证据**：`sect_task_settlement_operations`（game DB）在任务成功提交时写入
`user_id/period/cost_type/cost/exp_reward/sect_reward/materials_reward/created_at`，
但玩家只能看到当前接取任务与一次完成提示，无法回看历史消耗与奖励。

交付内容：

- 新增 `features/sect/task_history.py`：`SectTaskHistoryApplication` 只读近 7 天本人
  结算行（窗口只用下界，按 `created_at DESC, rowid DESC`），聚合完成次数与累计
  修为/宗门贡献/宗门资材，最近 10 次显示时间、气血/灵石消耗与三项奖励；缺表/缺列/
  读失败返回空态，不建表不补列。
- 新命令 `我的宗门任务记录`（别名 `宗门任务记录`，priority 7）注册在 `xiuxian_sect`；
  `宗门帮助`「任务与商店」与 `修仙帮助`「流水与总览」登记；manifest 增 `CommandSpec`，
  inventory +8 行；`docs/features/sect.md` 增「个人宗门任务记录」章节。
- 测试：新增 `tests/test_sect_task_history.py` `5 passed`（窗口/聚合/排序、玩家隔离、
  缺表降级不建表、渲染、命令/manifest/帮助合同）；定向集 `308 passed, 91 subtests`；
  Phase-2 派生计数 654→655、340→341。
- 三门禁 exit 0；compileall/diff-check 干净。
- 交付与 CI：功能提交 `89288d9d` 的 develop run `42247682771` success；
  `--ff-only` 合入 main 后 main run `42249077539` success。
  未部署、未读写真实游戏库、未发真实 QQ 消息、未打 tag；用户删除的四个文件保持未暂存，
  `.codex-next-goal.md` 未提交。

## 已完成目标：副本个人探索记录

**缺口证据**：`dungeon_explore_operations` 在探索成功结算时写入规范
`request_identity`（`{"action":"explore","user_id":...}`）、`phase='completed'`、
`result_status='applied'`、`result_json`（message/battle_messages）、`current_layer`、
`dungeon_status` 与 `updated_at`；玩家只有 `我的副本状态` 的当前层数/进度，无法回看
近几天探索过哪些层、结果与奖励摘要。

交付内容：

- 新增 `features/dungeon/explore_history.py`：`DungeonExploreHistoryApplication` 在
  `read_only=True` UoW 中按精确 identity 查询近 7 天（只用下界）成功结算行，按
  `updated_at DESC, rowid DESC` 排序；被拒绝/未完成行不计入。聚合探索次数、通关次数、
  最高层、战斗场数，最近 10 次显示时间、层数、通关/探索中与折叠后的结果摘要（120 字）；
  缺库/缺表/缺列/坏 JSON/读失败降级空态，不建表不补列。
- 新命令 `我的副本记录`（别名 `副本记录`、`副本探索记录`，priority 5）注册在
  `xiuxian_dungeon`；`副本帮助` 与 `修仙帮助`「流水与总览」登记；manifest 增
  `CommandSpec`，inventory +9 行；`docs/features/dungeon.md` 增「个人副本探索记录」章节。
- 测试：新增 `tests/test_dungeon_explore_history.py` `6 passed`（窗口/聚合/排序、玩家隔离、
  缺表降级不建表、坏 JSON/错误 identity 跳过、渲染、命令/manifest/帮助合同）；
  定向集 `309 passed, 91 subtests`；Phase-2 派生计数 655→656、341→342。
- 三门禁 exit 0；compileall/diff-check 干净。
- 交付与 CI：功能提交 `3fa0327f` 的 develop run `42253408907` success；
  `--ff-only` 合入 main 后 main run `42254737327` success。
  未部署、未读写真实游戏库、未发真实 QQ 消息、未打 tag；用户删除的四个文件保持未暂存，
  `.codex-next-goal.md` 未提交。

## 已完成目标：洞府个人收获记录

**缺口证据**：`dongfu_harvest_operations`（game DB）在成功收获时写入
`payload=[user_id, expected_slots, slot_numbers, reward_rows, failed_slots]`，
`reward_rows` 含物品 ID/名称/类型/数量，`created_at` 可排序；玩家只有 `我的洞府`
当前灵田与作物，无法回看最近收获。

交付内容：

- 新增 `features/dongfu/harvest_history.py`：`DongfuHarvestHistoryApplication` 在
  `read_only=True` UoW 中按 `json_extract(payload,'$[0]')` 查询近 7 天（只用下界）成功
  收获行，`json_valid` 过滤坏 payload，按 `created_at DESC, rowid DESC` 排序；聚合收获
  次数、总件数、物品种类数，最近 10 次显示时间、最多 4 种物品与未成熟灵田数；缺库/缺表/
  缺列/坏 JSON/读失败降级空态，不建表不补列。
- 新命令 `我的洞府收获记录`（别名 `洞府收获记录`、`灵田收获记录`，priority 8）注册在
  `xiuxian_dongfu`；`洞府帮助` 与 `修仙帮助`「流水与总览」登记；manifest 由
  `commands_for("dongfu")` 自动纳入，inventory +9 行；`docs/features/dongfu.md` 增
  「个人洞府收获记录」章节。
- 测试：新增 `tests/test_dongfu_harvest_history.py` `6 passed`（窗口/聚合/排序、玩家隔离、
  缺表降级不建表、坏/旧 payload 跳过、渲染、命令/manifest/帮助合同）；
  定向集 `309 passed, 91 subtests`；Phase-2 派生计数 656→657、342→343。
- 三门禁 exit 0；compileall/diff-check 干净。
- 交付与 CI：功能提交 `5c1adc88` 的 develop run `42259621403` success；
  main run `42260631315` success（两分支同 SHA）。提交最初落在 main，未重写历史，
  develop 以 `--ff-only` 快进到同一提交后推送。
  未部署、未读写真实游戏库、未发真实 QQ 消息、未打 tag；用户删除的四个文件保持未暂存，
  `.codex-next-goal.md` 未提交。

## 已完成目标：历练个人事件记录

**缺口证据**：`training_event_operations` 在每次历练事件结算时写入规范 payload
（user_id、前后状态、灵石/修为/气血变化、奖励列表）与 `result_json.message`，
但玩家只有 `历练状态` 的当前进度与上一次事件，看不到近几天的事件、资源变化与掉落。

交付内容：

- 新增 `features/training/event_history.py`：`TrainingEventHistoryApplication` 在
  `read_only=True` UoW 中按 `json_valid(payload)` + `json_extract(payload,'$[0]')`
  查询近 7 天（只用下界）`status='applied'` 的事件，按 `created_at DESC, rowid DESC`
  排序；聚合事件次数、累计灵石/修为/气血变化与奖励件数，最近 10 次展示时间、折叠后的
  事件文本、三项变化与奖励摘要；缺库/缺表/缺列/坏 JSON/读失败降级空态，不建表不补列。
- 新命令 `我的历练记录`（别名 `历练记录`，priority 5）注册在 `xiuxian_training`；
  `历练帮助` 与 `修仙帮助`「流水与总览」登记；manifest 由
  `commands_for("training")` 自动纳入，inventory +8 行；`docs/features/training.md`
  增「个人历练记录」章节。
- 测试：新增 `tests/test_training_event_history.py` `6 passed`（窗口/聚合/排序、玩家隔离、
  缺表降级不建表、坏/非 applied 行跳过、渲染、命令/manifest/帮助合同）；
  定向集 `309 passed, 91 subtests`；Phase-2 派生计数 657→658、343→344。
- 三门禁 exit 0；compileall/diff-check 干净。
- 交付与 CI：功能提交 `c895f764` 的 develop run `42265602854` success；
  `--ff-only` 合入 main 后 main run `42266650937` success。
  未部署、未读写真实游戏库、未发真实 QQ 消息、未打 tag；用户删除的四个文件保持未暂存，
  `.codex-next-goal.md` 未提交。

## 已完成目标：修为奖励合同与三玩法反馈收口（2026-11-07）

本轮收口「修为奖励合同 + 悬赏令 / 世界BOSS / 虚神界三个玩法的回执与状态可见性
+ CI 重复执行」五个包；不新增表、列、迁移、配置键或命令，不改奖励数值。

### P0 CI 重复执行收口（用户插入的最高优先级）

- 用真实 `unittest.TestLoader.discover`（不执行用例）测量：`discover -s tests`
  4079 个叶子 ID；`discover -s nonebot_plugin_xiuxian_2` 1311；`discover -s . -p
  'test_*.py'` 5383。三者满足 A∪C==B、0 缺失 / 0 多余、三个全量模式发现错误均为 0，
  因此旧 CI 第一步是第二步的严格子集，整套 `tests/` 每次推送被执行两遍。
- 另有 49 个 ID 被重复收集：六个测试模块 `from tests.test_x import BaseClass`，
  `loadTestsFromModule` 会把导入的 TestCase 再收一遍；改为
  `from tests import test_x as _base` 并以 `_base.BaseClass` 继承/调用。
- 结果：根发现 5383 unique == 5383 leaves、0 duplicate、0 discovery error；CI 合并为
  单步 `python -m unittest discover -s . -p 'test_*.py' -q`，每次推送少执行 4079 个
  用例（约 43%）。旧「窄 pattern 从根发现会因缺少 NoneBot 初始化报 40 个
  `_FailedTest`」的既有脆弱性未改变，CI 一直使用全量 pattern。
- 诚实记录：`efecc122` 的 develop run `42277923101` **failure** —— 一个夹具用合成境界名
  依赖被 P1 移除的 `current` 回退；`b6963441` 显式注入奖励函数修复，未放宽断言。

### P1 gap 锚定未知境界 fail-closed（根因）

`numeric_bind.percent_exp_reward` 在 `anchor='gap'/'next_power'` 且
`stage_power_gap(level)<=0` 时曾回退 `current` 锚定；`docs/features/xiuxian_progression.md`
明确禁止该回退，但当时只有世界BOSS 自行守卫。现在字符串境界名取不到 gap 时直接返回 0，
int/float 境界序号与 `None` 保留旧语义；新增 `tests/test_percent_exp_fail_closed.py` 6 项
（未知名 exp 1e5/1e20 均为 0、已知境界 gap 不变、int/None 仍 current、末境不变、源合同）。
提交 `b2b9b2c1`。

### P2 魔修入侵领奖：修为不越过本境上限

`WorldEventClaimSqlRepository.claim` 原先 `exp = exp + 奖励`，与悬赏令/宗门任务/历练/
闭关的 `set_closing_type(level) × closing_exp_upper_limit` 截断口径不一致，回执照报全额。
现在命令层计算 `max_exp` 传入（协议新增 keyword-only 参数，ledger payload 不变），仓储在
写事务内按余量截断，`demon_claim_operations.exp` 与回执存实际值，回放返回实际值；被截断时
回复追加「（已达本境修为上限，超出部分未发放）」。旧调用不传 `max_exp` 时保持历史行为。
新增 `tests/test_world_event_claim_exp_cap.py` 6 项（真实迁移建表、低于/接近/已满、
回放、旧行为、超大整数）。提交 `a2e53d49`。

### P3 虚神界信息：闭关状态可见

`虚神界信息` 过去在 `user_cd.type=4` 时仍不显示闭关状态。新增纯函数
`features/impart_pk/closing_status.py::closing_status_lines`（闭关中/已入定 N 分钟/
今日祝福余额/本境修为上限余量/出关提示），坏时间记 0、负值夹 0、上限缺失省略该行；
非闭关返回空元组，未闭关玩家面板逐字不变。命令层只提供 `user_cd`、`exp_day`、当前修为
与上限。新增 `tests/test_impart_closing_status_view.py` 6 项。提交 `a4c815c1`。

### P4 世界BOSS：每日额度单一来源 + 回执剩余次数

`12000` / `300000000` / `30` 原在结算与 `世界BOSS信息` 各写一遍。现集中在
`xiuxian_boss/boss_limit.py`（`DAILY_INTEGRAL_LIMIT` / `DAILY_STONE_LIMIT` /
`DAILY_BATTLE_COUNT` / `daily_quota_line`），两处引用同一常量；战斗成功回执追加
`今日讨伐：X/30（剩余 N 次）`，最后一次显示「已用尽」。数值未调整。
新增 `tests/test_boss_daily_quota_view.py` 6 项。提交 `2127f110`。

### P5 悬赏令：剩余刷新次数 + 截断说明

`悬赏令查看`（状态 3）新增 `今日剩余刷新次数：N次`（取 `user_xiuxian.work_num`，
缺失/坏值回退总数并夹在 `[0,总数]`）；结算回执 `result_json` 新增 `exp_requested`，
实际值低于请求值时追加「（已达本境修为上限，超出部分未发放）」，旧回执无该键则不追加。
文案集中在 `features/work/presentation.py`。新增 `tests/test_work_reward_feedback.py` 9 项
（含真实仓储截断/回放）。提交 `f4789356`。

### 验证与 CI

- 定向：P0 八个受影响/新增模块 70 项 OK（4.9 s）；P1/P2 15 + 49 项；P3 6 项；
  P4/P5 与受影响悬赏/世界BOSS 套件 41 项。均单次执行，未重复跑已绿套件。
- 三门禁真实 exit 0（progress `failures: None`；`exit_ready: False` 仍由用户删除的冻结
  文件造成，11 条既有 blocker）；`compileall` 与 `git diff --check` 干净。
- develop：`a4c815c1` run `42279097569` success；`f4789356` run `42281656573` success
  （`Ran 5404 tests in 207.508s`）。`--ff-only` 合入 main 后 main run `42282783410`
  success（headSha 均 `f4789356`）。
- 未部署、未读写真实游戏库、未发真实 QQ 消息、未打 tag；用户删除的四个文件保持未暂存，
  `.codex-next-goal.md` 未提交。
