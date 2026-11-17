# 旧调度任务兼容层

## 用户流程

旧版本 APScheduler 任务继续运行；旧模块导入时只收集声明，不修改 live scheduler。组合根在 `jobs` 生命周期阶段激活调度桥并登记稳定任务 ID，管理端只读展示登记结果。

## 命令与别名

无新增命令。旧命令仍由旧包兼容入口处理。

## Web API（方法、路径、请求/响应、权限、幂等键）

由 `/api/v1/scheduler` 提供只读任务清单，权限为 `admin`；旧管理页面保留兼容 URL。

## 数据模型与迁移

无新增表。旧调度覆盖配置仍由旧仓储读取，迁移期间不改变其字段。

## 事务与失败回滚

任务执行由旧服务负责；新层只提供稳定 ID、超时、重试和幂等策略声明。失败必须进入旧任务日志并可人工重试。
定时入口不得在捕获异常或明确失败结果后正常返回；否则 APScheduler 会把“只记日志”的伪成功
当作完成。当前已对傀儡收取、副本邀请扫描、NewAPI 自动签到、体力恢复和宗门维护保留异常
透传回归。可恢复的单条业务跳过仍记录具体状态，但批次存在失败时任务整体失败。

宗门解散/继承/封闭的 `schema_missing` 等显式失败、日重置 `operation_conflict` 也必须
产生调度错误事件；正常竞争按原具体状态跳过。傀儡定时路径将缺迁移、坏时间等结果
抛回调度器，未成熟/库存上限/灵石不足等原业务结果保持。NewAPI 完成分页后汇总逐条
异常、请求失败、远端拒绝和历史未落盘；错误含本地定位及具体原因码，保留原异常链与
诊断详情，日志不拼入远端原文或凭据。历史补写仅重试本地写入，不在同次执行中重发
远端POST；任务部分失败不表示其他账号的远端签到或宗门处理已回滚。

调度桥只在全部声明注册成功后标记 active。中途注册失败原样抛出异常，已成功注册的
任务保留，失败声明及尚未处理的声明继续 pending；同进程启动重试只处理剩余任务，
不能以一次失败后的空列表伪报就绪。既有同 ID 外部任务冲突仍拒绝，不覆盖原任务。
故障持续时 lifecycle 的 jobs 阶段保持 not_ready；恢复并完整注册后才允许 ready。

## 定时任务

任务 ID 清单见 `compatibility/legacy_manifest.py`，禁止通过模块导入新增未声明任务。

## 配置项

无新增配置。

## 适配器差异

无消息适配器差异；任务仍通过原有 NoneBot/APScheduler 运行时执行，但注册由 `compatibility/scheduler.py` 延迟到组合根的 `jobs` 阶段完成。

## 测试与手工验收

启动两次任务清单不重复，`/api/v1/scheduler` 返回稳定 ID，旧调度测试保持通过。
`tests/architecture/test_scheduler_bridge.py` 覆盖部分注册故障、持续故障、恢复期间新增声明
与既有任务冲突；`tests/test_demon_scheduler.py` 用独立 APScheduler 和隔离 runtime
验证 jobs 阶段连续失败保留原 traceback，恢复后魔修/灵脉完整注册、已成功任务不重复，
再实际触发恢复的魔修任务并核对 SQLite 活动状态、生命周期回执和 integrity_check。
该链使用无 NoneBot driver 的维护 runtime，不发送消息；驱动异常透传另有回归。
`tests/test_scheduled_job_failure_visibility.py` 用真实 APScheduler 的执行/错误事件、
隔离 SQLite 与 mock 远端验证宗门五条维护分支、日重置、傀儡资产/回执失败回滚、
NewAPI 部分成功的实际历史、修复后恢复与正常幂等，不以普通返回或无 traceback 代替结算证据。

## 定时优先批次交付证据（2026-11-17）

以下均为develop上的隔离验证，精确提交与回归入口一一对应。已核通过的CI复用，
不重复长时间watch或全量测试；没有用日志无traceback或回调正常返回证明业务正确。

| 完整路径 | 交付提交 | CI证据 | 可重放入口 |
| --- | --- | --- | --- |
| 魔修启动有效窗/跨期状态/19:00讨伐至22:00关闭 | 968a0034 | 43951435416 success | test_demon_scheduler.py、test_demon_event_lifecycle_application.py |
| 调度桥部分失败保留pending、连续故障与注册恢复 | 9d8d816e、5c5ca20c | 43947994349 success；后者另固定既有随机夹具 | architecture/test_scheduler_bridge.py、test_demon_scheduler.py |
| 宗门/傀儡/NewAPI内层显式失败与部分恢复 | 2f17b57f | 43954272571 success | test_scheduled_job_failure_visibility.py |
| 鬼市时区/截止、pending恢复、存取及到期返还/批量清空 | d8ecefe1至3befe21b的连续切片 | 最近3befe21b/43988136572 success | test_guishi_schedule.py、test_guishi_parameter_dispatch.py各明确group及trade feature文档 |
| 拍卖注册/触发/边界/启动对账、开场交接、结算与领取 | ee80d0ac、eac10e62、774b67ec、520a79df | 43995218089/43996331825/43999705305/44001821163均success | test_auction_schedule.py、test_auction_parameter_dispatch.py、start/settlement/pending_write_rejection.py |
| 拍卖等待区退物/上架及竞拍资金交接拒写恢复 | 498a9f32、44d001a2 | 44005611091/44007297793 success | test_auction_queue_write_rejection.py、test_auction_bid_write_rejection.py、对应真实参数group |
| 世界BOSS定时全量刷新、玩家查询/讨伐与同槽回放 | b9a354d1 | 44011718379 success | test_world_boss_refresh_recovery.py、world-boss-refresh参数group |
| 世界BOSS每日重置、分块拒写保留pending及业务日 | e5c7e881、96d6d935 | 44016104819失败于旧日志静态契约；96d6d935修正该契约，44018801363 success | test_world_boss_daily_reset_recovery.py、world-boss-daily-reset参数group |
| 世界BOSS人工生成与定时场次交接、参数冲突与拒写恢复 | 96d6d935 | 44018801363 success；文档69073cee/44019699806 success | test_world_boss_manual_recovery.py、world-boss-manual-spawn参数group |
| 宗门定时资材、逐宗门失败恢复及玩家读取 | 1ab871e9 | 44022748659 success | test_sect_material_recovery.py、sect-material-schedule参数group |

宗门定时资材另补完整闭环（详见sect文档）：首轮7红实证状态/回执拒写伪成功、
缺迁移实际任务伪EXECUTED及UTC取错业务日。现写后验证/整宗门回滚、具体原因至
实际任务ERROR，先前宗门提交保留，同日恢复只补剩余；同刻跨时区和次日行为稳定。
十一受影响模块88pass（含参数包装中的两条真实玩家场景），不作为全量通过计数。
任务注册/pending恢复、真实触发、两步IGNORE/改写/ABORT、旧大数及玩家读取/三库
完整性均有独立断言；OneBot/QQ仍仅mock。新回归接现有CI，不重跑已绿全量。

宗门每日次数与丹房维护另有完整闭环：四步IGNORE原本真实EXECUTED且提交副作用，
UTC日期也会错分业务日。现玩家/资材/等级/永久回执行数和最终回读、整日回滚，
四个具体write_rejected原因携主体到任务ERROR；原异常透传、失败不记完成。
实际注册/pending持续失败恢复/08:00:15触发边界、历史payload兼容及业务时区均验证；
同日重放保留新领取和任务，次日正常维护。新回归与两条真实玩家参数包装31pass，
受影响旧组62pass（重叠不累计）；详见sect文档。新回归接CI，OneBot/QQ仅mock，
不能提前将新SHA CI写成通过或将此checkpoint当作完整批次发布验收。

参数探针在96d6d935的清单有280个有参入口：126个有直接场景分配、154个
pending_semantic_cases。清单仅匹配每个case声明的主命令，不能统计场景内的其他
真实调用；例如世界BOSS列表在刷新/人工生成场景执行，清单仍显示pending。
这些数字不表示已通过/失败的命令数，也不证明任一完整功能已覆盖。实际证据须
核对case内调用、断言与对应运行结果；inventory-only没有执行任何参数场景。

部署只读证据显示进程和系统业务时区为Asia/Shanghai，而旧魔修源码按UTC小时
判断；11月12至16日有刷新日志，缺少自动开启/结束成功记录。这只支持排查方向，
不证明线上活动状态或修复已部署。未改线上数据库、更新/重启部署或修改部署环境。
OneBot/QQ文本、Markdown和降级仅mock，真实客户端/连接与线上调度观察仍未验。
Phase2冻结JSON的用户既有删除、剩余玩家参数矩阵及G3--G5门禁仍未解决，不能以
本批次代替完整goal、合main或发布。后续相关玩家路径范围见唯一status入口。

## 灰度开关、回滚和已知限制

移除该 manifest 即可关闭新登记和 Web 展示；旧任务会在兼容周期内继续运行。当前任务 handler 仍由旧包提供，调度声明已由桥接层统一延迟注册；下一发布周期再迁移至 feature jobs。

## Manifest 清单
- `job: auto_guishi_transactions`
- `job: auto_harvest`
- `job: auto_start_auction`
- `job: auto_handle_inactive_sect_owners_job`
- `job: backup_database_files`
- `job: cleanup_media_parser_cache_job`
- `job: check_auction_end`
- `job: clear_expired_baitan_orders`
- `job: daily_dungeon_reset`
- `job: daily_add_impart_lv`
- `job: daily_clean_expired_items`
- `job: daily_reset_arena`
- `job: daily_reset_beg`
- `job: daily_reset_boss_limits`
- `job: daily_reset_day_num`
- `job: daily_reset_illusion`
- `job: daily_reset_impart_num`
- `job: daily_reset_impart_pk`
- `job: daily_reset_lottery`
- `job: daily_reset_mixelixir_num`
- `job: daily_reset_sect_task`
- `job: daily_reset_sign`
- `job: daily_reset_stone_limits`
- `job: daily_reset_two_exp`
- `job: daily_reset_work_refresh_num`
- `job: daily_reset_xiangyuan`
- `job: demon_invasion_refresh_schedule`
- `job: demon_invasion_schedule`
- `job: generate_all_bosses`
- `job: newapi_auto_checkin_daily`
- `job: recover_user_stamina`
- `job: reset_message_rate_limits`
- `job: reset_data_by_time_job`
- `job: scheduled_rift_generation_job`
- `job: sect_materials_grant`
- `job: spirit_vein_schedule`
- `job: weekly_reduce_arena_rank`
- `job: weekly_reduce_impart_lv`
- `job: weekly_reset_tower_floors`
