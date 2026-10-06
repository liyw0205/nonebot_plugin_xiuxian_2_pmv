# Administration

## 固定队列与已审边界（2026-10-07）

当前按子插件顺序完成 admin 最后一组验收。50 个冻结入口中，开始盘点前已关闭 6 个；剩余 44 个的共同 owner 已一次盘清，现全部完成归类，admin blocker 为 0。配置 6 条、已有资产/重置 13 条、兼容输出 9 条、黑屋 3 条、命令管控 3 条和广播 6 条均已验收；黑屋 `dc46515f`、命令管控 `0cbfcabe` 已推送并核对远端，生成秘境、重载items、用户伪装三项也已在 `80a4e59c` 提交推送并核对远端。本组转换QQID已通过聚焦、扩大回归与冻结门禁，全局冻结计数为 `220/107/19/150`。本组提交推送后按 `allowed_features` 固定顺序进入 arena，再到 back，不跳其它插件、不重审已验收项。

`转换QQID` 由 `AdminQqidApplication` 编排：`AdminQqidCandidateRepository` 只读四库候选，`AdminQqidBatchRepository` 持久保存单活跃批次、冻结解析映射、请求 alias、固定 child operation ID 和逐项进度；恢复不重扫候选或重解析已冻结项，完成批次的旧请求不会误执行另一个活跃批次。实际四库和玩家目录变更仅复用既有 `AdminApplication.update_user_id`，不复制底层写者；重复目标及候选间链环保守拒绝，child 已完成而批次进度失败时用同一 child 回执补记。独立 QQID 批次锁协调线程和进程，不重入单 ID writer 的同一个 flock；`legacy.admin.008` 仅在 game DB 启动建表，请求不建库或补 schema。其它 ID operation 尚未恢复时仍阻止本批；仅明确无写入的 `reconcile_pending` 拒绝可保存新 attempt 后重试，不宣称自动修复所有单条操作。

QQID 本阶段由三名子代理并行实现仓储、应用/兼容层、真实贯通测试，主线程集成并串行验收。首轮聚焦 `91 passed, 55 subtests passed`（pytest 21.92 秒，含隔离初始化进程 28.13 秒）；最终扩大首轮即通过 `694 passed, 55 subtests passed`（pytest 60.58 秒，含隔离初始化进程 66.26 秒，1 个 anyio 预加载 warning），隔离路径 `/tmp/xiuxian-unittest-1jfgl8_n/xiuxian`。四项 QQID 门禁全 true，`admin_blocked=[]`，冻结 membership 有效、`integrity_errors=[]`，静态检查 1.75 秒；Phase2 `--check` 仅因其它 150 项返回 1。分派至首轮检测约 24 分钟，是并行实现与集成的粗粒度墙钟记录，不能据此准确拆出排查/修改占比；聚焦及扩大检测进程分别 28.13 秒和 66.26 秒，可确认主要耗时不在 pytest 执行。应用记录的 scan/resolve/update 只是本次对应阶段耗时，不包含批次锁等待及部分编排开销，不是端到端总耗时，也不混同开发耗时。保留限制：不保证全批或四库整体原子，候选和条目仍全量读取，外部解析仍串行；未做线上提速测量，不能将可恢复性改进等同于已证明提速。

本组由三名子代理分别独占 items 目录 owner、伪装状态 owner 和秘境贯通回归，主线程负责共享入口、门禁、冻结证据及串行验收。首轮聚焦 pytest 21.00 秒，含隔离初始化的进程总计 28.67 秒；8 个失败均来自旧 avatar 静态门禁字符串与新调用路径不符，不是业务行为失败。首轮扩大集 `655 passed, 1 failed, 49 subtests passed`（pytest 51.42 秒，进程 56.81 秒），唯一失败是 `game_events` 预加载后，旧 `test_partial_game_event_projection_replay_reuses_all_effect_ids` 仅 patch `sys.modules`，未 patch 持有函数的实际查询位置；仅修测试 mock lookup，没有重构业务。最终扩大集 `656 passed, 49 subtests passed`（pytest 52.87 秒，含隔离初始化进程 58.29 秒，1 个 anyio 预加载 warning），隔离路径 `/tmp/xiuxian-unittest-hc2axc3q/xiuxian`。四项 admin runtime gate 与 avatar gate 全 true，496 项冻结 membership 有效、`integrity_errors=[]`，静态检查合计 1.21 秒；全局其余 151 项仍受阻。没有完整排查、实现和收尾分段计时，也未进行线上性能实测。

`重载items` 经 `Items.refresh -> AdminItemCatalogApplication -> AdminItemCatalogRepository`，按既有分类顺序在临时状态中构建 17 类目录，一次发布物品缓存与礼包来源。初次载入保持宽容，缺失或损坏分类不阻断其余可用数据；显式 reload 全批严格，任何读取、JSON 或结构失败均保留整代旧状态，不写用户 JSON。稳定 Mapping 让既有引用看到新一代目录，加载期间读者仍能读取旧快照；单项与类型查询只复制返回条目，避免调用方原地修改污染共享缓存。保留技能字段转换、拆分礼包优先及重复 ID 顺序。`mixelixirutil.mix_config` 等导入时派生缓存不会自动重建，既有显式写回接口仍保留，不扩展为物品 CRUD 迁移。

`用户伪装` 经 `AdminImpersonationApplication -> AdminImpersonationRepository`，兼容 facade 与身份解析共享同一个进程内原子映射；真实消费者先解析 avatar，再用真实管理员 ID 查询伪装覆盖，最终伪装优先于 avatar，失败不会发布半份映射。状态仍不持久化、不跨进程共享，本组不改 avatar 自身状态 owner，也不把调整旧 avatar 静态调用断言记作再次迁移 avatar 功能。

`生成秘境` 保留既有 `RiftApplication -> RiftGenerationSqlRepository` SQL owner，不重迁生成仓储。真实 admin 入口贯通当前 SQL 状态核对后才更新内存与 JSON 投影；历史回执对应的秘境已被替代或结束时不覆盖新投影，冲突、schema 缺失、状态读取和投影失败分别报告，不误报成功。SQL 已提交但 JSON 失败时允许重试同步；SQL/JSON 不是整体原子操作，也不保证 SQL 状态核对至 projection 写入之间的跨线程一致性。

广播六条和普通事件补发共用 `AdminBroadcastApplication -> AdminBroadcastRepository`，兼容 facade 和既有 Web 消费者没有私有任务副本。任务保持进程内、重启丢失的原语义；短线程锁只保护状态，发送前原子 claim，网络 await 不持锁。取消、清空、过期阻止后续目标，已发出的请求不能撤回；generation/token 避免旧回执污染同 ID 新任务，失败释放 claim 允许事件补发。QQ `pending_audit` 不再算成功，也不重复补发；没有审核回调，待审核状态保留到取消/过期。错误只记类型，明细最多 50 条且保留累计数；每次 claim 仅复制固定发送字段，不深拷贝持续增长的目标集合。

`AdminBroadcastHistoryRepository` 经 `asyncio.to_thread` 只读查询消息库，严格匹配 adapter 与 bot_id，QQ 取最近一分钟接收记录并在 SQL 中按 scene/target 选最新回复 ID；OB11 保留所有历史 direction、群优先于私聊。查询不建库/表、不读正文，缺库/表返回空目标，缺必要列显式失败；发送前连接已关闭。QQ 和普通 OB11 复用 delivery，保留 QQ Markdown 和 OB11 单节点合并转发；不自行分配 QQ msg_seq。创建失败向 bot/Web 显式报告，不再误包成 Web success；bot 时间参数保护超长数字及日期溢出。Web 自身既有无效数字退回 1440 分钟的解析仍保留，不算已迁移 Web 路由。

三名子代理分别实现状态核心、只读历史仓储、适配层，再接续门禁、冻结证据与普通入口测试，主线程负责真实 handler/facade/端口贯通和串行验收。首轮聚焦 `84 passed`（pytest 4.74 秒，含隔离初始化进程共 11.13 秒，另有预加载 anyio warning）；启动前先 `import tests`，隔离路径 `/tmp/xiuxian-unittest-txytjgvh/xiuxian`。目标及任务列表仍全量读取，广播首轮仍串行发送，任务不持久化、没有跨进程协调或远端 exactly-once 保证；没有线上性能实测，不把结构改进当作已证明的运行提速。

最终扩大集覆盖 admin、feature/admin、command store、Phase2/progress、messaging 和定向 source quality：`538 passed, 32 subtests passed`（pytest 43.59 秒，含隔离初始化 49.36 秒，预加载 anyio warning）；本轮最终临时目录 `/tmp/xiuxian-unittest-utp1dkti/xiuxian`。普通入口测试证明群/私聊屏蔽在补发前生效，Web 创建失败不会被报为成功。六项 broadcast gate 全 true，496 项 membership 有效、`integrity_errors=[]`；静态进度及冻结检查合计 1.71 秒，Phase2 `--check` 仅因其它 154 项返回 1。首轮扩大集唯一失败是证据文字 `in flight` 与 `in-flight` 的断言拼写差异，修正后复验通过，没有重做生产实现。测试执行并非本轮主要耗时，主要工作仍是实现、贯通用例和证据收尾；没有完整分段计时，不能量化三者占比。用户 `boss_info.json` 不纳入提交。

本组指令禁用、指令解禁、指令列表统一经 `AdminCommandControlApplication -> AdminCommandControlRepository`，兼容函数与既有 Web 消费者共享同一个 owner。保留原 `command_disable.json` 路径，兼容 flat 与 `commands` wrapper 两种结构，未知元数据和退役命令保留在文件中，运行期 active 注册表只暴露当前登记命令。缓存、别名和注册表按规范化路径在单进程内共享，写操作使用同一线程锁和原子替换，失败不发布新缓存，相同值不重复落盘。命令写后不再重建路由索引，重建时先成功同步注册表再发布别名；坏 JSON 报错且不覆盖，路由 fail closed 仅拦截 selected 中已路由的非管理员候选，无候选不读文件。保留分页/分组输出并保护超长纯数字页码；旧 `save_command_disable_memory` 只校验当前状态可读，写接口即时持久化，不再有待存的私有内存。热路径仍做文件 stat 检查，但复用 active view，不为每个候选深拷贝整份状态，模块批量写复用 locations。列表仍全量读取，无跨进程锁或 operation-ID 回执；未做线上提速实测，不把上述结构优化表述为已证明运行提速。聚焦集 `73 passed, 17 subtests passed`（4.13 秒）；主线程补真实 rebuild 顺序测试后，扩大集 `411 passed, 17 subtests passed`（34.62 秒，另有预加载 anyio warning）。启动导入先 `import tests`，确认临时目录 `/tmp/xiuxian-unittest-vuh44p85/xiuxian`。四项 command control gate 全 true，496 项冻结 membership 有效、`integrity_errors=[]`；计数为 210 已迁移、107 兼容、19 不可达、160 受阻，`--check` 因其余 160 项返回 1，静态检查合计 0.79 秒。首轮聚焦的三个子断言失败源于绑定刷新合法地为回复说明边绑定 handler，而测试误要求所有 handler 边仅一条；已改为只要求 owner 边恰一条，没有重做黑屋实现。

本组黑屋统一经 `AdminApplication -> AdminBlackhouseSqlRepository`：`admin_blackhouse_users` 是唯一名单，注册玩家 `user_xiuxian.is_ban` 是兼容投影，名单、投影与原 `admin_blackhouse_status_operations` 回执在同一事务提交；未注册用户同样可封禁/解除。`legacy.admin.007` 在启动时一次合并旧 JSON 与 SQL `is_ban=1` 名单，保留原 JSON 和历史回执；导入 marker 必须落盘并纳入运行期就绪检查，避免漏导入或旧名单重放导致已解禁用户复活。请求不建表，路由重建不再加载 JSON 或写黑屋状态；缺 schema/marker 或存储故障时路由 fail closed，但管理命令和未路由 matcher 保持原豁免。重复 operation 返回历史回执，不把历史封禁结果描述为当前状态。聚焦回归 `76 passed`（5.25 秒），隔离 lifecycle 回归 `2 passed`（0.83 秒）；最终 admin、feature/admin、command store 和 Phase2/progress 扩大集 `369 passed, 14 subtests passed`（27.15 秒，另有预加载 anyio warning）。五项黑屋门禁全 true，冻结 membership 有效、integrity errors 为空；其余 163 项仍受阻，不代表全局完成。

| 顺序 | 入口组 | 状态与下一步 |
| --- | --- | --- |
| 1 | 运行配置 6 条：群修仙、私聊、自动灵根、自动宗名、欢迎开/关 | 本组接入 `AdminConfigApplication -> AdminConfigRepository`；`JsonConfig` 继承同一仓储，旧读者与 Web 备注/置顶/全量群共享锁与缓存。修复三处字段错配。 |
| 2 | 已迁移资产/重置 13 条：传承、修为、造化、轮回、创造、毁灭、修仙适配、新手礼包、悬赏、塔、BOSS、仙缘、易名 | 已验收现有 admin_asset/work/tower/boss/base 写终端，未重迁。修复 4 个 helper 重复 status 关键字、易名/BOSS误报、传承并发批次误报，补仙缘剩余退款、跨笔回滚、缺失赠礼者保护。全服饰品目标读取仍无界，不宣称已优化。 |
| 3 | 兼容输出 9 条：修仙手册、广播帮助、艾特测试、按钮测试、消息信息、取链接、取raw、取reply、全量申请 | 已统一归类为允许保留的兼容路径；行为测试覆盖当前事件解析、分页/按钮边界、共享发送回退和授权 URL 保留，不新建九套 application。共享消息记录不等于零基础设施写入。 |
| 4 | 黑屋 3 条 | 已验收唯一 SQL 名单 owner，封禁、解除、列表与路由读取共用；修复注册玩家路由失效和失败误报，启动导入、回放、回滚与路由回归通过。 |
| 5 | 命令管控 3 条 | 已验收共享 JSON owner 与路由消费者适配，注册表/别名同源，取消命令写后重建；聚焦、扩大回归和四项门禁通过。 |
| 6 | 广播 6 条 | 已验收进程任务 owner、原子 claim、取消/清空与普通消息补发；只读历史及发送经注入端口，扩大回归和六项门禁通过。 |
| 7 | 生成秘境、重载items、用户伪装，各 1 条 | 已验收，扩大回归、四项 runtime 门禁与冻结证据通过。复用秘境 SQL owner 并保护投影回放；Items 目录严格完整发布；伪装共享进程映射接入真实身份消费者。 |
| 8 | 转换QQID 1 条 | 已验收，聚焦、扩大回归、四项 QQID 门禁与冻结证据通过。持久批次、请求 alias、冻结映射及 child ID 闭合恢复，复用既有四库 ID writer；admin 50 项全部归类、blocker 为 0，后续固定 arena、back。 |

配置使用原字段 `group/private/root_selection/sect_name/welcome_disabled_groups`，保留未知字段和原 JSON 路径；读缺失文件返回默认值，兼容 `JsonConfig` 构造仍创建默认文件。写入同目录暂存后原子替换，失败不发布缓存；相同开关值不重写。锁只覆盖同一进程内的线程，不提供多进程协调或 operation-ID 回执。全局欢迎关闭时不再谎报本群已开启。notice 的生命周期进程状态不在这六个命令的完成范围内。

资产结果组定向回归 `271 passed, 2 subtests passed`，覆盖真实 helper/handler 函数抽取执行和默认 feature 仓储。已关闭的历练重置仅因发现 `schema_missing` 被报成功这一明确回归而补失败分支，未重迁其 owner。仙缘清池没有 operation-ID 回执，全量读取及 SQL 历史重复玩家行等边界不在本组性能完成声明内。

兼容输出组由 3 名子代理独占交付两套行为测试和 progress gate，主线程修复与验收；admin 和 Phase2 聚焦集 `241 passed, 11 subtests passed`。四个 event 调试命令只提取当前事件，不查历史消息或下载链接。全量申请保持公开命令，只构造授权 URL；键盘群主点击限制不能等同于链接回退的权限保证，目标页面负责实际授权。共享发送基础设施仍有消息记录、回复计数及配置/映射/渲染资源访问，本组未迁移这些共享状态，也不宣称输入解析或发送耗时有界。

黑屋列表仍全量读取，旧 `blackhouse.json` 只作为一次导入来源，不再随新封禁/解除实时同步。回退旧代码需要恢复同一时间点的一致数据库与 JSON 备份，不能直接让旧代码读取已过期 JSON。没有线上性能测量，本组不宣称已证明运行提速。

## 用户流程
The compatibility command remains available while the new application boundary is enabled.

## 命令与别名
The historical package owns command names during the compatibility release. New names are added only through this feature manifest.

## Web API
No new HTTP route is exposed in this migration slice. Existing URLs remain served by the legacy web adapter.

## 数据模型与迁移
Migration `legacy.admin.001` records the slice in `game_db`; the feature-owned repository and explicit service port now own the operation boundary while legacy tables remain the compatibility data source.

管理员 `易名` 使用 `BaseApplication -> BaseRenameSqlRepository` 更新玩家道号，并复用 `base.002` 的操作回执；旧管理器仅用于目标查询，不再执行该写入。

## 事务与失败回滚
SQL 事务写入的回执语义以各仓储为准。运行配置使用绝对值设置和原子 JSON 替换，相同值不重写，没有 operation-ID ledger；写入失败保留旧文件与旧缓存。不能把所有管理操作概括为统一的回放契约。

## 定时任务
No new scheduled jobs. Legacy jobs stay registered through the compatibility scheduler.

## 配置项
`admin_enabled` controls the application boundary and defaults to true.

## 适配器差异
Command and web adapters translate transport input into the application DTO; business code does not import NoneBot or Flask.

## 测试与手工验收
Run the feature application test and the full architecture gate. Repeat the same operation ID to verify replay.

## 灰度开关、回滚和已知限制
Legacy algorithms and schemas remain behind the repository adapter for one complete release cycle; the compatibility hit counter determines when removal is safe.

## Manifest 清单
- `command: ID交换`
- `command: ID更新`
- `command: md模板`
- `command: 传承力量`
- `command: 修为调整`
- `command: 修仙手册`
- `alias: 修仙管理`
- `command: 修仙适配`
- `command: 全局广播`
- `command: 全量申请`
- `command: 关闭进群欢迎`
- `alias: 关掉进群欢迎`
- `alias: 禁用进群欢迎`
- `command: 创造力量`
- `command: 取raw`
- `alias: 原始JSON`
- `command: 取reply`
- `alias: 原始reply`
- `alias: 取引用`
- `command: 取消广播`
- `command: 取链接`
- `alias: 提取链接`
- `alias: 获取链接`
- `command: 启用修仙功能`
- `alias: 禁用修仙功能`
- `command: 启用私聊功能`
- `alias: 禁用私聊功能`
- `command: 启用自动宗名`
- `alias: 禁用自动宗名`
- `command: 小黑屋`
- `command: 广播帮助`
- `alias: 广播指令`
- `alias: 广播说明`
- `command: 开启自动灵根`
- `alias: 关闭自动灵根`
- `command: 开启进群欢迎`
- `alias: 启用进群欢迎`
- `alias: 打开进群欢迎`
- `command: 指令列表`
- `command: 指令禁用`
- `command: 指令解禁`
- `command: 按钮测试`
- `command: 易名`
- `command: 查看小黑屋`
- `alias: 小黑屋列表`
- `command: 查看广播`
- `alias: 广播列表`
- `command: 毁灭力量`
- `command: 消息信息`
- `command: 清空仙缘`
- `command: 清空广播`
- `alias: 清除广播`
- `command: 生成秘境`
- `command: 用户伪装`
- `command: 私聊广播`
- `command: 群聊广播`
- `command: 艾特测试`
- `command: 解除小黑屋`
- `alias: 放出小黑屋`
- `alias: 解禁`
- `command: 转换QQID`
- `command: 轮回力量`
- `command: 造化力量`
- `command: 重置世界BOSS`
- `command: 重置历练`
- `command: 重置悬赏令`
- `command: 重置新手礼包`
- `command: 重置状态`
- `command: 重置通天塔`
- `command: 重载items`
