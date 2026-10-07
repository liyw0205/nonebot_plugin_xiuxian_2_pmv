# 重构切片执行与磁盘控制协议

状态：执行中
适用范围：全面底层重构第二阶段

2026-10-08 QQ bind Web owner：合并 `qq_bind_routes.py` 五条冻结 route，一次接入 `QqBindApplication`，保留 Flask 登录/CSRF、HTTP 状态与响应契约；owner 统筹 QQ create/poll、600 秒进程内任务 key、完成态回放、`.env` 安全落盘与重启 gate。2 名只读代理分别审计副作用/共享 owner 和重试/安全/测试缺口，未编辑、未运行测试；主线程实现及串行验收。最终 route/helper 聚焦 `18 passed`（3.38 秒），Phase 2/progress `55 passed, 49 subtests passed`（5.79 秒），隔离初始化后 inventory `2 passed`（7.47 秒，含 NoneBot 初始化）；scope gate 约 0.4 秒。membership `7787a74a...ff51fac` 有效、integrity errors 为 0，状态 `285/133/19/59`。QQ create/poll HTTP timeout 各 15 秒，`run_async` 在同步 Flask handler 内等结果，页面首次延迟 1.5 秒、后续每 2 秒请求 poll；本地重构不能消掉远端等待，也未测真实 QQ 请求延迟。task/key 仍是进程内状态，本片未改持久化或 poll 策略。用户 `boss_info.json` 未暂存。下一组继续按冻结 route 的 source module/共同 provider owner 合并审计，不按 URL 拆片。

2026-10-08 Admin commands Web owner：同一 `commands.py` 批次关闭 `/commands` 静态页和 `/execute_command` 写路由；POST 七类命令统一转交 admin_asset owners，不按 item/accessory 子分支拆重复批次。新增全服修为 set-based receipt、管理页 retry-stable `request_id`，修复单人 `hmll` 误发放和余额扣减结果误报；空 roster 饰品批次恢复先查运行回执。两名只读子代理分别复核兼容语义与 exp batch 原子/性能边界，未修改文件、未运行测试；主线程实现及验收。定向回归 `34 passed`，初始化 NoneBot 的 admin_asset/route/source 聚焦回归 `103 passed, 1 warning`（6.89 秒）；progress/inventory 测试 `20 passed, 1 warning`、inventory freshness 通过。Phase2 计数 `280/133/19/64`、496 项 membership 有效、`integrity_errors=[]`。迁移后全服物品/饰品/传承石不再按每用户循环开连接，但 HTTP 仍同步处理全部 chunks；全服 exp 旧路径本来是一条 SQL。本地测试/静态调用图不是生产延迟测量，不宣称已验证提速。下一批合并审计 `qq_bind_routes.py` 的五条冻结 route；用户 `boss_info.json` 未暂存。

2026-10-07 Configuration backup JSON owner：一次收口 10 条本地/云端配置导入导出、备份、列表、恢复、删除和同步路由，统一进入 `ConfigBackupApplication -> ConfigBackupRepository`；`UpdateManager` 保留兼容转发。导入及 Web 恢复仅暂存配置值，仍由 `/save_config` 应用；云恢复本地优先、缺档才下载；修复自动云备份开启时手动云备份重复上传同一文件。JSON 最大 16 MiB，云端列表限 2 MiB/1000 项，下载校验 JSON、no-follow 读取并原子安装；Flask 管理员/CSRF/旧响应契约保留。配置 feature、manager adapter、Flask HTTP、progress 聚焦集 `51 passed`；config owner gate 全 true，冻结 membership/source inventory 有效、integrity errors 为 0，状态 `184/97/19/196`。全局 Phase 2 `--check` 仅因其余 196 个 blocker 返回 1。路由挪动影响的 28 条已关闭 backups route source anchor 已在同批刷新。下一组只处理 `/backups` 页面消费者分类与 `/manual_backup` 跨类型编排；它们不是这 10 条配置 JSON 路由的重复 owner。未访问运行数据库或真实 WebDAV。

2026-10-06 Plugin backup cloud ZIP owner：同组关闭云端插件 ZIP 列表、单/批量同步、批量删除和云恢复取档：路由经 `PluginBackupCloudApplication -> PluginBackupCloudRepository`，云恢复只在本地缺档时同步，再复用已完成的 restore application；`UpdateManager` 保留兼容转发。PROPFIND/XML 限 2 MiB/1000 项，下载限 4 GiB，下载校验 ZIP、同目录临时写入并原子安装；批处理最多 100 项，逐项成功/失败结果不变。同期刷新受路由挪动影响的冻结源码锚点和旧云恢复测试契约。插件备份、Flask HTTP、UpdateManager adapter、progress 与 Phase 2 gate 聚焦集合 `79 passed, 4 subtests passed`（5.09 秒）；catalog/local-file/restore/cloud 四个 owner gate 均 true，Phase 2 membership 有效、integrity errors 为 0，状态 `165/97/19/215`。全局 `--check` 仅因剩余 215 个 blocker 返回 1；progress checker 约 0.28 秒、scope gate 约 0.31 秒。此前未推送的直接原因是 owner 仍有行号证据和旧 API 测试未闭合，并非远端拒绝；中断在验收前暴露了切片过小的问题。插件 ZIP 路由到此归类完成；队列留在 backups feature，接下来按共同 repository/state owner 收口数据库 ZIP、配置 JSON 和跨类型手动备份后再前进。未访问运行数据库或测线上延迟。

2026-10-06 Plugin backup restore owner：本地 `/restore_backup` 与云端 `/cloud_restore_backup` 共用 `PluginBackupRestoreApplication -> PluginBackupRestoreRepository`；云端保留本地命中优先、缺失才下载。ZIP 在任何覆盖前校验成员路径/类型/数量和可用空间，先落入暂存目录，再覆盖配置的数据/插件根；SQLite 替换和运行时句柄重载留在 updater runtime port，成功后才更新版本标记。多文件 overlay 不具备整体原子性，失败不更新版本标记。补充恢复 application/repository、SQLite 同盘原子替换、路由权限/响应/下载契约及故障边界测试；聚焦集合 `58 passed, 4 subtests passed`。Phase 2 为 `161/97/19/219`，membership 有效；修正本次路由编辑导致的 3 个旧 ZIP 文件路由行号证据漂移后，integrity errors 为 0，`--check` 仍只因 219 个未完成 blocker 返回 1。未访问运行数据库或测生产恢复耗时。本 feature 前两批将 catalog 与本地文件操作拆为两次，重复触碰同一 Web 模块，属于切片过细；以后同一 feature 的路由先按共同 repository/state owner 合并，只有独立事务或恢复风险才单独拆分。

2026-10-06 Plugin backup list owner：冻结项 `GET /get_backups` 一次收口为只读 catalog owner，不扩展 `/api/v1/backups`、备份创建/恢复/删除。`PluginBackupCatalogApplication -> PluginBackupCatalogRepository` 用单次 no-follow `stat` 枚举有效 regular `backup_*.zip`，跳过 symlink/并发消失文件，缺失目录只返回空列表；旧页面的 `{success, backups}` 与 `filename/version/timestamp/created_at/size` 保持，去掉未使用的绝对 `path`。Flask、catalog、Phase 2 source-bound、progress 聚焦测试 `39 passed, 2 subtests passed`；冻结状态 `156/97/19/224`，membership/integrity 有效，Phase 2 `--check` 只因其余 224 个 blocker 返回 1。inventory freshness 未通过：隔离 `XIUXIAN_DATA_DIR` 缺少旧插件运行资源，exporter 在初始化导入时退出并报 stale；未刷新 inventory、未访问运行数据库。

2026-10-06 本地插件备份文件 owner：在 `PluginBackupFileApplication -> PluginBackupFileRepository` 内一次收口下载、单删和批删三条本地 ZIP route，列表和文件操作共享 `backup_*.zip` 名称校验；下载 no-follow 打开 regular file，删除拒绝 symlink/非 regular entry。保持下载附件、管理员/CSRF、单删 envelope 与批删逐项部分成功响应；不扩展到 database/config/WebDAV、恢复或创建备份。feature 与 Flask HTTP 回归 `13 passed`，Phase 2/progress 聚焦集 `34 passed, 2 subtests passed`，总计 `47 passed, 2 subtests passed`；状态由 `156/97/19/224` 更新为 `159/97/19/221`，integrity errors 为 0。未访问运行数据库或测生产延迟。下一 owner 仍在 backups feature，处理其余插件 ZIP 冻结路由，不跳到其它 Web feature。

2026-10-06 Entertainment music capability group：一次完成冻结项 `点歌`、`点歌翻页`、`选歌`、`点歌配置`，帮助仍保留为静态兼容路径；没有按 handler 拆成四次重复改动。旧搜索默认每页 15 秒、最多 3 页且 HTTP 重试 2 次，调用方 30 秒超时后底层线程仍继续占用 4 个 I/O 槽之一。新 `EntertainmentApplication.music` 保留配置/选歌列表的进程内语义，限制列表至 128 个 session，每个 session 歌曲字段编码量至 512 KiB；搜索最多 50 首/5 页，零重试、5 秒单请求 timeout、512 KiB 响应及逐块执行的 20 秒总 deadline，command handler 等待上限 25 秒。聚焦应用/provider/handler contract tests `21 passed, 4 subtests passed`；完整 Entertainment 与 phase2 gate 回归 `92 passed, 1 warning, 6 subtests passed`。Phase 2 冻结成员 496 项、membership 有效、integrity errors 为 0，阻塞项降至 308，`--check` 只因其余既有 blocker 返回 1。用户 `boss_info.json` 改动未进入本组。

2026-10-05 compensation gift/redeem batch：复核并闭合同一奖励目录下 14 个冻结入口，其中 10 个业务 handler 已走 `CompensationApplication -> CompensationRepository` 的 SQL 定义/领取仓储，4 个静态帮助 handler 按允许保留的兼容路径分类；未重复改写已有业务实现。奖励发放在 immediate transaction 内写资产与唯一 claim `(reward_type,record_id,user_id)`，业务唯一键保证同一用户同一奖励至多发一次；`CompensationRewardClaimSqlRepository.claim()` 接收但不使用 `operation_id`，因此不主张 operation-ID replay 或冲突检测，若要求该契约应另立切片。聚焦仓储、schema 与 handler 契约 `22 passed`；冻结 membership/source inventory 不变、integrity errors 为 0，状态为 `34/49/19/394`，phase2 `--check` 仅因剩余 blocker 返回 1。

2026-10-05 Entertainment NewAPI user-info batch：冻结项 `command:entertainment:newapi信息` 经 `EntertainmentApplication.resolve_info_targets -> EntertainmentRepository` 有界读取账号凭据，复用 redacted target DTO；最多查询 8 个账号、最多 4 路并发。旧 `mode` 缺省时继续按 secret 推断 token/cookie；用户信息请求使用无重试 client、(5,15) 秒 HTTP timeout、512 KiB 流式响应上限，回复按 UTF-8 字节限制 64 KiB 且为截断提示留空间。handler 只解析 selector、调度请求、格式化和发送，不改账号 JSON 或其它 NewAPI 命令。聚焦回归 `37 passed`（另有单项 HTTP source-quality 检查通过）；phase2 冻结总数 `24/45/19/408`，membership hash 不变。

2026-10-05 Entertainment WebDAV read-only batch：将冻结项 `command:entertainment:webdav查看`、`command:entertainment:webdav列表` 和 `command:entertainment:webdav信息` 按共享 WebDAV 读取边界合并实现，而不是拆成三个重复切片。`webdav查看` 经 `EntertainmentApplication.webdav_bindings -> WebDavRepository.load_bindings` 读取现有绑定 JSON；`webdav列表`/`webdav信息` 经 `webdav_list`/`webdav_info -> WebDavRepository.propfind` 执行 bounded PROPFIND。绑定文件最多 256 KiB/32 行，响应最多 2 MiB、128 个 XML 条目，字段和 URL 有界归一化；缺失、损坏、超限文件不创建、备份或重写。旧 handler 只做参数提取、格式化、发送，保留参数错误、401/403/404 和非 200/207 文案；绑定、删除、链接、文件留在后续独立副作用组。新增 WebDAV repository/application、207/状态错误/上限/不改写测试及 handler contract，聚焦集合 `36 passed`；phase2 gate integrity errors 为 0，冻结 496 项计数 `19/45/19/413`，membership hash `7787a74a...` 不变。

2026-10-05 frozen Phase 2 execution：只处理冻结清单中的 blocker，不追加玩法/命令范围。`docs/refactor_phase2_legacy_path_items.json` 的 496 项为固定完成分母：314 个 legacy command、40 个 legacy job、116 个 Flask route 和 26 个 effect/exclusion/suppression/family 项。当前状态 `24 已迁移 / 45 允许兼容 / 19 不可达 / 408 受阻`；受阻项拆为 292 个 command、115 个 route、1 个 non-command matcher family。Backlog 有 330 条，含 323 个已知清单外 AST command candidate；新增发现只进 backlog，不自动扩分母。

2026-10-05 `command:status:ping测试` slice：默认 handler 保留“正在测试网络延迟，请稍候...”进度消息、固定八站点顺序、国内/国外分组、延迟 emoji、平台参数和超时文案；探测与渲染收口到无状态 `StatusApplication -> PingProbe`，固定 allowlist、`asyncio.gather` 并发、同步 runner 注入测试，不写数据库、JSON、operation ledger 或 schema。当前计数更新为 `16/45/19/416`，membership/source hash 不变，完整性错误为 0。实测真实探测约 10 秒是外部 ICMP 超时（单 host 10 秒），不是迁移或 feature application 开销；缩短 timeout/增加 `-W` 会改变旧行为，暂不调整。

2026-10-05 `command:info:我的ID` slice：默认 handler 保留旧 matcher 与消息契约，但 active identity read 已收口到 `PlayerAvatarApplication -> AvatarStateSqlRepository` 的 player-only read-only UOW；缺库/schema/row 回退真实 ID，无请求期 DDL 或持久写入。avatar 聚焦回归、handler source contract 与 phase2 gate 通过，计数更新为 `15/45/19/417`，membership/source hash 不变。

2026-10-05 `GET /search_users` slice：旧 Flask alias 保留裸数组兼容契约，但通过 `PlayerProfileApplication -> PlayerProfileSqlRepository` 使用 SQLite 只读 URI 查询；LIKE 参数转义、64 字符 query 上限、10 条结果上限和缺库/缺表 fail-closed 避免 legacy `execute_sql` 的目录创建/WAL/提交副作用。新增 `/api/v1/info/users/search` admin canonical endpoint，统一 envelope。定向测试 `17 passed`，phase2 gate 计数更新为 `14/45/19/418`，membership/source hash 与 integrity errors 保持有效。

清单身份 hash 绑定 `id/kind/entry/source`；status 和调用图证据只在同一冻结身份下更新。Gate 对所有冻结 command 校验 AST 启动闭包 declaration、绑定 handler，要求当前 declaration/handler 的源码位置同时出现在证据和调用图中；已迁移/兼容项还必须有绑定当前 handler 位置的真实下游边，拒绝只放“reviewed”占位文案，且不能残留未闭合 handler 占位边。job 项校验 legacy manifest 与 admin scheduler API 的手动执行路径。APScheduler 装饰器原函数是独立自动执行路径，不对所有 manifest job 一概声称可自动调度。Web/admin 与命令默认可达性基于源码静态导入/注册闭包，不是 live runtime 测量；dynamic/conditional imports 和 non-command callbacks 仍是发现限制。P7 真实发布 gate 单独保留，不并入 phase 2。`command:entertainment:newapi信息` 已迁移：账号目标经有界 repository 读取，远端查询限制 8 个账号/4 路并发、512 KiB 响应，输出受 64 KiB 字节上限保护；`newapi删除` 仍受阻，其 compatibility repository 改 JSON 后另行完成 SQLite ledger，缺少跨存储崩溃恢复。`newapi帮助` 是允许保留的兼容路径，`newapi查看` 与手动 `newapi签到` 已迁移；签到 POST 响应、回复与历史读改写有上限，远端成功到本地历史间的崩溃窗口只进 backlog。冻结项当前计数为 `24/45/19/408`，其中受阻为 292 command、115 route、1 non-command matcher family。新发现只进 backlog，不扩分母。为避免同一子插件在多个零散提交间反复触碰，后续先按 feature 汇总关联冻结项与共同状态/网络所有者，再按共享边界成组完成；只有独立事务/恢复风险才拆为单独能力组。一次变更完成其 handler 集合的代码、测试和证据后再提交，避免已迁移能力在后续切片重复返工。P7 仍由独立真实发布证据门禁判定。

本轮验证（2026-10-05，`command:entertainment:newapi签到`）：NewAPI/repository/json-store/phase2/source-quality 聚焦回归 `50 passed, 1 warning, 2 subtests passed`；关闭 pytest cache 与 pyc，专用 `/tmp/codex-phase2-newapi-checkin-final-20261005` 已清理。phase 2 共 496 项，当前 `13/45/19/419`，membership/source provenance 有效、integrity errors 为 0，backlog 330；`phase2_legacy_path_gate.py --check` 返回 `1` 仅因 419 个既有 blocker。门禁性能复测为 phase2 约 3.4 秒、聚合 progress 约 4.1 秒；另一次完整 P0-P6 审计由约 17 秒降至约 6 秒，主要优化了生命周期、inventory 和 scheduler 的 token 预筛选，判定结果不变。清单 JSON 解析与 `git diff --check` 通过。P7 真实发布门禁保持独立，本轮未运行；用户 `boss_info.json` 修改保留且不纳入阶段提交。

**耗时诊断（2026-10-05）**：一次真实五库 migration apply 总耗时约 `1.8~2.0s`，但五库 `schema_migrations.duration_ms` 合计约 `85~87ms`，迁移 SQL/文件修改只占约 `4.5%`；其余时间来自 Python/旧插件导入、旧命令清单 AST 扫描和 SQLite 连接初始化。无 pending 的第二次 migrate 仍约 `1.5s`，证明主要成本不在修改。当前 `.venv` 实测 Phase2 冷启动约 `2.7~3.0s`、命令索引缓存命中约 `0.4~0.5s`，completion audit 约 `4.6s`，architecture audit 约 `6.6s`；完整门禁串行执行时，检测/审计仍占绝大多数。最大热点是 `phase2_legacy_path_gate._default_legacy_command_inventory`：每个新进程需检查约 104 个旧源码文件、解析约 638 个命令候选；现在索引只遍历导入时语句并跳过函数体，且有带源码指纹的跨进程缓存。架构门禁的 `check_manifest` 仍会加载约 42 个旧插件，约 `3.4s`，这是剩余最大导入热点。另有 `ping测试` 单独受外部目标超时影响，当前约 `10s`，与迁移无关。因此当前“迁移效果差、耗时长”主要是检测/导入成本和 416 个旧路径 blocker，不是数据库修改；后续高收益方向是复用单个审计进程或提供静态门禁模式，而不是继续优化 migration SQL。

冻结项 `command:entertainment:newapi查看` 先经 `EntertainmentApplication.list_account_summaries -> EntertainmentRepository` 完成 1 MiB/48 条受限只读读取，输出 secret-free summary；缺失/损坏/超限文件不创建、备份或重写。该路径之后才完成 `newapi签到` 的 bounded target selection、1 MiB streamed POST response 与 64 KiB/3-row history write；其它 NewAPI 绑定、信息、删除、自动签到和全局扫描仍保留在各自冻结项/backlog。账号 JSON、历史与 `.invalid.*.bak` 是持久数据，不是可清理缓存。

以下按日期记录的计数均为当时历史快照；当前状态以本节、scope manifest 和 `scripts/phase2_legacy_path_gate.py` 报告为准。

2026-10-04 Steam compatibility and bounded response：冻结项 `command:entertainment:Steam喜加一` 的默认 handler 保留为兼容路径，不标记迁移；调用图止于共享 `HttpClient` 对外部 API 的 GET、1 MiB 流式响应上限、格式化和发送，不触及本地游戏/玩家状态或持久文件。该项保留至独立 P7 真实发布门禁允许移除。Steam/HTTP 定向回归 `14 passed`。冻结成员仍为 496、状态 `9/42/19/426`（已迁移/兼容/不可达/受阻），membership hash 不变；phase 2 gate `--check` 仍只会因 426 个已冻结 blocker 未完成而失败，P7 独立。

2026-10-04 bounded Phase 2 completion contract：暂停追加玩法切片。完成分母固定为 `docs/refactor_phase2_legacy_path_items.json` 中 identity hash 验证通过的 496 项；每项使用 `已迁移 / 允许保留的兼容路径 / 不可达 / 受阻`，必须有调用图与源码证据。`scripts/phase2_legacy_path_gate.py --check` 以该冻结清单、来源快照、启动导入闭包和逐项 handler 绑定计算 v1 状态；inventory drift 和清单外候选只进入报告 backlog，不改成员、不改 hash、不扩展分母。AST 静态启动闭包扫描得到 637 个未被 suppression 的旧命令候选，323 项不在 v1 中；这是源码推导，不是实跑 runtime 注册数。323 项含源位置与 handler 调用图，等待显式范围评审；动态导入、条件注册和非命令回调不由该扫描器自动发现，须人工登记 backlog。v1 关闭只表示冻结成员完成，不宣称全仓旧路径已经清零。P7 发布门禁继续由 `scripts/refactor_completion_audit.py` 单独使用真实 release evidence 判定，不合并进 phase 2 完成状态。

2026-10-04 bot overview read boundary：冻结项 `command:status:bot信息` 改由 `StatusApplication -> BotOverviewSqlRepository` 执行只读统计；逐次读取 game/trade 数据库，无结果缓存，请求不建表，缺 schema 返回 unavailable。固定日期、旧筛选口径、实时重读、缺表 fail-closed 和 handler 委托测试已补；冻结 membership 未变，P7 独立。

本范围方案允许最多 2 名只读子代理分别检查启动可达性/调用图和门禁/P7 分离；优先复用已有审计，只有互不重复的问题才派工。子代理不得改文件、导入/编译/运行插件、访问运行数据库/凭据或生成缓存；主线程负责代码、门禁、文档和串行验证。本轮使用 2 名只读子代理，分别交叉检查冻结项调用图/真实可达范围与完成门禁/P7/backlog 边界；两者均未改文件或运行测试。验证禁用 pytest cache/pyc；测试使用隔离临时目录，结束后只清理由本轮创建且确认进程退出的产物。磁盘可用低于 10 GiB 或 `MemAvailable` 低于 512 MiB 时停止重任务；不触碰运行库/WAL/SHM、正式备份、持久 receipt、`.git`、`.venv` 或用户数据。

2026-10-04 admin ID-update cutover：冻结命令 `command:admin:ID更新` 改由 `AdminApplication -> AdminIdUpdateSqlRepository` 执行。四库步骤回执分别与单库白名单列替换原子提交，game DB 计划冻结实际目标列、ledger/outbox 和恢复进度；players 目录改名可跨进程重试。四库容量预检计入 main/WAL/SHM 并保留 reserve，SQLite cache 限 2 MiB、`temp_store=FILE`，ID 路径段校验及目标目录冲突拒绝；与 `ID交换` 共享锁、互相阻止待恢复操作。game roster 和 player-data 查询缓存按实际受影响字段失效；ID 交换路径同步补齐 player-data 缓存失效。`.005` 步骤回执路由四库，`.006` 恢复计划仅 game DB，未添加请求期 DDL。聚焦仓储/迁移/handler 与 ID 交换回归 `24 passed`；phase 2 gate 9 项通过，状态 `9/41/19/427`，membership 不变，`--check` 因 427 个 blocker 返回 `1`，P7 独立且未运行。

2026-10-04 admin ID-swap cutover：冻结项 `command:admin:ID交换` 的默认 handler 改由 `AdminApplication -> AdminIdSwapSqlRepository` 执行；`ID更新` 与 QQID 转换仍是独立范围。四个 SQLite 文件分别原子提交交换步骤和 receipt，game DB 保存 operation ledger/outbox/恢复计划；players 目录按阶段 journal rename，startup callback 恢复未完成操作，rename 与 phase commit 之间中断可由路径状态识别。用户数据更新前按四库主文件大小的三阶段 WAL 工作预算预检容量并保留共享 64 MiB/10% reserve，空间不足则留 pending 且不执行数据更新；仅使已初始化的 roster cache 失效，不构造 legacy manager。交换 UoW 局部关闭 FK 检查、SQLite cache 2 MiB、`temp_store=FILE`。迁移 `.003` 路由四库、`.004` 仅 game DB；无请求期建表。复用 1 名只读调用图审计、未重复派工。专项测试 `11 passed`；phase2/progress/UoW 首轮 `22 passed, 1 failed`（唯一失败为过时计数断言），修正后对应单测 `1 passed, 5 deselected`。inventory `--check` 通过；冻结 496 项计数为 `8/41/19/428`，membership/source inventory 有效，backlog 未扩大，phase 2 未完成，P7 独立且未运行。隔离数据目录，关闭 pytest/pyc cache；保留用户 `boss_info.json`。

2026-10-04 frozen-scope backlog reporting：phase 2 gate 将相对冻结来源快照新增的 inventory 条目作为具体候选并入只读 `backlog` 报告，列出来源字段、原始条目和评审理由；不写回 scope、membership hash 或冻结成员，也不影响已冻结成员的完成计算。回归 `6 passed`，当前 496 项的状态计数与来源快照未变；phase 2 仍有 429 个 blocker，P7 仍独立且本轮未运行。

2026-10-04 stateless 60S news command compatibility classification：冻结项 `command:entertainment:60S读世界` 只做外部新闻 API GET，通过共享 `http_client`/`run_blocking_io` 格式化发送，无本地业务状态或持久文件读写；作为明确兼容 matcher 保留到独立 P7 真实发布门禁允许移除，不宣称已迁移，不新增 app/cache/scope member。phase 2 gate 回归 `6 passed`；当前状态为 496 项、7 migrated/41 compatibility/19 unreachable/429 blocked，membership 不变。清单 gate `--check` 因 429 个 blocker 返回 `1`，聚合 `phase2_complete=false`，inventory exporter `--check` 通过；P7 保持独立且未运行。

2026-10-04 non-command matcher family call-graph audit：冻结项 `legacy.matcher.non_command_dispatch` 仍受阻。静态确认七个默认注册：`xiuxian_work`、`xiuxian_bank` 的业务 regex，`media_parse_link` 的三条 regex，admin empty fallback 的 message matcher，以及直接 `nonebot.on_notice` 注册的 group lifecycle handler；admin 与 entertainment 包分别显式导入对应模块。6 个 message/regex handler 经 `on_compat` 注册，其中 work 仍维护旧 JSON 投影、bank 写操作由 feature application 承担、media/fallback 保留旧 handler；notice 不走该 provider并执行 lifecycle/config/send 逻辑。单一 family 状态无法诚实概括为全迁移或兼容，故保持 blocker 并在 evidence 中列出所有确认入口；不新增冻结成员。清单外新发现先入 backlog，只有显式评审才变更 scope/member hash。phase2 gate 回归 `6 passed`，JSON 报告 496 项、430 blockers、membership 有效、无新增 inventory candidate，P7 独立且未运行。

2026-10-04 frozen work matcher call-graph refinement：复用 1 名只读子代理核对 `do_work` 默认注册、feature repository 所有权和测试边界；代理未改文件、跑测试、导入插件或访问数据库。后续只在同一 frozen blocker 内收敛 claim/settlement 边界：claim repository 现在在同一 immediate UoW 原子提交 `user_cd`、status=2 `work_offer_snapshots`、含选中任务与开始时间的 `work_active_snapshots` 和 receipt；handler 的 `savef(sync_snapshot=False)` 只保留 JSON 文件投影。Settlement 经 `WorkClaimApplication.get_active_snapshot` 使用只读 feature repository；历史 status=1 active rows 可由 `user_cd` 只读补全，只有缺少 active snapshot 时才走 `readf` 兼容 fallback。投影 SQL 失败会使整个 claim 回滚。聚焦 claim repository/application、abort-cleanup、结算计算器和 handler 契约 `28 passed`；全量 work source-quality 约束另有 `245 passed, 1 failed`，唯一失败是与本片无关的 BOSS 静态断言旧文本。刷新与过期仍写 JSON，bank/media/fallback/notice 等 matcher 仍未闭合，因此 family 状态继续为 `受阻`，496 项 membership/hash 未变；没有新 migration、请求期 DDL、scope 扩张或新增缓存。

2026-10-04 title all-user enumeration cutover：`赠送称号 <title> all` 的默认目标来源由 `_sql_message().get_all_user_id()` 改为 `TitleGrantTargetApplication -> TitleGrantTargetSqlRepository` 的只读 game DB 查询。按 SQLite 游标逐行选择 `substr(user_id, 1, 1 MiB + 1)` 并冻结 `user_xiuxian` 全表结果到匿名临时 spool；超长值在取入 Python 后的有界前缀检查或 SQLite 长度限制错误中失败，保留重复记录且不添加过滤、去重或排序；spool 容量按增量调用既有 `preflight_capacity` 并保留累计 64 MiB/10% 余量，单个 ID 限 1 MiB。名单读完即关闭 game DB UoW，之后后台 job 才逐条执行原 `get_user_unlocked_titles -> TitleApplication.execute('grant')`；operation ID 和重复行导致的重复回放计数不变，缓存不新增/不主动清理。空间不足、缺 schema、异常/取消或 job 未启动都会关闭本轮临时文件；失败发生在任何称号写入之前。冻结项 `title.grant_all.user_enumeration` 更新为“已迁移”，没有新增范围成员、migration 或持久状态。复用既有 1 名只读 title 调用图审计，本片未重复派工；title target/Application `11 passed`，phase 2/progress 回归 `22 passed`。inventory freshness 通过；phase 2 为 496 项，其中 `7/40/19/430` 分别是已迁移/兼容/不可达/受阻，membership 有效且 source inventory 无新增候选；phase 2 `--check` 因 430 个既有 blocker 返回 1。聚合 progress 报告 phase 2 未完成、P7 独立，本片未运行发布门禁。测试串行、隔离 `XIUXIAN_DATA_DIR`、关闭 pytest/pyc cache，测试与临时产物在最终复核后清理。

2026-10-04 admin single-player status reset cutover：默认单人 `重置状态` handler 的快照和写入现均经 `AdminApplication -> AdminPlayerStatusResetSqlRepository`；feature repository 在只读/`BEGIN IMMEDIATE` UoW 内读取和原子更新状态+receipt，仅校验启动迁移预建 schema，不做请求期 DDL。复用既有 receipt 表及严格 payload/force 冲突、operation ID、`state_changed`/`duplicate` 和 force/CAS 语义；旧 `AdminPlayerStatusResetService` 留作未从默认 handler 到达的兼容批处理实现。随后从同一冻结命令项追出道号查询旧读，改复用既有 `get_user_profile_by_name -> PlayerProfileApplication -> PlayerProfileSqlRepository`；原 `rowid ASC LIMIT 1` 重名选择顺序不变，查询只读且缺库不建库。冻结项 `admin.player_status.reset.single` 与 `command:admin:重置状态` 均更新为“已迁移”，没有新增清单成员或 migration。1 名子代理只读核对了调用图和单人回执契约；代码、测试与清单由主线程修改，测试串行。单人/全服/共享 profile 仓储及 phase 2 gate 回归 `24 passed`；inventory exporter `--check` 通过。旧顶层 `tests/test_admin_player_status_reset.py` 单独尝试收集仍被 `xiuxian_base` 导入不存在的 `get_active_user_id` 阻断，未执行该模块测试，未扩展修复无关入口。phase 2 计数为 `496` 项、`已迁移 6 / 允许兼容 40 / 不可达 19 / 受阻 431`；membership/source inventory 有效且无新增 backlog 候选，`--check` 因其余受阻项返回 1。P7 由独立 release audit 提供真实发布证据，本片未伪造或合并该门禁。pytest cache/pyc 已禁用，专用 basetemp 已清理；磁盘余量 `20G`、RAM available 约 `1.4GiB`。未访问运行数据库、备份、业务回执或用户 `boss_info.json`。

2026-10-03 legacy stone-gift counter reset scheduler boundary：默认新 application 的灵石额度按业务日记录；旧 `stone_limit` 午夜重置现在只在显式 `XIUXIAN_STONE_GIFT_LEGACY_HANDLER=true` 时执行，开关在模块初始化时冻结并与 legacy matcher 注册保持一致，默认不再构造旧 manager/写旧表。旧表和 rollback handler 保留，无 migration 或运行数据变更。1 名子代理只读审计了真实调用点、schema 和回滚风险，未改代码/跑测试/访问数据库；主线程实现并串行验收。facade lazy-reader/legacy gate `2 passed`、目标 compileall、inventory freshness、diff check 通过。scheduler runtime 测试收集被当前树的 `get_active_user_id` 导入错误阻断，未执行；不扩展修复无关代码。pytest/字节码缓存关闭并清理专用临时目录；用户 `boss_info.json` 保留。该切片只关闭旧额度定时写入，不代表 player/economy 旧写路径整体清零。

2026-10-03 direct-breakthrough poison outbox recovery：恢复扫描与 handler 直接重放共用 payload identity 校验，检查 JSON、operation ID、user ID、aggregate ID 和事件 ID；坏 JSON、字段缺失、错 operation/user 或无匹配回执只对对应事件执行指数退避，单条异常不再终止批次。覆盖 5 条 poison 后第 6 条有效事件恢复及 handler 错配 payload 不执行 effects；recovery/repository/handler `45 passed`、refactor progress `7 passed`、source guard `1 passed`，inventory freshness 通过。pytest cache/字节码关闭、测试串行；本片没有需要独立只读审计的分工，未启用子代理。architecture 全量脚本因当前树中 `get_active_user_id` 导入缺失失败；未扩展修复无关代码。专用临时目录清理后磁盘余量 `20G`、RAM available `1.4GiB`；未触碰运行数据库或用户 `boss_info.json`。恢复正确性已关闭，整体 `exit_ready=false`。

2026-10-02 dongfu harvest snapshot ownership：灵田槽位规范化提取到共享 `plant_slots.py`，扩建与收获共用同一 legacy 字段恢复规则；`DongfuApplication.prepare_harvest_snapshot` 经 feature repository 在 player UoW 内 CAS 保存首份随机奖励快照并复用既有快照。收获 handler 不再 `_save_dongfu`；背包满保留快照，成功发奖、槽位清空、legacy 字段同步和快照删除在结算事务内完成。复用既有 `map.017` 的 `harvest_settlement` 列，无新增 migration。progress `7 passed`、洞府 feature/handler `38 passed`；inventory freshness、目标 compileall、diff check 通过。子代理分工：复用一份只读 migration/调用路径审计结论；代码修改、测试、资源检查和最终整合由主线程负责，测试串行。pytest/pyc 禁用或隔离，专用 `/tmp/dongfu-harvest-*` 与上一轮残留测试 fixture 已清理；未触碰 `.venv`、`.git`、`data/`、运行数据库/WAL/SHM 或用户 `boss_info.json`。收尾磁盘可用 `20G`、RAM available `1.4GiB`。下一片单独处理洞府随机目标全量候选读取与共享缓存容量边界；整体 `exit_ready=false` 仍由全局旧 transaction services、`xiuxian2_handle` 和正式发布/P7 证据缺失阻塞。

2026-10-02 dongfu status display writeback removal：`我的洞府` 只在读取对象上按业务日派生计数，不再调用
`_save_dongfu`；真实巡山/潜入事务保持负责持久化当日归零及计数更新。source 回归 `8 passed`、聚合 progress
用例 `1 passed`，compileall/diff check 通过。下一片继续审计收获快照/扩建同步和随机目标旧读取边界。

2026-10-02 dongfu infiltration eligibility writeback removal：`_can_infiltrate`/`_can_intrude` 只做
只读资格判断，不再通过 `_save_dongfu` 写回跨日计数；成功/失败 repository 已在 operation transaction 中
按 `day` 冻结并原子更新计数。聚焦洞府/潜入/progress 回归 `33 passed`，保留显式计数、收获快照和扩建 mutation。
下一片继续审计随机目标/同节点读取与剩余洞府写路径，不以本片通过宣称洞府 mutation 全部完成。

2026-10-02 dongfu status read boundary：`_get_dongfu` 改由 `DongfuApplication.status` 读取，默认值、灵田
兼容同步及地图地脉补全都只改返回对象；只读仓储不触发动态建表/补列。player-only `map.017` 启动迁移补齐
既有 `dongfu_status` 状态列并保留当前行数据。聚焦洞府/地图回归 `76 passed`，状态仓储/source/progress 用例
通过；inventory 已重生成，架构检查未新增洞府错误，但全局仍有 direct-breakthrough contract、game_events/info
和 reconcile operation-id 等既有问题。
旧 `_save_dongfu` 仍负责多个显式变更命令，不在本片范围。下一片优先追踪这些调用是否已由 feature repository
覆盖，再单独切断真实默认旧写；不得以移除查询写回替代 mutation 收口。

2026-10-02 world-events status read boundary：世界事件状态命令的展示路径移除冗余
`_save_state` 写回；生命周期应用已经是状态持久化 owner，查询不再通过
`PlayerDataManager.update_or_write_data` 触发请求期 schema/写入。新增 source regression `11 passed`，
无 migration、无运行数据访问。下一片继续按真实调用图审计仍可达的 player/economy 旧写入。

2026-10-01 root unittest shutdown harness：`test_sign_in_effects_wiring` 原先用两次 `asyncio.run()` 分别启动和关闭 runtime，使全局后台队列 worker 绑定到已关闭的 event loop，导致 `lifecycle.shutdown()` 永久等待 drain。测试现改为在同一 coroutine/event loop 内完成 start、断言与 shutdown，符合真实 runtime 生命周期；定向 unittest `2 tests` 通过。

2026-10-01 root unittest shutdown harness follow-up：全量顺序最小复现发现更早的 `test_back_alchemy_wiring` 也在两次 `asyncio.run()` 间拆分 lifecycle，遗留的全局 queue unfinished-task 使后续签到测试仍等待 drain。现改为同一 loop 启停；按 suite 顺序运行 Back/SignIn 两个模块共 `3 tests`，全部通过。

2026-10-01 本轮切片：连续爬塔、世界首领训练入口和突破入口的前置体力退款改由 `restore_player_stamina -> PlayerStaminaApplication -> PlayerStaminaSqlRepository` 承担。单用户返还按首个 `user_xiuxian` 行做封顶 CAS；缺数据库、schema 或用户时 fail closed，不执行请求期 DDL。无 migration、无运行数据访问；验收后只清理本轮测试/pyc/临时目录，保留运行数据库、WAL/SHM、`.venv`、`.git`、`data/` 和用户 `boss_info.json` 修改。

2026-10-01 normal pvp dynamic attribute read boundary：普通切磋 handler 的动态属性存在性读取统一经 `get_player_attributes -> PlayerAttributeApplication`，不再直接调用 `get_user_real_info`；原始 HP/MP/体力/修为快照仍由 profile 查询供结算 CAS 使用，战斗计算与结算语义不变。新增 source/progress 门禁，未新增 schema/migration；本片只触及默认读取入口，旧动态属性公式和资产事务保持兼容边界。

2026-10-01 dynamic attribute read boundary：信息页、功法状态、默认玩家战斗辅助和 JSON 战斗 helper 的动态属性读取统一经 `PlayerAttributeApplication`；应用保留 `ratio`、`include_current` 和 buff/impart/accessory/tianti provider 注入端口。旧 `get_final_attributes` 移至显式 `compatibility/legacy_player_attributes.py`，本片不改公式或资产写入。专项属性/profile/provider 回归通过，并补齐 Info/Buff 幂等测试的通用 `operation_ledger` 夹具。

2026-10-01 本轮切片：定时体力恢复已纳入 `PlayerStaminaApplication`。恢复使用 SQLite `MIN` 上限和 `rowid` 有界批处理，不创建用户列表、不在运行时建表；`0` 恢复点直接 no-op，缺 schema 返回 `schema_missing`。验收后只清理本轮 pytest/pyc/`__pycache__` 与专用临时目录，保留 `.venv`、`.git`、`data/`、数据库和 WAL/SHM。

## 目标

将重构按冻结清单拆成边界清晰的单项。只实施当前冻结范围内的条目；单项验收后清理本轮明确产生的测试缓存和临时输出，再开始下一项。清单外发现只进入 backlog，必须经过显式范围评审和版本升级后才能纳入，不得自动追加玩法切片。

## 后续路线（非当前执行队列）

以下是全仓重构的长期路线，不是当前可直接领取的切片队列。当前唯一完成分母是 `docs/refactor_phase2_legacy_path_items.json` 中冻结的 496 项；先按其四态和调用图关闭 v1，不从下列领域目标自动开新玩法切片。清单外发现保持 backlog，显式批准新 scope 版本前不进入 phase 2。P7 真实发布证据仍由独立门禁判定，不并入 v1。

1. **player/economy 旧写路径清零**：继续按真实调用图迁移 `xiuxian2_handle`、剩余 `transaction_service` 的默认 handler、scheduler 和 Web 写入；旧实现只能作为显式 compatibility/rollback，不得由默认入口构造或调用。
2. **其余领域边界**：按 cultivation/training、combat/dungeon/boss、sect/trade/scheduler/Web 的顺序完成真实入口、随机/时间注入、operation receipt、启动 migration、跨库恢复和缺 schema fail-closed。
   - 当前洞府/地图进度：共享 field-list cache 的 64 项/8 MiB 总预算/1 MiB 单项预算及过期回收、指定潜入单目标和随机潜入有界选择均已提交推送；地图指定道号论道/战绩单目标只读查询、随机论道有界选择及附近展示有界去重均已验收完成。地图完整 list 的可写 UoW/attach、单个大字段峰值、SQL 工作集/耗时及逐候选 I/O 仍单独开放，不以候选容量上限宣称进程 RAM 全部有界。不得复制或主动清空 `ITEMS_CACHE` 等其他玩法共享缓存。
   - 本轮随机候选切片复用 1 名只读子代理核对过滤/分页/公平性/取消与 schema 兼容；子代理不改代码、不跑 SQLite 测试、不访问运行库或生成缓存，主线程负责实现、串行验收和资源收尾。
   - 当前副本已完成项：player-only `dungeon.007`、prepared settlement schema、operation-scoped RNG、game-only `dungeon.008` 输入冻结、player-only `dungeon.009` 队伍成员索引/同事务投影同步和 session schema/ABA 检查，以及 reset clock 贯穿、scheduler 业务时区、跨午夜日期冻结与 manual 跨日回放；本轮 `.010` 到期索引/ID 路由和单 worker 关闭默认每邀请一个 sleeper 的任务资源边界。成员读取与探索结算都改为单行选择；真实 `user_xiuxian` owner 为 game DB，队伍写入仍只在 player UoW 内执行。
   - 当前副本未完成项：Web 认证 actor 绑定、正式 migration/recovery/P7，历史损坏邀请/冲突 receipt 的受控修复。intent Web manifest 已补齐，现有 permission/CSRF guard 未改；全局 `user` resolver 默认允许，operation identity 校验不能替代认证。通知不是 outbox，不能宣称持久补发。legacy team reader、`PersistentTeamInviteMapping` 无界迭代/请求期 DDL 和旧 expiry helper 仍保留兼容实现，但最新限定调用图未证明默认 handler 可达，不以 helper 存在作为迁移依据。跨队历史重叠成员尚未清洗，本片仅统一选择最小 `team_id`；清洗必须独立制定备份、冲突决策和对账方案。
   - 队伍回滚边界：legacy writer 不维护 `.009` 投影，不得与默认新 writer 混用。回滚期间一旦发生旧写，再回默认入口必须停机并受控重建/核验投影；migration ledger 不会自动重跑已应用的 `.009`，不能仅切换开关恢复服务。
   - 最近只读调用图审计已确认普通修炼结算切到 `BuffApplication`，没有新的 `up_exp_` 旧 service 切片证据；`RewardService._grant_exp` 的生产入口仍未证明，不据此开切片。
   - 当前历练未完成项：普通修炼生命周期默认入口已由 `BuffApplication` 承担，后续只审计事件随机计划、排行榜有界分页和 `training_limit.py` 兼容读写，不重复迁移已关闭的结算边界。
3. **持久状态与恢复证据**：盘点 operation ledger、outbox、projection receipt、失败/死信和 bet/payout 等历史回执的保留窗口；在有备份、checksum、dry-run、restore、reconcile 和人工决策记录前，不删除或压缩任何持久状态。
4. **发布退出条件**：完成正式数据备份/迁移/恢复演练、P7 发布证据、全局 legacy 门禁和真实运行 readiness；只有脚本输出 `exit_ready=true` 且证据归档后，才可声明全面重构完成。

### 历史路线快照（2026-10-03，不驱动当前执行）

本节记录 v1 冻结前的路线安排，仅作历史背景；其中“自动进入下一条切片”等旧调度语句已暂停，不能覆盖上方的有限 scope、backlog 和 P7 边界。

1. **校准证据，不先改业务代码（已完成）**：1 名只读子代理对照 architecture 检查器、manifest、实际 blueprint 路由和已有回归，确认旧 dungeon explore intent Web permission/manifest 报告已过期。`FEATURE.routes` 已声明 `POST /api/v1/dungeon/explore/intent`/`user`，实际 blueprint 路由匹配，且 handler 使用 `guard("user", permission, write=True)`；既有测试也核对五条路由并覆盖 permission/CSRF。未改代码、未跑测试/导入/编译、未访问数据库或生成缓存；不重复补已有声明。若完整 architecture CLI 仍报告此项，后续须先定位实际运行差异，不把静态旧记录当作当前 blocker。
2. **回到首要 blocker：player/economy 默认旧写路径**。按真实生产调用图，每次只选一个由默认 command、scheduler 或 Web 可达的 `xiuxian2_handle`/`transaction_service` 写边界；先证明 owner、写入和回放语义，再迁到 application/repository。旧实现保留为显式 compatibility/rollback；不按旧 service/helper 的存在与否批量删代码。
3. **逐域关闭余项**：仅在 player/economy 阶段取得可验证进展后，按上方领域顺序继续。洞府/地图只处理列出的完整-list attach/工作集/逐候选 I/O 和真实默认写边界；副本队伍 legacy writer/reader、历史冲突修复、Web actor 绑定等各自独立计划，不合并清洗历史数据。
4. **持久状态与发布最后验收**：ledger、outbox、projection receipt、失败/死信、批次目标和备份均不是缓存；没有保留窗口、checksum 备份、dry-run、restore、reconcile 和人工处置证据前不删除/压缩。切片只清理由本轮明确生成的 `/tmp`、pytest/pyc 产物，不清系统 page cache、业务共享缓存、`data/`、运行库或未知文件；每次重任务前检查 RAM/磁盘阈值，测试和恢复保持串行。
5. **子代理分工**：本次已使用 1 名只读代理完成第 1 项证据交叉核对；后续只有在一个独立边界能明确文件/入口且不竞争 SQLite/测试资源时才再委派。代码、测试、资源监测、清理、文档和提交均由主线程负责；每片记录代理职责和未执行事项。

### 本轮全服状态重置方案（2026-10-04）

1. 只处理 `xiuxian_admin.restate_` 无参数的全服批处理。1 名只读代理已确认默认入口当前先调用旧 `get_all_user_id()` 全量 `fetchall`，经缓存 deepcopy、ID list/tuple/set/sort 和 JSON roster 多次复制；application 仍转发到 `transaction_service`，operation 表由请求期 DDL 创建。代理未改代码、运行测试/导入/编译、访问数据库或生成缓存。
2. 主线程把批处理 application 接到 feature-owned SQL repository，使用 game DB SQL 冻结去重 ID 到持久 target 表；单次工作最多取 100 个 pending target，保留稳定 child operation ID、单用户 reset receipt、started/retry 和历史结果语义。默认 handler 不再读取或捕获全服 ID 列表；`@用户` 单人重置及其他管理员路径不在本片。
3. 新增 `legacy.admin.002` startup migration 预建 batch operation/target/progress schema，并用 JSON1 SQL 回填历史 roster、原 progress 保持不变；migration 和新冻结都做磁盘预检，旧 payload 按估算工作集检查系统/cgroup RAM。SQLite cache 限约 2 MiB，临时排序使用文件。迁移先验证旧 payload、保留旧表/JSON，坏数据或空间不足时 fail closed，不删除持久历史。新请求路径只读验证 schema，缺 schema 返回明确状态，不建表。
4. 串行验证 old/new resume、completed replay、operation conflict、缺用户、目标写入与 receipt 故障回滚、低磁盘/损坏 legacy payload migration、player/game owner 路由和不调用 `get_all_user_id` 的默认入口。测试禁用 pytest/字节码缓存，所有数据库和缓存产物限于本片专用 `/tmp`；恢复演练与测试串行，收尾只清理本片已退出进程生成的临时文件并复核磁盘/RAM。
5. 子代理分工：复用前序 1 名只读默认调用图审计；本续行另委派 1 名只读 migration 路由、SQLite JSON1 与容量预检范例审计。两者均未改文件、跑测试/导入/编译、访问数据库或生成缓存；代码与测试不委派。主线程负责实现、串行验收、最终 diff、资源收尾和进度记录。切片完成且远端 SHA 核验后，自动进入 player/economy 下一条真实默认旧写路径，不把该批处理迁移当成全局 blocker 关闭。

**本片实现与验收（2026-10-04）**：`features/admin/tests/test_player_status_batch_repository.py` `8 passed`，覆盖 legacy JSON target 回填与 migration 重跑、原 progress 保留、冻结集合/100 人上限、低磁盘与低 RAM fail closed、新 operation 磁盘预检、receipt 故障重放（含 legacy forced child）、缺 schema、game-only migration 路由和默认 handler 无全量名单读取。refactor inventory 由 exporter 更新并通过 `--check`；`check_full_refactor_progress.py --json` 的本片四项门禁均为 `true`；全局 `exit_ready=false` 仍由既有 transaction service 与 `xiuxian2_handle` 阻塞。旧顶层 batch/single-reset 测试 collection 被已知 legacy plugin `get_active_user_id` 导入错误阻断，未执行且未扩大修复。最终回归使用专用 `/tmp/admin-reset-batch-final3-20261004`，关闭 pytest cacheprovider/pyc；本轮临时产物在验收后核对并清理。

### 子代理与资源约束

- 允许合理使用子代理，默认仅分派只读调用图审计、静态门禁和文档证据整理；本轮禁止代理测试、Python 导入、编译、恢复、生成缓存或访问数据库。不得扫描无关用户目录、读取凭据或执行正式 migration。
- 代码修改型子代理必须限定在一个小边界，先给出文件/行号和回滚点；主线程负责整合、复核 diff、运行验收并决定是否提交。默认最多 1 个只读子代理；只有资源检查通过且任务完全独立时才增加，并始终不超过 3 个。测试和恢复任务全部串行，不使用并行 pytest worker，不与子代理测试同时运行。
- 子代理不是默认步骤；仅当调用图审计、静态门禁、独立只读证据整理能与主线隔离时使用。方案/切片记录需注明子代理分工与并发数；所有代码修改、测试协调、资源监测、最终 diff 和提交由主线程统一负责。
- RAM 可用量低于 512 MiB、磁盘可用量低于 10 GiB 或 inode 可用量异常时，停止新测试/恢复任务，只保留必要的收尾和清理；测试默认串行、禁用 pytest cacheprovider 和字节码写入，批处理/查询必须有界。
- 每个切片使用独立 `/tmp/<slice>-*` basetemp、recovery 和 compile cache，验收后立即删除并复核 `df -hT`、`df -ih`、`free -h`。不清理 `.venv`、`.git`、仓库 `data/`、运行数据库/WAL/SHM、备份、配置或用户 `boss_info.json`。
- 进程缓存只允许 TTL/容量有界和按需加载；`ITEMS_CACHE` 等共享缓存不复制、不主动清空。operation ledger、outbox、projection receipt、失败/死信、批次目标和审计日志是持久事实，不属于可清理缓存。

## 单切片闭环

1. 读取进度、架构约束、当前工作树和磁盘使用情况。
2. 只选择一个真实未完成的 handler/route/application 边界。
3. 追踪旧入口和数据路径，先建立可失败的 focused 回归。
4. 按 domain/application/repository/migration/adapter 的实际边界实现，不在请求路径隐式建表。
5. 运行 focused tests、compileall、architecture、inventory 和 `git diff --check`。
6. 在显式临时数据目录执行 backup、migration dry-run/apply、readiness、reconcile 和 recovery smoke；不得触碰仓库 `data/` 或生产运行数据。
7. 更新进度文档，记录旧路径、新路径、测试、迁移路由、回滚点和未完成边界。
8. 验收成功后，清理本轮明确产生的 pytest basetemp、pytest cache、Python 字节码缓存和临时 receipt/log；清理范围不得包含 `.venv`、`.git`、`data/`、配置、备份或运行数据。
9. 重新检查 `df -hT`、`df -ih`、仓库和临时目录大小，确认工作树与运行数据未被误删。
10. 只有清理和磁盘复核完成后，才读取并执行下一个切片。

### 本轮队伍/session 方案（2026-10-03）

1. 先由只读子代理核对队伍查询调用图、legacy fallback 和 schema 缺口；范围限于源代码与静态门禁，不访问运行数据库、不改代码、不运行 SQLite 测试。
2. 主线程新增 player-only `dungeon.009` 成员投影 migration，使用 SQLite JSON1 在 SQL 内回填，不物化全队伍 Python 列表；随后将默认队伍查询和探索结算查询切换为索引 `LIMIT 1`，并在 create/join/transfer/leave/kick/disband 的同一 UoW 内维护投影版本。旧 schema fallback 只返回一行，但仍可能扫描旧 JSON，不能当作索引性能保证。
3. 主线程为 session replay/transition 增加文件、表、列预检和 read-only replay；缺 schema 返回 `schema_missing`，handler 显式失败，不创建缺失数据库或执行请求期 DDL。reset generation/operation ID 和完整 expected snapshot 必须存在，防止 ABA；已有 receipt 优先回放。rollback-journal 缺 schema fixture 验证数据库字节不变且不新增 sidecar，不泛化为 SQLite WAL 数据库永不产生 sidecar 的承诺。
4. 用户存在性只读查询真实 game DB，不在 player DB 复制用户表；game 缺文件/用户 schema 时不记失败 receipt，已有队伍 receipt 仍可回放。子代理复核 owner、typed JSON 成员、跨队选择、ABA 和 replay-first 缺口，主线程实现修正。
5. 测试按串行顺序执行：migration malformed/mixed JSON、索引/query plan/版本同步、receipt 失败整体回滚、分库 owner、缺 projection/依赖 schema、session 缺库/缺表/缺列、退出 handler、progress/source/architecture/inventory/内存编译/diff check。测试 fixture 必须显式应用 `.009`，不修改历史 `.004/.006/.008` migration 实现；隔离 recovery 覆盖五库备份、restore dry-run/restore 和 reconcile，不接触运行库。
6. 验收后仅清理本轮专用 pytest/pyc/`__pycache__` 与 `/tmp` 产物，复核 `df -hT`、`df -ih`、`free -h`；不删除持久回执、运行数据库/WAL/SHM、备份或用户文件。主线程统一整合 diff、记录真实失败基线并决定提交，子代理不并行争抢 SQLite 资源。legacy writer 回滚边界和历史重叠成员必须保留为未完成项，不能以投影上线宣称已经修复。

### 本轮 Reset 时钟方案（2026-10-03）

1. 已复用 1 名只读子代理核对 manager/application/repository、scheduler 和 replay 契约；确认默认 UTC 时钟与 scheduler 业务时区不一致，application 丢弃注入 clock，跨午夜还可能混用 operation ID 日期、payload 日期和展示日期。代理不改文件、不跑测试、不访问运行数据库或生成缓存。
2. 主线程将 clock 贯穿 manager/application/reset repository，生产 lazy manager 显式使用 scheduler 时区；automatic operation ID 必须接收显式业务日，crossday 的 ID/payload 复用一次冻结日期。展示使用同份已发布 global snapshot 的日期、代次和模板，不再重新取今天拼接。
3. 同 manual operation 且未显式指定日期时，manager 从持久 receipt 恢复原业务日；repository 的显式日期/source 冲突契约保持不变。回放只同步当前已发布全局状态，不重新发布旧副本或重抽模板；历史 receipt 日期不改写。
4. 先建立失败回归，再串行验证上海午夜/DST、daily/crossday 去重、跨午夜冻结、真实 feature manual 跨日重试、显式日期/source 冲突、同实例注入 clock 和 receipt 时间；随后验证副本既有行为、source/progress/inventory、内存编译和 diff。本片不改变 schema/migration 或正式数据，不重复执行已完成的索引切片。
5. 使用本片独立 `/tmp/codex-dungeon-reset-clock-20261003`，禁用 pytest cacheprovider/pyc；测试完成后清理专用目录并复核资源，再提交推送、自动进入下一队列项。发布时区修正需受控核验当前 global 业务日，既有 UTC 历史日期不得自动重标记；隔离测试不能替代正式发布演练。

### 本轮邀请过期任务方案（2026-10-03）

1. 复用 1 名只读子代理核对默认邀请调用图、NoneBot/QQ 通知路由、scheduler 生命周期和回放风险；不改文件、不运行测试、不访问运行库或生成缓存，不新增代理。主线程负责代码、迁移、串行测试、证据和提交。
2. 默认邀请不再逐条 `create_task` 或持有 `bot/event` 睡眠。单个 scheduler job 使用 `max_instances=1`、coalesce 和每轮最多 100 条的持久到期扫描，常量大小 keyset 游标允许越过整批坏行、扫完回绕重试，启动/重启后继续处理已存在的 pending 邀请；不删除邀请或 operation receipt。沿用 DeferredScheduler 的 composition-root 激活和既有 scheduler manifest/lazy target 声明，默认 5 秒 tick 直接运行 feature worker，不经过 `JobExecutor._completed` 去重集合，避免新增高频无界缓存；CLI/Web 手动执行仍走既有 scheduler bridge。
3. 新增独立 player-only `dungeon.010`，建立 pending 到期索引，保存最小 `bot_id/source_message_id/notification_scene` 路由。冻结原接收 bot（不使用 `assign_bot` 选出的其他 QQ 应用）并区分群/频道，路由随首份邀请冻结，不新增内存缓存；旧无路由邀请仍能过期。默认 handler/扫描缺 schema/index fail closed，不做请求期 DDL，不改历史 migration。无路由的既有 feature API 仍兼容旧 schema。
4. 过期状态与成功 receipt 同事务提交；未到 deadline 不写终结 receipt，同 ID 可在时钟回拨后重试。成功 replay 返回 duplicate，不重复通知。已存在的旧 not_expired receipt 仅作为非终结状态恢复，更新为最终结果时与邀请状态原子提交，不删除历史行。
5. 先完成本轮所有状态/receipt 提交，再串行、最佳努力通知；单条 2 秒、整轮 10 秒预算，超预算丢弃剩余提示。无 bot、路由过期、发送异常不影响后续邀请，不通过无限重试/任务积压补发。该通知不是持久 outbox，提交与通知之间崩溃可能丢通知，恢复保证仅覆盖邀请状态。
   - SQLite 扫描/过期使用零锁等待，锁冲突立即结束本轮并重试，逐条让出事件循环；共享 UoW 尊重显式 timeout（默认 30 秒不变），不额外创建线程或后台任务。持锁回归必须验证 heartbeat、取消和解锁后恢复，不能只证明通知限时。
6. 先建失败回归，再验证索引/query plan、100 条上限、路由冻结、重启恢复、精确 deadline/回拨、join/reject 竞争、receipt 失败回滚、重复执行和异常隔离。所有测试与 recovery 串行，关闭 pytest/pyc 缓存并使用 `/tmp/codex-dungeon-invite-expiry-20261003`；进程退出后仅清理本轮产物并复核磁盘/RAM，保留运行库/WAL/SHM、备份、持久回执和用户 `boss_info.json`。

本片执行收口：代码 `e1391cd0` 已推送，最终聚合回归 `197 passed`，静态/progress/source 聚焦 `27 passed`，额外聚合 progress/scheduler/锁补口 `7 passed`；42 项邀请边界与 UoW 合并回归 `45 passed`。隔离五库备份和 restore dry-run/restore 成功，265 项 migration 的回执路由为 `203/58/7/1/1`，`.010` 仅 player，reconcile clean。完整 architecture 仍有 17 项既有错误、整体 `exit_ready=false`；未跑全量 pytest 或正式迁移/P7。验收后专用目录已删除（收尾约 4 MiB，早期旧测试约 25 MiB 已清），磁盘 `19G`、RAM available 约 `1.4GiB`、inode `15%`。下一片复用 1 名只读代理审计 intent Web permission/manifest 的真实鉴权链；不跑测试、不访问运行库、不生成缓存，主线程写方案与实现。

### 本片 Intent Web 声明方案（2026-10-03）

1. 已复用 1 名只读代理核对真实路由链；`adapters/web/blueprints/dungeon.py` 的 intent 已与 replay/prepare/settle 共用 `guard("user", permission, write=True)`，先检查 permission 再检查 CSRF。architecture 的两项 intent 错误来自 manifest 路由索引漂移，不是独立证明的运行时 guard 缺失；不重复改业务 intent 持久计划。
2. 最小实现仅在 `features/dungeon/manifest.py` 补 `POST /api/v1/dungeon/explore/intent`、permission `user`，同步 feature 文档与 inventory。主线程增加聚焦 Web 回归：五项 POST 路由声明、注入拒绝 permission 时零应用调用、缺/错/跨会话 CSRF、Idempotency-Key 优先、同 user replay 与不同 user 的 409/conflict 且不泄露原结果。
3. 现有 Web `user` permission resolver 默认允许，operation identity 的 user_id 校验不等同于认证 actor 绑定。该通用策略保持独立未完成项，本片不扩改全部 Web auth，也不以注入拒绝 resolver 的测试宣称默认策略已加强。
4. 子代理只负责上述静态审计，不改文件、运行测试或访问运行库；主线程串行测试、静态门禁、清理和提交。继续关闭 pytest/pyc 缓存并使用独立临时目录，复核 10 GiB 磁盘/512 MiB RAM 阈值；上一邀请切片目录已清理，不重新保留测试副本。

本片执行收口：manifest、feature 文档与 inventory 已补齐 intent；运行时 guard 和全局 auth 未改，无新增 migration。新增 Web 边界回归修复前为 `1 failed, 35 passed`，唯一失败是漏声明；修复后的 Web/副本仓储/inventory 聚合 `97 passed`，progress/architecture 单元 `25 passed`。完整 architecture 两项 intent 错误消失，`web_permissions/manifest_routes/manifest_documentation` 均为空，仍有 15 项既有错误；不宣称全量门禁已通过。progress 保持 `exit_ready=false`；inventory freshness、2 个 Python 文件内存编译、diff check 通过。隔离五库 backup、restore dry-run/restore 和 265 项 migration recovery 成功，路由数 `203/58/7/1/1`，reconcile clean（operations/outbox/dead events 均为 0）；没有正式运行库迁移/P7。复用前一轮已完成的 1 名只读代理结论，不重复派相同审计；主线程实施全部修改与串行验收。进程全部退出后已删除本片专用 `/tmp/codex-dungeon-intent-web-20261003`（收尾约 6.5 MiB），仓库保护范围外未发现 pytest/pyc 缓存；磁盘可用 `20G`、RAM available 约 `1.4GiB`、inode 使用 `14%`。保留运行数据库/WAL/SHM、备份、持久回执和用户 `boss_info.json`；下一片只读审计洞府候选与共享 field-list cache 的真实资源边界。

### 共享 Field-List Cache 既定方案（已实现验收）

1. 资源复核后启用 1 名只读子代理，限定审计 `PlayerDataManager._field_list_cache` 的生产调用、复制和失效路径；主线程核对洞府候选 matcher。代理未改文件、执行测试、导入代码、访问运行库或生成缓存。本次并发仅主线程与 1 名只读代理，无并行测试；方案不依赖扩大代理数。
2. 真实资源问题已证明：`player_data_manager.py:189/309` 的双查询入口共用无界字典，TTL 使用 monotonic 但不删除过期 key；`get_all_field_data` 保存深层 JSON，`list_users_by_fields` 的 key 包含条件和排除用户。`xiuxian_dongfu/__init__.py:1265/1271` 的真实随机/指定潜入入口分别调用 `:525/439` 的 helper；每个随机发起用户缓存近似相同的完整候选列表，随后完整建立 profile/candidates 列表。
3. 下一片仅关闭共享缓存的保留边界，不改 SQL、返回结果/顺序或候选随机规则。初始策略采用最多 64 项、总计 8 MiB charged bytes、单项最多 1 MiB；这是保守策略而非运行测量。统一私有 get/store/drop/clear/expire helpers，在既有 `RLock` 内维护记账，FIFO 淘汰；替换、删除、表/字段失效和 close/reconnect 都更新额度。只清此缓存，不复制或清空 `ITEMS_CACHE` 等其他缓存，也不改持久数据。
4. key、容器及深层 JSON 内容全部计量；只限定条目数/行数不能约束长字符串。估算遍历必须有节点/深度上限，超大 key/value、超预算或不支持对象不缓存，仍返回查询结果；先预检再复制，副本计量通过后才淘汰旧项并发布，避免为了无法入缓存的新值先清掉有效缓存。保留返回副本隔离；非有限 TTL 不得形成永久缓存，保持 TTL 0 不缓存。
5. 两个入口每次读取/保存前按需扫描过期项，TTL 0 的查询也维护缓存；至多 64 项使扫描有确定上限，不新增 heap、后台 task 或每查询 timer。保留非滑动 monotonic TTL。无访问时过期项仍可能驻留，但有容量上界；charged bytes 不是整个进程 RSS，也不能承诺 TTL 到点立即释放 RSS。若后续要求闲置时主动回收，必须另立生命周期拥有的单一维护任务，只访问已存在的 manager，不为清缓存新建数据库连接，不持有 bot/event。
6. 主线程先建立假 clock/cursor 与隔离 manager 回归：双入口共享预算、超大 key/字符串/深层 JSON 绕过缓存、命中副本隔离、精确过期边界、替换/FIFO 多项淘汰、表/字段失效和 close/reconnect 额度清零、TTL 0/查询失败不发布。所有测试与恢复串行，关闭 pytest/pyc，使用独立 `/tmp/codex-field-list-cache-20261003`；验收后立即清理本片产物并复核磁盘/RAM。不得在未实现/验证前把本方案列为已完成。
7. 下一独立洞府候选片处理一次请求的 RAM/事件循环占用：当前 `fetchall`、JSON 解析、profile/candidates 全量物化不由缓存上限解决。设计 keyset 有界分页、协作让出、无全量列表的公平随机选择和指定名字单行查询，保留过滤、玩家数据 game owner 和结算 CAS；如需索引另加启动 migration，不请求期 DDL。地图自己的 `_get_all_in_same_node` 已走 `MapApplication.nearby_players -> MapNearbyPlayersSqlQueryRepository`，但仍 `query_all` 全量，这是另一个独立可达读取边界，不与洞府 helper 混淆。`save_doc` 提交后未触发 field-list 失效是既有一致性缺口，本缓存片不扩大其语义，后续须单独核对真实写入调用。

本片执行收口（2026-10-03）：按上述容量/TTL/复制/失效/生命周期方案完成双入口缓存，无新增 migration、线程或后台任务，不改 SQL、返回顺序和其它共享缓存。1 名只读子代理复核实现并建议补充累计节点、深度边界、deadline 溢出和同 key 替换/FIFO 用例，主线程补齐并串行验收；代理未改文件、跑测试、导入代码、访问运行库或生成缓存。cache 定向 `37 passed`；cache/DB backend/洞府状态/地图聚合 `79 passed`；progress/architecture/inventory 聚合 `28 passed`；缓存三项 progress gate 全 true，状态仍明确保留 `query_materialization_open`。inventory freshness、4 个 Python 文件 AST 内存编译、diff check 通过。完整 architecture 仍有既有 15 项错误，`exit_ready=false`；隔离五库备份/恢复、265 项 migration recovery 和 reconcile clean 通过，路由数 `203/58/7/1/1`，不是正式发布演练。pytest/pyc 禁用，进程退出后已删除专用 `/tmp/codex-field-list-cache-20261003` 测试/恢复/日志产物，未触碰运行数据库/WAL/SHM、正式备份、持久回执、`data/`、`.git`、`.venv` 或用户 `boss_info.json`。收尾磁盘 `19G`、RAM available `1.3GiB`、inode `14%`。下一片继续处理洞府随机候选/指定潜入的真实全量读取，不把本缓存预算当作一次查询或进程 RSS 上限。

### 指定潜入单行读取方案（已实现验收）

1. 复用 1 名只读子代理的洞府候选审计，代理已核对 matcher、profile 首行和 player/game schema，不改文件、不测试、不访问运行库；主线程实现和串行验收，不增加并发任务。
2. 将真实指定潜入 matcher 的 `_get_same_node_users -> 完整 profile 列表 -> next(user_name)` 改为 `DongfuApplication.nearby_target -> feature-owned read-only query`。位置仍读取 player DB 的 `map_status`，profile 仍归 game DB；只返回单个 `user_id/user_name`，使用参数绑定和只读 URI，不缓存全量候选、不请求期 DDL、不建缺失数据库。
3. 保留先选人再检查资格的顺序：game 历史重复 user_id 取最早 rowid，候选查询不先排除本人、未建府、无灵田或今日目标次数已满的用户，仍由后续 handler 检查并拒绝，不跳到更合格的同名后者。旧候选 SQL 没有 ORDER BY，并不保证同名顺序；新查询参考地图投影明确按 map rowid ASC 固定首人，这是行为确定化而非声称旧 SQL 保证。没有等级、成熟度或全服过滤。
4. 聚焦测试覆盖位置三维匹配、缺 profile/库/表/列、重复 game user_id、同名首行、自身与无洞府用户仍可被选中、引号道号参数绑定、SQL 输出单行和源调用图。单行返回约束的是 Python 候选物化，不保证索引未齐时 SQL 扫描耗时；`map_status` 唯一性没有新启动迁移保证，显式固定首行，不做数据清洗。
5. 使用 `/tmp/codex-dongfu-nearby-target-20261003`，关闭 pytest/pyc 缓存，测试/恢复串行，低于 10 GiB 磁盘或 512 MiB RAM available 停止重任务，验收后清理本片产物。下一随机片另实现 rowid keyset/初始高水位、协作让出和 reservoir sampling；地图 nearby 与单个超大 JSON/字段的峰值另列未完成，不混成同一次修改。

本片执行收口（2026-10-03）：Application/repository/matcher 已改为单目标只读查询，删除旧全量同节点 helper；无新增 migration、cache、DDL 或持久状态变更。位置保持 legacy JSON 解码、字符串 ID 参数和三维等值规则，名字按 BINARY 精确匹配；game 重复 profile 先取首行再过滤名字，map 同名首人明确 rowid 排序，资格检查顺序不变。洞府/潜入/地图聚合 `84 passed`，最终 named-target/inventory `39 passed`，cache/named-target progress `2 passed`；三项 named-target gate 全 true，inventory freshness、7 个 Python 文件 AST 内存编译、diff check 通过。完整 architecture 最终仍有既有 15 项错误，无新增洞府错误；全局 `exit_ready=false`。隔离五库 backup、restore dry-run/restore、265 项 migration recovery/reconcile clean 通过，路由 `203/58/7/1/1`，不是正式发布。复用 1 名只读子代理完成调用图/实现复核，代理未改代码、运行测试、访问运行库或生成缓存；主线程补齐其非阻断边界建议并串行验收。测试 pytest/pyc 禁用；导出器产生的临时仓库字节码已清理，inventory 的测试 import 误识别已通过普通 import 避免，未改扫描器或引入虚假表名。进程退出后已删除专用 `/tmp/codex-dongfu-nearby-target-20261003`（约 3.9 MiB），保护运行数据库/WAL/SHM、正式备份、持久回执、`data/`、`.git`、`.venv` 和用户 `boss_info.json`。磁盘 `19G`、RAM available `1.3GiB`；随机候选 RAM 边界仍开放。

### 本轮随机潜入有界选择方案

1. 只处理真实 `_get_random_dongfu_target`，不重做已完成的指定名字查询。通过 feature-owned 只读 repository 读取 player 洞府候选，按 rowid keyset 每页至多 256 行，冻结扫描边界；不使用共享 field-list cache，不把整个候选集或 profile 列表保存在 RAM。
2. 本轮选用独立页面/候选短只读事务，所有连接在异步让出前关闭，避免扫描期间长读事务延长 WAL 生命周期；这是磁盘压力优先的选择，不提供跨页全局快照。初始 rowid 高水位只冻结扫描边界：新行若大于高水位不进入，空洞中插入/rowid 复用可能被观察，删除与更新按后续读取时状态处理，已经越过的 rowid 不重扫。测试验证这些边界，成功、异常、取消均无连接留存，不跨事件保存 connection/generator 或 bot/event。
3. 保留排除本人、built=1、有效 seed 灵田、目标当日次数和首 profile 存在规则，不新增位置/等级/成熟度过滤。只对最终合格候选做 reservoir(k=1)，第 n 个合格项以 1/n 概率替换；异步扫描按页面/固定小批协作让出，故障丢弃部分扫描结果，不能把半个集合当成公平选择成功。最终结算 CAS 保持负责状态漂移拒绝。
4. 默认只复用 1 名只读代理核对过滤/分页/公平性及取消清理，主线程实现和串行测试；资源允许且任务独立才增加代理，最多同时 3 名。覆盖空集、晚页有效候选、各拒绝规则、均匀选择、页面/高水位、异常/取消/事务关闭和源调用图；测试关闭 pytest/pyc，使用新专用 `/tmp`，结束立即清理，不删除旧持久回执或业务数据。需要索引时只加独立启动 migration，先证明现有 schema/索引，不做请求期 DDL。

本轮实施细化：页只加载 rowid/user_id/built 等值标志，最多 256 个 raw 行，避免一次加载 256 个大 JSON；候选状态与首 profile 另用短只读单行查询。每 32 个 raw 行及页面末尾协作让出，业务日与 seed 配置在调用开始冻结，最终 matcher/结算 CAS 继续重查当前资格。正常 `dongfu_status` 的 user_id 主键使 reservoir 对合格用户均匀；遗留非唯一表仍保留旧重复 UID 候选加权语义，显式固定首次 player/profile 行，不在读取片清洗数据。独立页面事务只对实际观察到的合格候选流均匀，不宣称并发写入下存在一个全局时刻的均匀集合。单个大 JSON/字段、索引缺失时的 SQL 扫描时间和地图 nearby 全量读取仍是独立未完成边界。

本片执行收口（2026-10-03）：无新 migration/cache/DDL，所有读取错误 fail closed；optional 列按固定允许字段投影，保持 legacy planting fallback、缺次数默认及 JSON 日期/count 解码。正常启动 schema 的 cave 主键查询计划使用既有索引，不为本片新增索引。逐候选短 UoW/attach 和扫描所有 raw 元数据是容量/协作优先的明确取舍，不能宣称 N+1 I/O 或 SQL 耗时已经解决。洞府/地图/潜入/progress 聚合 `236 passed`，最终随机/inventory `55 passed`，architecture/inventory 单元 `17 passed`；四项随机 progress gate、inventory freshness、8 文件 AST 编译和 diff check 通过，完整 architecture 仍有既有 15 项错误，整体 `exit_ready=false`。首次定向运行的唯一失败是 WAL fixture 错误要求无 sidecar（45 项行为通过）；已改为 rollback-journal fixture，不承诺 WAL 只读永不产生 sidecar。隔离五库 backup、restore dry-run/restore、265 项 migration/reconcile clean 通过，路由 `203/58/7/1/1`；不是正式发布。复用 1 名代理审计；交接后核实代理另外执行过隔离测试/恢复/编译并产生缓存，未改代码或访问运行库，不计入上述主线程验收数据。其确认归属的专用目录和缓存已清理，本轮地图片明确禁止代理测试/导入/编译/恢复；主线程实现、补测和串行验收。所有进程退出后已删除本片专用 `/tmp`（约 3.9 MiB）与最终导入生成的未跟踪字节码，未碰运行数据库/WAL/SHM、正式备份、持久回执、`data/`、`.git`、`.venv` 或用户 `boss_info.json`。资源收尾为磁盘 `20G`、RAM available `1.2GiB`、inode `14%`；下一片只读审计地图 nearby 的真实默认调用图和消费方，再确定分页/目标选择/展示输出边界。

### 本轮地图指定目标只读查询方案

源调用图已核对：`xiuxian_map/__init__.py::_get_all_in_same_node -> MapApplication.nearby_players -> MapNearbyPlayersSqlQueryRepository.list` 仍整次 `query_all` 并复制全部 public profile。真实消费方为附近道友展示、论道目标选择、指定道号战绩查询三处；现有 repository 还是可写 UoW/可写 attach，缺库可能创建文件。这不是洞府已关闭边界的重复任务。

1. 先只关闭指定道号论道与战绩查询的全量读取，提供 feature-owned `LIMIT 1` 只读目标查询；保留三维位置、当前 CAST ID 等值与名称精确匹配。论道先排除本人，战绩查询允许本人，不能共用一个固定排除规则。同名顺序保留 map rowid，历史重复 profile 的并列顺序需明确并测试，不趁读片清洗数据。
2. 随机论道选择与附近道友展示另立两个切片：前者对实际候选流公平 reservoir(k=1)，后者最多展示 10 个去重用户，需设计无需无界 seen-ID set 的去重/抽样契约。不得直接 LIMIT 10 前十人替代随机展示，也不以一次查询有界宣称缺索引 SQL 时间有界。
3. 本片允许复用至多 1 名只读子代理核对三个消费方的 self、重复 ID/profile、名字/顺序与缺 schema 行为；主线程实现、串行测试和最终提交。代理不启动测试、不读运行库、不生成缓存，避免与主线程争抢 RAM/SQLite。
4. 测试覆盖同名、本人排除差异、缺位置/profile、重复 profile、CAST 等值、引号/大小写、只读 URI/路径空格、缺库/缺表不创建及 handler 源调用图；随后串行执行地图/论道相邻回归、progress/inventory/architecture、AST 编译与隔离恢复。继续使用独立 `/tmp`、禁用 pytest/pyc、资源低于 10 GiB/512 MiB 停止重任务，进程退出后仅清本片临时产物；现有 Boss JSON 修改和所有持久状态保持不动。

本片执行收口（2026-10-03）：query/application/DTO 和两个指定道号 handler 已切换，随机/附近路径未改；无 migration/cache/DDL。Map 的全部重复 profile 先匹配名字再 LIMIT，与洞府首 profile 规则不同；保留 map rowid，追加 profile rowid 明确旧并列顺序，现行 TEXT 道号精确匹配，不声称任意 REAL/BLOB 储存类型与 Python str 完全等价。target/presence 同短只读 UoW，缺库/schema、坏附库、第二条 SELECT 故障和所选 power 解析异常均 fail closed/关闭连接。修复前 3 回归失败；地图/论道/progress 聚合 `160 passed`，最终查询/inventory/architecture 单元 `62 passed`，source 定向 `1 passed`；四项 named-nearby gate、inventory freshness、7 文件 AST 编译和 diff check 通过。抽取真实 handler 配合真实 query/结算验证 replay 和位置漂移，不是 NoneBot 注册/在线 smoke；完整 architecture 仍为既有 15 错，`exit_ready=false`。隔离五库 backup、restore dry-run/restore、265 项 migration/reconcile clean 通过，路由 `203/58/7/1/1`，非正式发布。本轮 1 名只读代理仅静态审计，未测试/导入/编译/恢复/生成缓存；主线程实现、补测和串行验收，上轮代理额外测试事实已更正。禁用 pytest/pyc 并隔离 pycache prefix；进程退出后已删本片 `/tmp`（约 3.9 MiB），仓库无新增字节码，未触碰运行数据库/WAL/SHM、正式备份、持久回执、`data/`、`.git`、`.venv` 或用户 Boss JSON。收尾磁盘 `20G`、RAM available `1.3GiB`、inode `14%`。

### 本轮随机论道有界选择方案

1. 只关闭 `dao_qc` 无道号分支的 `_get_all_in_same_node -> list -> choice` 全量物化，指定道号使用本轮已完成查询不重做；附近展示另留独立去重/抽样切片。保留排除本人、同节点、缺 profile 跳过和全部重复 profile 候选权重，不引入洞府首 profile 或隐式去重。
2. 设计 map/profile rowid 联合游标，每页最多 256 条 raw 数值元数据，候选 public profile 逐行短只读读取；明确双库高水位、rowid 非快照插入/删除/更新/复用语义，避免为 RAM 节省却持有跨扫描长读而阻碍 WAL 回收。游标越过的是联合 pair，不是 profile 全局 rowid；后续 map 行仍读取较小 profile rowid，不重扫已越过 pair。只保存一份最终目标，公平 reservoir(k=1)，固定小批/页末协作让出，所有连接在 await 前关闭；读取故障丢弃部分结果，取消向上传播。
3. 复用至多 1 名只读代理审计联合游标/重复权重/资格漂移和公平性，不运行测试、导入、恢复或编译；主线程先建失败回归再实现，测试/恢复全部串行。覆盖空集、自身/缺 profile、晚页候选、重复加权与精确公平、负/零 rowid、高水位/游标、异常/取消/连接关闭、handler await 和结算漂移。
4. 继续独立 `/tmp`、禁用 pytest/pyc，资源低于 10 GiB/512 MiB 停止重任务；验收、缓存清理、资源复核、提交推送后再进入附近展示。单字段大小、SQLite JOIN/排序临时工作集、缺索引 SQL 耗时、逐候选 I/O 和全进程 RAM 上限仍须单独证明，不以分页候选数替代这些边界。

本片执行收口（2026-10-03）：随机 handler/application/query 已切换，指定查询和附近展示未扩改。修复前 3 回归失败；地图/论道/progress `242 passed`、最终 random/inventory/architecture 单元 `87 passed`（random 70 项）、source 定向 `1 passed`；五项随机门禁、inventory freshness、9 文件 AST 编译和 diff check 通过。完整 architecture 保持既有 15 错、`exit_ready=false`；隔离五库 backup、restore dry-run/restore、265 项 migration/reconcile clean、路由 `203/58/7/1/1` 通过，非正式发布/P7。1 名代理仅静态审计实现/覆盖，主线程实现和串行验收；无代理测试/导入/编译/恢复/数据库访问或缓存。禁用 pytest/pyc 并隔离 prefix，进程全部退出后删除本片专用 `/tmp`（约 6.6 MiB），无仓库字节码；保留运行数据、正式备份、持久回执和用户 Boss JSON。收尾磁盘 `20G`、RAM available `1.3GiB`、inode `14%`；无 migration/cache/请求期 DDL，回滚仅恢复查询及 handler 接线。抽取 handler 是功能回归，不是 NoneBot 注册/在线验收。

### 本轮附近展示有界选择方案

1. 已确认真实路径 `nearby_users_cmd -> _get_all_in_same_node -> nearby_players.list` 仍物化完整 JOIN/list，再创建 filtered list、无界 seen 集合与去重 list，超过 10 个用户才 sample。仅关闭此入口，不重做已完成的随机/指定目标选择或结算。
2. 展示按 BINARY 字符串用户 ID seek，SQL 只返回一个首 map/profile pair 的 ID 和 rowid；候选点查必须带 expected ID，不能复用论道的重复 pair 加权，也不能用无界 Python `seen` 集合。单 ID 读取而非 256 个 ID 批量，避免大标识字段按页放大。
3. 最多保留 10 份展示对象，按稳定数据下的唯一用户流公平 reservoir(k=10)；不足 10 按冻结 map pair 顺序，超过 10 再随机排列。采用双高水位、短只读 UoW、每个 ID 后协作让出和读取故障丢弃结果；明确并发非全局快照、首 pair 删除/更新和唯一性边界，不加共享缓存或请求期 DDL。
4. 默认至多复用 1 名只读代理审计去重/首行/公平性/查询峰值，代理禁止测试/导入/编译/恢复/数据库访问/生成缓存；主线程负责实现、串行验收、专用缓存清理与提交推送。继续 10 GiB/512 MiB 阈值，SQL 工作集、单字段大小、缺索引耗时及正式发布证据仍开放。

本片执行收口（2026-10-03）：`nearby_users_cmd` 改经 async feature application 与 BINARY ID seek query；一页只读一个 ID 与首 pair，候选以 expected ID 重查。稳定数据下最多保留 10 个唯一对象，不足 10 按 map rowid 顺序，超过 10 使用 reservoir(k=10) 并随机排列。地图/论道/progress 聚合 `326 passed`，附近展示专项 `83 passed`，最终 progress/inventory/architecture 单元 `33 passed`，source-quality 地图随机/附近定向 `2 passed`；五项展示 progress gate、inventory freshness、11 文件 AST 内存编译和 diff check 通过。完整 source-quality 为 `278 passed, 1 failed`，单个既有 BOSS handler source 断言仍查找旧 `boss_data=` 片段；本片未改该 handler，Boss JSON 用户改动保留。完整 architecture 仍 `15` 项错误，无新增展示错误，`exit_ready=false`。隔离五库 backup/restore dry-run/restore、265 项 migration/reconcile clean、路由 `203/58/7/1/1` 通过，非正式发布/P7。1 名只读代理指出数值游标下并发首 pair 删除可能重复计权；最终改为 ID seek，并复审无阻断。实时变化不是全局快照，唯一流公平只对稳定扫描成立；非首重复 profile 坏字段不再触发展示失败。pytest/pyc 关闭，所有进程退出后删除本轮 `/tmp/codex-map-nearby-display-20261003`（约 8.3 MiB），无仓库字节码；保留运行数据库/WAL/SHM、正式备份、持久回执、`.venv`、`.git`、`data/` 和用户 Boss JSON。收尾资源复核后提交并推送，再进入下一个真实默认旧写入口调用图审计。

### 冻结默认旧路径清单方案

1. 暂停按 player/economy 或 combat/dungeon/boss 顺序继续追加玩法切片；本期范围固定为 `docs/refactor_phase2_legacy_paths.json` 的 `scope_id`。它从已初始化 NoneBot 的默认生产加载根，枚举旧命令候选、旧 Web route、legacy scheduler job，并把细分到具体效果的已核实路径单独列出。
2. 每项状态只能是 `已迁移`、`允许保留的兼容路径`、`不可达`、`受阻`。迁移必须有默认入口到 feature application/repository 的调用边；兼容项必须写明授权周期/移除条件；不可达必须证明没有从生产根到达；动态目标或调用链未闭合一律受阻。各项附注册入口、调用图和源码证据，不以文件数、feature 名或 import/定义存在判定。
3. `scripts/phase2_legacy_path_gate.py --check` 对 `docs/refactor_phase2_legacy_path_items.json` 的冻结成员校验 membership hash，并按每项状态计算；有 `受阻`、非法状态、缺调用图证据、仍标为受阻的命令/路由失去对应源码注册、不可达命令重新可注册或冻结成员损坏均失败。活动 inventory 与基线的差异以 `source_inventory_added` 等字段报告 backlog 候选，不自动扩范围或改变冻结清单完成状态。新发现经人工核实后进入 `docs/refactor_phase2_legacy_paths.json` 的 `backlog`；只有显式评审、升级 `scope_id` 并重算成员 hash 后才能纳入本期。成员变更后用 `scripts/phase2_legacy_path_gate.py --membership-hash` 计算新 hash，再人工更新 scope manifest；此命令不写文件。旧的 slice 布尔值仅作诊断，不替代清单状态；P7 真实发布周期 gate 独立运行，不混入 phase 2。
4. 可以合理追加最多 2 名只读子代理，按互不重叠的问题分工核对调用可达性、迁移/回执契约和门禁测试覆盖。子代理不改代码、不运行测试/导入/编译/恢复、不访问运行数据库或凭据、不制造缓存；主线程负责实现、测试、整合、资源复核与清理，测试/恢复串行。
5. 每项保留用户/持久状态边界；缓存只清理本轮确认归属、相关进程已退出的临时产物，不清理 operation ledger、outbox、compatibility hits、数据库/WAL/SHM、备份或用户文件。继续遵守磁盘 10 GiB / `MemAvailable` 512 MiB 重任务停止阈值和 pytest/pyc 隔离。

## 缓存清理允许范围

- 确认属于本轮、相关进程已退出的未跟踪 `__pycache__/`、`*.pyc`、`.pytest_cache/`；不按目录名盲删未知缓存。
- 当前切片专用的 `/tmp/<slice>-pytest*`、`/tmp/<slice>-*` receipt 和日志目录。
- 已确认属于本轮测试的旧临时 smoke 目录。

以下目录默认保留：

- `/home/nonebot_plugin_xiuxian_2_pmv/.venv`
- `/home/nonebot_plugin_xiuxian_2_pmv/.git`
- `/home/nonebot_plugin_xiuxian_2_pmv/data`
- `.env`、配置文件、数据库、备份和任何运行态目录
- 无法确认所有权或用途的 `/tmp` 内容

## 最近完成切片

`activity gameplay state game-db cutover`：默认活动玩法状态表改由 `game_db` 承载；新增 `activity_state.001/.002` 启动迁移预建 schema，并从旧 `activity.db` 只读、每批最多 200 行回填，冲突、旧 schema 不完整或磁盘空间不足时拒绝继续。移除 service 导入期 schema 校验，使 startup migration 能先于玩法调用执行。活动 Web 数据管理指向 `game_db`；旧文件保留配置事件数据及备份用途，不删除，也不描述为整库只读。活动日志和领取状态参与上限/资格/审计，不得作为缓存清理。空白隔离数据目录下迁移/活动行为/progress 回归 `41 passed`；activity progress 指标全绿，整体 `exit_ready=false`；inventory freshness、目标 Python 文件 compileall、diff check 通过。专属测试、字节码和 inventory 对照产物在收尾清理，未访问运行数据库。

`activity boss milestone child reward ledger cutover`：默认入口和兼容 facade 改经 feature application/repository；game DB 原子准备 operation ledger、feature receipt 和 reservation，另一个事务原子发放资产并标记 `granted`，随后幂等确认旧活动 projection。首次请求固定可领子集，显式 child ID 恢复原快照，普通请求用新 ID 支持后续解锁与失败后新尝试。game-only `.008/.009` 预建 schema 并分块回填历史回执，pass `.007` 回填保持独立；CLI 注册 pass/milestone reconcile。聚焦及架构/库存合同 `97 passed`，architecture CLI `ok=true`，隔离五库恢复覆盖 207 项迁移并确认 milestone 仅路由 game DB。Rank 及 legacy activity state 尚未迁移，真实发布/P7 证据仍缺；测试串行执行，专用临时产物在收尾删除，不清理运行数据或幂等账本。

`work item-use compatibility isolation`：`WorkItemUseService/Result` 已从 `xiuxian_work/transaction_service.py` 移至 `compatibility/legacy_work_item_use.py`，历史 transaction import 保持对象身份 re-export；默认 `20014/20015` matcher 继续由 `WorkItemUseApplication -> WorkItemUseSqlRepository` 承担，旧 service 不在默认执行图中。进度门禁与 source-quality 新增 compatibility isolation 断言，物品 service 行为测试覆盖加速、捕获、幂等冲突和晚失败回滚。测试/compileall/recovery 只使用专用临时目录并在收尾清理 pytest、pyc 和字节码缓存，未触碰 `.venv`、`.git`、运行数据库或用户 `boss_info.json`。下一片审计 daily refresh reset 的旧 service 可达性与请求期 schema；全局旧 transaction services、`xiuxian2_handle`、真实发布迁移/P7 仍开放。

`work daily-reset compatibility isolation`：`WorkDailyRefreshResetService/Result` 已从 `xiuxian_work/transaction_service.py` 移至 `compatibility/legacy_work_daily_refresh_reset.py`，历史 transaction import 保持对象身份 re-export；默认 scheduler 继续由 `WorkDailyRefreshResetApplication -> WorkDailyRefreshResetRepository` 使用 `work.002` 启动 schema，旧 service 不在默认执行图中。新增 progress/source-quality/identity 门禁，daily reset 行为回归覆盖分块、删除用户、冲突、重放和晚失败回滚。测试/compileall 只使用专用临时目录并在收尾清理 pytest、pyc 与字节码缓存，未触碰 `.venv`、`.git`、运行数据库或用户 `boss_info.json`。下一片审计仍可达的 `xiuxian2_handle`/legacy transaction 调用链，避免重复迁移已完成 work 边界；全局旧 transaction services、`xiuxian2_handle`、真实发布迁移/P7 仍开放。

`work settlement transaction ownership`：悬赏结算默认入口已由 `WorkSettlementApplication -> WorkSettlementSqlRepository` 承担；`work.008` 启动迁移预建 operation 表并补齐历史 `result_json`，请求路径只读校验 schema。仓储在单一 game-db `BEGIN IMMEDIATE` 中校验 operation payload、冷却快照和用户，按真实奖励字段写背包，处理背包上限，原子更新修为、背包和 `user_cd`，并保存成功类型/消息/任务名供 replay；晚期 SQL 异常回滚，旧 `WorkSettlementService` 移至 compatibility-only 模块并保留历史 import identity。结算 repository/application/source-quality/progress 聚焦回归 `76 passed`、source-quality `8 passed`、progress `1 passed`；隔离 architecture、inventory、全量 migration recovery、reconcile 与 diff check 通过。测试与恢复只使用专用临时目录，pytest/pyc/compile 缓存已清理，未触碰 `.venv`、`.git`、运行数据库或用户 `boss_info.json`。下一片审计 `WorkItemUseService` 请求期 DDL 与 legacy service 可达性；全局旧 transaction services、`xiuxian2_handle`、真实发布迁移/P7 仍是未完成 blocker。

`demon wave refresh application cutover`：定时 wave refresh 改经
`DemonWaveRefreshApplication -> DemonWaveRefreshSqlRepository`；共享事件状态 codec/read/write 从 lifecycle repository
抽至 feature-owned `event_state.py`。将旧 `DemonWaveRefreshResult/Service` 移至
`compatibility/legacy_demon_wave_refresh.py`，原 transaction module 保持对象身份 re-export。新增 player-only
`world_events.005` operation migration，默认请求不建表。World Events wave/lifecycle/claim/attack/Web、source-quality、
progress 与 architecture 聚焦回归 `69 passed`；194 项 migration 的隔离 recovery、五库/config backup/restore、
五库 dry-run 无 pending、readiness 六项全绿、reconcile clean 均通过。缓存和临时数据已回收；Spirit Vein lifecycle
及 live migration/P7、全局 legacy blockers 仍待处理。

`demon event lifecycle application cutover`：真实自动/手动开始与结束路径改经
`DemonEventLifecycleApplication -> DemonEventLifecycleSqlRepository`，从旧 transaction service 移除实现并移入
`compatibility/legacy_demon_event_lifecycle.py`，原模块保持对象身份 re-export。新增 player-only
`world_events.004` 预建 operation 表，请求路径不建表；补齐手动结束 replay 分支回归。World Events、source-quality、
progress 与 architecture 聚焦套件 `64 passed`；五库 recovery 覆盖 193 项 migration，backup/restore 含 config 成功，
五库 migration dry-run 无 pending，readiness 六项全绿，reconcile clean（operations/outbox/dead events 均为 0）。
临时数据、receipt 与字节码自动回收；wave refresh、spirit vein、live migration/P7 和全局 legacy blockers 仍开放。

`mixelixir two-phase refine claim`：真实 `配方` handler 的扣材、补领查询和奖励领取统一经
`MixelixirApplication -> MixelixirRefineCostSqlRepository/MixelixirRefineRewardSqlRepository`。扣材时校验
每日次数、材料和丹炉并保存完整奖励及炼丹状态快照；领取时 CAS 校验修为状态和背包容量，在 attached UoW
中发放丹药、更新 `mix_elixir_info`、增加炼丹统计、完成任务并写入幂等结果。新增 game DB `mixelixir.002`
和 player DB `mixelixir.003`，请求路径不再建表；旧 transaction services 留作显式兼容对照。炼丹 feature/兼容/source
回归（含真实注册 matcher 端到端）`26 passed, 2 subtests`，compileall、architecture、inventory、progress 和 diff
check 通过。五库 recovery 完成 `158` 项迁移，路由命中数 `126/28/7/1/1`；backup/restore dry-run/restore、health
六项 readiness、migration dry-run（五库 pending 为空）、reconcile clean（operations/outbox/dead events 均为 `0`）。
专用 basetemp、recovery 数据、receipt 和字节码缓存已清理。

`work item accelerate`：真实注册 `道具使用` matcher 的 `20014` 分派改经
`WorkItemUseApplication -> WorkItemUseSqlRepository`；以库存数量、绑定数量和 `user_cd` 快照作 CAS，
事务内扣除一个悬赏令、把开始时间设为立即可结算并写入幂等操作结果。新增 game DB `work.003`，
加速请求路径不再建表。追捕令 `20015` 的随机 offer 生成和快照仍通过 `WorkItemUseService.capture`，
明确作为独立兼容边界。application/旧 service/source/registered matcher 聚焦套件 `256 passed`，compileall、
architecture、progress、inventory 与 `git diff --check` 通过。隔离五库 recovery 完成 `159` 项 migration，
路由计数 `127/28/7/1/1`；backup/restore dry-run/restore、五库 migration dry-run（pending 为空）、health 六项
readiness 全绿、reconcile clean（operations/outbox/dead events 均为 `0`）。专用 pytest basetemp、恢复数据、receipt
和 compileall 字节码缓存已清理；`.venv`、`.git`、`data/` 和运行数据保留。

`buff partner cultivation`：`xiuxian_buff.partner::direct_two_exp` 默认结算改为调用
`PartnerCultivationApplication -> PartnerCultivationSqlRepository`；修为/属性、双修次数、统计、亲密度、邀请接受与 operation ledger 在 attached UoW 内提交。新增 game DB `buff.004` 和 player DB `buff.005`，次数读取保留旧 JSON 一次性导入但不再请求时建表；非邀请 operation payload 与旧 ledger 保持兼容。聚焦回归覆盖回放、冲突、过期/保护、超大数、快照拒绝和跨库异常回滚；旧 `PartnerCultivationService` 仅留作兼容对照。
focused/source/progress/architecture/inventory 回归 `255 passed, 4 subtests`；compileall、architecture、inventory、progress 和 diff check 均通过。五库 recovery 完成 `157` 项迁移，`.004` 仅 game、`.005` 仅 player；backup/restore dry-run/restore、migration dry-run（五库 pending 为空）、health 六项和 reconcile clean 均通过，operations/outbox/dead events 为 `0`。根目录全量测试已启动但未完成：`1950 passed` 后有 5 个与本切片无关的 blessed-flag legacy service `TypeError`，并停在 legacy sign-in startup 测试；该进程已中断。指定 basetemp、recovery 数据、receipt 和源码字节码缓存均已清理。

`buff partner-token`：`道具使用 双修令牌` 默认 handler 调用
`PartnerTokenUseApplication -> PartnerTokenUseSqlRepository`；`buff.002` 在 game DB 创建 operation 表，
`buff.003` 在 player DB 创建次数 projection，跨库写入使用 attached UoW，不在请求时建表。旧
`PartnerTokenUseService` 保留作兼容对照，但不再被默认 handler 调用。聚焦测试 `10 passed`，五库 recovery
`154` 项、readiness 六项全绿；恢复数据、receipt 和缓存均已清理。

`work item capture`：真实注册 `道具使用` matcher 的 `20015` 追捕令扣除和 offer 持久化改经
`WorkItemUseApplication -> WorkItemUseSqlRepository`；随机 offer 与首次奖励倍率写入 game DB 并与背包扣减、幂等结果同事务提交，
同 operation 重放返回首个 offer 和倍率。新增 `work.004`，以 `CREATE TABLE IF NOT EXISTS` 建立兼容 snapshot 表并保留已有行；
成功后旧 JSON 只作展示投影，旧 service 留作兼容对照。聚焦 application/旧 service/真实注册 matcher/source/progress 测试
`235 passed`；compileall、architecture、inventory、progress 和 diff check 通过。隔离五库 recovery 完成 `160` 项 migration，
路由 `128/28/7/1/1`，`work.004` 仅 game DB；backup/restore dry-run/restore、五库 migration dry-run（pending 为空）、
health 六项全绿和 reconcile clean 均通过，operations/outbox/dead events 为 `0`。本轮临时测试、recovery、receipt 和字节码缓存
清理并复核后，下一步只做 6.2 目标 5 的单项真实入口审计。

`rift demon-token battle`：真实注册的 `道具使用` matcher 对 `20018` 的 replay/结算改走
`RiftApplication -> RiftDemonTokenBattleSqlRepository`；outcome 保持 handler 预滚，repository 在 attached game/player UoW
内提交道具、战斗资产、奖励、探索次数、统计与秘境状态。旧 operation payload 和 replay 兼容；新增 game `rift.002` 与 player
`rift.003` schema migration，请求路径不建表/补列。application、旧 service 互操作、真实 matcher、裂隙相邻/source/progress 回归
`71 passed`；compileall、architecture、inventory、progress 和 diff check 通过。五库 recovery 完成 `162` 项迁移，路由
`129/29/7/1/1`；backup/restore dry-run/restore、migration dry-run（pending 为空）、health 六项和 reconcile clean 均通过。
专用测试、recovery、receipt 和字节码缓存已清理，资源复核后仍有约 `24 GB` 磁盘和 `1.4 GB` 可用 RAM。

`impart prayer`：`20005` 真实 handler 在 adapter 预滚卡片后调用
`ImpartApplication.prayer_settle -> ImpartPrayerSqlRepository`，在 attached game/impart/player UoW 中提交道具、卡片、加成、统计和 replay；
game `impart.002`、player `impart.003` 启动迁移准备 schema，请求不建表。聚焦回归 `39 passed`；五库 recovery `169` 项，路由
`130/30/7/1/1`，reconcile clean。未做真实 live/P7 或根目录全量回归，专属 recovery 数据与 receipt 已清理。

`arena challenge-ticket legacy service removal`：真实挑战券 matcher 已使用
`ArenaApplication.use_challenge_ticket -> ArenaChallengePurchaseSqlRepository`；移除只剩旧测试和被 SQL 子类覆盖的
`LegacyArenaRepository` 回退调用的 `ArenaChallengeTicketService`，保留 adapter 结果 DTO。`arena.004` game DB 启动迁移不变，
请求不执行 DDL。matcher/application/repository/事务聚焦回归 `11 passed`；compileall、architecture、progress、inventory 和
diff check 通过。五库 recovery 完成 `164` 项 migration，路由 `130/30/7/1/1`，backup/restore dry-run/restore 与 reconcile clean。
本切片未做 live/P7 或根目录全量回归；专用测试、恢复数据、receipt 和字节码缓存已清理，保留运行数据与 Boss JSON 用户改动。

`impart love-sand request-schema boundary`：`20016` handler 继续预滚奖励，repository 移除请求期 DDL；新增 game
`impart.004` replay table 与 player `impart.005` statistics columns startup migrations，缺 migration 返回 `schema_missing`，
已有 replay/统计数据保留。focused `22 passed`（1 条既有 compatibility warning）；五库 recovery `166` 项、路由
`131/31/7/1/1`，`.004` game-only、`.005` player-only，reconcile clean；没有运行根目录全量测试或真实 live/P7。
恢复数据、receipt、pytest basetemp 和字节码缓存已清理。

`rift speedup default-repository and startup-schema boundary`：默认 `RiftApplication.speedup` 改走
`RiftSpeedupSqlRepository`，其他 Rift actions 仍使用 legacy compatibility repository。新增 game-only `rift.004`，
迁移创建/补齐 replay 表；repository 缺迁移时返回 `schema_missing`，请求路径不建表/补列，旧 compact/rich payload 均兼容。
默认 composition、缺迁移拒绝、compact/rich 旧 payload replay、绑定数量不变量、无绑定列兼容、事务回滚和路由聚焦测试通过；未运行根目录全量回归或真实 live/P7。

`rift termination`：真实 `秘境终止` handler 的 replay/终止结算改走 `RiftApplication -> RiftTerminationSqlRepository`；repository 在
game DB 单事务内校验 active entry、秘境快照与 cooldown，原子终止 entry、释放 cooldown 并保留旧 `user_id + rift_plan` replay payload。
新增 game-only `rift.006`，缺 migration 返回 `schema_missing`，请求/replay 路径不执行 DDL；重复请求回放首次结果，用户/快照冲突、未激活和晚期 SQL
异常均不改变资产或状态，旧 `RiftTerminationService` 不再位于真实 facade 路径。Rift 聚焦回归 `98 passed`，compileall、architecture、progress、
inventory、diff check 和五库 recovery 全部通过；recovery 共 `169` 项 migration，路由 `134/31/7/1/1`，`rift.006` 仅 game，reconcile clean。
专用测试/recovery/receipt/字节码缓存已清理，约 `24 GB` 磁盘可用、`1.3 GB` RAM 可用；`.venv`、`.git`、`data/`、运行数据库/备份及 Boss JSON 保留。

`rift key-event`：真实 `秘境钥匙` handler 的 replay/事件结算改走 `RiftApplication -> RiftKeyEventSqlRepository`；attached game/player UoW
原子校验并扣除钥匙、提交预滚事件和奖励、更新探索次数/统计、结束 entry 与 cooldown，沿用旧 operation payload。新增 game-only `rift.007`，
缺迁移或 player schema 返回 `schema_missing`，请求/replay 不执行 DDL；重复、快照冲突、库存不足和晚期 SQL 失败均保持幂等/回滚语义。
聚焦回归 `100 passed`、compileall、architecture、progress、inventory、diff check 和五库 recovery 全部通过；recovery 共 `170` 项 migration，
路由 `135/31/7/1/1`，`rift.007` 仅 game，reconcile clean。专用测试/recovery/receipt/字节码缓存已清理，约 `24 GB` 磁盘可用、
`1.2 GB` RAM 可用；`.venv`、`.git`、`data/`、运行数据库/备份及 Boss JSON 保留。下一片为 `秘境结算`。

`rift ordinary settlement`：真实 `秘境结算` handler 的 replay/普通事件结算改走 `RiftApplication -> RiftSettlementSqlRepository`；repository 注入
Clock，在 attached game/player UoW 内校验时间窗口、资源/快照/探索次数并原子提交奖励、统计、entry、cooldown 与旧 payload。新增 game-only `rift.008`，
启动时补历史 operation 表的 `message` 列；请求/replay 不执行 DDL。聚焦回归 `104 passed`，并完成 compileall、architecture、progress、inventory、
diff check 与五库 recovery；recovery 共 `171` 项 migration，路由 `136/31/7/1/1`，`rift.008` 仅 game，reconcile clean。专用测试/recovery/receipt/
字节码缓存已清理，约 `24 GB` 磁盘可用、`1.3 GB` RAM 可用；下一步审计剩余 Rift entry/兼容只读边界。

`rift entry`：真实普通进入和秘藏令进入改走 `RiftApplication -> RiftEntrySqlRepository`；game DB 单事务校验 world generation/revision、参与者、
体力、秘藏令和 cooldown，更新 entry/world participants/revision/count 并写入旧 replay payload。新增 game-only `rift.009`，启动时预建/补齐 entry
schema，请求路径不建表/补列；重复、world 冲突、资源不足和晚期 SQL 失败均保持幂等/回滚。聚焦回归 `109 passed`，完成 compileall、architecture、
progress、inventory、diff check 与五库 recovery；recovery 共 `172` 项 migration，路由 `137/31/7/1/1`，`rift.009` 仅 game，reconcile clean。专用
测试/recovery/receipt/字节码缓存已清理，约 `24 GB` 磁盘可用、`1.3 GB` RAM 可用；下一步审计 Rift 剩余兼容只读边界。

`rift active-entry read projection`：`jsondata.read_rift_data` 改经 `RiftEntrySqlRepository.read_entry` 的 read-only UoW；表/历史列缺失不执行 DDL，
仅在没有 active entry 时回退玩家 JSON。覆盖 active/inactive、缺表/缺列、对象校验和旧 fallback；Rift 聚焦 `113 passed`，compileall、architecture、progress、
inventory、diff check 与五库 recovery 均通过，recovery `172` 项、路由 `137/31/7/1/1`、reconcile clean。专用产物已清理；下一步审计 Rift cooldown 只读边界。

`rift cooldown read projection`：真实 `秘境结算` handler 的 cooldown 读取改经 `RiftApplication.read_cooldown -> RiftCooldownSqlRepository` read-only UoW；
缺表/缺列不插入用户、不执行 DDL，移除 facade 的 `XiuxianDateManage.get_user_cd`。cooldown/Rift/source/progress 聚焦 `118 passed`，compileall、
architecture、inventory、diff check 与五库 recovery 均通过；无新增 migration，recovery 仍 `172` 项、路由 `137/31/7/1/1`、reconcile clean。专用产物已清理；
下一步审计 Rift 残留 speedup legacy getter/只读投影边界。

`rift speedup legacy-construction cleanup`：真实 facade 移除未调用的 `RiftSpeedupService` import、instance 和 lazy getter；handler 继续经
`RiftApplication.speedup -> RiftSpeedupSqlRepository`，显式 compatibility repository 保留。speedup/Rift/source/progress 聚焦 `118 passed`，
compileall、architecture、inventory、diff check 与五库 recovery 通过；无新增 migration，recovery `172` 项、路由 `137/31/7/1/1`、reconcile clean。
专用产物已清理；下一步审计 Rift 显式 compatibility repository 与故事/资产只读边界。

## 当前切片与下一切片选择

本轮完成 `back skill-confirmation cache bound`：技能确认缓存设为 30 秒 monotonic TTL、最多 2048 条，只保存标量 DTO；读写会清除过期前缀，并由单个共享 expirer task 主动清理，不再为每张邀请创建持有 `bot/event` 的 sleeper task。确认入口检查过期状态；消费按 invite ID 条件删除，保留 operation ID 和新旧票据隔离语义。cache/skill-learning/source 回归 `16 passed`，目标源码内存编译、inventory freshness 与 diff check 通过，无 migration/运行数据库访问。1 名子代理只读审计调用点和风险，未改代码、跑测试或访问数据；修改与测试由主线程串行完成。pytest cacheprovider/pyc 禁用，专用 basetemp 清理；收尾磁盘约 `19G`、RAM available `1.3GiB`。保留用户 `boss_info.json`；下一片继续审计 player/economy 真实默认旧写入口，整体 `exit_ready=false`。

本轮完成 `player combat vital write boundary`：战斗结束后的 HP/MP 写回统一使用既有
`PlayerStateApplication -> PlayerStateRepository.update_vitals`，不再从 `schema_missing` 回退
到 `XiuxianDateManage.update_user_hp_mp`，同时移除战斗辅助模块内的旧 SQL manager 缓存。
按首条 `rowid` 更新；异常状态给出不含用户标识的告警，不存在的数据库不会被写回操作创建。
调用没有提供战前 HP/MP 快照，因此不做快照 CAS。覆盖调用委托、隔离 SQLite 实际写回、缺库不建库、source/progress 门禁；无 migration。
验收后清理本片 pytest/pyc/临时目录。下一片按 6.2 继续审计洞府背包和补偿回写。

本轮完成 `player activity timestamp read/write ownership`：普通 work、impart、world-events、info、beg、buff 和 Boss facade 的 `user_cd.last_check_info_time` 读写统一经 `PlayerActivityApplication -> PlayerActivitySqlRepository`，运行时注入 `Clock`。仓储只更新既有用户行；缺数据库/表/列/用户返回空结果，不创建数据库、表或执行请求期 DDL，并保留旧本地无时区字符串格式。Sect 闲置判定仍由 `SectActivitySqlRepository` 负责，Boss 旧结算 transaction 内写入仍和战斗 CAS 共事务。新增 activity 行为/source 回归；无 migration、无运行数据访问。验收后只删除仓库 `__pycache__`、`*.pyc`、`.pytest_cache` 和本轮临时目录，保留 `.venv`、`.git`、`data/`、数据库/WAL/SHM、持久回执及用户修改。

本轮完成 `normal pvp settlement ownership`：普通切磋默认回放和结算改经 `BuffApplication -> NormalPvpSqlRepository`；`buff.006` 在 game DB 预建 `normal_pvp_operations`，`buff.007` 在 player DB 预建 `statistics` 的切磋胜负列。attached UoW 内完成双方 HP/MP/体力 CAS、统计与回执，重复 operation 回放、payload 冲突、缺 schema/用户和快照冲突均 fail closed；请求路径不执行 DDL。`pvp_battle` 只保留旧战斗引擎的纯计算适配，旧 `NormalPvpSettlementService` 仅作显式兼容 API，不在默认 handler 执行图中。聚焦回归 `8 passed`，仓储独立 unittest `2 tests OK`；compileall、progress gates 与 diff check 通过。测试/编译缓存验收后清理，保留用户 `boss_info.json`。下一片按 6.2 审计 `get_user_real_info`、体力/经济写入及仍可达的 `xiuxian2_handle`/legacy transaction 路径。

本轮完成 `partner identity read ownership`：双修与师徒默认 handler 中仅用于姓名、境界、修为和存在性判断的 `get_user_real_info` 全部改经 `get_user_profile -> PlayerProfileApplication -> PlayerProfileSqlRepository` 只读查询；资产结算、动态功法/装备属性和旧 transaction compatibility 未改。profile 缺 schema 时仍 fail closed，不执行请求期 DDL。partner/profile/source/progress 聚焦回归 `18 passed`，无 migration、无运行数据访问；专用测试/字节码缓存验收后清理。动态战斗属性仍由 `get_final_attributes` 及旧 provider 提供，下一片继续审计该 provider 与体力/经济写入默认路径。

本轮完成 `cooldown stamina ownership`：通用 `Cooldown(stamina_cost>0)` 的扣除改经 `PlayerStaminaApplication -> PlayerStaminaSqlRepository`，按 profile 首行 `rowid` 在既有 `user_xiuxian` 上执行 expected snapshot/CAS；重复 user_id、体力不足、用户缺失和 schema 缺失均 fail closed，请求路径不执行 DDL。运行时由 plugin 注入 application，定时体力恢复任务仍保留旧批量边界，作为下一片独立迁移。聚焦回归 `5 passed`，layout/profile/source/progress、compileall 和 diff check 通过；临时测试/字节码缓存已清理。下一片迁移恢复任务并继续审计动态属性 provider、经济写入和 `xiuxian2_handle` 旧路径。

管理员资产队列：`.005/.006` 已分别为 item-destroy/item-grant 预建 game DB 回执；`.007` 完成境界/灵根回执 schema；`.008` 预建单人传承石回执；`.009` 预建单人饰品回执；`.010` 预建全服饰品批次；`.011` 预建全服传承石批次；`.012` 预建普通全服物品批次。传承石真实余额仍在 legacy `impart_db.xiuxian_impart`，饰品 bag 仍归 player-side `player_accessory` 表；feature 仓储只校验既有资产 schema，不在请求期建表或补列，game DB 保存兼容操作回执、批次进度和经济审计。管理员单人资产、全服饰品、全服传承石和全服普通物品命令均走 feature application，缺 DB/schema fail closed；其他管理员边界仍待迁移。

本片验收：admin asset repositories/application/source/progress `33 passed`，architecture/inventory contracts `17 passed`；progress item-destroy/item-grant no-DDL/startup-schema/game-only 门禁均为 true。五库 recovery backup/restore dry-run/restore 成功，`.006` 仅 game DB applied，reconcile clean；隔离 architecture CLI `ok=true`，compileall 与 diff check 通过。item-destroy 前片专属产物已清理；本片 pytest、recovery、architecture 和 pycache 临时目录在本次验收后清理并复核。用户 `boss_info.json` 改动保留。

本片验收：管理员资产 repositories/application/source/progress 合并回归 `78 passed`，覆盖 `.008` game-only 路由、已有回执兼容、实际 impart 余额变化、`None` 缺行快照、CAS、重复用户行、无请求期 DDL 和跨库晚失败回滚；progress impart gates 全绿，inventory freshness、NoneBot 初始化后的 architecture CLI、compileall 和 diff check 通过。五库 recovery backup/restore dry-run/restore 成功，全部 migration applied 且 pending 为空，`.008` 仅路由 game DB、`impart_db` 不增加迁移，reconcile clean。专用测试、recovery、architecture 与字节码产物在验收后清理，保留用户 `boss_info.json` 修改。下一项审计管理员饰品单人调整默认路径仍调用的 legacy transaction service 及其请求期 schema；全服传承石批次仍是独立兼容边界。

上一片完成：单人饰品默认路径改由 `AdminAssetApplication -> AdminAccessorySqlRepository` 承担；game-only `admin_asset.009` 预建兼容 operation receipt，player-side 饰品表沿用既有启动迁移。保留旧回执回放、容量/品质/UID 校验、CAS、部分扣除和经济审计；请求路径不建表，缺 schema fail closed。

本片范围：全服饰品 grant/destroy 从旧 transaction service 切到 `AdminAssetApplication -> AdminAccessoryBatchSqlRepository`；新增 game-only `admin_asset.010` 预建批次、legacy progress 和规范化 targets schema。新任务不将全量用户列表塞入 payload，冻结名单和进度分离并限制单块读取；旧内嵌名单及 progress 在恢复时兼容导入。child receipt 幂等恢复、双数据库磁盘空间预检和 production command ownership 有回归覆盖。全服传承石、普通全服物品和其他管理员边界不在本片范围。测试使用 `PYTHONDONTWRITEBYTECODE=1` 与 `-p no:cacheprovider`；不得清理持久进度/receipt、运行数据库/WAL/SHM、备份、`.venv` 或 `.git`。WAL 下 attached 多库崩溃原子性和正式发布 P7 恢复仍为开放风险。

最近完成 `admin global impart-stone batch cutover`：`传承力量 <数量> all` 改走 `AdminAssetApplication -> AdminImpartStoneBatchSqlRepository`，新增 game-only `admin_asset.011` 预建批次 operation、legacy progress 和规范化 targets。新任务在 game DB 内用 `INSERT ... SELECT` 冻结玩家名单，不再由命令层读取/复制完整 ID 列表；每次最多加载 100 个目标，impart 余额仍由单人 feature repository 经 child receipt 更新。旧运行 payload 及已完成 progress 可恢复导入，旧完成任务保留原摘要；旧名单以流式解析、每 500 条写入，超 64 Mi 字符则 fail closed 并保留历史进度；child receipt 已提交但批次进度写入失败时可幂等重放。建批前按 game/impart 所在磁盘可用空间预检，空间不足不创建 operation；缺 migration/schema 不触发请求期 DDL。回归覆盖冻结名单、分块与重放、低磁盘、缺 schema、旧任务恢复、过大 payload 拒绝、进度故障和 migration 幂等。全服普通物品批次是下一片。测试/pyc/pytest cache 使用禁用或专用临时目录并在验收后清理；不得清理持久 receipt/progress、运行 DB/WAL/SHM、备份、`.venv`、`.git` 或用户工作树改动。跨库 WAL 崩溃原子性与正式发布 P7 仍为开放风险。

最近完成 `admin global item batch cutover`：普通物品 `创造力量 all`/`毁灭力量 all` 改走 `AdminAssetApplication -> AdminItemBatchSqlRepository`，新增 game-only `.012` 批次、旧 grant progress 与规范化 targets schema；饰品分支保持不变。名单在 game DB 冻结，handler 不读取完整 user ID 列表，每次至多加载 100 个目标。旧 grant payload 按固定前缀识别、流式解析并按 500 条导入，已完成 progress 用数据库内 set-based 更新；名单数错误、64 Mi 字符上限或空间预检失败时不执行用户调整并保留原进度。child receipt 已提交但批次进度失败时可幂等恢复；grant 保留容量限制，destroy 保留按实际持有量部分扣除及 economy log。旧 `AdminApplication` grant wrapper 与 facade lazy getter 已移除，compatibility service 仍保留恢复用途。operation receipts/targets/progress 属于持久业务状态，不能当缓存清理；测试缓存和本轮临时库只在本轮专用目录内清理。SQLite 单库日志原子性已覆盖；全局 legacy service、`xiuxian2_handle` 和真实发布 P7 仍开放。

当前执行基线（2026-09-30）：活动 `tasks`、`pass`、`boss_milestone`、`boss_rank` 子领奖由 game DB feature repositories、幂等账本和 reservations 管理奖励；`activity_state.001/.002` 已将玩法状态 schema 与旧数据回填到 `game_db`。旧 `activity.db` 仍保留配置事件数据和备份用途；不得删除，也不能把其中参与上限、资格和审计的历史日志当缓存。bank 历史账户生命周期审计已收口：`bank.003` 是唯一 startup legacy projection 回填，默认读写不再访问旧账户；兼容 writer/rollback 保留但生产不可达，自动结息 jobs 为空。下一片按 progress 6.2 审计仍可达的 `xiuxian2_handle`/legacy transaction 路径。历史 ATTACH 版本的跨库崩溃仍需真实数据备份/P7 资产核验；正式发布 recovery/reconcile 与全局 legacy blockers 仍未完成。

最近完成 `work abort cleanup application cutover`：终止、未接/过期悬赏清理与重置改经 `WorkAbortCleanupApplication -> WorkAbortCleanupSqlRepository`；旧 service 移至 compatibility 模块并保留 transaction API re-export。game-only `work.006` 预建 active snapshot 与 cleanup ledger，缺 migration 时拒绝写入且不在请求期建表。聚焦回归 `68 passed`、source-quality `5 passed`、progress `1 passed`；compileall、inventory、progress、隔离 architecture (`ok=true`) 与 diff check 通过。五库 recovery 覆盖 197 项 migration，全部 applied、pending 为空，`work.006` 仅路由 game DB，reconcile clean。pytest、字节码、recovery 和 architecture 临时产物收尾时清理，未触碰仓库 `data/`、运行数据库或用户 `boss_info.json`。下一片迁移 `WorkClaimSqlRepository` 的 claim/active-snapshot 请求期 schema 到启动 migration；settlement schema、全局 legacy transaction services、`xiuxian2_handle` 与真实发布迁移/P7 仍开放。

最近完成 `work claim startup-schema boundary`：默认接取继续经 `WorkClaimApplication -> WorkClaimSqlRepository`，请求路径不再创建 claim operation/active snapshot 表；新增 game-only `work.007` 启动迁移并保留历史回执，缺 schema 返回 `schema_missing`。聚焦回归 `71 passed`、source-quality `6 passed`、progress `1 passed`；compileall、inventory、progress、隔离 architecture (`ok=true`) 与 diff check 通过。五库 recovery 覆盖 198 项 migration，全部 applied、pending 为空，`work.007` 仅路由 game DB，backup/restore 与 reconcile clean。测试、恢复和字节码临时产物收尾时清理，未触碰仓库 `data/`、运行数据库或用户 `boss_info.json`。下一片审计 `WorkSettlementSqlRepository` 的请求期 schema/历史列补齐；settlement 事务 ownership、全局 legacy transaction services、`xiuxian2_handle` 和真实发布迁移/P7 仍开放。

最近完成 `work settlement startup-schema boundary`：新增 game-only `work.008` 预建结算回执表并补齐历史 `result_json` 列，request path 不再执行 DDL，缺 migration 返回 `schema_missing`。本片明确不把现有奖励字段映射/结果 DTO/完整结算事务标为完成。聚焦回归 `73 passed`、source-quality `7 passed`、progress `1 passed`；compileall、inventory、progress、隔离 architecture (`ok=true`) 与 diff check 通过。五库 recovery 覆盖 199 项 migration，全部 applied、pending 为空，`work.008` 仅路由 game DB，backup/restore 与 reconcile clean。测试、恢复和字节码临时产物收尾时清理，未触碰仓库 `data/`、运行数据库或用户 `boss_info.json`。下一片迁移结算奖励字段/结果 DTO 与事务 ownership；全局 legacy transaction services、`xiuxian2_handle` 和真实发布迁移/P7 仍开放。

前一切片完成 `work refresh settlement application cutover`：默认普通/强制刷新经 `WorkRefreshApplication -> WorkRefreshSqlRepository`，旧刷新 service 实现移入 compatibility 模块并保留 transaction API re-export；game-only `work.005` 在启动阶段预建刷新回执表，请求路径不建表。`reward_data_source` 缺 migration 时回退只读旧 JSON，数据库写入明确失败。聚焦回归 `61 passed`、source-quality `4 passed`、progress `1 passed`；compileall、inventory、progress、隔离 architecture (`ok=true`) 和 diff check 通过。五库 recovery 覆盖 196 项 migration，backup/restore 成功，pending 为空，`work.005` 仅路由 game DB，reconcile clean。一次未隔离 NoneBot 初始化曾更新 `data/xiuxian/compatibility_hits.json` 和 `data/xiuxian/xiuxian_impart.db-shm` 的时间戳，文件原样保留；隔离重跑的 architecture 检查通过。

最近完成 `pet transaction compatibility isolation`：宠物默认 handler 经 `PetApplication` 使用 feature SQL repositories；旧 `Pet*Service` 实现已移入
`compatibility/legacy_pet_transactions.py`，`xiuxian_pet/transaction_service.py` 只保留历史 API re-export，默认宠物 facade 不再导入未调用的旧 service/lazy getter。
`LegacyPetRepository` 仍保留显式回滚路径，宠物 JSON projection/read path 也未因此整体关闭。宠物/source/progress 聚焦回归 `310 passed`，排除一项已知无关的 Rift source assertion；
architecture、inventory、progress 与 diff check 均通过。五库隔离 recovery 的 173 项 migration、backup/restore 和 3 项 attached migration 均通过，reconcile clean；
临时数据与 receipt 已清理。本片无新增业务 migration。

最近完成 `generic task progress application cutover`：真实通用任务事件和状态读写经
`TaskProgressApplication -> TasksProgressRepository` 使用 player-db immediate UoW；`game_events`、work、buff、
impart 仍保留旧 task-data adapter 作为事件 DTO/任务映射边界，签到专属 projection 不变。新增 player-only
`tasks.001` 预建历史周期列和 operation 表，生产请求不做 DDL。聚焦任务进度、领奖、feature task 和签到 effects/wiring
回归 `26 passed`（1 条既有 ServicePort `DeprecationWarning`），compileall、architecture、progress、inventory、diff check
通过；隔离五库 backup/restore、迁移和 reconcile clean，恢复目录已清理。旧 service import 为兼容转发，但旧实现仍留在
`LegacyTaskProgressEventService`，不宣称其源码已隔离。真实发布应用和 P7 证据仍缺失。

`task reward claim startup-schema boundary` 已完成：新增 game-only `tasks.002`，启动迁移预建领奖 operation 表并补齐旧
`economy_log` 缺失列；player-only `tasks.001` 继续负责任务周期状态列，领奖生产请求不再创建/补充 schema。任务领奖、进度、
feature tasks 与签到 effects/wiring 聚焦回归 `29 passed`（1 条既有 ServicePort `DeprecationWarning`）；compileall、architecture、
progress、inventory freshness 和 `git diff --check` 通过。隔离 recovery 的五库 backup/restore dry-run/restore 均成功，
共覆盖 175 个不同 migration version，路由计数 `139/32/7/1/1`；`tasks.001` 仅路由到 player、`tasks.002` 仅路由到 game，
reconcile clean（operations/outbox/dead events 均为 `0`）。专用 pytest basetemp、恢复数据、receipt 和 compileall 字节码缓存
已清理；`.venv`、`.git`、`data/`、运行数据及 Boss JSON 用户修改保留。此切片只完成 schema 启动化，
`TasksRepository -> task_manager -> TaskRewardClaimService` 仍负责动态奖励快照、奖励库存、economy log、claimed 状态与
game/player attached transaction。下一片先审计 operation replay/conflict、外层 operation ledger 卡在 started 后的续跑，
并明确 WAL 环境下跨库崩溃原子性的真实保证，再迁移领奖事务 ownership；不可通过保留 facade 或单纯移动旧类把领奖标成完成。
之后继续按
6.2 逐项覆盖训练、洞府、地图、宗门、竞技场/副本、世界事件、Boss 和交易剩余 adapter；追捕令 `20015` 的随机 offer
仍由旧领域逻辑生成，不能把既有扣除/快照边界解释成 work 领域整体完成。
已切换的 partner cultivation、partner token、背包通用 item-use Web、宠物蛋、饰品礼包、炼丹两阶段领取、斩妖令、祈愿石、
挑战券事务、love-sand schema 边界、Rift speedup 和 Rift world-generation schema 边界不重复迁移。

`rift damage-event outcome`：真实 `_roll_rift_event` 的掉血事件改经
`RiftApplication.roll_damage_event -> RiftDamageEventResolver`；resolver 只接收显式 battle config、经验计算器、数字格式化器和 RandomSource，
返回可持久化的 delta/message DTO，不读取或写入用户状态。旧 `get_dxsj_info` 仅保留兼容适配，Boss/宝物随机与资产解析不在本片迁移。
damage domain/application/source 与 Rift 回归 `122 passed`，compileall、architecture、progress、inventory、diff check 均通过；无新增 migration。
下一片继续审计 Boss battle outcome 的战斗资产 provider，避免把 `Boss_fight` 或宝物 `Items` 读取误算成 feature-owned。

`rift Boss-battle outcome`：真实普通秘境事件和斩妖令事件改经
`RiftApplication.roll_boss_battle -> RiftBossBattleResolver`；resolver 接收显式 `Boss_fight` runner、Boss 配置、境界/经验 provider 与 RandomSource，
固定以 `type_in=0` 运行并只返回战斗结果及 delta/message outcome。旧 `get_boss_battle_info(persist=True)` 仅保留兼容适配和显式经验/灵石写入；
不迁移 Boss/宝物资产读取或 `Items` provider。Boss/domain/application/source 与 Rift 回归 `125 passed`，compileall、architecture、progress、inventory、
diff check 均通过；无新增 migration。下一片审计 Boss battle runner 的资产查询边界或宝物 resolver，继续保持旧战斗实现为显式 provider。

`rift treasure outcome`：真实普通秘境宝物事件改经
`RiftApplication.roll_treasure -> RiftTreasureResolver`；resolver 只编排随机类型、奖励消息和可结算 outcome，
`Items`、功法/装备查询保持显式兼容 provider，不把 provider 注入误算为底层资产迁移。旧 `get_treasure_info` 仅保留兼容适配；
本片不新增 migration，验收覆盖 treasure/domain/application/source 与 Rift 聚焦回归。

`rift Boss asset snapshot boundary`：`RiftBossBattleResolver` 通过显式
`RiftBossBattleAssetProvider` 预解析玩家战斗快照，再以 `player_data` 注入 `Boss_fight`；Boss 引擎对未注入快照的旧调用仍回退
`get_players_attributes`，因此 `Items`、功法/装备、宠物和本命法宝底层查询仍是兼容 provider，不能宣称资产迁移完成。Rift 聚焦回归 `130 passed`；
compileall、architecture、progress、inventory 和 diff check 通过；无新增 migration。下一步继续把该 provider 的单一资产查询边界拆成可验证的只读 provider，保持 Boss 引擎与结算事务解耦。

`rift Boss item lookup provider`：Rift 玩家战斗快照 provider 将 `Items.get_data_by_item_id` 作为显式
`item_provider` 传入 `get_players_attributes`；未注入时保留旧全局 Items fallback。该切片只拆出一个只读查询边界，
不改变 `get_final_attributes`、宠物、本命法宝或资产文件的兼容读取，也不新增 migration。

`rift Boss pet lookup provider`：Rift 玩家战斗快照 provider 同样显式注入
`get_user_pet_for_battle` 作为 `pet_provider`；未注入时保留旧宠物读取 fallback。该切片只拆出宠物只读查询边界，
不改变宠物数据模型、战斗状态写入或其他玩家资产读取。

`rift Boss final-attribute provider`：Rift 玩家战斗快照 provider 显式注入
`get_final_attributes` 作为 `attribute_provider`，并透传 `ratio` 与 `include_current=True`；未注入时保留旧属性计算 fallback。
该切片只拆出最终属性只读查询边界，其内部 buff/传承读取仍是兼容 provider，不新增 migration。

`task reward claim game/player saga`：默认领奖 matcher 改走
`TaskClaimApplication -> TaskClaimGameRepository/TaskClaimPlayerRepository`；player reservation、game item grant/checkpoint、player claim receipt
分阶段提交，复用同一 operation 可从中断阶段恢复。新增 game-only `tasks.003` 和 player-only `tasks.004`，请求不建表且不依赖 WAL 跨库 `ATTACH`。
静态任务定义、Items 查找和奖励快照仍由 `task_data` adapter 提供，旧 `TaskRewardClaimService` 保留为兼容对照。聚焦回归 `37 passed`
（1 条既有 ServicePort `DeprecationWarning`）；compileall、architecture、progress、inventory 和 diff check 通过。五库 recovery 覆盖 177 个
migration version，路由计数 `140/33/7/1/1`，`.003` 仅 game、`.004` 仅 player；backup/restore dry-run/restore、五库 migration dry-run
（pending 为空）、health 六项和 reconcile clean 均通过。专用 recovery 数据、receipt、pytest cache 与字节码缓存已清理；真实发布数据迁移和 P7 仍未完成。

`training administrator reset`：`重置历练` 默认后台分块路径切换至 feature-owned `TrainingResetSqlRepository`；新增 game-only `training.004`，首次 chunk 冻结用户集合，后续以同一 operation id 续跑 pending targets，保留 duplicate/conflict/skipped、previous state 和晚失败 rollback。请求路径只读已迁移 schema，普通 application ledger 不承载跨 chunk replay；兼容服务仅在缺少 player database 时保留。聚焦回归覆盖分块冻结、删除用户、重复 user id、空集合、旧 schema 补列、缺 schema 不建表、晚失败 rollback、Clock 注入和真实入口 source contract。恢复演练应验证 `training.001/.003/.004` 只进入 game_db、`training.002` 只进入 player_db，完成后清理 basetemp、字节码、pytest cache、receipt 和临时数据库，并复核 `df -h`/`free -h`。真实发布数据迁移/P7 仍需单独记录。

`sect weekly reward application cutover`：默认周常领取经 `SectApplication.claim_weekly -> SectWeeklyRewardSqlRepository`；game-only `sect.011`、player-only `sect.012` 启动预建 schema，operation receipt 与资产/领取标记在同一 attached transaction 提交。聚焦回归 `48 passed`，progress/inventory/architecture `18 passed`，recovery 路由和 reconcile clean；SQLite WAL 多库崩溃原子性仍不作保证。

`sect confirmed-disband application cutover`：`确认解散宗门` 经 `SectApplication.disband -> SectManualDisbandSqlRepository`；game-only `sect.013` 预建旧格式兼容回执，保留宗主复核、成员解绑、宗门删除、replay 和事务回滚。宗门筛选回归 `65 passed`，progress/inventory/architecture `18 passed`；隔离五库 backup/restore、migration routing 与 reconcile clean。用户原有 `boss_info.json` 修改保持未提交，仓库 Python/pytest 缓存和本片 recovery 临时数据已清理。下一片按宗门剩余边界审计 Fairyland claim 及其 Tianti profile/settlement 依赖；真实发布数据/P7 和全局 legacy blockers 继续开放。

`sect fairyland claim application cutover`：默认领取经 `SectFairylandApplication -> SectFairylandSqlRepository`，在 player-db immediate UoW 同步提交 tianti 气血、每日 marker、旧列兼容投影和 operation receipt；应用不再跨两个事务写外层 ledger。新增 player-only `sect_fairyland.002`，回填旧动态领取列；旧兼容 service 读取新 marker，运行时 Clock 与天降灵脉 multiplier 均显式注入。切片聚焦回归 `69 passed`，额外宽回归有 3 项已知无关失败；compileall、architecture、progress、inventory、diff check 通过。五库 recovery 覆盖 185 migrations，路由 `145/36/7/1/1`，`.002` 仅 player_db，reconcile clean。下一片先界定 Tianti profile 的写入/结算 owner 与 Fairyland upgrade 事务边界；本切片不代表宗门/Tianti 整体完成。

`tianti read-only profile legacy normalization`：feature-owned `TiantiProfileReader.clean` 恢复旧 manager 的境界/气血/药浴容错、JSON 字段清洗和窍穴配置校验；已开启列表去重保序，详情按列表重建并由配置补全，缺失窍穴配置按空配置处理。`TiantiProfileSqlReader` 仍只读，不回写历史脏行；无 schema/migration 变化。Tianti Training、Fairyland、只读 facade/source 与 architecture 聚焦回归 `77 passed`；Tianti/Fairyland、inventory/source-quality 与 architecture 扩展套件 `321 passed, 1 failed`，唯一失败是已知过期 Rift source-quality 断言仍要求旧 `_sql_message_instance` 字段；compileall、progress、inventory 与 diff check 通过。progress 仍报告 legacy transaction services 和 `xiuxian2_handle` 两项全局 blocker，不能将本切片视作重构完成。pytest 禁用 cacheprovider/字节码，compileall 专用 `/tmp` 缓存已清理，`data/` 与运行数据库未触碰。下一片继续界定 Tianti profile 写入/结算 owner 与 Fairyland upgrade 事务边界。

`sect fairyland upgrade startup-schema boundary`：真实 `宗门炼体堂升级` 继续经 `SectApplication -> SectFairylandSqlRepository`；新增 game-only `sect.014` 预建 `sect_fairyland_operations`，兼容旧版请求时建表产生的同结构操作表及回执。repository 不再请求时执行 DDL，缺 schema 返回 `schema_missing`；宗主/成员身份、等级只升一级、双资产余额与 CAS 更新、升级状态及回执在同一 UoW 中校验/提交。修复 `SectMutationResult.applied` 漏含 `upgraded` 导致升级成功后 handler 报失败，并将 duplicate 提示放在经济日志之前。宗门/Fairyland/source-quality/inventory/architecture 聚焦回归 `325 passed, 1 deselected`（排除已知过期 Rift source-quality 断言）；compileall、progress、inventory、architecture、diff check 通过。五库恢复演练覆盖 `186` 个 migration，路由 `146/36/7/1/1`，backup/restore dry-run/restore 均成功，`sect.014` 仅进入 game_db，reconcile clean。专用恢复目录和字节码缓存已清理；`data/`、运行数据库及用户 `boss_info.json` 修改保留。progress 仍有 legacy transaction services 与 `xiuxian2_handle` 全局 blocker；下一片审计 Tianti profile 写入/结算 owner，不能将宗门或整体重构视作完成。

`tianti profile writer ownership`：Tianti Training、Tianti Settlement、宗门炼体堂领取的默认 SQL repositories 共用 `upsert_tianti_profile` 序列化/UPSERT primitive，支持 main 与 attached `player_data` schema；各 feature 仍在自己的 UoW 内拥有资产、profile 和 operation receipt 原子提交。移除默认 Tianti command facade 中未使用的 `TiantiDataManager` 单例和 stone repository 的无效 manager 注入；保留 `get_user_tianti_info` 作为有明确标记的 legacy write-through API，Settlement/Training legacy adapters 仍显式保留，默认路径不使用它们。无 migration。Tianti/Fairyland/source-quality/inventory/architecture 回归 `356 passed, 1 deselected`（排除已知过期 Rift source-quality 断言）；compileall、progress、inventory、architecture、diff check 通过。五库恢复演练 186 migrations、路由 `146/36/7/1/1`，backup/restore dry-run/restore 成功，reconcile clean；专用恢复与字节码目录已清理。全局 legacy transaction services 和 `xiuxian2_handle` blocker 仍在；下一片审计 Tianti `transaction_service.py` 的结算/展示规则依赖与显式 legacy settlement adapter，不代表 Tianti 或全局重构完成。

`tianti gain-rule presentation ownership`：新增 training feature `presentation.py`，统一药浴有效时点、窍穴收益、宗门炼体堂加成和每分钟收益预览；状态命令及宗门展示改为直接使用 feature rules，Tianti Settlement SQL repository 与 Training SQL repository 复用同一规则，旧 `transaction_service.py` 保留同名兼容包装。调用审计确认默认 settlement 使用 feature application/repository，`TiantiSettlementService` 仅由显式 `LegacyTiantiSettlementRepository` 惰性实例化；状态页 `get_tianti_cap` 仍从 legacy module 读取，列为下一边界。无 migration。Tianti Training、Settlement、Fairyland 与历史兼容服务回归 `70 passed`；progress、inventory、source-quality、architecture 回归 `247 passed, 1 deselected`（排除已知过期 Rift source-quality 断言），progress 脚本通过且仍报告 legacy transaction services、`xiuxian2_handle` 两项全局 blocker。测试禁用 pytest/字节码缓存，未触碰 `data/`、运行数据库或用户 `boss_info.json`；下一片迁移状态页 cap 查询到 feature-owned profile/read rules，再审计可删除的显式 legacy adapter，不代表 Tianti 或全局重构完成。

`tianti profile cap query ownership`：`TiantiProfileReader.cap` 原有的等级上限规则通过 `TiantiProfileSqlReader` 与 `TiantiTrainingApplication.profile_cap` 对外提供；默认 Tianti facade 删除对整个 `transaction_service.py` 的 import，状态页上限改由 feature profile reader 计算。保留既有关闭倍率默认值 `1.5`，测试覆盖下一等级需求倍率及最高等级 `10**30` 上限，无 schema/migration。profile/application/progress/inventory 聚焦回归 `14 passed`，progress checker 通过；整体 blocker 仍为 legacy transaction services 和 `xiuxian2_handle`。测试禁用 pytest/字节码缓存，用户 `boss_info.json` 仍未暂存；下一片追踪 `grant_tianti_settle_minutes` 的真实宗门调用及 legacy training adapters 的运行时注入边界，不代表 Tianti 或全局重构完成。

`tianti legacy adapter service construction`：`LegacyTiantiTrainingRepository` 原先每次任一兼容调用都构造 Stone Training、Medicine Bath、Breakthrough、Qiaoxue 四个 service；现按实际 operation 延迟构造唯一对应 service，不改旧 APIs 或事务实现。追踪确认 `grant_tianti_settle_minutes` 仅由旧 Fairyland claim service 使用，默认领取已由 feature repository 接管；旧 claim service 继续保留给显式兼容 adapter。无 migration。Tianti Training 全套回归 `43 passed`，progress 断言及 adapter 调用测试通过；测试禁用 pytest/字节码缓存，`data/`、运行数据库及用户 `boss_info.json` 未触碰。全局 blocker 仍为 legacy transaction services 和 `xiuxian2_handle`；下一片清理宗门 facade 中已无调用点的 `SectMembershipService` lazy getter/import，保留其公开 legacy service 类及直接兼容测试。

`sect membership stale facade adapter removal`：全仓调用搜索确认 `_sect_membership_service()` 除定义外无调用，成员增删/职位调整默认入口已在 `SectApplication`；移除 facade 中旧 `SectMembershipService` import、单例和 lazy getter，service 类及其直接兼容测试仍保留。无 schema/migration。宗门 membership、progress、inventory、architecture 回归 `26 passed`；sect source-quality `12 passed`，progress/inventory/architecture 检查通过。测试禁用 pytest/字节码缓存，工作区 `boss_info.json` 保持未暂存；全局 blocker 仍为 legacy transaction services 和 `xiuxian2_handle`，下一片继续按 progress 文件追踪剩余默认 legacy service 连接，不代表宗门或整体重构完成。

`sect disconnected service facade removal`：全仓调用搜索确认 close mountain、owner inherit、open/close join、daily reset 五个 legacy getter 除定义外均无调用，默认 handler 均经 `SectApplication`；移除 facade getter、实例槽和只被 getter 使用的 service imports，保留 legacy service 类及其独立事务测试。无 schema/migration。宗门 service、join-state、progress、inventory、architecture 回归 `57 passed`；sect source-quality `12 passed`。测试禁用 pytest/字节码缓存，`boss_info.json` 用户修改仍保持未暂存；progress 全局 blocker 仍为 legacy transaction services 和 `xiuxian2_handle`，下一片盘点 sect facade 其余 service imports 与调用点，不代表宗门或整体重构完成。

`sect legacy service import audit`：核对当前 facade 后确认不存在任何 `.transaction_service` import；进度脚本新增约束防止旧 service import 回流。`XiuxianDateManage` 仍承担多处读模型及活动时间戳更新，属于 `xiuxian2_handle` 路径而非 transaction service；不在本审计中移除。progress/inventory/join-state 回归 `4 passed`，无 migration/数据变更；测试禁用 pytest/字节码缓存，`boss_info.json` 继续保持未暂存。下一片从 `update_last_check_info_time` 这类窄 side effect 选定 feature owner 并追踪调用契约，不代表 sect 或整体重构完成。

`sect elixir activity timestamp ownership`：丹房领取的活动时间更新现经 `SectApplication -> SectActivitySqlRepository` 写入 game-db `user_cd`；Clock 显式注入，保存格式兼容旧 reader，调用仍在成员资格判断前，缺失行不触发插入或 DDL。无 migration；聚焦行为/source/progress `7 passed`，architecture/inventory contract `17 passed`，compileall、architecture CLI、progress 与 diff check 通过。pytest 禁用 cacheprovider/字节码；pytest/pyc 与专用 `/tmp` 临时目录已清理，磁盘/RAM 已复核，`boss_info.json` 保持用户未提交修改。架构 CLI 初始化时写入 `data/xiuxian/compatibility_hits.json` 并刷新已有 `xiuxian_impart.db-shm`；发现运行中 NoneBot PID `115747` 共用数据库，故保留运行期文件，不手工清理。全局 blocker 仍为 legacy transaction services 与 `xiuxian2_handle`；下一片从宗门 facade 里其余直接使用 `XiuxianDateManage` 的状态读写中，继续选择窄、可独立验证的 feature-owned 边界。

`sect activity timestamp read ownership`：自动传位/解散 scheduler 的两处最后活跃时间读取现经 `SectApplication -> SectActivitySqlRepository`；只读现有 `user_cd`，旧时间字符串转为本地 aware datetime。`SectDisbandSqlRepository` 对 legacy naive 时间与 aware Clock 做兼容比较。无 migration；行为/progress/source 聚焦 `8 passed`，pytest 禁用 cacheprovider/字节码且本轮未启动 NoneBot/访问共享运行数据库。basetemp 已清理；共享 `xiuxian_impart.db-shm` 仍由运行中 bot 保护。下一片继续审计宗门 facade 中其他直接 `XiuxianDateManage` 状态读取。

`sect scheduled material startup-schema cutover`：定时发放目标集合改由 `SectApplication -> SectScheduledMaterialSqlRepository.list_targets` 只读获取，grant repository 不再请求时建表；新增 game-only `sect.015` 启动迁移预建回执表，既有表/回执由 `IF NOT EXISTS` 保留，缺 migration 不写资产且不 DDL。成功状态 `granted` 现在计入 application `.applied`。scheduled grant、progress、inventory、migration count 与 architecture contract `28 passed`；无运行数据迁移，本轮测试只使用临时 SQLite。pytest/pyc 缓存禁用，basetemp 清理后复核；下一片继续审计宗门 facade 的其他 manager 只读入口。

`sect directory query ownership`：定时状态任务和宗门列表命令的共用全量查询改由 `SectApplication -> SectDirectorySqlRepository` 只读执行，旧 tuple 顺序及全部宗门/成员计数语义保留，无 migration 与 DDL。repository 行为与 progress/inventory/migration-count/architecture contract `26 passed`，pytest/pyc 缓存禁用且专用 basetemp 已清理，未读取运行数据库；下一片继续审计宗门 facade 的其他 manager 只读入口。

`sect inactive-owner state read ownership`：自动状态任务用到的 `closed` 与 `sect_owner` 改由 `SectApplication -> SectInactiveOwnerSqlRepository` 只读读取，保留缺失宗门返回 `None`，无 migration 与 DDL。repository 行为与 progress/inventory/migration-count/architecture `27 passed`；pytest/pyc 缓存禁用且专用 basetemp 已清理，未访问运行数据库。成员和用户快照仍待后续迁移。

`sect inactive-owner member snapshot read ownership`：scheduler 的两次成员查询改经 Sect application/read repository，保留 `SELECT *` 行结构、原数值归一化、列表顺序与候选选择逻辑，无 migration/DDL。纯数值转换/row normalization 移入 `core.numeric`，旧 `numeric_bind` 导出兼容；core numeric、兼容 helper、Sect repository、progress/inventory/migration-count/architecture `40 passed`，pytest/pyc 缓存禁用且专用 basetemp 已清理。宗主用户资料仍是下一个同场景读取边界。

`sect inactive-owner profile read ownership`：scheduler 的宗主读取改经 Sect feature，只选 `user_name`，保留重复 id 首行和用户缺失处理；该 handler 中 manager DB reads 均已移除。repository/progress/inventory/migration-count/architecture/core numeric `41 passed`，测试缓存和临时目录已清理，无运行库访问。下一片审计其余宗门命令的重复 `get_sect_info` 读取。

`sect info read ownership`：facade 中 25 个宗门详情读取改经 Sect application/repository，保留完整字典、缺失行与 sect numeric normalization，无 migration/DDL。相关 repository、core numeric、progress/inventory/migration-count/architecture `43 passed`，pytest/pyc cache 已清理；继续迁移其他 manager 读模型。

`sect member list read ownership`：默认 facade 的成员列表、人数上限、宗门成员展示及闲置宗主 scheduler 快照统一经 `SectApplication -> SectMemberSqlRepository` 只读查询；保留 `SELECT *` 行结构、无显式排序的旧结果顺序、空列表语义和 `core.numeric.normalize_user_row`。`sect_member_utils` 由 facade 注入 Sect application，默认详情/成员数读取不再经 `XiuxianDateManage`；未绑定时保留独立兼容 fallback。无 migration/DDL。宗门 repositories、Sect 入口与进度契约、core numeric、source-quality、architecture、inventory/progress 聚焦回归 `509 passed`；compileall、inventory、progress、隔离数据目录下的 architecture CLI 和 diff check 通过。pytest/pyc 专用目录已清理；未访问运行数据库，用户 `boss_info.json` 修改保留。下一片审计 `sect_weekly_commands.py` 宗门详情及其余用户资料 manager reads；全局 legacy transaction services 与 `xiuxian2_handle` blockers、真实发布迁移/P7 仍未完成。

`sect weekly status detail query ownership`：`宗门周常` 默认展示 handler 的宗门名称读取改经 `_sect_weekly_application().get_sect_info`，不再通过 `XiuxianDateManage`；manager lazy getter 与 lock 仅留在显式 legacy claim fallback，不移除兼容边界。无 migration/DDL。周常领取/展示、progress contract、Sect member/info contract 与 source-quality 回归 `243 passed`；inventory、progress、隔离数据目录下的 architecture CLI、compileall 和 diff check 通过。pytest/pyc/architecture 专用临时目录已清理，未访问运行数据库。接下来盘点多个 Sect handler 与 `sect_weekly.py` 中的 user-profile reads；全局 legacy transaction services 与 `xiuxian2_handle` blockers、真实发布迁移/P7 仍未完成。

`sect user profile read ownership`：新增 `SectMemberSqlRepository.get_user_profile` 与 `SectApplication.get_user_profile`，按 user_id 只读取完整用户行，保留重复 id 时最小 rowid、缺失返回 `None` 和 `normalize_user_row`；Sect facade 的六处资料读取、`sect_member_utils` 默认 task helper 与周常进度缺省 sect_id 查找均改走 application。未绑定 helper fallback 保留；周常 manager 对目标表的写入/锁定仍是遗留边界。无 migration/DDL。member repository、Sect 创建/商店/任务、weekly、progress/source-quality 回归 `267 passed`；inventory/progress、隔离数据目录下 architecture CLI、compileall、diff check 通过。pytest/pyc/architecture 专用临时目录已清理，未访问运行数据库。接下来审计 Sect 按用户名/宗门名读取及周常进度 helper 的剩余 manager I/O；全局 legacy transaction services、`xiuxian2_handle` 与真实发布迁移/P7 blockers 仍未完成。

`sect user-name profile read ownership`：新增 `SectMemberSqlRepository.get_user_profile_by_name` 与 application API，按 `user_name` 查询完整资料并保留重复道号最早 rowid、缺失 `None` 和 numeric normalization；踢出成员与职位变更两个入口不再读取 legacy manager。无 migration/DDL。member repository、成员移除/职位变更、progress/source-quality 回归 `255 passed`；inventory/progress、隔离数据目录下 architecture CLI、compileall、diff check 通过。pytest/pyc/architecture 临时目录已清理，未访问运行数据库。后续审计宗门名/编号查询与名称去重读取；全局 legacy transaction services、`xiuxian2_handle` 与真实发布迁移/P7 blockers 仍未完成。

`sect name and active-name lookup ownership`：`get_sect_info_by_id` 两处回退展示读取改用既有 `SectApplication.get_sect_info`；加入命令的宗门名到 ID 查找由 `SectInfoSqlRepository.get_id_by_name` 只读承担，名称重复时保留旧查询的首行语义；随机宗名的重名集合由 `SectDirectorySqlRepository.list_active_sect_names` 查询，仅包含 `sect_owner IS NOT NULL` 的宗门。未绑定 helper 的 manager fallback 保留。无 migration/DDL。Sect info/directory repositories、创建/加入/source/progress 回归 `243 passed`；inventory/progress、隔离数据目录下 architecture CLI、compileall、diff check 通过。pytest/pyc/architecture 临时目录已清理，未访问运行数据库。周常进度 helper 的 manager 写入/锁仍是下一块数据边界；全局 legacy transaction services、`xiuxian2_handle` 与真实发布迁移/P7 blockers 仍未完成。

`sect weekly progress application cutover`：目标初始化、读取、事件进度和排行榜改经 `SectApplication -> SectWeeklyProgressSqlRepository`；短生命周期 SQLite UoW 校验已登记 `sect.011` schema，操作结束即关闭连接，不请求期建表、无长连接/进程内进度缓存，也不新增 migration。保持目标顺序、进度封顶、参与者 JSON 累计、完成跃迁、claimed_users 和排行榜最多 50 条语义。Sect 定向回归 `492 passed`，周常/source-quality 最终回归 `256 passed`；progress/inventory/隔离数据目录 architecture 与 diff check 通过。pytest/pyc cache 禁用，测试与架构临时目录自动清理且未访问运行数据库。下一片审计周常命令残余 lazy compatibility manager/fallback 的真实调用；全局 legacy transaction services、`xiuxian2_handle`、发布迁移/P7 仍开放。

`sect weekly command fallback removal`：调用图确认默认 claim 已走 Sect application、旧 getter 没有生产调用；从命令 façade 移除不可达 legacy service getter 和其专用 SQL manager/lock，旧 `SectWeeklyRewardClaimService` 的显式兼容 API 与直接事务测试保留。无 migration。Sect、`test_sect_*`、source-quality/refactor-progress 回归 `493 passed`，progress/inventory/隔离目录 architecture 与 diff check 通过；pytest cache/字节码禁用，临时目录自动回收。下一片进入 `SectTaskStateManager` 请求期建表和状态读写 owner 审计；全局 legacy transaction services、`xiuxian2_handle`、真实发布迁移/P7 仍开放。
`world-events spirit vein lifecycle application cutover`：自动触发、手动开启/关闭与过期操作改经 `SpiritVeinLifecycleApplication -> SpiritVeinLifecycleSqlRepository`；noop 仍写 replay receipt 但不改状态，开始时间严格早于结束时间，finish/expire 保留原 event id 与时间窗。旧 result/service 移入 `compatibility/legacy_spirit_vein_lifecycle.py`，历史 transaction import 对象身份不变；新增 player-only `world_events.006`，请求路径无 DDL，progress gate 同时核验 Demon lifecycle/wave 的 compatibility isolation。聚焦回归 `81 passed`，inventory freshness、目标文件 compileall、architecture CLI、progress 与 diff check 通过。隔离 recovery 覆盖 195 项 migration，五库/config backup/restore、dry-run、readiness 六项全绿和 reconcile clean；`world_events.006` 仅 player DB，全部 migration pending 为空。临时数据自动回收，未触碰 `.venv`、`.git`、`data/`、运行数据库或用户 `boss_info.json` 修改。下一片依协议审计 `SectTaskStateManager` 的请求期 schema 与状态读写 owner；全局 legacy transaction services、`xiuxian2_handle` 和真实发布迁移/P7 仍开放。
`sect task-state projection cache removal`：审计确认 `SectTaskStateManager` 已是无 SQL compatibility façade，默认状态读写、领取/刷新和结算由 `SectApplication` 与 feature repositories 承担，启动 migrations 预建 schema，请求不建表；旧 membership task SQL 只留兼容实现。删除 `userstask` 跨请求缓存和按日失效逻辑，命令与 buff 显示改用本次查询的局部 task DTO，保留查询型 `isUserTask` 兼容 API 与 settlement snapshot 校验。状态/领取/刷新/结算、buff/source-quality 聚焦 `264 passed`；inventory freshness、compileall、NoneBot 初始化后的 architecture (`ok=true`)、progress gate 与 diff check 通过。pytest basetemp、编译缓存、架构隔离数据和本轮仓库字节码已清理，无 migration/live DB 访问。下一片审计 compatibility membership 中 task claim/refresh/settlement 旧实现的生产可达性；全局 legacy transaction services、`xiuxian2_handle` 与真实发布迁移/P7 仍开放。
`sect legacy task transaction call-graph audit`：确认默认 task claim/refresh/settlement 都从 Sect facade 经 `SectApplication` 到 feature repositories；`SectMembershipService` 在生产源码中只由历史 `transaction_service.py` 与 `membership_service.py` re-export，无实例化点。旧任务 SQL 及 `_ensure_task_*` 动态建表只保留于 `compatibility/legacy_sect_membership.py` 的显式回滚 API 和直接兼容测试，不在默认请求图中。progress gate 中 Sect claim/settlement ownership、migration routing 和 no-DDL 条件均为 true；无代码/migration/运行数据变更。下一步从全局 `legacy transaction services` 与 `xiuxian2_handle` blockers 重新选择仍可达的默认执行路径，不重复迁移此任务边界。

`admin global stone batch cutover`：`神秘力量 数量 all` 使用 feature-owned game DB 批次 repository 后台分块执行，首次即冻结目标集并保存逐用户回执；`admin_asset.003` 启动迁移建表，请求路径无 DDL，启动前执行按用户数的磁盘空间预检。同管理员/增量的并发新 operation 在 `BEGIN IMMEDIATE` 内返回 `in_progress`，新增部分唯一索引兜底，避免重复全服调整。admin asset/application/source/progress 窄回归 `16 passed`，合并 admin asset、额外管理员批次、inventory、architecture contracts 与 progress 回归 `48 passed`；覆盖不同 operation ID 的活动重复请求、唯一索引约束、冻结/恢复、冲突、晚失败回滚和低磁盘拒绝。五库隔离 recovery 应用 214 项迁移且 `.003` 仅路由 game DB，五库 backup/restore dry-run/restore 成功、reconcile clean；NoneBot 初始化后的 architecture CLI `ok=true`，inventory freshness、目标 compileall、progress 与 diff check 通过。专用 `/tmp` 测试/恢复/编译产物已清理并复核资源；不清理 `.venv`、`.git`、仓库 `data/`、运行数据库、用户 `boss_info.json` 或持久批次回执。下一步按 6.2 审计 player/economy 剩余旧执行路径；普通物品、其他管理员资产、全局 legacy transaction services、`xiuxian2_handle` 和 P7 仍开放。

`admin exp startup-schema boundary`：`修为调整` 默认走 `AdminAssetApplication -> AdminExpAdjustmentSqlRepository`；game-only `admin_asset.004` 预建 exp operation receipt，repository 只读校验 `.002/.004` schema，缺失时拒绝且不建表、不改资产。保持 exp CAS、replay、下限与 economy trace audit。admin repositories/application/source、额外管理员批次、inventory、architecture contracts 与 progress 回归 `50 passed`；progress CLI 修为/灵石迁移门禁全绿，整体仍有旧 transaction service 与 `xiuxian2_handle` blockers。五库 recovery 应用 215 项迁移且 `.004` 仅路由 game DB，backup/restore dry-run/restore 成功、reconcile clean；NoneBot 初始化后的 architecture CLI `ok=true`，inventory freshness、compileall、diff check 通过。专用 pytest/recovery/pycache/architecture 数据目录验收后清理并复核资源；未触碰运行数据库或用户 `boss_info.json`。下一项审计 `features/admin_asset` 其他单人仓储请求期 DDL及旧兼容 getter 可达性；全局 P7 仍未完成。

`bank legacy fallback integrity audit`：matcher 保留 game DB account projection/bootstrap 主路径；legacy row 缺失才允许无旧账户写回退，已有用户行或无效 schema 在回退前明确拒绝。首次升级/结息只在 projection 缺失时按旧读取快照初始化默认账户，并与余额/operation receipt 同事务提交；升级保留原 `updated_at` 计息起点。修复首次结息 bootstrap 的 applied 返回值，新增 progress 指标断言。bank 全套、progress/inventory/architecture contract `106 passed`，隔离 architecture CLI、inventory、compileall 和 diff check 通过；无 migration/live DB 写入。pytest/pyc/architecture 专用临时目录清理完毕，磁盘/RAM 已复核；保留用户 `boss_info.json` 修改。v1 Web `LegacyBankRepository` 仍是 bank 未完成边界；下一片按活动队列审计 `pass` 子领奖，不能据此宣称全局重构完成。

`activity pass child reward ledger cutover`：pass 默认领奖和旧 `ActivityPassClaimService` facade 改经 `ActivityPassClaimApplication -> ActivityPassClaimRepository`；灵石/物品与 operation ledger、feature receipt、等级 reservation 在 game DB 内原子提交，旧 `activity.db` 仅负责幂等 claim-state projection。projection 失败保留 `granted` 状态，startup/CLI reconcile 与同 child operation ID 用户重试都可续跑；即使 projection 已提交但确认丢失，也会先恢复原 operation。operation replay 校验 payload；game-only `activity_reward.006/.007` 预建新账本并分块只读导入旧回执及已领取等级，损坏 payload/reservation 冲突会拒绝迁移。pass、tasks、claim-all、平台迁移与 progress 聚焦 `60 passed`，有 2 条既有 compatibility deprecation warning；progress ownership/stable child ID、inventory freshness、隔离数据目录 architecture (`ok=true`)、目标 compileall 与 diff check 通过。测试/架构/字节码产物位于 `/tmp`，未访问运行库，用户 `boss_info.json` 修改保留。旧 ATTACH 版本若曾遇跨库崩溃，历史回执无法证明对应 game 资产已提交，真实部署前仍需备份并完成 P7 资产/恢复审计；活动状态表仍是 legacy projection。下一片审计 `boss_milestone`，之后 `boss_rank`；全局 legacy transaction services、`xiuxian2_handle` 和正式发布 migration/P7 仍未完成。

`player profile read ownership`：共用 `check_user()` 资料读取改由 `PlayerProfileApplication -> PlayerProfileSqlRepository` 只读查询 `user_xiuxian`，保留化身/伪装解析、重复 user_id 首行和 numeric normalization；缺少数据库或 schema 时返回未知用户，不在请求期创建数据库/表。生命周期在 runtime service 中注入该 reader，导入期只保留轻量应用对象；完整“我的修仙信息”仍依赖属性、宗门、师徒等多个 projection，未宣称整体迁移。profile/application/source contract 定向测试通过；无 migration、无运行数据访问。下一片继续审计 `get_user_real_info` 与体力/经济写入的真实默认路径；全局 legacy transaction services、`xiuxian2_handle`、正式发布/P7 仍未完成。

`tasks status read ownership and frozen entry closeout`：同一 `tasks` feature 的周常、个人、每日任务和领奖四个冻结入口一起核对；三条展示 handler 保留旧任务目录/回复适配，但通过 `TaskProgressApplication.read_states -> TasksProgressRepository.read_states` 的只读 UoW 查询，只投影请求周期，不建数据库/进度行、不做周期 rollover，缺库/表/所需列时返回空状态。领奖默认路径已由 `TaskClaimApplication` 及 game/player repositories 持有 ledger、reservation、grant、confirmation 和 reconcile，本批只刷新其冻结证据，不重复改写 Saga；静态 task catalog 与 Items 快照保留为明确兼容边界。无 schema/migration 变化。命令 owner/只读行为、领奖、progress 与 Phase2/completion gate 联合回归 `64 passed, 34 subtests passed`；task event 聚合集合 `73 passed, 34 subtests passed, 1 failed`，唯一失败是既有 `test_task_progress_event_transaction.py::test_production_entries_use_batched_idempotent_task_events` 对 pet handler `trace_id` 的源码断言，与本片无关。冻结计数为 `267/126/19/84`，membership hash 保持 `7787a74a...ff51fac`、integrity errors 为 0。未测生产延迟；下一个 feature 按 `allowed_features` 为 tianti。

`tianti frozen display entry closeout`：一次处理四条冻结展示入口，不重做既有训练、结算、突破、药浴或冲窍事务。`我的炼体`/`我的体窍` 通过 `TiantiTrainingApplication.read_profile -> TiantiProfileSqlReader` 获取只读状态；前者将 Sect level 查询接入 `SectApplication.get_sect_info`，并使用已存在的气血/药浴/ sect bonus presentation；后者将加成汇总委托给 `calc_qiaoxue_bonus`，仅把配置目录保留为静态兼容读取。`炼体帮助`/`炼体境界` 只发送固定文案，归类为允许保留的 message-only 路径。无 schema/migration 变化。owner、训练 feature、Phase2/progress/completion `99 passed, 36 subtests passed`，Tianti source-quality 子集 `8 passed`；有 1 条既有 anyio pytest rewrite warning。冻结计数由 `267/126/19/84` 更新为 `269/128/19/80`，membership hash `7787a74a...ff51fac` 不变、integrity errors 为 0。未测生产延迟；下一项按 `allowed_features` 为 title。

`base player profile lookup cutover`：基础命令的改名道号去重、注册复查、送/偷/抢灵石目标查询统一改用 `get_user_profile/get_user_profile_by_name`，由同一只读 profile application 承担；保留首行重复 ID/道号语义，资产事务和兼容 service 未改。基础 handler 不再直接调用 `XiuxianDateManage.get_user_info_with_id/with_name`；无 migration、无请求期 DDL、无运行数据访问。profile/base/source 回归通过并完成缓存清理；下一片继续审计 `get_user_real_info` 及体力/经济写入默认路径。

`info identity projection read cutover`：`我的修仙信息` 的主用户、道侣、师父和徒弟名称读取改用 `PlayerProfileApplication`，动态属性仍由既有 `get_final_attributes` 单独计算；排名、宗门、功法和最后查看时间等跨投影依赖保持原边界，未宣称完整信息页迁移。无 migration、无请求期 DDL；profile/info source 回归通过。下一片继续审计信息页的排名/宗门只读 projection 与 `update_last_check_info_time` 副作用。

`base stone-robbery settlement cutover`：`抢劫` 默认 handler 通过 `BaseApplication -> BaseStoneRobberySqlRepository` 在 ATTACH player DB 的单一事务中完成双玩家快照/CAS、统计和 game receipt；资产部分 CAS 失败由 savepoint 回滚。新增 game-only `base.004` 与 player-only `base.005` 启动迁移，request path 只读检查 schema，缺 migration/database 时 fail closed。旧 `StoneRobberySettlementService` 仅保留为显式兼容路径。聚焦 feature/source/progress/migration 回归通过，临时测试与编译产物在验收后清理；全局 legacy transaction services、`xiuxian2_handle` 与正式发布/P7 仍未完成。
最近完成 `base player-rename replay-read and startup-schema boundary`：改名 handler 的 receipt replay lookup 经 `BaseApplication -> BaseRenameSqlRepository` 只读执行，移除 facade 默认 `PlayerRenameService` getter；game-only `base.002` 预建新 schema 并为历史 receipt 补可空 `payload`，既有记录不重写，旧 `NULL payload` receipt 仍作为 duplicate 返回。改名 repository 请求期不再创建/修改 schema，缺迁移时拒绝且不更新资产。handler/source/progress `16 passed`、base feature application/repository `4 passed`；migration game-only 路由、inventory、progress、NoneBot 初始化 architecture (`ok=true`)、目标 compileall 与 diff check 通过。五库恢复演练覆盖 224 项 migration，五库 backup/restore dry-run/restore 成功，`base.002` 仅 game DB，reconcile clean。临时验证产物已清理；旧 `PlayerRenameService` 仍保留为显式 compatibility API。下一项按 6.2 目标 2 继续审计 player/economy 旧执行路径；全局 blockers 与正式发布 P7 仍开放。
最近完成 `base stone-theft settlement cutover`：默认 `偷灵石` handler 通过 `BaseApplication -> BaseStoneTheftSqlRepository` 只读重放并在单个 game-db immediate UoW 更新灵石、体力、receipt；随机结果、duplicate/conflict、参与者状态、部分 CAS 失败和晚失败回滚语义有隔离 SQLite 覆盖。game-only `base.003` 在启动时建新表/补旧 transfer receipt 列，请求期不执行 DDL，缺 schema 不创建数据库或更改资产。旧 `StoneContestService` 仍留给未迁移 transfer/Web 兼容边界，`抢劫` 路径未改。base feature unittest `12 passed`，theft/contest handler 与 progress 选择集 `24 passed`，source/migration contract `7 passed`；progress 新门禁全绿，整体仍被全局 blockers 阻塞。inventory、progress、NoneBot 初始化 architecture (`ok=true`)、compileall 与 diff check 通过；五库 backup/restore dry-run/restore 覆盖 225 项 migration，`.003` 仅 game DB，reconcile clean。所有测试、恢复和编译数据在本轮专用 `/tmp` 路径验收后清理，未访问运行数据库或用户 `boss_info.json`。下一项继续依 6.2 目标 2 审计仍真实可达的 player/economy 默认路径。

最近完成 `dongfu expansion atomic slot projection`：扩建 repository 在扣除地契/灵石的同一 UoW/savepoint 内 CAS 更新 `plot_count`、规范化 `plant_slots` 和 legacy 首槽字段，再写操作回执；旧扩建回执重放会幂等补齐历史短槽列表。handler 移除事务后的 `_save_dongfu`，并向仓储传入种子名称映射，保证 legacy-only 种植快照与后续灵田 CAS 一致。未新增 migration，隔离测试用既有 `map.017` 启动迁移升级旧式 fixture；聚焦测试 `23 passed`，progress 中扩建原子槽位/无 handler 回写门禁均为 `true`，overall `exit_ready=false`，仍有 `legacy transaction services`、`xiuxian2_handle` blockers。inventory、目标 compileall、diff check 通过；architecture CLI 失败于既有 info/game_events/pet/map 检查及缺失 `get_active_user_id` 导入，未报告本切片错误。子代理分工：委派 1 个只读兼容/fixture 审计，无修改、无测试；主线程独占代码整合、串行验收、资源检查与提交，峰值并发 1。专用 pytest/compile/inventory 临时产物验收后清理，运行数据与用户 `boss_info.json` 保留。下一片继续审计洞府收获快照/随机目标旧读取边界，再按目标顺序回到 player/economy 旧默认路径；本切片不代表洞府或全局重构完成。
`impart feature owner close`：十个冻结命令入口一次按 feature owner 收口：抽卡/祈愿、合成/分解、背包/信息/卡图与属性刷新走 `ImpartApplication`，两条静态帮助命令保留兼容 adapter。抽卡 receipt 分别由 game `impart.006`、impart `impart.007`、player statistics `impart.008` 启动迁移准备；卡片新旧分类、统计、余额/状态、卡片及 receipt 在对应 attached UoW 内结算，paid-draw 原始请求数纳入幂等校验。静态 card catalog 通过 `ast.literal_eval` 读取，旧 impart helper 中 manager/data manager 改为首次兼容调用才构造；修复 paid multi-draw 命中后未重置概率基数。Phase 2 十项证据闭合，integrity errors 为 `0`、membership 有效、全局 blocker 从 `144` 降至 `134`。impart command/service/application/migration/inventory 聚焦集合 `47 passed`，progress/path gate `47 passed, 29 subtests passed`，impart source-quality `1 passed`；广义 source-quality/architecture 集合仍有与本片无关的既有失败，不视为通过。未运行根目录全量回归、五库 recovery、真实 live 或 P7；全局 `legacy transaction services`、`xiuxian2_handle` blocker 与正式发布迁移仍开放。用户 `boss_info.json` 保持未暂存。
