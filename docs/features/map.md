# 地图探索与战斗
## 用户流程
历史命令包继续作为 transport adapter；移动、回城、交互、战斗、探索、地图委托、种子购买和建设洞府的默认路径均进入 `MapApplication`、feature repositories 或 `CombatSettlementApplication`。

指定道号论道与战绩查询经 `MapApplication.nearby_target -> MapNearbyPlayersSqlQueryRepository.find` 只读查询单目标，不加载完整同节点列表。论道排除本人，指定战绩允许本人；无参数战绩直接查询本人。目标缺失时以同一短只读事务内的 `LIMIT 1` 存在性查询区分“无其他候选”和“有候选但无该道号”，缺文件/schema 或读取错误返回空结果，不创建数据库或执行修复。

保留三维位置、双侧 CAST 字符串 ID 等值、地图侧输出 ID 与道号 BINARY 精确匹配。地图旧 JOIN 包含同一用户的全部 profile，因此先匹配名字，再按 map rowid 与 profile rowid 选首个命中；后一个 profile 同名仍可选中。旧顺序只保证 map rowid，新增 profile rowid 是并列顺序确定化，不清洗历史重复数据。战力继续用 Python `int(value or 0)`，不经 SQLite 整数截断；结算仍由原 application 检查当前位置和幂等回执。

无道号论道经 `await MapApplication.random_nearby_target -> select_random_nearby_target`：两库各冻结一个 rowid 高水位，以 `(map.rowid, profile.rowid)` 联合游标按页读取最多 256 个纯数值元数据 pair，每个候选再用短只读 UoW 点查 public 字段并重查位置、JOIN、本人排除及双上界。所有连接在 await 前关闭，每 32 个 raw pair 及页末协作让出；只保留一个 reservoir(k=1) 目标，按实际观察到的有效 JOIN pair 等概率抽样，不按用户去重，也不改为首 profile。缺库/schema、附库/页/候选读取或战力解析故障丢弃全部 sample，取消向上传播；无缓存、请求期 DDL 或 migration。

这不是跨扫描全局快照：高水位以上的追加不入流；空洞插入、rowid 复用和删除/更新按后续短读取处理。越过的是联合 pair，后续 map 行仍可读取更小的 profile rowid；已经越过的 pair 不重扫。并发变更可改变观察到的候选流，抽样保证仅针对该流。结算继续按原 CAS 拒绝位置漂移，不以读取资格代替结算检查。
## 命令与别名
`地图`、`探索`、`回家`。
## Web API
`POST /api/v1/map/{move,return_home,interactive_start,interactive_finish,combat_start,combat_settle,explore_start,explore_settle,resource_reward,mission_claim,purchase_seed,build_dongfu}`，权限 `user`。
## 数据模型与迁移
地图平台 schema 与各 asset operation schema 由启动迁移 `map.001` 至 `map.016` 管理，按 game/player 数据库路由；请求路径不依赖旧 transaction service 创建回执表。
## 事务与失败回滚
默认 repository 保留各动作的 operation replay、状态快照校验和失败回滚。旧事务实现集中在 `compatibility/legacy_map_transactions.py`；历史 `xiuxian/xiuxian_map/transaction_service.py` 只 re-export 旧名字。默认路径不调用这些实现；显式注入的 `LegacyMapRepository`、`LegacyCombatSettlementRepository` 或旧 `*_service` wrappers 仍可调用。此隔离未改变 ledger/schema，不需要数据库回滚。
## 定时任务
无。
## 配置项
`map_enabled`。
## 适配器差异
消息层不包含战斗算法。
## 测试与手工验收
覆盖体力不足、状态变化、背包容量和幂等重放。
## 灰度开关、回滚和已知限制
战斗引擎、Items/配置解析和静态地图 JSON 仍由显式旧 provider/transport adapter 提供；这不把事务 service 计入默认执行图。旧 service 仍可经 shim 导入，显式 rollback repositories 保持可用。此切片无 migration；既有 `map.001` 至 `map.016` 若需回退，应先恢复代码版本，不删除迁移数据。

附近道友展示经 `await MapApplication.nearby_display -> select_nearby_display`，不再物化完整 `nearby_players` 列表或无界 `seen_ids`。展示查询按 BINARY 字符串 ID 单向 seek，每页只读取一个 ID 和首个 map/profile rowid pair，再以 expected ID 重查 public 字段；最多保留 10 个对象。稳定数据下唯一用户流保持旧语义：不足 10 人按 map rowid 顺序，超过 10 人 reservoir(k=10) 后随机排列。短只读 UoW、双高水位、连接关闭、错误丢弃和取消传播与随机论道一致；单 ID 仍可能很大，SQL 工作集/缺索引耗时/逐候选 I/O/全进程 RAM 不由页大小保证。

展示使用首 map/profile pair 去重，不复用论道的重复 JOIN 权重。双高水位不是全局快照：高水位以上追加不入流，空洞/删除/更新按后续 ID seek 处理；并发变更可能减少结果或使唯一流公平性失去保证，但最终结果最多 10 人且不重复。为控制查询规模，非首重复 profile 的坏字段不会触发展示失败；首 pair 当前字段解析失败仍 fail closed。无缓存、请求期 DDL 或 migration。

## Manifest 清单
- `route: POST /api/v1/map/move`
- `route: POST /api/v1/map/return_home`
- `route: POST /api/v1/map/interactive_start`
- `route: POST /api/v1/map/interactive_finish`
- `route: POST /api/v1/map/combat_start`
- `route: POST /api/v1/map/combat_settle`
- `route: POST /api/v1/map/explore_start`
- `route: POST /api/v1/map/explore_settle`
- `route: POST /api/v1/map/resource_reward`
- `route: POST /api/v1/map/mission_claim`
- `route: POST /api/v1/map/purchase_seed`
- `route: POST /api/v1/map/build_dongfu`
