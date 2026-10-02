# 修炼与基础资产
## 用户流程
突破、渡劫、改名、重入仙途、灵石争夺、抢夺和签到统一进入应用服务。
## 命令与别名
`修炼`、`突破`、`修仙签到`。
## Web API
`POST /api/v1/base/{breakthrough,tribulation,rename,stone_contest,stone_robbery,sign}`，权限 `user`，要求幂等键。
`POST /api/v1/base/xiangyuan/{create,claim}` 与 `GET /api/v1/base/xiangyuan/group`，权限 `user`；写入请求要求幂等键和 CSRF。
## 数据模型与迁移
迁移 `base.001` 创建 feature 标记，`base.002` 在 game DB 启动时预建改名回执表并为旧表补充可空 `payload` 列；`base.003` 预建灵石争夺回执表，并为旧表补齐 theft 字段；`base.004` 在 game DB 预建抢劫回执表，`base.005` 在 player DB 预建抢劫成功/失败统计列；`base.006` 在 game DB 预建仙缘池与操作回执表，`base.007` 在 player DB 预建仙缘发送/领取限额表；`base.008` 仅在 game DB 预建重入仙途操作回执表。玩家奖励经济写入复用既有 `user_xiuxian` schema，由 `PlayerEconomyApplication -> PlayerEconomySqlRepository` 做运行时只读 schema 检查，不新增迁移。历史玩家/修炼表仍由既有 schema owner 持有。
## 事务与失败回滚
`base.009` 在 game DB 预建直接突破核心回执，保留旧回执并补齐 payload；`base.010` 在 game DB 保存冻结计划和关系奖励回执，`base.011` 在 player DB 预建关系预留回执及统计/历史字段。迁移不为历史无 outbox 回执猜测或补造副作用。

`直接突破`（别名 `破`）在冷却检查前查同消息回执，在 game 写事务内冻结成功/失败、惩罚和关系奖励计划，并原子提交核心资产、回执和 `base.direct_breakthrough.effects` outbox。师徒次数先在独立 player 事务中与 prepared 回执一起预留，再提交 game 奖励/回执，最后 player 统计、历史和 applied 回执一起提交；不依赖 WAL 下跨文件 ATTACH 崩溃原子性。中断重放不重抽、不重复计数或发奖，换绑后恢复不覆盖新 count；奖励受当前及冻结修为上限约束，无法兑现时保持待恢复，不取消已预留的旧承诺。

runtime 和 CLI reconcile 都注册 effects handler；每次有效命令最多恢复 5 笔 pending，失败增加 attempts 并轮转，避免长期待恢复记录阻塞队列。统计、日志和历史保持稳定事件 ID/时间，持久回执不属于可清缓存。连续突破、渡厄及其他渡劫仍是兼容边界；通用 Web `breakthrough` 返回契约不变。上述代码切换与隔离恢复测试不代表正式发布迁移或 P7 已完成。

改名写入与回执由 `BaseApplication -> BaseRenameSqlRepository` 承担；重放查询也经 application 使用只读 repository。旧回执缺少 `payload` 时仍作为已完成操作返回，不改动玩家或背包数据。请求路径不创建表或补列，缺少 `base.002` 时拒绝写入。
`偷灵石` 的查重与结算由 `BaseApplication -> BaseStoneTheftSqlRepository` 承担，在 game DB 的单一 immediate UoW 中更新灵石、体力并写回执；缺少 `base.003` 或所需玩家 schema 时 fail closed。`抢劫` 的默认 handler 由 `BaseApplication -> BaseStoneRobberySqlRepository` 承担，在 game DB ATTACH player DB 的单一事务中完成快照校验、双玩家 CAS、统计和回执；资产 CAS 失败通过 savepoint 回滚，缺少 `base.004`/`base.005` 或所需 schema 时 fail closed。普通 stone-contest 由 `BaseApplication -> BaseStoneContestSqlRepository` 承担，在 `base.003` 回执表上执行单事务余额 CAS；重复 operation 只读回首次结果，缺少迁移或玩家字段时 fail closed，不在请求期建表。通用命令 `Cooldown` 的体力扣除由 `PlayerStaminaApplication -> PlayerStaminaSqlRepository` 按 profile 首行 `rowid` 做原子 CAS；缺 schema、用户或快照变化时拒绝继续，不在请求期建表。旧 `StoneContestService` 仅保留显式兼容/回滚调用。
`重入仙途` 的自动择优与手动选择都由 `BaseApplication -> BaseRootRerollSqlRepository` 执行，在 `base.008` 回执表和用户首行上用快照 CAS 原子更新灵根、灵根类型、战力与灵石；同一操作重放返回首次结果，手动回复绑定发起用户和初始快照。缺少迁移/玩家字段、灵石不足或快照变化时 fail closed，不在请求期建表；默认 handler 不再调用 `ramaker`。
## 定时任务
每分钟恢复未满体力。任务通过 `recover_player_stamina -> PlayerStaminaApplication -> PlayerStaminaSqlRepository` 执行，按 `XIUXIAN_STAMINA_RECOVERY_BATCH_SIZE` 分批更新并用 `MIN` 封顶；不读取完整用户列表、不在请求或任务路径建表。缺少数据库/schema 时 fail closed，`stamina_recovery_points=0` 直接返回，避免空转。
连续爬塔、世界首领训练入口和突破入口在前置体力扣除后提前结束时，使用同一 application 的 `restore` 单用户返还；按首个 `user_xiuxian` 行做封顶 CAS，缺 schema 或用户时不写入。
通用奖励、补偿兼容奖励、师徒历史奖励和 Rift Boss 兼容适配器的灵石、修为、宗门贡献写入使用 `PlayerEconomyApplication`；成功路径按首个用户行执行 CAS，修为按传入上限封顶，缺 schema 或状态变化时不生成奖励文本。该边界不负责物品库存、统计或经济流水，它们仍由各自 feature/兼容层负责。
修为小数整理命令也经同一 application，在既有 `user_xiuxian.exp` 上按查询快照 CAS 截断小数；缺 schema、用户或快照变化时拒绝写入，不新增迁移。
## 配置项
`base_enabled`。
## 适配器差异
NoneBot 和 Web 只负责输入输出转换。
## 测试与手工验收
覆盖余额不足、状态冲突、重复操作和异常回滚；重入仙途覆盖战力重算、操作重放/冲突、缺 schema、灵石不足和 game-only 迁移；体力恢复覆盖多批更新、上限封顶、零恢复点、缺 schema fail closed，以及有界查询约束。
## 灰度开关、回滚和已知限制
关闭开关可回退旧基础玩法；旧数值算法暂不复制。

## Manifest 清单
- `route: POST /api/v1/base/breakthrough`
- `route: POST /api/v1/base/tribulation`
- `route: POST /api/v1/base/rename`
- `route: POST /api/v1/base/stone_contest`
- `route: POST /api/v1/base/stone_robbery`
- `route: POST /api/v1/base/sign`
- `route: POST /api/v1/base/xiangyuan/create`
- `route: POST /api/v1/base/xiangyuan/claim`
- `route: GET /api/v1/base/xiangyuan/group`
