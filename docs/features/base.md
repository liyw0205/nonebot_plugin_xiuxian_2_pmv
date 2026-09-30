# 修炼与基础资产
## 用户流程
突破、渡劫、改名、灵石争夺、抢夺和签到统一进入应用服务。
## 命令与别名
`修炼`、`突破`、`修仙签到`。
## Web API
`POST /api/v1/base/{breakthrough,tribulation,rename,stone_contest,stone_robbery,sign}`，权限 `user`，要求幂等键。
`POST /api/v1/base/xiangyuan/{create,claim}` 与 `GET /api/v1/base/xiangyuan/group`，权限 `user`；写入请求要求幂等键和 CSRF。
## 数据模型与迁移
迁移 `base.001` 创建 feature 标记，`base.002` 在 game DB 启动时预建改名回执表并为旧表补充可空 `payload` 列；`base.003` 预建灵石争夺回执表，并为旧表补齐 theft 字段；`base.004` 在 game DB 预建抢劫回执表，`base.005` 在 player DB 预建抢劫成功/失败统计列；`base.006` 在 game DB 预建仙缘池与操作回执表，`base.007` 在 player DB 预建仙缘发送/领取限额表。玩家奖励经济写入复用既有 `user_xiuxian` schema，由 `PlayerEconomyApplication -> PlayerEconomySqlRepository` 做运行时只读 schema 检查，不新增迁移。历史玩家/修炼表仍由既有 schema owner 持有。
## 事务与失败回滚
改名写入与回执由 `BaseApplication -> BaseRenameSqlRepository` 承担；重放查询也经 application 使用只读 repository。旧回执缺少 `payload` 时仍作为已完成操作返回，不改动玩家或背包数据。请求路径不创建表或补列，缺少 `base.002` 时拒绝写入。
`偷灵石` 的查重与结算由 `BaseApplication -> BaseStoneTheftSqlRepository` 承担，在 game DB 的单一 immediate UoW 中更新灵石、体力并写回执；缺少 `base.003` 或所需玩家 schema 时 fail closed。`抢劫` 的默认 handler 由 `BaseApplication -> BaseStoneRobberySqlRepository` 承担，在 game DB ATTACH player DB 的单一事务中完成快照校验、双玩家 CAS、统计和回执；资产 CAS 失败通过 savepoint 回滚，缺少 `base.004`/`base.005` 或所需 schema 时 fail closed。普通 stone-contest 由 `BaseApplication -> BaseStoneContestSqlRepository` 承担，在 `base.003` 回执表上执行单事务余额 CAS；重复 operation 只读回首次结果，缺少迁移或玩家字段时 fail closed，不在请求期建表。通用命令 `Cooldown` 的体力扣除由 `PlayerStaminaApplication -> PlayerStaminaSqlRepository` 按 profile 首行 `rowid` 做原子 CAS；缺 schema、用户或快照变化时拒绝继续，不在请求期建表。旧 `StoneContestService` 仅保留显式兼容/回滚调用。
## 定时任务
每分钟恢复未满体力。任务通过 `recover_player_stamina -> PlayerStaminaApplication -> PlayerStaminaSqlRepository` 执行，按 `XIUXIAN_STAMINA_RECOVERY_BATCH_SIZE` 分批更新并用 `MIN` 封顶；不读取完整用户列表、不在请求或任务路径建表。缺少数据库/schema 时 fail closed，`stamina_recovery_points=0` 直接返回，避免空转。
连续爬塔、世界首领训练入口和突破入口在前置体力扣除后提前结束时，使用同一 application 的 `restore` 单用户返还；按首个 `user_xiuxian` 行做封顶 CAS，缺 schema 或用户时不写入。
通用奖励、补偿兼容奖励和师徒历史奖励的灵石、修为、宗门贡献写入使用 `PlayerEconomyApplication`；成功路径按首个用户行执行 CAS，修为按传入上限封顶，缺 schema 或状态变化时不生成奖励文本。该边界不负责物品库存、统计或经济流水，它们仍由各自 feature/兼容层负责。
## 配置项
`base_enabled`。
## 适配器差异
NoneBot 和 Web 只负责输入输出转换。
## 测试与手工验收
覆盖余额不足、状态冲突、重复操作和异常回滚；体力恢复覆盖多批更新、上限封顶、零恢复点、缺 schema fail closed，以及有界查询约束。
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
