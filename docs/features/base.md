# 修炼与基础资产
## 用户流程
突破、渡劫、改名、灵石争夺、抢夺和签到统一进入应用服务。
## 命令与别名
`修炼`、`突破`、`修仙签到`。
## Web API
`POST /api/v1/base/{breakthrough,tribulation,rename,stone_contest,stone_robbery,sign}`，权限 `user`，要求幂等键。
`POST /api/v1/base/xiangyuan/{create,claim}` 与 `GET /api/v1/base/xiangyuan/group`，权限 `user`；写入请求要求幂等键和 CSRF。
## 数据模型与迁移
迁移 `base.001` 创建 feature 标记，`base.002` 在 game DB 启动时预建改名回执表并为旧表补充可空 `payload` 列；`base.003` 预建灵石争夺回执表，并为旧表补齐 theft 字段；`base.004` 在 game DB 预建抢劫回执表，`base.005` 在 player DB 预建抢劫成功/失败统计列；`base.006` 在 game DB 预建仙缘池与操作回执表，`base.007` 在 player DB 预建仙缘发送/领取限额表。历史玩家/修炼表仍由既有 schema owner 持有。
## 事务与失败回滚
改名写入与回执由 `BaseApplication -> BaseRenameSqlRepository` 承担；重放查询也经 application 使用只读 repository。旧回执缺少 `payload` 时仍作为已完成操作返回，不改动玩家或背包数据。请求路径不创建表或补列，缺少 `base.002` 时拒绝写入。
`偷灵石` 的查重与结算由 `BaseApplication -> BaseStoneTheftSqlRepository` 承担，在 game DB 的单一 immediate UoW 中更新灵石、体力并写回执；缺少 `base.003` 或所需玩家 schema 时 fail closed。`抢劫` 的默认 handler 由 `BaseApplication -> BaseStoneRobberySqlRepository` 承担，在 game DB ATTACH player DB 的单一事务中完成快照校验、双玩家 CAS、统计和回执；资产 CAS 失败通过 savepoint 回滚，缺少 `base.004`/`base.005` 或所需 schema 时 fail closed。普通 stone-contest 兼容动作仍保留旧 service 路径。
## 定时任务
无。
## 配置项
`base_enabled`。
## 适配器差异
NoneBot 和 Web 只负责输入输出转换。
## 测试与手工验收
覆盖余额不足、状态冲突、重复操作和异常回滚。
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
