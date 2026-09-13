# 旧玩法兼容层清单

旧玩法暂不在一次发布中重写。`compatibility/feature_inventory.py` 为每个旧玩法声明 owner、测试标签和迁移归属，命令、路由和数据仍由对应的旧包提供；新写入必须迁移到 `features/<name>` 的 application/repository 边界后才能从兼容清单移除。

迁移顺序遵循风险优先级：签到/活动领取 -> 灵石与物品 -> 交易拍卖 -> 战斗结算 -> 管理操作。每个切片完成后再把旧命令登记为 `compatibility.commands` 转发，并在本文件记录回滚版本。

当前已完成新 application 边界的切片包括：`base`、`back`、`buff`、`map`、`natal_treasure`、`rift`、`trade`，经济、战斗、宗门、宠物和副本 feature，以及 `illusion`、`interactive`、`activity`、`admin`、`beg`、`compensation`、`dongfu`、`dufang`、`entertainment`、`fusion`、`impart`、`impart_pk`、`info`、`lunhui`、`past_life`、`simulator`、`status`、`tasks`、`tianti`、`title`、`training` 等切片。每个 feature 都有独立的 application、repository、schema 和 migration；跨库历史算法通过 feature-owned service port 调用，不能绕过 application 的 operation ledger。

所有清单项的 `migration_target` 都存在完整的 manifest/application/repository/schemas/migrations/commands/web/jobs/tests 结构，迁移版本会在数据库目录中记录。旧包仍保留一个发布周期，用于命令注册、读取历史数据和灰度回滚；命中次数会写入 `compatibility_hits.json`，不满足 P7 删除条件前不会静默移除。

兼容周期由 `CompatibilityReleaseGate` 记录发布起点和命中基线。关闭周期前必须提供后续版本号、无旧 import/URL 标记的运行日志，以及 `recovery_smoke.py --evidence` 生成的完整迁移和备份恢复回执；任一检查失败都不能删除旧入口。
