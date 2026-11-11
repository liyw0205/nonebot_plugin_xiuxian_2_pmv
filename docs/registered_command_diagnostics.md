# 已注册指令离线诊断

任务进度以 [当前状态](current_project_status.md) 为准。本文件说明可复用的测试合同，
不定义新玩法、奖励或发布流程。

## 执行边界

`tests.command_probe` 从实际 NoneBot matcher 注册表盘点命令、别名、正则和 handler，
走真实 provider、permission、rule、依赖注入及 handler。源文件逐字复制到临时目录，
静态数据使用现有测试 bootstrap，数据库通过真实启动迁移建立；虚构玩家与 mock
OneBot Bot/Event 不连接生产。源码旁旧 JSON 也只在临时副本写入。

审计钩子拒绝网络连接、子进程、临时目录外文件写入和 fixture 外数据库连接。
不开 driver 启动任务、Web 或 QQ；全局事件预处理、QQ 官方呈现与定时任务不在此诊断
范围。修炼仅替换命令模块的60秒等待，仍执行实际开始、结算与落库。

每个场景恢复数据库和 JSON，清理已知读取缓存及冷却状态。内部守卫回复不能把异常
变成通过：原始异常日志、未捕获异常、无堆栈 ERROR 都计失败；离线依赖单列受阻。
保留本场景创建的 task 至退出，未被 awaiter 取回的后台异常也计失败并记录指令、
actor和堆栈，已被调用方处理的异常不误报。未完成后台依赖按具体超时标受阻。
未知命令核查 provider 与 rule 后不执行 handler，别名核查 provider 选择结果。

## 一次基线与定向重试

所有实际验证使用项目 venv、共享 flock 与有界 timeout；基线只执行一次：

```bash
flock -w 600 /tmp/nonebot-project-validation.lock timeout 480 \
  .venv/bin/python -m tests.command_probe --report /tmp/xiuxian-command-baseline.json
```

默认场景是注册玩家无参、未注册玩家、通用参数 `1`、超级用户无参。通用参数不能
代替每条指令的合法参数与成功前置条件。`pass` 只证明该输入实际触发分支无异常并
有回复，权限拒绝、未选中路由、无回复与外部依赖分别记 `blocked`。不能宣称所有
玩法、配置组合、群类型或并发条件都已通过。

```bash
flock -w 600 /tmp/nonebot-project-validation.lock timeout 180 \
  .venv/bin/python -m tests.command_probe \
  --retry-from /tmp/xiuxian-command-baseline.json --report /tmp/xiuxian-command-recheck.json
```

重试通过稳定的模块与主指令选择，不依赖源码行号。也可用 `--command` 与
`--scenario` 精确限制影响范围；`--routes-only` 检查别名与未知命令，不重跑业务。
`tests.command_probe_report` 合并原始 JSON 与定向重试，逐项保留输入、回复、日志、
堆栈及证据来源，生成全部指令 Markdown 清单；离线边界不会被误计为通过。

## 已复现的接线缺陷

| 指令/消费者 | 根因与修复 |
| --- | --- |
| 修仙改名、灵根改名、仙肆购买 | 适配层用外层 rejected 覆盖业务状态；保留 data.status/code 与具体原因 |
| 背包检测 | 已返回领域结果却继续按 OperationOutcome 解包；接受真实返回类型 |
| 洞天福地购买 | cost/name 与实际 stone_cost/default_name 接口不符；修正接线 |
| 灵石修炼 | 把 OperationOutcome 当旧结算结果；正确解包并识别重放 |
| 洞天购买、灵石修炼重放 | 时间或旧资产快照参与外层请求身份；稳定请求身份，业务回执严格核对身份后恢复旧账本 |
| player数据同步 | 漏 save_partner 依赖，且空安装直接遍历不存在的旧目录；补依赖与空目录兼容 |
| 宗门帮助、宗门职位变更 | 页码解析返回 dict，无宗门职位为 None；适配页码及具体资格拒绝 |
| 炼体读取/结算/训练/突破/冲窍 | 默认配置根目录重复拼 xiuxian；使用 player.db 的真实父目录 |
| 冲窍 | 把缺档案已知拒绝抛为 RuntimeError；保留具体拒绝 |
| 宠物启明 | 无宠物返回2项，handler要求3项；统一返回契约，无道具消耗 |
| 拍卖活动个人额度 | 仍引用已移除的 trade_manager；绑定现有队列应用读取 |
| 更新日志、仙肆购买 | broad except误捕 NoneBot 正常 finish；正常流程异常透传 |
| 赛季榜周常回退 | 引用未定义 sql_message；接入现有惰性读取器，异常保留责任方和堆栈 |
| 悬赏令无子命令 | mode=None落空无回复；按现有帮助约定进入查看分支 |

`tests.registered_command_cases` 使用真实注册及隔离数据库复现上述入口，并对成功、
拒绝、缺档/缺物品、旧目录、消息失败和 NoneBot 正常流程分别断言。
`tests.test_buff_command_replay` 验证重复扣费防护、旧 hash、业务提交后账本中断恢复、
异身份/参数冲突、真实快照变化拒绝及回执写入失败回滚。事务、幂等和 v1.2.20
数值规范保持原约束，不以测试 fixture 或模糊文案掩盖实际异常。

2026-11-12 本轮一次基线共672个 matcher、2688场景，初始75个异常包含产品错误及
fixture惰性表缓存/等待/网关接线问题，二者分开处理。1326条主名/别名均选中对应
provider，未知命令没有误匹配。逐指令清单与原始堆栈保存在
`/tmp/xiuxian-command-audit-list.{md,json}`；范围和最终验收以当前状态入口为准。
