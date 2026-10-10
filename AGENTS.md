# 项目执行入口

用简体中文沟通。当前重构任务唯一入口为
[`docs/refactor_slice_execution_protocol.md`](docs/refactor_slice_execution_protocol.md)，
执行范围为 [`refactor-closeout-v1`](docs/refactor_closeout_scope_v1.md)，
结果更新 [`docs/full_refactor_progress.md`](docs/full_refactor_progress.md)。

仅处理固定 C1-C5；历史账本和旧“下一片”不是当前指令，不从任意 false 字段、
旧 helper、目录缺件或清单外发现自动开启新玩法。Phase 2、技术验收和当前发布 P7 分开判断。
当前库存陈旧与普通闭关拒绝重放问题需核验，不凭旧通过记录判完成。

2026-10-10 用户已授权完成本地收口并提交推送。C3 必需的测试包初始化、夹具、
旧源码契约证据维护和已证实回归的最小修复属于本轮，不再因“清单外”暂停。
先修已知八项收集错误和两项 Dongfu 旧断言，再完成最终根目录全量验收；
不新增 skip/xfail/ignore，不删行为断言或降低门槛。已有 C1/C2/C4 和门禁证据按相关状态复用。
审查相关改动后提交并普通推送到 `origin/refactor/full-bottom-layer`，核验远端 HEAD 后
才 complete；用户 `boss_info.json` 不暂存，P7 仍独立受阻，不强推、不合 main、不发版。

保留现有用户改动，手工编辑用 apply_patch。子代理须有明确委派和互不重叠的边界，
主线程统一整合；共享夹具测试与恢复串行。未运行的验收写明受阻，不弱化门禁。
QQ/OneBot 对已证明共用同一 application/repository 的业务长链只验收一份，
两种接入各保留身份、权限、命令与发送语义的短合同测试；旧路径先核对实际 owner，
不凭平台名称去重或增加重复长测。具体边界见唯一执行入口的适配器验证规则。
仅清理本任务创建且进程已退出的临时产物；禁止删除 `/tmp/codex-daemon-*`、
Codex IPC/socket/lock、其它活跃代理目录、运行数据/WAL/SHM、备份和用户 `boss_info.json`，
禁止清空系统缓存或杀 daemon 来解决执行循环。
