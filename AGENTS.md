# 项目执行入口

用简体中文沟通。当前重构任务唯一入口为
[`docs/refactor_slice_execution_protocol.md`](docs/refactor_slice_execution_protocol.md)，
执行范围为 [`refactor-closeout-v1`](docs/refactor_closeout_scope_v1.md)，
结果更新 [`docs/full_refactor_progress.md`](docs/full_refactor_progress.md)。

`refactor-closeout-v1` 的 C1-C4 与本地交付已完成并推送，当前分支与 origin 对齐；
P7 仍受外部发布证据阻塞。不得自动开启下一重构切片；新开发需先明确范围和验收。

仅处理固定 C1-C5；历史账本和旧“下一片”不是当前指令，不从任意 false 字段、
旧 helper、目录缺件或清单外发现自动开启新玩法。Phase 2、技术验收和当前发布 P7 分开判断。
固定清单内库存 freshness 与普通闭关拒绝重放已按当前证据验收，结果见当前进度。

2026-10-10 收口已修复 C3 必需的测试包初始化、夹具、旧源码契约证据和已证实回归；
根目录全量与 unittest 验收通过，结果见当前进度。文档单独变化不重跑门禁；
用户 `boss_info.json` 不暂存，P7 仍独立受阻，不强推、不合 main、不发版。

保留现有用户改动，手工编辑用 apply_patch。子代理须有明确委派和互不重叠的边界，
主线程统一整合；共享夹具测试与恢复串行。未运行的验收写明受阻，不弱化门禁。
OneBot WebSocket 对共同 application/repository 的代表性业务长链只验一次；QQ 只保留
消息格式、Markdown、蓝字、按钮、回调 ACK 和降级短合同，不重复业务长链。官方呈现与
权限未实测时不得宣称通过。具体边界见唯一执行入口的适配器验证规则。
仅清理本任务创建且进程已退出的临时产物；禁止删除 `/tmp/codex-daemon-*`、
Codex IPC/socket/lock、其它活跃代理目录、运行数据/WAL/SHM、备份和用户 `boss_info.json`，
禁止清空系统缓存或杀 daemon 来解决执行循环。
