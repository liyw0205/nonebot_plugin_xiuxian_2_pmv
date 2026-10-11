# 项目执行入口

用简体中文沟通。当前唯一进度入口是
[`docs/current_project_status.md`](docs/current_project_status.md)；每次任务先读该文件。

## 分支与发布

- `main` 是安装器、README/安装教程和正式 Release 分支。
- `develop` 是唯一活动开发分支。新切片先在 `develop` 实施、定向验证并普通推送，审查后普通合入 `main`。
- 旧 `work`、`refactor/*`、文档分支和 worktree 是历史证据；不删除、不重置、不盲目合入。
- 不移动旧 tag，不强推。README 和安装教程只指向 `main` 与真实存在的正式 Release。

## 部署与数据

- 安装、重启和测试共用 `flock -w 600 /tmp/nonebot-project-validation.lock`，串行执行并限制持锁时间。
- 修改部署前先停机；把 `.env`、凭据、配置和全部数据冷备份到权限受限目录，校验源与副本；不覆盖或清理旧备份。
- 不改共享 Python 环境，不输出密钥或个人数据，不对真实数据库做游戏写入测试。
- 兼容性迁移只在隔离数据副本上验证；连接状态和 QQ 官方消息/界面呈现分别记录，未实测不宣称通过。
- 保留 `boss_info.json`、运行缓存和其他用户改动；只暂存当前任务白名单。

## 开发与验证

- 只按 `docs/current_project_status.md` 中冻结的切片范围实施；不扩成重构、奖励系统或全量翻新。
- 聚焦测试执行一次并复用未变化基线；不做全量测试、仿真、压测或真实 QQ 消息。
- 完成即停止：develop 提交/推送、审查、普通合入 main 并推送；Release 只在确有需要且质量门通过时创建。
- 手工编辑使用 `apply_patch`。遇到冲突或备份校验不一致时停止，不强推、不回滚用户状态。
