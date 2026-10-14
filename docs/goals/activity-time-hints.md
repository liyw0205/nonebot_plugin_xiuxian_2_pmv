# 目标：活动中心时间提示（activity time hints）

状态：develop 实施中（切片于 `time_hint`）。

## 用户价值

`活动中心` 卡片的进行中/未开始状态直接显示配置驱动的倒计时
（"剩余 4 天" / "2 天后开启"），玩家不需要再进入 `活动信息`
计算剩余时间。

## 验收标准

1. `center.time_hint(status, start_time, end_time, now)` 只读、纯配置驱动：
   - IN_PROGRESS 且配置了有限 `end_time` → `剩余 N 天` / `剩余 N 小时`（<24h 显示小时，至少 1）。
   - NOT_STARTED → `N 天后开启` / `N 小时后开启`。
   - `end_time` 为 `无限`/`0`/缺失，或 ENDED/REWARD_CLAIM 状态 → 空串（不显示行）。
2. `build_activity_center_items` 的主卡携带 `time_hint`；
   `activity_center_text` 在玩法行之后、奖励行之前渲染一行该提示；
   无提示时不产生空行，QQ 蓝字与纯文本降级合同不变。
3. 时间来源只有现有 `parse_time` 与版本化配置；不新增配置键、
   不改配置存储、不做任何写操作。
4. 聚焦测试 `tests/test_activity_center.py` 全绿（含无限/无时限
   配置不显示提示的回归断言）；不要求全量测试。

## 非目标

- 不做阶段(stage)倒计时、开服日历、提醒推送或定时通知。
- 不改状态机 `resolve_status` 或入口映射 `commands_for_status`。
- 不新增迁移、奖励经济、Web 页面或通用 UI。
- 不触发 `/root/xiu2` 部署、tag 移动或 Release 发布。
