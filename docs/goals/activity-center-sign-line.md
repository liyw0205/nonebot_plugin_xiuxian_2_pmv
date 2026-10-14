# 目标：活动中心个人进度行的今日签到状态（activity center sign line）

状态：develop 实施中（切片于 `personal_progress_summary`）。

前置：状态入口映射/单卡聚合（`9ad7ee8c`）与倒计时行（`131563af`）
已合入 develop/main 且 CI 通过。

## 用户价值

`活动中心` 卡片的个人进度行目前只有任务、战令与可领数，玩家
看不出今天是否已签到，而 IN_PROGRESS 卡的主入口正是 `活动签到`。
复用同一快照里的 `sign.last_sign_date`，在进度行加一段
`签到 已签/未签`，不新增任何查询。

## 验收标准

1. `personal_progress_summary` 在既有行内追加签到段：
   - 以 `snapshot["sign"]["last_sign_date"] == now.strftime("%Y-%m-%d")`
     判定今日是否已签（与结算侧 `last_sign_date == sign_date`
     同一语义）；快照缺 `sign` 键或未签 → `签到 未签`。
   - 已签 → `签到 已签`；段插在任务段之前，行仍为单行、以
     `进度：` 开头。
2. 只读：不新增仓储查询、不改 `overview_snapshot` 的 SQL、
   不新增配置键、不做任何写操作。
3. 既有绿色测试 `tests/test_activity_center.py` 不修改断言仍全绿
   （历史断言均为子串/前缀断言，插入段落不破坏）；新增聚焦断言：
   今日已签 → `签到 已签`，昨日/缺失 → `签到 未签`，
   `activity_center_text(user_id)` 渲染含该段。
4. 只运行 `tests/test_activity_center.py` 聚焦测试；不跑全量。

## 非目标

- 不做签到提醒/推送、连续签到奖励逻辑或签到状态机改动。
- 不改 `resolve_status`、`commands_for_status`、`time_hint` 或
  奖励汇总 `claimable_reward_count`。
- 不新增迁移、Web 页面、真实 QQ 消息或 `/root/xiu2` 部署。

## 停止条件

- 若 `sign` 快照字段语义与结算侧不一致或需要新查询才能判定
  今日签到 → 停止并报告，不扩仓储范围。
- 若既有测试因该插入需要修改断言 → 视为超出只读范围，停止重评。
