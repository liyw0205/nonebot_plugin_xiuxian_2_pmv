from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

from ...infrastructure.clock import SystemClock
from .read_model_repository import ActivityReadModelSqlRepository


class ActivityReadModelApplication:
    def __init__(
        self,
        database: str | Path,
        *,
        repository: ActivityReadModelSqlRepository | None = None,
        config_loader: Callable[[], dict[str, Any]] | None = None,
        clock: Any | None = None,
    ) -> None:
        self.repository = repository or ActivityReadModelSqlRepository(database)
        self.config_loader = config_loader
        self.clock = clock or SystemClock()

    def _config(self) -> dict[str, Any]:
        if self.config_loader is not None:
            return self.config_loader()
        from ...xiuxian.xiuxian_activity.activity_config import load_config

        return load_config()

    @staticmethod
    def _overview_helpers():
        from ...xiuxian.xiuxian_activity.activity_config import (
            _activity_config_key,
            _activity_elapsed_days,
            _activity_info_mode,
            activity_runtime_state,
            activity_state,
        )
        from ...xiuxian.xiuxian_activity.activity_rules import (
            _activity_pass_config,
            get_activity_tasks,
            get_gameplay_activities,
        )
        from ...xiuxian.xiuxian_activity.activity_utils import (
            _as_float,
            _as_int,
            _drop_rate,
        )
        from ...xiuxian.xiuxian_activity.activity_views import (
            ACTIVITY_EVENT_LABELS,
            _activity_event_text,
            _format_activity_task,
            _scope_label,
            _stage_feature_text,
            _stage_time_text,
        )

        return locals()

    def rewards_text(self) -> str:
        from ...xiuxian.xiuxian_activity.activity_utils import _as_int

        config = self._config()
        lines = [f"【{config.get('name', '节日签到活动')} · 奖励】", "", "【每日签到奖励】"]
        rewards = config.get("daily_rewards") or []
        if rewards:
            for reward in sorted(rewards, key=lambda item: _as_int(item.get("day"))):
                day = _as_int(reward.get("day"))
                name = str(reward.get("name") or f"第{day}天")
                lines.append(f"- 第{day}天 {name}：{reward.get('reward') or '暂无奖励'}")
        else:
            lines.append("暂无每日奖励配置")
        milestones = config.get("milestone_rewards") or []
        if milestones:
            lines.extend(("", "【累计签到奖励】"))
            for reward in sorted(milestones, key=lambda item: _as_int(item.get("days"))):
                days = _as_int(reward.get("days"))
                name = str(reward.get("name") or f"累计{days}天")
                lines.append(f"- {name}：{reward.get('reward') or '暂无奖励'}")
        return "\n".join(lines).strip()

    def gameplay_text(self) -> str:
        helpers = self._overview_helpers()
        config = self._config()
        runtime = helpers["activity_runtime_state"]
        state = helpers["activity_state"]
        as_float = helpers["_as_float"]
        as_int = helpers["_as_int"]
        event_text = helpers["_activity_event_text"]
        event_labels = helpers["ACTIVITY_EVENT_LABELS"]
        drop_rate = helpers["_drop_rate"]
        activities = helpers["get_gameplay_activities"](config)

        lines = [f"【{config.get('name', '节日签到活动')} · 玩法】"]
        self._append_stage_summary(
            lines, config, runtime, as_float, helpers, detail=True
        )
        self._append_gameplay_summary(
            lines, activities, state, as_int, as_float, event_text,
            event_labels, drop_rate, detail=True,
        )
        if len(lines) <= 1:
            lines.extend(("", "暂无玩法活动"))
        return "\n".join(lines).strip()

    def activity_info_text(self, user_id: str | None = None) -> str:
        helpers = self._overview_helpers()
        config = self._config()
        activity_key = helpers["_activity_config_key"](config)
        tasks = helpers["get_activity_tasks"](config)
        pass_config = helpers["_activity_pass_config"](config)
        gameplay = helpers["get_gameplay_activities"](config)
        collect_keys = [
            str(activity.get("key") or "")
            for activity in gameplay
            if activity.get("type") == "collect_words"
            and helpers["_as_int"](activity.get("pity_threshold"), 0) > 0
        ]
        now = self.clock.now().astimezone().replace(tzinfo=None)
        state = (
            self.repository.overview_snapshot(
                str(user_id),
                activity_key,
                include_tasks=bool(user_id and tasks),
                include_pass=bool(user_id and pass_config.get("enabled")),
                collect_activity_keys=collect_keys if user_id else [],
            )
            if user_id
            else {
                "sign": {}, "tasks": {}, "pass_total_exp": 0,
                "pass_claimed_levels": set(), "pass_highest_level": 0,
                "collect_pity": {},
            }
        )
        return self._render_activity_info(
            config, str(user_id) if user_id else None, state, now, helpers,
            tasks, pass_config, gameplay,
        )

    @staticmethod
    def _append_stage_summary(
        lines, config, runtime_fn, as_float, helpers, *, detail=False
    ):
        runtime = runtime_fn(config)
        stage = runtime.get("stage") or {}
        lines.extend(("", "【活动阶段】", f"当前：{runtime.get('stage_name') or '未开放'}"))
        if stage:
            time_text = helpers["_stage_time_text"](stage)
            if time_text:
                lines.append(f"阶段时间：{time_text}")
        features = list(runtime.get("features") or [])
        lines.append("开放内容：" + helpers["_stage_feature_text"](features))
        multiplier = as_float(runtime.get("multiplier"), 1.0)
        if detail or abs(multiplier - 1.0) > 0.001:
            lines.append(f"阶段倍率：{multiplier:.2f}x")
        next_stage = runtime.get("next_stage")
        if next_stage:
            time_text = helpers["_stage_time_text"](next_stage)
            lines.append(f"下一阶段：{next_stage.get('name', '活动阶段')}（{time_text}）")
        if not runtime.get("can_produce") and "claim" in set(features):
            lines.append("当前阶段主要用于领奖、兑换和结算，不再产出活动积分、字牌或战令经验。")

    @staticmethod
    def _append_gameplay_summary(
        lines, activities, activity_state_fn, as_int, as_float,
        event_text, event_labels, drop_rate, *, detail,
    ):
        if not activities:
            return
        lines.extend(("", "【玩法活动】"))
        for activity in activities:
            ok, reason = activity_state_fn(activity)
            lines.append(f"- {activity.get('name', '集字活动')}：{'进行中' if ok else reason}")
            if not detail:
                continue
            if activity.get("description"):
                lines.append(f"  {activity.get('description')}")
            activity_type = activity.get("type")
            if activity_type == "event_points":
                point_name = activity.get("point_name") or "活动积分"
                rules = activity.get("event_rules") or []
                if rules:
                    rule_text = "、".join(
                        f"{event_labels.get(rule.get('event'), rule.get('event'))}+{as_int(rule.get('points'))}"
                        for rule in rules
                    )
                    lines.append(f"  积分来源：{rule_text}")
                shop = activity.get("shop") or []
                if shop:
                    shop_text = "、".join(str(item.get("name") or item.get("item_key")) for item in shop)
                    lines.append(f"  {point_name}商店：{shop_text}")
                continue
            if activity_type == "activity_boss":
                mode = activity.get("mode") or "cooperative"
                lines.append(f"  首领：{activity.get('boss_name', '活动首领')}")
                if mode in {"item_raid", "both"}:
                    item_names = "、".join(str(item.get("name")) for item in activity.get("items") or [])
                    lines.append(f"  道具讨伐：{item_names or '未配置'}（随机伤害区间）")
                    lines.append("  命令：活动讨伐 [首领名] 道具名")
                if mode in {"cooperative", "both"}:
                    cap_pct = int(as_float(activity.get("hit_hp_cap_ratio"), 0.01) * 100)
                    attack_pct = int(as_float(activity.get("atk_ratio"), 0.1) * 100)
                    limit = as_int(activity.get("daily_fight_limit"), 3)
                    lines.append(
                        f"  全服协作：攻力×{attack_pct}%，每日{limit}次，单次伤害上限{cap_pct}%血量"
                    )
                    lines.append("  命令：活动讨伐 / 讨伐世界BOSS 也会计入（若开启）")
                lines.append("  活动首领 / 活动首领排行 / 活动首领领奖")
                continue
            labels = event_text(activity.get("drop_events"))
            lines.append(
                f"  掉落来源：{labels or '未配置'}，每日上限："
                f"{as_int(activity.get('daily_drop_limit'), 0)}，"
                f"掉落概率：{int(drop_rate(activity.get('drop_rate'), 0) * 100)}%"
            )
            phrases = activity.get("phrases") or []
            if phrases:
                phrase_text = "、".join(str(item.get("name") or item.get("phrase")) for item in phrases)
                lines.append(f"  兑换词组：{phrase_text}")

    def _render_activity_info(
        self, config, user_id, snapshot, now, helpers, tasks, pass_config, gameplay
    ):
        as_int = helpers["_as_int"]
        as_float = helpers["_as_float"]
        event_text = helpers["_activity_event_text"]
        event_labels = helpers["ACTIVITY_EVENT_LABELS"]
        activity_key = helpers["_activity_config_key"](config)
        ok, reason = helpers["activity_state"](config)
        lines = [
            f"【{config.get('name', '节日签到活动')}】",
            str(config.get("description") or ""),
            f"状态：{'进行中' if ok else reason}",
            f"活动时间：{config.get('start_time', '0')} 至 {config.get('end_time', '无限')}",
            f"签到命令：{config.get('sign_command', '活动签到')}",
        ]
        task_map = snapshot["tasks"]
        total_exp = snapshot["pass_total_exp"]
        level_exp = max(1, as_int(pass_config.get("level_exp"), 100))
        max_level = max(1, as_int(pass_config.get("max_level"), 12))
        pass_level = min(total_exp // level_exp, max_level)
        pass_balance = {
            "exp": level_exp if pass_level >= max_level else max(0, total_exp - pass_level * level_exp),
            "total_exp": total_exp,
            "level": pass_level,
            "level_exp": level_exp,
            "max_level": max_level,
            "claimed_levels": snapshot["pass_claimed_levels"],
            "highest_level": snapshot["pass_highest_level"],
        }
        if user_id:
            sign = snapshot["sign"]
            lines.extend(("", f"我的累计签到：{sign.get('sign_days', 0)} 天", f"上次签到：{sign.get('last_sign_date') or '暂无'}"))

        if helpers["_activity_info_mode"](config) == "full":
            self._append_stage_summary(
                lines, config, helpers["activity_runtime_state"], as_float,
                helpers, detail=True,
            )
            lines.extend(("", "【每日签到奖励】"))
            rewards = config.get("daily_rewards") or []
            if rewards:
                for reward in sorted(rewards, key=lambda item: as_int(item.get("day"))):
                    day = as_int(reward.get("day"))
                    name = str(reward.get("name") or f"第{day}天")
                    lines.append(f"- 第{day}天 {name}：{reward.get('reward') or '暂无奖励'}")
            else:
                lines.append("暂无每日奖励配置")
            milestones = config.get("milestone_rewards") or []
            if milestones:
                lines.extend(("", "【累计签到奖励】"))
                for reward in sorted(milestones, key=lambda item: as_int(item.get("days"))):
                    days = as_int(reward.get("days"))
                    name = str(reward.get("name") or f"累计{days}天")
                    lines.append(f"- {name}：{reward.get('reward') or '暂无奖励'}")
            for field, title in (("daily_tasks", "【每日活动任务】"), ("weekly_tasks", "【周常活动任务】")):
                rows = config.get(field) or []
                if rows:
                    lines.extend(("", title))
                    lines.extend(
                        helpers["_format_activity_task"](task)
                        for task in rows if isinstance(task, dict)
                    )
            self._append_gameplay_summary(
                lines, gameplay, helpers["activity_state"], as_int, as_float,
                event_text, event_labels, helpers["_drop_rate"], detail=True,
            )
            self._append_task_summary(
                lines, tasks, task_map, now, helpers,
                user_id=user_id, detail=True,
            )
            self._append_pass_summary(lines, pass_config, user_id, pass_balance, event_labels, as_int, detail=True)
            if user_id:
                self._append_action_summary(
                    lines, config, tasks, task_map, pass_config, pass_balance,
                    snapshot["collect_pity"], now, helpers,
                )
        else:
            lines.extend((
                "", "查询奖励：活动奖励", "查询任务：活动任务", "领取任务：活动任务领取",
                "活动战令：活动战令 / 活动战令领取", "玩法说明：活动玩法",
                "个人进度：活动排行 / 活动背包 / 活动积分",
            ))
            if user_id:
                self._append_action_summary(
                    lines, config, tasks, task_map, pass_config, pass_balance,
                    snapshot["collect_pity"], now, helpers,
                )
            self._append_stage_summary(
                lines, config, helpers["activity_runtime_state"], as_float,
                helpers,
            )
            self._append_task_summary(
                lines, tasks, task_map, now, helpers,
                user_id=user_id, detail=False,
            )
            self._append_pass_summary(lines, pass_config, user_id, pass_balance, event_labels, as_int)
            self._append_gameplay_summary(
                lines, gameplay, helpers["activity_state"], as_int, as_float,
                event_text, event_labels, helpers["_drop_rate"], detail=False,
            )
        return "\n".join(lines).strip()

    @staticmethod
    def _append_task_summary(lines, tasks, progress_map, now, helpers, *, user_id, detail):
        if not tasks:
            return
        lines.extend(("", "【活动目标】"))
        if not user_id:
            daily_count = len([task for task in tasks if task.get("scope_type") == "daily"])
            weekly_count = len([task for task in tasks if task.get("scope_type") == "weekly"])
            lines.extend((f"每日 {daily_count} 项，周常 {weekly_count} 项", "命令：活动任务 / 活动任务领取"))
            return
        for scope_type in ("daily", "weekly"):
            scope_tasks = [task for task in tasks if task.get("scope_type") == scope_type]
            if not scope_tasks:
                continue
            scope_key = now.strftime("%Y-%m-%d") if scope_type == "daily" else f"{now.isocalendar().year}-W{now.isocalendar().week:02d}"
            finished = 0
            claimable = 0
            for task in scope_tasks:
                row = progress_map.get((scope_type, scope_key, task["key"]), {})
                target = max(1, helpers["_as_int"](task.get("target"), 1))
                progress = min(target, max(0, helpers["_as_int"](row.get("progress"), 0)))
                if row.get("claimed"):
                    finished += 1
                elif progress >= target:
                    claimable += 1
            lines.append(f"{helpers['_scope_label'](scope_type)}：已领 {finished}/{len(scope_tasks)}，可领 {claimable}")
        daily_key = now.strftime("%Y-%m-%d")
        tips = []
        for task in (row for row in tasks if row.get("scope_type") == "daily"):
            state = progress_map.get(("daily", daily_key, task["key"]), {})
            if state.get("claimed"):
                continue
            target = max(1, helpers["_as_int"](task.get("target"), 1))
            progress = min(target, max(0, helpers["_as_int"](state.get("progress"), 0)))
            tips.append(f"{task['name']}可领" if progress >= target else f"{task['name']} {progress}/{target}")
            if len(tips) >= 3:
                break
        if tips:
            lines.append("今日建议：" + "、".join(tips))
        if detail:
            lines.append("查看明细：活动任务")
        lines.append("领奖：活动任务领取")

    @staticmethod
    def _append_pass_summary(lines, pass_config, user_id, balance, event_labels, as_int, *, detail=False):
        if not pass_config.get("enabled"):
            return
        lines.extend(("", f"【{pass_config['name']}】"))
        if user_id:
            lines.append(
                f"等级 {balance['level']}/{balance['max_level']}，"
                f"{pass_config['exp_name']} {balance['exp']}/{balance['level_exp']}"
            )
        else:
            lines.append(f"每 {pass_config['level_exp']}{pass_config['exp_name']} 提升 1 级")
        if detail:
            rules = pass_config.get("event_rules") or []
            if rules:
                text = "、".join(
                    f"{event_labels.get(rule.get('event'), rule.get('event'))}+{as_int(rule.get('exp'))}"
                    for rule in rules
                )
                lines.append(f"来源：{text}")
        lines.append("命令：活动战令 / 活动战令领取")

    @staticmethod
    def _append_action_summary(
        lines, config, tasks, task_map, pass_config, pass_balance, pity_map, now, helpers
    ):
        tips = []
        claimable_tasks = 0
        for task in tasks:
            scope_type = task["scope_type"]
            scope_key = now.strftime("%Y-%m-%d") if scope_type == "daily" else f"{now.isocalendar().year}-W{now.isocalendar().week:02d}"
            state = task_map.get((scope_type, scope_key, task["key"]), {})
            target = max(1, helpers["_as_int"](state.get("target"), task.get("target", 1)))
            if not state.get("claimed") and helpers["_as_int"](state.get("progress"), 0) >= target:
                claimable_tasks += 1
        if claimable_tasks:
            tips.append(f"{claimable_tasks}个任务奖励可领")
        if pass_config.get("enabled"):
            claimable = [
                reward for reward in pass_config.get("level_rewards") or []
                if helpers["_as_int"](reward.get("level"), 0) > 0
                and helpers["_as_int"](reward.get("level"), 0) <= pass_balance["level"]
                and helpers["_as_int"](reward.get("level"), 0) not in pass_balance["claimed_levels"]
            ]
            if claimable:
                tips.append(f"{len(claimable)}档战令奖励可领")
            elapsed = helpers["_activity_elapsed_days"](config, now)
            gap = max(0, pass_balance["highest_level"] - pass_balance["level"])
            multiplier = max(1.0, helpers["_as_float"](pass_config.get("catchup_multiplier"), 1.5))
            if (
                pass_config.get("catchup_enabled")
                and elapsed >= max(1, helpers["_as_int"](pass_config.get("catchup_start_day"), 5))
                and gap >= max(1, helpers["_as_int"](pass_config.get("catchup_level_gap"), 3))
                and multiplier > 1.0
            ):
                tips.append(f"战令追赶{multiplier:.2f}x生效")
        for activity in helpers["get_gameplay_activities"](config):
            if activity.get("type") != "collect_words":
                continue
            threshold = max(0, helpers["_as_int"](activity.get("pity_threshold"), 0))
            if threshold <= 0:
                continue
            for event_key in activity.get("drop_events") or []:
                if pity_map.get((str(activity.get("key") or ""), str(event_key)), 0) >= max(1, threshold - 1):
                    label = helpers["ACTIVITY_EVENT_LABELS"].get(event_key, event_key)
                    tips.append(f"{activity['name']}接近保底：{label}")
                    break
            else:
                continue
            break
        if tips:
            lines.extend(("", "【行动建议】", "；".join(tips[:4]), "一键领取：活动领取"))

    @staticmethod
    def _boss_activities(config: dict[str, Any]) -> list[dict[str, Any]]:
        from ...xiuxian.xiuxian_activity.activity_rules import get_gameplay_activities

        return [
            activity
            for activity in get_gameplay_activities(config)
            if activity.get("type") == "activity_boss"
        ]

    @staticmethod
    def _find_boss(activities: list[dict[str, Any]], query: str) -> dict[str, Any] | None:
        text = str(query or "").strip()
        if text:
            for activity in activities:
                names = {
                    str(activity.get("key") or ""),
                    str(activity.get("name") or ""),
                    str(activity.get("boss_name") or ""),
                    str(activity.get("template_key") or ""),
                }
                if text in names or text in str(activity.get("name") or "") or text in str(activity.get("boss_name") or ""):
                    return activity
            return None
        active = []
        from ...xiuxian.xiuxian_activity.activity_config import activity_state

        for activity in activities:
            ok, _ = activity_state(activity)
            if ok:
                active.append(activity)
        return active[0] if len(active) == 1 else None

    @staticmethod
    def _active_bosses(activities: list[dict[str, Any]]) -> list[dict[str, Any]]:
        from ...xiuxian.xiuxian_activity.activity_config import activity_state

        return [activity for activity in activities if activity_state(activity)[0]]

    @staticmethod
    def _status_block(activity: dict[str, Any], row: dict[str, Any]) -> str:
        from ...xiuxian.xiuxian_utils.utils import number_to

        configured_max = max(1, int(activity.get("max_hp") or 1))
        stored_max = max(1, int(row.get("stored_max_hp") or configured_max))
        hp_left = max(0, int(row.get("hp_left") or configured_max))
        if stored_max != configured_max:
            hp_left = int(configured_max * hp_left / stored_max)
        damage = max(0, int(row.get("damage") or 0))
        used = max(0, int(row.get("fight_count") or 0))
        pct = 100.0 * hp_left / configured_max if configured_max else 0
        lines = [
            f"【{activity.get('boss_name', '活动首领')}】{activity.get('name', '')}",
            str(activity.get("description") or ""),
            f"全服血量 {number_to(hp_left)} / {number_to(configured_max)}（{pct:.2f}%）",
            f"我的累计伤害 {number_to(damage)}",
        ]
        if activity.get("mode") in {"cooperative", "both"}:
            lines.append(f"今日挑战 {used}/{max(1, int(activity.get('daily_fight_limit') or 1))}")
        inventory = row.get("inventory") or {}
        items = [
            f"{item.get('name', item.get('id', '道具'))}x{inventory.get(str(item.get('id')), 0)}"
            for item in activity.get("items") or []
            if inventory.get(str(item.get("id")), 0) > 0
        ]
        if items:
            lines.append("活动道具：" + "、".join(items))
        return "\n".join(lines)

    def boss_status_text(self, user_id: str, query: str = "") -> str:
        activities = self._boss_activities(self._config())
        selected = self._find_boss(activities, query)
        if selected is not None:
            rows = self.repository.boss_snapshot(
                str(user_id), [str(selected["key"])], self.clock.now().strftime("%Y-%m-%d")
            )
            return self._status_block(selected, rows.get(str(selected["key"]), {}))
        active = self._active_bosses(activities)
        if not active:
            return "当前没有进行中的活动首领玩法"
        rows = self.repository.boss_snapshot(
            str(user_id), [str(activity["key"]) for activity in active],
            self.clock.now().strftime("%Y-%m-%d"),
        )
        return "\n\n".join(
            ["【活动首领】"]
            + [
                self._status_block(activity, rows.get(str(activity["key"]), {}))
                for activity in active
            ]
        )

    def boss_rank_text(self, query: str = "", limit: int = 10) -> str:
        activities = self._boss_activities(self._config())
        selected = self._find_boss(activities, query)
        if selected is None:
            active = self._active_bosses(activities)
            selected = active[0] if len(active) == 1 else None
        if selected is None:
            return "请指定活动首领名称后查询排行"
        rows = self.repository.boss_rank(str(selected["key"]), limit)
        lines = [f"【{selected.get('boss_name', '活动首领')}伤害排行】"]
        if not rows:
            lines.append("暂无数据")
            return "\n".join(lines)
        for index, row in enumerate(rows, 1):
            user_id = str(row.get("user_id") or "")
            name = str(row.get("user_name") or "").strip()
            if not name:
                name = f"修士·{user_id[-4:]}" if len(user_id) > 6 else user_id or "无名修士"
            from ...xiuxian.xiuxian_utils.utils import number_to

            lines.append(f"{index}. {name} 伤害 {number_to(max(0, int(row.get('total_damage') or 0)))}")
        return "\n".join(lines)

    def task_progress_text(self, user_id: str) -> str:
        from ...xiuxian.xiuxian_activity.activity_config import _activity_config_key
        from ...xiuxian.xiuxian_activity.activity_rules import (
            _task_scope_key,
            get_activity_tasks,
        )
        from ...xiuxian.xiuxian_activity.activity_utils import _as_int, _clean_text
        from ...xiuxian.xiuxian_activity.activity_views import (
            _activity_event_text,
            _scope_label,
            _task_status_text,
        )

        config = self._config()
        tasks = get_activity_tasks(config)
        lines = [f"【{config.get('name', '节日签到活动')} · 活动任务】"]
        if not tasks:
            lines.append("暂无活动任务")
            return "\n".join(lines)

        now = self.clock.now().astimezone().replace(tzinfo=None)
        activity_key = _activity_config_key(config)
        progress_map = self.repository.task_progress(activity_key, str(user_id))
        for scope_type in ("daily", "weekly"):
            scope_tasks = [task for task in tasks if task.get("scope_type") == scope_type]
            if not scope_tasks:
                continue
            scope_key = _task_scope_key(scope_type, now)
            lines.extend(("", f"【{_scope_label(scope_type)}目标】"))
            for task in scope_tasks:
                state = progress_map.get((scope_type, scope_key, task["key"]), {})
                target = max(1, _as_int(task.get("target"), 1))
                progress = min(target, max(0, _as_int(state.get("progress"), 0)))
                event_text = _activity_event_text(task.get("events"))
                description = _clean_text(task.get("description"))
                if not description:
                    description = (
                        f"{event_text} {target} 次" if event_text else f"目标 {target}"
                    )
                lines.append(
                    f"- {task['name']}："
                    f"{_task_status_text(progress, target, bool(state.get('claimed')))}，"
                    f"{description}，奖励：{task.get('reward') or '暂无奖励'}"
                )
        lines.extend(("", "领奖：活动任务领取（自动领取全部可领任务）"))
        return "\n".join(lines).strip()

    def task_catalog_text(self) -> str:
        from ...xiuxian.xiuxian_activity.activity_rules import _activity_pass_config
        from ...xiuxian.xiuxian_activity.activity_utils import _as_int
        from ...xiuxian.xiuxian_activity.activity_views import (
            ACTIVITY_EVENT_LABELS,
            _format_activity_task,
        )

        config = self._config()
        lines = [f"【{config.get('name', '节日签到活动')} · 任务】"]
        daily_tasks = config.get("daily_tasks") or []
        if daily_tasks:
            lines.extend(("", "【每日活动任务】"))
            lines.extend(
                _format_activity_task(task)
                for task in daily_tasks
                if isinstance(task, dict)
            )
        else:
            lines.extend(("", "暂无每日活动任务"))

        weekly_tasks = config.get("weekly_tasks") or []
        if weekly_tasks:
            lines.extend(("", "【周常活动任务】"))
            lines.extend(
                _format_activity_task(task)
                for task in weekly_tasks
                if isinstance(task, dict)
            )

        pass_config = _activity_pass_config(config)
        if pass_config.get("enabled"):
            lines.extend(("", f"【{pass_config['name']}】"))
            lines.append(
                f"每 {pass_config['level_exp']}{pass_config['exp_name']} 提升 1 级"
            )
            rules = pass_config.get("event_rules") or []
            if rules:
                rule_text = "、".join(
                    f"{ACTIVITY_EVENT_LABELS.get(rule.get('event'), rule.get('event'))}+"
                    f"{_as_int(rule.get('exp'))}"
                    for rule in rules
                )
                lines.append(f"来源：{rule_text}")
            lines.append("命令：活动战令 / 活动战令领取")
        return "\n".join(lines).strip()

    def sign_rank_text(self, limit: int = 10) -> str:
        config = self._config()
        rows = self.repository.sign_rank(limit)
        lines = [f"【{config.get('name', '节日签到活动')}排行】"]
        if not rows:
            lines.append("暂无排行数据")
            return "\n".join(lines)
        for index, row in enumerate(rows, 1):
            lines.append(
                f"{index}. {row['display_name']} 累计签到 "
                f"{max(0, int(row.get('sign_days', 0) or 0))} 天"
            )
        return "\n".join(lines)

    def collect_bag_text(self, user_id: str) -> str:
        from ...xiuxian.xiuxian_activity.activity_config import activity_state
        from ...xiuxian.xiuxian_activity.activity_progress import _phrase_need_counter
        from ...xiuxian.xiuxian_activity.activity_rules import (
            _collect_letters,
            _collect_phrases,
            get_gameplay_activities,
        )
        from ...xiuxian.xiuxian_activity.activity_utils import _as_int
        from ...xiuxian.xiuxian_activity.activity_views import ACTIVITY_EVENT_LABELS

        uid = str(user_id)
        activities = [
            activity
            for activity in get_gameplay_activities(self._config())
            if activity.get("type") == "collect_words"
        ]
        lines = ["【活动背包】"]
        if not activities:
            lines.append("暂无集字活动")
            return "\n".join(lines)

        state = self.repository.collect_state(
            uid, [str(activity["key"]) for activity in activities]
        )
        inventory_state = state["inventory"]
        claim_state = state["claims"]
        pity_state = state["pity"]
        for activity in activities:
            activity_key = str(activity["key"])
            ok, reason = activity_state(activity)
            phrases = _collect_phrases(activity)
            letters = _collect_letters(activity, phrases)
            inventory = {
                str(item.get("char") or ""): inventory_state.get(
                    (activity_key, str(item.get("char") or "")), 0
                )
                for item in letters
            }
            letter_text = "、".join(
                f"{item['char']}x{inventory.get(item['char'], 0)}" for item in letters
            )
            lines.extend(
                [
                    "",
                    f"【{activity['name']}】{'进行中' if ok else reason}",
                    "字牌：" + (letter_text or "暂无"),
                ]
            )
            pity_threshold = max(0, _as_int(activity.get("pity_threshold"), 0))
            if pity_threshold > 0:
                pity_parts = []
                for event_key in activity.get("drop_events") or []:
                    label = ACTIVITY_EVENT_LABELS.get(event_key, event_key)
                    miss_count = pity_state.get((activity_key, str(event_key)), 0)
                    pity_parts.append(
                        f"{label} {min(pity_threshold, miss_count)}/{pity_threshold}"
                    )
                if pity_parts:
                    lines.append("保底进度：" + "、".join(pity_parts))
            if not phrases:
                lines.append("暂无兑换词组")
                continue
            lines.append("可兑换词组：")
            for phrase in phrases:
                need = _phrase_need_counter(phrase["phrase"])
                owned = sum(
                    min(inventory.get(word_char, 0), count)
                    for word_char, count in need.items()
                )
                total_need = sum(need.values())
                claimed = claim_state.get((activity_key, phrase["phrase"]), 0)
                limit = _as_int(phrase.get("limit"), 1)
                limit_text = "不限" if limit <= 0 else f"{claimed}/{limit}"
                lines.append(
                    f"- {phrase['name']}：{owned}/{total_need}，已兑换 {limit_text}，"
                    f"兑换：活动兑换 {phrase['name']}"
                )
        return "\n".join(lines).strip()

    def points_text(self, user_id: str) -> str:
        from ...xiuxian.xiuxian_activity.activity_config import activity_state
        from ...xiuxian.xiuxian_activity.activity_rules import get_gameplay_activities
        from ...xiuxian.xiuxian_activity.activity_utils import _as_int
        from ...xiuxian.xiuxian_activity.activity_views import ACTIVITY_EVENT_LABELS

        activities = [
            activity
            for activity in get_gameplay_activities(self._config())
            if activity.get("type") == "event_points"
        ]
        lines = ["【活动积分】"]
        if not activities:
            lines.append("暂无积分活动")
            return "\n".join(lines)

        balances = self.repository.point_balances(
            str(user_id), [str(activity["key"]) for activity in activities]
        )
        for activity in activities:
            ok, reason = activity_state(activity)
            balance = balances.get(str(activity["key"]), {})
            point_name = activity.get("point_name") or "活动积分"
            lines.extend(
                [
                    "",
                    f"【{activity['name']}】{'进行中' if ok else reason}",
                    f"当前{point_name}：{max(0, _as_int(balance.get('points'), 0))}，"
                    f"累计获得：{max(0, _as_int(balance.get('total_points'), 0))}",
                ]
            )
            rules = activity.get("event_rules") or []
            if rules:
                rule_text = "、".join(
                    f"{ACTIVITY_EVENT_LABELS.get(rule.get('event'), rule.get('event'))}+"
                    f"{_as_int(rule.get('points'))}"
                    for rule in rules
                )
                lines.append(f"积分来源：{rule_text}")
        return "\n".join(lines).strip()

    def point_shop_text(self, user_id: str) -> str:
        from ...xiuxian.xiuxian_activity.activity_config import activity_state
        from ...xiuxian.xiuxian_activity.activity_rules import get_gameplay_activities
        from ...xiuxian.xiuxian_activity.activity_utils import _as_int

        activities = [
            activity
            for activity in get_gameplay_activities(self._config())
            if activity.get("type") == "event_points"
        ]
        lines = ["【活动商店】"]
        if not activities:
            lines.append("暂无积分商店")
            return "\n".join(lines)

        state = self.repository.point_shop_state(
            str(user_id), [str(activity["key"]) for activity in activities]
        )
        for activity in activities:
            activity_key = str(activity["key"])
            ok, reason = activity_state(activity)
            point_name = activity.get("point_name") or "活动积分"
            balance = state["balances"].get(activity_key, 0)
            shop = activity.get("shop") or []
            lines.extend(
                [
                    "",
                    f"【{activity['name']}】{'进行中' if ok else reason}",
                    f"当前{point_name}：{balance}",
                ]
            )
            if not shop:
                lines.append("暂无商店商品")
                continue
            for item in shop:
                item_key = str(item.get("item_key") or "")
                purchase_key = (activity_key, item_key)
                bought = state["purchases"].get(purchase_key, 0)
                limit = _as_int(item.get("limit"), 1)
                limit_text = "不限" if limit <= 0 else f"{bought}/{limit}"
                stock_limit = _as_int(item.get("stock_limit"), 0)
                stock_text = ""
                if stock_limit > 0:
                    sold = state["stock"].get(purchase_key, 0)
                    stock_text = f"，全服库存 {sold}/{stock_limit}"
                lines.append(
                    f"- {item.get('name') or item_key}：{_as_int(item.get('cost'))}{point_name}，"
                    f"已兑换 {limit_text}{stock_text}，奖励：{item.get('reward') or '暂无奖励'}"
                )
        return "\n".join(lines).strip()

    def pass_text(self, user_id: str) -> str:
        from ...xiuxian.xiuxian_activity.activity_config import (
            _activity_config_key,
            _activity_elapsed_days,
        )
        from ...xiuxian.xiuxian_activity.activity_rules import _activity_pass_config
        from ...xiuxian.xiuxian_activity.activity_utils import _as_float, _as_int
        from ...xiuxian.xiuxian_activity.activity_views import ACTIVITY_EVENT_LABELS

        config = self._config()
        pass_config = _activity_pass_config(config)
        lines = [f"【{pass_config['name']}】"]
        if not pass_config.get("enabled"):
            lines.append("活动战令未开启")
            return "\n".join(lines)

        balance = self.repository.pass_summary(
            _activity_config_key(config),
            str(user_id),
            level_exp=pass_config["level_exp"],
            max_level=pass_config["max_level"],
        )
        lines.extend(
            (
                f"等级：{balance['level']}/{balance['max_level']}",
                f"{pass_config['exp_name']}：{balance['exp']}/{balance['level_exp']}"
                f"（累计 {balance['total_exp']}）",
            )
        )

        elapsed_day = _activity_elapsed_days(
            config, self.clock.now().astimezone().replace(tzinfo=None)
        )
        enabled = bool(pass_config.get("catchup_enabled"))
        start_day = max(1, _as_int(pass_config.get("catchup_start_day"), 5))
        level_gap = max(1, _as_int(pass_config.get("catchup_level_gap"), 3))
        multiplier = max(1.0, _as_float(pass_config.get("catchup_multiplier"), 1.5))
        gap = max(0, balance["highest_level"] - balance["level"])
        catchup_active = (
            enabled
            and elapsed_day >= start_day
            and gap >= level_gap
            and multiplier > 1.0
        )
        if enabled:
            if catchup_active:
                lines.append(
                    f"追赶加成：已触发 {multiplier:.2f}x，当前落后最高等级 {gap} 级"
                )
            else:
                lines.append(
                    f"追赶加成：第{start_day}天后、落后{level_gap}级时触发"
                )

        rules = pass_config.get("event_rules") or []
        if rules:
            rule_text = "、".join(
                f"{ACTIVITY_EVENT_LABELS.get(rule.get('event'), rule.get('event'))}+"
                f"{_as_int(rule.get('exp'))}"
                for rule in rules
            )
            lines.append(f"获取来源：{rule_text}")
        rewards = pass_config.get("level_rewards") or []
        if rewards:
            lines.extend(("", "【等级奖励】"))
            for reward in rewards:
                level = _as_int(reward.get("level"), 0)
                if level <= 0:
                    continue
                if level in balance["claimed_levels"]:
                    status = "已领取"
                elif balance["level"] >= level:
                    status = "可领取"
                else:
                    status = "未达成"
                lines.append(
                    f"- Lv.{level} {reward.get('name') or '等级奖励'}：{status}，"
                    f"{reward.get('reward') or '暂无奖励'}"
                )
        lines.extend(("", "领奖：活动战令领取（自动领取全部可领等级奖励）"))
        return "\n".join(lines).strip()


__all__ = ["ActivityReadModelApplication"]
