from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Mapping

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

    def _sign_reward_lines(self, config: Mapping[str, Any]) -> list[str]:
        """Configured sign reward ladders, shared by the catalog and 奖励 view."""
        from ...xiuxian.xiuxian_activity.activity_utils import _as_int

        lines = ["", "【每日签到奖励】"]
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
        return lines

    def rewards_text(self) -> str:
        config = self._config()
        lines = [f"【{config.get('name', '节日签到活动')} · 奖励】"]
        lines.extend(self._sign_reward_lines(config))
        return "\n".join(lines).strip()

    def reward_catalog_text(self, user_id: str = "") -> str:
        """Every configured reward source plus the player's own claim state.

        ``rewards_text`` stays byte-identical with the legacy config-only
        ``build_activity_rewards_text`` (parity-tested), so the personal catalog
        lives here: one read-only snapshot, then each configured ladder is
        annotated with 已领取 / 可领取 / 未达成.
        """
        from ...xiuxian.xiuxian_activity.activity_config import activity_rank_config
        from ...xiuxian.xiuxian_activity.activity_rules import (
            _activity_pass_config,
            _task_scope_key,
            get_activity_tasks,
            get_gameplay_activities,
        )
        from ...xiuxian.xiuxian_activity.activity_utils import _as_int, _clean_text
        from ...xiuxian.xiuxian_activity.activity_views import _format_activity_task
        from .center import ENDED, _boss_milestone_keys
        from .gameplay_progress import claimable_pass_rewards, task_claimable
        from .rank import position_text, rewards_for_board, tier_for_position
        from .stage import stage_timeline, stage_window_bounds

        config = self._config()
        uid = str(user_id or "")
        now = self.clock.now().astimezone().replace(tzinfo=None)
        lines = [f"【{config.get('name', '节日签到活动')} · 奖励】"]
        lines.extend(self._sign_reward_lines(config))

        snapshot: dict[str, Any] = {}
        if uid:
            activity_key = self._overview_helpers()["_activity_config_key"](config)
            activities = get_gameplay_activities(config)
            collect_keys = [
                str(activity.get("key") or "")
                for activity in activities
                if activity.get("type") == "collect_words"
                and _as_int(activity.get("pity_threshold"), 0) > 0
            ]
            point_keys = [
                str(activity.get("key") or "")
                for activity in activities
                if activity.get("type") == "event_points"
            ]
            stage_windows = [
                (stage["key"], *stage_window_bounds(stage))
                for stage in stage_timeline(config, now)["ended"]
            ]
            board_sources = [
                str(board.get("source") or "")
                for board in activity_rank_config(config)["boards"]
                if str(board.get("source") or "")
            ]
            boss_keys = [
                str(activity.get("key") or "")
                for activity in activities
                if activity.get("type") == "activity_boss" and str(activity.get("key") or "")
            ]
            if boss_keys:
                board_sources.append("boss")
            snapshot = self.repository.overview_snapshot(
                uid,
                activity_key,
                include_tasks=bool(get_activity_tasks(config)),
                include_pass=bool(_activity_pass_config(config).get("enabled")),
                collect_activity_keys=collect_keys,
                point_activity_keys=point_keys,
                stage_windows=stage_windows,
                board_sources=board_sources,
                boss_activity_keys=boss_keys,
                now=now,
            )

        def mark(claimed: bool, claimable: bool) -> str:
            if not uid:
                return ""
            if claimed:
                return "（已领取）"
            return "（可领取）" if claimable else "（未达成）"

        # 签到：本人累计天数与已达成档位。
        if uid:
            sign = snapshot.get("sign") or {}
            days = _as_int(sign.get("total_sign_days"), 0)
            milestones = [
                row for row in config.get("milestone_rewards") or [] if isinstance(row, Mapping)
            ]
            achieved = sum(1 for row in milestones if _as_int(row.get("days"), 0) <= days)
            lines.append(f"我的累计签到 {days} 天，已达成 {achieved}/{len(milestones)} 档")

        # 任务奖励。
        tasks = snapshot.get("tasks") or {}
        for scope_type, rows in (
            ("daily", config.get("daily_tasks") or []),
            ("weekly", config.get("weekly_tasks") or []),
        ):
            tasks_rows = [row for row in rows if isinstance(row, Mapping)]
            if not tasks_rows:
                continue
            scope_key = _task_scope_key(scope_type, now)
            lines.extend(("", f"【{'每日' if scope_type == 'daily' else '周常'}任务奖励】"))
            for task in tasks_rows:
                state = tasks.get((scope_type, scope_key, _clean_text(task.get("key"))), {})
                status = mark(
                    bool(state.get("claimed")), task_claimable(state, task.get("target"))
                )
                lines.append(f"{_format_activity_task(dict(task))}{status}")

        # 战令奖励。
        pass_config = _activity_pass_config(config)
        levels = [row for row in pass_config.get("level_rewards") or [] if isinstance(row, Mapping)]
        if levels:
            claimed_levels = set(snapshot.get("pass_claimed_levels") or ())
            claimable_levels = {
                _as_int(level, 0)
                for level in claimable_pass_rewards(
                    pass_config,
                    snapshot.get("pass_total_exp"),
                    claimed_levels,
                )
            }
            lines.extend(("", "【战令奖励】"))
            for row in sorted(levels, key=lambda item: _as_int(item.get("level"), 0)):
                level = _as_int(row.get("level"), 0)
                name = str(row.get("name") or f"Lv{level}")
                status = mark(
                    level in claimed_levels, level in claimable_levels
                )
                lines.append(f"- Lv{level} {name}：{row.get('reward') or '暂无奖励'}{status}")

        # 阶段奖励。
        stages = [
            stage for stage in stage_timeline(config, now)["stages"] if stage["reward_rows"]
        ]
        if stages:
            participated = set(snapshot.get("stage_participated") or ())
            claimed_stages = set(snapshot.get("stage_claimed") or ())
            lines.extend(("", "【阶段奖励】"))
            for stage in stages:
                status = mark(
                    stage["key"] in claimed_stages,
                    stage["status"] == ENDED
                    and stage["key"] in participated
                    and stage["key"] not in claimed_stages,
                )
                for row in stage["reward_rows"]:
                    lines.append(f"- {stage['name']} {row['name']}：{row['reward']}{status}")

        # 排行奖励。
        rank_config = activity_rank_config(config)
        rank_rewards = rank_config.get("rewards") or []
        positions = snapshot.get("board_positions") or {}
        claimed_ranks = set(snapshot.get("rank_claimed") or ())
        if rank_rewards:
            lines.extend(("", "【排行奖励】"))
            for board in rank_config.get("boards") or []:
                tiers = rewards_for_board(rank_rewards, board)
                if not tiers:
                    continue
                source = str(board.get("source") or "")
                board_key = str(board.get("key") or source)
                board_label = str(board.get("label") or board.get("name") or board_key)
                position = _as_int(positions.get(source), 0)
                matched = tier_for_position(tiers, position)
                for tier in tiers:
                    tier_key = str(tier.get("key") or "")
                    low = _as_int(tier.get("rank_min"), 0)
                    high = _as_int(tier.get("rank_max"), 0)
                    status = mark(
                        (board_key, tier_key) in claimed_ranks,
                        matched is not None
                        and str(matched.get("key") or "") == tier_key,
                    )
                    lines.append(
                        f"- {board_label} 第{low}-{high}名：{tier.get('reward') or '暂无奖励'}{status}"
                    )
                if uid:
                    lines.append(f"  我的名次：{position_text(position, 0)}")

        # 首领进度（全服）与首领排行奖励。
        from .center import _boss_claim_state_by_activity

        boss_claim_states = _boss_claim_state_by_activity(snapshot)
        milestone_keys = _boss_milestone_keys(config)
        for activity in get_gameplay_activities(config):
            if activity.get("type") != "activity_boss":
                continue
            label = str(activity.get("name") or activity.get("key") or "活动首领")
            activity_key = str(activity.get("key") or "")
            claim_state = boss_claim_states.get(activity_key)
            if claim_state is None and "" in boss_claim_states:
                # Flat single-activity snapshot: fall back to the shared state.
                claim_state = boss_claim_states[""]
            unlocked, claimed_milestones, claimed_boss_ranks = (
                claim_state if claim_state is not None else (set(), set(), set())
            )
            milestones = [
                row for row in activity.get("server_milestones") or [] if isinstance(row, Mapping)
            ]
            if milestones:
                lines.extend(("", f"【{label} · 进度奖励】"))
                for row in milestones:
                    key = str(
                        row.get("key") or f"p{int(float(row.get('hp_percent') or 0))}"
                    )
                    if key not in milestone_keys:
                        continue
                    name = str(row.get("name") or key)
                    hp = row.get("hp_percent")
                    status = mark(
                        key in claimed_milestones,
                        key in unlocked and key not in claimed_milestones,
                    )
                    lines.append(f"- {name}（血量≤{hp}%）：{row.get('reward') or '暂无奖励'}{status}")
            tiers = [
                row for row in activity.get("rank_rewards") or [] if isinstance(row, Mapping)
            ]
            if tiers:
                lines.extend(("", f"【{label} · 排行奖励】"))
                # Each boss activity is judged by its own rank position; the
                # shared "boss" position is only the single-boss fallback.
                boss_positions = snapshot.get("boss_board_positions") or {}
                if activity_key and activity_key in boss_positions:
                    position = _as_int(boss_positions.get(activity_key), 0)
                else:
                    position = _as_int(positions.get("boss"), 0)
                matched = tier_for_position(tiers, position)
                for row in tiers:
                    low = _as_int(row.get("rank_min"), 0)
                    high = _as_int(row.get("rank_max"), 0)
                    tier_key = f"{low}-{high}"
                    status = mark(
                        tier_key in claimed_boss_ranks,
                        matched is not None
                        and _as_int(matched.get("rank_min"), 0) == low
                        and _as_int(matched.get("rank_max"), 0) == high,
                    )
                    lines.append(
                        f"- 第{low}-{high}名：{row.get('reward') or '暂无奖励'}{status}"
                    )
                if uid:
                    lines.append(f"  我的名次：{position_text(position, 0)}")

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
            self._overview_snapshot_for_detail(str(user_id), config, helpers, now)
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

    def _overview_snapshot_for_detail(
        self, user_id: str, config: dict[str, Any], helpers: Mapping[str, Any], now: datetime
    ) -> dict[str, Any]:
        """Detail-view snapshot: overview state plus every claim source the
        one-click claim covers, so the action summary matches the center card."""
        from ...xiuxian.xiuxian_activity.activity_config import activity_rank_config
        from ...xiuxian.xiuxian_activity.activity_utils import _as_int
        from .stage import stage_timeline, stage_window_bounds

        activity_key = helpers["_activity_config_key"](config)
        tasks = helpers["get_activity_tasks"](config)
        pass_config = helpers["_activity_pass_config"](config)
        gameplay = helpers["get_gameplay_activities"](config)
        collect_keys = [
            str(activity.get("key") or "")
            for activity in gameplay
            if activity.get("type") == "collect_words"
            and _as_int(activity.get("pity_threshold"), 0) > 0
        ]
        stage_windows = [
            (stage["key"], *stage_window_bounds(stage))
            for stage in stage_timeline(config, now)["ended"]
        ]
        board_sources = [
            str(board.get("source") or "")
            for board in activity_rank_config(config)["boards"]
            if str(board.get("source") or "")
        ]
        boss_keys = [
            str(activity.get("key") or "")
            for activity in gameplay
            if activity.get("type") == "activity_boss"
            and str(activity.get("key") or "")
        ]
        if boss_keys:
            board_sources.append("boss")
        return self.repository.overview_snapshot(
            user_id,
            activity_key,
            include_tasks=bool(tasks),
            include_pass=bool(pass_config.get("enabled")),
            collect_activity_keys=collect_keys,
            stage_windows=stage_windows,
            board_sources=board_sources,
            boss_activity_keys=boss_keys,
            now=now,
        )

    def activity_center_text(self, user_id: str | None = None) -> str:
        """Render the small activity-center view through the shared message path."""
        from ...xiuxian.xiuxian_activity.activity_rules import (
            _activity_pass_config,
            get_activity_tasks,
            get_gameplay_activities,
        )
        from ...xiuxian.xiuxian_activity.activity_utils import _as_int
        from ...xiuxian.xiuxian_utils.message_markdown import build_md_command_link
        from .center import (
            build_activity_center_items,
            claimable_reward_count,
            personal_progress_summary,
        )

        config = self._config()
        tasks = get_activity_tasks(config)
        pass_config = _activity_pass_config(config)
        now = self.clock.now().astimezone().replace(tzinfo=None)
        snapshot = {
            "tasks": {},
            "pass_total_exp": 0,
            "pass_claimed_levels": set(),
        }
        if user_id:
            activity_key = self._overview_helpers()["_activity_config_key"](config)
            collect_keys = [
                str(activity.get("key") or "")
                for activity in get_gameplay_activities(config)
                if activity.get("type") == "collect_words"
                and _as_int(activity.get("pity_threshold"), 0) > 0
            ]
            point_keys = [
                str(activity.get("key") or "")
                for activity in get_gameplay_activities(config)
                if activity.get("type") == "event_points"
            ]
            # Claim state the center needs so 奖励领取 tracks every source the
            # one-click claim covers, not just tasks and the battle pass.
            from ...xiuxian.xiuxian_activity.activity_config import activity_rank_config
            from .stage import stage_timeline, stage_window_bounds

            stage_windows = [
                (stage["key"], *stage_window_bounds(stage))
                for stage in stage_timeline(config, now)["ended"]
            ]
            board_sources = [
                str(board.get("source") or "")
                for board in activity_rank_config(config)["boards"]
                if str(board.get("source") or "")
            ]
            boss_keys = [
                str(activity.get("key") or "")
                for activity in get_gameplay_activities(config)
                if activity.get("type") == "activity_boss"
                and str(activity.get("key") or "")
            ]
            if boss_keys:
                board_sources.append("boss")
            snapshot = self.repository.overview_snapshot(
                str(user_id),
                activity_key,
                include_tasks=bool(tasks),
                include_pass=bool(pass_config.get("enabled")),
                collect_activity_keys=collect_keys,
                point_activity_keys=point_keys,
                stage_windows=stage_windows,
                board_sources=board_sources,
                boss_activity_keys=boss_keys,
                now=now,
            )
        claimable = bool(user_id and claimable_reward_count(config, snapshot, now))
        lines = ["**活动中心**"]
        for item in build_activity_center_items(
            config, now, claimable=claimable, snapshot=snapshot if user_id else None
        ):
            lines.extend(("", f"**{item.name}** · {item.status}"))
            if item.description:
                lines.append(item.description)
            if item.gameplay_summary:
                lines.append(f"玩法：{item.gameplay_summary}")
            if item.time_hint:
                lines.append(item.time_hint)
            lines.append(f"奖励：{item.reward_hint}")
            if user_id:
                lines.append(personal_progress_summary(config, snapshot, now))
            links = " | ".join(
                build_md_command_link(label, command)
                for label, command in item.commands
            )
            if links:
                lines.append(links)
            # One compact link row per gameplay activity: every configured
            # entry stays reachable without building a single unbounded row.
            for group_name, group_entries in item.gameplay_entry_groups:
                group_links = " | ".join(
                    build_md_command_link(label, command)
                    for label, command in group_entries
                )
                if group_links:
                    lines.append(f"{group_name}：{group_links}")
        return "\n".join(lines).strip()

    def activity_stage_text(self, user_id: str | None = None) -> str:
        """Read-only stage view: current stage, next stage, claimable rewards.

        The stage window is the activity's own time progression; the player
        only ever sees stages the config declares, and claim eligibility comes
        from their own participation evidence.
        """
        from ...xiuxian.xiuxian_activity.activity_config import (
            STAGE_TYPE_LABELS,
            _activity_config_key,
            activity_state,
        )
        from ...xiuxian.xiuxian_activity.activity_views import _stage_feature_text
        from .stage import claimable_stage_rows, stage_timeline, stage_window_bounds
        from .stage_participation import ActivityStageParticipationRepository

        config = self._config()
        now = self.clock.now().astimezone().replace(tzinfo=None)
        lines = ["**活动阶段**"]
        ok, reason = activity_state(config)
        if not ok:
            lines.append(reason)
            return "\n".join(lines).strip()

        timeline = stage_timeline(config, now)
        active = timeline["active"]
        if active is None:
            lines.append("当前没有进行中的活动阶段")
        else:
            stage_type = STAGE_TYPE_LABELS.get(
                active["stage_type"], active["stage_type"]
            )
            lines.append(f"{active['name']}（{stage_type}）")
            lines.append(f"阶段时间：{active['start_time']} 至 {active['end_time']}")
            if active["remaining_text"]:
                lines.append(active["remaining_text"])
            lines.append("开放内容：" + _stage_feature_text(list(active["features"])))
            lines.append(f"阶段倍率：{float(active['multiplier']):.2f}x")
        next_stage = timeline["next"]
        if next_stage is not None:
            lines.append(f"下一阶段：{next_stage['name']}（{next_stage['start_time']} 后开启）")

        if user_id:
            activity_key = _activity_config_key(config)
            windows = [
                (stage["key"], *stage_window_bounds(stage))
                for stage in timeline["ended"]
            ]
            participated = ActivityStageParticipationRepository(
                getattr(self.repository, "database", "")
            ).participated_stages(str(user_id), activity_key, windows)
            claimed = self.repository.stage_claims(str(user_id), activity_key)
            rows = claimable_stage_rows(config, now, participated, claimed)
            if rows:
                names = "、".join(row["name"] for row in rows)
                lines.append(f"可领阶段奖励：{names}｜活动阶段领取")
            else:
                lines.append("可领阶段奖励：暂无")
            self._append_stage_timeline(
                lines, timeline, participated=participated, claimed=claimed,
            )
        else:
            self._append_stage_timeline(lines, timeline)
        return "\n".join(lines).strip()

    @staticmethod
    def _append_stage_timeline(
        lines,
        timeline: dict,
        *,
        participated: set[str] | None = None,
        claimed: set[str] | None = None,
    ) -> None:
        """Full stage schedule: every configured stage with its own state.

        Personal claim marks render only for a resolved player; ended stages
        without configured rewards carry no claim marker.
        """
        stages = timeline.get("stages") or []
        if not stages:
            return
        lines.extend(("", "【阶段日程】"))
        for stage in stages:
            status = str(stage.get("status") or "")
            marker = {"进行中": "▶", "已结束": "✓", "未开始": "…"}.get(status, "·")
            row = f"- {marker} {stage['name']}（{status}）"
            window = f"{stage.get('start_time', '')} 至 {stage.get('end_time', '')}"
            row += f" {window}"
            remaining = str(stage.get("remaining_text") or "")
            if remaining:
                row += f" {remaining}"
            if stage.get("reward_rows"):
                key = str(stage.get("key") or "")
                if participated is None or claimed is None:
                    row += " 奖励：已配置"
                elif key in claimed:
                    row += " 奖励：已领取"
                elif key in (participated or set()):
                    row += " 奖励：可领取｜活动阶段领取"
                else:
                    row += " 奖励：未领取"
            lines.append(row)

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
                    snapshot["collect_pity"], now, helpers, snapshot=snapshot,
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
                    snapshot["collect_pity"], now, helpers, snapshot=snapshot,
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
        lines, config, tasks, task_map, pass_config, pass_balance, pity_map, now, helpers,
        snapshot=None,
    ):
        from .center import detail_action_tips

        if snapshot is None:
            snapshot = {
                "tasks": task_map,
                "pass_total_exp": pass_balance["total_exp"],
                "pass_claimed_levels": pass_balance["claimed_levels"],
                "pass_highest_level": pass_balance["highest_level"],
                "collect_pity": pity_map,
            }
        tips = detail_action_tips(config, snapshot, now, helpers)
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
        milestones = activity.get("server_milestones") or []
        if milestones:
            unlocked = set(row.get("milestones_unlocked") or ())
            claimed = set(row.get("milestones_claimed") or ())
            lines.append("全服进度奖励：")
            pending = False
            for milestone in milestones:
                key = str(milestone.get("key") or "")
                name = str(milestone.get("name") or "全服进度奖励")
                percent = float(milestone.get("hp_percent") or 0)
                reward = str(milestone.get("reward") or "")
                if key and key in claimed:
                    state = "已领取"
                elif key and key in unlocked:
                    state = "可领取"
                    pending = True
                else:
                    state = "未解锁"
                lines.append(f"- {name}（血量≤{percent:g}%）{reward}｜{state}")
            if pending:
                lines.append("可用 活动首领领奖 进度 领取已解锁的进度奖励")
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

    def rank_overview_text(self, user_id: str = "") -> str:
        from ...xiuxian.xiuxian_activity.activity_config import (
            _activity_config_key,
            activity_rank_config,
            activity_state,
        )
        from .rank import position_text, value_text

        config = self._config()
        rank_config = activity_rank_config(config)
        boards = rank_config["boards"]
        lines = [f"【{config.get('name', '节日签到活动')} · 排行榜】"]
        if not boards:
            lines.append("暂无排行榜配置")
            return "\n".join(lines).strip()
        ok, reason = activity_state(config)
        if not ok:
            lines.append(reason)
        activity_key = _activity_config_key(config)
        uid = str(user_id or "")
        for board in boards:
            source = str(board.get("source") or "")
            label = str(board.get("label") or board.get("key") or source)
            total = self.repository.board_total(source, activity_key)
            position, value = (
                (0, 0)
                if not uid
                else self.repository.board_position(source, activity_key, uid)
            )
            lines.append(
                f"{label}：{position_text(position, total)}（{value_text(source, value)}）"
            )
        if rank_config["rewards"]:
            lines.append("发送 活动排行 <榜单> 查看榜单，活动排行领取 领取排行奖励")
        else:
            lines.append("发送 活动排行 <榜单> 查看榜单")
        return "\n".join(lines).strip()

    def rank_board_text(self, user_id: str = "", query: str = "") -> str:
        from ...xiuxian.xiuxian_activity.activity_config import (
            _activity_config_key,
            activity_rank_config,
            activity_state,
        )
        from .rank import (
            board_by_query,
            board_labels,
            display_name,
            position_text,
            rewards_for_board,
            tier_for_position,
            value_text,
        )

        config = self._config()
        rank_config = activity_rank_config(config)
        boards = rank_config["boards"]
        if not boards:
            return f"【{config.get('name', '节日签到活动')} · 排行榜】\n暂无排行榜配置"
        target = str(query or "").strip()
        board = board_by_query(boards, target) if target else dict(boards[0])
        if board is None:
            return f"未找到榜单【{target}】\n可用榜单：{board_labels(boards)}"
        source = str(board.get("source") or "")
        label = str(board.get("label") or board.get("key") or source)
        limit = max(1, min(int(board.get("limit") or 10), 100))
        activity_key = _activity_config_key(config)
        rows = self.repository.board_rank(source, activity_key, limit)
        lines = [f"【{config.get('name', '节日签到活动')} · {label}】"]
        ok, reason = activity_state(config)
        if not ok:
            lines.append(reason)
        if not rows:
            lines.append("暂无排行数据")
        else:
            for index, row in enumerate(rows, 1):
                lines.append(
                    f"{index}. "
                    f"{display_name(str(row.get('user_id') or ''), str(row.get('user_name') or ''))} "
                    f"{value_text(source, int(row.get('value') or 0))}"
                )
        uid = str(user_id or "")
        total = self.repository.board_total(source, activity_key)
        position, value = (
            (0, 0)
            if not uid
            else self.repository.board_position(source, activity_key, uid)
        )
        lines.append(
            f"我的名次：{position_text(position, total)}（{value_text(source, value)}）"
        )
        tiers = rewards_for_board(rank_config["rewards"], board)
        if tiers:
            tier = tier_for_position(tiers, position)
            if tier is not None:
                lines.append(f"可领取：{tier.get('name')}（{tier.get('reward')}）")
                lines.append("发送 活动排行领取 领取排行奖励")
            else:
                lines.append("当前名次没有可领取的排行奖励")
        elif rank_config["rewards"]:
            lines.append("该榜单未配置排行奖励")
        return "\n".join(lines).strip()

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
        from .gameplay_progress import collect_pity_entries

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
            pity_entries = collect_pity_entries(activity, pity_state)
            if pity_entries:
                lines.append(
                    "保底进度："
                    + "、".join(
                        f"{label} {miss}/{threshold}"
                        for label, miss, threshold in pity_entries
                    )
                )
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
                    f"{ACTIVITY_EVENT_LABELS.get(str(rule.get('event')), str(rule.get('event')))}+"
                    f"{_as_int(rule.get('points'))}"
                    for rule in rules
                    if isinstance(rule, dict)
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

    def point_shop_guidance_text(self, user_id: str) -> str:
        """Shop view with per-item purchase guidance and terminal states.

        Sibling of the parity-pinned ``point_shop_text``: same data path, plus
        per-item `购买：活动购买 {商品名}` guidance, `已兑换完`/`已售罄`
        terminal states, and a purchasable-count summary line.
        """
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
        purchasable = 0
        example_name = ""
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
                item_name = str(item.get("name") or item_key)
                purchase_key = (activity_key, item_key)
                bought = state["purchases"].get(purchase_key, 0)
                limit = _as_int(item.get("limit"), 1)
                stock_limit = _as_int(item.get("stock_limit"), 0)
                sold = (
                    state["stock"].get(purchase_key, 0) if stock_limit > 0 else 0
                )
                status_text = ""
                guidance = f"，购买：活动购买 {item_name}"
                if limit > 0 and bought >= limit:
                    status_text = f"，已兑换完（{bought}/{limit}）"
                    guidance = ""
                elif stock_limit > 0 and sold >= stock_limit:
                    status_text = "，已售罄"
                    guidance = ""
                else:
                    purchasable += 1
                    if not example_name:
                        example_name = item_name
                limit_text = "不限" if limit <= 0 else f"{bought}/{limit}"
                stock_text = (
                    f"，全服库存 {sold}/{stock_limit}" if stock_limit > 0 else ""
                )
                lines.append(
                    f"- {item_name}：{_as_int(item.get('cost'))}{point_name}，"
                    f"已兑换 {limit_text}{stock_text}{status_text}，"
                    f"奖励：{item.get('reward') or '暂无奖励'}{guidance}"
                )
        if purchasable:
            lines.append(
                f"可购商品 {purchasable} 件，购买：活动购买 {example_name}"
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
