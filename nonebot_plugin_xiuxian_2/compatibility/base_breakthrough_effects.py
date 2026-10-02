from __future__ import annotations

import json
import sys
from pathlib import Path

from ..core.numeric import as_int_like
from ..features.base.breakthrough_relations import DirectBreakthroughRelations
from ..features.game_events.statistics import GameEventStatisticsRepository
from ..infrastructure.database import DatabaseUnitOfWork
from .buff_closing_effects import log_closing_event_once


def direct_breakthrough_root_rate(row):
    from ..xiuxian.xiuxian_utils.data_source import jsondata
    from ..xiuxian.xiuxian_utils.numeric_bind import compute_fate_root_rate

    roots = jsondata.root_data()
    root_type = row["root_type"]
    if root_type == "命运道果":
        return compute_fate_root_rate(row.get("root_level", 0), roots["永恒道果"]["type_speeds"], roots[root_type]["type_speeds"])
    return roots[root_type]["type_speeds"]


def _power_for(row, exp):
    from ..xiuxian.xiuxian_utils.data_source import jsondata

    return round(as_int_like(exp) * direct_breakthrough_root_rate(row) * jsondata.level_data()[row["level"]]["spend"], 0)


def _cap_for(row):
    from ..xiuxian.xiuxian_config import XiuConfig
    from ..xiuxian.xiuxian_utils.data_source import jsondata

    config = XiuConfig()
    index = config.level.index(row["level"])
    threshold = 0.001 if index == len(config.level) - 1 else jsondata.level_data()[config.level[index + 1]]["power"]
    return int(threshold) * config.closing_exp_upper_limit


def _refresh_titles(user_id):
    from ..xiuxian.xiuxian_title.title_data import check_and_unlock_titles

    check_and_unlock_titles(user_id)


class LegacyDirectBreakthroughEffects:
    def __init__(self, game_database, player_database, *, players_dir=None, power_for=None, cap_for=None, refresh_titles=None):
        from ..paths import get_paths

        self.game_database = Path(game_database)
        self.player_database = Path(player_database)
        self.players_dir = Path(players_dir) if players_dir is not None else get_paths().players
        self.statistics = GameEventStatisticsRepository(player_database)
        self.cap_for = cap_for or _cap_for
        self.refresh_titles = refresh_titles or _refresh_titles
        self.relations = DirectBreakthroughRelations(
            game_database, player_database, power_for=power_for or _power_for, cap_for=self.cap_for,
        )

    def plan_relations(self, game, user, new_level, occurred_at):
        from ..xiuxian.xiuxian_buff.partner import (
            MENTOR_BREAKTHROUGH_REWARD_LIMIT, MENTOR_HISTORY_LIMIT,
            _mentor_breakthrough_reward_rate, runtime_random,
        )
        from ..xiuxian.xiuxian_utils.numeric_bind import percent_exp_reward
        from ..xiuxian.xiuxian_utils.utils import number_to

        if not self.player_database.is_file():
            raise RuntimeError("direct breakthrough player database unavailable")
        source_id = str(user["user_id"])
        plans = []
        with DatabaseUnitOfWork(self.player_database, read_only=True) as player:
            pair = player.query_one("SELECT * FROM partner WHERE user_id=?", (source_id,)) or {}
            target_id = str(pair.get("partner_id") or "")
            reciprocal = player.query_one("SELECT * FROM partner WHERE user_id=?", (target_id,)) or {}
            target = game.query_one("SELECT * FROM user_xiuxian WHERE user_id=? ORDER BY rowid LIMIT 1", (target_id,))
            if target and target_id != source_id and str(reciprocal.get("partner_id")) == source_id:
                amount = min(
                    percent_exp_reward(as_int_like(user["exp"]), 0.01, new_level, apply_rank_suppress=False, anchor="gap"),
                    int(as_int_like(target["exp"]) * 0.1),
                )
                if amount > 0 and runtime_random.randint(1, 100) <= min(40 + as_int_like(pair.get("affection")) // 1000, 50):
                    plans.append({
                        "kind": "partner", "source_id": source_id, "target_id": target_id,
                        "bind_time": str(pair.get("bind_time") or ""),
                        "target_bind_time": str(reciprocal.get("bind_time") or ""),
                        "reward_exp": amount, "occurred_at": occurred_at,
                        "source_description": f"突破{new_level}，道侣共享修为：{number_to(amount)}",
                        "target_description": f"道侣突破{new_level}，获得共享修为：{number_to(amount)}",
                        "message": f"\n道侣{target['user_name']}感受到你的突破，获得{number_to(amount)}修为！",
                    })
            apprentice = player.query_one("SELECT * FROM mentor WHERE user_id=?", (source_id,)) or {}
            mentor_id = str(apprentice.get("mentor_id") or "")
            mentor = player.query_one("SELECT * FROM mentor WHERE user_id=?", (mentor_id,)) or {}
            target = game.query_one("SELECT * FROM user_xiuxian WHERE user_id=? ORDER BY rowid LIMIT 1", (mentor_id,))
            children = json.loads(mentor.get("apprentice_ids") or "[]")
            count = as_int_like(apprentice.get("breakthrough_reward_count"))
            if target and mentor_id != source_id and source_id in [str(v) for v in children] and 0 <= count < MENTOR_BREAKTHROUGH_REWARD_LIMIT:
                depends_on_partner = bool(plans and plans[0]["target_id"] == mentor_id)
                exp = as_int_like(target["exp"])
                if depends_on_partner:
                    exp += plans[0]["reward_exp"]
                cap = int(self.cap_for(target))
                amount = min(int(exp * _mentor_breakthrough_reward_rate(new_level)), cap - exp)
                if amount > 0:
                    plans.append({
                        "kind": "mentor", "source_id": source_id, "target_id": mentor_id,
                        "bind_time": str(apprentice.get("bind_time") or ""),
                        "reward_exp": amount, "max_exp": cap, "occurred_at": occurred_at,
                        "reward_limit": MENTOR_BREAKTHROUGH_REWARD_LIMIT, "history_limit": MENTOR_HISTORY_LIMIT,
                        "depends_on_partner": depends_on_partner,
                        "source_description": f"突破{new_level}，师父{target['user_name']}获得返修{number_to(amount)}",
                        "target_description": f"徒弟{user['user_name']}突破{new_level}，获得返修{number_to(amount)}",
                        "message": f"\n师父{target['user_name']}因徒弟突破{new_level}，获得{number_to(amount)}修为返修！",
                    })
        return plans

    def _log(self, user_id, message, event_id, occurred_at):
        log_closing_event_once(
            user_id=user_id, message=message, event_id=event_id, occurred_at=occurred_at,
            player_database=self.player_database, players_dir=self.players_dir,
            resolve_impersonation=False,
        )

    def on_settled(self, *, payload, event_id):
        user_id, occurred_at = str(payload["user_id"]), payload["occurred_at"]
        if not self.player_database.is_file():
            raise RuntimeError("direct breakthrough player database unavailable")
        self.statistics.record(
            event_id=event_id, user_id=user_id, increments=payload["statistics"], occurred_at=occurred_at,
        )
        legacy = sys.modules.get("nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.utils")
        invalidate = getattr(legacy, "invalidate_player_data_cache", None)
        if callable(invalidate):
            invalidate("statistics", payload["statistics"])
        self._log(user_id, payload["log_message"], f"{event_id}:log", occurred_at)
        partner_applied = False
        messages = []
        for intent in payload["relations"]:
            kind = intent["kind"]
            relation_id = f"{event_id}:{kind}"
            reward = self.relations.settle(
                payload["operation_id"], relation_id, intent,
                skip=bool(intent.get("depends_on_partner") and not partner_applied),
            )
            if reward is None:
                continue
            if kind == "partner":
                partner_applied = True
            else:
                if callable(invalidate):
                    invalidate("statistics", ("师父突破返修", "徒弟突破回馈"))
                    invalidate("mentor", ("breakthrough_reward_count", "mentor_history"))
            suffix = f"（{reward['reward_count']}/{reward['reward_limit']}）" if kind == "mentor" else ""
            prefix = "[师徒] " if kind == "mentor" else ""
            for side in ("source", "target"):
                self._log(reward[f"{side}_id"], prefix + reward[f"{side}_description"] + suffix, f"{relation_id}:{side}:log", occurred_at)
            messages.append(reward["message"])
        self.refresh_titles(user_id)
        return "".join(messages)


__all__ = ["LegacyDirectBreakthroughEffects"]
