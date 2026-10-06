from __future__ import annotations

from pathlib import Path
from typing import Any

from .._service_port import ServicePort
from .collect_exchange_repository import ActivityCollectExchangeSqlRepository
from .boss_settlement_repository import ActivityBossSettlementSqlRepository
from .point_shop_purchase_repository import ActivityPointShopPurchaseSqlRepository
from .sign_settlement_repository import ActivitySignSettlementSqlRepository


class ActivityRepository(ServicePort):
    def __init__(self, database: str | Path) -> None:
        super().__init__("activity", "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_activity", handlers={
            "claim_activity_rewards": self._claim_all,
            "claim_activity_tasks": self._claim_tasks,
            "claim_activity_pass_rewards": self._claim_pass,
            "claim_sign": self._claim_sign,
            "claim_collect_phrase": self._claim_collect,
            "claim_point_shop_item": self._claim_shop,
            "activity_boss.claim_boss_rewards": self._claim_boss,
            "activity_boss.fight_cooperative_boss": self._fight_boss,
            "activity_boss.use_item_on_boss": self._use_boss_item,
            "set_enabled": self._set_enabled,
        })
        self.database = str(database)
        self.sign_settlement = ActivitySignSettlementSqlRepository(database)
        self.collect_exchange = ActivityCollectExchangeSqlRepository(database)
        self.point_shop_purchase = ActivityPointShopPurchaseSqlRepository(database)
        self.boss_settlement = ActivityBossSettlementSqlRepository(database)
        self.retry_started_actions = frozenset({"claim_collect_phrase"})

    def _claim_all(self, **kwargs: Any):
        from ...compatibility.legacy_activity_claim_steps import build_legacy_activity_claim_runners
        from ..activity_reward.claim_all_application import ActivityClaimAllApplication

        user_id = str(kwargs.get("user_id", ""))
        result = ActivityClaimAllApplication(self.database).run(
            str(kwargs.get("operation_id", "")), user_id,
            build_legacy_activity_claim_runners(user_id),
        )
        if result.status == "retryable_failure":
            raise RuntimeError(result.text)
        return result.ok, result.text

    def _claim_tasks(self, **kwargs: Any):
        from ...xiuxian.xiuxian_activity.service import claim_activity_tasks
        return claim_activity_tasks(str(kwargs.get("user_id", "")), str(kwargs.get("query", "")), str(kwargs.get("operation_id", "")))

    def _claim_pass(self, **kwargs: Any):
        from ...xiuxian.xiuxian_activity.service import claim_activity_pass_rewards
        return claim_activity_pass_rewards(str(kwargs.get("user_id", "")), str(kwargs.get("query", "")), str(kwargs.get("operation_id", "")))

    def _claim_sign(self, **kwargs: Any):
        from ...xiuxian.xiuxian_activity.service import claim_sign
        return claim_sign(
            str(kwargs.get("user_id", "")),
            str(kwargs.get("operation_id", "")),
            settlement_repository=self.sign_settlement,
        )

    def _claim_collect(self, **kwargs: Any):
        from ...xiuxian.xiuxian_activity.service import claim_collect_phrase
        return claim_collect_phrase(
            str(kwargs.get("user_id", "")),
            str(kwargs.get("query", "")),
            str(kwargs.get("operation_id", "")),
            settlement_repository=self.collect_exchange,
        )

    def _claim_shop(self, **kwargs: Any):
        from ...xiuxian.xiuxian_activity.service import claim_point_shop_item
        return claim_point_shop_item(
            str(kwargs.get("user_id", "")),
            str(kwargs.get("query", "")),
            str(kwargs.get("operation_id", "")),
            settlement_repository=self.point_shop_purchase,
        )

    def _claim_boss(self, **kwargs: Any):
        from ...xiuxian.xiuxian_activity.activity_boss import claim_boss_rewards
        return claim_boss_rewards(
            str(kwargs.get("user_id", "")),
            str(kwargs.get("query", "")),
            str(kwargs.get("operation_id", "")),
        )

    def _fight_boss(self, **kwargs: Any):
        return self._settle_boss(str(kwargs.get("user_id", "")), "", str(kwargs.get("operation_id", "")))

    def _use_boss_item(self, **kwargs: Any):
        return self._settle_boss(
            str(kwargs.get("user_id", "")), str(kwargs.get("query", "")),
            str(kwargs.get("operation_id", "")),
        )

    @staticmethod
    def _boss_activity(query: str, config: dict[str, Any]):
        from ...xiuxian.xiuxian_activity.activity_config import activity_state
        from ...xiuxian.xiuxian_activity.activity_rules import get_gameplay_activities

        activities = [
            activity for activity in get_gameplay_activities(config)
            if activity.get("type") == "activity_boss"
        ]
        text = str(query or "").strip()
        if text:
            for activity in activities:
                names = {
                    str(activity.get("key") or ""), str(activity.get("name") or ""),
                    str(activity.get("boss_name") or ""), str(activity.get("template_key") or ""),
                }
                if text in names or text in str(activity.get("name") or "") or text in str(activity.get("boss_name") or ""):
                    return activity
            return None
        active = [activity for activity in activities if activity_state(activity)[0]]
        return active[0] if len(active) == 1 else None

    def _settle_boss(self, user_id: str, query: str, operation_id: str):
        from datetime import datetime

        from ...infrastructure.database import DatabaseUnitOfWork
        from ...xiuxian.xiuxian_activity.activity_config import activity_runtime_state, load_config
        from ...xiuxian.xiuxian_activity.activity_views import STAGE_FEATURES
        from ...xiuxian.xiuxian_utils.utils import number_to
        import hashlib

        config = load_config()
        runtime = activity_runtime_state(config)
        if not runtime.get("ok"):
            return False, runtime.get("reason") or "活动未开放"
        if "boss" not in set(runtime.get("features") or ()):
            return False, f"当前阶段【{runtime.get('stage_name', '活动阶段')}】不开放{STAGE_FEATURES.get('boss', '活动首领')}"
        raw = str(query or "").strip()
        item_query = raw.split()[-1] if raw else ""
        boss_query = " ".join(raw.split()[:-1]) if raw else ""
        activity = self._boss_activity(boss_query if raw else "", config)
        item_def = None
        item_mode = bool(raw)
        if item_mode:
            if activity is None:
                return False, "未找到活动首领"
            for item in activity.get("items") or ():
                if item_query in {str(item.get("id")), str(item.get("name"))}:
                    item_def = item
                    break
            if item_def is None:
                return False, f"未找到道具【{item_query}】，请查看活动玩法说明"
            if activity.get("mode") not in {"item_raid", "both"}:
                return False, "该首领不支持道具讨伐，请用活动讨伐"
        else:
            activity = self._boss_activity("", config)
            if activity is None:
                return False, "当前没有可挑战的活动首领，或请指定首领名称"
            if activity.get("mode") not in {"cooperative", "both"}:
                return False, "该首领请使用活动讨伐 道具名"

        key = str(activity["key"])
        now = datetime.now()
        today = now.strftime("%Y-%m-%d")
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            required = {
                "activity_boss_state", "activity_boss_damage", "activity_boss_fight_log",
                "user_xiuxian",
            }
            if item_mode:
                required.add("activity_item_inventory")
            tables = {
                str(row["name"])
                for row in uow.query_all("SELECT name FROM sqlite_master WHERE type='table'")
            }
            missing = sorted(required - tables)
            if missing:
                raise RuntimeError(f"activity_state.001 schema_missing: {', '.join(missing)}")
            columns = {
                str(row["name"]) for row in uow.query_all("PRAGMA table_info(user_xiuxian)")
            }
            selected = ["atk"] if "atk" in columns else []
            if "user_name" in columns:
                selected.append("user_name")
            profile = uow.query_one(
                f"SELECT {','.join(selected) or 'user_id'} FROM user_xiuxian WHERE user_id=?",
                (user_id,),
            ) or {}
            state = uow.query_one(
                "SELECT hp_left,max_hp FROM activity_boss_state WHERE activity_key=?", (key,)
            )
            max_hp = max(1, int(activity.get("max_hp") or 1))
            hp_left = max_hp if state is None else max(0, int(state["hp_left"] or 0))
            stored_max = max_hp if state is None else max(1, int(state["max_hp"] or max_hp))
            if stored_max != max_hp:
                hp_left = int(max_hp * hp_left / stored_max)
            used_row = uow.query_one(
                "SELECT COUNT(*) AS count FROM activity_boss_fight_log "
                "WHERE activity_key=? AND user_id=? AND fight_date=? "
                "AND source IN ('coop','world_boss','item')",
                (key, user_id, today),
            )
            used = max(0, int((used_row or {}).get("count") or 0))
            have = 0
            if item_mode:
                inventory = uow.query_one(
                    "SELECT count FROM activity_item_inventory "
                    "WHERE activity_key=? AND user_id=? AND item_id=?",
                    (key, user_id, str(item_def["id"])),
                )
                have = max(0, int((inventory or {}).get("count") or 0))
            atk = max(0, int(profile.get("atk") or 0))

        multiplier = max(0.0, float(runtime.get("multiplier") or 1.0))
        if item_mode:
            dmin = max(1, int(item_def.get("damage_min") or 100))
            dmax = max(dmin, int(item_def.get("damage_max") or 500))
            span = dmax - dmin + 1
            value = int.from_bytes(hashlib.sha256(operation_id.encode()).digest()[:8], "big")
            damage = max(1, int((dmin + value % span) * multiplier))
        else:
            damage = max(1, int(atk * float(activity.get("atk_ratio") or 0.1) * multiplier))
            cap = max(1, int(max_hp * float(activity.get("hit_hp_cap_ratio") or 0.01)))
            damage = min(damage, cap)
        limit = max(1, int(activity.get("daily_fight_limit") or 3))
        if item_mode:
            result = self.boss_settlement.settle_item(
                operation_id=operation_id, user_id=user_id, activity_key=key,
                item_id=str(item_def["id"]), expected_inventory=have,
                item_cost=max(1, int(item_def.get("cost") or 1)), expected_hp=hp_left,
                expected_max_hp=max_hp, expected_fight_count=used, daily_limit=limit,
                fixed_damage=damage, fight_date=today, timestamp=now.strftime("%Y-%m-%d %H:%M:%S"),
                milestones=activity.get("server_milestones") or (),
            )
        else:
            result = self.boss_settlement.settle_cooperative(
                operation_id=operation_id, user_id=user_id, activity_key=key,
                expected_hp=hp_left, expected_max_hp=max_hp, expected_fight_count=used,
                daily_limit=limit, fixed_damage=damage, fight_date=today,
                timestamp=now.strftime("%Y-%m-%d %H:%M:%S"), milestones=activity.get("server_milestones") or (),
            )
        if result.status == "item_insufficient":
            return False, f"【{item_def['name']}】不足（需要{item_def.get('cost', 1)}，持有{result.inventory or 0}）"
        if result.status == "limit_reached":
            return False, f"今日挑战次数已用完（{limit}次）"
        if result.status == "boss_defeated":
            return False, "首领已被击退" if item_mode else "首领已被全服击破"
        if result.status in {"state_changed", "operation_conflict"}:
            return False, "讨伐未计入：首领状态已更新，请重新讨伐"
        if not result.succeeded:
            return False, "活动首领讨伐失败，请重试"
        name = str(profile.get("user_name") or "").strip() or (f"修士·{user_id[-4:]}" if len(user_id) > 6 else user_id)
        if item_mode:
            text = f"【{activity['boss_name']}】\n{name} 使用【{item_def['name']}】造成 {number_to(result.damage)} 点伤害\n首领剩余 {number_to(result.hp_left)} / {number_to(result.max_hp)}"
        else:
            cap_pct = int(float(activity.get("hit_hp_cap_ratio") or 0.01) * 100)
            pct = 100.0 * result.hp_left / result.max_hp if result.max_hp else 0
            text = "\n".join([
                f"【{activity['boss_name']}】",
                f"{name} 造成伤害 {number_to(result.damage)}（单次上限为首领血量 {cap_pct}%）",
                f"首领剩余 {number_to(result.hp_left)} / {number_to(result.max_hp)}（{pct:.2f}%）",
                f"今日剩余挑战 {max(0, limit - result.fight_count)} 次",
            ])
        if result.hp_left <= 0:
            text += (
                "\n首领已被击退！全服可领取进度奖励。"
                if item_mode
                else "\n首领已被全服击破！可领取全服进度奖励与排行奖励。"
            )
        return True, text

    def _set_enabled(self, **kwargs: Any):
        from ...xiuxian.xiuxian_activity.service import set_enabled
        return set_enabled(
            bool(kwargs.get("enabled")),
            kwargs.get("target"),
            operation_id=str(kwargs.get("operation_id", "")),
            operator_id=str(kwargs.get("operator_id", "")),
        )


__all__ = ["ActivityRepository"]
