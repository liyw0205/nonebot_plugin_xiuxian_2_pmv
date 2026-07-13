from __future__ import annotations

import hashlib
import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_compensation.common import get_item_list
from ..xiuxian_config import XiuConfig
from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class BossRewardClaimResult:
    status: str
    names: tuple[str, ...] = ()
    rank: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class BossRewardClaimService:
    def __init__(self, activity_database, game_database, lock=None, max_goods_num=None):
        self.activity_database = Path(activity_database)
        self.game_database = Path(game_database)
        self.lock = lock or RLock()
        self.max_goods_num = int(max_goods_num or XiuConfig().max_goods_num)

    @staticmethod
    def _json(value) -> str:
        return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))

    @staticmethod
    def _ensure_schema(conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS activity_boss_reward_claim_operations("
            "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_json TEXT NOT NULL,"
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )

    @staticmethod
    def _operation_id(kind, payload) -> str:
        return f"activity-boss-{kind}:" + hashlib.sha256(payload.encode()).hexdigest()

    @staticmethod
    def _rewards(rows):
        merged = {}
        for reward_text in rows:
            for item in get_item_list(reward_text) if reward_text.strip() else []:
                if item["type"] == "stone":
                    key = ("stone", 0, "", "")
                else:
                    key = ("item", int(item["id"]), str(item["name"]), str(item["type"]))
                merged[key] = merged.get(key, 0) + int(item["quantity"])
        return tuple((*key, amount) for key, amount in sorted(merged.items()))

    def _grant(self, conn, user_id, rewards):
        if conn.execute("SELECT 1 FROM game_data.user_xiuxian WHERE user_id=%s", (user_id,)).fetchone() is None:
            return "user_missing"
        for kind, item_id, _, _, amount in rewards:
            if kind != "item":
                continue
            row = conn.execute(
                "SELECT COALESCE(goods_num,0) FROM game_data.back WHERE user_id=%s AND goods_id=%s",
                (user_id, item_id),
            ).fetchone()
            if (int(row[0]) if row else 0) + amount > self.max_goods_num:
                return "inventory_full"
        stone = sum(row[4] for row in rewards if row[0] == "stone")
        if stone:
            conn.execute("UPDATE game_data.user_xiuxian SET stone=COALESCE(stone,0)+%s WHERE user_id=%s", (stone, user_id))
        for kind, item_id, name, item_type, amount in rewards:
            if kind != "item":
                continue
            conn.execute(
                "INSERT INTO game_data.back(user_id,goods_id,goods_name,goods_type,goods_num,create_time,update_time,bind_num) "
                "VALUES(%s,%s,%s,%s,%s,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,%s) "
                "ON CONFLICT(user_id,goods_id) DO UPDATE SET goods_num=game_data.back.goods_num+excluded.goods_num,"
                "bind_num=COALESCE(game_data.back.bind_num,0)+excluded.bind_num,update_time=CURRENT_TIMESTAMP",
                (user_id, item_id, name, item_type, amount, amount),
            )

    def _finish(self, conn, operation_id, payload, names, rank=0):
        result = self._json({"names": names, "rank": rank})
        conn.execute(
            "INSERT INTO activity_boss_reward_claim_operations(operation_id,payload,result_json) VALUES(%s,%s,%s)",
            (operation_id, payload, result),
        )
        conn.commit()
        return BossRewardClaimResult("applied", tuple(names), rank)

    def claim_milestones(self, user_id, activity_key, milestones):
        user_id, activity_key = str(user_id), str(activity_key)
        with self.lock, closing(db_backend.connect(self.activity_database)) as conn:
            try:
                conn.execute("ATTACH DATABASE %s AS game_data", (str(self.game_database),))
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                unlocked = {str(row[0]) for row in conn.execute("SELECT milestone_key FROM activity_boss_milestone WHERE activity_key=%s", (activity_key,)).fetchall()}
                if not unlocked:
                    conn.rollback()
                    return BossRewardClaimResult("not_unlocked")
                claimed = {str(row[0]) for row in conn.execute("SELECT milestone_key FROM activity_boss_milestone_claim WHERE activity_key=%s AND user_id=%s", (activity_key, user_id)).fetchall()}
                pending = [(str(row["key"]), str(row.get("name") or row["key"]), str(row.get("reward") or "")) for row in milestones if str(row["key"]) in unlocked and str(row["key"]) not in claimed]
                if not pending:
                    conn.rollback()
                    return BossRewardClaimResult("already_claimed")
                payload = self._json([user_id, activity_key, pending])
                operation_id = self._operation_id("milestone", payload)
                previous = conn.execute("SELECT payload,result_json FROM activity_boss_reward_claim_operations WHERE operation_id=%s", (operation_id,)).fetchone()
                if previous:
                    conn.rollback()
                    data = json.loads(str(previous[1]))
                    return BossRewardClaimResult("duplicate", tuple(data["names"]))
                error = self._grant(conn, user_id, self._rewards([row[2] for row in pending]))
                if error:
                    conn.rollback()
                    return BossRewardClaimResult(error)
                conn.executemany("INSERT INTO activity_boss_milestone_claim(activity_key,user_id,milestone_key,create_time) VALUES(%s,%s,%s,CURRENT_TIMESTAMP)", [(activity_key, user_id, row[0]) for row in pending])
                return self._finish(conn, operation_id, payload, [row[1] for row in pending])
            except Exception:
                conn.rollback()
                raise

    def claim_rank(self, user_id, activity_key, tiers):
        user_id, activity_key = str(user_id), str(activity_key)
        with self.lock, closing(db_backend.connect(self.activity_database)) as conn:
            try:
                conn.execute("ATTACH DATABASE %s AS game_data", (str(self.game_database),))
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                ordered = [str(row[0]) for row in conn.execute("SELECT user_id FROM activity_boss_damage WHERE activity_key=%s ORDER BY total_damage DESC", (activity_key,)).fetchall()]
                if user_id not in ordered:
                    conn.rollback()
                    return BossRewardClaimResult("not_participant")
                rank = ordered.index(user_id) + 1
                tier = next((row for row in tiers if int(row["rank_min"]) <= rank <= int(row["rank_max"])), None)
                if tier is None:
                    conn.rollback()
                    return BossRewardClaimResult("not_eligible", rank=rank)
                tier_key = f"{tier['rank_min']}-{tier['rank_max']}"
                if conn.execute("SELECT 1 FROM activity_boss_rank_claim WHERE activity_key=%s AND user_id=%s AND tier_key=%s", (activity_key, user_id, tier_key)).fetchone():
                    conn.rollback()
                    return BossRewardClaimResult("already_claimed", rank=rank)
                payload = self._json([user_id, activity_key, rank, tier_key, tier.get("reward", "")])
                operation_id = self._operation_id("rank", payload)
                previous = conn.execute("SELECT payload,result_json FROM activity_boss_reward_claim_operations WHERE operation_id=%s", (operation_id,)).fetchone()
                if previous:
                    conn.rollback()
                    data = json.loads(str(previous[1]))
                    return BossRewardClaimResult("duplicate", tuple(data["names"]), int(data["rank"]))
                error = self._grant(conn, user_id, self._rewards([str(tier.get("reward") or "")]))
                if error:
                    conn.rollback()
                    return BossRewardClaimResult(error, rank=rank)
                conn.execute("INSERT INTO activity_boss_rank_claim(activity_key,user_id,tier_key,create_time) VALUES(%s,%s,%s,CURRENT_TIMESTAMP)", (activity_key, user_id, tier_key))
                return self._finish(conn, operation_id, payload, [str(tier.get("name") or "排行奖励")], rank)
            except Exception:
                conn.rollback()
                raise
