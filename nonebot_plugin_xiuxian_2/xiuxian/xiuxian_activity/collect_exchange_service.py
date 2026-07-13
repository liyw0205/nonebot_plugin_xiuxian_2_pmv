from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class ActivityCollectExchangeResult:
    status: str
    claim_count: int = 0
    missing: tuple[tuple[str, int], ...] = ()
    rewards: tuple[str, ...] = ()

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class ActivityCollectExchangeService:
    """Exchange collect-word tokens and grant rewards in one transaction."""

    def __init__(
        self,
        activity_database: str | Path,
        game_database: str | Path,
        lock: RLock | None = None,
    ) -> None:
        self._activity_database = Path(activity_database)
        self._game_database = Path(game_database)
        self._lock = lock or RLock()

    @staticmethod
    def _reward_rows(rewards) -> tuple[int, tuple[tuple[int, str, str, int], ...], tuple[str, ...]]:
        stone = 0
        items: dict[int, list] = {}
        descriptions: list[str] = []
        for reward in rewards:
            quantity = int(reward["quantity"])
            if quantity <= 0:
                raise ValueError("reward quantity must be positive")
            descriptions.append(str(reward.get("desc") or f"获得 {reward.get('name', '')}x{quantity}"))
            if str(reward["type"]) == "stone":
                stone += quantity
                continue
            item_id = int(reward["id"])
            item_type = str(reward["type"])
            if item_type in {"辅修功法", "神通", "功法", "身法", "瞳术"}:
                item_type = "技能"
            elif item_type in {"法器", "防具"}:
                item_type = "装备"
            metadata = [str(reward["name"]), item_type]
            if item_id in items and items[item_id][:2] != metadata:
                raise ValueError("conflicting reward metadata")
            items.setdefault(item_id, metadata + [0])[2] += quantity
        return stone, tuple(
            (item_id, values[0], values[1], values[2])
            for item_id, values in sorted(items.items())
        ), tuple(descriptions)

    def exchange(
        self,
        operation_id,
        user_id,
        activity_key,
        phrase,
        required_tokens,
        limit,
        rewards,
        max_goods_num,
    ) -> ActivityCollectExchangeResult:
        operation_id = str(operation_id).strip()
        user_id, activity_key, phrase = map(str, (user_id, activity_key, phrase))
        limit, max_goods_num = int(limit), int(max_goods_num)
        token_rows = tuple(sorted(
            (str(word_char), int(quantity))
            for word_char, quantity in dict(required_tokens).items()
        ))
        if (
            not operation_id
            or not activity_key
            or not phrase
            or not token_rows
            or any(not word_char or quantity <= 0 for word_char, quantity in token_rows)
            or limit < 0
            or max_goods_num < 0
        ):
            raise ValueError("valid collect exchange is required")

        stone, item_rows, reward_descriptions = self._reward_rows(rewards)
        payload = json.dumps(
            [user_id, activity_key, phrase, token_rows, limit, stone, item_rows, max_goods_num],
            ensure_ascii=True,
            separators=(",", ":"),
        )

        with self._lock, closing(db_backend.connect(self._activity_database)) as conn:
            try:
                conn.execute("ATTACH DATABASE %s AS game_data", (str(self._game_database),))
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS activity_collect_exchange_operations("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_json TEXT NOT NULL,"
                    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                previous = conn.execute(
                    "SELECT payload,result_json FROM activity_collect_exchange_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return ActivityCollectExchangeResult("operation_conflict")
                    previous_result = json.loads(previous[1])
                    return ActivityCollectExchangeResult(
                        "duplicate",
                        int(previous_result[0]),
                        rewards=tuple(previous_result[1]),
                    )

                if conn.execute(
                    "SELECT 1 FROM game_data.user_xiuxian WHERE user_id=%s", (user_id,)
                ).fetchone() is None:
                    conn.rollback()
                    return ActivityCollectExchangeResult("user_missing")

                claim_row = conn.execute(
                    "SELECT COALESCE(count,0) FROM activity_collect_claim "
                    "WHERE activity_key=%s AND user_id=%s AND phrase=%s",
                    (activity_key, user_id, phrase),
                ).fetchone()
                claim_count = int(claim_row[0]) if claim_row else 0
                if limit > 0 and claim_count >= limit:
                    conn.rollback()
                    return ActivityCollectExchangeResult("limit_reached", claim_count)

                missing: list[tuple[str, int]] = []
                for word_char, quantity in token_rows:
                    inventory = conn.execute(
                        "SELECT COALESCE(count,0) FROM activity_collect_inventory "
                        "WHERE activity_key=%s AND user_id=%s AND word_char=%s",
                        (activity_key, user_id, word_char),
                    ).fetchone()
                    owned = int(inventory[0]) if inventory else 0
                    if owned < quantity:
                        missing.append((word_char, quantity - owned))
                if missing:
                    conn.rollback()
                    return ActivityCollectExchangeResult("tokens_insufficient", claim_count, tuple(missing))

                for item_id, _, _, quantity in item_rows:
                    inventory = conn.execute(
                        "SELECT COALESCE(goods_num,0) FROM game_data.back "
                        "WHERE user_id=%s AND goods_id=%s",
                        (user_id, item_id),
                    ).fetchone()
                    if (int(inventory[0]) if inventory else 0) + quantity > max_goods_num:
                        conn.rollback()
                        return ActivityCollectExchangeResult("inventory_full", claim_count)

                now = datetime.now()
                for word_char, quantity in token_rows:
                    changed = conn.execute(
                        "UPDATE activity_collect_inventory SET count=count-%s,update_time=%s "
                        "WHERE activity_key=%s AND user_id=%s AND word_char=%s AND count>=%s",
                        (quantity, now, activity_key, user_id, word_char, quantity),
                    )
                    if changed.rowcount != 1:
                        raise RuntimeError("collect inventory state changed")
                conn.execute(
                    "INSERT INTO activity_collect_claim(activity_key,user_id,phrase,count,update_time) "
                    "VALUES(%s,%s,%s,1,%s) ON CONFLICT(activity_key,user_id,phrase) DO UPDATE SET "
                    "count=activity_collect_claim.count+1,update_time=excluded.update_time",
                    (activity_key, user_id, phrase, now),
                )
                claim_count += 1

                if stone:
                    conn.execute(
                        "UPDATE game_data.user_xiuxian SET stone=COALESCE(stone,0)+%s WHERE user_id=%s",
                        (stone, user_id),
                    )
                for item_id, name, item_type, quantity in item_rows:
                    conn.execute(
                        "INSERT INTO game_data.back(user_id,goods_id,goods_name,goods_type,goods_num,"
                        "create_time,update_time,bind_num) VALUES(%s,%s,%s,%s,%s,%s,%s,%s) "
                        "ON CONFLICT(user_id,goods_id) DO UPDATE SET goods_name=excluded.goods_name,"
                        "goods_type=excluded.goods_type,goods_num=back.goods_num+excluded.goods_num,"
                        "bind_num=COALESCE(back.bind_num,0)+excluded.bind_num,update_time=excluded.update_time",
                        (user_id, item_id, name, item_type, quantity, now, now, quantity),
                    )

                result_json = json.dumps(
                    [claim_count, reward_descriptions], ensure_ascii=True, separators=(",", ":")
                )
                conn.execute(
                    "INSERT INTO activity_collect_exchange_operations(operation_id,payload,result_json) "
                    "VALUES(%s,%s,%s)",
                    (operation_id, payload, result_json),
                )
                conn.commit()
                return ActivityCollectExchangeResult(
                    "applied", claim_count, rewards=reward_descriptions
                )
            except Exception:
                conn.rollback()
                raise


__all__ = ["ActivityCollectExchangeResult", "ActivityCollectExchangeService"]
