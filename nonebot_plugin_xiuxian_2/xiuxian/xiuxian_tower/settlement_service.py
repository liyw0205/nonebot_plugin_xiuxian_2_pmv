from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class TowerSettlementResult:
    status: str
    score: int
    stone: int
    exp: int
    floor: int = 0
    challenge_succeeded: bool = True
    rewards: tuple = ()
    stamina_cost: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class TowerSettlementService:
    """Commit one tower-floor result and all rewards across both databases."""

    def __init__(self, game_database: str | Path, player_database: str | Path, lock: RLock | None = None) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    @staticmethod
    def _payload(user_id, floor, stamina_cost) -> str:
        # Request identity only — tower/player snapshots and rewards are outcomes/concurrency checks.
        return json.dumps([str(user_id), int(floor), int(stamina_cost)], ensure_ascii=True, separators=(",", ":"))

    def _ensure_ops_table(self, conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS tower_settlement_operations ("
            "operation_id TEXT PRIMARY KEY, payload TEXT NOT NULL, "
            "result_json TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )
        cols = {str(r[1]) for r in conn.execute("PRAGMA table_info(tower_settlement_operations)").fetchall()}
        if "result_json" not in cols:
            try:
                conn.execute("ALTER TABLE tower_settlement_operations ADD COLUMN result_json TEXT")
            except Exception:
                pass

    def _result_from_row(self, status: str, payload: str, result_json: str | None) -> TowerSettlementResult:
        floor = 0
        stamina_cost = 0
        try:
            body = json.loads(payload or "[]")
            if isinstance(body, list) and len(body) >= 3:
                floor = int(body[1] or 0)
                stamina_cost = int(body[2] or 0)
        except Exception:
            pass
        score = stone = exp = 0
        challenge_succeeded = True
        rewards: tuple = ()
        if result_json:
            try:
                data = json.loads(result_json)
                score = int(data.get("score") or 0)
                stone = int(data.get("stone") or 0)
                exp = int(data.get("exp") or 0)
                challenge_succeeded = bool(data.get("challenge_succeeded", True))
                rewards = tuple(tuple(x) for x in (data.get("rewards") or []))
                floor = int(data.get("floor") or floor)
                stamina_cost = int(data.get("stamina_cost") or stamina_cost)
            except Exception:
                pass
        return TowerSettlementResult(
            status,
            score,
            stone,
            exp,
            floor=floor,
            challenge_succeeded=challenge_succeeded,
            rewards=rewards,
            stamina_cost=stamina_cost,
        )

    def get_result(self, operation_id: str) -> TowerSettlementResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            self._ensure_ops_table(conn)
            row = conn.execute(
                "SELECT payload, result_json FROM tower_settlement_operations WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if row is None:
                return None
            return self._result_from_row("duplicate", str(row[0] or ""), row[1])

    def settle(
        self,
        operation_id,
        user_id,
        expected_tower,
        floor,
        score,
        stone,
        exp,
        items,
        max_goods_num,
        *,
        expected_player=None,
        final_hp=None,
        final_mp=None,
        stamina_cost=0,
        challenge_succeeded=True,
    ) -> TowerSettlementResult:
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        expected = {key: int(dict(expected_tower)[key]) for key in ("current_floor", "max_floor", "score")}
        floor, score, stone, exp, max_goods_num, stamina_cost = map(
            int, (floor, score, stone, exp, max_goods_num, stamina_cost)
        )
        player = None if expected_player is None else {key: int(dict(expected_player)[key]) for key in ("hp", "mp", "user_stamina")}
        final_hp = None if final_hp is None else max(1, int(final_hp))
        final_mp = None if final_mp is None else max(1, int(final_mp))
        challenge_succeeded = bool(challenge_succeeded)
        rewards = tuple(
            (int(item["id"]), str(item["name"]), str(item["type"]), int(item["amount"]))
            for item in items
            if int(item.get("amount", 0)) > 0
        )
        if not operation_id or floor <= 0 or min(score, stone, exp, max_goods_num, *expected.values()) < 0:
            raise ValueError("valid operation, tower state and rewards are required")
        payload = self._payload(user_id, floor, stamina_cost)
        result_json = json.dumps(
            {
                "score": score,
                "stone": stone,
                "exp": exp,
                "floor": floor,
                "stamina_cost": stamina_cost,
                "challenge_succeeded": challenge_succeeded,
                "rewards": [list(r) for r in rewards],
            },
            ensure_ascii=True,
            separators=(",", ":"),
        )

        def result(status: str) -> TowerSettlementResult:
            ok = status in {"applied", "duplicate"}
            return TowerSettlementResult(
                status,
                score if ok else 0,
                stone if ok else 0,
                exp if ok else 0,
                floor=floor if ok else 0,
                challenge_succeeded=challenge_succeeded if ok else True,
                rewards=rewards if ok else (),
                stamina_cost=stamina_cost if ok else 0,
            )

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_ops_table(conn)
                previous = conn.execute(
                    "SELECT payload, result_json FROM tower_settlement_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return result("state_changed")
                    return self._result_from_row("duplicate", str(previous[0] or ""), previous[1])
                user = conn.execute(
                    "SELECT hp, mp, user_stamina FROM user_xiuxian WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if user is None:
                    conn.rollback()
                    return result("user_missing")
                if player is not None and tuple(map(int, user)) != (player["hp"], player["mp"], player["user_stamina"]):
                    conn.rollback()
                    return result("state_changed")
                if player is not None and player["user_stamina"] < stamina_cost:
                    conn.rollback()
                    return result("stamina_insufficient")
                columns = {str(column[1]) for column in conn.execute("PRAGMA player_data.table_info(tower)").fetchall()}
                if not {"current_floor", "max_floor", "score"}.issubset(columns):
                    conn.rollback()
                    return result("state_changed")
                tower = conn.execute(
                    "SELECT current_floor, max_floor, score FROM player_data.tower WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if tower is None or tuple(map(int, tower)) != (
                    expected["current_floor"],
                    expected["max_floor"],
                    expected["score"],
                ):
                    conn.rollback()
                    return result("state_changed")
                for item_id, _, _, amount in rewards:
                    inventory = conn.execute(
                        "SELECT COALESCE(goods_num, 0) FROM back WHERE user_id=%s AND goods_id=%s",
                        (user_id, item_id),
                    ).fetchone()
                    if (int(inventory[0]) if inventory else 0) + amount > max_goods_num:
                        conn.rollback()
                        return result("inventory_full")
                if challenge_succeeded:
                    new_max_floor = max(expected["max_floor"], floor)
                    if (
                        conn.execute(
                            "UPDATE player_data.tower SET current_floor=%s, max_floor=%s, score=%s WHERE user_id=%s",
                            (floor, new_max_floor, expected["score"] + score, user_id),
                        ).rowcount
                        != 1
                    ):
                        conn.rollback()
                        return result("state_changed")
                if player is None:
                    conn.execute(
                        "UPDATE user_xiuxian SET stone=stone+%s, exp=exp+%s WHERE user_id=%s",
                        (stone, exp, user_id),
                    )
                else:
                    conn.execute(
                        "UPDATE user_xiuxian SET stone=stone+%s, exp=exp+%s, hp=%s, mp=%s, user_stamina=user_stamina-%s WHERE user_id=%s",
                        (stone, exp, final_hp, final_mp, stamina_cost, user_id),
                    )
                now = datetime.now()
                for item_id, name, item_type, amount in rewards:
                    conn.execute(
                        "INSERT INTO back (user_id,goods_id,goods_name,goods_type,goods_num,create_time,update_time,bind_num) "
                        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(user_id,goods_id) DO UPDATE SET "
                        "goods_num=back.goods_num+EXCLUDED.goods_num, bind_num=COALESCE(back.bind_num,0)+EXCLUDED.goods_num, "
                        "update_time=EXCLUDED.update_time",
                        (user_id, item_id, name, item_type, amount, now, now, amount),
                    )
                conn.execute(
                    "INSERT INTO tower_settlement_operations(operation_id,payload,result_json) VALUES (%s,%s,%s)",
                    (operation_id, payload, result_json),
                )
                conn.commit()
                return result("applied")
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    conn.execute("DETACH DATABASE player_data")


__all__ = ["TowerSettlementResult", "TowerSettlementService"]
