from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from ...infrastructure.database import DatabaseUnitOfWork


@dataclass(frozen=True)
class DongfuInfiltrateSuccessResult:
    status: str
    infiltrate_left: int = 0
    intrude_left: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"settled", "duplicate"}


class DongfuInfiltrateSuccessSqlRepository:
    """Atomically persist one successful cave-dwelling infiltration."""

    def __init__(self, game_database: str | Path, player_database: str | Path) -> None:
        self.game_database = str(game_database)
        self.player_database = str(player_database)

    @staticmethod
    def _canonical(value: object) -> str:
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))

    def settle(
        self,
        operation_id: str,
        visitor_id: str,
        target_id: str,
        day: str,
        mode_field: str,
        mode_limit: int,
        target_limit: int,
        expected_slots: str,
        slot_no: int,
        new_finish: str,
        rewards: Iterable[tuple[int, str, str, int]],
        stone: int,
        consume_guard: bool,
        max_goods_num: int,
    ) -> DongfuInfiltrateSuccessResult:
        operation_id = str(operation_id).strip()
        visitor_id, target_id, day, mode_field = map(
            str, (visitor_id, target_id, day, mode_field)
        )
        mode_limit, target_limit, slot_no, stone, max_goods_num = map(
            int, (mode_limit, target_limit, slot_no, stone, max_goods_num)
        )
        consume_guard = int(bool(consume_guard))
        try:
            expected_slots_value = self._canonical(json.loads(expected_slots))
        except (TypeError, ValueError) as exc:
            raise ValueError("expected slots must be valid JSON") from exc
        reward_rows = tuple(
            (int(item_id), str(name), str(item_type), int(amount))
            for item_id, name, item_type, amount in rewards
        )
        new_finish = str(new_finish or "")
        if (
            not operation_id
            or visitor_id == target_id
            or mode_field not in {"infiltrate_active_count", "infiltrate_random_count"}
        ):
            raise ValueError("valid infiltration success operation is required")
        payload = self._canonical(
            (
                visitor_id,
                target_id,
                day,
                mode_field,
                mode_limit,
                target_limit,
                expected_slots_value,
                slot_no,
                new_finish,
                reward_rows,
                stone,
                consume_guard,
                max_goods_num,
            )
        )

        with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
            uow.attach_database(self.player_database, "player_data")
            previous = uow.query_one(
                "SELECT payload,infiltrate_left,intrude_left "
                "FROM dongfu_infiltrate_success_operations WHERE operation_id=?",
                (operation_id,),
            )
            if previous is not None:
                if str(previous["payload"]) != payload:
                    return DongfuInfiltrateSuccessResult("state_changed")
                return DongfuInfiltrateSuccessResult(
                    "duplicate",
                    int(previous["infiltrate_left"]),
                    int(previous["intrude_left"]),
                )

            if uow.query_one(
                "SELECT 1 AS present FROM user_xiuxian WHERE user_id=?", (visitor_id,)
            ) is None:
                return DongfuInfiltrateSuccessResult("state_changed")
            visitor = uow.query_one(
                f"SELECT built,infiltrate_date,{mode_field} "
                "FROM player_data.dongfu_status WHERE user_id=?",
                (visitor_id,),
            )
            target = uow.query_one(
                "SELECT built,intrude_date,intrude_count,patrol_guard,plant_slots "
                "FROM player_data.dongfu_status WHERE user_id=?",
                (target_id,),
            )
            if (
                visitor is None
                or target is None
                or int(visitor["built"] or 0) != 1
                or int(target["built"] or 0) != 1
            ):
                return DongfuInfiltrateSuccessResult("state_changed")
            try:
                slots = json.loads(str(target["plant_slots"] or ""))
            except (TypeError, ValueError):
                return DongfuInfiltrateSuccessResult("state_changed")
            if (
                self._canonical(slots) != expected_slots_value
                or slot_no < 1
                or slot_no > len(slots)
            ):
                return DongfuInfiltrateSuccessResult("state_changed")

            mode_count = int(visitor[mode_field] or 0) if str(visitor["infiltrate_date"] or "") == day else 0
            intrude_count = int(target["intrude_count"] or 0) if str(target["intrude_date"] or "") == day else 0
            if mode_count >= mode_limit or intrude_count >= target_limit:
                return DongfuInfiltrateSuccessResult("daily_limit")

            totals: dict[int, int] = {}
            metadata: dict[int, tuple[str, str]] = {}
            for item_id, name, item_type, amount in reward_rows:
                totals[item_id] = totals.get(item_id, 0) + amount
                metadata[item_id] = (name, item_type)
            for item_id, amount in totals.items():
                item = uow.query_one(
                    "SELECT COALESCE(goods_num,0) AS goods_num FROM back WHERE user_id=? AND goods_id=?",
                    (visitor_id, item_id),
                )
                if (int(item["goods_num"]) if item is not None else 0) + amount > max_goods_num:
                    return DongfuInfiltrateSuccessResult("inventory_full")

            if new_finish:
                slots[slot_no - 1]["plant_finish"] = new_finish
            mode_count += 1
            intrude_count += 1
            uow.execute(
                f"UPDATE player_data.dongfu_status SET infiltrate_date=?,{mode_field}=? WHERE user_id=?",
                (day, mode_count, visitor_id),
            )
            uow.execute(
                "UPDATE player_data.dongfu_status SET intrude_date=?,intrude_count=?,"
                "patrol_guard=MAX(patrol_guard-?,0),plant_slots=? WHERE user_id=?",
                (day, intrude_count, consume_guard, self._canonical(slots), target_id),
            )
            if stone:
                uow.execute(
                    "UPDATE user_xiuxian SET stone=CAST(COALESCE(stone,0) AS REAL)+CAST(? AS REAL) "
                    "WHERE user_id=?",
                    (stone, visitor_id),
                )
            for item_id, amount in totals.items():
                name, item_type = metadata[item_id]
                uow.execute(
                    "INSERT INTO back(user_id,goods_id,goods_name,goods_type,goods_num,create_time,update_time,bind_num) "
                    "VALUES(?,?,?,?,?,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,?) "
                    "ON CONFLICT(user_id,goods_id) DO UPDATE SET goods_name=excluded.goods_name,"
                    "goods_type=excluded.goods_type,goods_num=back.goods_num+excluded.goods_num,"
                    "bind_num=COALESCE(back.bind_num,0)+excluded.bind_num,update_time=excluded.update_time",
                    (visitor_id, item_id, name, item_type, amount, amount),
                )
            infiltrate_left = max(0, mode_limit - mode_count)
            intrude_left = max(0, target_limit - intrude_count)
            uow.execute(
                "INSERT INTO dongfu_infiltrate_success_operations("
                "operation_id,payload,infiltrate_left,intrude_left) VALUES(?,?,?,?)",
                (operation_id, payload, infiltrate_left, intrude_left),
            )
            return DongfuInfiltrateSuccessResult(
                "settled", infiltrate_left, intrude_left
            )


__all__ = ["DongfuInfiltrateSuccessResult", "DongfuInfiltrateSuccessSqlRepository"]
