from __future__ import annotations

from ...core.numeric import as_int_like
import json
from pathlib import Path
from typing import Any

from ...infrastructure.database import DatabaseUnitOfWork
from ...infrastructure.database.attached_uow import AttachedDatabaseUnitOfWork
from .accessory_decompose_domain import AccessoryDecomposeChange


class _DecomposeWriteRejected(Exception):
    def __init__(self, reason: str, subject: str):
        self.reason = reason
        self.subject = subject


class AccessoryDecomposeSqlRepository:
    """Atomically exchange one bag accessory for wash stones."""

    def __init__(self, game_database: str | Path, player_database: str | Path) -> None:
        self.game_database = str(game_database)
        self.player_database = str(player_database)

    @staticmethod
    def _json(value: Any) -> str:
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))

    @staticmethod
    def _decode(value: Any, expected_type: type) -> Any:
        if not value:
            return expected_type()
        decoded = json.loads(value) if isinstance(value, str) else value
        return decoded if isinstance(decoded, expected_type) else expected_type()

    @classmethod
    def _find_bag(cls, bag: list[Any], uid: str) -> tuple[int | None, dict[str, Any] | None]:
        for index, item in enumerate(bag):
            if isinstance(item, dict) and str(item.get("uid", "")) == uid:
                return index, item
        return None, None

    @classmethod
    def _result(cls, status: str, result_json: str) -> AccessoryDecomposeChange:
        value = json.loads(result_json)
        return AccessoryDecomposeChange(
            status=status,
            action=str(value.get("action", "decompose")),
            user_id=str(value.get("user_id", "")),
            affected=int(value.get("affected", 0)),
            stone_delta=as_int_like(value.get("stone_delta", 0)),
            accessory=value.get("accessory"),
        )

    def replay(self, operation_id: str) -> AccessoryDecomposeChange | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        with DatabaseUnitOfWork(self.game_database, read_only=True) as uow:
            row = uow.query_one(
                "SELECT action,result_json FROM accessory_transaction_operations "
                "WHERE operation_id=?",
                (operation_id,),
            )
            if row is None or str(row["action"]) != "decompose":
                return None
            return self._result("duplicate", str(row["result_json"]))

    def replay_batch(self, operation_id: str) -> AccessoryDecomposeChange | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        legacy_prefix = operation_id + ":"
        with DatabaseUnitOfWork(self.game_database, read_only=True) as uow:
            # Older batch ids append the selected UIDs; replay them before reading a new bag.
            rows = uow.query_all(
                "SELECT result_json FROM accessory_transaction_operations WHERE action='batch_decompose' "
                "AND (operation_id=? OR substr(operation_id,1,?)=?) LIMIT 2",
                (operation_id, len(legacy_prefix), legacy_prefix),
            )
            if not rows:
                return None
            if len(rows) != 1:
                return AccessoryDecomposeChange(
                    "failed", "batch_decompose", "", reason="receipt_unreadable", subject=operation_id
                )
            return self._result("duplicate", str(rows[0]["result_json"]))

    @classmethod
    def _add_stones(
        cls,
        uow: AttachedDatabaseUnitOfWork,
        user_id: str,
        stone_id: int,
        stone_name: str,
        amount: int,
        max_goods_num: int,
        subject: str,
    ) -> dict[str, Any] | None:
        stone_query = "SELECT goods_name,goods_type,goods_num,bind_num FROM back WHERE user_id=? AND goods_id=?"
        row = uow.query_one(stone_query, (user_id, stone_id))
        current = int(row["goods_num"] or 0) if row else 0
        bound = int(row["bind_num"] or 0) if row else 0
        if current + amount > max_goods_num:
            return None
        expected_stones = {
            "goods_name": stone_name, "goods_type": "特殊道具", "goods_num": current + amount,
            "bind_num": min(bound + amount, current + amount),
        }
        write = uow.execute(
            "INSERT INTO back (user_id,goods_id,goods_name,goods_type,goods_num,bind_num) "
            "VALUES (?,?,?,?,?,?) ON CONFLICT(user_id,goods_id) DO UPDATE SET "
            "goods_name=excluded.goods_name,goods_type=excluded.goods_type, "
            "goods_num=COALESCE(back.goods_num,0)+excluded.goods_num, "
            "bind_num=MIN(COALESCE(back.bind_num,0)+excluded.goods_num, "
            "COALESCE(back.goods_num,0)+excluded.goods_num)",
            (user_id, stone_id, stone_name, "特殊道具", amount, amount),
        )
        if write.rowcount != 1:
            raise _DecomposeWriteRejected("accessory_batch_decompose_stone_write_rejected", subject)
        if uow.query_one(stone_query, (user_id, stone_id)) != expected_stones:
            raise _DecomposeWriteRejected("accessory_batch_decompose_stone_projection_mismatch", subject)
        return expected_stones

    def decompose(
        self,
        operation_id: str,
        user_id: str,
        uid: str,
        expected_accessory: dict[str, Any],
        stone_id: int,
        stone_name: str,
        stone_gain: int,
        max_goods_num: int,
    ) -> AccessoryDecomposeChange:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        uid = str(uid)
        stone_id = int(stone_id)
        stone_gain = int(stone_gain)
        max_goods_num = int(max_goods_num)
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        if stone_gain <= 0 or max_goods_num <= 0:
            raise ValueError("stone_gain and max_goods_num must be positive")

        payload = {
            "user_id": user_id,
            "uid": uid,
            "expected_accessory": expected_accessory,
            "stone_id": stone_id,
            "stone_gain": stone_gain,
            "max_goods_num": max_goods_num,
        }
        payload_json = self._json(payload)
        receipt_query = (
            "SELECT action,payload,result_json FROM accessory_transaction_operations WHERE operation_id=?"
        )
        state_query = "SELECT equipped,bag FROM player_data.player_accessory WHERE user_id=?"
        stone_query = (
            "SELECT goods_name,goods_type,goods_num,bind_num FROM back WHERE user_id=? AND goods_id=?"
        )
        try:
            with AttachedDatabaseUnitOfWork(
                self.game_database,
                attachments={"player_data": self.player_database},
                immediate=True,
            ) as uow:
                previous = uow.query_one(receipt_query, (operation_id,))
                if previous is not None:
                    if str(previous["action"]) != "decompose" or str(previous["payload"]) != payload_json:
                        return AccessoryDecomposeChange(
                            "state_changed", "decompose", user_id, reason="operation_conflict", subject=uid
                        )
                    return self._result("duplicate", str(previous["result_json"]))

                row = uow.query_one(state_query, (user_id,))
                if row is None:
                    return AccessoryDecomposeChange("accessory_missing", "decompose", user_id, subject=uid)
                equipped = self._decode(row["equipped"], dict)
                bag = self._decode(row["bag"], list)
                index, current = self._find_bag(bag, uid)
                if index is None or current is None:
                    return AccessoryDecomposeChange("accessory_missing", "decompose", user_id, subject=uid)
                if self._json(current) != self._json(expected_accessory):
                    return AccessoryDecomposeChange(
                        "state_changed", "decompose", user_id, reason="accessory_changed", subject=uid
                    )
                stone_row = uow.query_one(stone_query, (user_id, stone_id))
                before_stones = int(stone_row["goods_num"] or 0) if stone_row else 0
                before_bound = int(stone_row["bind_num"] or 0) if stone_row else 0
                if before_stones + stone_gain > max_goods_num:
                    return AccessoryDecomposeChange("inventory_full", "decompose", user_id, subject=uid)
                expected_stones = {
                    "goods_name": stone_name,
                    "goods_type": "特殊道具",
                    "goods_num": before_stones + stone_gain,
                    "bind_num": min(before_bound + stone_gain, before_stones + stone_gain),
                }
                stones = uow.execute(
                    "INSERT INTO back (user_id,goods_id,goods_name,goods_type,goods_num,bind_num) "
                    "VALUES (?,?,?,?,?,?) ON CONFLICT(user_id,goods_id) DO UPDATE SET "
                    "goods_name=excluded.goods_name,goods_type=excluded.goods_type, "
                    "goods_num=COALESCE(back.goods_num,0)+excluded.goods_num, "
                    "bind_num=MIN(COALESCE(back.bind_num,0)+excluded.goods_num, "
                    "COALESCE(back.goods_num,0)+excluded.goods_num)",
                    (user_id, stone_id, stone_name, "特殊道具", stone_gain, stone_gain),
                )
                if stones.rowcount != 1:
                    raise _DecomposeWriteRejected("accessory_decompose_stone_write_rejected", uid)
                if uow.query_one(stone_query, (user_id, stone_id)) != expected_stones:
                    raise _DecomposeWriteRejected("accessory_decompose_stone_projection_mismatch", uid)

                del bag[index]
                expected_state = {
                    "equipped": json.dumps(equipped, ensure_ascii=False),
                    "bag": json.dumps(bag, ensure_ascii=False),
                }
                state = uow.execute(
                    "UPDATE player_data.player_accessory SET equipped=?,bag=? WHERE user_id=?",
                    (expected_state["equipped"], expected_state["bag"], user_id),
                )
                if state.rowcount != 1:
                    raise _DecomposeWriteRejected("accessory_decompose_state_write_rejected", uid)
                if uow.query_one(state_query, (user_id,)) != expected_state:
                    raise _DecomposeWriteRejected("accessory_decompose_state_projection_mismatch", uid)
                result_json = self._json(
                    {
                        "action": "decompose",
                        "user_id": user_id,
                        "affected": 1,
                        "stone_delta": stone_gain,
                        "accessory": current,
                        "details": None,
                    }
                )
                receipt = uow.execute(
                    "INSERT INTO accessory_transaction_operations "
                    "(operation_id,action,payload,result_json) VALUES(?,?,?,?)",
                    (operation_id, "decompose", payload_json, result_json),
                )
                if receipt.rowcount != 1:
                    raise _DecomposeWriteRejected("accessory_decompose_receipt_write_rejected", uid)
                if uow.query_one(receipt_query, (operation_id,)) != {
                    "action": "decompose", "payload": payload_json, "result_json": result_json
                }:
                    raise _DecomposeWriteRejected("accessory_decompose_receipt_projection_mismatch", uid)
                if uow.query_one(stone_query, (user_id, stone_id)) != expected_stones:
                    raise _DecomposeWriteRejected("accessory_decompose_stone_projection_mismatch", uid)
                if uow.query_one(state_query, (user_id,)) != expected_state:
                    raise _DecomposeWriteRejected("accessory_decompose_state_projection_mismatch", uid)
                return AccessoryDecomposeChange(
                    "applied", "decompose", user_id, 1, stone_gain, current, subject=uid
                )
        except _DecomposeWriteRejected as exc:
            return AccessoryDecomposeChange("failed", "decompose", user_id, reason=exc.reason, subject=exc.subject)

    def batch_decompose(
        self,
        operation_id: str,
        user_id: str,
        expected_bag: list[dict[str, Any]],
        selected_uids: tuple[str, ...],
        stone_id: int,
        stone_name: str,
        total_gain: int,
        max_goods_num: int,
    ) -> AccessoryDecomposeChange:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        selected_uids = tuple(str(uid) for uid in selected_uids)
        stone_id = int(stone_id)
        total_gain = int(total_gain)
        max_goods_num = int(max_goods_num)
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        if not selected_uids or total_gain <= 0 or max_goods_num <= 0:
            raise ValueError("selected_uids, total_gain and max_goods_num must be valid")

        payload = {
            "user_id": user_id,
            "expected_bag": expected_bag,
            "selected_uids": selected_uids,
            "stone_id": stone_id,
            "total_gain": total_gain,
            "max_goods_num": max_goods_num,
        }
        payload_json = self._json(payload)
        subject = ",".join(selected_uids)
        receipt_query = "SELECT action,payload,result_json FROM accessory_transaction_operations WHERE operation_id=?"
        state_query = "SELECT equipped,bag FROM player_data.player_accessory WHERE user_id=?"
        stone_query = "SELECT goods_name,goods_type,goods_num,bind_num FROM back WHERE user_id=? AND goods_id=?"
        try:
            with AttachedDatabaseUnitOfWork(
                self.game_database,
                attachments={"player_data": self.player_database},
                immediate=True,
            ) as uow:
                previous = uow.query_one(receipt_query, (operation_id,))
                if previous is not None:
                    if str(previous["action"]) != "batch_decompose" or str(previous["payload"]) != payload_json:
                        return AccessoryDecomposeChange(
                            "state_changed", "batch_decompose", user_id, reason="operation_conflict", subject=subject
                        )
                    return self._result("duplicate", str(previous["result_json"]))

                row = uow.query_one(state_query, (user_id,))
                if row is None:
                    return AccessoryDecomposeChange("accessory_missing", "batch_decompose", user_id, subject=subject)
                equipped = self._decode(row["equipped"], dict)
                bag = self._decode(row["bag"], list)
                if self._json(bag) != self._json(expected_bag):
                    return AccessoryDecomposeChange(
                        "state_changed", "batch_decompose", user_id, reason="accessory_bag_changed", subject=subject
                    )
                selected = set(selected_uids)
                hit = [item for item in bag if isinstance(item, dict) and str(item.get("uid", "")) in selected]
                if len(hit) != len(selected_uids):
                    return AccessoryDecomposeChange("accessory_missing", "batch_decompose", user_id, subject=subject)
                expected_stones = self._add_stones(
                    uow, user_id, stone_id, stone_name, total_gain, max_goods_num, subject
                )
                if expected_stones is None:
                    return AccessoryDecomposeChange("inventory_full", "batch_decompose", user_id, subject=subject)

                bag = [item for item in bag if str(item.get("uid", "")) not in selected]
                expected_state = {
                    "equipped": json.dumps(equipped, ensure_ascii=False),
                    "bag": json.dumps(bag, ensure_ascii=False),
                }
                state = uow.execute(
                    "UPDATE player_data.player_accessory SET equipped=?,bag=? WHERE user_id=?",
                    (expected_state["equipped"], expected_state["bag"], user_id),
                )
                if state.rowcount != 1:
                    raise _DecomposeWriteRejected("accessory_batch_decompose_state_write_rejected", subject)
                if uow.query_one(state_query, (user_id,)) != expected_state:
                    raise _DecomposeWriteRejected("accessory_batch_decompose_state_projection_mismatch", subject)
                result_json = self._json(
                    {
                        "action": "batch_decompose",
                        "user_id": user_id,
                        "affected": len(hit),
                        "stone_delta": total_gain,
                        "accessory": None,
                        "details": None,
                    }
                )
                receipt = uow.execute(
                    "INSERT INTO accessory_transaction_operations "
                    "(operation_id,action,payload,result_json) VALUES(?,?,?,?)",
                    (operation_id, "batch_decompose", payload_json, result_json),
                )
                if receipt.rowcount != 1:
                    raise _DecomposeWriteRejected("accessory_batch_decompose_receipt_write_rejected", subject)
                if uow.query_one(receipt_query, (operation_id,)) != {
                    "action": "batch_decompose", "payload": payload_json, "result_json": result_json
                }:
                    raise _DecomposeWriteRejected("accessory_batch_decompose_receipt_projection_mismatch", subject)
                if uow.query_one(stone_query, (user_id, stone_id)) != expected_stones:
                    raise _DecomposeWriteRejected("accessory_batch_decompose_stone_projection_mismatch", subject)
                if uow.query_one(state_query, (user_id,)) != expected_state:
                    raise _DecomposeWriteRejected("accessory_batch_decompose_state_projection_mismatch", subject)
                return AccessoryDecomposeChange(
                    "applied", "batch_decompose", user_id, len(hit), total_gain, subject=subject
                )
        except _DecomposeWriteRejected as exc:
            return AccessoryDecomposeChange("failed", "batch_decompose", user_id, reason=exc.reason, subject=exc.subject)


__all__ = ["AccessoryDecomposeSqlRepository"]
