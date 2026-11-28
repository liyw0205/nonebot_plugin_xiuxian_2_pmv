from __future__ import annotations

from ...core.numeric import bind_sqlite_param, exact_crystal_integer, require_exact_integer
from ...core.errors import DomainError

import json
import math
from pathlib import Path

from ...infrastructure.database import DatabaseUnitOfWork
from .operation_schema import SCHEMA_MISSING, stone_training_ready


def _training_power(exp: int, multiplier: float) -> int:
    rate = float(multiplier)
    if not math.isfinite(rate) or rate < 0:
        raise ValueError("invalid stone training power multiplier")
    if exp <= 2 ** 53:
        product = exp * rate
        if math.isfinite(product) and product <= 2 ** 53:
            # Preserve SQLite ROUND(x,0), including the original binary product.
            return int(product if product > 2 ** 52 else product + .5)
    numerator, denominator = rate.as_integer_ratio()
    return (2 * exp * numerator + denominator) // (2 * denominator)


def _write_rejected(reason):
    return DomainError("stone_training_write_rejected", reason,
                       {"reason": "stone_training_" + reason, "subject": "actor"})


class StoneTrainingResult:
    def __init__(self, status: str, exp_gain: int = 0, stone_cost: int = 0, power: int = 0, reason: str = "") -> None:
        self.status, self.exp_gain, self.stone_cost, self.power = status, exp_gain, stone_cost, power
        self.reason = reason


class StoneTrainingSqlRepository:
    def __init__(self, game_database: str | Path, player_database: str | Path) -> None:
        self.game_database, self.player_database = str(game_database), str(player_database)

    def settle(self, operation_id: str, user_id: str, *, requested_stone: int, expected_exp: int, expected_stone: int, exp_cap: int, power_multiplier: float) -> StoneTrainingResult:
        requested_stone, expected_exp, expected_stone, exp_cap = map(int, (requested_stone, expected_exp, expected_stone, exp_cap))
        if not operation_id or requested_stone <= 0 or min(expected_exp, expected_stone, exp_cap) < 0:
            raise ValueError("invalid stone training settlement arguments")
        possible_exp = requested_stone // 10
        exp_gain = min(possible_exp, max(0, exp_cap - expected_exp))
        stone_cost = exp_gain * 10 if possible_exp >= max(0, exp_cap - expected_exp) else requested_stone
        payload = json.dumps([user_id, requested_stone, exp_cap, float(power_multiplier)], separators=(",", ":"))
        with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
            if not stone_training_ready(uow):
                return StoneTrainingResult(SCHEMA_MISSING)
            previous = uow.query_one("SELECT payload,result_json FROM stone_training_operations WHERE operation_id=?", (operation_id,))
            if previous is not None:
                if str(previous["payload"]) != payload:
                    return StoneTrainingResult("operation_conflict")
                return StoneTrainingResult("duplicate", **json.loads(str(previous["result_json"])))
            user = uow.query_one("SELECT COALESCE(exp,0) AS exp,COALESCE(stone,0) AS stone FROM user_xiuxian WHERE user_id=?", (user_id,))
            if user is None:
                return StoneTrainingResult("user_missing")
            current_exp = require_exact_integer(user["exp"], field="exp", subject="actor")
            current_stone = require_exact_integer(user["stone"], field="stone", subject="actor")
            if (current_exp, current_stone) != (expected_exp, expected_stone):
                return StoneTrainingResult("state_changed", reason="exp_changed" if current_exp != expected_exp else "stone_changed")
            if exp_gain <= 0:
                return StoneTrainingResult("exp_capped")
            if expected_stone < stone_cost:
                return StoneTrainingResult("stone_insufficient", exp_gain, stone_cost)
            new_exp, new_stone = current_exp + exp_gain, current_stone - stone_cost
            power = _training_power(new_exp, power_multiplier)
            changed = uow.execute(
                "UPDATE user_xiuxian SET exp=?,stone=?,power=? WHERE user_id=? "
                "AND xiuxian_int_cmp(exp,?)=0 AND xiuxian_int_cmp(stone,?,1,'stone','actor')=0",
                (bind_sqlite_param(new_exp, exact=True), bind_sqlite_param(new_stone, exact=True),
                 bind_sqlite_param(power, exact=True), user_id,
                 bind_sqlite_param(current_exp, exact=True), bind_sqlite_param(current_stone, exact=True)),
            )
            if changed.rowcount != 1:
                raise _write_rejected("player_write_rejected")
            saved = {"exp_gain": exp_gain, "stone_cost": stone_cost, "power": power}
            encoded = json.dumps(saved, separators=(",", ":"))
            receipt = uow.execute("INSERT INTO stone_training_operations(operation_id,payload,result_json) VALUES(?,?,?)", (operation_id, payload, encoded))
            if receipt.rowcount != 1:
                raise _write_rejected("receipt_write_rejected")
            final = uow.query_one(
                "SELECT actor.exp,actor.stone,actor.power,receipt.payload,receipt.result_json "
                "FROM user_xiuxian AS actor LEFT JOIN stone_training_operations AS receipt "
                "ON receipt.operation_id=? WHERE actor.user_id=?", (operation_id, user_id),
            )
            if final is None or tuple(exact_crystal_integer(final[field]) for field in ("exp", "stone", "power")) != (new_exp, new_stone, power):
                raise _write_rejected("player_projection_mismatch")
            if (final["payload"], final["result_json"]) != (payload, encoded):
                raise _write_rejected("receipt_projection_mismatch")
            return StoneTrainingResult("applied", **saved)


__all__ = ["StoneTrainingSqlRepository", "StoneTrainingResult"]
