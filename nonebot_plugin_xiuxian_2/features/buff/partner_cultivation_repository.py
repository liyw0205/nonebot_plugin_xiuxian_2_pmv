from __future__ import annotations

import json
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path

from ...infrastructure.database.attached_uow import AttachedDatabaseUnitOfWork

_SQLITE_MAX_INT = 2**63 - 1
_SQLITE_MIN_INT = -(2**63)
_PROTECTION_STATES = {"on", "off", "refusal"}


def _integer_like(value) -> int:
    try:
        return int(Decimal(str(value or 0)))
    except (InvalidOperation, TypeError, ValueError, OverflowError):
        return 0


def _sql_nonnegative(value):
    integer = _integer_like(value)
    if integer < 0:
        raise ValueError("numeric field must be non-negative")
    if integer > _SQLITE_MAX_INT or integer < _SQLITE_MIN_INT:
        return str(integer)
    return integer


@dataclass(frozen=True)
class PartnerCultivationResult:
    status: str
    exp_1: int
    exp_2: int
    used_count: int
    affection_1: int
    affection_2: int

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class PartnerCultivationSqlRepository:
    """Commit cultivation, usage, relationship and invite state in one UoW."""

    def __init__(self, game_database: str | Path, player_database: str | Path) -> None:
        self.game_database = str(game_database)
        self.player_database = str(player_database)

    @staticmethod
    def _require_table(uow: AttachedDatabaseUnitOfWork, schema: str, table: str) -> None:
        row = uow.query_one(
            f"SELECT 1 AS present FROM {schema}.sqlite_master "
            "WHERE type='table' AND name=?",
            (table,),
        )
        if row is None:
            raise RuntimeError(f"required migration table is missing: {schema}.{table}")

    @staticmethod
    def _protection(value: str) -> str:
        normalized = str(value or "off").strip().lower()
        return normalized if normalized in _PROTECTION_STATES else "off"

    def apply(
        self,
        operation_id,
        user_id_1,
        user_id_2,
        *,
        expected_exp_1,
        expected_exp_2,
        exp_1,
        exp_2,
        used_count,
        power_1,
        power_2,
        hp_1,
        mp_1,
        atk_1,
        hp_2,
        mp_2,
        atk_2,
        level_rate_1=0,
        level_rate_2=0,
        expected_affection_1=None,
        expected_affection_2=None,
        affection_1=0,
        affection_2=0,
        invite_id=None,
        expected_used_count_1=None,
        expected_used_count_2=None,
        expected_target_protection=None,
        now_timestamp,
    ) -> PartnerCultivationResult:
        operation_id = str(operation_id).strip()
        user_id_1, user_id_2 = str(user_id_1), str(user_id_2)
        expected_exp_1, expected_exp_2 = int(expected_exp_1), int(expected_exp_2)
        used_count = int(used_count)
        level_rate_1, level_rate_2 = int(level_rate_1), int(level_rate_2)
        affection_1, affection_2 = int(affection_1), int(affection_2)
        exp_1, exp_2 = _sql_nonnegative(exp_1), _sql_nonnegative(exp_2)
        power_1, power_2 = _sql_nonnegative(power_1), _sql_nonnegative(power_2)
        hp_1, mp_1, atk_1 = map(_sql_nonnegative, (hp_1, mp_1, atk_1))
        hp_2, mp_2, atk_2 = map(_sql_nonnegative, (hp_2, mp_2, atk_2))
        if not operation_id or used_count <= 0 or _integer_like(exp_1) < 0 or _integer_like(exp_2) < 0:
            raise ValueError("invalid partner cultivation operation")

        expected_affection_1 = (
            None if expected_affection_1 is None else int(expected_affection_1)
        )
        expected_affection_2 = (
            None if expected_affection_2 is None else int(expected_affection_2)
        )
        invite_id = None if invite_id is None else str(invite_id)
        usage_snapshot_1 = 0 if expected_used_count_1 is None else int(expected_used_count_1)
        usage_snapshot_2 = 0 if expected_used_count_2 is None else int(expected_used_count_2)
        payload_usage_1 = usage_snapshot_1 if invite_id else None
        payload_usage_2 = usage_snapshot_2 if invite_id else None
        if invite_id and (expected_used_count_1 is None or expected_used_count_2 is None):
            raise ValueError("invite settlement requires usage snapshots")
        if min(usage_snapshot_1, usage_snapshot_2) < 0:
            raise ValueError("invalid partner usage snapshot")
        if expected_target_protection is not None:
            expected_target_protection = str(expected_target_protection).strip().lower()
            if expected_target_protection not in _PROTECTION_STATES:
                raise ValueError("invalid partner protection status")

        values = (
            expected_exp_1,
            expected_exp_2,
            exp_1,
            exp_2,
            used_count,
            power_1,
            power_2,
            hp_1,
            mp_1,
            atk_1,
            hp_2,
            mp_2,
            atk_2,
            level_rate_1,
            level_rate_2,
            affection_1,
            affection_2,
        )
        payload = json.dumps(
            [
                user_id_1,
                user_id_2,
                *values,
                expected_affection_1,
                expected_affection_2,
                invite_id,
                payload_usage_1,
                payload_usage_2,
                expected_target_protection,
            ],
            separators=(",", ":"),
            ensure_ascii=True,
        )

        def result(status, exp_result_1=exp_1, exp_result_2=exp_2, count=used_count):
            return PartnerCultivationResult(
                status,
                _integer_like(exp_result_1),
                _integer_like(exp_result_2),
                int(count),
                affection_1,
                affection_2,
            )

        with AttachedDatabaseUnitOfWork(
            self.game_database,
            attachments={"player_data": self.player_database},
            immediate=True,
        ) as uow:
            self._require_table(uow, "main", "partner_cultivation_operations")
            for table in ("partner_two_exp_usage", "statistics", "partner", "status"):
                self._require_table(uow, "player_data", table)
            if invite_id:
                self._require_table(uow, "player_data", "partner_cultivation_invites")

            previous = uow.query_one(
                "SELECT payload,exp_1,exp_2,used_count,affection_1,affection_2 "
                "FROM partner_cultivation_operations WHERE operation_id=?",
                (operation_id,),
            )
            if previous is not None:
                if str(previous["payload"]) != payload:
                    return result("operation_conflict")
                return result(
                    "duplicate",
                    previous["exp_1"],
                    previous["exp_2"],
                    previous["used_count"],
                )

            if expected_target_protection is not None:
                status = uow.query_one(
                    "SELECT two_exp_protect FROM player_data.status WHERE user_id=?",
                    (user_id_2,),
                )
                actual = self._protection(status["two_exp_protect"] if status else "off")
                if actual != expected_target_protection:
                    return result("protection_changed")

            if invite_id:
                invite = uow.query_one(
                    "SELECT inviter_id,target_id,count,status,expires_at "
                    "FROM player_data.partner_cultivation_invites WHERE invite_id=?",
                    (invite_id,),
                )
                if (
                    invite is None
                    or str(invite["inviter_id"]) != user_id_1
                    or str(invite["target_id"]) != user_id_2
                    or str(invite["status"]) != "pending"
                    or float(invite["expires_at"]) <= float(now_timestamp)
                    or int(invite["count"]) < used_count
                ):
                    return result("invitation_changed")

            for user_id, expected in (
                (user_id_1, usage_snapshot_1),
                (user_id_2, usage_snapshot_2),
            ):
                usage = uow.query_one(
                    "SELECT used_count FROM player_data.partner_two_exp_usage WHERE user_id=?",
                    (user_id,),
                )
                current = int(usage["used_count"]) if usage is not None else 0
                if current != expected:
                    return result("state_changed")

            rows = uow.query_all(
                "SELECT user_id,exp FROM user_xiuxian WHERE user_id IN (?,?)",
                (user_id_1, user_id_2),
            )
            current_exp = {str(row["user_id"]): _integer_like(row["exp"]) for row in rows}
            if current_exp != {
                user_id_1: _integer_like(expected_exp_1),
                user_id_2: _integer_like(expected_exp_2),
            }:
                return result("state_changed")

            if expected_affection_1 is not None:
                for user_id, partner_id, expected in (
                    (user_id_1, user_id_2, expected_affection_1),
                    (user_id_2, user_id_1, expected_affection_2),
                ):
                    relation = uow.query_one(
                        "SELECT partner_id,affection FROM player_data.partner WHERE user_id=?",
                        (user_id,),
                    )
                    if (
                        relation is None
                        or str(relation["partner_id"]) != partner_id
                        or _integer_like(relation["affection"]) != expected
                    ):
                        return result("state_changed")

            for user_id, gain, power, hp, mp, atk, rate in (
                (user_id_1, exp_1, power_1, hp_1, mp_1, atk_1, level_rate_1),
                (user_id_2, exp_2, power_2, hp_2, mp_2, atk_2, level_rate_2),
            ):
                changed = uow.execute(
                    "UPDATE user_xiuxian SET exp=CAST(COALESCE(exp,0) AS REAL)+CAST(? AS REAL),"
                    "power=?,hp=?,mp=?,atk=?,"
                    "level_up_rate=COALESCE(level_up_rate,0)+? WHERE user_id=?",
                    (gain, power, hp, mp, atk, rate, user_id),
                )
                if changed.rowcount != 1:
                    raise RuntimeError("cultivation target changed inside settlement transaction")

                stat = uow.execute(
                    'UPDATE player_data.statistics SET "双修次数"=COALESCE("双修次数",0)+? '
                    "WHERE user_id=?",
                    (used_count, user_id),
                )
                if stat.rowcount == 0:
                    uow.execute(
                        'INSERT INTO player_data.statistics(user_id,"双修次数") VALUES(?,?)',
                        (user_id, used_count),
                    )

            if expected_affection_1 is not None:
                for user_id, expected, increase in (
                    (user_id_1, expected_affection_1, affection_1),
                    (user_id_2, expected_affection_2, affection_2),
                ):
                    changed = uow.execute(
                        "UPDATE player_data.partner SET affection=? "
                        "WHERE user_id=? AND COALESCE(affection,0)=?",
                        (expected + increase, user_id, expected),
                    )
                    if changed.rowcount != 1:
                        raise RuntimeError("partner relation changed inside settlement transaction")

            for user_id, expected in (
                (user_id_1, usage_snapshot_1),
                (user_id_2, usage_snapshot_2),
            ):
                if expected == 0:
                    usage = uow.query_one(
                        "SELECT 1 AS present FROM player_data.partner_two_exp_usage WHERE user_id=?",
                        (user_id,),
                    )
                    if usage is None:
                        uow.execute(
                            "INSERT INTO player_data.partner_two_exp_usage(user_id,used_count) VALUES(?,?)",
                            (user_id, used_count),
                        )
                        continue
                changed = uow.execute(
                    "UPDATE player_data.partner_two_exp_usage SET used_count=used_count+?,"
                    "updated_at=CURRENT_TIMESTAMP WHERE user_id=? AND used_count=?",
                    (used_count, user_id, expected),
                )
                if changed.rowcount != 1:
                    raise RuntimeError("partner usage changed inside settlement transaction")

            if invite_id:
                changed = uow.execute(
                    "UPDATE player_data.partner_cultivation_invites SET status='accepted',"
                    "resolved_at=strftime('%s','now') WHERE invite_id=? AND status='pending'",
                    (invite_id,),
                )
                if changed.rowcount != 1:
                    raise RuntimeError("partner invite changed inside settlement transaction")

            uow.execute(
                "INSERT INTO partner_cultivation_operations "
                "(operation_id,payload,exp_1,exp_2,used_count,affection_1,affection_2) "
                "VALUES(?,?,?,?,?,?,?)",
                (operation_id, payload, exp_1, exp_2, used_count, affection_1, affection_2),
            )
            return result("applied")


__all__ = ["PartnerCultivationResult", "PartnerCultivationSqlRepository"]
