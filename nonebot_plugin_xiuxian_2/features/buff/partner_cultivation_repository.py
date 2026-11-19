from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from ...core.errors import DomainError
from ...core.numeric import as_int_like

from ...infrastructure.database.attached_uow import AttachedDatabaseUnitOfWork

_SQLITE_MAX_INT = 2**63 - 1
_SQLITE_MIN_INT = -(2**63)
_PROTECTION_STATES = {"on", "off", "refusal"}


def _same_sql_number(actual, expected) -> bool:
    try:
        return float(actual) == float(expected)
    except (TypeError, ValueError, OverflowError):
        return False


def _integer_like(value) -> int:
    # Match user-row snapshots: float -> str -> Decimal changes large REAL values.
    return as_int_like(value)


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
    reason: str = ""
    subject: str = ""

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
            raise DomainError("schema_missing", "partner cultivation migration is missing")

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

        def result(status, exp_result_1=exp_1, exp_result_2=exp_2, count=used_count, *, reason="", subject=""):
            return PartnerCultivationResult(
                status,
                _integer_like(exp_result_1),
                _integer_like(exp_result_2),
                int(count),
                affection_1,
                affection_2,
                reason,
                subject,
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
                    return result("protection_changed", subject=user_id_2)

            if invite_id:
                invite = uow.query_one(
                    "SELECT inviter_id,target_id,count,status,expires_at "
                    "FROM player_data.partner_cultivation_invites WHERE invite_id=?",
                    (invite_id,),
                )
                if invite is None:
                    return result("invitation_changed", reason="invite_missing")
                if str(invite["inviter_id"]) != user_id_1 or str(invite["target_id"]) != user_id_2:
                    return result("invitation_changed", reason="invite_subject_mismatch")
                if str(invite["status"]) != "pending":
                    reason = {"accepted": "invite_accepted", "rejected": "invite_rejected", "expired": "invite_expired"}.get(str(invite["status"]), "invite_resolved")
                    return result("invitation_changed", reason=reason)
                if float(invite["expires_at"]) <= float(now_timestamp):
                    return result("invitation_changed", reason="invite_expired")
                if int(invite["count"]) < used_count:
                    return result("invitation_changed", reason="invite_count_mismatch")

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
                    return result("state_changed", reason="usage_changed", subject=user_id)

            rows = uow.query_all(
                "SELECT user_id,exp,power,hp,mp,atk,level_up_rate FROM user_xiuxian WHERE user_id IN (?,?)",
                (user_id_1, user_id_2),
            )
            user_snapshots = {str(row["user_id"]): row for row in rows}
            current_exp = {user_id: _integer_like(row["exp"]) for user_id, row in user_snapshots.items()}
            for user_id, expected in ((user_id_1, expected_exp_1), (user_id_2, expected_exp_2)):
                if user_id not in current_exp:
                    return result("state_changed", reason="user_missing", subject=user_id)
                if current_exp[user_id] != _integer_like(expected):
                    return result("state_changed", reason="exp_changed", subject=user_id)

            if expected_affection_1 is not None:
                for user_id, partner_id, expected in (
                    (user_id_1, user_id_2, expected_affection_1),
                    (user_id_2, user_id_1, expected_affection_2),
                ):
                    relation = uow.query_one(
                        "SELECT partner_id,affection FROM player_data.partner WHERE user_id=?",
                        (user_id,),
                    )
                    if relation is None:
                        return result("state_changed", reason="relation_missing", subject=user_id)
                    if str(relation["partner_id"]) != partner_id:
                        return result("state_changed", reason="relation_changed", subject=user_id)
                    if _integer_like(relation["affection"]) != expected:
                        return result("state_changed", reason="affection_changed", subject=user_id)

            for user_id, gain, power, hp, mp, atk, rate in (
                (user_id_1, exp_1, power_1, hp_1, mp_1, atk_1, level_rate_1),
                (user_id_2, exp_2, power_2, hp_2, mp_2, atk_2, level_rate_2),
            ):
                before = user_snapshots[user_id]
                changed = uow.execute(
                    "UPDATE user_xiuxian SET exp=CAST(COALESCE(exp,0) AS REAL)+CAST(? AS REAL),"
                    "power=?,hp=?,mp=?,atk=?,"
                    "level_up_rate=COALESCE(level_up_rate,0)+? WHERE user_id=?",
                    (gain, power, hp, mp, atk, rate, user_id),
                )
                saved = uow.query_one(
                    "SELECT exp,power,hp,mp,atk,level_up_rate FROM user_xiuxian WHERE user_id=?",
                    (user_id,),
                )
                expected_user = (
                    float(before["exp"] or 0) + float(gain),
                    power, hp, mp, atk,
                    _integer_like(before["level_up_rate"] or 0) + rate,
                )
                actual_user = None if saved is None else tuple(
                    saved[field] for field in ("exp", "power", "hp", "mp", "atk", "level_up_rate")
                )
                if changed.rowcount != 1 or actual_user is None or any(
                    not _same_sql_number(actual, expected)
                    for actual, expected in zip(actual_user, expected_user)
                ):
                    raise DomainError(
                        "partner_cultivation_user_write_rejected",
                        "partner cultivation player projection did not persist exactly",
                        {"reason": "partner_cultivation_user_write_rejected", "subject": user_id},
                    )

                previous_stat = uow.query_one(
                    'SELECT "双修次数" AS value FROM player_data.statistics WHERE user_id=?',
                    (user_id,),
                )
                expected_stat = _integer_like(previous_stat["value"] if previous_stat else 0) + used_count
                stat = uow.execute(
                    'UPDATE player_data.statistics SET "双修次数"=COALESCE("双修次数",0)+? '
                    "WHERE user_id=?",
                    (used_count, user_id),
                )
                if stat.rowcount == 0 and previous_stat is None:
                    stat = uow.execute(
                        'INSERT INTO player_data.statistics(user_id,"双修次数") VALUES(?,?)',
                        (user_id, used_count),
                    )
                saved_stat = uow.query_one(
                    'SELECT "双修次数" AS value FROM player_data.statistics WHERE user_id=?',
                    (user_id,),
                )
                if stat.rowcount != 1 or saved_stat is None or _integer_like(saved_stat["value"]) != expected_stat:
                    raise DomainError(
                        "partner_cultivation_statistics_write_rejected",
                        "partner cultivation statistics did not persist exactly",
                        {"reason": "partner_cultivation_statistics_write_rejected", "subject": user_id},
                    )

            if expected_affection_1 is not None:
                for user_id, expected, increase in (
                    (user_id_1, expected_affection_1, affection_1),
                    (user_id_2, expected_affection_2, affection_2),
                ):
                    changed = uow.execute(
                        "UPDATE player_data.partner SET affection=? "
                        "WHERE user_id=? AND xiuxian_int_cmp(COALESCE(affection,0),?)=0",
                        (expected + increase, user_id, expected),
                    )
                    if changed.rowcount != 1:
                        raise DomainError(
                            "partner_affection_write_rejected",
                            "partner affection write did not affect one row",
                            {
                                "reason": "partner_affection_write_rejected",
                                "subject": "actor" if user_id == user_id_1 else "target",
                            },
                        )
                    saved = uow.query_one(
                        "SELECT affection FROM player_data.partner WHERE user_id=?",
                        (user_id,),
                    )
                    if saved is None or _integer_like(saved["affection"]) != expected + increase:
                        raise DomainError(
                            "partner_affection_write_rejected",
                            "partner affection did not persist exactly",
                            {
                                "reason": "partner_affection_write_rejected",
                                "subject": "actor" if user_id == user_id_1 else "target",
                            },
                        )

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
                        saved = uow.query_one(
                            "SELECT used_count FROM player_data.partner_two_exp_usage WHERE user_id=?",
                            (user_id,),
                        )
                        if saved is None or int(saved["used_count"]) != used_count:
                            raise DomainError(
                                "partner_cultivation_usage_write_rejected",
                                "partner usage insert did not persist exactly",
                                {"reason": "partner_cultivation_usage_write_rejected", "subject": user_id},
                            )
                        continue
                changed = uow.execute(
                    "UPDATE player_data.partner_two_exp_usage SET used_count=used_count+?,"
                    "updated_at=CURRENT_TIMESTAMP WHERE user_id=? AND used_count=?",
                    (used_count, user_id, expected),
                )
                saved = uow.query_one(
                    "SELECT used_count FROM player_data.partner_two_exp_usage WHERE user_id=?",
                    (user_id,),
                )
                if changed.rowcount != 1 or saved is None or int(saved["used_count"]) != expected + used_count:
                    raise DomainError(
                        "partner_cultivation_usage_write_rejected",
                        "partner usage update did not persist exactly",
                        {"reason": "partner_cultivation_usage_write_rejected", "subject": user_id},
                    )

            if invite_id:
                changed = uow.execute(
                    "UPDATE player_data.partner_cultivation_invites SET status='accepted',"
                    "resolved_at=? WHERE invite_id=? AND status='pending'",
                    (now_timestamp, invite_id),
                )
                saved_invite = uow.query_one(
                    "SELECT status,resolved_at FROM player_data.partner_cultivation_invites WHERE invite_id=?",
                    (invite_id,),
                )
                if (
                    changed.rowcount != 1
                    or saved_invite is None
                    or str(saved_invite["status"]) != "accepted"
                    or saved_invite["resolved_at"] is None
                    or float(saved_invite["resolved_at"]) != float(now_timestamp)
                ):
                    raise DomainError(
                        "partner_cultivation_invite_write_rejected",
                        "partner invite acceptance did not persist",
                        {"reason": "partner_cultivation_invite_write_rejected", "subject": "target"},
                    )

            inserted = uow.execute(
                "INSERT INTO partner_cultivation_operations "
                "(operation_id,payload,exp_1,exp_2,used_count,affection_1,affection_2) "
                "VALUES(?,?,?,?,?,?,?)",
                (operation_id, payload, exp_1, exp_2, used_count, affection_1, affection_2),
            )
            receipt = uow.query_one(
                "SELECT payload,exp_1,exp_2,used_count,affection_1,affection_2 "
                "FROM partner_cultivation_operations WHERE operation_id=?",
                (operation_id,),
            )
            expected_receipt = (
                payload, _integer_like(exp_1), _integer_like(exp_2), used_count,
                affection_1, affection_2,
            )
            actual_receipt = None if receipt is None else (
                str(receipt["payload"]), _integer_like(receipt["exp_1"]),
                _integer_like(receipt["exp_2"]), int(receipt["used_count"]),
                int(receipt["affection_1"]), int(receipt["affection_2"]),
            )
            if inserted.rowcount != 1 or actual_receipt != expected_receipt:
                raise DomainError(
                    "partner_cultivation_receipt_write_rejected",
                    "partner cultivation receipt did not persist exactly",
                    {"reason": "partner_cultivation_receipt_write_rejected", "subject": operation_id},
                )
            return result("applied")


__all__ = ["PartnerCultivationResult", "PartnerCultivationSqlRepository"]
