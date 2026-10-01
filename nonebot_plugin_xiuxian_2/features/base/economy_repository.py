from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from ...infrastructure.database import DatabaseUnitOfWork


@dataclass(frozen=True)
class EconomyMutationResult:
    status: str
    user_id: str
    requested: int = 0
    applied: int = 0
    value: int | None = None

    @property
    def succeeded(self) -> bool:
        return self.status == "applied"


@dataclass(frozen=True)
class ExperienceNormalizationResult:
    status: str
    user_id: str
    previous_value: int | float | str | None = None
    value: int | None = None

    @property
    def succeeded(self) -> bool:
        return self.status in {"normalized", "unchanged"}


class PlayerEconomySqlRepository:
    """Persist player economy changes without creating runtime schema."""

    def __init__(self, database: str | Path) -> None:
        self.database = Path(database)

    @staticmethod
    def _columns(uow: DatabaseUnitOfWork) -> set[str]:
        table = uow.query_one(
            "SELECT 1 AS present FROM sqlite_master "
            "WHERE type='table' AND name='user_xiuxian'"
        )
        if table is None:
            return set()
        return {
            str(row["name"]).casefold()
            for row in uow.query_all('PRAGMA table_info("user_xiuxian")')
        }

    @classmethod
    def _ready(cls, uow: DatabaseUnitOfWork, field: str) -> bool:
        return {"user_id", field.casefold()}.issubset(cls._columns(uow))

    @staticmethod
    def _result(
        status: str,
        user_id: str,
        requested: int = 0,
        applied: int = 0,
        value: int | None = None,
    ) -> EconomyMutationResult:
        return EconomyMutationResult(
            status,
            str(user_id),
            int(requested),
            int(applied),
            None if value is None else int(value),
        )

    def _grant_capped(
        self,
        user_id: str,
        amount: int,
        *,
        field: str,
        cap: int | None = None,
        expected_value: int | None = None,
    ) -> EconomyMutationResult:
        user_id = str(user_id).strip()
        amount = int(amount)
        if not user_id or amount < 0 or (cap is not None and int(cap) < 0):
            return self._result("invalid", user_id, amount)
        if amount == 0:
            return self._result("applied", user_id, amount, 0, expected_value)
        if not self.database.is_file():
            return self._result("schema_missing", user_id, amount)

        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            if not self._ready(uow, field):
                return self._result("schema_missing", user_id, amount)
            row = uow.query_one(
                f"SELECT rowid AS _rowid,COALESCE(\"{field}\",0) AS value "
                "FROM user_xiuxian WHERE user_id=? ORDER BY rowid ASC LIMIT 1",
                (user_id,),
            )
            if row is None:
                return self._result("user_missing", user_id, amount)
            current = int(row["value"] or 0)
            if expected_value is not None and current != int(expected_value):
                return self._result("state_changed", user_id, amount, 0, current)
            target = current + amount
            if cap is not None:
                target = min(target, int(cap))
            applied = max(0, target - current)
            changed = uow.execute(
                f"UPDATE user_xiuxian SET \"{field}\"=? WHERE rowid=? "
                f"AND user_id=? AND COALESCE(\"{field}\",0)=?",
                (target, row["_rowid"], user_id, current),
            )
            if changed.rowcount != 1:
                return self._result("state_changed", user_id, amount, 0, current)
            return self._result("applied", user_id, amount, applied, target)

    def grant_stone(self, user_id: str, amount: int) -> EconomyMutationResult:
        return self._grant_capped(user_id, amount, field="stone")

    def grant_experience(
        self,
        user_id: str,
        amount: int,
        *,
        max_exp: int,
        expected_exp: int | None = None,
    ) -> EconomyMutationResult:
        return self._grant_capped(
            user_id,
            amount,
            field="exp",
            cap=int(max_exp),
            expected_value=expected_exp,
        )

    def grant_sect_contribution(
        self,
        user_id: str,
        amount: int,
        *,
        expected_value: int | None = None,
    ) -> EconomyMutationResult:
        return self._grant_capped(
            user_id,
            amount,
            field="sect_contribution",
            expected_value=expected_value,
        )

    def normalize_experience(
        self,
        user_id: str,
        expected_exp: int | float | str,
    ) -> ExperienceNormalizationResult:
        user_id = str(user_id).strip()
        try:
            expected_numeric = float(expected_exp)
            target = int(expected_numeric)
        except (TypeError, ValueError, OverflowError):
            return ExperienceNormalizationResult("invalid", user_id)
        if not user_id:
            return ExperienceNormalizationResult("invalid", user_id)
        if not self.database.is_file():
            return ExperienceNormalizationResult("schema_missing", user_id)

        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            if not self._ready(uow, "exp"):
                return ExperienceNormalizationResult("schema_missing", user_id)
            row = uow.query_one(
                "SELECT rowid AS _rowid,exp FROM user_xiuxian "
                "WHERE user_id=? ORDER BY rowid ASC LIMIT 1",
                (user_id,),
            )
            if row is None:
                return ExperienceNormalizationResult("user_missing", user_id)
            current = row["exp"]
            try:
                current_numeric = float(current)
            except (TypeError, ValueError, OverflowError):
                return ExperienceNormalizationResult("invalid", user_id)
            if current_numeric != expected_numeric:
                return ExperienceNormalizationResult(
                    "state_changed", user_id, current, int(current_numeric)
                )
            if current == target:
                return ExperienceNormalizationResult(
                    "unchanged", user_id, current, target
                )

            changed = uow.execute(
                "UPDATE user_xiuxian SET exp=? "
                "WHERE rowid=? AND user_id=? AND exp IS ?",
                (target, row["_rowid"], user_id, current),
            )
            if changed.rowcount != 1:
                return ExperienceNormalizationResult(
                    "state_changed", user_id, current, target
                )
            return ExperienceNormalizationResult("normalized", user_id, current, target)


__all__ = [
    "EconomyMutationResult",
    "ExperienceNormalizationResult",
    "PlayerEconomySqlRepository",
]
