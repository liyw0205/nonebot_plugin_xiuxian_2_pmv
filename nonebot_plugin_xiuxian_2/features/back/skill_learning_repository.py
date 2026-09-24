from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from ...infrastructure.database import DatabaseUnitOfWork


SKILL_COLUMNS = {
    "功法": "main_buff",
    "辅修功法": "sub_buff",
    "神通": "sec_buff",
    "身法": "effect1_buff",
    "瞳术": "effect2_buff",
}


@dataclass(frozen=True)
class SkillLearningResult:
    status: str
    user_id: str
    skill_item_id: int
    skill_type: str
    previous_item_id: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"learned", "duplicate"}


class SkillLearningSqlRepository:
    """Atomically consume a skill book and update its BuffInfo slot."""

    def __init__(self, database: str | Path) -> None:
        self.database = str(database)

    def learn(self, operation_id: str, user_id: str, skill_item_id: int, skill_type: str) -> SkillLearningResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        skill_item_id = int(skill_item_id)
        skill_type = str(skill_type)
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        try:
            column = SKILL_COLUMNS[skill_type]
        except KeyError as exc:
            raise ValueError(f"unsupported skill type: {skill_type}") from exc

        def result(status: str, previous_item_id: int = 0) -> SkillLearningResult:
            return SkillLearningResult(status, user_id, skill_item_id, skill_type, int(previous_item_id or 0))

        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            previous = uow.query_one(
                "SELECT previous_item_id FROM skill_learning_operations WHERE operation_id=?",
                (operation_id,),
            )
            if previous is not None:
                return result("duplicate", int(previous["previous_item_id"] or 0))

            inventory = uow.query_one(
                "SELECT goods_num FROM back WHERE user_id=? AND goods_id=?",
                (user_id, skill_item_id),
            )
            if inventory is None or int(inventory["goods_num"] or 0) <= 0:
                return result("item_missing")
            buff = uow.query_one(
                f"SELECT {column} FROM BuffInfo WHERE user_id=?",
                (user_id,),
            )
            if buff is None:
                return result("buff_missing")
            previous_item_id = int(buff[column] or 0)
            if previous_item_id == skill_item_id:
                return result("already_learned", previous_item_id)

            columns = {str(row["name"]) for row in uow.query_all("PRAGMA table_info(back)")}
            updates = ["goods_num=goods_num-1"]
            if "bind_num" in columns:
                updates.append(
                    "bind_num=CASE WHEN goods_num-1=0 THEN 0 "
                    "WHEN COALESCE(bind_num, 0)>0 THEN COALESCE(bind_num, 0)-1 "
                    "ELSE MIN(COALESCE(bind_num, 0), goods_num-1) END"
                )
            for update_column in ("update_time", "action_time"):
                if update_column in columns:
                    updates.append(f"{update_column}=CURRENT_TIMESTAMP")
            consumed = uow.execute(
                f"UPDATE back SET {', '.join(updates)} "
                "WHERE user_id=? AND goods_id=? AND goods_num>0",
                (user_id, skill_item_id),
            )
            updated = uow.execute(
                f"UPDATE BuffInfo SET {column}=? WHERE user_id=?",
                (skill_item_id, user_id),
            )
            if consumed.rowcount != 1 or updated.rowcount != 1:
                return result("state_changed", previous_item_id)
            uow.execute(
                "INSERT INTO skill_learning_operations "
                "(operation_id,user_id,skill_item_id,skill_type,previous_item_id) VALUES(?,?,?,?,?)",
                (operation_id, user_id, skill_item_id, skill_type, previous_item_id),
            )
            return result("learned", previous_item_id)


__all__ = ["SKILL_COLUMNS", "SkillLearningResult", "SkillLearningSqlRepository"]
