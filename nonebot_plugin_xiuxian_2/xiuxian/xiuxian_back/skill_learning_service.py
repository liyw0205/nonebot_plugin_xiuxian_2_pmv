from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


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


class SkillLearningService:
    """Consume a skill book and update the matching skill slot atomically."""

    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    @staticmethod
    def _ensure_operations(conn) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS skill_learning_operations (
                operation_id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                skill_item_id INTEGER NOT NULL,
                skill_type TEXT NOT NULL,
                previous_item_id INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    def learn(self, operation_id, user_id, skill_item_id, skill_type) -> SkillLearningResult:
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        user_id = str(user_id)
        skill_item_id = int(skill_item_id)
        skill_type = str(skill_type)
        try:
            column = SKILL_COLUMNS[skill_type]
        except KeyError as exc:
            raise ValueError(f"unsupported skill type: {skill_type}") from exc

        def result(status: str, previous_item_id=0) -> SkillLearningResult:
            return SkillLearningResult(
                status, user_id, skill_item_id, skill_type, int(previous_item_id or 0)
            )

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_operations(conn)
                previous = conn.execute(
                    "SELECT previous_item_id FROM skill_learning_operations "
                    "WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    return result("duplicate", previous[0])

                inventory = conn.execute(
                    "SELECT goods_num FROM back WHERE user_id=%s AND goods_id=%s",
                    (user_id, skill_item_id),
                ).fetchone()
                if inventory is None or int(inventory[0] or 0) <= 0:
                    conn.rollback()
                    return result("item_missing")
                buff = conn.execute(
                    f"SELECT {column} FROM BuffInfo WHERE user_id=%s", (user_id,)
                ).fetchone()
                if buff is None:
                    conn.rollback()
                    return result("buff_missing")
                previous_item_id = int(buff[0] or 0)
                if previous_item_id == skill_item_id:
                    conn.rollback()
                    return result("already_learned", previous_item_id)

                columns = set(conn.column_names("back"))
                updates = ["goods_num=goods_num-1"]
                if "bind_num" in columns:
                    updates.append(
                        "bind_num=CASE WHEN goods_num-1=0 THEN 0 "
                        "WHEN COALESCE(bind_num, 0)>0 THEN COALESCE(bind_num, 0)-1 "
                        "ELSE MIN(COALESCE(bind_num, 0), goods_num-1) END"
                    )
                consumed = conn.execute(
                    f"UPDATE back SET {', '.join(updates)} "
                    "WHERE user_id=%s AND goods_id=%s AND goods_num>0",
                    (user_id, skill_item_id),
                )
                updated = conn.execute(
                    f"UPDATE BuffInfo SET {column}=%s WHERE user_id=%s",
                    (skill_item_id, user_id),
                )
                if consumed.rowcount != 1 or updated.rowcount != 1:
                    conn.rollback()
                    return result("state_changed", previous_item_id)
                conn.execute(
                    "INSERT INTO skill_learning_operations "
                    "(operation_id, user_id, skill_item_id, skill_type, previous_item_id) "
                    "VALUES (%s, %s, %s, %s, %s)",
                    (operation_id, user_id, skill_item_id, skill_type, previous_item_id),
                )
                conn.commit()
                return result("learned", previous_item_id)
            except Exception:
                conn.rollback()
                raise


__all__ = ["SKILL_COLUMNS", "SkillLearningResult", "SkillLearningService"]
