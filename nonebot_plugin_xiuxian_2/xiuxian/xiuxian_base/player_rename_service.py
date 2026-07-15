from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class PlayerRenameResult:
    status: str
    user_id: str
    rename_type: str
    new_name: str
    previous_name: str = ""

    @property
    def succeeded(self) -> bool:
        return self.status in {"renamed", "duplicate"}


class PlayerRenameService:
    """Charge a rename cost and update the player record atomically."""

    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    @staticmethod
    def _ensure_operations(conn) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS player_rename_operations (
                operation_id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                rename_type TEXT NOT NULL,
                new_name TEXT NOT NULL,
                previous_name TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    @staticmethod
    def _consume_item(conn, user_id: str, item_id: int) -> bool:
        columns = set(conn.column_names("back"))
        updates = ["goods_num=goods_num-1"]
        if "bind_num" in columns:
            updates.append(
                "bind_num=CASE WHEN goods_num-1=0 THEN 0 "
                "WHEN COALESCE(bind_num, 0)>0 THEN COALESCE(bind_num, 0)-1 "
                "ELSE MIN(COALESCE(bind_num, 0), goods_num-1) END"
            )
        changed = conn.execute(
            f"UPDATE back SET {', '.join(updates)} "
            "WHERE user_id=%s AND goods_id=%s AND goods_num>0",
            (user_id, int(item_id)),
        )
        return changed.rowcount == 1

    def get_result(self, operation_id: str) -> PlayerRenameResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            self._ensure_operations(conn)
            previous = conn.execute(
                "SELECT user_id, rename_type, new_name, previous_name "
                "FROM player_rename_operations WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if previous is None:
                return None
            return PlayerRenameResult(
                "duplicate", str(previous[0]), str(previous[1]), str(previous[2]), str(previous[3] or "")
            )

    def _rename(
        self,
        operation_id,
        user_id,
        rename_type,
        new_name,
        *,
        target_column,
        item_id=None,
        stone_cost=0,
        require_unique=False,
    ) -> PlayerRenameResult:
        operation_id = str(operation_id).strip()
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        user_id = str(user_id)
        rename_type = str(rename_type)
        new_name = str(new_name).strip()
        stone_cost = int(stone_cost)
        if not new_name:
            raise ValueError("new_name must not be empty")
        if stone_cost < 0:
            raise ValueError("stone_cost must not be negative")
        if item_id is not None and stone_cost:
            raise ValueError("rename can charge either an item or stones")

        def result(status: str, previous_name="", result_name=new_name) -> PlayerRenameResult:
            return PlayerRenameResult(
                status, user_id, rename_type, str(result_name), str(previous_name or "")
            )

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_operations(conn)
                previous = conn.execute(
                    "SELECT previous_name, new_name FROM player_rename_operations "
                    "WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    return result("duplicate", previous[0], previous[1])

                player = conn.execute(
                    f"SELECT {target_column}, stone FROM user_xiuxian WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if player is None:
                    conn.rollback()
                    return result("user_missing")
                previous_name = str(player[0] or "")
                if previous_name == new_name:
                    conn.rollback()
                    return result("unchanged", previous_name)
                if require_unique:
                    conflict = conn.execute(
                        "SELECT 1 FROM user_xiuxian WHERE user_name=%s AND user_id<>%s",
                        (new_name, user_id),
                    ).fetchone()
                    if conflict is not None:
                        conn.rollback()
                        return result("name_conflict", previous_name)

                if item_id is not None:
                    if not self._consume_item(conn, user_id, int(item_id)):
                        conn.rollback()
                        return result("item_missing", previous_name)
                elif stone_cost:
                    charged = conn.execute(
                        "UPDATE user_xiuxian SET stone=stone-%s "
                        "WHERE user_id=%s AND stone>=%s",
                        (stone_cost, user_id, stone_cost),
                    )
                    if charged.rowcount != 1:
                        conn.rollback()
                        return result("stone_insufficient", previous_name)

                renamed = conn.execute(
                    f"UPDATE user_xiuxian SET {target_column}=%s WHERE user_id=%s",
                    (new_name, user_id),
                )
                if renamed.rowcount != 1:
                    conn.rollback()
                    return result("state_changed", previous_name)
                conn.execute(
                    "INSERT INTO player_rename_operations "
                    "(operation_id, user_id, rename_type, new_name, previous_name) "
                    "VALUES (%s, %s, %s, %s, %s)",
                    (operation_id, user_id, rename_type, new_name, previous_name),
                )
                conn.commit()
                return result("renamed", previous_name)
            except Exception:
                conn.rollback()
                raise

    def rename_user(self, operation_id, user_id, new_name, *, item_id=None, stone_cost=0):
        return self._rename(
            operation_id,
            user_id,
            "user_name",
            new_name,
            target_column="user_name",
            item_id=item_id,
            stone_cost=stone_cost,
            require_unique=True,
        )

    def rename_root(self, operation_id, user_id, new_name, *, item_id):
        return self._rename(
            operation_id,
            user_id,
            "root",
            new_name,
            target_column="root",
            item_id=item_id,
        )


__all__ = ["PlayerRenameResult", "PlayerRenameService"]
