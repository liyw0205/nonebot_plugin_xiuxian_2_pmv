from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from ...infrastructure.database.attached_uow import AttachedDatabaseUnitOfWork


@dataclass(frozen=True)
class PartnerTokenUseResult:
    status: str
    used_tokens: int = 0
    used_count: int = 0
    item_remaining: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class PartnerTokenUseSqlRepository:
    """Consume partner tokens and update daily usage in one attached UoW."""

    def __init__(self, game_database: str | Path, player_database: str | Path) -> None:
        self.game_database = str(game_database)
        self.player_database = str(player_database)

    @staticmethod
    def _require_table(uow: AttachedDatabaseUnitOfWork, schema: str, table: str) -> None:
        qualified = f"{schema}.{table}"
        row = uow.query_one(
            f"SELECT 1 AS present FROM {schema}.sqlite_master WHERE type='table' AND name=?",
            (table,),
        )
        if row is None:
            raise RuntimeError(f"required migration table is missing: {qualified}")

    def apply(
        self,
        operation_id: str,
        user_id: str,
        item_id: int,
        *,
        requested_count: int,
        expected_item_count: int,
        expected_used_count: int,
    ) -> PartnerTokenUseResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        item_id = int(item_id)
        requested_count = int(requested_count)
        expected_item_count = int(expected_item_count)
        expected_used_count = int(expected_used_count)
        if (
            not operation_id
            or not user_id
            or item_id <= 0
            or requested_count <= 0
            or expected_item_count < 0
            or expected_used_count < 0
        ):
            raise ValueError("invalid partner token operation")
        payload = json.dumps(
            [user_id, item_id, requested_count, expected_item_count, expected_used_count],
            ensure_ascii=True,
            separators=(",", ":"),
        )

        with AttachedDatabaseUnitOfWork(
            self.game_database,
            attachments={"player_data": self.player_database},
            immediate=True,
        ) as uow:
            self._require_table(uow, "main", "partner_token_operations")
            self._require_table(uow, "player_data", "partner_two_exp_usage")
            previous = uow.query_one(
                "SELECT payload,used_tokens,used_count,item_remaining "
                "FROM partner_token_operations WHERE operation_id=?",
                (operation_id,),
            )
            if previous is not None:
                status = "duplicate" if str(previous["payload"]) == payload else "operation_conflict"
                if status == "operation_conflict":
                    return PartnerTokenUseResult(status)
                return PartnerTokenUseResult(
                    status,
                    int(previous["used_tokens"]),
                    int(previous["used_count"]),
                    int(previous["item_remaining"]),
                )

            usage = uow.query_one(
                "SELECT used_count FROM player_data.partner_two_exp_usage WHERE user_id=?",
                (user_id,),
            )
            if usage is not None and int(usage["used_count"]) != expected_used_count:
                return PartnerTokenUseResult("state_changed")
            item = uow.query_one(
                "SELECT COALESCE(goods_num,0) AS goods_num,COALESCE(bind_num,0) AS bind_num "
                "FROM back WHERE user_id=? AND goods_id=?",
                (user_id, item_id),
            )
            if item is None:
                return PartnerTokenUseResult("item_missing")
            if int(item["goods_num"]) != expected_item_count:
                return PartnerTokenUseResult("state_changed")

            used_tokens = min(requested_count, expected_used_count, expected_item_count)
            if used_tokens <= 0:
                return PartnerTokenUseResult(
                    "limit_full", 0, expected_used_count, expected_item_count
                )
            new_used_count = expected_used_count - used_tokens
            item_remaining = expected_item_count - used_tokens
            bind_remaining = min(max(0, int(item["bind_num"]) - used_tokens), item_remaining)
            changed = uow.execute(
                "UPDATE back SET goods_num=?,bind_num=? WHERE user_id=? AND goods_id=? "
                "AND COALESCE(goods_num,0)=?",
                (item_remaining, bind_remaining, user_id, item_id, expected_item_count),
            )
            if changed.rowcount != 1:
                return PartnerTokenUseResult("state_changed")

            if usage is None:
                uow.execute(
                    "INSERT INTO player_data.partner_two_exp_usage(user_id,used_count) VALUES(?,?)",
                    (user_id, new_used_count),
                )
            else:
                changed = uow.execute(
                    "UPDATE player_data.partner_two_exp_usage SET used_count=? "
                    "WHERE user_id=? AND used_count=?",
                    (new_used_count, user_id, expected_used_count),
                )
                if changed.rowcount != 1:
                    return PartnerTokenUseResult("state_changed")
            uow.execute(
                "INSERT INTO partner_token_operations "
                "(operation_id,payload,used_tokens,used_count,item_remaining) VALUES(?,?,?,?,?)",
                (operation_id, payload, used_tokens, new_used_count, item_remaining),
            )
            return PartnerTokenUseResult(
                "applied", used_tokens, new_used_count, item_remaining
            )


__all__ = ["PartnerTokenUseResult", "PartnerTokenUseSqlRepository"]
