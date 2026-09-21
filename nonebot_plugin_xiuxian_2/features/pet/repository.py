from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from ...infrastructure.database import DatabaseUnitOfWork


@dataclass(frozen=True)
class PetActiveSwitchResult:
    status: str
    active_uid: str = ""

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate", "already_active"}


class PetActiveSwitchSqlRepository:
    def __init__(self, database: str | Path) -> None:
        self.database = str(database)

    def switch(self, operation_id: str, user_id: str, expected_active_uid: str, target_uid: str, travel_pet_uid: str = "") -> PetActiveSwitchResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        expected_active_uid = str(expected_active_uid or "")
        target_uid = str(target_uid).strip()
        travel_pet_uid = str(travel_pet_uid or "")
        if not operation_id or not user_id or not target_uid:
            raise ValueError("operation, user and target pet are required")
        payload = json.dumps([user_id, target_uid], ensure_ascii=True, separators=(",", ":"))
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            uow.execute(
                "CREATE TABLE IF NOT EXISTS pet_active_switch_operations("
                "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,active_uid TEXT NOT NULL,"
                "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
            )
            previous = uow.query_one(
                "SELECT payload,active_uid FROM pet_active_switch_operations WHERE operation_id=?",
                (operation_id,),
            )
            if previous is not None:
                if str(previous["payload"]) != payload:
                    return PetActiveSwitchResult("operation_conflict")
                return PetActiveSwitchResult("duplicate", str(previous["active_uid"]))
            target = uow.query_one(
                "SELECT COALESCE(is_active,0) AS is_active FROM player_pet_item WHERE user_id=? AND uid=?",
                (user_id, target_uid),
            )
            if target is None:
                return PetActiveSwitchResult("pet_missing")
            current = uow.query_all(
                "SELECT uid FROM player_pet_item WHERE user_id=? AND COALESCE(is_active,0)=1",
                (user_id,),
            )
            current_uids = tuple(str(row["uid"]) for row in current)
            actual_active = current_uids[0] if len(current_uids) == 1 else ""
            if len(current_uids) > 1 or actual_active != expected_active_uid:
                return PetActiveSwitchResult("state_changed")
            if target_uid == actual_active:
                return PetActiveSwitchResult("already_active", target_uid)
            if target_uid == travel_pet_uid:
                return PetActiveSwitchResult("pet_traveling")
            meta = uow.query_one("SELECT active_uid FROM player_pet WHERE user_id=?", (user_id,))
            if meta is None or str(meta["active_uid"] or "") != expected_active_uid:
                return PetActiveSwitchResult("state_changed")
            now = int(time.time())
            uow.execute(
                "UPDATE player_pet_item SET is_active=0,updated_at=? WHERE user_id=? AND COALESCE(is_active,0)=1",
                (now, user_id),
            )
            changed = uow.execute(
                "UPDATE player_pet_item SET is_active=1,updated_at=? WHERE user_id=? AND uid=? AND COALESCE(is_active,0)=0",
                (now, user_id, target_uid),
            )
            if changed.rowcount != 1:
                raise RuntimeError("target pet state changed")
            changed = uow.execute(
                "UPDATE player_pet SET active_uid=?,active=? WHERE user_id=? AND COALESCE(active_uid,'')=?",
                (target_uid, target_uid, user_id, expected_active_uid),
            )
            if changed.rowcount != 1:
                raise RuntimeError("pet metadata state changed")
            uow.execute(
                "INSERT INTO pet_active_switch_operations(operation_id,payload,active_uid) VALUES(?,?,?)",
                (operation_id, payload, target_uid),
            )
            return PetActiveSwitchResult("applied", target_uid)


class PetRepository(Protocol):
    def switch(self, *args: Any, **kwargs: Any) -> Any: ...
    def travel_claim(self, *args: Any, **kwargs: Any) -> Any: ...
    def travel_start(self, *args: Any, **kwargs: Any) -> Any: ...
    def feed(self, *args: Any, **kwargs: Any) -> Any: ...
    def hatch(self, *args: Any, **kwargs: Any) -> Any: ...
    def hatch_result(self, *args: Any, **kwargs: Any) -> Any: ...


class LegacyPetRepository:
    def __init__(self, game_database: str | Path, player_database: str | Path) -> None:
        self.game_database = str(game_database)
        self.player_database = str(player_database)

    def _services(self):
        from ...xiuxian.xiuxian_pet.transaction_service import PetTravelClaimService, PetTravelStartService, PetFeedService, PetHatchService

        return (PetTravelClaimService(self.game_database, self.player_database), PetTravelStartService(self.player_database), PetFeedService(self.game_database, self.player_database), PetHatchService(self.game_database, self.player_database))

    def travel_claim(self, *args: Any, **kwargs: Any) -> Any:
        return self._services()[0].claim(*args, **kwargs)

    def travel_start(self, *args: Any, **kwargs: Any) -> Any:
        return self._services()[1].start(*args, **kwargs)

    def feed(self, *args: Any, **kwargs: Any) -> Any:
        return self._services()[2].feed(*args, **kwargs)

    def hatch(self, *args: Any, **kwargs: Any) -> Any:
        return self._services()[3].hatch(*args, **kwargs)

    def hatch_result(self, *args: Any, **kwargs: Any) -> Any:
        return self._services()[3].get_result(*args, **kwargs)

    def switch(self, *args: Any, **kwargs: Any) -> Any:
        from ...xiuxian.xiuxian_pet.transaction_service import PetActiveSwitchService

        return PetActiveSwitchService(self.player_database).switch(*args, **kwargs)


__all__ = ["PetActiveSwitchResult", "PetActiveSwitchSqlRepository", "PetRepository", "LegacyPetRepository"]
