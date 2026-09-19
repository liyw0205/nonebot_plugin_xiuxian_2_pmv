from __future__ import annotations

from pathlib import Path
from typing import Any

from .._legacy_application import LegacyApplication
from .repository import BackRepository, LegacyBackRepository


class BackApplication(LegacyApplication):
    def __init__(self, database: str | Path, player_database: str | Path | None = None, *, repository: BackRepository | None = None) -> None:
        super().__init__(database, repository=repository or LegacyBackRepository(database, player_database), feature="back")

    def _action(self, action: str, *, operation_id: str, user_id: str, **kwargs: Any):
        return self._execute(operation_id=operation_id, user_id=user_id, action=f"back.{action}", payload={"user_id": user_id, **kwargs}, call=lambda: self.repository.invoke(action, operation_id, user_id, **kwargs))

    def open_package(self, *, operation_id: str, user_id: str, **kwargs: Any): return self._action("open_package", operation_id=operation_id, user_id=user_id, **kwargs)
    def use_item(self, *, operation_id: str, user_id: str, **kwargs: Any): return self._action("use_item", operation_id=operation_id, user_id=user_id, **kwargs)
    def change_equipment(self, *, operation_id: str, user_id: str, **kwargs: Any): return self._action("change_equipment", operation_id=operation_id, user_id=user_id, **kwargs)
    def learn_skill(self, *, operation_id: str, user_id: str, **kwargs: Any): return self._action("learn_skill", operation_id=operation_id, user_id=user_id, **kwargs)
    def repair(self, *, operation_id: str, user_id: str = "system", **kwargs: Any):
        return self.repository.invoke("repair", operation_id, user_id, **kwargs)
    def use_pet_eggs(self, *, operation_id: str, user_id: str, **kwargs: Any): return self._action("use_pet_eggs", operation_id=operation_id, user_id=user_id, **kwargs)
    def alchemy(self, *, operation_id: str, user_id: str, **kwargs: Any): return self._action("alchemy", operation_id=operation_id, user_id=user_id, **kwargs)
    def unbind(self, *, operation_id: str, user_id: str, **kwargs: Any): return self._action("unbind", operation_id=operation_id, user_id=user_id, **kwargs)



__all__ = ["BackApplication"]
