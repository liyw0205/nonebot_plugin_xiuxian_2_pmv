from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian.xiuxian_utils import db_backend


@dataclass(frozen=True)
class SectElixirClaimResult:
    status: str
    rewards: tuple[tuple[int, str, str, int], ...] = ()
    reason: str = ""
    subject: str = ""

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class SectElixirClaimService:
    """Legacy daily elixir-room settlement retained for explicit rollback."""

    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    def claim(self, operation_id, user_id, sect_id, contribution_required, materials_required, rewards, max_goods_num):
        operation_id = str(operation_id).strip()
        user_id, sect_id = str(user_id), int(sect_id)
        contribution_required, materials_required = int(contribution_required), int(materials_required)
        max_goods_num = int(max_goods_num)
        normalized = tuple((int(item_id), str(name), str(item_type), int(quantity)) for item_id, name, item_type, quantity in rewards)
        if not operation_id or not normalized or any(item_id < 0 or quantity <= 0 for item_id, _, _, quantity in normalized):
            raise ValueError("valid operation and rewards are required")
        from ..features.sect.repository import SectRenameSqlRepository
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS sect_elixir_claim_operations ("
                "operation_id TEXT PRIMARY KEY, payload TEXT NOT NULL, rewards TEXT NOT NULL, "
                "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
            )
            conn.commit()
            result = SectRenameSqlRepository(self._database).claim_elixir(
                operation_id, user_id, sect_id, contribution_required, materials_required, normalized, max_goods_num, legacy=True)
        return SectElixirClaimResult(result["status"], result.get("rewards", ()),
                                    result.get("reason", ""), result.get("subject", ""))


__all__ = ["SectElixirClaimResult", "SectElixirClaimService"]
