from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class HarvestReward:
    item_id: int
    name: str
    quantity: int


@dataclass(frozen=True)
class MixelixirHarvestResult:
    status: str
    harvested_at: str
    rewards: tuple[HarvestReward, ...]

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class MixelixirHarvestService:
    """Grant field rewards and advance harvest time atomically across databases."""

    def __init__(self, game_database: str | Path, player_database: str | Path, lock: RLock | None = None) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    @staticmethod
    def _decode_rewards(payload: str) -> tuple[HarvestReward, ...]:
        return tuple(HarvestReward(**reward) for reward in json.loads(payload))

    def harvest(
        self,
        operation_id,
        user_id,
        expected_last_time,
        harvested_at,
        rewards,
        *,
        max_goods_num,
    ) -> MixelixirHarvestResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        expected_last_time = str(expected_last_time)
        harvested_at = str(harvested_at)
        max_goods_num = int(max_goods_num)
        merged: dict[int, HarvestReward] = {}
        for reward in rewards:
            normalized = reward if isinstance(reward, HarvestReward) else HarvestReward(
                int(reward[0]), str(reward[1]), int(reward[2])
            )
            if normalized.quantity <= 0:
                continue
            previous = merged.get(normalized.item_id)
            quantity = normalized.quantity + (previous.quantity if previous else 0)
            merged[normalized.item_id] = HarvestReward(normalized.item_id, normalized.name, quantity)
        normalized_rewards = tuple(sorted(merged.values(), key=lambda reward: reward.item_id))
        if not operation_id or not harvested_at or not normalized_rewards or max_goods_num <= 0:
            raise ValueError("operation, harvest time, rewards and capacity are required")

        rewards_json = json.dumps(
            [reward.__dict__ for reward in normalized_rewards], ensure_ascii=True, sort_keys=True
        )
        payload = json.dumps(
            [user_id, expected_last_time, harvested_at, rewards_json, max_goods_num], ensure_ascii=True
        )
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS mixelixir_harvest_operations ("
                    "operation_id TEXT PRIMARY KEY, payload TEXT NOT NULL, harvested_at TEXT NOT NULL, "
                    "rewards_json TEXT NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                previous = conn.execute(
                    "SELECT payload, harvested_at, rewards_json FROM mixelixir_harvest_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return MixelixirHarvestResult("state_changed", harvested_at, ())
                    return MixelixirHarvestResult("duplicate", str(previous[1]), self._decode_rewards(str(previous[2])))

                user = conn.execute("SELECT 1 FROM user_xiuxian WHERE user_id=%s", (user_id,)).fetchone()
                if user is None:
                    conn.rollback()
                    return MixelixirHarvestResult("user_missing", harvested_at, ())
                table = conn.execute(
                    "SELECT 1 FROM player_data.sqlite_master WHERE type='table' AND name=%s",
                    ("mix_elixir_info",),
                ).fetchone()
                if table is None:
                    conn.rollback()
                    return MixelixirHarvestResult("state_changed", harvested_at, ())
                columns = {
                    str(column[1])
                    for column in conn.execute("PRAGMA player_data.table_info(mix_elixir_info)").fetchall()
                }
                if "收取时间" not in columns:
                    conn.rollback()
                    return MixelixirHarvestResult("state_changed", harvested_at, ())
                row = conn.execute(
                    f"SELECT {db_backend.quote_ident('收取时间')} FROM player_data.mix_elixir_info WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if row is None or str(row[0]) != expected_last_time:
                    conn.rollback()
                    return MixelixirHarvestResult("state_changed", harvested_at, ())

                back_columns = set(conn.column_names("back"))
                insert_columns = "user_id, goods_id, goods_name, goods_type, goods_num"
                insert_values = "%s, %s, %s, %s, %s"
                if "bind_num" in back_columns:
                    insert_columns += ", bind_num"
                    insert_values += ", 0"
                for reward in normalized_rewards:
                    conn.execute(
                        f"INSERT INTO back ({insert_columns}) VALUES ({insert_values}) "
                        "ON CONFLICT (user_id, goods_id) DO UPDATE SET "
                        "goods_name=EXCLUDED.goods_name, goods_type=EXCLUDED.goods_type, "
                        "goods_num=MIN(COALESCE(back.goods_num, 0)+EXCLUDED.goods_num, %s)",
                        (user_id, reward.item_id, reward.name, "药材", reward.quantity, max_goods_num),
                    )
                updated = conn.execute(
                    f"UPDATE player_data.mix_elixir_info SET {db_backend.quote_ident('收取时间')}=%s "
                    f"WHERE user_id=%s AND {db_backend.quote_ident('收取时间')}=%s",
                    (harvested_at, user_id, expected_last_time),
                )
                if updated.rowcount != 1:
                    conn.rollback()
                    return MixelixirHarvestResult("state_changed", harvested_at, ())
                conn.execute(
                    "INSERT INTO mixelixir_harvest_operations (operation_id, payload, harvested_at, rewards_json) "
                    "VALUES (%s, %s, %s, %s)",
                    (operation_id, payload, harvested_at, rewards_json),
                )
                conn.commit()
                return MixelixirHarvestResult("applied", harvested_at, normalized_rewards)
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    conn.execute("DETACH DATABASE player_data")


__all__ = ["HarvestReward", "MixelixirHarvestResult", "MixelixirHarvestService"]
