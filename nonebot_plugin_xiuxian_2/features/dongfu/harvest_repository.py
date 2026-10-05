from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from ...infrastructure.clock import SystemClock
from ...infrastructure.database import DatabaseUnitOfWork, OutboxStore
from .operation_schema import operation_databases_ready, operation_schema_ready
from .plant_slots import canonical_plant_slots, legacy_plant_fields, normalize_plant_slots


@dataclass(frozen=True)
class DongfuHarvestResult:
    status: str
    rewards: tuple = ()
    effects_event_id: str | None = None
    effects_pending: bool = False

    @property
    def succeeded(self):
        return self.status in {"harvested", "duplicate"}


@dataclass(frozen=True)
class DongfuHarvestSnapshotResult:
    status: str
    snapshot: dict | None = None

    @property
    def succeeded(self):
        return self.status in {"prepared", "existing"}


class DongfuHarvestSqlRepository:
    def __init__(
        self,
        game_database: str | Path,
        player_database: str | Path,
        *,
        clock: Any | None = None,
    ) -> None:
        self.game_database = str(game_database)
        self.player_database = str(player_database)
        self.clock = clock or SystemClock()
        self.outbox = OutboxStore(clock=self.clock)

    @staticmethod
    def _canonical(value):
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))

    @classmethod
    def _decode_snapshot(cls, value):
        try:
            snapshot = json.loads(value) if isinstance(value, str) else value
        except (TypeError, ValueError):
            return None
        if not isinstance(snapshot, dict):
            return None
        if not all(key in snapshot for key in ("expected_slots", "slot_numbers", "items", "failed_slots")):
            return None
        if not isinstance(snapshot["expected_slots"], list) or not isinstance(snapshot["slot_numbers"], list):
            return None
        if not isinstance(snapshot["items"], list) or not isinstance(snapshot["failed_slots"], list):
            return None
        return snapshot

    @staticmethod
    def _outbox_schema_ready(uow: DatabaseUnitOfWork) -> bool:
        expected = {
            "event_id", "aggregate_type", "aggregate_id", "event_type", "payload_json",
            "status", "attempts", "next_attempt_at", "created_at", "updated_at",
        }
        columns = {
            str(row["name"])
            for row in uow.query_all('PRAGMA main.table_info("domain_outbox")')
        }
        return expected.issubset(columns)

    def prepare_snapshot(
        self,
        operation_id,
        user_id,
        expected_slots,
        snapshot,
        *,
        base_plot_count=3,
        max_plot_count=6,
        fertilizer_max=3,
        seed_names=None,
    ):
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        seed_names = {int(key): str(value) for key, value in (seed_names or {}).items()}
        snapshot = self._decode_snapshot(snapshot)
        if (
            not operation_id
            or snapshot is None
            or self._canonical(snapshot["expected_slots"]) != self._canonical(expected_slots)
        ):
            return DongfuHarvestSnapshotResult("snapshot_invalid")
        snapshot["operation_id"] = str(snapshot.get("operation_id") or operation_id)
        if not operation_databases_ready(self.game_database, self.player_database):
            return DongfuHarvestSnapshotResult("schema_missing")

        with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
            uow.attach_database(self.player_database, "player_data")
            if not operation_schema_ready(uow, "dongfu_harvest_operations"):
                return DongfuHarvestSnapshotResult("schema_missing")
            if uow.query_one("SELECT 1 AS found FROM user_xiuxian WHERE user_id=?", (user_id,)) is None:
                return DongfuHarvestSnapshotResult("user_missing")
            row = uow.query_one(
                "SELECT built,plot_count,plant_slots,planting,plant_seed_id,plant_start,plant_finish,harvest_settlement "
                "FROM player_data.dongfu_status WHERE user_id=?",
                (user_id,),
            )
            if row is None or int(row["built"] or 0) != 1:
                return DongfuHarvestSnapshotResult("dongfu_missing")
            state = dict(row)
            slots = normalize_plant_slots(
                state,
                base_plot_count=base_plot_count,
                max_plot_count=max_plot_count,
                fertilizer_max=fertilizer_max,
                seed_names=seed_names,
            )
            if self._canonical(slots) != self._canonical(expected_slots):
                return DongfuHarvestSnapshotResult("state_changed")

            existing_raw = row.get("harvest_settlement")
            existing = self._decode_snapshot(existing_raw) if existing_raw else None
            if existing_raw and existing is None:
                return DongfuHarvestSnapshotResult("snapshot_invalid")
            if existing is not None and self._canonical(existing["expected_slots"]) == self._canonical(expected_slots):
                existing["operation_id"] = str(existing.get("operation_id") or operation_id)
                return DongfuHarvestSnapshotResult("existing", existing)

            legacy = legacy_plant_fields(
                slots, valid_seed_ids=set(seed_names) if seed_names else None
            )
            slot_json = canonical_plant_slots(slots)
            changed = uow.execute(
                "UPDATE player_data.dongfu_status SET plot_count=?,plant_slots=?,planting=?,plant_seed_id=?,plant_start=?,plant_finish=?,harvest_settlement=? "
                "WHERE user_id=? AND built=1 AND plot_count IS ? AND plant_slots IS ? AND planting IS ? AND plant_seed_id IS ? "
                "AND plant_start IS ? AND plant_finish IS ? AND harvest_settlement IS ?",
                (
                    state["plot_count"], slot_json, *legacy, self._canonical(snapshot), user_id,
                    row["plot_count"], row["plant_slots"], row["planting"], row["plant_seed_id"],
                    row["plant_start"], row["plant_finish"], row["harvest_settlement"],
                ),
            )
            if changed.rowcount != 1:
                return DongfuHarvestSnapshotResult("state_changed")
            return DongfuHarvestSnapshotResult("prepared", snapshot)

    def harvest(self, operation_id, user_id, expected_slots, slot_numbers, rewards, max_goods_num, settled_at, failed_slots=()):
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        slot_numbers = tuple(sorted({int(value) for value in slot_numbers}))
        failed_slots = tuple(str(value) for value in failed_slots)
        max_goods_num = int(max_goods_num)
        reward_rows = [
            (int(item["id"]), str(item["name"]), str(item["type"]), int(item["amount"]))
            for item in rewards
            if int(item["amount"]) > 0
        ]
        payload = self._canonical([user_id, expected_slots, slot_numbers, reward_rows, failed_slots])
        effects_event_id = f"dongfu.harvest.effects:{operation_id}"
        if not operation_id or not operation_databases_ready(self.game_database, self.player_database):
            return DongfuHarvestResult("schema_missing")

        with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
            uow.attach_database(self.player_database, "player_data")
            if (
                not operation_schema_ready(uow, "dongfu_harvest_operations")
                or not self._outbox_schema_ready(uow)
            ):
                return DongfuHarvestResult("schema_missing")
            old = uow.query_one(
                "SELECT payload,rewards FROM dongfu_harvest_operations WHERE operation_id=?",
                (operation_id,),
            )
            if old is not None:
                old_payload = str(old["payload"])
                try:
                    decoded = json.loads(old_payload)
                except (TypeError, ValueError):
                    decoded = None
                legacy_duplicate = (
                    isinstance(decoded, list)
                    and len(decoded) == 2
                    and decoded == [user_id, list(slot_numbers)]
                )
                if isinstance(decoded, list) and len(decoded) in {4, 5}:
                    legacy_duplicate = (
                        decoded[0] == user_id
                        and self._canonical(decoded[1]) == self._canonical(expected_slots)
                        and decoded[2] == list(slot_numbers)
                        and self._canonical(decoded[3]) == self._canonical(reward_rows)
                        and (len(decoded) == 4 or self._canonical(decoded[4]) == self._canonical(failed_slots))
                    )
                if old_payload != payload and not legacy_duplicate:
                    return DongfuHarvestResult("state_changed")
                event_id = effects_event_id if self.outbox.get(uow, effects_event_id) is not None else None
                return DongfuHarvestResult(
                    "duplicate",
                    tuple(tuple(item) for item in json.loads(str(old["rewards"]))),
                    event_id,
                )

            user = uow.query_one("SELECT 1 AS found FROM user_xiuxian WHERE user_id=?", (user_id,))
            row = uow.query_one(
                "SELECT built,plant_slots FROM player_data.dongfu_status WHERE user_id=?",
                (user_id,),
            )
            if user is None:
                return DongfuHarvestResult("user_missing")
            if row is None or int(row["built"] or 0) != 1:
                return DongfuHarvestResult("dongfu_missing")
            actual = json.loads(str(row["plant_slots"]))
            if self._canonical(actual) != self._canonical(expected_slots):
                return DongfuHarvestResult("state_changed")
            for slot_no in slot_numbers:
                if slot_no < 1 or slot_no > len(actual):
                    return DongfuHarvestResult("state_changed")
                finish = str(actual[slot_no - 1].get("plant_finish") or "")
                if not finish or finish > str(settled_at):
                    return DongfuHarvestResult("not_mature")

            totals: dict[int, int] = {}
            metadata: dict[int, tuple[str, str]] = {}
            for item_id, name, item_type, amount in reward_rows:
                totals[item_id] = totals.get(item_id, 0) + amount
                metadata[item_id] = (name, item_type)
            for item_id, amount in totals.items():
                item = uow.query_one(
                    "SELECT COALESCE(goods_num,0) AS goods_num FROM back WHERE user_id=? AND goods_id=?",
                    (user_id, item_id),
                )
                if (0 if item is None else int(item["goods_num"])) + amount > max_goods_num:
                    return DongfuHarvestResult("inventory_full")

            for slot_no in slot_numbers:
                actual[slot_no - 1] = {
                    "slot": slot_no,
                    "seed_id": 0,
                    "seed_name": "",
                    "plant_start": "",
                    "plant_finish": "",
                    "fertilizer": 0,
                }
            active = next((slot for slot in actual if int(slot.get("seed_id") or 0) > 0), None)
            legacy = (1, int(active["seed_id"]), active.get("plant_start", ""), active.get("plant_finish", "")) if active else (0, 0, "", "")
            if uow.execute(
                "UPDATE player_data.dongfu_status SET plant_slots=?,planting=?,plant_seed_id=?,plant_start=?,plant_finish=?,harvest_settlement=? WHERE user_id=?",
                (self._canonical(actual), *legacy, "", user_id),
            ).rowcount != 1:
                return DongfuHarvestResult("state_changed")

            now = self.clock.now().isoformat()
            for item_id, amount in totals.items():
                name, item_type = metadata[item_id]
                uow.execute(
                    "INSERT INTO back(user_id,goods_id,goods_name,goods_type,goods_num,create_time,update_time,bind_num) "
                    "VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(user_id,goods_id) DO UPDATE SET "
                    "goods_name=excluded.goods_name,goods_type=excluded.goods_type,"
                    "goods_num=back.goods_num+excluded.goods_num,"
                    "bind_num=COALESCE(back.bind_num,0)+excluded.bind_num,update_time=excluded.update_time",
                    (user_id, item_id, name, item_type, amount, now, now, amount),
                )

            compact = tuple(sorted(totals.items()))
            uow.execute(
                "INSERT INTO dongfu_harvest_operations(operation_id,payload,rewards) VALUES(?,?,?)",
                (operation_id, payload, json.dumps(compact)),
            )
            self.outbox.append(
                uow,
                event_id=effects_event_id,
                aggregate_type="player",
                aggregate_id=user_id,
                event_type="game_event.projection",
                payload={
                    "operation_id": operation_id,
                    "user_id": user_id,
                    "event_key": "dongfu_harvest",
                    "amount": len(slot_numbers),
                    "occurred_at": datetime.fromisoformat(str(settled_at)).isoformat(),
                    "meta": {
                        "source": "dongfu",
                        "action": "harvest",
                        "trace_id": operation_id,
                        "item_delta": [
                            {"id": item_id, "name": name, "type": item_type, "amount": amount}
                            for item_id, name, item_type, amount in reward_rows
                        ],
                        "detail": {
                            "slots": list(slot_numbers),
                            "failed_slots": list(failed_slots),
                        },
                    },
                },
            )
            return DongfuHarvestResult("harvested", compact, effects_event_id)


__all__ = ["DongfuHarvestResult", "DongfuHarvestSnapshotResult", "DongfuHarvestSqlRepository"]
