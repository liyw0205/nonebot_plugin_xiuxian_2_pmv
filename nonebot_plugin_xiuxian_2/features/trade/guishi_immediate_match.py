"""Durable continuation of the first matching pass after order creation."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Any, Iterable

from ...core.errors import DomainError
from ...infrastructure.database import DatabaseUnitOfWork
from .guishi_match_repository import GuishiMatchResult


class GuishiMatchFailure(RuntimeError):
    def __init__(self, reason: str, qiugou_order_id: str, baitan_order_id: str,
                 *, matches: Iterable[GuishiMatchResult] = (), subject: str = "") -> None:
        self.reason = str(reason)
        self.qiugou_order_id = str(qiugou_order_id)
        self.baitan_order_id = str(baitan_order_id)
        self.matches = list(matches)
        self.subject = str(subject)
        super().__init__(f"{self.reason}: qiugou_order_id={self.qiugou_order_id} "
                         f"baitan_order_id={self.baitan_order_id} subject={self.subject}")


@dataclass(frozen=True)
class GuishiImmediateMatchResult:
    status: str
    matches: tuple[GuishiMatchResult, ...] = ()
    failure: GuishiMatchFailure | None = None


def _require_requests(uow: DatabaseUnitOfWork, schema: str = "main") -> None:
    if uow.query_one(f"SELECT 1 FROM {schema}.sqlite_master WHERE type='table' AND name=?",
                     ("guishi_immediate_match_requests",)) is None:
        raise DomainError("schema_missing", "Guishi immediate match migration is missing",
                          {"reason": "schema_missing", "subject": "guishi_immediate_match_requests"})


def enqueue_immediate_match(uow: DatabaseUnitOfWork, *, order_id: str, order_type: str,
                            user_id: str, table: str = "guishi_immediate_match_requests") -> None:
    _require_requests(uow, table.split(".")[0] if "." in table else "main")
    inserted = uow.execute(
        f"INSERT INTO {table}(order_id,order_type,user_id) VALUES(?,?,?)",
        (order_id, order_type, user_id),
    )
    if inserted.rowcount != 1:
        raise DomainError(
            "guishi_immediate_match_write_rejected",
            "Guishi immediate match request was not inserted",
            {"reason": "guishi_immediate_match_write_rejected", "subject": str(order_id)},
        )
    saved = uow.query_one(
        f"SELECT order_id,order_type,user_id,status,reason,subject,qiugou_order_id,baitan_order_id "
        f"FROM {table} WHERE order_id=?",
        (order_id,),
    )
    expected = {
        "order_id": str(order_id),
        "order_type": str(order_type),
        "user_id": str(user_id),
        "status": "pending",
        "reason": "",
        "subject": "",
        "qiugou_order_id": "",
        "baitan_order_id": "",
    }
    if saved != expected:
        raise DomainError(
            "guishi_immediate_match_write_rejected",
            "Guishi immediate match request did not persist exactly",
            {"reason": "guishi_immediate_match_write_rejected", "subject": str(order_id)},
        )


class GuishiImmediateMatchRepository:
    def __init__(self, database: str | Path) -> None:
        self.database = str(database)

    def get(self, order_id: str) -> Mapping[str, Any] | None:
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            _require_requests(uow)
            return uow.query_one("SELECT * FROM guishi_immediate_match_requests WHERE order_id=?", (order_id,))

    def pending(self) -> list[Mapping[str, Any]]:
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            _require_requests(uow)
            return uow.query_all("SELECT * FROM guishi_immediate_match_requests WHERE status<>'complete' ORDER BY order_id")

    def finish(self, order_id: str, failure: GuishiMatchFailure | None = None) -> None:
        with DatabaseUnitOfWork(self.database, immediate=True, require_exists=True) as uow:
            _require_requests(uow)
            expected = {
                "order_id": str(order_id),
                "status": "pending" if failure else "complete",
                "reason": failure.reason if failure else "",
                "subject": failure.subject if failure else "",
                "qiugou_order_id": failure.qiugou_order_id if failure else "",
                "baitan_order_id": failure.baitan_order_id if failure else "",
            }
            written = uow.execute(
                "UPDATE guishi_immediate_match_requests SET status=?,reason=?,subject=?,qiugou_order_id=?,baitan_order_id=? "
                "WHERE order_id=? AND status='pending'",
                ("pending" if failure else "complete", failure.reason if failure else "",
                 failure.subject if failure else "",
                 failure.qiugou_order_id if failure else "", failure.baitan_order_id if failure else "", order_id),
            )
            if written.rowcount != 1:
                current = uow.query_one(
                    "SELECT order_id,status,reason,subject,qiugou_order_id,baitan_order_id "
                    "FROM guishi_immediate_match_requests WHERE order_id=?",
                    (order_id,),
                )
                # Another continuation may already have completed this request.
                if current is None or current != {**expected, "status": "complete", "reason": "", "subject": "",
                                                  "qiugou_order_id": "", "baitan_order_id": ""}:
                    raise DomainError("request_write_rejected", "Guishi continuation update was rejected",
                                      {"reason": "request_write_rejected", "subject": order_id})
                return
            saved = uow.query_one(
                "SELECT order_id,status,reason,subject,qiugou_order_id,baitan_order_id "
                "FROM guishi_immediate_match_requests WHERE order_id=?",
                (order_id,),
            )
            if saved != expected:
                raise DomainError("request_write_rejected", "Guishi continuation update was not persisted exactly",
                                  {"reason": "request_write_rejected", "subject": order_id})
