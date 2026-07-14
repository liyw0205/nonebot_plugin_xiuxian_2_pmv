from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class ArenaWeeklyRankReductionResult:
    status: str
    business_week: str
    task_status: str = ""
    reduce_steps: int = 0
    total: int = 0
    completed: int = 0
    changed: int = 0
    skipped: int = 0
    conflicted: int = 0
    last_error: str = ""

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class ArenaWeeklyRankReductionService:
    """Reduce a week-frozen arena player set in durable chunks."""

    RANKS = ("青铜", "白银", "黄金", "铂金", "钻石", "王者")
    INITIAL_SCORES = {
        "青铜": 1000,
        "白银": 1500,
        "黄金": 1900,
        "铂金": 2300,
        "钻石": 2700,
        "王者": 3200,
    }

    def __init__(self, player_database: str | Path, lock: RLock | None = None) -> None:
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    @staticmethod
    def _normalize_week(value) -> str:
        if value is None:
            value = date.today()
        if isinstance(value, datetime):
            value = value.date()
        if isinstance(value, date):
            iso = value.isocalendar()
            return f"{iso.year}-W{iso.week:02d}"
        text = str(value).strip()
        if "-W" in text:
            year_text, week_text = text.split("-W", 1)
            monday = date.fromisocalendar(int(year_text), int(week_text), 1)
            iso = monday.isocalendar()
            return f"{iso.year}-W{iso.week:02d}"
        return ArenaWeeklyRankReductionService._normalize_week(date.fromisoformat(text))

    @classmethod
    def _rank_for_score(cls, score: int) -> str:
        if score >= 3200:
            return "王者"
        if score >= 2700:
            return "钻石"
        if score >= 2300:
            return "铂金"
        if score >= 1900:
            return "黄金"
        if score >= 1500:
            return "白银"
        return "青铜"

    @classmethod
    def _target(cls, score: int, rank: str, reduce_steps: int) -> tuple[str, int]:
        current_rank = rank if rank in cls.RANKS else cls._rank_for_score(score)
        target_index = max(0, cls.RANKS.index(current_rank) - reduce_steps)
        target_rank = cls.RANKS[target_index]
        return target_rank, cls.INITIAL_SCORES[target_rank]

    @classmethod
    def _ensure_schema(cls, conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS arena(user_id TEXT PRIMARY KEY,score INTEGER DEFAULT 1000,"
            "rank TEXT DEFAULT '青铜',win_streak INTEGER DEFAULT 0)"
        )
        columns = set(conn.column_names("arena"))
        missing = {
            "score": "INTEGER DEFAULT 1000",
            "rank": "TEXT DEFAULT '青铜'",
            "win_streak": "INTEGER DEFAULT 0",
        }
        for name, definition in missing.items():
            if name not in columns:
                conn.execute(
                    f"ALTER TABLE arena ADD COLUMN {db_backend.quote_ident(name)} {definition}"
                )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS arena_weekly_rank_reduction_operations("
            "business_week TEXT PRIMARY KEY,reduce_steps INTEGER NOT NULL,total INTEGER NOT NULL,"
            "completed INTEGER NOT NULL DEFAULT 0,changed INTEGER NOT NULL DEFAULT 0,"
            "skipped INTEGER NOT NULL DEFAULT 0,conflicted INTEGER NOT NULL DEFAULT 0,"
            "status TEXT NOT NULL DEFAULT 'running',last_error TEXT NOT NULL DEFAULT '',"
            "created_at TEXT NOT NULL,updated_at TEXT NOT NULL)"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS arena_weekly_rank_reduction_targets("
            "business_week TEXT NOT NULL,user_id TEXT NOT NULL,ordinal INTEGER NOT NULL,"
            "previous_score INTEGER NOT NULL,previous_rank TEXT NOT NULL,"
            "previous_win_streak INTEGER NOT NULL,target_score INTEGER NOT NULL,"
            "target_rank TEXT NOT NULL,status TEXT NOT NULL DEFAULT 'pending',"
            "error_text TEXT NOT NULL DEFAULT '',updated_at TEXT NOT NULL,"
            "PRIMARY KEY(business_week,user_id))"
        )

    @staticmethod
    def _result(conn, business_week: str, status: str) -> ArenaWeeklyRankReductionResult:
        row = conn.execute(
            "SELECT status,reduce_steps,total,completed,changed,skipped,conflicted,last_error "
            "FROM arena_weekly_rank_reduction_operations WHERE business_week=%s",
            (business_week,),
        ).fetchone()
        if row is None:
            return ArenaWeeklyRankReductionResult(status, business_week)
        return ArenaWeeklyRankReductionResult(
            status=status,
            business_week=business_week,
            task_status=str(row[0]),
            reduce_steps=int(row[1]),
            total=int(row[2]),
            completed=int(row[3]),
            changed=int(row[4]),
            skipped=int(row[5]),
            conflicted=int(row[6]),
            last_error=str(row[7] or ""),
        )

    def reduce(
        self,
        business_week=None,
        reduce_steps=2,
        *,
        chunk_size=500,
        updated_at=None,
    ) -> ArenaWeeklyRankReductionResult:
        business_week = self._normalize_week(business_week)
        reduce_steps = max(0, int(reduce_steps))
        chunk_size = max(1, int(chunk_size))
        updated_at = str(
            updated_at or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        )

        with self._lock, closing(db_backend.connect(self._player_database)) as conn:
            operation_created = False
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                operation = conn.execute(
                    "SELECT reduce_steps,status FROM arena_weekly_rank_reduction_operations "
                    "WHERE business_week=%s",
                    (business_week,),
                ).fetchone()
                if operation is None:
                    users = tuple(
                        (
                            str(row[0]),
                            int(row[1] or 0),
                            str(row[2] or ""),
                            int(row[3] or 0),
                        )
                        for row in conn.execute(
                            "SELECT user_id,COALESCE(score,1000),COALESCE(rank,''),"
                            "COALESCE(win_streak,0) FROM arena ORDER BY user_id"
                        ).fetchall()
                    )
                    task_status = "completed" if not users else "running"
                    conn.execute(
                        "INSERT INTO arena_weekly_rank_reduction_operations("
                        "business_week,reduce_steps,total,status,created_at,updated_at) "
                        "VALUES(%s,%s,%s,%s,%s,%s)",
                        (
                            business_week,
                            reduce_steps,
                            len(users),
                            task_status,
                            updated_at,
                            updated_at,
                        ),
                    )
                    targets = []
                    for ordinal, (user_id, score, rank, streak) in enumerate(users):
                        target_rank, target_score = self._target(
                            score, rank, reduce_steps
                        )
                        targets.append(
                            (
                                business_week,
                                user_id,
                                ordinal,
                                score,
                                rank,
                                streak,
                                target_score,
                                target_rank,
                                updated_at,
                            )
                        )
                    conn.executemany(
                        "INSERT INTO arena_weekly_rank_reduction_targets("
                        "business_week,user_id,ordinal,previous_score,previous_rank,"
                        "previous_win_streak,target_score,target_rank,updated_at) "
                        "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                        targets,
                    )
                    conn.commit()
                    operation_created = True
                    if not users:
                        return self._result(conn, business_week, "applied")
                else:
                    if int(operation[0]) != reduce_steps:
                        result = self._result(
                            conn, business_week, "operation_conflict"
                        )
                        conn.rollback()
                        return result
                    if str(operation[1]) == "completed":
                        result = self._result(conn, business_week, "duplicate")
                        conn.rollback()
                        return result
                    conn.commit()

                conn.execute("BEGIN IMMEDIATE")
                pending = conn.execute(
                    "SELECT user_id,previous_score,previous_rank,previous_win_streak,"
                    "target_score,target_rank FROM arena_weekly_rank_reduction_targets "
                    "WHERE business_week=%s AND status='pending' ORDER BY ordinal LIMIT %s",
                    (business_week, chunk_size),
                ).fetchall()
                changed = 0
                skipped = 0
                conflicted = 0
                for row in pending:
                    user_id = str(row[0])
                    previous = (int(row[1]), str(row[2]), int(row[3]))
                    target = (int(row[4]), str(row[5]), 0)
                    current = conn.execute(
                        "SELECT COALESCE(score,1000),COALESCE(rank,''),"
                        "COALESCE(win_streak,0) FROM arena WHERE user_id=%s",
                        (user_id,),
                    ).fetchone()
                    if current is None:
                        skipped += 1
                        conn.execute(
                            "UPDATE arena_weekly_rank_reduction_targets SET status='skipped',"
                            "error_text='user_missing',updated_at=%s WHERE business_week=%s "
                            "AND user_id=%s AND status='pending'",
                            (updated_at, business_week, user_id),
                        )
                        continue
                    actual = (int(current[0]), str(current[1]), int(current[2]))
                    if actual != previous:
                        skipped += 1
                        conflicted += 1
                        conn.execute(
                            "UPDATE arena_weekly_rank_reduction_targets SET status='conflict',"
                            "error_text='state_changed',updated_at=%s WHERE business_week=%s "
                            "AND user_id=%s AND status='pending'",
                            (updated_at, business_week, user_id),
                        )
                        continue
                    if actual != target:
                        updated = conn.execute(
                            "UPDATE arena SET score=%s,rank=%s,win_streak=0 WHERE user_id=%s "
                            "AND COALESCE(score,1000)=%s AND COALESCE(rank,'')=%s "
                            "AND COALESCE(win_streak,0)=%s",
                            (
                                target[0],
                                target[1],
                                user_id,
                                previous[0],
                                previous[1],
                                previous[2],
                            ),
                        )
                        if updated.rowcount != 1:
                            raise db_backend.IntegrityError(
                                "arena weekly rank target changed"
                            )
                        changed += 1
                    conn.execute(
                        "UPDATE arena_weekly_rank_reduction_targets SET status='applied',"
                        "error_text='',updated_at=%s WHERE business_week=%s AND user_id=%s "
                        "AND status='pending'",
                        (updated_at, business_week, user_id),
                    )

                progress = conn.execute(
                    "SELECT COUNT(*),COALESCE(SUM(CASE WHEN status='pending' THEN 1 ELSE 0 END),0) "
                    "FROM arena_weekly_rank_reduction_targets WHERE business_week=%s",
                    (business_week,),
                ).fetchone()
                completed = int(progress[0]) - int(progress[1])
                task_status = "completed" if int(progress[1]) == 0 else "running"
                conn.execute(
                    "UPDATE arena_weekly_rank_reduction_operations SET completed=%s,"
                    "changed=changed+%s,skipped=skipped+%s,conflicted=conflicted+%s,"
                    "status=%s,last_error='',updated_at=%s WHERE business_week=%s",
                    (
                        completed,
                        changed,
                        skipped,
                        conflicted,
                        task_status,
                        updated_at,
                        business_week,
                    ),
                )
                result = self._result(conn, business_week, "applied")
                conn.commit()
                return result
            except Exception as exc:
                conn.rollback()
                if operation_created or self._operation_exists(conn, business_week):
                    try:
                        conn.execute("BEGIN IMMEDIATE")
                        conn.execute(
                            "UPDATE arena_weekly_rank_reduction_operations SET "
                            "last_error=%s,updated_at=%s WHERE business_week=%s",
                            (str(exc), updated_at, business_week),
                        )
                        conn.commit()
                    except Exception:
                        conn.rollback()
                raise

    @staticmethod
    def _operation_exists(conn, business_week: str) -> bool:
        try:
            return conn.execute(
                "SELECT 1 FROM arena_weekly_rank_reduction_operations "
                "WHERE business_week=%s",
                (business_week,),
            ).fetchone() is not None
        except Exception:
            return False


__all__ = [
    "ArenaWeeklyRankReductionResult",
    "ArenaWeeklyRankReductionService",
]
