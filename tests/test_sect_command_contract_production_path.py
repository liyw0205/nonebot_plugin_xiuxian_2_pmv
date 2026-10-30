"""Production-shaped evidence for the 宗门 command contract (搜寻/创建/加入/传位/任务).

Every defect pinned here needed the REAL legacy table shape (``sects`` carries
``mainbuff``/``secbuff`` and has **no** ``sect_mainbuff``/``sect_secbuff``), the
REAL ``SectApplication`` and the real feature repositories:

* ``xiuxian_sect`` called ``SectApplication.apply_buff_search(..., owner_position=...)``
  while the method accepted no such keyword -> ``TypeError`` on every 搜寻.
* ``SectBuffSearchSqlRepository`` wrote a nonexistent ``sect_mainbuff``/
  ``sect_secbuff`` column.  SQLite resolves an unknown identifier in a SELECT
  list to a string literal (silent wrong read) but raises ``OperationalError``
  on UPDATE, so the whole command died.
* ``SectBuffSearchSqlRepository`` compared the actor's ``sect_position`` against
  a hard-coded 0, so a configured owner position other than 0 was always denied.
* ``SectMutationResult.applied`` omitted the real success statuses
  ``applied``/``charged``/``created``/``opened``/``transferred``/``settled``, so
  six commands rendered a *successful* mutation as a failure and told the
  player to retry.
* The 宗主传位 handler read ``result.succeeded`` on that dict-backed result,
  raising ``AttributeError`` before the success branch.

The pre-existing tests hand-built a table carrying ``sect_mainbuff`` and asserted
against it, so all of the above stayed green locally.
"""

from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.features.sect.application import (
    SectApplication,
    SectMutationResult,
)
from nonebot_plugin_xiuxian_2.features.sect.buff_search_repository import (
    SectBuffSearchSqlRepository,
)
from tests.test_db_backend import db_backend

# Authoritative legacy column sets: base DDL from xiuxian2_handle plus the
# ALTER list in XiuConfig().sql_user_xiuxian / sql_sects / sql_buff / sql_back.
SECTS_DDL = (
    "CREATE TABLE sects ("
    "sect_id INTEGER PRIMARY KEY, sect_name TEXT, sect_owner TEXT, sect_scale INTEGER, "
    "sect_used_stone INTEGER, join_open INTEGER DEFAULT 1, closed INTEGER DEFAULT 0, "
    "combat_power INTEGER DEFAULT 0, sect_fairyland INTEGER, sect_materials INTEGER, "
    "mainbuff TEXT, secbuff TEXT, elixir_room_level INTEGER)"
)
USER_XIUXIAN_DDL = (
    "CREATE TABLE user_xiuxian ("
    "id INTEGER PRIMARY KEY, user_id TEXT UNIQUE, user_name TEXT, stone INTEGER, "
    "exp INTEGER, sect_id INTEGER, sect_position INTEGER, hp INTEGER, mp INTEGER, "
    "sect_task INTEGER DEFAULT 0, sect_contribution INTEGER DEFAULT 0, "
    "sect_elixir_get INTEGER DEFAULT 0)"
)
BUFFINFO_DDL = (
    "CREATE TABLE BuffInfo ("
    "id INTEGER PRIMARY KEY, user_id TEXT, main_buff INTEGER, sec_buff INTEGER)"
)
BACK_DDL = (
    "CREATE TABLE back ("
    "user_id TEXT, goods_id INTEGER, goods_name TEXT, goods_type TEXT, goods_num INTEGER, "
    "bind_num INTEGER, UNIQUE(user_id, goods_id))"
)

# Statuses the sect repositories emit to mean "the mutation happened".
SUCCESS_STATUSES = (
    "applied",
    "charged",
    "created",
    "opened",
    "transferred",
    "settled",
    "claimed",
    "closed",
    "disbanded",
    "granted",
    "inherited",
    "upgraded",
    "duplicate",
)
# Statuses that must NEVER be reported as success.
FAILURE_STATUSES = (
    "not_owner",
    "stone_insufficient",
    "materials_insufficient",
    "scale_insufficient",
    "state_changed",
    "sect_changed",
    "name_exists",
    "already_member",
    "task_missing",
    "task_snapshot_changed",
    "higher_priority",
    "ineligible",
    "target_missing",
    "target_not_member",
    "self_transfer",
    "membership_changed",
    "user_missing",
    "sect_missing",
    "actor_missing",
    "invalid_buff_type",
    "daily_limit",
    "schema_missing",
)


class SectCommandContractProductionPathTests(unittest.TestCase):
    """Drive the affected sect commands the way the bot handlers drive them."""

    def build(
        self,
        temp: str,
        *,
        owner_position: int = 0,
        caller_position: int = 0,
        caller: str = "outsider",
        sect_open: int = 1,
        sect_closed: int = 0,
        used_stone: int = 100000,
        materials: int = 50000,
        mainbuff: str = "[]",
        secbuff: str = "[]",
    ) -> Path:
        database = Path(temp) / "sect-contract.sqlite3"
        with db_backend.transaction(database) as conn:
            conn.execute(SECTS_DDL)
            conn.execute(USER_XIUXIAN_DDL)
            conn.execute(BUFFINFO_DDL)
            conn.execute(BACK_DDL)
            conn.execute(
                "INSERT INTO sects(sect_id,sect_name,sect_owner,sect_scale,sect_used_stone,"
                "sect_materials,mainbuff,secbuff,join_open,closed,elixir_room_level) "
                "VALUES(1,'测试宗','founder',5,?,?,?,?,?,?,1)",
                (used_stone, materials, mainbuff, secbuff, sect_open, sect_closed),
            )
            conn.execute(
                "INSERT INTO user_xiuxian(id,user_id,user_name,sect_id,sect_position,stone,hp,mp) "
                "VALUES(1,'founder','宗主',1,?,500000,500000,500000)",
                (owner_position,),
            )
            if caller != "founder":
                conn.execute(
                    "INSERT INTO user_xiuxian(id,user_id,user_name,sect_id,sect_position,stone,hp,mp) "
                    "VALUES(2,?,?,1,?,500000,500000,500000)",
                    (caller, "道友", caller_position),
                )
            else:
                # caller IS the founder: align its position with the configured one.
                conn.execute(
                    "UPDATE user_xiuxian SET sect_position=? WHERE user_id='founder'",
                    (caller_position,),
                )
            conn.execute(
                "INSERT INTO user_xiuxian(id,user_id,user_name,sect_id,sect_position,stone,hp,mp) "
                "VALUES(3,'loose_cultivator','散修',NULL,99,500000,500000,500000)"
            )
            conn.execute("INSERT INTO BuffInfo(id,user_id,main_buff,sec_buff) VALUES(2,?,0,0)", (caller,))
        # Real task tables, created by the feature's own migration functions.
        self._apply_task_schema(database)
        return database

    @staticmethod
    def _apply_task_schema(database: Path) -> None:
        from nonebot_plugin_xiuxian_2.features.sect.migrations import (
            apply_sect_task_claim_operations,
            apply_sect_task_settlement_operations,
            apply_sect_task_state,
        )
        from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork

        with DatabaseUnitOfWork(str(database)) as uow:
            apply_sect_task_state(uow)
            apply_sect_task_claim_operations(uow)
            apply_sect_task_settlement_operations(uow)

    def value(self, database: Path, sql: str, params=()):
        with db_backend.connection(database) as conn:
            row = conn.execute(sql, params).fetchone()
        return None if row is None else tuple(row)

    def columns(self, database: Path, table: str) -> set[str]:
        with db_backend.connection(database) as conn:
            return {str(row[1]) for row in conn.execute(f'PRAGMA table_info("{table}")')}

    # ------------------------------------------------------------------ shape
    def test_legacy_fixture_really_lacks_the_two_bogus_columns(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = self.build(temp)
            columns = self.columns(database, "sects")
            self.assertIn("mainbuff", columns)
            self.assertIn("secbuff", columns)
            self.assertNotIn("sect_mainbuff", columns)
            self.assertNotIn("sect_secbuff", columns)

    def test_repository_source_has_no_bogus_column_names(self) -> None:
        import ast

        path = (
            Path(__file__).resolve().parents[1]
            / "nonebot_plugin_xiuxian_2"
            / "features"
            / "sect"
            / "buff_search_repository.py"
        )
        tree = ast.parse(path.read_text(encoding="utf-8"))
        # Compare against executable string literals only, so prose comments may
        # still name the historical defect.
        literals = {
            node.value
            for node in ast.walk(tree)
            if isinstance(node, ast.Constant) and isinstance(node.value, str)
        }
        self.assertNotIn("sect_mainbuff", literals)
        self.assertNotIn("sect_secbuff", literals)
        self.assertIn("mainbuff", literals)
        self.assertIn("secbuff", literals)

    def test_application_accepts_owner_position_keyword(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = self.build(temp)
            application = SectApplication(database)
            # Exactly the call shape the 宗门功法搜寻 / 宗门神通搜寻 handlers use.
            result = application.apply_buff_search(
                "op-main", "founder", 1, "main", "[]", "[1,2]", 100, 200, owner_position=0
            )
            self.assertEqual(result.status, "applied")
            self.assertTrue(result.applied)

    # --------------------------------------------------------- success set
    def test_success_set_covers_every_real_success_status(self) -> None:
        for status in SUCCESS_STATUSES:
            with self.subTest(status=status):
                self.assertTrue(SectMutationResult({"status": status}).applied)

    def test_success_set_never_includes_a_failure_status(self) -> None:
        for status in FAILURE_STATUSES:
            with self.subTest(status=status):
                self.assertFalse(SectMutationResult({"status": status}).applied)

    # ------------------------------------------------------------ 功法搜寻
    def test_main_search_applies_on_the_real_schema(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = self.build(temp)
            repository = SectBuffSearchSqlRepository(database)
            result = repository.apply("op", "founder", 1, "main", "[]", "[1,2]", 100, 200)
            self.assertEqual(result["status"], "applied")
            self.assertEqual(
                self.value(database, "SELECT mainbuff,sect_used_stone,sect_materials FROM sects"),
                ("[1,2]", 99900, 49800),
            )

    def test_secondary_search_writes_secbuff(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = self.build(temp)
            repository = SectBuffSearchSqlRepository(database)
            result = repository.apply("op-sec", "founder", 1, "secondary", "[]", "[7]", 10, 20)
            self.assertEqual(result["status"], "applied")
            self.assertEqual(
                self.value(database, "SELECT secbuff,sect_used_stone,sect_materials FROM sects"),
                ("[7]", 99990, 49980),
            )

    def test_search_replay_does_not_deduct_twice(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = self.build(temp)
            repository = SectBuffSearchSqlRepository(database)
            first = repository.apply("op", "founder", 1, "main", "[]", "[1,2]", 100, 200)
            duplicate = repository.apply("op", "founder", 1, "main", "[]", "[9,9]", 999, 999)
            self.assertEqual((first["status"], duplicate["status"]), ("applied", "duplicate"))
            self.assertEqual(
                self.value(database, "SELECT mainbuff,sect_used_stone,sect_materials FROM sects"),
                ("[1,2]", 99900, 49800),
            )
            self.assertEqual(
                self.value(database, "SELECT COUNT(*) FROM sect_buff_search_operations")[0], 1
            )

    def test_search_denied_for_non_owner_without_side_effects(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = self.build(temp, caller="member", caller_position=3)
            result = SectBuffSearchSqlRepository(database).apply(
                "op", "member", 1, "main", "[]", "[1]", 100, 200
            )
            self.assertEqual(result["status"], "not_owner")
            self.assertFalse(SectMutationResult(result).applied)
            self.assertEqual(
                self.value(database, "SELECT mainbuff,sect_used_stone,sect_materials FROM sects"),
                ("[]", 100000, 50000),
            )

    def test_search_reports_insufficient_materials_without_side_effects(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = self.build(temp, materials=10)
            result = SectBuffSearchSqlRepository(database).apply(
                "op", "founder", 1, "main", "[]", "[1]", 100, 200
            )
            self.assertEqual(result["status"], "materials_insufficient")
            self.assertEqual(
                self.value(database, "SELECT mainbuff,sect_used_stone,sect_materials FROM sects"),
                ("[]", 100000, 10),
            )

    def test_search_rejects_unknown_buff_type(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = self.build(temp)
            result = SectBuffSearchSqlRepository(database).apply(
                "op", "founder", 1, "tertiary", "[]", "[1]", 0, 0
            )
            self.assertEqual(result["status"], "invalid_buff_type")
            self.assertEqual(
                self.value(database, "SELECT mainbuff,secbuff FROM sects"), ("[]", "[]")
            )

    # -------------------------------------------------- honour owner_position
    def test_configured_owner_position_other_than_zero_is_accepted(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            # 宗主位配置为 3：旧实现硬编码比较 0，会误判 not_owner。
            database = self.build(temp, owner_position=3, caller="founder", caller_position=3)
            result = SectBuffSearchSqlRepository(database).apply(
                "op", "founder", 1, "main", "[]", "[5]", 10, 20, owner_position=3
            )
            self.assertEqual(result["status"], "applied")
            self.assertEqual(
                self.value(database, "SELECT mainbuff FROM sects"), ("[5]",)
            )

    def test_owner_position_mismatch_is_denied(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            # 同一玩家在配置位为 3 的宗门里，但调用方传入的期望位是 0。
            database = self.build(temp, owner_position=3, caller="founder", caller_position=3)
            result = SectBuffSearchSqlRepository(database).apply(
                "op", "founder", 1, "main", "[]", "[5]", 10, 20, owner_position=0
            )
            self.assertEqual(result["status"], "not_owner")
            self.assertEqual(self.value(database, "SELECT mainbuff FROM sects"), ("[]",))

    # ------------------------------------------------------- 创建/改名/加入
    def test_create_sect_reports_created_as_success(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = self.build(temp)
            application = SectApplication(database)
            result = application.create_sect("c1", "loose_cultivator", "新宗门", 1000, 0)
            self.assertEqual(result.status, "created")
            self.assertTrue(result.applied)
            self.assertEqual(
                self.value(database, "SELECT sect_owner FROM sects WHERE sect_name='新宗门'"),
                ("loose_cultivator",),
            )

    def test_create_sect_duplicate_name_is_not_success(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = self.build(temp)
            result = SectApplication(database).create_sect("c2", "loose_cultivator", "测试宗", 1000, 0)
            self.assertEqual(result.status, "name_exists")
            self.assertFalse(result.applied)

    def test_create_sect_existing_member_is_not_success(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = self.build(temp, caller="member", caller_position=3)
            result = SectApplication(database).create_sect("c3", "member", "新宗门", 1000, 0)
            self.assertEqual(result.status, "already_member")
            self.assertFalse(result.applied)

    def test_charge_name_refresh_reports_charged_as_success(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = self.build(temp)
            result = SectApplication(database).charge_name_refresh("r1", "loose_cultivator", 1000)
            self.assertEqual(result.status, "charged")
            self.assertTrue(result.applied)

    def test_open_join_reports_opened_as_success(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = self.build(temp, sect_open=0, owner_position=0, caller="founder")
            result = SectApplication(database).open_join(
                "j1", "founder", owner_position=0, expected_sect_id=1
            )
            self.assertEqual(result.status, "opened")
            self.assertTrue(result.applied)
            self.assertEqual(self.value(database, "SELECT join_open FROM sects"), (1,))

    def test_close_join_reports_closed_as_success(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = self.build(temp, sect_open=1, owner_position=0, caller="founder")
            result = SectApplication(database).close_join(
                "j2", "founder", owner_position=0, expected_sect_id=1
            )
            self.assertEqual(result.status, "closed")
            self.assertTrue(result.applied)
            self.assertEqual(self.value(database, "SELECT join_open FROM sects"), (0,))

    def test_open_join_denied_for_non_owner(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = self.build(temp, sect_open=0, caller="member", caller_position=3)
            result = SectApplication(database).open_join(
                "j3", "member", owner_position=0, expected_sect_id=1
            )
            self.assertEqual(result.status, "not_owner")
            self.assertFalse(result.applied)

    # ------------------------------------------------------------ 宗主传位
    def test_transfer_owner_reports_transferred_as_success(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = self.build(temp, caller="member", caller_position=3)
            result = SectApplication(database).transfer_owner(
                "t1", "founder", "member", owner_position=0
            )
            self.assertEqual(result.status, "transferred")
            self.assertTrue(result.applied)
            # Result must expose the names the handler renders, and no
            # `succeeded` attribute (that read used to raise AttributeError).
            self.assertEqual(result.actor_name, "宗主")
            self.assertEqual(result.target_name, "道友")
            self.assertEqual(result.sect_name, "测试宗")
            with self.assertRaises(AttributeError):
                result.succeeded

    def test_transfer_owner_denied_for_non_owner(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = self.build(temp, caller="member", caller_position=3)
            result = SectApplication(database).transfer_owner(
                "t2", "member", "founder", owner_position=0
            )
            self.assertEqual(result.status, "not_owner")
            self.assertFalse(result.applied)

    # ---------------------------------------------------------- 任务结算
    def test_task_settlement_reports_settled_as_success(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = self.build(temp, caller="member", caller_position=3)
            application = SectApplication(database)
            # 接取任务后结算：settled 必须算成功。结算 period 必须与接取时一致。
            claim = application.claim_task(
                "task-claim", "member", 1, {"a": {"任务内容": {"exp": 1, "sect": 1}}}, 3,
            )
            self.assertEqual(claim.status, "claimed")
            self.assertTrue(claim.applied)
            settled = application.settle_task(
                "task-settle", "member", 1, claim.period, "hp", 1, 10, 5,
                claim.task_key, claim.task_data,
            )
            self.assertEqual(settled.status, "settled")
            self.assertTrue(settled.applied)

    def test_task_settlement_missing_task_is_not_success(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = self.build(temp, caller="member", caller_position=3)
            application = SectApplication(database)
            result = application.settle_task(
                "task-missing", "member", 1, application.current_task_period(),
                "hp", 1, 10, 5, "a", {"任务内容": {}},
            )
            self.assertEqual(result.status, "task_missing")
            self.assertFalse(result.applied)

    # ------------------------------------------------------ handler contract
    def test_handler_source_contract(self) -> None:
        source = (
            Path(__file__).resolve().parents[1]
            / "nonebot_plugin_xiuxian_2"
            / "xiuxian"
            / "xiuxian_sect"
            / "__init__.py"
        ).read_text(encoding="utf-8")
        # 宗主传位 gates on the dict-backed result's real predicate.
        self.assertIn("if result.applied:", source)
        self.assertNotIn("if result.succeeded:", source)
        # Both 搜寻 handlers pass owner_position through, right after the
        # buff_search operation id.
        search_calls = [
            block
            for block in source.split("apply_buff_search(")[1:]
            if "]" in block
        ]
        self.assertEqual(len(search_calls), 2)
        for block in search_calls:
            call_text = block.split("if not result.applied:")[0]
            self.assertIn("owner_position=owner_position", call_text)
