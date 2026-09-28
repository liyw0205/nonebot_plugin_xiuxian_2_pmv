from pathlib import Path
import sqlite3
import tempfile

import nonebot


ROOT = Path(__file__).resolve().parents[1]
PACKAGE = ROOT / "nonebot_plugin_xiuxian_2"


def test_bank_upgrade_and_interest_implementations_are_compatibility_only() -> None:
    transaction = (
        PACKAGE / "xiuxian" / "xiuxian_bank" / "transaction_service.py"
    ).read_text(encoding="utf-8")
    compatibility = (
        PACKAGE / "compatibility" / "legacy_bank_upgrade_interest.py"
    ).read_text(encoding="utf-8")
    assert "class BankUpgradeService" not in transaction
    assert "class BankInterestService" not in transaction
    assert "class BankUpgradeService" in compatibility
    assert "class BankInterestService" in compatibility


def test_bank_legacy_facades_import_explicit_compatibility_module() -> None:
    for name in ("upgrade_service.py", "interest_service.py"):
        source = (
            PACKAGE / "xiuxian" / "xiuxian_bank" / name
        ).read_text(encoding="utf-8")
        assert "legacy_bank_upgrade_interest" in source
        assert "from .transaction_service" not in source


def test_transaction_import_identity_is_preserved() -> None:
    try:
        nonebot.get_driver()
    except ValueError:
        nonebot.init()

    from nonebot_plugin_xiuxian_2.compatibility.legacy_bank_upgrade_interest import (
        BankInterestService as CompatibilityInterestService,
        BankUpgradeService as CompatibilityUpgradeService,
    )
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_bank.transaction_service import (
        BankInterestService,
        BankUpgradeService,
    )

    assert BankUpgradeService is CompatibilityUpgradeService
    assert BankInterestService is CompatibilityInterestService


def test_upgrade_and_interest_handlers_use_read_only_legacy_receipt_boundary() -> None:
    source = (PACKAGE / "xiuxian" / "xiuxian_bank" / "__init__.py").read_text(encoding="utf-8")
    upgrade = source.split("elif mode == '升级会员'", 1)[1].split("elif mode == '信息'", 1)[0]
    interest = source.split("elif mode == '结算'", 1)[1].split("def get_give_stone", 1)[0]
    assert "get_upgrade_result(operation_id)" in upgrade
    assert "_bank_upgrade_service().get_result" not in upgrade
    assert "get_interest_result(operation_id)" in interest
    assert "_bank_interest_service().get_result" not in interest


def test_legacy_bank_receipt_lookup_is_read_only_and_tolerates_missing_tables() -> None:
    from nonebot_plugin_xiuxian_2.compatibility.legacy_bank_operation_receipts import (
        LegacyBankOperationReceiptRepository,
    )

    with tempfile.TemporaryDirectory() as temp_dir:
        database = Path(temp_dir) / "game.sqlite3"
        with sqlite3.connect(database) as connection:
            connection.execute("CREATE TABLE unrelated (id INTEGER PRIMARY KEY)")

        repository = LegacyBankOperationReceiptRepository(database)
        assert repository.get_upgrade_result("old-op") is None
        assert repository.get_interest_result("old-op") is None
        with sqlite3.connect(database) as connection:
            tables = {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        assert tables == {"unrelated"}


def test_legacy_bank_receipt_lookup_reads_existing_upgrade_and_interest_rows() -> None:
    from nonebot_plugin_xiuxian_2.compatibility.legacy_bank_operation_receipts import (
        LegacyBankOperationReceiptRepository,
    )

    with tempfile.TemporaryDirectory() as temp_dir:
        database = Path(temp_dir) / "game.sqlite3"
        with sqlite3.connect(database) as connection:
            connection.execute(
                "CREATE TABLE bank_upgrade_operations (operation_id TEXT PRIMARY KEY, payload TEXT, "
                "cost INTEGER, wallet_stone INTEGER, bank_level TEXT, created_at TEXT)"
            )
            connection.execute(
                "INSERT INTO bank_upgrade_operations VALUES ('up-1', '[]', 50, 150, '2', 'now')"
            )
            connection.execute(
                "CREATE TABLE bank_interest_operations (operation_id TEXT PRIMARY KEY, payload TEXT, "
                "interest INTEGER, wallet_stone INTEGER, saved_at TEXT, created_at TEXT)"
            )
            connection.execute(
                "INSERT INTO bank_interest_operations VALUES ('int-1', '[]', 12, 162, 'then', 'now')"
            )

        repository = LegacyBankOperationReceiptRepository(database)
        assert repository.get_upgrade_result("up-1") == {
            "status": "duplicate",
            "cost": 50,
            "wallet_stone": 150,
            "bank_level": "2",
        }
        assert repository.get_interest_result("int-1") == {
            "status": "duplicate",
            "interest": 12,
            "wallet_stone": 162,
            "saved_at": "then",
        }
