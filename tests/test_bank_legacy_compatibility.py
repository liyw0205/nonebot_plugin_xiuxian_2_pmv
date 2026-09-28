from pathlib import Path


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
