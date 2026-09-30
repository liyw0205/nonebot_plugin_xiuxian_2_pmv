from pathlib import Path


def test_admin_asset_application_uses_feature_owned_stone_and_item_repositories():
    source = (Path(__file__).parents[1] / "nonebot_plugin_xiuxian_2/features/admin_asset/application.py").read_text(encoding="utf-8")
    assert "AdminStoneSqlRepository" in source
    assert "AdminItemSqlRepository" in source
    assert "self.repository or LegacyAdminStoneRepository" not in source
    assert "self.item_repository or LegacyAdminItemRepository" not in source


def test_default_composition_uses_migrated_admin_stone_repository():
    root = Path(__file__).parents[1]
    plugin = (root / "nonebot_plugin_xiuxian_2/plugin.py").read_text(encoding="utf-8")
    migrations = (root / "nonebot_plugin_xiuxian_2/features/admin_asset/migrations.py").read_text(encoding="utf-8")
    assert "LegacyAdminStoneRepository" not in plugin
    assert 'Migration("admin_asset.002", "admin_stone_adjustment_operations", apply_admin_stone_adjustment)' in plugin
    assert "def apply_admin_stone_adjustment(" in migrations
