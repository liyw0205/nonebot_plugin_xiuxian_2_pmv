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


def test_global_stone_command_uses_resumable_feature_batch():
    root = Path(__file__).parents[1]
    admin = (root / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_admin/__init__.py").read_text(encoding="utf-8")
    handler = admin[admin.index("@gm_command.handle") : admin.index("# GM加思恋结晶")]
    plugin = (root / "nonebot_plugin_xiuxian_2/plugin.py").read_text(encoding="utf-8")
    manifest = (root / "nonebot_plugin_xiuxian_2/features/admin_asset/manifest.py").read_text(encoding="utf-8")
    repository = (root / "nonebot_plugin_xiuxian_2/features/admin_asset/stone_batch_repository.py").read_text(encoding="utf-8")
    assert "update_ls_all(" not in handler
    assert "find_running_stone_batch(" in handler
    assert "admin_asset_application.adjust_stone_batch(" in handler
    assert "run_chunked_until_done" in handler
    assert 'Migration("admin_asset.003", "admin_stone_batch_adjustment_operations", apply_admin_stone_batch)' in plugin
    assert 'migration_version="admin_asset.006"' in manifest
    assert "CREATE TABLE" not in repository
    assert "shutil.disk_usage" in repository


def test_admin_exp_repository_has_no_request_time_ddl():
    root = Path(__file__).parents[1]
    plugin = (root / "nonebot_plugin_xiuxian_2/plugin.py").read_text(encoding="utf-8")
    manifest = (root / "nonebot_plugin_xiuxian_2/features/admin_asset/manifest.py").read_text(encoding="utf-8")
    repository = (root / "nonebot_plugin_xiuxian_2/features/admin_asset/exp_repository.py").read_text(encoding="utf-8")
    migrations = (root / "nonebot_plugin_xiuxian_2/features/admin_asset/migrations.py").read_text(encoding="utf-8")
    admin = (root / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_admin/__init__.py").read_text(encoding="utf-8")
    handler = admin[admin.index("@adjust_exp_command.handle") : admin.index("@zaohua_xiuxian.handle")]
    assert 'Migration("admin_asset.004", "admin_exp_adjustment_operations", apply_admin_exp_adjustment)' in plugin
    assert 'migration_version="admin_asset.006"' in manifest
    assert "CREATE TABLE" not in repository
    assert "def apply_admin_exp_adjustment(" in migrations
    assert 'result.status == "schema_missing"' in handler


def test_admin_item_destroy_repository_has_no_request_time_ddl():
    root = Path(__file__).parents[1]
    plugin = (root / "nonebot_plugin_xiuxian_2/plugin.py").read_text(encoding="utf-8")
    manifest = (root / "nonebot_plugin_xiuxian_2/features/admin_asset/manifest.py").read_text(encoding="utf-8")
    repository = (root / "nonebot_plugin_xiuxian_2/features/admin_asset/item_destroy_repository.py").read_text(encoding="utf-8")
    migrations = (root / "nonebot_plugin_xiuxian_2/features/admin_asset/migrations.py").read_text(encoding="utf-8")
    admin = (root / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_admin/__init__.py").read_text(encoding="utf-8")
    handler = admin[admin.index("@hmll.handle") : admin.index("@restate.handle")]
    assert 'Migration("admin_asset.005", "admin_item_destroy_operations", apply_admin_item_destroy)' in plugin
    assert 'migration_version="admin_asset.006"' in manifest
    assert "CREATE TABLE" not in repository
    assert "def apply_admin_item_destroy(" in migrations
    assert 'result.status == "schema_missing"' in handler


def test_admin_item_grant_repository_has_no_request_time_ddl():
    root = Path(__file__).parents[1]
    plugin = (root / "nonebot_plugin_xiuxian_2/plugin.py").read_text(encoding="utf-8")
    manifest = (root / "nonebot_plugin_xiuxian_2/features/admin_asset/manifest.py").read_text(encoding="utf-8")
    repository = (root / "nonebot_plugin_xiuxian_2/features/admin_asset/item_repository.py").read_text(encoding="utf-8")
    migrations = (root / "nonebot_plugin_xiuxian_2/features/admin_asset/migrations.py").read_text(encoding="utf-8")
    admin = (root / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_admin/__init__.py").read_text(encoding="utf-8")
    handler = admin[admin.index("@cz.handle") : admin.index("@hmll.handle")]
    assert 'Migration("admin_asset.006", "admin_item_grant_operations", apply_admin_item_grant)' in plugin
    assert 'migration_version="admin_asset.006"' in manifest
    assert "CREATE TABLE" not in repository
    assert "def apply_admin_item_grant(" in migrations
    assert 'result.status == "schema_missing"' in handler
