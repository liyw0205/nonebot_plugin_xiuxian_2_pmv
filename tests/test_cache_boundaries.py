import ast
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_sticker_download_archive_is_removed_after_extraction() -> None:
    source = (ROOT / "nonebot_plugin_xiuxian_2/features/stickers/repository.py").read_text(
        encoding="utf-8"
    )
    tree = ast.parse(source)
    method = next(node for node in ast.walk(tree) if isinstance(node, ast.FunctionDef) and node.name == "install_pack")
    install = ast.get_source_segment(source, method)
    assert "try:" in install
    assert "self._extract_to_staging(archive_path, selected_id)" in install
    cleanup = [ast.unparse(statement) for node in ast.walk(method) if isinstance(node, ast.Try) for statement in node.finalbody]
    assert "archive_path.unlink(missing_ok=True)" in cleanup
    assert "MAX_STICKER_ARCHIVE_BYTES" in source
    assert "MAX_STICKER_UNCOMPRESSED_BYTES" in source
    assert "source.read(DOWNLOAD_CHUNK_BYTES)" in source
    schemas = (ROOT / "nonebot_plugin_xiuxian_2/features/stickers/schemas.py").read_text(encoding="utf-8")
    assert "DOWNLOAD_CHUNK_BYTES = 64 * 1024" in schemas
    assert "def _download_file(" in source
    facade = (ROOT / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_web/stickers.py").read_text(encoding="utf-8")
    application = (ROOT / "nonebot_plugin_xiuxian_2/features/stickers/application.py").read_text(encoding="utf-8")
    assert "sticker_application.start_install(" in facade
    assert "self._repository.install_pack(" in application


def test_web_upload_cache_has_age_and_count_bounds() -> None:
    source = (ROOT / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_web/core.py").read_text(
        encoding="utf-8"
    )
    assert "WEB_UPLOAD_CACHE_MAX_FILES" in source
    assert "WEB_UPLOAD_CACHE_MAX_AGE_SECONDS" in source
    assert "def _cleanup_web_upload_cache()" in source
    assert "path.unlink()" in source
    assert "_cleanup_web_upload_cache()" in source[source.index("def initialize_web_storage(") : source.index("def api_success(")]
    save = source[source.index("def save_uploaded_media(") :]
    assert "_cleanup_web_upload_cache()" in save
