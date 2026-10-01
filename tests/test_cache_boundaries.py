from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_sticker_download_archive_is_removed_after_extraction() -> None:
    source = (ROOT / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_web/stickers.py").read_text(
        encoding="utf-8"
    )
    install = source[source.index("def install_stickers(") : source.index("def _run_install_job", source.index("def install_stickers("))]
    assert "try:" in install
    assert "meta = _extract_pack_zip(cache_path, selected_id)" in install
    assert "cache_path.unlink(missing_ok=True)" in install
    assert "MAX_STICKER_ARCHIVE_BYTES" in source
    assert "MAX_STICKER_UNCOMPRESSED_BYTES" in source
    assert "source.read(64 * 1024)" in source
    assert "def _download_file(" in source


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
