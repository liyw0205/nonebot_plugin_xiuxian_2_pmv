"""Compatibility entrypoint for feature-owned media parser cache cleanup."""


def cleanup_media_parser_cache(**options):
    from ....features.entertainment.media_parser_cache import (
        cleanup_media_parser_cache as cleanup,
    )

    return cleanup(**options)


__all__ = ["cleanup_media_parser_cache"]
