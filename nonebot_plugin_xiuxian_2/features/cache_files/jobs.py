"""This owner declares no job. Cache eviction is owned by other slices: the only cache-cleaning
scheduler job, ``cleanup_media_parser_cache_job``
(``xiuxian/xiuxian_scheduler/__init__.py:343-352``), calls
``entertainment_application.media_parser.cleanup_cache`` and belongs to the entertainment owner.
"""

JOBS = ()

__all__ = ["JOBS"]
