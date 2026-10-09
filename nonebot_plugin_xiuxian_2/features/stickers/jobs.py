"""This owner declares no scheduler job. An install runs as a one-shot daemon thread tracked in
``StickerApplication._jobs`` under ``_jobs_lock``; the job map is in-process only and is dropped
with the process, so a restart loses progress but never replays a download.
"""

JOBS = ()

__all__ = ["JOBS"]
