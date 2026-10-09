"""This owner declares no job.

Sessions are reaped when their child process exits, when the session descriptor
breaks, and during shutdown; an expired authorization window only makes the next
request fail the page-level check, it does not close a live shell.  The shutdown
hook stays in the legacy layer
(``xiuxian/xiuxian_web/system.py:182`` reached via ``bootstrap/legacy.py:36``)
because a scheduled sweep could not reach a process-local session store anyway.
"""

JOBS = ()

__all__ = ["JOBS"]
