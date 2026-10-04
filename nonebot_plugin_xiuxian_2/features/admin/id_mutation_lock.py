from __future__ import annotations

import os
from contextlib import contextmanager
from pathlib import Path
import threading
from typing import Iterator


_operation_lock = threading.RLock()


@contextmanager
def exclusive_id_mutation_lock(game_database: str | Path) -> Iterator[None]:
    """Serialize ID swaps and renames across threads and bot processes."""
    with _operation_lock:
        lock_path = Path(game_database).with_name(".admin-id-mutation.lock")
        with lock_path.open("a+b") as handle:
            if os.name == "nt":
                import msvcrt

                handle.seek(0, os.SEEK_END)
                if handle.tell() == 0:
                    handle.write(b"0")
                    handle.flush()
                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_LOCK, 1)
            else:
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                if os.name == "nt":
                    import msvcrt

                    handle.seek(0)
                    msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
                else:
                    import fcntl

                    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


__all__ = ["exclusive_id_mutation_lock"]
