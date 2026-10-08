from __future__ import annotations

from array import array
from collections import OrderedDict
from datetime import datetime
import os
from pathlib import Path
import re
import threading
import time


class LogFileRepository:
    MAX_ROOTS = 32
    MAX_FILES = 512
    MAX_READ_BYTES = 512 * 1024 * 1024
    MAX_LINE_BYTES = 256 * 1024
    MAX_TAIL_BYTES = 1024 * 1024
    MAX_TAIL_LINES = 1000
    MAX_INDEXED_ROWS = 250_000
    INDEX_CACHE_BYTES = 2 * 1024 * 1024

    def __init__(
        self,
        database: str | Path,
        module_path: str | Path,
        *,
        project_dir: str | None = None,
        cwd: str | Path | None = None,
        home: str | Path | None = None,
        monotonic=time.monotonic,
        year_provider=None,
    ) -> None:
        self.database = Path(database)
        self.module_path = Path(module_path)
        self.project_dir = project_dir if project_dir is not None else os.environ.get("XIUXIAN_PROJECT_DIR")
        self.cwd = Path(cwd) if cwd is not None else Path.cwd()
        self.home = Path(home) if home is not None else Path.home()
        self._monotonic = monotonic
        self._year_provider = year_provider or (lambda: datetime.now().year)
        self._catalog_lock = threading.RLock()
        self._catalog_at = float("-inf")
        self._roots: list[Path] = []
        self._file_map: dict[str, Path] = {}
        self._index_lock = threading.RLock()
        self._read_indexes: OrderedDict[tuple, tuple[int, array]] = OrderedDict()
        self._read_index_bytes = 0
        self._tail_signatures: dict[str, tuple[int, int, int]] = {}

    @staticmethod
    def _resolve_existing_path(path) -> Path | None:
        try:
            candidate = Path(path).expanduser()
            if not candidate.exists():
                return None
            if candidate.is_file():
                candidate = candidate.parent
            return candidate.resolve()
        except Exception:
            return None

    @staticmethod
    def _looks_like_project(path: Path) -> bool:
        indicators = (".env", ".env.dev", "bot.py", "pyproject.toml", "requirements.txt")
        return (
            any((path / name).exists() for name in indicators)
            or (path / "data" / "xiuxian").exists()
            or any((path / name).exists() for name in ("bot", "server", "xiuxianbot", "xiuxianserver"))
        )

    def _roots_snapshot(self) -> list[Path]:
        roots: list[Path] = []
        seen: set[str] = set()

        def add(path) -> None:
            resolved = self._resolve_existing_path(path)
            if resolved is None:
                return
            key = str(resolved)
            if key not in seen and len(roots) < self.MAX_ROOTS:
                seen.add(key)
                roots.append(resolved)

        if self.project_dir:
            add(self.project_dir)
        add(self.cwd)
        if self.cwd.name.lower() in {"bot", "server", "xiuxianbot", "xiuxianserver"}:
            add(self.cwd.parent)
        try:
            add(self.database.resolve().parents[2])
        except Exception:
            pass
        try:
            for parent in self.module_path.resolve().parents:
                if self._looks_like_project(parent):
                    add(parent)
        except Exception:
            pass

        try:
            home = self.home.resolve()
            for name in ("xiu2", "xiuxian", "xiuxian3", "nonebot_plugin_xiuxian_2_pmv"):
                add(home / name)
            try:
                for index, child in enumerate(home.iterdir()):
                    if index >= 256:
                        break
                    if child.is_dir() and ("xiu" in child.name.lower() or "nonebot" in child.name.lower()):
                        add(child)
            except OSError:
                pass
        except Exception:
            pass

        expanded = list(roots)
        for root in roots:
            for child_name in ("bot", "server", "xiuxianbot", "xiuxianserver"):
                child = root / child_name
                if child.is_dir():
                    add(child)
                    if child.resolve() not in expanded:
                        expanded.append(child.resolve())
        return expanded[: self.MAX_ROOTS]

    @staticmethod
    def _supported_file(path: Path) -> bool:
        try:
            if not path.is_file():
                return False
            if path.suffix.lower() in {".gz", ".zip", ".xz", ".bz2", ".7z"}:
                return False
            return ".log" in path.name.lower() or path.parent.name == "logs"
        except OSError:
            return False

    @classmethod
    def _display_name(cls, path: Path, roots: list[Path]) -> str:
        resolved = path.resolve()
        best = str(resolved)
        for root in roots:
            try:
                relative = resolved.relative_to(root.resolve())
            except (OSError, ValueError):
                continue
            label = f"{root.name}/{relative.as_posix()}" if root.name else relative.as_posix()
            if len(label) < len(best):
                best = label
        return best

    def _scan_catalog(self) -> tuple[list[Path], dict[str, Path]]:
        roots = self._roots_snapshot()
        files: list[Path] = []
        for root in roots:
            try:
                for pattern in ("*.log", "*.log.*"):
                    files.extend(path for path in root.glob(pattern) if self._supported_file(path))
                log_dir = root / "logs"
                if log_dir.is_dir():
                    files.extend(path for path in log_dir.glob("*") if self._supported_file(path))
            except OSError:
                continue

        unique: dict[str, Path] = {}
        for path in files:
            try:
                unique[str(path.resolve())] = path
            except OSError:
                continue

        result = list(unique.values())

        def mtime(path: Path) -> float:
            try:
                return path.stat().st_mtime
            except OSError:
                return 0

        result.sort(key=mtime, reverse=True)
        result = result[: self.MAX_FILES]
        file_map: dict[str, Path] = {}
        for path in result:
            try:
                file_map[str(path.resolve())] = path
                file_map.setdefault(path.name, path)
                file_map.setdefault(self._display_name(path, roots), path)
            except OSError:
                continue
        return roots, file_map

    def _catalog(self) -> tuple[list[Path], dict[str, Path]]:
        with self._catalog_lock:
            now = self._monotonic()
            if now - self._catalog_at >= 3:
                self._roots, self._file_map = self._scan_catalog()
                self._catalog_at = now
            return list(self._roots), dict(self._file_map)

    def files(self) -> dict:
        roots, file_map = self._catalog()
        entries = []
        unique = {str(path.resolve()): path for path in file_map.values()}
        for path in unique.values():
            try:
                stat = path.stat()
                resolved = str(path.resolve())
                entries.append({
                    "id": resolved,
                    "name": path.name,
                    "display_name": self._display_name(path, roots),
                    "path": resolved,
                    "size": stat.st_size,
                    "mtime": datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
                })
            except OSError:
                continue
        entries.sort(key=lambda item: item["mtime"], reverse=True)
        return {"success": True, "files": entries, "searched_roots": [str(path) for path in roots]}

    @staticmethod
    def _parse_datetime(value: str):
        if not value:
            return None
        normalized = str(value).strip().replace("T", " ")
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
            try:
                return datetime.strptime(normalized, fmt)
            except ValueError:
                continue
        raise ValueError(f"time data '{value}' does not match supported formats")

    @staticmethod
    def _strip_ansi(line: str) -> str:
        if not line:
            return ""
        text = str(line)
        text = re.sub(r"\x1b\][^\x07\x1b]*(?:\x07|\x1b\\)?", "", text)
        text = re.sub(r"\x1b\[[0-9;?]*[ -/]*[@-~]", "", text)
        text = re.sub(r"\x1b.", "", text)
        text = re.sub(r"\[[0-9;]*m", "", text)
        return re.sub(r"\\x1b\[[0-9;]*m", "", text, flags=re.I)

    @staticmethod
    def _parse_level(line: str) -> str:
        upper = (line or "").upper()
        for level in ("TRACE", "DEBUG", "INFO", "SUCCESS", "WARNING", "ERROR", "CRITICAL"):
            if level in upper:
                return level
        return "UNKNOWN"

    def _parse_line_time(self, line: str):
        clean = self._strip_ansi(line)
        match = re.search(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})", clean)
        if match:
            try:
                return datetime.strptime(match.group(1), "%Y-%m-%d %H:%M:%S")
            except ValueError:
                pass
        match = re.search(r"(\d{2}-\d{2} \d{2}:\d{2}:\d{2})", clean)
        if match:
            try:
                year = int(self._year_provider())
                return datetime.strptime(f"{year}-{match.group(1)}", "%Y-%m-%d %H:%M:%S")
            except ValueError:
                pass
        return None

    def _row(self, raw: bytes) -> dict:
        text = raw.decode("utf-8", errors="ignore").rstrip("\r\n")
        clean = self._strip_ansi(text)
        parsed_time = self._parse_line_time(text)
        return {
            "time": parsed_time.strftime("%Y-%m-%d %H:%M:%S") if parsed_time else "",
            "level": self._parse_level(clean),
            "text": clean,
        }

    def _filter_row(self, raw: bytes, keyword: str, level: str, start_obj, end_obj) -> dict | None:
        row = self._row(raw)
        if keyword and keyword not in row["text"]:
            return None
        if level and level != "ALL" and row["level"] != level:
            return None
        parsed_time = self._parse_line_time(raw.decode("utf-8", errors="ignore"))
        if start_obj and parsed_time and parsed_time < start_obj:
            return None
        if end_obj and parsed_time and parsed_time > end_obj:
            return None
        return row

    @staticmethod
    def _date_bounds(start: str, end: str):
        start_obj = LogFileRepository._parse_datetime(start) if start else None
        end_obj = LogFileRepository._parse_datetime(end) if end else None
        if end and re.fullmatch(r"\d{4}-\d{2}-\d{2}", end):
            end_obj = end_obj.replace(hour=23, minute=59, second=59)
        return start_obj, end_obj

    @staticmethod
    def _signature(path: Path):
        stat = path.stat()
        return stat, (stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns)

    def _resolve_file(self, file_name: str, roots: list[Path], file_map: dict[str, Path]) -> Path | None:
        if file_name not in file_map:
            return None
        path = file_map[file_name]
        try:
            resolved = path.resolve(strict=True)
            if not resolved.is_file():
                return None
            return resolved
        except OSError:
            return None

    @staticmethod
    def _read_index_key(path: Path, signature: tuple, keyword: str, level: str, start: str, end: str) -> tuple:
        return (str(path),) + signature + (keyword, level, start, end)

    def _put_index(self, key: tuple, total: int, offsets: array) -> None:
        size = len(offsets) * offsets.itemsize
        if size > self.INDEX_CACHE_BYTES:
            return
        with self._index_lock:
            previous = self._read_indexes.pop(key, None)
            if previous:
                self._read_index_bytes -= len(previous[1]) * previous[1].itemsize
            self._read_indexes[key] = (total, offsets)
            self._read_index_bytes += size
            while self._read_indexes and self._read_index_bytes > self.INDEX_CACHE_BYTES:
                _, (_, oldest) = self._read_indexes.popitem(last=False)
                self._read_index_bytes -= len(oldest) * oldest.itemsize

    def _get_index(self, key: tuple):
        with self._index_lock:
            value = self._read_indexes.pop(key, None)
            if value is None:
                return None
            self._read_indexes[key] = value
            return value

    def read(self, file_name: str, keyword: str, level: str, start: str, end: str, page: int, page_size: int) -> dict:
        roots, file_map = self._catalog()
        target = self._resolve_file(file_name, roots, file_map)
        if target is None:
            return {"success": False, "error": "日志文件不存在或不允许访问"}
        stat, signature = self._signature(target)
        if stat.st_size > self.MAX_READ_BYTES:
            return {"success": False, "error": f"日志文件超过 {self.MAX_READ_BYTES // (1024 * 1024)} MiB 读取上限，请使用实时模式"}
        start_obj, end_obj = self._date_bounds(start, end)
        key = self._read_index_key(target, signature, keyword, level, start, end)
        cached = self._get_index(key)
        offsets = cached[1] if cached else None
        start_idx = (page - 1) * page_size
        end_idx = start_idx + page_size

        if cached is not None:
            total = cached[0]
            rows = []
            response_bytes = 0
            with target.open("rb") as stream:
                for line_offset in offsets[start_idx:end_idx]:
                    stream.seek(line_offset)
                    raw = stream.readline(self.MAX_LINE_BYTES + 1)
                    if len(raw) > self.MAX_LINE_BYTES and not raw.endswith(b"\n"):
                        return {"success": False, "error": "日志行超过单行读取上限"}
                    response_bytes += len(raw)
                    if response_bytes > 4 * 1024 * 1024:
                        return {"success": False, "error": "当前页日志内容超过 4 MiB 响应上限"}
                    row = self._row(raw)
                    if row is not None:
                        rows.append(row)
        else:
            count = 0
            page_rows = []
            found_offsets = array("Q")
            scanned_bytes = 0
            response_bytes = 0
            with target.open("rb") as stream:
                while True:
                    line_offset = stream.tell()
                    raw = stream.readline(self.MAX_LINE_BYTES + 1)
                    if not raw:
                        break
                    scanned_bytes += len(raw)
                    if scanned_bytes > self.MAX_READ_BYTES:
                        return {"success": False, "error": f"日志文件超过 {self.MAX_READ_BYTES // (1024 * 1024)} MiB 读取上限，请使用实时模式"}
                    if len(raw) > self.MAX_LINE_BYTES and not raw.endswith(b"\n"):
                        return {"success": False, "error": "日志行超过单行读取上限"}
                    if self._filter_row(raw, keyword, level, start_obj, end_obj) is None:
                        continue
                    if count < self.MAX_INDEXED_ROWS:
                        found_offsets.append(line_offset)
                    if start_idx <= count < end_idx:
                        response_bytes += len(raw)
                        if response_bytes > 4 * 1024 * 1024:
                            return {"success": False, "error": "当前页日志内容超过 4 MiB 响应上限"}
                        page_rows.append(self._row(raw))
                    count += 1
            total = count
            rows = page_rows
            if len(found_offsets) == total:
                self._put_index(key, total, found_offsets)

        return {
            "success": True,
            "file": str(target),
            "name": target.name,
            "display_name": self._display_name(target, roots),
            "total": total,
            "page": page,
            "page_size": page_size,
            "rows": rows if start_idx < total else [],
        }

    def tail(
        self,
        file_name: str,
        offset: int,
        keyword: str,
        level: str,
        start: str,
        end: str,
        ignore_unknown: bool,
        ignore_keywords: list[str],
    ) -> dict:
        roots, file_map = self._catalog()
        target = self._resolve_file(file_name, roots, file_map)
        if target is None:
            return {"success": False, "error": "日志文件不存在或不允许访问"}
        stat, _ = self._signature(target)
        resolved = str(target)
        current_identity = (stat.st_dev, stat.st_ino)
        previous = self._tail_signatures.get(resolved)
        if previous and (previous[:2] != current_identity or stat.st_size < previous[2]):
            offset = 0
        if offset < 0 or offset > stat.st_size:
            offset = 0
        start_obj, end_obj = self._date_bounds(start, end)

        lines = []
        next_offset = offset
        with target.open("rb") as stream:
            stream.seek(offset)
            while len(lines) < self.MAX_TAIL_LINES and stream.tell() - offset < self.MAX_TAIL_BYTES:
                line_start = stream.tell()
                raw = stream.readline(self.MAX_LINE_BYTES + 1)
                if not raw:
                    break
                overlong = len(raw) > self.MAX_LINE_BYTES and not raw.endswith(b"\n")
                if overlong:
                    raw = raw[: self.MAX_LINE_BYTES].rstrip(b"\r\n") + b"...[line fragment truncated]\n"
                elif not raw.endswith(b"\n"):
                    next_offset = line_start
                    break
                next_offset = stream.tell()
                clean = self._strip_ansi(raw.decode("utf-8", errors="ignore").rstrip("\r\n"))
                if keyword and keyword not in clean:
                    continue
                row_level = self._parse_level(clean)
                if level and level != "ALL" and row_level != level:
                    continue
                if ignore_unknown and row_level == "UNKNOWN":
                    continue
                if ignore_keywords and any(value in clean for value in ignore_keywords):
                    continue
                parsed_time = self._parse_line_time(raw.decode("utf-8", errors="ignore"))
                if start_obj and parsed_time and parsed_time < start_obj:
                    continue
                if end_obj and parsed_time and parsed_time > end_obj:
                    continue
                lines.append({
                    "time": parsed_time.strftime("%Y-%m-%d %H:%M:%S") if parsed_time else "",
                    "level": row_level,
                    "text": clean,
                })

        try:
            final_stat = target.stat()
            self._tail_signatures[resolved] = (final_stat.st_dev, final_stat.st_ino, final_stat.st_size)
        except OSError:
            pass
        return {
            "success": True,
            "file": resolved,
            "name": target.name,
            "display_name": self._display_name(target, roots),
            "offset": offset,
            "next_offset": next_offset,
            "lines": lines,
        }
