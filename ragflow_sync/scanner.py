from __future__ import annotations

import hashlib
import os
from pathlib import Path
from typing import Dict

from .models import LocalFileSnapshot, REMOTE_NAME_MARKER, SyncState, SyncTargetConfig


def normalize_rel_path(path: Path) -> str:
    return path.as_posix()


def remote_name_for(rel_path: str, filename: str) -> str:
    suffix = Path(filename).suffix.lower()
    stem = Path(filename).stem
    digest = hashlib.sha256(rel_path.encode("utf-8")).hexdigest()[:16]
    return f"{stem}{REMOTE_NAME_MARKER}{digest}{suffix}"


def is_managed_remote_name(name: str) -> bool:
    return REMOTE_NAME_MARKER in Path(name).stem


def file_md5(path: Path) -> str:
    digest = hashlib.md5()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def scan_local_files(config: SyncTargetConfig, state: SyncState, logger) -> Dict[str, LocalFileSnapshot]:
    max_size = config.max_file_size_mb * 1024 * 1024
    result: Dict[str, LocalFileSnapshot] = {}
    for current_root, dir_names, file_names in os.walk(config.local_dir):
        dir_names[:] = [name for name in dir_names if name not in config.ignore_dirs]
        root_path = Path(current_root)
        for file_name in file_names:
            path = root_path / file_name
            if file_name in config.ignore_files:
                continue
            extension = path.suffix.lower()
            if extension not in config.allowed_extensions:
                continue
            stat = path.stat()
            if stat.st_size > max_size:
                continue
            abs_path = str(path.resolve())
            rel_path = normalize_rel_path(path.relative_to(config.local_dir))
            previous = state.files.get(abs_path)
            if previous and previous.mtime_ns == stat.st_mtime_ns and previous.size == stat.st_size:
                md5 = previous.md5
            else:
                md5 = file_md5(path)
            result[abs_path] = LocalFileSnapshot(
                abs_path=abs_path,
                rel_path=rel_path,
                filename=path.name,
                remote_name=remote_name_for(rel_path, path.name),
                extension=extension,
                size=stat.st_size,
                mtime_ns=stat.st_mtime_ns,
                md5=md5,
                path=path,
            )
    logger.info("Scanned local files. count=%s", len(result))
    return result
