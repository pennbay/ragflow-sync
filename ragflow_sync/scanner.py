from __future__ import annotations

import hashlib
import os
from pathlib import Path
from typing import Dict, NamedTuple, Optional

from .models import (
    LocalFileSnapshot,
    REMOTE_NAME_MAX_BYTES,
    REMOTE_MD5_MARKER,
    REMOTE_NAME_MARKER,
    SyncState,
    SyncTargetConfig,
)


class ManagedRemoteName(NamedTuple):
    path_digest: str
    md5: str
    suffix: str


def normalize_rel_path(path: Path) -> str:
    return path.as_posix()


def path_digest_for(rel_path: str) -> str:
    return hashlib.sha256(rel_path.encode("utf-8")).hexdigest()[:16]


def utf8_truncate(value: str, max_bytes: int, fallback: str = "file") -> str:
    if max_bytes <= 0:
        return fallback
    result = []
    used = 0
    for char in value:
        char_size = len(char.encode("utf-8"))
        if used + char_size > max_bytes:
            break
        result.append(char)
        used += char_size
    truncated = "".join(result).rstrip()
    return truncated or fallback


def remote_name_for(rel_path: str, filename: str, md5: str) -> str:
    suffix = Path(filename).suffix.lower()
    stem = Path(filename).stem
    path_digest = path_digest_for(rel_path)
    identity_segment = f"{REMOTE_NAME_MARKER}{path_digest}{REMOTE_MD5_MARKER}{md5}{suffix}"
    stem_budget = REMOTE_NAME_MAX_BYTES - len(identity_segment.encode("utf-8"))
    safe_stem = utf8_truncate(stem, stem_budget)
    remote_name = f"{safe_stem}{identity_segment}"
    if len(remote_name.encode("utf-8")) > REMOTE_NAME_MAX_BYTES:
        raise ValueError(f"Remote name exceeds {REMOTE_NAME_MAX_BYTES} bytes: {remote_name}")
    return remote_name


def parse_managed_remote_name(name: str) -> Optional[ManagedRemoteName]:
    path = Path(name)
    stem = path.stem
    suffix = path.suffix.lower()
    if REMOTE_NAME_MARKER not in stem or REMOTE_MD5_MARKER not in stem:
        return None
    _, rest = stem.rsplit(REMOTE_NAME_MARKER, 1)
    if REMOTE_MD5_MARKER not in rest:
        return None
    path_digest, md5 = rest.split(REMOTE_MD5_MARKER, 1)
    if len(path_digest) != 16 or any(ch not in "0123456789abcdef" for ch in path_digest):
        return None
    if len(md5) != 32 or any(ch not in "0123456789abcdef" for ch in md5):
        return None
    return ManagedRemoteName(path_digest=path_digest, md5=md5, suffix=suffix)


def is_managed_remote_name(name: str) -> bool:
    return parse_managed_remote_name(name) is not None


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
            path_digest = path_digest_for(rel_path)
            result[abs_path] = LocalFileSnapshot(
                abs_path=abs_path,
                rel_path=rel_path,
                filename=path.name,
                remote_name=remote_name_for(rel_path, path.name, md5),
                path_digest=path_digest,
                extension=extension,
                size=stat.st_size,
                mtime_ns=stat.st_mtime_ns,
                md5=md5,
                path=path,
            )
    logger.info("Scanned local files. count=%s", len(result))
    return result
