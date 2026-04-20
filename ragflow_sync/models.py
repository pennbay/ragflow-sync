from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional


DEFAULT_ALLOWED_EXTENSIONS = {
    ".pdf",
    ".doc",
    ".docx",
    ".ppt",
    ".pptx",
    ".md",
    ".markdown",
}
REMOTE_NAME_MARKER = "__rf__"
REMOTE_MD5_MARKER = "__md5__"
RAGFLOW_FILENAME_MAX_BYTES = 255
REMOTE_FILENAME_RESERVE_BYTES = 16
REMOTE_NAME_MAX_BYTES = RAGFLOW_FILENAME_MAX_BYTES - REMOTE_FILENAME_RESERVE_BYTES
STATE_VERSION = 2


class ConfigError(RuntimeError):
    pass


class SyncError(RuntimeError):
    pass


class SyncApiError(SyncError):
    pass


class ParseRunStatus(str, Enum):
    UNSTART = "UNSTART"
    RUNNING = "RUNNING"
    DONE = "DONE"
    FAIL = "FAIL"
    CANCEL = "CANCEL"
    UNKNOWN = "UNKNOWN"

    @classmethod
    def from_raw(cls, value: object) -> "ParseRunStatus":
        raw = str(value or "").strip().upper()
        for item in cls:
            if item.value == raw:
                return item
        return cls.UNKNOWN


@dataclass(frozen=True)
class SyncTargetConfig:
    api_key: str
    base_url: str
    dataset_name: str
    local_dir: Path
    allowed_extensions: set[str]
    ignore_dirs: set[str]
    ignore_files: set[str]
    max_file_size_mb: int
    remote_page_size: int
    parse_trigger_batch_size: int
    api_retry_times: int
    api_retry_interval_seconds: float
    api_timeout_seconds: float
    upload_retry_times: int
    upload_retry_interval_seconds: float
    upload_timeout_seconds: float
    log_level: str
    state_dir: Path
    log_dir: Path
    max_parse_retry_times: int

    @property
    def state_path(self) -> Path:
        return self.state_dir / f"{safe_slug(self.dataset_name)}.json"

    @property
    def log_path(self) -> Path:
        return self.log_dir / f"{safe_slug(self.dataset_name)}.log"


@dataclass(frozen=True)
class DatasetRef:
    dataset_id: str
    dataset_name: str


@dataclass(frozen=True)
class LocalFileSnapshot:
    abs_path: str
    rel_path: str
    filename: str
    remote_name: str
    path_digest: str
    extension: str
    size: int
    mtime_ns: int
    md5: str
    path: Path


@dataclass(frozen=True)
class RemoteDocumentSnapshot:
    document_id: str
    name: str
    run_status: ParseRunStatus
    progress: float
    chunk_count: int
    token_count: int
    size: int = 0
    meta_fields: Dict[str, object] = field(default_factory=dict)


@dataclass
class TrackedFileState:
    abs_path: str
    rel_path: str
    remote_name: str
    path_digest: str
    document_id: str
    md5: str
    mtime_ns: int
    size: int
    parse_retry_count: int = 0
    last_parse_trigger_at: str = ""
    last_upload_at: str = ""
    last_error: str = ""


@dataclass
class SyncState:
    version: int
    dataset_name: str
    dataset_id: str
    target_root: str
    last_sync_at: str
    files: Dict[str, TrackedFileState] = field(default_factory=dict)


@dataclass(frozen=True)
class DeleteAction:
    document_id: str
    reason: str
    abs_path: Optional[str] = None


@dataclass(frozen=True)
class AdoptAction:
    local_file: LocalFileSnapshot
    document_id: str
    reason: str


@dataclass(frozen=True)
class UploadAction:
    local_file: LocalFileSnapshot
    reason: str
    previous_document_id: Optional[str] = None


@dataclass
class SyncPlan:
    adopt_actions: List[AdoptAction] = field(default_factory=list)
    delete_actions: List[DeleteAction] = field(default_factory=list)
    upload_actions: List[UploadAction] = field(default_factory=list)
    parse_actions: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    abnormal_files: List[str] = field(default_factory=list)


def safe_slug(value: str) -> str:
    chars = []
    for ch in value.strip():
        if ch.isascii() and (ch.isalnum() or ch in "._-"):
            chars.append(ch)
        else:
            chars.append("_")
    slug = "".join(chars).strip("_")
    while "__" in slug:
        slug = slug.replace("__", "_")
    if not slug:
        raise ConfigError(f"Cannot derive safe slug from dataset name: {value!r}")
    return slug
