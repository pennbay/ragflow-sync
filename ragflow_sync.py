#!/usr/bin/env python3
"""Synchronize local directories to a RAGFlow dataset.

The implementation intentionally uses only the official RAGFlow Python SDK
surface for dataset/document operations. Local source files are read-only.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib
import json
import logging
from logging.handlers import RotatingFileHandler
import os
from pathlib import Path
import re
import shutil
import sys
import tempfile
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Set, Tuple


DEFAULT_ALLOWED_EXTENSIONS = {
    ".pdf",
    ".doc",
    ".docx",
    ".ppt",
    ".pptx",
    ".md",
    ".markdown",
}
REMOTE_NAME_MARKER = "__rf_"
STATE_SCHEMA_VERSION = 1
PYTHON_MIN_VERSION = (3, 12)


class ConfigError(RuntimeError):
    """Raised when user configuration is invalid."""


class SyncError(RuntimeError):
    """Raised for unrecoverable synchronization errors."""


@dataclass
class AppConfig:
    api_key: str
    base_url: str
    dataset_name: str
    local_sync_dirs: List[Path]
    allowed_extensions: Set[str]
    ignore_dirs: Set[str]
    ignore_files: Set[str]
    max_file_size_mb: int
    max_parse_retry_times: int
    sync_state_file: Path
    log_file_path: Path
    log_level: str
    upload_batch_size: int
    remote_page_size: int
    api_retry_times: int
    api_retry_interval_seconds: float


@dataclass
class LocalFile:
    abs_path: str
    path: Path
    remote_display_name: str
    md5: str
    mtime_ns: int
    size: int
    extension: str


@dataclass
class RemoteDocument:
    document_id: str
    name: str
    extension: str
    status: str
    created_at: Optional[str] = None
    failure_reason: str = ""


@dataclass
class UploadTask:
    local_file: LocalFile
    old_document_id: Optional[str] = None
    reason: str = "new"


@dataclass
class DeleteTask:
    document_id: str
    reason: str
    abs_path: Optional[str] = None
    display_name: Optional[str] = None


@dataclass
class Decision:
    delete_tasks: List[DeleteTask] = field(default_factory=list)
    upload_tasks: List[UploadTask] = field(default_factory=list)
    parse_document_ids: List[str] = field(default_factory=list)
    abnormal_files: List[str] = field(default_factory=list)
    unchanged: int = 0


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_abs_path(path: Path) -> str:
    return str(path.expanduser().resolve())


def make_remote_display_name(abs_path: str, original_name: str) -> str:
    path_hash = hashlib.sha256(abs_path.encode("utf-8")).hexdigest()[:16]
    original = Path(original_name)
    return f"{original.stem}{REMOTE_NAME_MARKER}{path_hash}{original.suffix.lower()}"


def path_hash_from_remote_name(name: str) -> Optional[str]:
    stem = Path(name).stem
    if REMOTE_NAME_MARKER not in stem:
        return None
    return stem.rsplit(REMOTE_NAME_MARKER, 1)[-1] or None


def file_md5(path: Path) -> str:
    digest = hashlib.md5()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def safe_dataset_slug(dataset_name: str) -> str:
    raw = dataset_name.strip()
    slug = re.sub(r"[^A-Za-z0-9._-]+", "_", raw)
    slug = re.sub(r"_+", "_", slug).strip("_")
    if not slug:
        raise ConfigError(f"DATASET_NAME cannot produce a safe file name: {dataset_name!r}")
    return slug


def default_state_file(dataset_name: str) -> Path:
    return Path("states") / f"{safe_dataset_slug(dataset_name)}.json"


def default_log_file(dataset_name: str) -> Path:
    return Path("logs") / f"{safe_dataset_slug(dataset_name)}.log"


def load_env_file(path: Path = Path(".env")) -> None:
    if not path.exists():
        return
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            raw = line.strip()
            if not raw or raw.startswith("#") or "=" not in raw:
                continue
            key, value = raw.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = value


def load_configs(module_name: str = "config") -> List[AppConfig]:
    load_env_file()
    cfg = importlib.import_module(module_name)
    api_key = os.environ.get("RAGFLOW_API_KEY", "").strip()
    base_url = str(getattr(cfg, "BASE_URL", "")).strip()
    targets = getattr(cfg, "SYNC_TARGETS", None)
    configured_exts = {str(ext).lower() for ext in getattr(cfg, "ALLOWED_EXTENSIONS", [])}
    allowed_extensions = DEFAULT_ALLOWED_EXTENSIONS | configured_exts

    if not isinstance(targets, list) or not targets:
        raise ConfigError("SYNC_TARGETS must be a non-empty list")

    common = {
        "api_key": api_key,
        "base_url": base_url,
        "allowed_extensions": allowed_extensions,
        "ignore_dirs": {str(item) for item in getattr(cfg, "IGNORE_DIRS", [])},
        "ignore_files": {str(item) for item in getattr(cfg, "IGNORE_FILES", [])},
        "max_file_size_mb": int(getattr(cfg, "MAX_FILE_SIZE_MB", 100)),
        "max_parse_retry_times": int(getattr(cfg, "MAX_PARSE_RETRY_TIMES", 3)),
        "log_level": str(getattr(cfg, "LOG_LEVEL", "INFO")).upper(),
        "upload_batch_size": int(getattr(cfg, "UPLOAD_BATCH_SIZE", 20)),
        "remote_page_size": int(getattr(cfg, "REMOTE_PAGE_SIZE", 100)),
        "api_retry_times": int(getattr(cfg, "API_RETRY_TIMES", 3)),
        "api_retry_interval_seconds": float(getattr(cfg, "API_RETRY_INTERVAL_SECONDS", 2)),
    }

    configs: List[AppConfig] = []
    seen_state_files: Set[str] = set()
    seen_log_files: Set[str] = set()
    for index, target in enumerate(targets, start=1):
        if not isinstance(target, dict):
            raise ConfigError(f"SYNC_TARGETS[{index}] must be a dict")
        forbidden = [key for key in ("SYNC_STATE_FILE", "LOG_FILE_PATH") if key in target]
        if forbidden:
            raise ConfigError(
                f"SYNC_TARGETS[{index}] must not configure: {', '.join(forbidden)}. "
                "State and log paths are generated from DATASET_NAME."
            )
        missing = [
            key
            for key in ("DATASET_NAME", "LOCAL_SYNC_DIRS")
            if not target.get(key)
        ]
        if missing:
            raise ConfigError(f"SYNC_TARGETS[{index}] missing required keys: {', '.join(missing)}")
        dataset_name = str(target["DATASET_NAME"]).strip()
        local_dirs = target["LOCAL_SYNC_DIRS"]
        if not isinstance(local_dirs, list) or not local_dirs:
            raise ConfigError(f"SYNC_TARGETS[{index}].LOCAL_SYNC_DIRS must be a non-empty list")
        config = AppConfig(
            api_key=common["api_key"],
            base_url=common["base_url"],
            dataset_name=dataset_name,
            local_sync_dirs=[Path(item).expanduser() for item in local_dirs],
            allowed_extensions=set(common["allowed_extensions"]),
            ignore_dirs=set(common["ignore_dirs"]),
            ignore_files=set(common["ignore_files"]),
            max_file_size_mb=int(common["max_file_size_mb"]),
            max_parse_retry_times=int(common["max_parse_retry_times"]),
            sync_state_file=default_state_file(dataset_name),
            log_file_path=default_log_file(dataset_name),
            log_level=str(common["log_level"]),
            upload_batch_size=int(common["upload_batch_size"]),
            remote_page_size=int(common["remote_page_size"]),
            api_retry_times=int(common["api_retry_times"]),
            api_retry_interval_seconds=float(common["api_retry_interval_seconds"]),
        )
        validate_config(config)
        state_key = str(config.sync_state_file)
        log_key = str(config.log_file_path)
        if state_key in seen_state_files:
            raise ConfigError(f"Duplicate SYNC_STATE_FILE is not allowed: {state_key}")
        if log_key in seen_log_files:
            raise ConfigError(f"Duplicate LOG_FILE_PATH is not allowed: {log_key}")
        seen_state_files.add(state_key)
        seen_log_files.add(log_key)
        configs.append(config)
    return configs


def load_config(module_name: str = "config") -> AppConfig:
    """Backward-compatible helper for tests that need the first target only."""
    return load_configs(module_name)[0]


def validate_config(config: AppConfig) -> None:
    missing = []
    if not config.api_key:
        missing.append("API_KEY or RAGFLOW_API_KEY")
    if not config.base_url:
        missing.append("BASE_URL")
    if not config.dataset_name:
        missing.append("DATASET_NAME")
    if not config.local_sync_dirs:
        missing.append("LOCAL_SYNC_DIRS")
    if missing:
        raise ConfigError(f"Missing required configuration: {', '.join(missing)}")

    if not DEFAULT_ALLOWED_EXTENSIONS.issubset(config.allowed_extensions):
        raise ConfigError("ALLOWED_EXTENSIONS must include all default allowed extensions")
    invalid_exts = [ext for ext in config.allowed_extensions if not ext.startswith(".")]
    if invalid_exts:
        raise ConfigError(f"Invalid extensions, must start with '.': {invalid_exts}")
    if config.max_file_size_mb <= 0:
        raise ConfigError("MAX_FILE_SIZE_MB must be greater than 0")
    if config.max_parse_retry_times < 0:
        raise ConfigError("MAX_PARSE_RETRY_TIMES must be >= 0")
    if config.upload_batch_size <= 0:
        raise ConfigError("UPLOAD_BATCH_SIZE must be greater than 0")
    if config.remote_page_size <= 0:
        raise ConfigError("REMOTE_PAGE_SIZE must be greater than 0")
    if config.api_retry_times <= 0:
        raise ConfigError("API_RETRY_TIMES must be greater than 0")


def ensure_environment(config: AppConfig) -> None:
    if sys.version_info < PYTHON_MIN_VERSION:
        raise ConfigError(
            "Python 3.12+ is required because the latest ragflow-sdk requires Python >=3.12."
        )

    for directory in config.local_sync_dirs:
        if not directory.exists():
            raise ConfigError(f"Sync directory does not exist: {directory}")
        if not directory.is_dir():
            raise ConfigError(f"Sync path is not a directory: {directory}")
        if not os.access(directory, os.R_OK):
            raise ConfigError(f"Sync directory is not readable: {directory}")

    for file_path, label in [
        (config.sync_state_file, "SYNC_STATE_FILE"),
        (config.log_file_path, "LOG_FILE_PATH"),
    ]:
        parent = file_path.parent if file_path.parent != Path("") else Path(".")
        parent.mkdir(parents=True, exist_ok=True)
        if not os.access(parent, os.W_OK):
            raise ConfigError(f"{label} parent directory is not writable: {parent}")


def setup_logging(config: AppConfig) -> logging.Logger:
    level_name = "WARNING" if config.log_level == "WARN" else config.log_level
    level = getattr(logging, level_name, None)
    if not isinstance(level, int):
        raise ConfigError("LOG_LEVEL must be one of DEBUG, INFO, WARN, ERROR")

    logger = logging.getLogger("ragflow_sync")
    logger.handlers.clear()
    logger.setLevel(level)
    formatter = logging.Formatter(
        f"%(asctime)s %(levelname)s dataset={config.dataset_name} %(message)s"
    )

    console = logging.StreamHandler()
    console.setFormatter(formatter)
    console.setLevel(level)
    logger.addHandler(console)

    config.log_file_path.parent.mkdir(parents=True, exist_ok=True)
    file_handler = RotatingFileHandler(
        config.log_file_path, maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8"
    )
    file_handler.setFormatter(formatter)
    file_handler.setLevel(level)
    logger.addHandler(file_handler)
    return logger


def empty_state(dataset_name: str = "", dataset_id: str = "") -> Dict[str, Any]:
    return {
        "schema_version": STATE_SCHEMA_VERSION,
        "dataset_name": dataset_name,
        "dataset_id": dataset_id,
        "last_sync_at": "",
        "files": {},
        "orphan_remote_docs": {},
        "abnormal_files": {},
    }


def backup_file(path: Path) -> None:
    if path.exists():
        shutil.copy2(path, path.with_suffix(path.suffix + ".bak"))


def load_state(path: Path, logger: logging.Logger, dataset_name: str = "") -> Dict[str, Any]:
    if path.exists():
        backup_file(path)
    if not path.exists():
        return empty_state(dataset_name)
    try:
        with path.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
        if not isinstance(data, dict) or "files" not in data or not isinstance(data["files"], dict):
            raise ValueError("state root must contain a files object")
        data.setdefault("schema_version", STATE_SCHEMA_VERSION)
        data.setdefault("dataset_name", dataset_name)
        data.setdefault("dataset_id", "")
        data.setdefault("last_sync_at", "")
        data.setdefault("orphan_remote_docs", {})
        data.setdefault("abnormal_files", {})
        return data
    except Exception as exc:
        logger.error("State file is invalid, trying backup. reason=%s", exc)
        backup = path.with_suffix(path.suffix + ".bak")
        if backup.exists():
            try:
                with backup.open("r", encoding="utf-8") as handle:
                    data = json.load(handle)
                if isinstance(data, dict) and isinstance(data.get("files"), dict):
                    return data
            except Exception as backup_exc:
                logger.error("Backup state is invalid. reason=%s", backup_exc)
        logger.warning("Creating a fresh state; remote documents will be used for safe decisions.")
        return empty_state(dataset_name)


def save_state(path: Path, state: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    backup_file(path)
    state["last_sync_at"] = utc_now()
    fd, temp_name = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(state, handle, ensure_ascii=False, indent=2, sort_keys=True)
            handle.write("\n")
        os.replace(temp_name, path)
    finally:
        if os.path.exists(temp_name):
            os.unlink(temp_name)


class RagflowClientAdapter:
    def __init__(self, config: AppConfig, logger: logging.Logger) -> None:
        self.config = config
        self.logger = logger
        try:
            from ragflow_sdk import RAGFlow  # type: ignore
        except ImportError as exc:
            raise SyncError("ragflow-sdk is not installed. Run: pip install -r requirements.txt") from exc
        self.client = RAGFlow(api_key=config.api_key, base_url=config.base_url)
        self.dataset = None
        self.dataset_id = ""

    def _with_retry(self, description: str, func: Callable[[], Any]) -> Any:
        last_exc: Optional[BaseException] = None
        for attempt in range(1, self.config.api_retry_times + 1):
            try:
                return func()
            except Exception as exc:
                last_exc = exc
                self.logger.warning(
                    "API call failed: %s attempt=%s/%s reason=%s",
                    description,
                    attempt,
                    self.config.api_retry_times,
                    exc,
                )
                if attempt < self.config.api_retry_times:
                    time.sleep(self.config.api_retry_interval_seconds)
        raise SyncError(f"API call failed after retries: {description}; reason={last_exc}")

    def ensure_dataset(self, dataset_name: str) -> Tuple[Any, str]:
        def action() -> Tuple[Any, str]:
            datasets = self._call_list_datasets(dataset_name)
            dataset = next((item for item in datasets if getattr(item, "name", None) == dataset_name), None)
            if dataset is None:
                dataset = self.client.create_dataset(name=dataset_name)
                self.logger.info("Created dataset: %s", dataset_name)
            dataset_id = str(getattr(dataset, "id", "") or getattr(dataset, "dataset_id", ""))
            if not dataset_id:
                raise SyncError("Dataset object does not expose an id")
            return dataset, dataset_id

        self.dataset, self.dataset_id = self._with_retry("ensure dataset", action)
        return self.dataset, self.dataset_id

    def _call_list_datasets(self, dataset_name: str) -> List[Any]:
        try:
            result = self.client.list_datasets(name=dataset_name)
        except TypeError:
            result = self.client.list_datasets()
        return list(result or [])

    def list_documents(self) -> List[RemoteDocument]:
        if self.dataset is None:
            raise SyncError("Dataset is not initialized")
        documents: List[RemoteDocument] = []
        page = 1
        while True:
            def action() -> Any:
                try:
                    return self.dataset.list_documents(page=page, page_size=self.config.remote_page_size)
                except TypeError:
                    return self.dataset.list_documents()

            result = self._with_retry(f"list documents page={page}", action)
            raw_docs = self._extract_list(result)
            documents.extend(self._normalize_document(item) for item in raw_docs)
            if len(raw_docs) < self.config.remote_page_size:
                break
            page += 1
        return documents

    def upload_document(self, local_file: LocalFile) -> str:
        if self.dataset is None:
            raise SyncError("Dataset is not initialized")

        def action() -> Any:
            with local_file.path.open("rb") as handle:
                try:
                    return self.dataset.upload_documents(
                        [{"display_name": local_file.remote_display_name, "blob": handle}]
                    )
                except TypeError:
                    handle.seek(0)
                    return self.dataset.upload_documents(
                        documents=[{"display_name": local_file.remote_display_name, "blob": handle}]
                    )

        result = self._with_retry(f"upload {local_file.abs_path}", action)
        document_id = self._extract_uploaded_id(result, local_file.remote_display_name)
        if document_id:
            return document_id
        refreshed = self.list_documents()
        for doc in refreshed:
            if doc.name == local_file.remote_display_name:
                return doc.document_id
        raise SyncError(f"Upload succeeded but document id was not found: {local_file.abs_path}")

    def delete_documents(self, document_ids: Sequence[str]) -> None:
        if self.dataset is None or not document_ids:
            return

        def action() -> Any:
            try:
                return self.dataset.delete_documents(ids=list(document_ids))
            except TypeError:
                return self.dataset.delete_documents(list(document_ids))

        self._with_retry(f"delete documents count={len(document_ids)}", action)

    def async_parse_documents(self, document_ids: Sequence[str]) -> None:
        if self.dataset is None or not document_ids:
            return

        def action() -> Any:
            try:
                return self.dataset.async_parse_documents(document_ids=list(document_ids))
            except TypeError:
                try:
                    return self.dataset.async_parse_documents(list(document_ids))
                except AttributeError:
                    return self.dataset.async_parse_documents(document_ids)

        self._with_retry(f"async parse documents count={len(document_ids)}", action)

    @staticmethod
    def _extract_list(result: Any) -> List[Any]:
        if result is None:
            return []
        if isinstance(result, list):
            return result
        for attr in ("docs", "documents", "items", "data"):
            value = getattr(result, attr, None)
            if isinstance(value, list):
                return value
            if isinstance(result, dict) and isinstance(result.get(attr), list):
                return list(result[attr])
        return list(result) if isinstance(result, tuple) else []

    @staticmethod
    def _get_value(item: Any, *names: str, default: Any = "") -> Any:
        for name in names:
            if isinstance(item, dict) and name in item:
                return item[name]
            if hasattr(item, name):
                return getattr(item, name)
        return default

    def _normalize_document(self, item: Any) -> RemoteDocument:
        name = str(self._get_value(item, "name", "display_name", "filename", default=""))
        document_id = str(self._get_value(item, "id", "document_id", default=""))
        status = str(self._get_value(item, "run", "status", "parse_status", default="")).upper()
        if not status:
            status = "UNKNOWN"
        return RemoteDocument(
            document_id=document_id,
            name=name,
            extension=Path(name).suffix.lower(),
            status=status,
            created_at=str(self._get_value(item, "created_at", "create_time", default="")),
            failure_reason=str(self._get_value(item, "error", "failure_reason", "message", default="")),
        )

    def _extract_uploaded_id(self, result: Any, display_name: str) -> Optional[str]:
        candidates = self._extract_list(result)
        if not candidates and result:
            candidates = [result]
        for item in candidates:
            name = str(self._get_value(item, "name", "display_name", "filename", default=""))
            if name and name != display_name:
                continue
            document_id = str(self._get_value(item, "id", "document_id", default=""))
            if document_id:
                return document_id
        return None


def scan_local_files(config: AppConfig, state: Dict[str, Any], logger: logging.Logger) -> Dict[str, LocalFile]:
    local_files: Dict[str, LocalFile] = {}
    filtered_count = 0
    by_extension: Dict[str, int] = {}
    max_size_bytes = config.max_file_size_mb * 1024 * 1024
    history = state.get("files", {})

    for root in config.local_sync_dirs:
        for current_root, dir_names, file_names in os.walk(root):
            dir_names[:] = [name for name in dir_names if name not in config.ignore_dirs]
            current_path = Path(current_root)
            for file_name in file_names:
                path = current_path / file_name
                extension = path.suffix.lower()
                if extension not in config.allowed_extensions:
                    filtered_count += 1
                    logger.debug("Filtered by extension: %s", path)
                    continue
                if file_name in config.ignore_files:
                    filtered_count += 1
                    logger.debug("Filtered by ignored file: %s", path)
                    continue
                try:
                    stat = path.stat()
                except OSError as exc:
                    filtered_count += 1
                    logger.warning("Cannot stat file, skipped: %s reason=%s", path, exc)
                    continue
                if stat.st_size > max_size_bytes:
                    filtered_count += 1
                    logger.debug("Filtered by size: %s", path)
                    continue
                try:
                    abs_path = normalize_abs_path(path)
                    old = history.get(abs_path, {})
                    if (
                        old
                        and int(old.get("mtime_ns", -1)) == stat.st_mtime_ns
                        and int(old.get("size", -1)) == stat.st_size
                        and old.get("md5")
                    ):
                        md5 = str(old["md5"])
                    else:
                        md5 = file_md5(path)
                except OSError as exc:
                    filtered_count += 1
                    logger.warning("Cannot read file, skipped: %s reason=%s", path, exc)
                    continue
                local_file = LocalFile(
                    abs_path=abs_path,
                    path=path,
                    remote_display_name=make_remote_display_name(abs_path, path.name),
                    md5=md5,
                    mtime_ns=stat.st_mtime_ns,
                    size=stat.st_size,
                    extension=extension,
                )
                local_files[abs_path] = local_file
                by_extension[extension] = by_extension.get(extension, 0) + 1

    logger.info(
        "Scan completed. valid=%s filtered=%s by_extension=%s",
        len(local_files),
        filtered_count,
        by_extension,
    )
    return local_files


def build_remote_maps(remote_docs: Sequence[RemoteDocument]) -> Tuple[Dict[str, RemoteDocument], Dict[str, RemoteDocument]]:
    by_id = {doc.document_id: doc for doc in remote_docs if doc.document_id}
    by_name = {doc.name: doc for doc in remote_docs if doc.name}
    return by_id, by_name


def decide_sync(
    local_files: Dict[str, LocalFile],
    state: Dict[str, Any],
    remote_docs: Sequence[RemoteDocument],
    config: AppConfig,
    logger: logging.Logger,
) -> Decision:
    decision = Decision()
    remote_by_id, remote_by_name = build_remote_maps(remote_docs)
    history = state.get("files", {})
    known_remote_ids = {
        str(record.get("document_id"))
        for record in history.values()
        if isinstance(record, dict) and record.get("document_id")
    }

    for doc in remote_docs:
        if doc.extension and doc.extension not in config.allowed_extensions:
            decision.delete_tasks.append(
                DeleteTask(doc.document_id, "remote disallowed extension", display_name=doc.name)
            )

    for abs_path, local_file in local_files.items():
        record = history.get(abs_path, {})
        document_id = str(record.get("document_id", "")) if isinstance(record, dict) else ""
        remote = remote_by_id.get(document_id) if document_id else None
        if remote is None:
            remote = remote_by_name.get(local_file.remote_display_name)

        if not record or remote is None:
            decision.upload_tasks.append(UploadTask(local_file=local_file, reason="new"))
            continue

        old_md5 = str(record.get("md5", ""))
        if old_md5 != local_file.md5:
            decision.upload_tasks.append(
                UploadTask(local_file=local_file, old_document_id=remote.document_id, reason="modified")
            )
            continue

        status = remote.status.upper()
        if status == "INIT":
            decision.parse_document_ids.append(remote.document_id)
        elif status == "DONE":
            decision.unchanged += 1
        elif status == "RUNNING":
            decision.unchanged += 1
        elif status == "FAIL":
            retries = int(record.get("parse_retry_count", 0))
            if retries < config.max_parse_retry_times:
                logger.warning(
                    "Remote parse failed, will retry: path=%s document_id=%s reason=%s",
                    abs_path,
                    remote.document_id,
                    remote.failure_reason,
                )
                decision.parse_document_ids.append(remote.document_id)
            else:
                decision.abnormal_files.append(abs_path)
                logger.warning("Parse retry limit reached: %s", abs_path)
        else:
            decision.unchanged += 1

    for abs_path, record in list(history.items()):
        if abs_path in local_files or not isinstance(record, dict):
            continue
        document_id = str(record.get("document_id", ""))
        if document_id and document_id in remote_by_id:
            decision.delete_tasks.append(DeleteTask(document_id, "local deleted", abs_path=abs_path))

    for doc in remote_docs:
        if doc.document_id in known_remote_ids:
            continue
        if doc.extension and doc.extension not in config.allowed_extensions:
            continue
        if path_hash_from_remote_name(doc.name) is None:
            logger.warning("Unmanaged remote document kept for manual review: %s", doc.name)

    decision.parse_document_ids = unique_keep_order(decision.parse_document_ids)
    logger.info(
        "Decision completed. deletes=%s uploads=%s parse=%s unchanged=%s abnormal=%s",
        len(decision.delete_tasks),
        len(decision.upload_tasks),
        len(decision.parse_document_ids),
        decision.unchanged,
        len(decision.abnormal_files),
    )
    return decision


def unique_keep_order(values: Iterable[str]) -> List[str]:
    seen: Set[str] = set()
    result: List[str] = []
    for value in values:
        if value and value not in seen:
            seen.add(value)
            result.append(value)
    return result


def chunked(values: Sequence[Any], size: int) -> Iterable[Sequence[Any]]:
    for index in range(0, len(values), size):
        yield values[index:index + size]


def update_state_for_upload(state: Dict[str, Any], local_file: LocalFile, document_id: str) -> None:
    state.setdefault("files", {})[local_file.abs_path] = {
        "abs_path": local_file.abs_path,
        "remote_display_name": local_file.remote_display_name,
        "md5": local_file.md5,
        "mtime_ns": local_file.mtime_ns,
        "size": local_file.size,
        "extension": local_file.extension,
        "document_id": document_id,
        "upload_time": utc_now(),
        "parse_status": "INIT",
        "parse_retry_count": 0,
        "last_parse_trigger_time": "",
        "last_failure_reason": "",
    }


def execute_deletes(
    adapter: RagflowClientAdapter,
    decision: Decision,
    state: Dict[str, Any],
    state_path: Path,
    logger: logging.Logger,
) -> None:
    if not decision.delete_tasks:
        return
    ids = unique_keep_order([task.document_id for task in decision.delete_tasks])
    adapter.delete_documents(ids)
    for task in decision.delete_tasks:
        if task.abs_path:
            state.get("files", {}).pop(task.abs_path, None)
        else:
            state.setdefault("orphan_remote_docs", {})[task.document_id] = {
                "display_name": task.display_name,
                "reason": task.reason,
                "deleted_at": utc_now(),
            }
        logger.info("Deleted remote document: id=%s reason=%s", task.document_id, task.reason)
    save_state(state_path, state)


def execute_uploads(
    adapter: RagflowClientAdapter,
    decision: Decision,
    state: Dict[str, Any],
    state_path: Path,
    config: AppConfig,
    logger: logging.Logger,
) -> List[str]:
    parse_ids: List[str] = []
    if not decision.upload_tasks:
        return parse_ids

    for batch in chunked(decision.upload_tasks, config.upload_batch_size):
        for task in batch:
            if task.old_document_id:
                try:
                    adapter.delete_documents([task.old_document_id])
                    logger.info("Deleted old modified document: id=%s", task.old_document_id)
                except Exception as exc:
                    logger.error(
                        "Failed to delete old document before upload: path=%s reason=%s",
                        task.local_file.abs_path,
                        exc,
                    )
                    continue
            try:
                document_id = adapter.upload_document(task.local_file)
                update_state_for_upload(state, task.local_file, document_id)
                parse_ids.append(document_id)
                logger.info(
                    "Uploaded document: path=%s document_id=%s reason=%s",
                    task.local_file.abs_path,
                    document_id,
                    task.reason,
                )
            except Exception as exc:
                logger.error(
                    "Upload failed, will retry next run: path=%s reason=%s",
                    task.local_file.abs_path,
                    exc,
                )
        save_state(state_path, state)
    return parse_ids


def verify_remote_consistency(
    adapter: RagflowClientAdapter,
    local_files: Dict[str, LocalFile],
    state: Dict[str, Any],
    logger: logging.Logger,
) -> bool:
    remote_docs = adapter.list_documents()
    _, remote_by_name = build_remote_maps(remote_docs)
    ok = True
    for abs_path, local_file in local_files.items():
        record = state.get("files", {}).get(abs_path, {})
        if not record:
            logger.error("Local file has no successful upload state: %s", abs_path)
            ok = False
            continue
        remote = remote_by_name.get(local_file.remote_display_name)
        if remote is None:
            logger.error("Remote document missing after upload: %s", abs_path)
            ok = False
    return ok


def final_parse_candidates(
    adapter: RagflowClientAdapter,
    candidate_ids: Sequence[str],
    logger: logging.Logger,
) -> List[str]:
    wanted = set(candidate_ids)
    if not wanted:
        return []
    remote_docs = adapter.list_documents()
    valid = []
    for doc in remote_docs:
        if doc.document_id not in wanted:
            continue
        if doc.status.upper() == "RUNNING":
            logger.info("Skip parse trigger for RUNNING document: %s", doc.document_id)
            continue
        valid.append(doc.document_id)
    return unique_keep_order(valid)


def trigger_parse(
    adapter: RagflowClientAdapter,
    document_ids: Sequence[str],
    state: Dict[str, Any],
    state_path: Path,
    logger: logging.Logger,
) -> None:
    ids = unique_keep_order(document_ids)
    if not ids:
        logger.info("No documents require parse trigger.")
        return
    adapter.async_parse_documents(ids)
    now = utc_now()
    ids_set = set(ids)
    for record in state.get("files", {}).values():
        if not isinstance(record, dict) or record.get("document_id") not in ids_set:
            continue
        record["last_parse_trigger_time"] = now
        record["parse_status"] = "RUNNING"
        record["parse_retry_count"] = int(record.get("parse_retry_count", 0)) + 1
    logger.info("Triggered async parse. count=%s document_ids=%s", len(ids), ids)
    save_state(state_path, state)


def rebuild_state_dataset_info(state: Dict[str, Any], dataset_name: str, dataset_id: str) -> None:
    state["dataset_name"] = dataset_name
    state["dataset_id"] = dataset_id
    state.setdefault("files", {})
    state.setdefault("orphan_remote_docs", {})
    state.setdefault("abnormal_files", {})


def run_sync(config: AppConfig, adapter_factory: Callable[[AppConfig, logging.Logger], RagflowClientAdapter] = RagflowClientAdapter) -> int:
    ensure_environment(config)
    logger = setup_logging(config)
    logger.info("Starting RAGFlow sync.")
    state = load_state(config.sync_state_file, logger, config.dataset_name)

    adapter = adapter_factory(config, logger)
    _, dataset_id = adapter.ensure_dataset(config.dataset_name)
    rebuild_state_dataset_info(state, config.dataset_name, dataset_id)

    local_files = scan_local_files(config, state, logger)
    remote_docs = adapter.list_documents()
    decision = decide_sync(local_files, state, remote_docs, config, logger)
    for abs_path in decision.abnormal_files:
        state.setdefault("abnormal_files", {})[abs_path] = {
            "reason": "parse retry limit reached",
            "updated_at": utc_now(),
        }

    execute_deletes(adapter, decision, state, config.sync_state_file, logger)
    uploaded_parse_ids = execute_uploads(adapter, decision, state, config.sync_state_file, config, logger)
    candidate_parse_ids = unique_keep_order(decision.parse_document_ids + uploaded_parse_ids)
    save_state(config.sync_state_file, state)

    if not verify_remote_consistency(adapter, local_files, state, logger):
        logger.error("Remote consistency check failed. Parse trigger is blocked.")
        save_state(config.sync_state_file, state)
        return 2

    final_ids = final_parse_candidates(adapter, candidate_parse_ids, logger)
    trigger_parse(adapter, final_ids, state, config.sync_state_file, logger)
    save_state(config.sync_state_file, state)
    logger.info("RAGFlow sync completed.")
    return 0


def run_all(
    configs: Sequence[AppConfig],
    adapter_factory: Callable[[AppConfig, logging.Logger], RagflowClientAdapter] = RagflowClientAdapter,
) -> int:
    exit_code = 0
    for config in configs:
        try:
            code = run_sync(config, adapter_factory=adapter_factory)
        except (ConfigError, SyncError) as exc:
            print(f"ERROR: dataset={config.dataset_name} {exc}", file=sys.stderr)
            return 1
        except Exception as exc:
            print(f"ERROR: dataset={config.dataset_name} unexpected failure: {exc}", file=sys.stderr)
            code = 2
        if code != 0:
            exit_code = 2
    return exit_code


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Synchronize local files to a RAGFlow dataset.")
    parser.add_argument("--config-module", default="config", help="Python module name for configuration.")
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    try:
        configs = load_configs(args.config_module)
        return run_all(configs)
    except (ConfigError, SyncError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("ERROR: interrupted by user", file=sys.stderr)
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
