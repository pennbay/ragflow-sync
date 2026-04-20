from __future__ import annotations

import importlib
import os
from pathlib import Path
from typing import List

from .models import ConfigError, DEFAULT_ALLOWED_EXTENSIONS, SyncTargetConfig, safe_slug


def load_env_file(path: Path = Path(".env")) -> None:
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        raw = line.strip()
        if not raw or raw.startswith("#") or "=" not in raw:
            continue
        key, value = raw.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def load_config(module_name: str = "config") -> List[SyncTargetConfig]:
    load_env_file()
    module = importlib.import_module(module_name)
    api_key = os.environ.get("RAGFLOW_API_KEY", "").strip()
    base_url = str(getattr(module, "BASE_URL", "")).strip()
    raw_targets = getattr(module, "SYNC_TARGETS", None)
    allowed_extensions = DEFAULT_ALLOWED_EXTENSIONS | {
        str(ext).lower() for ext in getattr(module, "ALLOWED_EXTENSIONS", [])
    }
    ignore_dirs = {str(item) for item in getattr(module, "IGNORE_DIRS", [])}
    ignore_files = {str(item) for item in getattr(module, "IGNORE_FILES", [])}
    max_file_size_mb = int(getattr(module, "MAX_FILE_SIZE_MB", 100))
    remote_page_size = int(getattr(module, "REMOTE_PAGE_SIZE", 100))
    parse_trigger_batch_size = int(getattr(module, "PARSE_TRIGGER_BATCH_SIZE", 32))
    api_retry_times = int(getattr(module, "API_RETRY_TIMES", 2))
    api_retry_interval_seconds = float(getattr(module, "API_RETRY_INTERVAL_SECONDS", 2))
    api_timeout_seconds = float(getattr(module, "API_TIMEOUT_SECONDS", 30))
    upload_retry_times = int(getattr(module, "UPLOAD_RETRY_TIMES", 2))
    upload_retry_interval_seconds = float(getattr(module, "UPLOAD_RETRY_INTERVAL_SECONDS", 3))
    upload_timeout_seconds = float(getattr(module, "UPLOAD_TIMEOUT_SECONDS", 180))
    log_level = str(getattr(module, "LOG_LEVEL", "INFO")).upper()
    state_dir = Path(str(getattr(module, "STATE_DIR", "states"))).expanduser()
    log_dir = Path(str(getattr(module, "LOG_DIR", "logs"))).expanduser()
    max_parse_retry_times = int(getattr(module, "MAX_PARSE_RETRY_TIMES", 3))

    if not api_key:
        raise ConfigError("Missing RAGFLOW_API_KEY")
    if not base_url:
        raise ConfigError("Missing BASE_URL")
    if not isinstance(raw_targets, list) or not raw_targets:
        raise ConfigError("SYNC_TARGETS must be a non-empty list")
    if max_file_size_mb <= 0:
        raise ConfigError("MAX_FILE_SIZE_MB must be > 0")
    if remote_page_size <= 0:
        raise ConfigError("REMOTE_PAGE_SIZE must be > 0")
    if parse_trigger_batch_size <= 0:
        raise ConfigError("PARSE_TRIGGER_BATCH_SIZE must be > 0")
    if api_retry_times <= 0:
        raise ConfigError("API_RETRY_TIMES must be > 0")
    if api_timeout_seconds <= 0:
        raise ConfigError("API_TIMEOUT_SECONDS must be > 0")
    if upload_retry_times <= 0:
        raise ConfigError("UPLOAD_RETRY_TIMES must be > 0")
    if upload_timeout_seconds <= 0:
        raise ConfigError("UPLOAD_TIMEOUT_SECONDS must be > 0")
    if upload_retry_interval_seconds <= 0:
        raise ConfigError("UPLOAD_RETRY_INTERVAL_SECONDS must be > 0")
    if max_parse_retry_times < 0:
        raise ConfigError("MAX_PARSE_RETRY_TIMES must be >= 0")
    invalid_extensions = [ext for ext in allowed_extensions if not ext.startswith(".")]
    if invalid_extensions:
        raise ConfigError(f"Invalid ALLOWED_EXTENSIONS: {invalid_extensions}")

    seen_dirs = set()
    seen_datasets = set()
    seen_state_paths = set()
    targets: List[SyncTargetConfig] = []
    for index, item in enumerate(raw_targets, start=1):
        if not isinstance(item, dict):
            raise ConfigError(f"SYNC_TARGETS[{index}] must be a dict")
        if set(item.keys()) - {"DATASET_NAME", "LOCAL_DIR"}:
            extra = sorted(set(item.keys()) - {"DATASET_NAME", "LOCAL_DIR"})
            raise ConfigError(f"SYNC_TARGETS[{index}] has unsupported keys: {extra}")
        dataset_name = str(item.get("DATASET_NAME", "")).strip()
        local_dir = str(item.get("LOCAL_DIR", "")).strip()
        if not dataset_name or not local_dir:
            raise ConfigError(f"SYNC_TARGETS[{index}] must include DATASET_NAME and LOCAL_DIR")
        normalized_dir = str(Path(local_dir).expanduser().resolve())
        if normalized_dir in seen_dirs:
            raise ConfigError(f"Duplicate LOCAL_DIR is not allowed: {normalized_dir}")
        if dataset_name in seen_datasets:
            raise ConfigError(f"Duplicate DATASET_NAME is not allowed: {dataset_name}")
        state_path = state_dir / f"{safe_slug(dataset_name)}.json"
        if str(state_path) in seen_state_paths:
            raise ConfigError(f"Duplicate derived state path is not allowed: {state_path}")
        seen_dirs.add(normalized_dir)
        seen_datasets.add(dataset_name)
        seen_state_paths.add(str(state_path))
        targets.append(
            SyncTargetConfig(
                api_key=api_key,
                base_url=base_url,
                dataset_name=dataset_name,
                local_dir=Path(normalized_dir),
                allowed_extensions=set(allowed_extensions),
                ignore_dirs=set(ignore_dirs),
                ignore_files=set(ignore_files),
                max_file_size_mb=max_file_size_mb,
                remote_page_size=remote_page_size,
                parse_trigger_batch_size=parse_trigger_batch_size,
                api_retry_times=api_retry_times,
                api_retry_interval_seconds=api_retry_interval_seconds,
                api_timeout_seconds=api_timeout_seconds,
                upload_retry_times=upload_retry_times,
                upload_retry_interval_seconds=upload_retry_interval_seconds,
                upload_timeout_seconds=upload_timeout_seconds,
                log_level=log_level,
                state_dir=state_dir,
                log_dir=log_dir,
                max_parse_retry_times=max_parse_retry_times,
            )
        )
    return targets
