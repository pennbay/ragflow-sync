from __future__ import annotations

import json
import os
import shutil
import tempfile
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping

from .models import STATE_VERSION, SyncState, TrackedFileState


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def empty_state(dataset_name: str, dataset_id: str, target_root: str) -> SyncState:
    return SyncState(
        version=STATE_VERSION,
        dataset_name=dataset_name,
        dataset_id=dataset_id,
        target_root=target_root,
        last_sync_at="",
        files={},
    )


def _load_payload(payload: Mapping[str, object], dataset_name: str, target_root: str) -> SyncState:
    version = int(payload.get("version", 0))
    if version != STATE_VERSION:
        raise ValueError(f"unsupported state version: {version}")
    raw_files = payload.get("files", {})
    if not isinstance(raw_files, dict):
        raise ValueError("state files must be an object")
    files = {}
    for abs_path, record in raw_files.items():
        if not isinstance(record, dict):
            raise ValueError(f"invalid state record for {abs_path}")
        files[str(abs_path)] = TrackedFileState(**record)
    return SyncState(
        version=STATE_VERSION,
        dataset_name=str(payload.get("dataset_name", dataset_name)),
        dataset_id=str(payload.get("dataset_id", "")),
        target_root=str(payload.get("target_root", target_root)),
        last_sync_at=str(payload.get("last_sync_at", "")),
        files=files,
    )


def load_state(path: Path, dataset_name: str, target_root: str, logger) -> SyncState:
    if not path.exists():
        return empty_state(dataset_name, "", target_root)
    candidates = [path, path.with_suffix(path.suffix + ".bak")]
    for candidate in candidates:
        if not candidate.exists():
            continue
        try:
            payload = json.loads(candidate.read_text(encoding="utf-8"))
            return _load_payload(payload, dataset_name, target_root)
        except Exception as exc:
            logger.warning("Ignoring state file: path=%s reason=%s", candidate, exc)
    logger.warning("Using empty v%s state for dataset=%s", STATE_VERSION, dataset_name)
    return empty_state(dataset_name, "", target_root)


def save_state(path: Path, state: SyncState) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    serializable = {
        "version": state.version,
        "dataset_name": state.dataset_name,
        "dataset_id": state.dataset_id,
        "target_root": state.target_root,
        "last_sync_at": utc_now(),
        "files": {key: asdict(value) for key, value in state.files.items()},
    }
    if path.exists():
        shutil.copy2(path, path.with_suffix(path.suffix + ".bak"))
    fd, tmp_name = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(serializable, handle, ensure_ascii=False, indent=2, sort_keys=True)
            handle.write("\n")
        os.replace(tmp_name, path)
    finally:
        if os.path.exists(tmp_name):
            os.unlink(tmp_name)
