from __future__ import annotations

import json
import os
import shutil
import tempfile
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path

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


def load_state(path: Path, dataset_name: str, target_root: str, logger) -> SyncState:
    if not path.exists():
        return empty_state(dataset_name, "", target_root)
    candidates = [path, path.with_suffix(path.suffix + ".bak")]
    for candidate in candidates:
        if not candidate.exists():
            continue
        try:
            payload = json.loads(candidate.read_text(encoding="utf-8"))
            files = {
                abs_path: TrackedFileState(**record)
                for abs_path, record in payload.get("files", {}).items()
            }
            return SyncState(
                version=int(payload.get("version", STATE_VERSION)),
                dataset_name=str(payload.get("dataset_name", dataset_name)),
                dataset_id=str(payload.get("dataset_id", "")),
                target_root=str(payload.get("target_root", target_root)),
                last_sync_at=str(payload.get("last_sync_at", "")),
                files=files,
            )
        except Exception as exc:
            logger.warning("Failed to read state file: path=%s reason=%s", candidate, exc)
    logger.warning("Falling back to empty state for dataset=%s", dataset_name)
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
