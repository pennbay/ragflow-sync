from __future__ import annotations

from typing import Dict, Iterable, List

from .models import (
    DeleteAction,
    LocalFileSnapshot,
    ParseRunStatus,
    RemoteDocumentSnapshot,
    SyncPlan,
    SyncState,
    SyncTargetConfig,
    UploadAction,
)
from .scanner import is_managed_remote_name


def unique(values: Iterable[str]) -> List[str]:
    seen = set()
    result = []
    for value in values:
        if value and value not in seen:
            seen.add(value)
            result.append(value)
    return result


def build_plan(
    local_files: Dict[str, LocalFileSnapshot],
    remote_docs: List[RemoteDocumentSnapshot],
    state: SyncState,
    config: SyncTargetConfig,
) -> SyncPlan:
    plan = SyncPlan()
    remote_by_id = {doc.document_id: doc for doc in remote_docs}
    remote_by_name = {doc.name: doc for doc in remote_docs}

    for abs_path, local_file in local_files.items():
        tracked = state.files.get(abs_path)
        if tracked is None:
            plan.upload_actions.append(UploadAction(local_file=local_file, reason="new"))
            continue

        remote = remote_by_id.get(tracked.document_id) or remote_by_name.get(tracked.remote_name)
        if tracked.md5 != local_file.md5:
            plan.upload_actions.append(
                UploadAction(
                    local_file=local_file,
                    reason="modified",
                    previous_document_id=tracked.document_id or None,
                )
            )
            continue

        if remote is None:
            plan.upload_actions.append(UploadAction(local_file=local_file, reason="remote_missing"))
            continue

        if remote.run_status == ParseRunStatus.UNSTART:
            plan.parse_actions.append(remote.document_id)
        elif remote.run_status == ParseRunStatus.RUNNING:
            continue
        elif remote.run_status == ParseRunStatus.DONE:
            continue
        elif remote.run_status in {ParseRunStatus.FAIL, ParseRunStatus.CANCEL}:
            if tracked.parse_retry_count < config.max_parse_retry_times:
                plan.parse_actions.append(remote.document_id)
            else:
                plan.abnormal_files.append(abs_path)
        else:
            plan.warnings.append(
                f"Unknown remote status for {abs_path}: {remote.run_status.value}"
            )

    for abs_path, tracked in state.files.items():
        if abs_path not in local_files and tracked.document_id:
            plan.delete_actions.append(
                DeleteAction(
                    document_id=tracked.document_id,
                    reason="local_deleted",
                    abs_path=abs_path,
                )
            )

    tracked_ids = {item.document_id for item in state.files.values() if item.document_id}
    for remote in remote_docs:
        if remote.document_id in tracked_ids:
            continue
        if is_managed_remote_name(remote.name):
            plan.delete_actions.append(DeleteAction(document_id=remote.document_id, reason="orphan_remote"))
        else:
            plan.warnings.append(f"External remote document kept: {remote.name}")

    plan.parse_actions = unique(plan.parse_actions)
    return plan
