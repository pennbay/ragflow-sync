from __future__ import annotations

from typing import Dict, Iterable, List

from .models import (
    AdoptAction,
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
    remote_by_name: Dict[str, List[RemoteDocumentSnapshot]] = {}
    for doc in remote_docs:
        remote_by_name.setdefault(doc.name, []).append(doc)
    claimed_doc_ids = set()

    for abs_path, local_file in local_files.items():
        tracked = state.files.get(abs_path)
        name_matches = remote_by_name.get(local_file.remote_name, [])
        canonical = _choose_canonical(name_matches, tracked.document_id if tracked else None)
        duplicates = [doc for doc in name_matches if canonical and doc.document_id != canonical.document_id]
        for duplicate in duplicates:
            plan.delete_actions.append(
                DeleteAction(document_id=duplicate.document_id, reason="duplicate_remote")
            )

        if tracked is None:
            if canonical is None:
                plan.upload_actions.append(UploadAction(local_file=local_file, reason="new"))
            else:
                plan.adopt_actions.append(
                    AdoptAction(
                        local_file=local_file,
                        document_id=canonical.document_id,
                        reason="remote_exists",
                    )
                )
                claimed_doc_ids.add(canonical.document_id)
                _plan_parse(plan, canonical, 0, config, abs_path)
            continue

        remote = remote_by_id.get(tracked.document_id)
        if tracked.md5 != local_file.md5:
            if canonical is None:
                plan.upload_actions.append(
                    UploadAction(
                        local_file=local_file,
                        reason="modified",
                        previous_document_id=tracked.document_id or None,
                    )
                )
            else:
                plan.adopt_actions.append(
                    AdoptAction(
                        local_file=local_file,
                        document_id=canonical.document_id,
                        reason="modified_remote_exists",
                    )
                )
                claimed_doc_ids.add(canonical.document_id)
                _plan_parse(plan, canonical, 0, config, abs_path)
                if tracked.document_id and tracked.document_id != canonical.document_id:
                    plan.delete_actions.append(
                        DeleteAction(
                            document_id=tracked.document_id,
                            reason="replaced_remote",
                            abs_path=abs_path,
                        )
                    )
            continue

        remote = remote or canonical
        if remote is None:
            plan.upload_actions.append(UploadAction(local_file=local_file, reason="remote_missing"))
            continue

        claimed_doc_ids.add(remote.document_id)
        if remote.document_id != tracked.document_id:
            plan.adopt_actions.append(
                AdoptAction(
                    local_file=local_file,
                    document_id=remote.document_id,
                    reason="document_id_recovered",
                )
            )
        _plan_parse(plan, remote, tracked.parse_retry_count, config, abs_path)

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
    tracked_ids.update(claimed_doc_ids)
    for remote in remote_docs:
        if remote.document_id in tracked_ids:
            continue
        if is_managed_remote_name(remote.name):
            plan.delete_actions.append(DeleteAction(document_id=remote.document_id, reason="orphan_remote"))
        else:
            plan.warnings.append(f"External remote document kept: {remote.name}")

    plan.parse_actions = unique(plan.parse_actions)
    plan.delete_actions = _unique_deletes(plan.delete_actions)
    return plan


def _choose_canonical(
    documents: List[RemoteDocumentSnapshot],
    preferred_document_id: str | None,
) -> RemoteDocumentSnapshot | None:
    if not documents:
        return None
    if preferred_document_id:
        for document in documents:
            if document.document_id == preferred_document_id:
                return document
    return documents[0]


def _plan_parse(
    plan: SyncPlan,
    remote: RemoteDocumentSnapshot,
    parse_retry_count: int,
    config: SyncTargetConfig,
    abs_path: str,
) -> None:
    if remote.run_status == ParseRunStatus.UNSTART:
        plan.parse_actions.append(remote.document_id)
    elif remote.run_status == ParseRunStatus.RUNNING:
        return
    elif remote.run_status == ParseRunStatus.DONE:
        return
    elif remote.run_status in {ParseRunStatus.FAIL, ParseRunStatus.CANCEL}:
        if parse_retry_count < config.max_parse_retry_times:
            plan.parse_actions.append(remote.document_id)
        else:
            plan.abnormal_files.append(abs_path)
    else:
        plan.warnings.append(f"Unknown remote status for {abs_path}: {remote.run_status.value}")


def _unique_deletes(actions: List[DeleteAction]) -> List[DeleteAction]:
    seen = set()
    result = []
    for action in actions:
        if action.document_id in seen:
            continue
        seen.add(action.document_id)
        result.append(action)
    return result
