from __future__ import annotations

from typing import Dict, List, Optional

from .models import (
    ParseRunStatus,
    RemoteDocumentSnapshot,
    SyncError,
    SyncPlan,
    SyncState,
    SyncTargetConfig,
    TrackedFileState,
    UploadAction,
)
from .state_store import save_state, utc_now


def _chunk(values, size):
    for index in range(0, len(values), size):
        yield values[index:index + size]


def _record_for_upload(action: UploadAction, document: RemoteDocumentSnapshot) -> TrackedFileState:
    return TrackedFileState(
        abs_path=action.local_file.abs_path,
        remote_name=action.local_file.remote_name,
        document_id=document.document_id,
        md5=action.local_file.md5,
        mtime_ns=action.local_file.mtime_ns,
        size=action.local_file.size,
        parse_retry_count=0,
        last_parse_trigger_at="",
        last_error="",
    )


def describe_plan(plan: SyncPlan) -> str:
    return (
        f"deletes={len(plan.delete_actions)} "
        f"uploads={len(plan.upload_actions)} "
        f"parse={len(plan.parse_actions)} "
        f"warnings={len(plan.warnings)} "
        f"abnormal={len(plan.abnormal_files)}"
    )


def execute_sync_plan(
    gateway,
    dataset_ref,
    config: SyncTargetConfig,
    state: SyncState,
    plan: SyncPlan,
    logger,
    dry_run: bool = False,
) -> int:
    for warning in plan.warnings:
        logger.warning(warning)
    for abs_path in plan.abnormal_files:
        tracked = state.files.get(abs_path)
        if tracked:
            tracked.last_error = "parse retry limit reached"
    logger.info("Plan summary: %s", describe_plan(plan))
    if dry_run:
        return 0

    initial_deletes = [item for item in plan.delete_actions if item.reason == "local_deleted"]
    orphan_deletes = [item for item in plan.delete_actions if item.reason != "local_deleted"]
    if initial_deletes:
        gateway.delete_documents(dataset_ref.dataset_id, [item.document_id for item in initial_deletes])
        for item in initial_deletes:
            if item.abs_path:
                state.files.pop(item.abs_path, None)

    replacement_deletes: List[str] = []
    uploaded_doc_ids: List[str] = []
    total_uploads = len(plan.upload_actions)
    if total_uploads:
        logger.info("Upload phase started. total=%s batch_size=%s", total_uploads, config.upload_batch_size)
    uploaded_count = 0
    for batch_index, batch in enumerate(_chunk(plan.upload_actions, config.upload_batch_size), start=1):
        logger.info(
            "Uploading batch %s. batch_size=%s uploaded=%s/%s",
            batch_index,
            len(batch),
            uploaded_count,
            total_uploads,
        )
        for action in batch:
            current_index = uploaded_count + 1
            logger.info(
                "Uploading file %s/%s. reason=%s path=%s remote_name=%s",
                current_index,
                total_uploads,
                action.reason,
                action.local_file.rel_path,
                action.local_file.remote_name,
            )
            uploaded = gateway.upload_document(
                dataset_ref.dataset_id,
                action.local_file.remote_name,
                action.local_file.path,
            )
            state.files[action.local_file.abs_path] = _record_for_upload(action, uploaded)
            uploaded_doc_ids.append(uploaded.document_id)
            uploaded_count += 1
            logger.info(
                "Uploaded file %s/%s. document_id=%s path=%s",
                uploaded_count,
                total_uploads,
                uploaded.document_id,
                action.local_file.rel_path,
            )
            if action.previous_document_id:
                replacement_deletes.append(action.previous_document_id)

    if total_uploads:
        logger.info("Upload phase completed. uploaded=%s/%s", uploaded_count, total_uploads)

    refreshed = gateway.list_documents(dataset_ref.dataset_id)
    refreshed_by_id: Dict[str, RemoteDocumentSnapshot] = {doc.document_id: doc for doc in refreshed}
    missing_uploads = [doc_id for doc_id in uploaded_doc_ids if doc_id not in refreshed_by_id]
    if missing_uploads:
        raise SyncError(f"Uploaded documents missing from remote listing: {missing_uploads}")

    if replacement_deletes:
        gateway.delete_documents(dataset_ref.dataset_id, replacement_deletes)
    if orphan_deletes:
        gateway.delete_documents(dataset_ref.dataset_id, [item.document_id for item in orphan_deletes])

    parse_ids = list(plan.parse_actions)
    for doc_id in uploaded_doc_ids:
        if doc_id not in parse_ids:
            parse_ids.append(doc_id)
    if parse_ids:
        logger.info("Triggering async parse. count=%s", len(parse_ids))
        gateway.trigger_async_parse(dataset_ref.dataset_id, parse_ids)
        now = utc_now()
        for tracked in state.files.values():
            if tracked.document_id in parse_ids:
                tracked.last_parse_trigger_at = now
                tracked.parse_retry_count += 1
                tracked.last_error = ""
        logger.info("Async parse triggered successfully. count=%s", len(parse_ids))
    else:
        logger.info("No documents require async parse.")

    state.dataset_id = dataset_ref.dataset_id
    state.dataset_name = dataset_ref.dataset_name
    state.target_root = str(config.local_dir)
    state.last_sync_at = utc_now()
    save_state(config.state_path, state)
    return 0
