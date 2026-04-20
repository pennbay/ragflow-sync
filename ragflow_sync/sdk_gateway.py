from __future__ import annotations

import time
from contextlib import contextmanager
from pathlib import Path
from typing import Dict, List

import requests
from ragflow_sdk import RAGFlow

from .models import DatasetRef, ParseRunStatus, RemoteDocumentSnapshot, SyncApiError, SyncTargetConfig


def _meta_fields(value: object) -> Dict[str, object]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return dict(value)
    to_json = getattr(value, "to_json", None)
    if callable(to_json):
        converted = to_json()
        if isinstance(converted, dict):
            return converted
    return {}


class RagflowGateway:
    def __init__(self, config: SyncTargetConfig, logger) -> None:
        self.config = config
        self.logger = logger
        self.client = RAGFlow(api_key=config.api_key, base_url=config.base_url)
        self._current_timeout = config.api_timeout_seconds
        self._install_timeout_transport()
        self._datasets_by_id: Dict[str, object] = {}

    def _install_timeout_transport(self) -> None:
        api_url = self.client.api_url
        headers = self.client.authorization_header

        def post(path, json=None, stream=False, files=None):
            return requests.post(
                url=api_url + path,
                json=json,
                headers=headers,
                stream=stream,
                files=files,
                timeout=self._current_timeout,
            )

        def get(path, params=None, json=None):
            return requests.get(
                url=api_url + path,
                params=params,
                headers=headers,
                json=json,
                timeout=self._current_timeout,
            )

        def delete(path, json):
            return requests.delete(url=api_url + path, json=json, headers=headers, timeout=self._current_timeout)

        def put(path, json):
            return requests.put(url=api_url + path, json=json, headers=headers, timeout=self._current_timeout)

        self.client.post = post
        self.client.get = get
        self.client.delete = delete
        self.client.put = put

    @contextmanager
    def _timeout_scope(self, timeout: float):
        previous_timeout = self._current_timeout
        self._current_timeout = timeout
        try:
            yield
        finally:
            self._current_timeout = previous_timeout

    def _retry(self, description: str, func):
        last_exc = None
        for attempt in range(1, self.config.api_retry_times + 1):
            try:
                return func()
            except Exception as exc:
                last_exc = exc
                self.logger.warning(
                    "SDK call failed: action=%s attempt=%s/%s reason=%s",
                    description,
                    attempt,
                    self.config.api_retry_times,
                    exc,
                )
                if attempt < self.config.api_retry_times:
                    time.sleep(self.config.api_retry_interval_seconds)
        raise SyncApiError(f"{description} failed after retries: {last_exc}")

    def get_or_create_dataset(self, dataset_name: str) -> DatasetRef:
        def action():
            datasets = self.client.list_datasets(name=dataset_name, page=1, page_size=100)
            dataset = next((item for item in datasets if item.name == dataset_name), None)
            if dataset is None:
                dataset = self.client.create_dataset(name=dataset_name)
            return dataset

        dataset = self._retry(f"get_or_create_dataset:{dataset_name}", action)
        self._datasets_by_id[str(dataset.id)] = dataset
        return DatasetRef(dataset_id=str(dataset.id), dataset_name=str(dataset.name))

    def _dataset(self, dataset_id: str):
        dataset = self._datasets_by_id.get(dataset_id)
        if dataset is None:
            def action():
                datasets = self.client.list_datasets(id=dataset_id, page=1, page_size=1)
                if not datasets:
                    raise SyncApiError(f"Dataset not found: {dataset_id}")
                return datasets[0]
            dataset = self._retry(f"load_dataset:{dataset_id}", action)
            self._datasets_by_id[dataset_id] = dataset
        return dataset

    def list_documents(self, dataset_id: str) -> List[RemoteDocumentSnapshot]:
        dataset = self._dataset(dataset_id)
        documents: List[RemoteDocumentSnapshot] = []
        page = 1
        while True:
            batch = self._retry(
                f"list_documents:{dataset_id}:page={page}",
                lambda: dataset.list_documents(page=page, page_size=self.config.remote_page_size),
            )
            for doc in batch:
                documents.append(
                    RemoteDocumentSnapshot(
                        document_id=str(doc.id),
                        name=str(doc.name),
                        run_status=ParseRunStatus.from_raw(doc.run),
                        progress=float(doc.progress or 0.0),
                        chunk_count=int(doc.chunk_count or 0),
                        token_count=int(doc.token_count or 0),
                        size=int(doc.size or 0),
                        meta_fields=_meta_fields(doc.meta_fields),
                    )
                )
            if len(batch) < self.config.remote_page_size:
                break
            page += 1
        return documents

    def upload_document_once(self, dataset_id: str, display_name: str, path: Path) -> RemoteDocumentSnapshot:
        dataset = self._dataset(dataset_id)
        def action():
            with path.open("rb") as handle:
                with self._timeout_scope(self.config.upload_timeout_seconds):
                    docs = dataset.upload_documents([{"display_name": display_name, "blob": handle}])
            if not docs:
                raise SyncApiError(f"Upload returned no documents for {display_name}")
            doc = docs[0]
            return RemoteDocumentSnapshot(
                document_id=str(doc.id),
                name=str(doc.name),
                run_status=ParseRunStatus.from_raw(doc.run),
                progress=float(doc.progress or 0.0),
                chunk_count=int(doc.chunk_count or 0),
                token_count=int(doc.token_count or 0),
                size=int(doc.size or 0),
                meta_fields=_meta_fields(doc.meta_fields),
            )
        try:
            return action()
        except Exception as exc:
            raise SyncApiError(f"upload_document:{display_name} failed: {exc}") from exc

    def upload_document(self, dataset_id: str, display_name: str, path: Path) -> RemoteDocumentSnapshot:
        return self.upload_document_once(dataset_id, display_name, path)

    def delete_documents(self, dataset_id: str, document_ids: List[str]) -> None:
        if not document_ids:
            return
        dataset = self._dataset(dataset_id)
        self._retry(
            f"delete_documents:{dataset_id}",
            lambda: dataset.delete_documents(ids=document_ids),
        )

    def trigger_async_parse(self, dataset_id: str, document_ids: List[str]) -> None:
        if not document_ids:
            return
        dataset = self._dataset(dataset_id)
        self._retry(
            f"trigger_async_parse:{dataset_id}",
            lambda: dataset.async_parse_documents(document_ids),
        )
