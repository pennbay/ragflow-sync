import io
import logging
import os
import tempfile
import types
import unittest
from pathlib import Path
from unittest import mock

from ragflow_sync.cli import main, run_target
from ragflow_sync.config import load_config
from ragflow_sync.executor import execute_sync_plan
from ragflow_sync.models import (
    ConfigError,
    DatasetRef,
    ParseRunStatus,
    REMOTE_NAME_MAX_BYTES,
    RemoteDocumentSnapshot,
    SyncState,
    SyncTargetConfig,
    TrackedFileState,
)
from ragflow_sync.planner import build_plan
from ragflow_sync.sdk_gateway import RagflowGateway, _meta_fields
from ragflow_sync.scanner import (
    is_managed_remote_name,
    parse_managed_remote_name,
    path_digest_for,
    remote_name_for,
    scan_local_files,
    utf8_truncate,
)
from ragflow_sync.state_store import empty_state, load_state


def make_config(root: Path) -> SyncTargetConfig:
    return SyncTargetConfig(
        api_key="key",
        base_url="http://example.test",
        dataset_name="dataset",
        local_dir=root,
        allowed_extensions={".md", ".pdf"},
        ignore_dirs={".git"},
        ignore_files={"Thumbs.db"},
        max_file_size_mb=2,
        remote_page_size=100,
        parse_trigger_batch_size=32,
        api_retry_times=1,
        api_retry_interval_seconds=0.0,
        api_timeout_seconds=60.0,
        upload_retry_times=2,
        upload_retry_interval_seconds=0.0,
        upload_timeout_seconds=180.0,
        log_level="ERROR",
        state_dir=root / "states",
        log_dir=root / "logs",
        max_parse_retry_times=2,
    )


class FakeGateway:
    def __init__(self, docs=None, fail_upload_once=False, fail_upload_names=None):
        self.docs = list(docs or [])
        self.deleted = []
        self.uploaded = []
        self.parsed = []
        self.parse_batches = []
        self.fail_upload_once = fail_upload_once
        self.fail_upload_names = set(fail_upload_names or [])

    def delete_documents(self, dataset_id, document_ids):
        self.deleted.extend(document_ids)
        self.docs = [doc for doc in self.docs if doc.document_id not in set(document_ids)]

    def upload_document(self, dataset_id, display_name, path):
        if display_name in self.fail_upload_names:
            from ragflow_sync.models import SyncApiError

            raise SyncApiError(f"simulated permanent upload failure for {display_name}")
        document_id = f"up-{len(self.uploaded) + 1}"
        doc = RemoteDocumentSnapshot(
            document_id=document_id,
            name=display_name,
            run_status=ParseRunStatus.UNSTART,
            progress=0.0,
            chunk_count=0,
            token_count=0,
            size=Path(path).stat().st_size,
        )
        self.uploaded.append((display_name, str(path)))
        self.docs.append(doc)
        if self.fail_upload_once:
            self.fail_upload_once = False
            from ragflow_sync.models import SyncApiError

            raise SyncApiError("simulated upload response failure")
        return doc

    def list_documents(self, dataset_id):
        return list(self.docs)

    def trigger_async_parse(self, dataset_id, document_ids):
        self.parse_batches.append(list(document_ids))
        self.parsed.extend(document_ids)


class FailingParseGateway(FakeGateway):
    def __init__(self, docs=None, fail_upload_once=False, fail_on_batch=1):
        super().__init__(docs=docs, fail_upload_once=fail_upload_once)
        self.fail_on_batch = fail_on_batch

    def trigger_async_parse(self, dataset_id, document_ids):
        from ragflow_sync.models import SyncApiError

        self.parse_batches.append(list(document_ids))
        if len(self.parse_batches) == self.fail_on_batch:
            raise SyncApiError("simulated parse failure")
        self.parsed.extend(document_ids)


def tracked_for(local, document_id="doc1", **overrides):
    values = {
        "abs_path": local.abs_path,
        "rel_path": local.rel_path,
        "remote_name": local.remote_name,
        "path_digest": local.path_digest,
        "document_id": document_id,
        "md5": local.md5,
        "mtime_ns": local.mtime_ns,
        "size": local.size,
    }
    values.update(overrides)
    return TrackedFileState(**values)


class JsonLike:
    def __init__(self, payload):
        self.payload = payload

    def to_json(self):
        return self.payload


class SyncProjectTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tempdir.name)
        self.logger = logging.getLogger(f"test-{id(self)}")
        self.logger.handlers.clear()
        self.logger.setLevel(logging.INFO)
        self.log_stream = io.StringIO()
        handler = logging.StreamHandler(self.log_stream)
        handler.setLevel(logging.INFO)
        self.logger.addHandler(handler)

    def tearDown(self):
        self.tempdir.cleanup()

    def test_load_config_requires_one_directory_per_target(self):
        module = types.ModuleType("temp_config")
        module.BASE_URL = "http://example.test"
        module.SYNC_TARGETS = [{"DATASET_NAME": "dataset", "LOCAL_DIR": str(self.root)}]
        module.ALLOWED_EXTENSIONS = [".txt"]
        module.IGNORE_DIRS = []
        module.IGNORE_FILES = []
        module.MAX_FILE_SIZE_MB = 1
        module.REMOTE_PAGE_SIZE = 10
        module.API_RETRY_TIMES = 1
        module.API_RETRY_INTERVAL_SECONDS = 0
        module.API_TIMEOUT_SECONDS = 60
        module.UPLOAD_RETRY_TIMES = 2
        module.UPLOAD_RETRY_INTERVAL_SECONDS = 3
        module.UPLOAD_TIMEOUT_SECONDS = 180
        module.PARSE_TRIGGER_BATCH_SIZE = 32
        module.LOG_LEVEL = "INFO"
        module.STATE_DIR = "states"
        module.LOG_DIR = "logs"
        module.MAX_PARSE_RETRY_TIMES = 3
        with mock.patch.dict("sys.modules", {"temp_config": module}), mock.patch.dict(
            os.environ, {"RAGFLOW_API_KEY": "env-key"}
        ):
            configs = load_config("temp_config")
        self.assertEqual(1, len(configs))
        self.assertEqual(self.root.resolve(), configs[0].local_dir)
        self.assertIn(".pdf", configs[0].allowed_extensions)
        self.assertEqual(32, configs[0].parse_trigger_batch_size)
        self.assertEqual(60.0, configs[0].api_timeout_seconds)
        self.assertEqual(2, configs[0].upload_retry_times)
        self.assertEqual(3.0, configs[0].upload_retry_interval_seconds)
        self.assertEqual(180.0, configs[0].upload_timeout_seconds)
        self.assertEqual(Path("states") / "dataset.json", configs[0].state_path)

    def test_load_config_uses_split_api_and_upload_defaults(self):
        module = types.ModuleType("defaults_config")
        module.BASE_URL = "http://example.test"
        module.SYNC_TARGETS = [{"DATASET_NAME": "dataset", "LOCAL_DIR": str(self.root)}]
        with mock.patch.dict("sys.modules", {"defaults_config": module}), mock.patch.dict(
            os.environ, {"RAGFLOW_API_KEY": "env-key"}
        ):
            configs = load_config("defaults_config")

        self.assertEqual(30.0, configs[0].api_timeout_seconds)
        self.assertEqual(2, configs[0].api_retry_times)
        self.assertEqual(2.0, configs[0].api_retry_interval_seconds)
        self.assertEqual(180.0, configs[0].upload_timeout_seconds)
        self.assertEqual(2, configs[0].upload_retry_times)
        self.assertEqual(3.0, configs[0].upload_retry_interval_seconds)

    def test_load_config_rejects_invalid_upload_retry_times(self):
        module = types.ModuleType("bad_upload_retry_config")
        module.BASE_URL = "http://example.test"
        module.SYNC_TARGETS = [{"DATASET_NAME": "dataset", "LOCAL_DIR": str(self.root)}]
        module.UPLOAD_RETRY_TIMES = 0
        with mock.patch.dict("sys.modules", {"bad_upload_retry_config": module}), mock.patch.dict(
            os.environ, {"RAGFLOW_API_KEY": "env-key"}
        ):
            with self.assertRaisesRegex(ConfigError, "UPLOAD_RETRY_TIMES"):
                load_config("bad_upload_retry_config")

    def test_load_config_rejects_invalid_upload_retry_interval(self):
        module = types.ModuleType("bad_upload_retry_interval_config")
        module.BASE_URL = "http://example.test"
        module.SYNC_TARGETS = [{"DATASET_NAME": "dataset", "LOCAL_DIR": str(self.root)}]
        module.UPLOAD_RETRY_INTERVAL_SECONDS = 0
        with mock.patch.dict("sys.modules", {"bad_upload_retry_interval_config": module}), mock.patch.dict(
            os.environ, {"RAGFLOW_API_KEY": "env-key"}
        ):
            with self.assertRaisesRegex(ConfigError, "UPLOAD_RETRY_INTERVAL_SECONDS"):
                load_config("bad_upload_retry_interval_config")

    def test_load_config_rejects_invalid_upload_timeout(self):
        module = types.ModuleType("bad_upload_timeout_config")
        module.BASE_URL = "http://example.test"
        module.SYNC_TARGETS = [{"DATASET_NAME": "dataset", "LOCAL_DIR": str(self.root)}]
        module.UPLOAD_TIMEOUT_SECONDS = 0
        with mock.patch.dict("sys.modules", {"bad_upload_timeout_config": module}), mock.patch.dict(
            os.environ, {"RAGFLOW_API_KEY": "env-key"}
        ):
            with self.assertRaisesRegex(ConfigError, "UPLOAD_TIMEOUT_SECONDS"):
                load_config("bad_upload_timeout_config")

    def test_load_config_rejects_duplicate_directory(self):
        module = types.ModuleType("dup_dir_config")
        module.BASE_URL = "http://example.test"
        module.SYNC_TARGETS = [
            {"DATASET_NAME": "one", "LOCAL_DIR": str(self.root)},
            {"DATASET_NAME": "two", "LOCAL_DIR": str(self.root)},
        ]
        with mock.patch.dict("sys.modules", {"dup_dir_config": module}), mock.patch.dict(
            os.environ, {"RAGFLOW_API_KEY": "env-key"}
        ):
            with self.assertRaisesRegex(ConfigError, "Duplicate LOCAL_DIR"):
                load_config("dup_dir_config")

    def test_load_config_rejects_invalid_parse_batch_size(self):
        module = types.ModuleType("bad_parse_batch_config")
        module.BASE_URL = "http://example.test"
        module.SYNC_TARGETS = [{"DATASET_NAME": "dataset", "LOCAL_DIR": str(self.root)}]
        module.PARSE_TRIGGER_BATCH_SIZE = 0
        with mock.patch.dict("sys.modules", {"bad_parse_batch_config": module}), mock.patch.dict(
            os.environ, {"RAGFLOW_API_KEY": "env-key"}
        ):
            with self.assertRaisesRegex(ConfigError, "PARSE_TRIGGER_BATCH_SIZE"):
                load_config("bad_parse_batch_config")

    def test_scan_local_files_filters_and_builds_remote_name(self):
        (self.root / "doc.md").write_text("hello")
        (self.root / "ignore.txt").write_text("no")
        (self.root / ".git").mkdir()
        (self.root / ".git" / "hidden.md").write_text("hidden")
        config = make_config(self.root)
        state = empty_state(config.dataset_name, "", str(config.local_dir))

        files = scan_local_files(config, state, self.logger)

        self.assertEqual(1, len(files))
        local = next(iter(files.values()))
        self.assertEqual(path_digest_for("doc.md"), local.path_digest)
        self.assertEqual(remote_name_for("doc.md", "doc.md", local.md5), local.remote_name)
        self.assertIn("__md5__", local.remote_name)
        parsed = parse_managed_remote_name(local.remote_name)
        self.assertIsNotNone(parsed)
        self.assertEqual(local.path_digest, parsed.path_digest)
        self.assertEqual(local.md5, parsed.md5)
        self.assertTrue(local.remote_name.endswith(".md"))
        self.assertLessEqual(len(local.remote_name.encode("utf-8")), REMOTE_NAME_MAX_BYTES)
        self.assertTrue(is_managed_remote_name(local.remote_name))
        self.assertFalse(is_managed_remote_name("doc__rf__legacy.md"))

    def test_remote_name_truncates_long_chinese_stem_by_utf8_bytes(self):
        filename = f"{'超长文件名' * 40}.pdf"
        rel_path = f"folder/{filename}"
        md5 = "a" * 32

        remote_name = remote_name_for(rel_path, filename, md5)
        parsed = parse_managed_remote_name(remote_name)

        self.assertLessEqual(len(remote_name.encode("utf-8")), REMOTE_NAME_MAX_BYTES)
        self.assertTrue(remote_name.endswith(".pdf"))
        self.assertIsNotNone(parsed)
        self.assertEqual(path_digest_for(rel_path), parsed.path_digest)
        self.assertEqual(md5, parsed.md5)
        remote_name.encode("utf-8").decode("utf-8")

    def test_remote_name_truncates_long_english_stem_by_utf8_bytes(self):
        filename = f"{'very-long-title-' * 40}.md"
        rel_path = f"notes/{filename}"
        md5 = "b" * 32

        remote_name = remote_name_for(rel_path, filename, md5)

        self.assertLessEqual(len(remote_name.encode("utf-8")), REMOTE_NAME_MAX_BYTES)
        self.assertTrue(remote_name.endswith(".md"))
        self.assertTrue(is_managed_remote_name(remote_name))

    def test_utf8_truncate_preserves_character_boundaries_and_fallback(self):
        self.assertEqual("你", utf8_truncate("你好", 3))
        self.assertEqual("file", utf8_truncate("你好", 2))
        self.assertEqual("abc", utf8_truncate("abc   ", 6))

    def test_gateway_meta_fields_accepts_sdk_base_like_object(self):
        self.assertEqual({"a": 1}, _meta_fields(JsonLike({"a": 1})))
        self.assertEqual({"b": 2}, _meta_fields({"b": 2}))
        self.assertEqual({}, _meta_fields(None))

    def test_gateway_upload_uses_upload_timeout_and_restores_api_timeout(self):
        config = make_config(self.root)
        config = SyncTargetConfig(
            **{
                **config.__dict__,
                "api_timeout_seconds": 30.0,
                "upload_timeout_seconds": 180.0,
            }
        )
        timeout_calls = []

        class FakeDoc:
            id = "doc-1"
            name = "doc.md"
            run = "UNSTART"
            progress = 0.0
            chunk_count = 0
            token_count = 0
            size = 3
            meta_fields = None

        class FakeDataset:
            def __init__(self, client):
                self.client = client

            def upload_documents(self, payload):
                timeout_calls.append(self.client.get("/datasets"))
                return [FakeDoc()]

        class FakeClient:
            def __init__(self):
                self.api_url = "http://example.test"
                self.authorization_header = {"Authorization": "Bearer key"}

            def list_datasets(self, **kwargs):
                return []

            def create_dataset(self, name):
                return type("DatasetRefLike", (), {"id": "ds1", "name": name})()

        fake_client = FakeClient()
        path = self.root / "doc.md"
        path.write_text("hey")

        with mock.patch("ragflow_sync.sdk_gateway.RAGFlow", return_value=fake_client), mock.patch(
            "ragflow_sync.sdk_gateway.requests.get",
            side_effect=lambda **kwargs: kwargs["timeout"],
        ), mock.patch(
            "ragflow_sync.sdk_gateway.requests.post",
            side_effect=lambda **kwargs: kwargs["timeout"],
        ), mock.patch(
            "ragflow_sync.sdk_gateway.requests.delete",
            side_effect=lambda **kwargs: kwargs["timeout"],
        ), mock.patch(
            "ragflow_sync.sdk_gateway.requests.put",
            side_effect=lambda **kwargs: kwargs["timeout"],
        ):
            gateway = RagflowGateway(config, self.logger)
            gateway._datasets_by_id["ds1"] = FakeDataset(fake_client)
            uploaded = gateway.upload_document_once("ds1", "doc.md", path)
            after_timeout = gateway.client.get("/datasets")

        self.assertEqual("doc-1", uploaded.document_id)
        self.assertEqual([180.0], timeout_calls)
        self.assertEqual(30.0, after_timeout)

    def test_remote_name_is_stable_for_same_long_file(self):
        filename = f"{'资料' * 80}.pdf"
        rel_path = f"archive/{filename}"
        md5 = "c" * 32

        first = remote_name_for(rel_path, filename, md5)
        second = remote_name_for(rel_path, filename, md5)

        self.assertEqual(first, second)
        self.assertLessEqual(len(first.encode("utf-8")), REMOTE_NAME_MAX_BYTES)

    def test_scan_reuses_md5_for_unchanged_file(self):
        path = self.root / "doc.md"
        path.write_text("hello")
        config = make_config(self.root)
        abs_path = str(path.resolve())
        rel_path = "doc.md"
        remote_name = remote_name_for(rel_path, "doc.md", "cached-md5")
        state = SyncState(
            version=2,
            dataset_name=config.dataset_name,
            dataset_id="",
            target_root=str(config.local_dir),
            last_sync_at="",
            files={
                abs_path: TrackedFileState(
                    abs_path=abs_path,
                    rel_path=rel_path,
                    remote_name=remote_name,
                    path_digest=path_digest_for(rel_path),
                    document_id="doc1",
                    md5="cached-md5",
                    mtime_ns=path.stat().st_mtime_ns,
                    size=path.stat().st_size,
                )
            },
        )

        files = scan_local_files(config, state, self.logger)

        self.assertEqual("cached-md5", files[abs_path].md5)

    def test_planner_marks_status_based_on_document_run(self):
        path = self.root / "doc.md"
        path.write_text("hello")
        config = make_config(self.root)
        abs_path = str(path.resolve())
        local_files = scan_local_files(config, empty_state(config.dataset_name, "", str(config.local_dir)), self.logger)
        local = local_files[abs_path]
        state = SyncState(
            version=2,
            dataset_name=config.dataset_name,
            dataset_id="ds1",
            target_root=str(config.local_dir),
            last_sync_at="",
            files={
                abs_path: tracked_for(local, document_id="doc1")
            },
        )
        remote_docs = [
            RemoteDocumentSnapshot("doc1", local.remote_name, ParseRunStatus.UNSTART, 0.0, 0, 0)
        ]

        plan = build_plan(local_files, remote_docs, state, config)

        self.assertEqual(["doc1"], plan.parse_actions)

    def test_planner_retries_failed_parse_until_limit(self):
        path = self.root / "doc.md"
        path.write_text("hello")
        config = make_config(self.root)
        abs_path = str(path.resolve())
        local_files = scan_local_files(config, empty_state(config.dataset_name, "", str(config.local_dir)), self.logger)
        local = local_files[abs_path]
        state = SyncState(
            version=2,
            dataset_name=config.dataset_name,
            dataset_id="ds1",
            target_root=str(config.local_dir),
            last_sync_at="",
            files={
                abs_path: tracked_for(
                    local,
                    document_id="doc1",
                    parse_retry_count=config.max_parse_retry_times,
                )
            },
        )
        remote_docs = [RemoteDocumentSnapshot("doc1", local.remote_name, ParseRunStatus.FAIL, 0.0, 0, 0)]

        plan = build_plan(local_files, remote_docs, state, config)

        self.assertEqual([abs_path], plan.abnormal_files)
        self.assertEqual([], plan.parse_actions)

    def test_planner_deletes_managed_orphan_remote_documents(self):
        config = make_config(self.root)
        state = empty_state(config.dataset_name, "ds1", str(config.local_dir))
        remote_name = remote_name_for("ghost.md", "ghost.md", "0" * 32)
        remote_docs = [
            RemoteDocumentSnapshot("doc1", remote_name, ParseRunStatus.DONE, 1.0, 2, 10),
            RemoteDocumentSnapshot("doc2", "manual-file.md", ParseRunStatus.DONE, 1.0, 2, 10),
        ]

        plan = build_plan({}, remote_docs, state, config)

        self.assertEqual(["doc1"], [item.document_id for item in plan.delete_actions])
        self.assertTrue(any("manual-file.md" in warning for warning in plan.warnings))

    def test_planner_adopts_existing_v2_remote_when_state_missing(self):
        path = self.root / "doc.md"
        path.write_text("hello")
        config = make_config(self.root)
        state = empty_state(config.dataset_name, "ds1", str(config.local_dir))
        local_files = scan_local_files(config, state, self.logger)
        local = next(iter(local_files.values()))
        remote_docs = [
            RemoteDocumentSnapshot("doc1", local.remote_name, ParseRunStatus.UNSTART, 0.0, 0, 0)
        ]

        plan = build_plan(local_files, remote_docs, state, config)

        self.assertEqual([], plan.upload_actions)
        self.assertEqual(["doc1"], [item.document_id for item in plan.adopt_actions])
        self.assertEqual(["doc1"], plan.parse_actions)

    def test_planner_cleans_duplicate_v2_managed_documents(self):
        path = self.root / "doc.md"
        path.write_text("hello")
        config = make_config(self.root)
        state = empty_state(config.dataset_name, "ds1", str(config.local_dir))
        local_files = scan_local_files(config, state, self.logger)
        local = next(iter(local_files.values()))
        remote_docs = [
            RemoteDocumentSnapshot("doc1", local.remote_name, ParseRunStatus.DONE, 1.0, 1, 1),
            RemoteDocumentSnapshot("doc2", local.remote_name, ParseRunStatus.DONE, 1.0, 1, 1),
        ]

        plan = build_plan(local_files, remote_docs, state, config)

        self.assertEqual(["doc1"], [item.document_id for item in plan.adopt_actions])
        self.assertEqual(["doc2"], [item.document_id for item in plan.delete_actions])

    def test_executor_uploads_then_deletes_previous_version_then_triggers_parse(self):
        path = self.root / "doc.md"
        path.write_text("new")
        config = make_config(self.root)
        local_files = scan_local_files(config, empty_state(config.dataset_name, "", str(config.local_dir)), self.logger)
        local = next(iter(local_files.values()))
        state = SyncState(
            version=2,
            dataset_name=config.dataset_name,
            dataset_id="ds1",
            target_root=str(config.local_dir),
            last_sync_at="",
            files={
                local.abs_path: tracked_for(local, document_id="old-doc", md5="old-md5", mtime_ns=1, size=1)
            },
        )
        plan = build_plan(local_files, [], state, config)
        gateway = FakeGateway()

        code = execute_sync_plan(
            gateway,
            DatasetRef(dataset_id="ds1", dataset_name=config.dataset_name),
            config,
            state,
            plan,
            self.logger,
            dry_run=False,
        )

        self.assertEqual(0, code)
        self.assertEqual(["old-doc"], gateway.deleted)
        self.assertEqual(1, len(gateway.uploaded))
        self.assertEqual(["up-1"], gateway.parsed)
        self.assertEqual("up-1", state.files[local.abs_path].document_id)
        self.assertTrue(config.state_path.exists())
        logs = self.log_stream.getvalue()
        self.assertIn("Upload phase started. total=1", logs)
        self.assertIn("Uploading file 1/1.", logs)
        self.assertIn("Uploaded file 1/1.", logs)
        self.assertIn("Async parse triggered successfully. count=1", logs)

    def test_executor_persists_uploaded_state_before_parse(self):
        path = self.root / "doc.md"
        path.write_text("new")
        config = make_config(self.root)
        state = empty_state(config.dataset_name, "ds1", str(config.local_dir))
        local_files = scan_local_files(config, state, self.logger)
        local = next(iter(local_files.values()))
        plan = build_plan(local_files, [], state, config)
        gateway = FailingParseGateway()

        with self.assertRaises(Exception):
            execute_sync_plan(
                gateway,
                DatasetRef(dataset_id="ds1", dataset_name=config.dataset_name),
                config,
                state,
                plan,
                self.logger,
                dry_run=False,
            )

        loaded = load_state(config.state_path, config.dataset_name, str(config.local_dir), self.logger)
        self.assertEqual("up-1", loaded.files[local.abs_path].document_id)

    def test_executor_recovers_when_upload_response_fails_after_remote_create(self):
        path = self.root / "doc.md"
        path.write_text("new")
        config = make_config(self.root)
        state = empty_state(config.dataset_name, "ds1", str(config.local_dir))
        local_files = scan_local_files(config, state, self.logger)
        local = next(iter(local_files.values()))
        plan = build_plan(local_files, [], state, config)
        gateway = FakeGateway(fail_upload_once=True)

        code = execute_sync_plan(
            gateway,
            DatasetRef(dataset_id="ds1", dataset_name=config.dataset_name),
            config,
            state,
            plan,
            self.logger,
            dry_run=False,
        )

        self.assertEqual(0, code)
        self.assertEqual(1, len(gateway.uploaded))
        self.assertEqual("up-1", state.files[local.abs_path].document_id)
        self.assertEqual(["up-1"], gateway.parsed)

    def test_executor_continues_after_one_upload_failure(self):
        config = make_config(self.root)
        state = empty_state(config.dataset_name, "ds1", str(config.local_dir))
        first_path = self.root / "first.md"
        bad_path = self.root / "bad.md"
        last_path = self.root / "last.md"
        first_path.write_text("first")
        bad_path.write_text("bad")
        last_path.write_text("last")
        local_files = scan_local_files(config, state, self.logger)
        bad_local = next(local for local in local_files.values() if local.filename == "bad.md")
        plan = build_plan(local_files, [], state, config)
        gateway = FakeGateway(fail_upload_names={bad_local.remote_name})

        code = execute_sync_plan(
            gateway,
            DatasetRef(dataset_id="ds1", dataset_name=config.dataset_name),
            config,
            state,
            plan,
            self.logger,
            dry_run=False,
        )

        self.assertEqual(2, code)
        self.assertEqual(2, len(gateway.uploaded))
        self.assertEqual(2, len(gateway.parsed))
        self.assertEqual("", state.files[bad_local.abs_path].document_id)
        self.assertIn("simulated permanent upload failure", state.files[bad_local.abs_path].last_error)
        logs = self.log_stream.getvalue()
        self.assertIn("Upload phase completed. uploaded=2/3 failed=1", logs)
        self.assertIn("Upload failure summary: path=bad.md", logs)

    def test_executor_uses_upload_retry_count_in_logs(self):
        path = self.root / "bad.md"
        path.write_text("bad")
        config = make_config(self.root)
        config = SyncTargetConfig(
            **{
                **config.__dict__,
                "api_retry_times": 5,
                "upload_retry_times": 2,
                "upload_retry_interval_seconds": 0.0,
            }
        )
        state = empty_state(config.dataset_name, "ds1", str(config.local_dir))
        local_files = scan_local_files(config, state, self.logger)
        bad_local = next(iter(local_files.values()))
        plan = build_plan(local_files, [], state, config)
        gateway = FakeGateway(fail_upload_names={bad_local.remote_name})

        code = execute_sync_plan(
            gateway,
            DatasetRef(dataset_id="ds1", dataset_name=config.dataset_name),
            config,
            state,
            plan,
            self.logger,
            dry_run=False,
        )

        self.assertEqual(2, code)
        logs = self.log_stream.getvalue()
        self.assertIn("attempt=1/2", logs)
        self.assertIn("attempt=2/2", logs)
        self.assertNotIn("attempt=1/5", logs)

    def test_executor_keeps_previous_remote_when_modified_upload_fails(self):
        path = self.root / "doc.md"
        path.write_text("new")
        config = make_config(self.root)
        local_files = scan_local_files(config, empty_state(config.dataset_name, "", str(config.local_dir)), self.logger)
        local = next(iter(local_files.values()))
        state = SyncState(
            version=2,
            dataset_name=config.dataset_name,
            dataset_id="ds1",
            target_root=str(config.local_dir),
            last_sync_at="",
            files={
                local.abs_path: tracked_for(local, document_id="old-doc", md5="old-md5", mtime_ns=1, size=1)
            },
        )
        plan = build_plan(local_files, [], state, config)
        gateway = FakeGateway(fail_upload_names={local.remote_name})

        code = execute_sync_plan(
            gateway,
            DatasetRef(dataset_id="ds1", dataset_name=config.dataset_name),
            config,
            state,
            plan,
            self.logger,
            dry_run=False,
        )

        self.assertEqual(2, code)
        self.assertEqual([], gateway.deleted)
        self.assertEqual("", state.files[local.abs_path].document_id)
        self.assertIn("simulated permanent upload failure", state.files[local.abs_path].last_error)

    def test_executor_triggers_parse_in_batches(self):
        config = make_config(self.root)
        config = SyncTargetConfig(**{**config.__dict__, "parse_trigger_batch_size": 32})
        state = empty_state(config.dataset_name, "ds1", str(config.local_dir))
        for index in range(106):
            path = self.root / f"doc-{index}.md"
            path.write_text(f"hello {index}")
        local_files = scan_local_files(config, state, self.logger)
        for index, local in enumerate(local_files.values()):
            state.files[local.abs_path] = tracked_for(local, document_id=f"doc-{index}")
        remote_docs = [
            RemoteDocumentSnapshot(f"doc-{index}", local.remote_name, ParseRunStatus.UNSTART, 0.0, 0, 0)
            for index, local in enumerate(local_files.values())
        ]
        plan = build_plan(local_files, remote_docs, state, config)
        gateway = FakeGateway(docs=remote_docs)

        code = execute_sync_plan(
            gateway,
            DatasetRef(dataset_id="ds1", dataset_name=config.dataset_name),
            config,
            state,
            plan,
            self.logger,
            dry_run=False,
        )

        self.assertEqual(0, code)
        self.assertEqual([32, 32, 32, 10], [len(batch) for batch in gateway.parse_batches])
        self.assertTrue(all(item.parse_retry_count == 1 for item in state.files.values()))
        logs = self.log_stream.getvalue()
        self.assertIn("Triggering async parse. total=106 batch_size=32", logs)
        self.assertIn("Async parse batch triggered. batch=4 triggered=106/106", logs)

    def test_executor_saves_successful_parse_batches_before_later_failure(self):
        config = make_config(self.root)
        config = SyncTargetConfig(**{**config.__dict__, "parse_trigger_batch_size": 2})
        state = empty_state(config.dataset_name, "ds1", str(config.local_dir))
        for index in range(5):
            path = self.root / f"doc-{index}.md"
            path.write_text(f"hello {index}")
        local_files = scan_local_files(config, state, self.logger)
        ordered_locals = list(local_files.values())
        for index, local in enumerate(ordered_locals):
            state.files[local.abs_path] = tracked_for(local, document_id=f"doc-{index}")
        remote_docs = [
            RemoteDocumentSnapshot(f"doc-{index}", local.remote_name, ParseRunStatus.UNSTART, 0.0, 0, 0)
            for index, local in enumerate(ordered_locals)
        ]
        plan = build_plan(local_files, remote_docs, state, config)
        gateway = FailingParseGateway(docs=remote_docs, fail_on_batch=2)

        with self.assertRaises(Exception):
            execute_sync_plan(
                gateway,
                DatasetRef(dataset_id="ds1", dataset_name=config.dataset_name),
                config,
                state,
                plan,
                self.logger,
                dry_run=False,
            )

        loaded = load_state(config.state_path, config.dataset_name, str(config.local_dir), self.logger)
        retry_counts = {
            tracked.document_id: tracked.parse_retry_count
            for tracked in loaded.files.values()
        }
        self.assertEqual(1, retry_counts["doc-0"])
        self.assertEqual(1, retry_counts["doc-1"])
        self.assertEqual(0, retry_counts["doc-2"])
        self.assertEqual(0, retry_counts["doc-3"])
        self.assertEqual(0, retry_counts["doc-4"])

    def test_executor_dry_run_writes_nothing(self):
        path = self.root / "doc.md"
        path.write_text("new")
        config = make_config(self.root)
        state = empty_state(config.dataset_name, "ds1", str(config.local_dir))
        local_files = scan_local_files(config, state, self.logger)
        plan = build_plan(local_files, [], state, config)
        gateway = FakeGateway()

        code = execute_sync_plan(
            gateway,
            DatasetRef(dataset_id="ds1", dataset_name=config.dataset_name),
            config,
            state,
            plan,
            self.logger,
            dry_run=True,
        )

        self.assertEqual(0, code)
        self.assertEqual([], gateway.uploaded)
        self.assertFalse(config.state_path.exists())

    def test_state_store_uses_backup_on_primary_failure(self):
        config = make_config(self.root)
        config.state_dir.mkdir(parents=True, exist_ok=True)
        path = config.state_path
        backup = path.with_suffix(path.suffix + ".bak")
        path.write_text("{broken", encoding="utf-8")
        backup.write_text(
            '{"version":2,"dataset_name":"dataset","dataset_id":"ds1","target_root":"x","last_sync_at":"","files":{}}',
            encoding="utf-8",
        )

        state = load_state(path, "dataset", "x", self.logger)

        self.assertEqual("ds1", state.dataset_id)

    def test_state_store_ignores_old_schema(self):
        config = make_config(self.root)
        config.state_dir.mkdir(parents=True, exist_ok=True)
        config.state_path.write_text(
            '{"version":1,"dataset_name":"dataset","dataset_id":"old","target_root":"x","last_sync_at":"","files":{}}',
            encoding="utf-8",
        )

        state = load_state(config.state_path, "dataset", "x", self.logger)

        self.assertEqual(2, state.version)
        self.assertEqual("", state.dataset_id)
        self.assertEqual({}, state.files)
        self.assertIn("unsupported state version", self.log_stream.getvalue())

    def test_run_target_uses_new_pipeline(self):
        config = make_config(self.root)
        (self.root / "doc.md").write_text("hello")
        with mock.patch("ragflow_sync.cli.RagflowGateway") as gateway_cls:
            gateway = gateway_cls.return_value
            gateway.get_or_create_dataset.return_value = DatasetRef("ds1", config.dataset_name)
            local_state = empty_state(config.dataset_name, "", str(config.local_dir))
            local = next(iter(scan_local_files(config, local_state, self.logger).values()))
            uploaded = RemoteDocumentSnapshot(
                "doc1", local.remote_name, ParseRunStatus.UNSTART, 0.0, 0, 0
            )
            gateway.list_documents.side_effect = [[], [uploaded]]
            gateway.upload_document.return_value = RemoteDocumentSnapshot(
                "doc1", local.remote_name, ParseRunStatus.UNSTART, 0.0, 0, 0
            )
            gateway.upload_document_once.return_value = RemoteDocumentSnapshot(
                "doc1", local.remote_name, ParseRunStatus.UNSTART, 0.0, 0, 0
            )
            code = run_target(config, dry_run=False)

        self.assertEqual(0, code)
        gateway.trigger_async_parse.assert_called_once()

    def test_cli_returns_partial_failure_for_one_bad_target(self):
        config1 = make_config(self.root / "one")
        config2 = make_config(self.root / "two")
        config1.local_dir.mkdir(parents=True)
        config2.local_dir.mkdir(parents=True)
        config1 = SyncTargetConfig(**{**config1.__dict__, "dataset_name": "one"})
        config2 = SyncTargetConfig(**{**config2.__dict__, "dataset_name": "two"})

        with mock.patch("ragflow_sync.cli.load_config", return_value=[config1, config2]), mock.patch(
            "ragflow_sync.cli.run_target", side_effect=[2, 0]
        ):
            code = main([])

        self.assertEqual(2, code)

    def test_cli_target_filter_reports_missing_target(self):
        with mock.patch("ragflow_sync.cli.load_config", return_value=[]), mock.patch(
            "sys.stdout", new_callable=io.StringIO
        ) as stdout:
            code = main(["--target", "missing"])

        self.assertEqual(1, code)
        self.assertIn("Target not found", stdout.getvalue())


if __name__ == "__main__":
    unittest.main()
