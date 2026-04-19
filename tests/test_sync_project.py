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
    RemoteDocumentSnapshot,
    SyncState,
    SyncTargetConfig,
    TrackedFileState,
)
from ragflow_sync.planner import build_plan
from ragflow_sync.scanner import is_managed_remote_name, remote_name_for, scan_local_files
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
        upload_batch_size=2,
        remote_page_size=100,
        api_retry_times=1,
        api_retry_interval_seconds=0.0,
        log_level="ERROR",
        state_dir=root / "states",
        log_dir=root / "logs",
        max_parse_retry_times=2,
    )


class FakeGateway:
    def __init__(self, docs=None):
        self.docs = list(docs or [])
        self.deleted = []
        self.uploaded = []
        self.parsed = []

    def delete_documents(self, dataset_id, document_ids):
        self.deleted.extend(document_ids)
        self.docs = [doc for doc in self.docs if doc.document_id not in set(document_ids)]

    def upload_document(self, dataset_id, display_name, path):
        document_id = f"up-{len(self.uploaded) + 1}"
        doc = RemoteDocumentSnapshot(
            document_id=document_id,
            name=display_name,
            run_status=ParseRunStatus.UNSTART,
            progress=0.0,
            chunk_count=0,
            token_count=0,
        )
        self.uploaded.append((display_name, str(path)))
        self.docs.append(doc)
        return doc

    def list_documents(self, dataset_id):
        return list(self.docs)

    def trigger_async_parse(self, dataset_id, document_ids):
        self.parsed.extend(document_ids)


class SyncProjectTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tempdir.name)
        self.logger = logging.getLogger(f"test-{id(self)}")
        self.logger.handlers.clear()
        self.logger.addHandler(logging.NullHandler())

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
        module.UPLOAD_BATCH_SIZE = 1
        module.REMOTE_PAGE_SIZE = 10
        module.API_RETRY_TIMES = 1
        module.API_RETRY_INTERVAL_SECONDS = 0
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
        self.assertEqual(Path("states") / "dataset.json", configs[0].state_path)

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
        self.assertTrue(local.remote_name.endswith(".md"))
        self.assertTrue(is_managed_remote_name(local.remote_name))

    def test_scan_reuses_md5_for_unchanged_file(self):
        path = self.root / "doc.md"
        path.write_text("hello")
        config = make_config(self.root)
        abs_path = str(path.resolve())
        state = SyncState(
            version=1,
            dataset_name=config.dataset_name,
            dataset_id="",
            target_root=str(config.local_dir),
            last_sync_at="",
            files={
                abs_path: TrackedFileState(
                    abs_path=abs_path,
                    remote_name=remote_name_for("doc.md", "doc.md"),
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
        remote_name = remote_name_for("doc.md", "doc.md")
        local_files = scan_local_files(config, empty_state(config.dataset_name, "", str(config.local_dir)), self.logger)
        state = SyncState(
            version=1,
            dataset_name=config.dataset_name,
            dataset_id="ds1",
            target_root=str(config.local_dir),
            last_sync_at="",
            files={
                abs_path: TrackedFileState(
                    abs_path=abs_path,
                    remote_name=remote_name,
                    document_id="doc1",
                    md5=local_files[abs_path].md5,
                    mtime_ns=local_files[abs_path].mtime_ns,
                    size=local_files[abs_path].size,
                )
            },
        )
        remote_docs = [
            RemoteDocumentSnapshot("doc1", remote_name, ParseRunStatus.UNSTART, 0.0, 0, 0)
        ]

        plan = build_plan(local_files, remote_docs, state, config)

        self.assertEqual(["doc1"], plan.parse_actions)

    def test_planner_retries_failed_parse_until_limit(self):
        path = self.root / "doc.md"
        path.write_text("hello")
        config = make_config(self.root)
        abs_path = str(path.resolve())
        local_files = scan_local_files(config, empty_state(config.dataset_name, "", str(config.local_dir)), self.logger)
        remote_name = local_files[abs_path].remote_name
        state = SyncState(
            version=1,
            dataset_name=config.dataset_name,
            dataset_id="ds1",
            target_root=str(config.local_dir),
            last_sync_at="",
            files={
                abs_path: TrackedFileState(
                    abs_path=abs_path,
                    remote_name=remote_name,
                    document_id="doc1",
                    md5=local_files[abs_path].md5,
                    mtime_ns=local_files[abs_path].mtime_ns,
                    size=local_files[abs_path].size,
                    parse_retry_count=config.max_parse_retry_times,
                )
            },
        )
        remote_docs = [RemoteDocumentSnapshot("doc1", remote_name, ParseRunStatus.FAIL, 0.0, 0, 0)]

        plan = build_plan(local_files, remote_docs, state, config)

        self.assertEqual([abs_path], plan.abnormal_files)
        self.assertEqual([], plan.parse_actions)

    def test_planner_deletes_managed_orphan_remote_documents(self):
        config = make_config(self.root)
        state = empty_state(config.dataset_name, "ds1", str(config.local_dir))
        remote_docs = [
            RemoteDocumentSnapshot("doc1", remote_name_for("ghost.md", "ghost.md"), ParseRunStatus.DONE, 1.0, 2, 10),
            RemoteDocumentSnapshot("doc2", "manual-file.md", ParseRunStatus.DONE, 1.0, 2, 10),
        ]

        plan = build_plan({}, remote_docs, state, config)

        self.assertEqual(["doc1"], [item.document_id for item in plan.delete_actions])
        self.assertTrue(any("manual-file.md" in warning for warning in plan.warnings))

    def test_executor_uploads_then_deletes_previous_version_then_triggers_parse(self):
        path = self.root / "doc.md"
        path.write_text("new")
        config = make_config(self.root)
        local_files = scan_local_files(config, empty_state(config.dataset_name, "", str(config.local_dir)), self.logger)
        local = next(iter(local_files.values()))
        state = SyncState(
            version=1,
            dataset_name=config.dataset_name,
            dataset_id="ds1",
            target_root=str(config.local_dir),
            last_sync_at="",
            files={
                local.abs_path: TrackedFileState(
                    abs_path=local.abs_path,
                    remote_name=local.remote_name,
                    document_id="old-doc",
                    md5="old-md5",
                    mtime_ns=1,
                    size=1,
                )
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
            '{"version":1,"dataset_name":"dataset","dataset_id":"ds1","target_root":"x","last_sync_at":"","files":{}}',
            encoding="utf-8",
        )

        state = load_state(path, "dataset", "x", self.logger)

        self.assertEqual("ds1", state.dataset_id)

    def test_run_target_uses_new_pipeline(self):
        config = make_config(self.root)
        (self.root / "doc.md").write_text("hello")
        with mock.patch("ragflow_sync.cli.RagflowGateway") as gateway_cls:
            gateway = gateway_cls.return_value
            gateway.get_or_create_dataset.return_value = DatasetRef("ds1", config.dataset_name)
            uploaded = RemoteDocumentSnapshot(
                "doc1", remote_name_for("doc.md", "doc.md"), ParseRunStatus.UNSTART, 0.0, 0, 0
            )
            gateway.list_documents.side_effect = [[], [uploaded]]
            gateway.upload_document.return_value = RemoteDocumentSnapshot(
                "doc1", remote_name_for("doc.md", "doc.md"), ParseRunStatus.UNSTART, 0.0, 0, 0
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
