import logging
import os
import tempfile
import types
import unittest
from pathlib import Path
from unittest import mock

import ragflow_sync as sync


def test_config(tmp_path: Path) -> sync.AppConfig:
    return sync.AppConfig(
        api_key="key",
        base_url="http://example.test",
        dataset_name="dataset",
        local_sync_dirs=[tmp_path],
        allowed_extensions=set(sync.DEFAULT_ALLOWED_EXTENSIONS),
        ignore_dirs={".git"},
        ignore_files={"Thumbs.db"},
        max_file_size_mb=100,
        max_parse_retry_times=3,
        sync_state_file=tmp_path / "state.json",
        log_file_path=tmp_path / "sync.log",
        log_level="ERROR",
        upload_batch_size=2,
        remote_page_size=100,
        api_retry_times=1,
        api_retry_interval_seconds=0,
    )


class FakeAdapter:
    def __init__(self, config, logger):
        self.config = config
        self.logger = logger
        self.dataset_id = "ds1"
        self.remote_docs = []
        self.deleted = []
        self.uploaded = []
        self.parsed = []

    def ensure_dataset(self, dataset_name):
        return object(), self.dataset_id

    def list_documents(self):
        return list(self.remote_docs)

    def delete_documents(self, document_ids):
        self.deleted.extend(document_ids)
        self.remote_docs = [doc for doc in self.remote_docs if doc.document_id not in set(document_ids)]

    def upload_document(self, local_file):
        document_id = f"doc{len(self.uploaded) + 1}"
        self.uploaded.append(local_file.abs_path)
        self.remote_docs.append(
            sync.RemoteDocument(
                document_id=document_id,
                name=local_file.remote_display_name,
                extension=local_file.extension,
                status="INIT",
            )
        )
        return document_id

    def async_parse_documents(self, document_ids):
        self.parsed.extend(document_ids)


def make_config_module(root: Path, targets):
    module = types.ModuleType(f"temp_sync_config_{id(root)}")
    module.BASE_URL = "http://example.test"
    module.SYNC_TARGETS = targets
    module.ALLOWED_EXTENSIONS = [".txt"]
    module.IGNORE_DIRS = []
    module.IGNORE_FILES = []
    module.MAX_FILE_SIZE_MB = 1
    module.MAX_PARSE_RETRY_TIMES = 3
    module.LOG_LEVEL = "INFO"
    module.UPLOAD_BATCH_SIZE = 1
    module.REMOTE_PAGE_SIZE = 100
    module.API_RETRY_TIMES = 1
    module.API_RETRY_INTERVAL_SECONDS = 0
    return module


class RagflowSyncTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.logger = logging.getLogger(f"test-{id(self)}")
        self.logger.addHandler(logging.NullHandler())

    def tearDown(self):
        self.tmp.cleanup()

    def test_scan_filters_and_accepts_case_insensitive_extensions(self):
        (self.root / "a.PDF").write_bytes(b"pdf")
        (self.root / "b.txt").write_text("no")
        (self.root / "c.pdf.txt").write_text("no")
        (self.root / ".git").mkdir()
        (self.root / ".git" / "d.pdf").write_text("no")
        config = test_config(self.root)

        files = sync.scan_local_files(config, sync.empty_state("dataset"), self.logger)

        self.assertEqual(1, len(files))
        only = next(iter(files.values()))
        self.assertEqual(".pdf", only.extension)
        self.assertTrue(only.remote_display_name.endswith(".pdf"))

    def test_scan_recomputes_md5_when_mtime_is_older_than_history(self):
        path = self.root / "doc.md"
        path.write_text("new local truth")
        old_time = 1_000_000_000
        os.utime(path, ns=(old_time, old_time))
        abs_path = sync.normalize_abs_path(path)
        state = sync.empty_state("dataset")
        state["files"][abs_path] = {
            "md5": "old-md5",
            "mtime_ns": old_time + 10_000,
            "size": path.stat().st_size,
        }
        config = test_config(self.root)

        files = sync.scan_local_files(config, state, self.logger)

        self.assertIn(abs_path, files)
        self.assertNotEqual("old-md5", files[abs_path].md5)
        self.assertEqual(sync.file_md5(path), files[abs_path].md5)

    def test_decision_treats_changed_md5_with_older_mtime_as_modified(self):
        path = self.root / "doc.md"
        path.write_text("old replacement")
        local = sync.LocalFile(
            abs_path=sync.normalize_abs_path(path),
            path=path,
            remote_display_name=sync.make_remote_display_name(sync.normalize_abs_path(path), path.name),
            md5=sync.file_md5(path),
            mtime_ns=1,
            size=path.stat().st_size,
            extension=".md",
        )
        state = sync.empty_state("dataset")
        state["files"][local.abs_path] = {
            "document_id": "remote1",
            "remote_display_name": local.remote_display_name,
            "md5": "different-old-md5",
            "mtime_ns": 999,
            "size": local.size,
            "parse_retry_count": 0,
        }
        remote = [
            sync.RemoteDocument(
                document_id="remote1",
                name=local.remote_display_name,
                extension=".md",
                status="DONE",
            )
        ]
        config = test_config(self.root)

        decision = sync.decide_sync({local.abs_path: local}, state, remote, config, self.logger)

        self.assertEqual(1, len(decision.upload_tasks))
        self.assertEqual("modified", decision.upload_tasks[0].reason)
        self.assertEqual("remote1", decision.upload_tasks[0].old_document_id)

    def test_decision_parse_rules(self):
        config = test_config(self.root)
        state = sync.empty_state("dataset")
        local_files = {}
        remote_docs = []
        for status in ["INIT", "DONE", "RUNNING", "FAIL"]:
            path = self.root / f"{status}.md"
            path.write_text(status)
            abs_path = sync.normalize_abs_path(path)
            local = sync.LocalFile(
                abs_path=abs_path,
                path=path,
                remote_display_name=sync.make_remote_display_name(abs_path, path.name),
                md5=sync.file_md5(path),
                mtime_ns=path.stat().st_mtime_ns,
                size=path.stat().st_size,
                extension=".md",
            )
            local_files[abs_path] = local
            doc_id = f"doc-{status}"
            state["files"][abs_path] = {
                "document_id": doc_id,
                "remote_display_name": local.remote_display_name,
                "md5": local.md5,
                "mtime_ns": local.mtime_ns,
                "size": local.size,
                "parse_retry_count": 0,
            }
            remote_docs.append(sync.RemoteDocument(doc_id, local.remote_display_name, ".md", status))

        decision = sync.decide_sync(local_files, state, remote_docs, config, self.logger)

        self.assertIn("doc-INIT", decision.parse_document_ids)
        self.assertIn("doc-FAIL", decision.parse_document_ids)
        self.assertNotIn("doc-DONE", decision.parse_document_ids)
        self.assertNotIn("doc-RUNNING", decision.parse_document_ids)

    def test_execute_order_upload_then_parse_gate(self):
        config = test_config(self.root)
        path = self.root / "doc.md"
        path.write_text("hello")
        state = sync.empty_state("dataset")
        local_files = sync.scan_local_files(config, state, self.logger)
        local = next(iter(local_files.values()))
        decision = sync.Decision(upload_tasks=[sync.UploadTask(local)])
        adapter = FakeAdapter(config, self.logger)

        uploaded = sync.execute_uploads(adapter, decision, state, config.sync_state_file, config, self.logger)
        self.assertEqual([], adapter.parsed)
        self.assertEqual(["doc1"], uploaded)
        self.assertTrue(sync.verify_remote_consistency(adapter, local_files, state, self.logger))
        final = sync.final_parse_candidates(adapter, uploaded, self.logger)
        sync.trigger_parse(adapter, final, state, config.sync_state_file, self.logger)

        self.assertEqual(["doc1"], adapter.parsed)
        self.assertEqual("RUNNING", state["files"][local.abs_path]["parse_status"])

    def test_load_config_env_api_key_priority_and_extension_defaults(self):
        module = make_config_module(
            self.root,
            [
                {
                    "DATASET_NAME": "dataset",
                    "LOCAL_SYNC_DIRS": [str(self.root)],
                    "SYNC_STATE_FILE": str(self.root / "state.json"),
                    "LOG_FILE_PATH": str(self.root / "sync.log"),
                }
            ],
        )

        with mock.patch.dict("sys.modules", {"temp_sync_config": module}), mock.patch.dict(
            os.environ, {"RAGFLOW_API_KEY": "env-key"}
        ):
            config = sync.load_config("temp_sync_config")

        self.assertEqual("env-key", config.api_key)
        self.assertIn(".pdf", config.allowed_extensions)
        self.assertIn(".txt", config.allowed_extensions)

    def test_load_configs_requires_non_empty_sync_targets(self):
        module = make_config_module(self.root, [])

        with mock.patch.dict("sys.modules", {"empty_targets_config": module}), mock.patch.dict(
            os.environ, {"RAGFLOW_API_KEY": "env-key"}
        ):
            with self.assertRaises(sync.ConfigError):
                sync.load_configs("empty_targets_config")

    def test_load_configs_requires_target_fields(self):
        module = make_config_module(
            self.root,
            [
                {
                    "DATASET_NAME": "dataset",
                    "LOCAL_SYNC_DIRS": [str(self.root)],
                    "SYNC_STATE_FILE": str(self.root / "state.json"),
                }
            ],
        )

        with mock.patch.dict("sys.modules", {"missing_target_config": module}), mock.patch.dict(
            os.environ, {"RAGFLOW_API_KEY": "env-key"}
        ):
            with self.assertRaisesRegex(sync.ConfigError, "LOG_FILE_PATH"):
                sync.load_configs("missing_target_config")

    def test_load_configs_builds_independent_targets(self):
        one = self.root / "one"
        two = self.root / "two"
        one.mkdir()
        two.mkdir()
        module = make_config_module(
            self.root,
            [
                {
                    "DATASET_NAME": "dataset-one",
                    "LOCAL_SYNC_DIRS": [str(one)],
                    "SYNC_STATE_FILE": str(self.root / "state-one.json"),
                    "LOG_FILE_PATH": str(self.root / "one.log"),
                },
                {
                    "DATASET_NAME": "dataset-two",
                    "LOCAL_SYNC_DIRS": [str(two)],
                    "SYNC_STATE_FILE": str(self.root / "state-two.json"),
                    "LOG_FILE_PATH": str(self.root / "two.log"),
                },
            ],
        )

        with mock.patch.dict("sys.modules", {"two_target_config": module}), mock.patch.dict(
            os.environ, {"RAGFLOW_API_KEY": "env-key"}
        ):
            configs = sync.load_configs("two_target_config")

        self.assertEqual(["dataset-one", "dataset-two"], [config.dataset_name for config in configs])
        self.assertNotEqual(configs[0].sync_state_file, configs[1].sync_state_file)
        self.assertNotEqual(configs[0].log_file_path, configs[1].log_file_path)

    def test_run_all_continues_after_target_failure_and_returns_partial_failure(self):
        configs = [
            test_config(self.root / "missing"),
            test_config(self.root),
        ]
        configs[0].dataset_name = "bad"
        configs[1].dataset_name = "good"
        called = []

        def fake_run_sync(config, adapter_factory=sync.RagflowClientAdapter):
            called.append(config.dataset_name)
            return 2 if config.dataset_name == "bad" else 0

        with mock.patch.object(sync, "run_sync", side_effect=fake_run_sync):
            code = sync.run_all(configs)

        self.assertEqual(2, code)
        self.assertEqual(["bad", "good"], called)


if __name__ == "__main__":
    unittest.main()
