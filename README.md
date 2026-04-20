# RAGFlow Sync

Synchronize multiple local directories to multiple RAGFlow datasets using the
official `ragflow-sdk 0.24.0` contract.

## Design

- One local directory maps to exactly one dataset.
- Multiple targets are supported through `SYNC_TARGETS`.
- Managed remote document names include both a path fingerprint and the local
  file MD5: `{safe_stem}__rf__{path_sha16}__md5__{md5}{suffix}`.
- RAGFlow limits upload file names to 255 UTF-8 bytes. This tool keeps managed
  remote names within 239 bytes, leaving room for RAGFlow duplicate suffixes
  such as `(1)`.
- Each successful remote side effect is persisted to local state immediately,
  so interrupted runs can resume without uploading the same managed file again.
- The local process never waits for server-side parsing to finish.
- State version 2 is not compatible with previous versions; old state files are
  ignored with a warning.

## Requirements

- Python 3.12+
- A configured `.venv` or environment with `ragflow-sdk>=0.24.0`
- A reachable RAGFlow server

Install dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -r requirements.txt
```

## Configuration

Edit [config.py](/Users/peng/ragflow-sync/config.py).

Required:

- `BASE_URL`
- `SYNC_TARGETS`
- `RAGFLOW_API_KEY` in the environment or `.env`

Each target must contain:

```python
SYNC_TARGETS = [
    {
        "DATASET_NAME": "dataset-a",
        "LOCAL_DIR": "/absolute/path/to/docs-a",
    },
    {
        "DATASET_NAME": "dataset-b",
        "LOCAL_DIR": "/absolute/path/to/docs-b",
    },
]
```

Optional global settings:

- `ALLOWED_EXTENSIONS`
- `IGNORE_DIRS`
- `IGNORE_FILES`
- `MAX_FILE_SIZE_MB`
- `MAX_PARSE_RETRY_TIMES`
- `UPLOAD_BATCH_SIZE`
- `REMOTE_PAGE_SIZE`
- `API_RETRY_TIMES`
- `API_RETRY_INTERVAL_SECONDS`
- `LOG_LEVEL`
- `STATE_DIR`
- `LOG_DIR`

## Run

If this is your first run after upgrading to the v2 state/name format, delete
or recreate the remote dataset contents first. The tool will ignore old local
state automatically, but it will not bulk-delete an entire dataset for safety.

```bash
source .venv/bin/activate
python -m ragflow_sync
```

Run a single target:

```bash
python -m ragflow_sync --target dataset-a
```

Preview only:

```bash
python -m ragflow_sync --dry-run
```

## Behavior

- New files are uploaded and then scheduled for async parse.
- Long local file names are truncated by UTF-8 byte length before upload. The
  visible stem is only for display; `path_sha16` and full 32-character MD5 are
  the stable recovery and de-duplication keys.
- If a previous upload reached RAGFlow but the local process stopped before
  saving state, the next run adopts the existing v2 managed remote document
  instead of uploading it again.
- If an upload call fails after RAGFlow created the document, the executor lists
  remote documents immediately and adopts the matching v2 document before
  retrying.
- Modified files are uploaded as new remote documents first; old remote
  documents are deleted only after the new upload is visible.
- Deleted local files cause matching managed remote documents to be deleted.
- Managed orphan and duplicate v2 remote documents are deleted.
- Unmanaged remote documents are kept and logged as warnings.

## State

State files are written under `states/` by default:

- `states/{dataset_slug}.json`
- `states/{dataset_slug}.json.bak`

The state model stores only the data needed for the next sync run:

- dataset binding
- target root
- tracked file metadata, including relative path, path fingerprint, MD5, size,
  mtime, remote name, and document ID
- parse retry counters
- last upload and parse trigger timestamps

Version 1 state is not migrated. This is intentional because v2 is designed for
freshly rebuilt remote datasets.

## Tests

```bash
source .venv/bin/activate
python -m unittest discover -s tests -v
```
