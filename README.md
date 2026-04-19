# RAGFlow Sync

Synchronize multiple local directories to multiple RAGFlow datasets using the
official `ragflow-sdk 0.24.0` contract.

## Design

- One local directory maps to exactly one dataset.
- Multiple targets are supported through `SYNC_TARGETS`.
- Upload completes first, then `async_parse_documents` is triggered.
- The local process never waits for server-side parsing to finish.
- State is newly defined for this project and is not compatible with any
  previous version.

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
- Modified files are uploaded as new remote documents first; old remote
  documents are deleted only after the new upload is visible.
- Deleted local files cause matching managed remote documents to be deleted.
- Managed orphan remote documents are deleted.
- Unmanaged remote documents are kept and logged as warnings.

## State

State files are written under `states/` by default:

- `states/{dataset_slug}.json`
- `states/{dataset_slug}.json.bak`

The state model stores only the data needed for the next sync run:

- dataset binding
- target root
- tracked file metadata
- parse retry counters
- last parse trigger time

## Tests

```bash
source .venv/bin/activate
python -m unittest discover -s tests -v
```
