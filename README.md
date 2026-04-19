# RAGFlow Local Directory Sync

Synchronize multiple local directory groups to multiple RAGFlow datasets with
strict file type filtering, idempotent uploads, remote cleanup, and asynchronous
parse triggering.

## Requirements

- Python 3.12+
- RAGFlow v0.15.0+ server reachable from this machine
- Official Python SDK from `requirements.txt`

The project uses the latest `ragflow-sdk` line, which currently requires
Python 3.12+. Install dependencies:

```bash
python3 -m pip install -r requirements.txt
```

## Configuration

Edit `config.py` only. Do not put real API keys in Git.

Required settings:

- `SYNC_TARGETS`: non-empty list of dataset sync targets.
- `BASE_URL`: RAGFlow server URL.
- `RAGFLOW_API_KEY`: preferred API key source via environment variable.

Each target defines one dataset and one or more local directories. State and
log files are generated automatically from `DATASET_NAME`; do not configure
them manually.

```python
SYNC_TARGETS = [
    {
        "DATASET_NAME": "dataset-a",
        "LOCAL_SYNC_DIRS": ["/absolute/path/to/docs-a"],
    },
    {
        "DATASET_NAME": "dataset-b",
        "LOCAL_SYNC_DIRS": [
            "/absolute/path/to/docs-b-1",
            "/absolute/path/to/docs-b-2",
        ],
    },
]
```

One target can include multiple local directories, and all directories in that
target sync to the same dataset.

Generated paths:

- `states/{safe_dataset_name}.json`
- `logs/{safe_dataset_name}.log`

`safe_dataset_name` keeps ASCII letters, digits, `.`, `_`, and `-`; other
characters become `_`. If two dataset names generate the same safe name, startup
fails to protect state consistency.

Example `.env`:

```bash
RAGFLOW_API_KEY="ragflow-..."
```

Run:

```bash
python3.12 ragflow_sync.py
```

Only `RAGFLOW_API_KEY` is used for the API key. The program loads `.env`
automatically without overriding an already exported shell variable.

## Behavior

- Only `.pdf`, `.doc`, `.docx`, `.ppt`, `.pptx`, `.md`, and `.markdown` files are synced by default.
- Extra extensions may be added in `ALLOWED_EXTENSIONS`, but the defaults are always preserved.
- Local files are never modified, moved, renamed, or deleted.
- Remote documents with non-whitelisted extensions are deleted.
- Upload and parse are strictly decoupled. The upload/delete phase never calls parse APIs.
- Parse triggering happens only after upload completion and remote consistency verification.
- Parsing is asynchronous through `async_parse_documents`; the program does not wait for parse completion.
- Parser settings are not changed by this tool. They follow the existing RAGFlow web UI dataset/document configuration.

Remote document names include a short hash of the normalized absolute local
path, so files with the same filename in different directories do not collide.

## State And Logs

State and log paths are generated from each target's `DATASET_NAME`.

The state file stores absolute paths, content MD5, file stat data, remote
document IDs, parse trigger timestamps, and retry counts. It is backed up before
updates and written atomically.

MD5 reuse is safe for old-file replacement: a previous hash is reused only when
both `mtime_ns` and file size are exactly unchanged. If a file is replaced by an
older copy and its mtime becomes earlier than the recorded value, the MD5 is
recomputed and the current local file remains the sync source of truth.

## Troubleshooting

- `Python 3.12+ is required`: run with `python3.12`, not the system `python3`.
- `ragflow-sdk is not installed`: run `python3.12 -m pip install -r requirements.txt`.
- `Missing required configuration`: set `SYNC_TARGETS`, `BASE_URL`, and `RAGFLOW_API_KEY`.
- `SYNC_TARGETS must be a non-empty list`: add at least one target in `config.py`.
- `SYNC_TARGETS[n] missing required keys`: every target needs `DATASET_NAME` and `LOCAL_SYNC_DIRS`.
- `SYNC_TARGETS[n] must not configure`: remove `SYNC_STATE_FILE` or `LOG_FILE_PATH`; they are generated automatically.
- `Remote consistency check failed`: at least one local file did not have a verified remote document after upload; inspect the error log and rerun after fixing the cause.
- `Parse retry limit reached`: the remote document has failed parsing too many times. Inspect it in RAGFlow web UI.

## Tests

Unit tests use `unittest` and mocks only:

```bash
python3 -m unittest discover -s tests -v
```

End-to-end verification requires a real RAGFlow instance:

1. Configure `config.py` with `SYNC_TARGETS`.
2. Export `RAGFLOW_API_KEY`.
3. Add test files to the configured local directory.
4. Run `python3.12 ragflow_sync.py`.
5. Confirm upload/delete happened first and parse was triggered only after consistency verification.
