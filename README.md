# RAGFlow Local Directory Sync

Synchronize multiple local directories to one RAGFlow dataset with strict file
type filtering, idempotent uploads, remote cleanup, and asynchronous parse
triggering.

## Requirements

- Python 3.12+
- RAGFlow v0.15.0+ server reachable from this machine
- Official Python SDK from `requirements.txt`

The project uses the latest `ragflow-sdk` line, which currently requires
Python 3.12+. Install dependencies:

```bash
python3.12 -m pip install -r requirements.txt
```

## Configuration

Edit `config.py` only. Do not put real API keys in Git.

Required settings:

- `DATASET_NAME`: target RAGFlow dataset name. It is created if missing.
- `LOCAL_SYNC_DIRS`: absolute paths to local directories.
- `BASE_URL`: RAGFlow server URL.
- `RAGFLOW_API_KEY`: preferred API key source via environment variable.

Example:

```bash
export RAGFLOW_API_KEY="ragflow-..."
python3.12 ragflow_sync.py
```

`API_KEY` in `config.py` is only a fallback. `RAGFLOW_API_KEY` always wins.

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

Default files:

- `ragflow_sync_state.json`
- `ragflow_sync_state.json.bak`
- `ragflow_sync.log`

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
- `Missing required configuration`: set `DATASET_NAME`, `LOCAL_SYNC_DIRS`, and `RAGFLOW_API_KEY`.
- `Remote consistency check failed`: at least one local file did not have a verified remote document after upload; inspect the error log and rerun after fixing the cause.
- `Parse retry limit reached`: the remote document has failed parsing too many times. Inspect it in RAGFlow web UI.

## Tests

Unit tests use `unittest` and mocks only:

```bash
python3 -m unittest discover -s tests -v
```

End-to-end verification requires a real RAGFlow instance:

1. Configure `config.py`.
2. Export `RAGFLOW_API_KEY`.
3. Add test files to the configured local directory.
4. Run `python3.12 ragflow_sync.py`.
5. Confirm upload/delete happened first and parse was triggered only after consistency verification.
