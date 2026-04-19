"""Project configuration for `python -m ragflow_sync`.

Set `RAGFLOW_API_KEY` in the environment or `.env`.
Each sync target maps exactly one local directory to exactly one dataset.
"""

BASE_URL = "https://ragapi.556554.xyz"

SYNC_TARGETS = [
    {
        "DATASET_NAME": "TEST",
        "LOCAL_DIR": "/Users/peng/Nutstore Files/TEST",
    },
    {
        "DATASET_NAME": "Obsidian-Peng",
        "LOCAL_DIR": "/Users/peng/Nutstore Files/Obsidian-Peng",
    },
]

ALLOWED_EXTENSIONS = [
    ".pdf",
    ".doc",
    ".docx",
    ".ppt",
    ".pptx",
    ".md",
    ".markdown",
]

IGNORE_DIRS = [".git", ".venv", "__pycache__", ".idea", ".DS_Store"]
IGNORE_FILES = ["Thumbs.db", ".gitignore"]

MAX_FILE_SIZE_MB = 128
MAX_PARSE_RETRY_TIMES = 3
UPLOAD_BATCH_SIZE = 20
REMOTE_PAGE_SIZE = 64
API_RETRY_TIMES = 3
API_RETRY_INTERVAL_SECONDS = 2
LOG_LEVEL = "INFO"
STATE_DIR = "states"
LOG_DIR = "logs"
