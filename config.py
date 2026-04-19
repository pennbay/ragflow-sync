"""User configuration for ragflow_sync.py.

Keep real secrets out of this file. RAGFLOW_API_KEY from the environment has
priority over API_KEY below.
"""

BASE_URL = "https://ragapi.556554.xyz"

# Each target syncs one or more local directories into exactly one RAGFlow
# dataset. State and log paths are generated automatically from DATASET_NAME.
SYNC_TARGETS = [
    {
        "DATASET_NAME": "TEST",
        "LOCAL_SYNC_DIRS": ["/Users/peng/Nutstore Files/TEST"],
    },
    {
        "DATASET_NAME": "dataset-b",
        "LOCAL_SYNC_DIRS": ["/absolute/path/to/docs-b"],
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
MAX_FILE_SIZE_MB = 100

MAX_PARSE_RETRY_TIMES = 3

LOG_LEVEL = "INFO"

UPLOAD_BATCH_SIZE = 20
REMOTE_PAGE_SIZE = 100
API_RETRY_TIMES = 3
API_RETRY_INTERVAL_SECONDS = 2
