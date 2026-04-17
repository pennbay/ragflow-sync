"""User configuration for ragflow_sync.py.

Keep real secrets out of this file. RAGFLOW_API_KEY from the environment has
priority over API_KEY below.
"""

API_KEY = ""
BASE_URL = "http://127.0.0.1:9380"
DATASET_NAME = ""

LOCAL_SYNC_DIRS = [
    # "/absolute/path/to/docs",
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

SYNC_STATE_FILE = "./ragflow_sync_state.json"
LOG_FILE_PATH = "./ragflow_sync.log"
LOG_LEVEL = "INFO"

UPLOAD_BATCH_SIZE = 20
REMOTE_PAGE_SIZE = 100
API_RETRY_TIMES = 3
API_RETRY_INTERVAL_SECONDS = 2
