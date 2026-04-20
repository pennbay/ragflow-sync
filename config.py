"""Project configuration for `python -m ragflow_sync`.

Set `RAGFLOW_API_KEY` in the environment or `.env`.
Each sync target maps exactly one local directory to exactly one dataset.
"""

BASE_URL = "https://ragapi.556554.xyz"

SYNC_TARGETS = [
    # {
    #     "DATASET_NAME": "TEST",
    #     "LOCAL_DIR": "/Users/peng/Nutstore Files/TEST",
    # },
    {
        "DATASET_NAME": "Obsidian-Peng",
        "LOCAL_DIR": "/Users/peng/Nutstore Files/Obsidian-Peng",
    },
    {
        "DATASET_NAME": "RAG Documents",
        "LOCAL_DIR": "/Users/peng/Nutstore Files/RAG Documents",
    },
    # {
    #     "DATASET_NAME": "My Documents",
    #     "LOCAL_DIR": "/Users/peng/Nutstore Files/My Documents",
    # },
    # {
    #     "DATASET_NAME": "Zotero",
    #     "LOCAL_DIR": "/Users/peng/Zotero/storage",
    # },
]

ALLOWED_EXTENSIONS = [
    ".pdf",
    ".docx",
    ".pptx",
    ".epub",
    ".md",
    ".mdx",
]

IGNORE_DIRS = [".git", ".venv", "__pycache__", ".idea", ".DS_Store"]
IGNORE_FILES = ["Thumbs.db", ".gitignore"]

MAX_FILE_SIZE_MB = 64
MAX_PARSE_RETRY_TIMES = 3
REMOTE_PAGE_SIZE = 64
UPLOAD_TIMEOUT_SECONDS = 180
UPLOAD_RETRY_TIMES = 1
UPLOAD_RETRY_INTERVAL_SECONDS = 3
API_TIMEOUT_SECONDS = 30
API_RETRY_TIMES = 2
API_RETRY_INTERVAL_SECONDS = 2
LOG_LEVEL = "INFO"
STATE_DIR = "states"
LOG_DIR = "logs"
