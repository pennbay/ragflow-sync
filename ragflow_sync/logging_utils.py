from __future__ import annotations

import logging
from logging.handlers import RotatingFileHandler

from .models import ConfigError, SyncTargetConfig


def setup_logger(config: SyncTargetConfig) -> logging.Logger:
    level_name = "WARNING" if config.log_level == "WARN" else config.log_level
    level = getattr(logging, level_name, None)
    if not isinstance(level, int):
        raise ConfigError("LOG_LEVEL must be one of DEBUG, INFO, WARN, ERROR")
    logger = logging.getLogger(f"ragflow_sync.{config.dataset_name}")
    logger.handlers.clear()
    logger.setLevel(level)
    logger.propagate = False
    formatter = logging.Formatter(
        "%(asctime)s %(levelname)s dataset=%(dataset)s %(message)s"
    )
    config.log_dir.mkdir(parents=True, exist_ok=True)
    console = logging.StreamHandler()
    console.setLevel(level)
    console.setFormatter(formatter)
    file_handler = RotatingFileHandler(
        config.log_path,
        maxBytes=10 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)
    logger.addHandler(console)
    logger.addHandler(file_handler)
    return logging.LoggerAdapter(logger, {"dataset": config.dataset_name})  # type: ignore[return-value]
