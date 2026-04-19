from __future__ import annotations

import argparse
import os
from typing import Sequence

from .config import load_config
from .executor import execute_sync_plan
from .logging_utils import setup_logger
from .models import ConfigError, SyncError
from .planner import build_plan
from .scanner import scan_local_files
from .sdk_gateway import RagflowGateway
from .state_store import load_state


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Synchronize local directories to RAGFlow datasets.")
    parser.add_argument("--config-module", default="config")
    parser.add_argument("--target", help="Only run the specified DATASET_NAME")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args(argv)


def run_target(config, dry_run: bool = False) -> int:
    if not config.local_dir.exists():
        raise ConfigError(f"LOCAL_DIR does not exist: {config.local_dir}")
    if not config.local_dir.is_dir():
        raise ConfigError(f"LOCAL_DIR is not a directory: {config.local_dir}")
    if not os.access(config.local_dir, os.R_OK):
        raise ConfigError(f"LOCAL_DIR is not readable: {config.local_dir}")

    logger = setup_logger(config)
    gateway = RagflowGateway(config, logger)
    dataset_ref = gateway.get_or_create_dataset(config.dataset_name)
    state = load_state(config.state_path, config.dataset_name, str(config.local_dir), logger)
    state.dataset_id = dataset_ref.dataset_id
    local_files = scan_local_files(config, state, logger)
    remote_docs = gateway.list_documents(dataset_ref.dataset_id)
    plan = build_plan(local_files, remote_docs, state, config)
    return execute_sync_plan(gateway, dataset_ref, config, state, plan, logger, dry_run=dry_run)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        targets = load_config(args.config_module)
        if args.target:
            targets = [target for target in targets if target.dataset_name == args.target]
            if not targets:
                raise ConfigError(f"Target not found: {args.target}")
        exit_code = 0
        for target in targets:
            try:
                result = run_target(target, dry_run=args.dry_run)
            except SyncError:
                exit_code = 2
                continue
            if result != 0:
                exit_code = 2
        return exit_code
    except ConfigError as exc:
        print(f"ERROR: {exc}")
        return 1
    except KeyboardInterrupt:
        print("ERROR: interrupted by user")
        return 130
