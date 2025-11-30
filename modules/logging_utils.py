"""Logging utilities with rotating file handler support."""

import logging
import os
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Optional


DEFAULT_CLUSTER_FILES_ROOT = "/data/lakehouse/gh_b_avd/lh_gh_bronze/Files"


def _resolve_log_directory() -> Path:
    """Determine the log directory based on the runtime environment."""

    # Prefer explicit environment override
    env_override = os.getenv("NOTEBOOK_LOG_ROOT")
    if env_override:
        return Path(env_override) / "notebook_outputs" / "logs"

    # Fabric notebooks
    if os.path.exists("/lakehouse/default"):
        return Path("/lakehouse/default/Files/notebook_outputs/logs")

    # Cluster (OneLake mounted under /data)
    if os.path.exists("/data/lakehouse"):
        return Path(DEFAULT_CLUSTER_FILES_ROOT) / "notebook_outputs" / "logs"

    # Local execution
    return Path("notebook_outputs/logs")


def configure_logging(
    run_name: Optional[str] = None,
    max_bytes: int = 5 * 1024 * 1024,
    backup_count: int = 5,
) -> Path:
    """Configure root logging with a rotating file handler.

    Args:
        run_name: Optional name prefix for the log file.
        max_bytes: Maximum log file size before rotation occurs.
        backup_count: Number of rotated log files to keep.

    Returns:
        Path: Full path to the log file for the current run.
    """

    if getattr(configure_logging, "_configured", False):
        return configure_logging._log_file  # type: ignore[attr-defined]

    log_dir = _resolve_log_directory()
    log_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = log_dir / f"{run_name or 'notebook_run'}_{timestamp}.log"

    formatter = logging.Formatter("%(asctime)s [%(levelname)s] - %(message)s")

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    # Ensure a single rotating file handler is present
    if not any(isinstance(h, RotatingFileHandler) for h in root_logger.handlers):
        file_handler = RotatingFileHandler(
            log_file, maxBytes=max_bytes, backupCount=backup_count, encoding="utf-8"
        )
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)

    # Ensure console output remains available
    if not any(isinstance(h, logging.StreamHandler) for h in root_logger.handlers):
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        root_logger.addHandler(console_handler)

    configure_logging._configured = True  # type: ignore[attr-defined]
    configure_logging._log_file = log_file  # type: ignore[attr-defined]

    return log_file

