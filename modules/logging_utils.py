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
    enable_console_logging: bool = True,
) -> Path:
    """Configure root logging with a rotating file handler.

    Args:
        run_name: Optional name prefix for the log file.
        max_bytes: Maximum log file size before rotation occurs.
        backup_count: Number of rotated log files to keep.

    Returns:
        Path: Full path to the log file for the current run.
    """

    formatter = logging.Formatter("%(asctime)s [%(levelname)s] - %(message)s")

    if getattr(configure_logging, "_configured", False):
        # Add console logging on demand if it was disabled initially
        root_logger = logging.getLogger()
        if enable_console_logging and not _has_console_handler(root_logger):
            _add_console_handler(root_logger, formatter)

        return configure_logging._log_file  # type: ignore[attr-defined]

    log_dir = _resolve_log_directory()
    log_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = log_dir / f"{run_name or 'notebook_run'}_{timestamp}.log"

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    _ensure_file_handler(root_logger, log_file, formatter, max_bytes, backup_count)

    if enable_console_logging and not _has_console_handler(root_logger):
        _add_console_handler(root_logger, formatter)

    configure_logging._configured = True  # type: ignore[attr-defined]
    configure_logging._log_file = log_file  # type: ignore[attr-defined]

    return log_file


def _has_console_handler(root_logger: logging.Logger) -> bool:
    return any(
        isinstance(handler, logging.StreamHandler)
        and not isinstance(handler, RotatingFileHandler)
        for handler in root_logger.handlers
    )


def _add_console_handler(root_logger: logging.Logger, formatter: logging.Formatter) -> None:
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)


def _ensure_file_handler(
    root_logger: logging.Logger,
    log_file: Path,
    formatter: logging.Formatter,
    max_bytes: int,
    backup_count: int,
) -> None:
    if any(isinstance(h, RotatingFileHandler) for h in root_logger.handlers):
        return

    file_handler = RotatingFileHandler(
        log_file, maxBytes=max_bytes, backupCount=backup_count, encoding="utf-8"
    )
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)
