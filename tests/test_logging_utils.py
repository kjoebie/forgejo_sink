import logging
import os
from pathlib import Path

import pytest

from modules import logging_utils
from modules.constants import CLUSTER_FILES_ROOT


@pytest.fixture(autouse=True)
def reset_logging_state(monkeypatch):
    logging_utils._resolve_log_directory.cache_clear()
    for attr in ["_configured", "_log_file"]:
        if hasattr(logging_utils.configure_logging, attr):
            delattr(logging_utils.configure_logging, attr)

    root_logger = logging.getLogger()
    existing_handlers = list(root_logger.handlers)
    yield existing_handlers

    logging_utils._resolve_log_directory.cache_clear()
    for attr in ["_configured", "_log_file"]:
        if hasattr(logging_utils.configure_logging, attr):
            delattr(logging_utils.configure_logging, attr)

    for handler in [h for h in root_logger.handlers if h not in existing_handlers]:
        root_logger.removeHandler(handler)


def test_resolve_log_directory_prefers_env_override(monkeypatch, tmp_path):
    env_dir = tmp_path / "override"
    monkeypatch.setenv("NOTEBOOK_LOG_ROOT", str(env_dir))
    result = logging_utils._resolve_log_directory()
    assert result == env_dir / "notebook_outputs" / "logs"


@pytest.mark.parametrize(
    "fabric_exists, data_exists, expected_suffix",
    [
        (True, False, Path("/lakehouse/default/Files/notebook_outputs/logs")),
        (False, True, Path(CLUSTER_FILES_ROOT) / "notebook_outputs" / "logs"),
        (False, False, Path("notebook_outputs/logs")),
    ],
)
def test_resolve_log_directory_prefers_fabric_then_cluster(monkeypatch, fabric_exists, data_exists, expected_suffix):
    monkeypatch.delenv("NOTEBOOK_LOG_ROOT", raising=False)

    def fake_exists(path, original=os.path.exists):
        if path == "/lakehouse/default":
            return fabric_exists
        if path == "/data/lakehouse":
            return data_exists
        return original(path)

    monkeypatch.setattr(os.path, "exists", fake_exists)
    logging_utils._resolve_log_directory.cache_clear()

    assert logging_utils._resolve_log_directory() == expected_suffix


def test_configure_logging_is_idempotent(tmp_path, monkeypatch):
    monkeypatch.setenv("NOTEBOOK_LOG_ROOT", str(tmp_path))
    first = logging_utils.configure_logging(run_name="demo", enable_console_logging=False)
    second = logging_utils.configure_logging(run_name="demo", enable_console_logging=False)
    assert first == second


def test_configure_logging_adds_console_on_second_call(tmp_path, monkeypatch, reset_logging_state):
    existing_handlers = reset_logging_state
    monkeypatch.setenv("NOTEBOOK_LOG_ROOT", str(tmp_path))
    logging_utils.configure_logging(run_name="demo", enable_console_logging=False)
    root_logger = logging.getLogger()
    initial_streams = sum(
        1
        for h in existing_handlers
        if isinstance(h, logging.StreamHandler) and not isinstance(h, logging.FileHandler)
    )
    current_streams = sum(
        1
        for h in root_logger.handlers
        if isinstance(h, logging.StreamHandler) and not isinstance(h, logging.FileHandler)
    )
    has_console_before = logging_utils._has_console_handler(root_logger)
    assert current_streams == initial_streams

    logging_utils.configure_logging(run_name="demo", enable_console_logging=True)
    new_streams = sum(
        1
        for h in root_logger.handlers
        if isinstance(h, logging.StreamHandler) and not isinstance(h, logging.FileHandler)
    )
    assert new_streams >= current_streams
    assert logging_utils._has_console_handler(root_logger)
    if not has_console_before:
        assert new_streams == current_streams + 1


def test_configure_logging_creates_file_under_resolved_directory(tmp_path, monkeypatch):
    monkeypatch.setenv("NOTEBOOK_LOG_ROOT", str(tmp_path))
    log_file = logging_utils.configure_logging(run_name="sample")
    assert log_file.parent == tmp_path / "notebook_outputs" / "logs"
    assert log_file.name.startswith("sample_")