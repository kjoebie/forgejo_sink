import json
from pathlib import Path

import pytest

try:  # pragma: no cover - import guard for optional dependency
    from modules import notebook_utils
except ImportError:  # pragma: no cover - skip when papermill is unavailable
    pytest.skip("papermill is required to import notebook_utils", allow_module_level=True)


@pytest.fixture
def dummy_notebook(tmp_path):
    nb_path = tmp_path / "sample.ipynb"
    nb_content = {
        "cells": [],
        "metadata": {},
        "nbformat": 4,
        "nbformat_minor": 5,
    }
    nb_path.write_text(json.dumps(nb_content))
    return nb_path


def test_run_adds_ipynb_extension_for_absolute_paths(dummy_notebook, monkeypatch):
    output_dir = dummy_notebook.parent / "outputs"
    def fake_execute(input_nb, output_nb, **_):
        Path(output_nb).write_text(Path(input_nb).read_text())

    monkeypatch.setattr(notebook_utils.pm, "execute_notebook", fake_execute)

    result_json = notebook_utils.NotebookRunner.run(str(dummy_notebook.with_suffix("")), output_dir=output_dir)
    result = json.loads(result_json)
    assert Path(result["output_notebook"]).is_file()


def test_run_raises_for_missing_notebook(tmp_path):
    missing = tmp_path / "does_not_exist.ipynb"
    with pytest.raises(FileNotFoundError):
        notebook_utils.NotebookRunner.run(str(missing))


def test_run_writes_into_output_directory(dummy_notebook, tmp_path, monkeypatch):
    created_outputs = tmp_path / "out"

    def fake_execute(input_nb, output_nb, **_):
        Path(output_nb).write_text(Path(input_nb).read_text())

    monkeypatch.setattr(notebook_utils.pm, "execute_notebook", fake_execute)

    result_json = notebook_utils.NotebookRunner.run(
        notebook_path=str(dummy_notebook),
        arguments={"param": "value"},
        output_dir=created_outputs,
    )

    result = json.loads(result_json)
    assert created_outputs.exists()
    assert Path(result["output_notebook"]).parent == created_outputs.resolve()
