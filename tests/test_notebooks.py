import json
import shutil
from pathlib import Path

import pytest


def _copy_fixtures(tmp_path: Path) -> Path:
    source = Path(__file__).parent / "data" / "Files"
    target = tmp_path / "Files"
    shutil.copytree(source, target)
    return target


def _assert_notebook_result(result_json: str, output_root: Path) -> Path:
    nbformat = pytest.importorskip("nbformat")
    result = json.loads(result_json)
    assert result["status"] == "success"

    output_path = Path(result["output_notebook"])
    assert output_path.is_file()
    assert output_root in output_path.parents

    executed = nbformat.read(output_path, as_version=4)
    assert executed.metadata.get("papermill", {}).get("status") == "completed"
    return output_path


@pytest.mark.notebook
@pytest.mark.parametrize(
    "notebook_name, base_params",
    [
        (
            "00_master_orchestrator.ipynb",
            {
                "source": "demo_source",
                "run_ts": "20240101T000000",
                "dag_path": "config/dag_notebook_demo.json",
                "retry_tables": [],
                "force_reload": False,
            },
        ),
        (
            "02_utils_config.ipynb",
            {"config_base_path": "{BASE_PATH}"},
        ),
        (
            "10_bronze_load.ipynb",
            {
                "RUN_ID": "unit-test",
                "BASE_PATH": "{BASE_PATH}",
                "run_ts": "20240101T000000",
                "run_date": "2024-01-01",
                "base_files": "greenhouse_sources",
                "source_name": "demo_source",
            },
        ),
        (
            "11_bronze_watermark_merge.ipynb",
            {
                "BASE_PATH": "{BASE_PATH}",
                "watermarks_path": "config/watermarks.json",
                "wm_folder": "runtime/demo_source/20240101T000000",
            },
        ),
        (
            "20_silver_cdc_merge.ipynb",
            {
                "BASE_PATH": "{BASE_PATH}",
                "source": "demo_source",
                "run_ts": "20240101T000000",
                "target_table": "bronze.demo_table",
                "watermark_key": "demo",
            },
        ),
        (
            "99_bronze_cleanup.ipynb",
            {
                "BASE_PATH": "{BASE_PATH}",
                "target_table": "bronze.demo_table",
                "retention_days": 1,
                "vacuum": False,
            },
        ),
    ],
)
def test_notebooks_execute_with_local_inputs(notebook_name, base_params, tmp_path, monkeypatch):
    papermill = pytest.importorskip("papermill")  # noqa: F841 - imported for side effect
    from modules.notebook_utils import NotebookRunner
    base_path = _copy_fixtures(tmp_path)

    # Inject a symlink so notebooks using relative "Files" still resolve to fixtures
    files_symlink = Path("Files")
    created_symlink = False
    if not files_symlink.exists():
        files_symlink.symlink_to(base_path)
        created_symlink = True

    def _cleanup_symlink():
        if created_symlink and files_symlink.exists():
            files_symlink.unlink()

    monkeypatch.addfinalizer(_cleanup_symlink)

    params = {k: (v.format(BASE_PATH=base_path) if isinstance(v, str) else v) for k, v in base_params.items()}
    params.setdefault("BASE_PATH", str(base_path))

    result_json = NotebookRunner.run(
        notebook_path=notebook_name,
        arguments=params,
        output_dir=tmp_path,
    )

    output_nb = _assert_notebook_result(result_json, tmp_path)

    # Ensure all referenced storage paths are confined to the temporary base
    assert str(base_path) in output_nb.read_text()

    parquet_files = list(base_path.glob("**/*.parquet"))
    delta_logs = list(base_path.glob("**/_delta_log/*.json"))
    assert parquet_files, "Parquet fixture files should be discoverable in the temp base path"
    assert delta_logs, "Delta log fixture files should be discoverable in the temp base path"