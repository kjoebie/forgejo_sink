import json
import shutil
from pathlib import Path

import pytest


def _copy_fixtures(tmp_path: Path) -> Path:
    data_root = Path(__file__).parent / "data"
    source = data_root / "Files"
    if not source.exists():
        source = data_root / "files"
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
            "00_master_orchestrator.ipynb",
            {
                "source": "demo_source",
                "run_ts": "20240102T000000",
                "dag_path": "config/dag_notebook_demo.json",
                "retry_tables": ["bronze.table_a"],
                "force_reload": True,
            },
        ),
        (
            "00_master_orchestrator.ipynb",
            {
                "source": "finance",
                "run_ts": "20240103T010000",
                "dag_path": "config/dag_finance.json",
                "retry_tables": [],
                "force_reload": False,
            },
        ),
        (
            "00_master_orchestrator.ipynb",
            {
                "source": "marketing",
                "run_ts": "20240104T020000",
                "dag_path": "config/dag_marketing.json",
                "retry_tables": ["silver.table_b", "silver.table_c"],
                "force_reload": False,
            },
        ),
        (
            "00_master_orchestrator.ipynb",
            {
                "source": "sales",
                "run_ts": "20240105T030000",
                "dag_path": "config/dag_sales.json",
                "retry_tables": [],
                "force_reload": True,
            },
        ),
        (
            "00_master_orchestrator.ipynb",
            {
                "source": "operations",
                "run_ts": "20240106T040000",
                "dag_path": "config/dag_operations.json",
                "retry_tables": ["bronze.retry_me"],
                "force_reload": False,
            },
        ),
        (
            "01_utils_logging.ipynb",
            {"BASE_PATH": "{BASE_PATH}", "log_level": "INFO"},
        ),
        (
            "01_utils_logging.ipynb",
            {"BASE_PATH": "{BASE_PATH}", "log_level": "DEBUG"},
        ),
        (
            "01_utils_logging.ipynb",
            {"BASE_PATH": "{BASE_PATH}", "log_level": "WARN", "append": True},
        ),
        (
            "01_utils_logging.ipynb",
            {"BASE_PATH": "{BASE_PATH}", "log_level": "ERROR", "stream": False},
        ),
        (
            "01_utils_logging.ipynb",
            {"BASE_PATH": "{BASE_PATH}", "log_level": "INFO", "with_metrics": True},
        ),
        (
            "02_utils_config.ipynb",
            {"config_base_path": "{BASE_PATH}"},
        ),
        (
            "02_utils_config.ipynb",
            {"config_base_path": "{BASE_PATH}", "config_name": "dag_default"},
        ),
        (
            "02_utils_config.ipynb",
            {"config_base_path": "{BASE_PATH}", "config_name": "dag_marketing"},
        ),
        (
            "02_utils_config.ipynb",
            {"config_base_path": "{BASE_PATH}", "config_name": "dag_sales"},
        ),
        (
            "02_utils_config.ipynb",
            {"config_base_path": "{BASE_PATH}", "config_name": "dag_finance"},
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
            "10_bronze_load.ipynb",
            {
                "RUN_ID": "unit-test-2",
                "BASE_PATH": "{BASE_PATH}",
                "run_ts": "20240102T000000",
                "run_date": "2024-01-02",
                "base_files": "greenhouse_sources",
                "source_name": "finance",
            },
        ),
        (
            "10_bronze_load.ipynb",
            {
                "RUN_ID": "unit-test-3",
                "BASE_PATH": "{BASE_PATH}",
                "run_ts": "20240103T000000",
                "run_date": "2024-01-03",
                "base_files": "greenhouse_sources",
                "source_name": "marketing",
            },
        ),
        (
            "10_bronze_load.ipynb",
            {
                "RUN_ID": "unit-test-4",
                "BASE_PATH": "{BASE_PATH}",
                "run_ts": "20240104T000000",
                "run_date": "2024-01-04",
                "base_files": "greenhouse_sources",
                "source_name": "sales",
            },
        ),
        (
            "10_bronze_load.ipynb",
            {
                "RUN_ID": "unit-test-5",
                "BASE_PATH": "{BASE_PATH}",
                "run_ts": "20240105T000000",
                "run_date": "2024-01-05",
                "base_files": "greenhouse_sources",
                "source_name": "operations",
            },
        ),
        (
            "10_bronze_load.ipynb",
            {
                "RUN_ID": "unit-test-6",
                "BASE_PATH": "{BASE_PATH}",
                "run_ts": "20240106T000000",
                "run_date": "2024-01-06",
                "base_files": "greenhouse_sources",
                "source_name": "engineering",
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
            "11_bronze_watermark_merge.ipynb",
            {
                "BASE_PATH": "{BASE_PATH}",
                "watermarks_path": "config/watermarks_finance.json",
                "wm_folder": "runtime/finance/20240102T000000",
            },
        ),
        (
            "11_bronze_watermark_merge.ipynb",
            {
                "BASE_PATH": "{BASE_PATH}",
                "watermarks_path": "config/watermarks_marketing.json",
                "wm_folder": "runtime/marketing/20240103T000000",
            },
        ),
        (
            "11_bronze_watermark_merge.ipynb",
            {
                "BASE_PATH": "{BASE_PATH}",
                "watermarks_path": "config/watermarks_sales.json",
                "wm_folder": "runtime/sales/20240104T000000",
            },
        ),
        (
            "11_bronze_watermark_merge.ipynb",
            {
                "BASE_PATH": "{BASE_PATH}",
                "watermarks_path": "config/watermarks_ops.json",
                "wm_folder": "runtime/operations/20240105T000000",
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
            "20_silver_cdc_merge.ipynb",
            {
                "BASE_PATH": "{BASE_PATH}",
                "source": "finance",
                "run_ts": "20240102T000000",
                "target_table": "bronze.finance_table",
                "watermark_key": "finance",
            },
        ),
        (
            "20_silver_cdc_merge.ipynb",
            {
                "BASE_PATH": "{BASE_PATH}",
                "source": "marketing",
                "run_ts": "20240103T000000",
                "target_table": "bronze.marketing_table",
                "watermark_key": "marketing",
            },
        ),
        (
            "20_silver_cdc_merge.ipynb",
            {
                "BASE_PATH": "{BASE_PATH}",
                "source": "sales",
                "run_ts": "20240104T000000",
                "target_table": "bronze.sales_table",
                "watermark_key": "sales",
            },
        ),
        (
            "20_silver_cdc_merge.ipynb",
            {
                "BASE_PATH": "{BASE_PATH}",
                "source": "operations",
                "run_ts": "20240105T000000",
                "target_table": "bronze.ops_table",
                "watermark_key": "operations",
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
        (
            "99_bronze_cleanup.ipynb",
            {
                "BASE_PATH": "{BASE_PATH}",
                "target_table": "bronze.finance_table",
                "retention_days": 7,
                "vacuum": False,
            },
        ),
        (
            "99_bronze_cleanup.ipynb",
            {
                "BASE_PATH": "{BASE_PATH}",
                "target_table": "bronze.marketing_table",
                "retention_days": 14,
                "vacuum": True,
            },
        ),
        (
            "99_bronze_cleanup.ipynb",
            {
                "BASE_PATH": "{BASE_PATH}",
                "target_table": "bronze.sales_table",
                "retention_days": 30,
                "vacuum": False,
            },
        ),
        (
            "99_bronze_cleanup.ipynb",
            {
                "BASE_PATH": "{BASE_PATH}",
                "target_table": "bronze.ops_table",
                "retention_days": 3,
                "vacuum": True,
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