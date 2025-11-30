import os
import pytest
from modules import path_utils


@pytest.mark.unit
def test_detect_environment_with_fabric_conf(mock_spark_session):
    spark = mock_spark_session({"spark.microsoft.fabric.workspaceId": "abc123"})
    assert path_utils.detect_environment(spark) == "fabric"


@pytest.mark.unit
def test_detect_environment_without_spark_defaults_to_local(mock_spark_session):
    spark = mock_spark_session()
    assert path_utils.detect_environment(spark) == "local"


@pytest.mark.unit
def test_build_parquet_dir_uses_cluster_root_when_local():
    result = path_utils.build_parquet_dir(
        base_files="greenhouse_sources",
        source_name="anva_concern",
        run_ts="20251125T060000",
        table_name="Dim_Relatie",
    )

    expected = (
        f"{path_utils.CLUSTER_FILES_ROOT}/greenhouse_sources/anva_concern/"
        "2025/11/25/20251125T060000/Dim_Relatie"
    )
    assert result == expected


@pytest.mark.unit
def test_build_parquet_glob_appends_parquet_suffix():
    glob = path_utils.build_parquet_glob(
        base_files="greenhouse_sources",
        source_name="anva_concern",
        run_ts="20251125T060000",
        table_name="Dim_Relatie",
    )

    assert glob.endswith("/*.parquet")
    assert "Dim_Relatie" in glob


@pytest.mark.unit
def test_build_delta_table_name_prefers_table_def_override():
    table_def = {"delta_table": "custom_name", "delta_schema": "silver"}
    assert path_utils.build_delta_table_name("bronze", "Dim_Relatie", table_def) == "silver.custom_name"


@pytest.mark.unit
@pytest.mark.parametrize(
    "run_ts, expected",
    [
        ("20251125T060000", ("2025", "11", "25")),
        ("19991231T235959", ("1999", "12", "31")),
    ],
)
def test_extract_date_from_run_ts(run_ts, expected):
    assert path_utils.extract_date_from_run_ts(run_ts) == expected


@pytest.mark.integration
def test_resolve_files_path_switches_to_fabric_when_env_present(mock_spark_session, tmp_path, monkeypatch):
    fabric_root = tmp_path / "lakehouse" / "default"
    (fabric_root / "Files").mkdir(parents=True)

    monkeypatch.setattr(os.path, "exists", lambda p, original=os.path.exists: True if p == "/lakehouse/default" else original(p))

    spark = mock_spark_session({"spark.microsoft.fabric.workspaceId": "abc123"})
    result = path_utils.resolve_files_path("Files/data", spark)
    assert result == "Files/data"