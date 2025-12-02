import os
import pytest
from modules import path_utils


@pytest.mark.unit
def test_detect_environment_with_fabric_conf(mock_spark_session, monkeypatch):
    # Ensure filesystem checks do not interfere
    monkeypatch.setattr(os.path, "exists", lambda p: False)
    spark = mock_spark_session({"spark.microsoft.fabric.workspaceId": "abc123"})
    assert path_utils.detect_environment(spark) == "fabric"


@pytest.mark.unit
def test_detect_environment_without_spark_defaults_to_local(mock_spark_session, monkeypatch):
    monkeypatch.setattr(os.path, "exists", lambda p: False)
    spark = mock_spark_session()
    assert path_utils.detect_environment(spark) == "local"


@pytest.mark.unit
def test_get_base_path_prefers_fabric_when_detected(mock_spark_session, monkeypatch):
    monkeypatch.setattr(os.path, "exists", lambda p: False)
    spark = mock_spark_session({"spark.microsoft.fabric.workspaceId": "abc123"})
    assert path_utils.get_base_path(spark) == "/lakehouse/default/Files"


@pytest.mark.unit
def test_get_base_path_uses_cluster_root_when_available(monkeypatch):
    monkeypatch.setattr(os.path, "exists", lambda p: p == path_utils.CLUSTER_FILES_ROOT)
    assert path_utils.get_base_path() == path_utils.CLUSTER_FILES_ROOT


@pytest.mark.unit
def test_get_base_path_uses_glob_match_when_no_fixed_root(monkeypatch):
    candidate = "/data/lakehouse/custom/Files"

    def fake_exists(path: str) -> bool:
        return path in {candidate, "/data/lakehouse"}

    monkeypatch.setattr(os.path, "exists", fake_exists)
    monkeypatch.setattr(path_utils.glob, "glob", lambda pattern, recursive: [candidate])

    assert path_utils.get_base_path() == candidate


@pytest.mark.unit
def test_get_base_path_falls_back_to_relative(monkeypatch):
    monkeypatch.setattr(os.path, "exists", lambda p: False)
    monkeypatch.setattr(path_utils.glob, "glob", lambda pattern, recursive: [])
    assert path_utils.get_base_path() == "Files"


@pytest.mark.unit
def test_resolve_files_path_maps_using_detected_base(monkeypatch):
    monkeypatch.setattr(path_utils, "get_base_path", lambda spark=None: "/data/lakehouse/custom/Files")
    resolved = path_utils.resolve_files_path("Files/data/table")
    assert resolved == "/data/lakehouse/custom/Files/data/table"


@pytest.mark.unit
def test_resolve_files_path_passes_through_non_files():
    assert path_utils.resolve_files_path("/tmp/data") == "/tmp/data"


@pytest.mark.unit
def test_resolve_files_path_leaves_fabric_paths(mock_spark_session, monkeypatch):
    monkeypatch.setattr(path_utils, "detect_environment", lambda spark=None: "fabric")

    result = path_utils.resolve_files_path("/Files/data", spark=mock_spark_session())

    assert result == "Files/data"


@pytest.mark.unit
def test_build_parquet_dir_relies_on_resolver(monkeypatch):
    calls = {}

    def fake_resolve(relative: str, spark=None) -> str:
        calls["relative"] = relative
        return "resolved-path"

    monkeypatch.setattr(path_utils, "resolve_files_path", fake_resolve)

    result = path_utils.build_parquet_dir(
        base_files="greenhouse_sources",
        source_name="anva_concern",
        run_ts="20251125T060000",
        table_name="Dim_Relatie",
    )

    assert calls["relative"] == "Files/greenhouse_sources/anva_concern/2025/11/25/20251125T060000/Dim_Relatie"
    assert result == "resolved-path"


@pytest.mark.unit
def test_build_parquet_dir_validates_run_ts():
    with pytest.raises(ValueError):
        path_utils.build_parquet_dir(
            base_files="greenhouse_sources",
            source_name="anva_concern",
            run_ts="2025",
            table_name="Dim_Relatie",
        )