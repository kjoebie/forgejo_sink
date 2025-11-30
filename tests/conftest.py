import sys
import types
from pathlib import Path
from typing import Dict, Optional

import pytest


# Ensure project root is importable when running tests without installing the package
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def _install_pyspark_stubs() -> None:
    """Provide minimal pyspark stubs so modules import without the real package."""
    pyspark = sys.modules.get("pyspark") or types.ModuleType("pyspark")
    sys.modules.setdefault("pyspark", pyspark)

    sql_module = types.ModuleType("pyspark.sql")

    class SparkConf:
        def __init__(self, settings: Optional[Dict[str, str]] = None):
            self._settings = settings or {}

        def get(self, key: str, default=None):
            if key in self._settings:
                return self._settings[key]
            if default is not None:
                return default
            raise KeyError(key)

    class SparkSession:
        def __init__(self, conf: Optional[SparkConf] = None):
            self.conf = conf or SparkConf()

    sql_module.SparkSession = SparkSession
    sql_module.SparkConf = SparkConf
    sys.modules.setdefault("pyspark.sql", sql_module)

    functions_module = types.ModuleType("pyspark.sql.functions")

    def _not_implemented(*_, **__):  # pragma: no cover - helpers are stubs
        raise NotImplementedError("pyspark function stub")

    for name in [
        "sha2",
        "concat_ws",
        "col",
        "coalesce",
        "lit",
        "when",
        "array",
        "concat",
        "collect_list",
        "explode",
        "struct",
        "sum",
    ]:
        setattr(functions_module, name, _not_implemented)

    sys.modules.setdefault("pyspark.sql.functions", functions_module)

    dataframe_module = types.ModuleType("pyspark.sql.dataframe")

    class DataFrame:  # pragma: no cover - structural stub
        pass

    dataframe_module.DataFrame = DataFrame
    sys.modules.setdefault("pyspark.sql.dataframe", dataframe_module)
    sql_module.DataFrame = DataFrame


_install_pyspark_stubs()


@pytest.fixture(scope="session", autouse=True)
def pyspark_stubs():
    _install_pyspark_stubs()


@pytest.fixture
def mock_spark_session():
    from pyspark.sql import SparkConf, SparkSession

    class DummySpark(SparkSession):
        def __init__(self, settings: Optional[Dict[str, str]] = None):
            super().__init__(conf=SparkConf(settings))

    return DummySpark


@pytest.fixture
def parquet_tmp_path(tmp_path):
    path = tmp_path / "parquet"
    path.mkdir()
    return path


@pytest.fixture
def delta_tmp_path(tmp_path):
    path = tmp_path / "delta"
    path.mkdir()
    return path
