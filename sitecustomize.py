"""Lightweight testing shims for pyspark imports in notebooks/tests."""
from __future__ import annotations
import sys
import types
from typing import Dict, Optional


def _install_pyspark_stubs() -> None:
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

        # Placeholder methods used in notebooks; raise to surface unexpected use
        def sql(self, *_args, **_kwargs):  # pragma: no cover - protective stub
            raise NotImplementedError("SparkSession.sql stubbed for tests")

        def table(self, *_args, **_kwargs):  # pragma: no cover - protective stub
            raise NotImplementedError("SparkSession.table stubbed for tests")

        class _Reader:
            def parquet(self, *_args, **_kwargs):  # pragma: no cover
                raise NotImplementedError("SparkSession.read.parquet stubbed for tests")

        @property
        def read(self):
            return self._Reader()

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
        "input_file_name",
        "year",
        "month",
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
