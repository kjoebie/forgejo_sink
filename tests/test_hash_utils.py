import types
import pytest

from modules import hash_utils


class DummyDataFrame:
    def __init__(self, columns, dtypes=None):
        self.columns = columns
        self.dtypes = dtypes or [(c, "string") for c in columns]


def test_add_row_hash_raises_when_column_exists():
    df = DummyDataFrame(["id", "row_hash"])
    with pytest.raises(ValueError):
        hash_utils.add_row_hash(df)


def test_add_row_hash_validates_hash_algorithm():
    df = DummyDataFrame(["id", "name"])
    with pytest.raises(ValueError):
        hash_utils.add_row_hash(df, hash_algorithm="sha1024")


def test_resolve_hash_columns_defaults_to_all_columns():
    all_cols = {"a", "b", "c"}
    assert hash_utils._resolve_hash_columns(all_cols, None, None) == ["a", "b", "c"]


def test_resolve_hash_columns_honors_include_list():
    all_cols = {"a", "b", "c"}
    assert hash_utils._resolve_hash_columns(all_cols, ["b"], None) == ["b"]


def test_resolve_hash_columns_raises_for_missing_include():
    with pytest.raises(ValueError):
        hash_utils._resolve_hash_columns({"a", "b"}, ["c"], None)


def test_resolve_hash_columns_excludes_columns():
    all_cols = {"a", "b", "c"}
    assert hash_utils._resolve_hash_columns(all_cols, None, ["b"]) == ["a", "c"]


def test_resolve_hash_columns_raises_for_missing_exclude():
    with pytest.raises(ValueError):
        hash_utils._resolve_hash_columns({"a", "b"}, None, ["c"])


@pytest.fixture
def fake_col(monkeypatch):
    class FakeColumn:
        def __init__(self, name):
            self.name = name

        def isNull(self):
            return self

        def cast(self, *_):
            return self

    monkeypatch.setattr(hash_utils, "col", lambda name: FakeColumn(name))
    monkeypatch.setattr(hash_utils, "spark_sum", lambda value: types.SimpleNamespace(alias=lambda _: value))
    return FakeColumn


def test_validate_hash_columns_checks_presence(fake_col):
    df = DummyDataFrame(["id"])
    with pytest.raises(ValueError):
        hash_utils.validate_hash_columns(df)