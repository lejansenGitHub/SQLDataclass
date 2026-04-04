"""Unit tests for return type wrappers."""

from __future__ import annotations

from sqldataclass import DictReturn, DictReturnMany, ModelReturn, ModelReturnMany, NonNullDictReturn


class TestModelReturn:
    def test_named_access(self) -> None:
        r = ModelReturn(data={"name": "Alice"}, errors={})
        assert r.data == {"name": "Alice"}
        assert r.errors == {}

    def test_iterator_unpacking(self) -> None:
        r = ModelReturn(data="result", errors={"field": "error"})
        data, errors = r
        assert data == "result"
        assert errors == {"field": "error"}


class TestModelReturnMany:
    def test_with_list(self) -> None:
        r = ModelReturnMany(data=[1, 2, 3], errors={})
        assert r.data == [1, 2, 3]

    def test_with_none(self) -> None:
        r: ModelReturnMany[str] = ModelReturnMany(data=None, errors={"msg": "not found"})
        assert r.data is None


class TestDictReturn:
    def test_with_dict(self) -> None:
        r = DictReturn(data={"key": "value"}, errors={})
        assert r.data == {"key": "value"}

    def test_with_none(self) -> None:
        r = DictReturn(data=None, errors={})
        assert r.data is None


class TestNonNullDictReturn:
    def test_with_dict(self) -> None:
        r = NonNullDictReturn(data={"key": "value"}, errors={})
        assert r.data == {"key": "value"}


class TestDictReturnMany:
    def test_with_list(self) -> None:
        r = DictReturnMany(data=[{"a": 1}, {"b": 2}], errors={})
        assert len(r.data) == 2  # type: ignore[arg-type]  # generic type param not inferred from constructor
