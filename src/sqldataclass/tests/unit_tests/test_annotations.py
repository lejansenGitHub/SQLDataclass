"""Unit tests for Np type annotations (numpy serialization/validation)."""

from __future__ import annotations

import numpy as np
import pytest

from sqldataclass import SQLDataclass
from sqldataclass.annotations import Np


class TestNpScalars:
    """Np.int64, Np.float64, Np.bool_ round-trip."""

    def test_int64_load_dump(self) -> None:
        class Data(SQLDataclass):
            value: Np.int64

        d = Data(value=42)
        assert isinstance(d.value, np.int64)
        dumped = d.dump()
        assert dumped["value"] == 42
        assert isinstance(dumped["value"], int)

    def test_float64_load_dump(self) -> None:
        class Data(SQLDataclass):
            value: Np.float64

        d = Data(value=3.14)
        assert isinstance(d.value, np.float64)
        dumped = d.dump()
        assert dumped["value"] == pytest.approx(3.14)
        assert isinstance(dumped["value"], float)

    def test_bool_load_dump(self) -> None:
        class Data(SQLDataclass):
            flag: Np.bool_

        d = Data(flag=True)
        assert isinstance(d.flag, np.bool_)
        dumped = d.dump()
        assert dumped["flag"] is True
        assert isinstance(dumped["flag"], bool)


class TestNpArray:
    """Np.Array.* 1D array round-trip."""

    def test_int64_array(self) -> None:
        class Data(SQLDataclass):
            values: Np.Array.int64

        d = Data(values=[1, 2, 3])
        assert isinstance(d.values, np.ndarray)
        assert d.values.dtype == np.int64
        dumped = d.dump()
        assert dumped["values"] == [1, 2, 3]

    def test_float64_array(self) -> None:
        class Data(SQLDataclass):
            values: Np.Array.float64

        d = Data(values=[1.0, 2.5, 3.7])
        assert isinstance(d.values, np.ndarray)
        dumped = d.dump()
        assert dumped["values"] == pytest.approx([1.0, 2.5, 3.7])

    def test_accepts_ndarray(self) -> None:
        class Data(SQLDataclass):
            values: Np.Array.int64

        arr = np.array([10, 20, 30], dtype=np.int64)
        d = Data(values=arr)
        assert list(d.values) == [10, 20, 30]


class TestNpArray2d:
    """Np.Array2d.* 2D array round-trip."""

    def test_int64_2d(self) -> None:
        class Data(SQLDataclass):
            grid: Np.Array2d.int64

        d = Data(grid=[[1, 2], [3, 4]])
        assert isinstance(d.grid, np.ndarray)
        assert d.grid.shape == (2, 2)
        dumped = d.dump()
        assert dumped["grid"] == [[1, 2], [3, 4]]

    def test_float64_2d(self) -> None:
        class Data(SQLDataclass):
            matrix: Np.Array2d.float64

        d = Data(matrix=[[1.0, 2.0], [3.0, 4.0]])
        dumped = d.dump()
        assert dumped["matrix"] == [[1.0, 2.0], [3.0, 4.0]]


class TestNpRoundTrip:
    """Full load → dump → reload round-trip."""

    def test_full_round_trip(self) -> None:
        class Measurement(SQLDataclass):
            score: Np.float64
            counts: Np.Array.int64

        original = Measurement(score=9.5, counts=[1, 2, 3])
        dumped = original.dump()
        reloaded = Measurement.load(dumped)
        assert isinstance(reloaded.score, np.float64)
        assert isinstance(reloaded.counts, np.ndarray)
        assert float(reloaded.score) == pytest.approx(9.5)
        assert list(reloaded.counts) == [1, 2, 3]
