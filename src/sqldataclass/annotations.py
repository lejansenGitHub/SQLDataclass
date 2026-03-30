"""Numpy type annotations with automatic pydantic serialization/validation.

Provides ``Np.int64``, ``Np.float64``, ``Np.bool_`` for scalars, and
``Np.Array.*`` / ``Np.Array2d.*`` for 1D and 2D arrays.  Each type
auto-converts between numpy and Python types during validation and
serialization.

Requires ``numpy`` — install with ``pip install sqldataclass[numpy]``.
"""

from __future__ import annotations

from functools import partial
from typing import Annotated, TypeVar

import numpy as np
import numpy.typing as npt
from pydantic import PlainSerializer
from pydantic.functional_validators import BeforeValidator


def _to_int(x: int | np.int64) -> int:
    return int(x)


def _to_float(x: float | np.float64) -> float:
    return float(x)


def _to_bool(x: bool | np.bool_) -> bool:
    return bool(x)


def _to_np_int64(x: int | np.int64) -> np.int64:
    return np.int64(x)


def _to_np_float64(x: float | np.float64) -> np.float64:
    return np.float64(x)


def _to_np_bool_(x: bool | np.bool_) -> np.bool_:
    return np.bool_(x)


T1 = TypeVar("T1", bound=np.float64 | np.int64 | np.bool_)


def _to_np_array(t: type[T1], x: list | npt.NDArray[T1]) -> npt.NDArray[T1]:  # type: ignore[type-arg]
    return np.asarray(x, dtype=t)


T2 = TypeVar("T2", bound=float | int | bool)


def _to_list(_t: type[T2], x: np.ndarray) -> list[T2]:
    result: list[T2] = x.tolist()
    return result


class Np:
    """Numpy type annotations for pydantic models.

    Scalars::

        class Data(SQLDataclass):
            score: Np.float64
            count: Np.int64
            flag: Np.bool_

    Arrays::

        class Matrix(SQLDataclass):
            values: Np.Array.float64
            grid: Np.Array2d.int64
    """

    int64 = Annotated[
        np.int64,
        PlainSerializer(_to_int, return_type=int, when_used="always"),
        BeforeValidator(_to_np_int64),
    ]
    float64 = Annotated[
        np.float64,
        PlainSerializer(_to_float, return_type=float, when_used="always"),
        BeforeValidator(_to_np_float64),
    ]
    bool_ = Annotated[
        np.bool_,
        PlainSerializer(_to_bool, return_type=bool, when_used="always"),
        BeforeValidator(_to_np_bool_),
    ]

    class Array:
        """1D numpy array annotations."""

        int64 = Annotated[
            npt.NDArray[np.int64],
            PlainSerializer(partial(_to_list, int), return_type=list[int], when_used="always"),
            BeforeValidator(partial(_to_np_array, np.int64)),
        ]
        float64 = Annotated[
            npt.NDArray[np.float64],
            PlainSerializer(partial(_to_list, float), return_type=list[float], when_used="always"),
            BeforeValidator(partial(_to_np_array, np.float64)),
        ]
        bool_ = Annotated[
            npt.NDArray[np.bool_],
            PlainSerializer(partial(_to_list, bool), return_type=list[bool], when_used="always"),
            BeforeValidator(partial(_to_np_array, np.bool_)),
        ]

    class Array2d:
        """2D numpy array annotations."""

        int64 = Annotated[
            npt.NDArray[np.int64],
            PlainSerializer(partial(_to_list, int), return_type=list[list[int]], when_used="always"),
            BeforeValidator(partial(_to_np_array, np.int64)),
        ]
        float64 = Annotated[
            npt.NDArray[np.float64],
            PlainSerializer(
                partial(_to_list, float),
                return_type=list[list[float]],
                when_used="always",
            ),
            BeforeValidator(partial(_to_np_array, np.float64)),
        ]
        bool_ = Annotated[
            npt.NDArray[np.bool_],
            PlainSerializer(
                partial(_to_list, bool),
                return_type=list[list[bool]],
                when_used="always",
            ),
            BeforeValidator(partial(_to_np_array, np.bool_)),
        ]
