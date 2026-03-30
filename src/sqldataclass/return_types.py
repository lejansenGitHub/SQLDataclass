"""Return type wrappers for model operations.

Provides dataclass containers that bundle a result with an errors dict,
supporting both named access and legacy iterator unpacking.
"""

from __future__ import annotations

from collections.abc import Generator
from dataclasses import dataclass
from typing import Any, Generic, TypeVar

Model = TypeVar("Model")


@dataclass
class ModelReturnBase:
    """Base return type with data and errors dict."""

    data: Any
    errors: dict[str, str]

    def __iter__(self) -> Generator[Any]:
        yield self.data
        yield self.errors


@dataclass
class ModelReturn(ModelReturnBase, Generic[Model]):
    """Return type for a single model instance."""

    data: Model


@dataclass
class ModelReturnMany(ModelReturnBase, Generic[Model]):
    """Return type for multiple model instances."""

    data: list[Model] | None


@dataclass
class DictReturnBase:
    """Base return type for dict results."""

    data: Any
    errors: dict[str, str]

    def __iter__(self) -> Generator[Any]:
        yield self.data
        yield self.errors


@dataclass
class DictReturn(DictReturnBase):
    """Return type for a single dict result."""

    data: dict[str, Any] | None


@dataclass
class NonNullDictReturn(DictReturnBase):
    """Return type for a non-null dict result."""

    data: dict[str, Any]


@dataclass
class DictReturnMany(DictReturnBase):
    """Return type for multiple dict results."""

    data: list[dict[str, Any]] | None
