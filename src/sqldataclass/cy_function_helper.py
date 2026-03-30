"""Cython function detection workaround for pydantic.

Pydantic may emit warnings for unannotated class attributes that are
actually Cython-compiled functions (``cython_function_or_method``).
Adding ``CyFunctionDetector`` to ``ConfigDict(ignored_types=(...))``
suppresses these false positives.

See: https://github.com/pydantic/pydantic/issues/6670
"""

from __future__ import annotations


class _CyFunctionDetectorMeta(type):
    """Metaclass that makes isinstance() match Cython functions by class name."""

    def __instancecheck__(self, instance: object) -> bool:
        return instance.__class__.__name__ == "cython_function_or_method"


class CyFunctionDetector(metaclass=_CyFunctionDetectorMeta):
    """Marker type for ``ConfigDict(ignored_types=(...))`` to skip Cython functions."""
