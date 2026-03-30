"""Custom pydantic validators."""

from __future__ import annotations

from typing import Any, Callable

from pydantic import BeforeValidator


def FillValueIfNone(
    default: Any = None,
    default_factory: Callable[[], Any] | None = None,
) -> BeforeValidator:
    """Return a ``BeforeValidator`` that replaces ``None`` with a default.

    Usage::

        from typing import Annotated
        from sqldataclass import SQLDataclass
        from sqldataclass.validators import FillValueIfNone

        class Config(SQLDataclass):
            name: Annotated[str, FillValueIfNone(default="unnamed")]
            tags: Annotated[list[str], FillValueIfNone(default_factory=list)]
    """
    if default is not None:
        return BeforeValidator(lambda x: x if x is not None else default)
    if default_factory is not None:
        return BeforeValidator(lambda x: x if x is not None else default_factory())
    msg = "Either default or default_factory must be provided."
    raise NotImplementedError(msg)
