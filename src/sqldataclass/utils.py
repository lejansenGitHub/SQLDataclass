"""Utility functions for model data manipulation."""

from __future__ import annotations

from typing import Any


def remove_unexpected_kwargs(obj: dict[str, Any], cls: type) -> None:
    """Remove dict keys that are not fields on the model class.

    Works with both ``SQLDataclass`` and ``SQLModel`` subclasses.
    Modifies *obj* in place.
    """
    if hasattr(cls, "data_fields"):
        fields = cls.data_fields()
    elif hasattr(cls, "model_fields"):
        fields = {field.alias if field.alias is not None else name for name, field in cls.model_fields.items()}
    else:
        msg = f"Expected an SQLDataclass or SQLModel subclass, got {cls}"
        raise TypeError(msg)

    for key in list(obj.keys() - fields):
        del obj[key]
