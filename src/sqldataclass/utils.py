"""Utility functions for model data manipulation."""

from __future__ import annotations

import re
from functools import lru_cache
from typing import Any

from sqldataclass.versioning import camel_to_snake_case_upper


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


@lru_cache
def _legacy_version_field_name(class_name: str) -> str:
    """Return the legacy version field name format (e.g. ADDRESSSCHEMA_VERSION)."""
    name = re.sub(r"\[.*?\]", "", class_name).upper()
    return f"{name}SCHEMA_VERSION"


def migrate_legacy_version_strings(obj: dict[str, Any], cls: type) -> None:
    """Rename legacy version field format to the current one.

    Converts e.g. ``ADDRESSSCHEMA_VERSION`` → ``ADDRESS_VERSION``.
    Modifies *obj* in place.
    """
    legacy_key = _legacy_version_field_name(cls.__name__)
    if legacy_key in obj:
        current_key = f"{camel_to_snake_case_upper(cls.__name__)}_VERSION"
        obj[current_key] = obj.pop(legacy_key)
