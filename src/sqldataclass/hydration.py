"""Dict reshaping for discriminated unions.

When SQL rows are flat but the domain model uses discriminated unions
(e.g. `data: NormalData | BatteryData` with a discriminator field),
these helpers reshape the flat dict into the nested structure pydantic expects.
"""

from __future__ import annotations

from typing import Any, get_args, get_type_hints


def nest_fields(data: dict[str, Any], field_name: str, keys: set[str]) -> dict[str, Any]:
    """Extract `keys` from flat `data` into a nested dict under `field_name`.

    Keys present in `data` are moved (popped) into the nested dict.
    Missing keys are silently skipped.
    """
    nested = {}
    for key in keys:
        if key in data:
            nested[key] = data.pop(key)
    data[field_name] = nested
    return data


def discriminator_map(
    parent_class: type,
    field_name: str,
    discriminator: str,
) -> dict[str, type]:
    """Build {discriminator_value: subclass} from a discriminated union type hint.

    E.g. for `data: NormalData | BatteryData` with discriminator="behavior":
    -> {"normal": NormalData, "battery": BatteryData}
    """
    hints = get_type_hints(parent_class)
    union_args = get_args(hints[field_name])
    mapping: dict[str, type] = {}
    for sub_class in union_args:
        sub_hints = get_type_hints(sub_class)
        if discriminator not in sub_hints:
            continue
        for literal_value in get_args(sub_hints[discriminator]):
            mapping[literal_value] = sub_class
    return mapping


def _data_fields(cls: type) -> set[str]:
    """Get the set of field names from a pydantic dataclass or stdlib dataclass."""
    if hasattr(cls, "__pydantic_fields__"):
        return set(cls.__pydantic_fields__.keys())
    if hasattr(cls, "__dataclass_fields__"):
        return set(cls.__dataclass_fields__.keys())
    raise TypeError(f"Expected a dataclass type, got {cls}")


def format_discriminated(
    data: dict[str, Any],
    parent_class: type,
    *,
    field_name: str,
    discriminator: str,
) -> dict[str, Any]:
    """Reshape a flat SQL row dict for a discriminated union field.

    1. Determines the active subtype from data[discriminator]
    2. Collects fields of the active subtype into a nested dict
    3. Removes fields of all other subtypes from the flat dict
    4. Returns the reshaped dict ready for parent_class construction
    """
    mapping = discriminator_map(parent_class, field_name, discriminator)
    active_class = mapping[data[discriminator]]
    other_classes = set(mapping.values()) - {active_class}

    nested = {}
    for key in _data_fields(active_class):
        if key in data:
            nested[key] = data.pop(key)

    for other_class in other_classes:
        for key in _data_fields(other_class):
            data.pop(key, None)

    data[field_name] = nested
    return data
