"""Tests that model instances are fully slotted."""

from __future__ import annotations

import pytest

from sqldataclass import Field, SQLDataclass


class SlotTable(SQLDataclass, table=True):
    __tablename__ = "slot_table"
    id: int | None = Field(default=None, primary_key=True)
    name: str = ""


class SlotData(SQLDataclass):
    name: str = ""


def _assert_fully_slotted(model: type) -> None:
    """Every class in the hierarchy except object declares slots, so instances get no dict or weakref."""
    instance = model(name="a")
    slots_in_hierarchy = [getattr(cls, "__slots__", None) for cls in model.__mro__ if cls is not object]
    assert not hasattr(instance, "__dict__")
    assert not hasattr(instance, "__weakref__")
    assert all(slots is not None for slots in slots_in_hierarchy)


def test_table_model_instances_have_no_dict_or_weakref() -> None:
    """The base class declares empty slots, so the generated dataclass slots are the whole layout."""
    _assert_fully_slotted(SlotTable)


def test_data_model_instances_have_no_dict_or_weakref() -> None:
    """Data-only models share the base class and therefore the same layout."""
    _assert_fully_slotted(SlotData)


def test_table_model_rejects_undeclared_attribute() -> None:
    """A stray attribute has nowhere to live, so assignment fails instead of hiding in a dict."""
    # --- Input ---
    instance = SlotTable(name="a")

    # --- Execute / Assert ---
    with pytest.raises(AttributeError):
        instance.other = 1


def test_data_model_rejects_undeclared_attribute() -> None:
    """Same guarantee for data-only models."""
    # --- Input ---
    instance = SlotData(name="a")

    # --- Execute / Assert ---
    with pytest.raises(AttributeError):
        instance.other = 1
