"""Pure dict tests for hydration — no DB needed."""

from __future__ import annotations

from typing import Literal

from pydantic import Field
from pydantic.dataclasses import dataclass as dataclass_pydantic

from sqldataclass.hydration import (
    discriminator_map,
    format_discriminated,
    nest_fields,
)

# --- Test domain models ---


@dataclass_pydantic(kw_only=True, slots=True)
class NormalData:
    behavior: Literal["normal"] = "normal"
    p_max: float = 0.0


@dataclass_pydantic(kw_only=True, slots=True)
class BatteryData:
    behavior: Literal["battery"] = "battery"
    capacity: float = 0.0


@dataclass_pydantic(kw_only=True, slots=True)
class Participant:
    participant_id: int
    name: str = "Undefined"
    data: NormalData | BatteryData = Field(..., discriminator="behavior")


# --- nest_fields ---


def test_nest_fields_moves_keys_into_nested_dict() -> None:
    data: dict[str, object] = {"a": 1, "b": 2, "c": 3}
    result = nest_fields(data, "nested", {"a", "c"})
    assert result == {"b": 2, "nested": {"a": 1, "c": 3}}


def test_nest_fields_skips_missing_keys() -> None:
    data: dict[str, object] = {"a": 1}
    result = nest_fields(data, "nested", {"a", "missing"})
    assert result == {"nested": {"a": 1}}


def test_nest_fields_empty_keys() -> None:
    data: dict[str, object] = {"a": 1, "b": 2}
    result = nest_fields(data, "nested", set())
    assert result == {"a": 1, "b": 2, "nested": {}}


# --- discriminator_map ---


def test_discriminator_map_builds_correct_mapping() -> None:
    mapping = discriminator_map(Participant, "data", "behavior")
    assert mapping == {"normal": NormalData, "battery": BatteryData}


# --- format_discriminated ---


def test_format_discriminated_normal() -> None:
    flat_row: dict[str, object] = {
        "participant_id": 1,
        "name": "Alice",
        "behavior": "normal",
        "p_max": 100.0,
        "capacity": None,
    }
    result = format_discriminated(flat_row, Participant, field_name="data", discriminator="behavior")
    assert result == {
        "participant_id": 1,
        "name": "Alice",
        "data": {"behavior": "normal", "p_max": 100.0},
    }


def test_format_discriminated_battery() -> None:
    flat_row: dict[str, object] = {
        "participant_id": 2,
        "name": "Bob",
        "behavior": "battery",
        "p_max": None,
        "capacity": 50.0,
    }
    result = format_discriminated(flat_row, Participant, field_name="data", discriminator="behavior")
    assert result == {
        "participant_id": 2,
        "name": "Bob",
        "data": {"behavior": "battery", "capacity": 50.0},
    }


def test_format_discriminated_then_construct_normal() -> None:
    flat_row: dict[str, object] = {
        "participant_id": 1,
        "name": "Alice",
        "behavior": "normal",
        "p_max": 100.0,
        "capacity": None,
    }
    shaped = format_discriminated(flat_row, Participant, field_name="data", discriminator="behavior")
    participant = Participant(**shaped)
    assert participant.participant_id == 1
    assert participant.name == "Alice"
    assert isinstance(participant.data, NormalData)
    assert participant.data.p_max == 100.0


def test_format_discriminated_then_construct_battery() -> None:
    flat_row: dict[str, object] = {
        "participant_id": 2,
        "name": "Bob",
        "behavior": "battery",
        "p_max": None,
        "capacity": 50.0,
    }
    shaped = format_discriminated(flat_row, Participant, field_name="data", discriminator="behavior")
    participant = Participant(**shaped)
    assert participant.participant_id == 2
    assert isinstance(participant.data, BatteryData)
    assert participant.data.capacity == 50.0


def test_format_discriminated_defaults_are_filled() -> None:
    """When subtype fields are missing from the flat row, defaults kick in."""
    flat_row: dict[str, object] = {
        "participant_id": 3,
        "name": "Charlie",
        "behavior": "normal",
    }
    shaped = format_discriminated(flat_row, Participant, field_name="data", discriminator="behavior")
    participant = Participant(**shaped)
    assert isinstance(participant.data, NormalData)
    assert participant.data.p_max == 0.0
