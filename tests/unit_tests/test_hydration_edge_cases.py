"""Edge-case tests for hydration helpers."""

from __future__ import annotations

import dataclasses
from typing import Literal

import pytest
from pydantic import Field
from pydantic.dataclasses import dataclass as dataclass_pydantic

from sqldataclass.hydration import (
    _data_fields,
    discriminator_map,
    format_discriminated,
    nest_fields,
)

# ---------------------------------------------------------------------------
# Test domain models
# ---------------------------------------------------------------------------


@dataclass_pydantic(kw_only=True, slots=True)
class Alpha:
    behavior: Literal["alpha"] = "alpha"
    x: float = 0.0


@dataclass_pydantic(kw_only=True, slots=True)
class Beta:
    behavior: Literal["beta"] = "beta"
    y: float = 0.0


@dataclass_pydantic(kw_only=True, slots=True)
class Gamma:
    behavior: Literal["gamma"] = "gamma"
    z: float = 0.0


@dataclass_pydantic(kw_only=True, slots=True)
class OnlyAlpha:
    """Parent with a single-variant union."""

    tag: str = ""
    data: Alpha = Field(default_factory=Alpha)


@dataclass_pydantic(kw_only=True, slots=True)
class TwoWay:
    tag: str = ""
    data: Alpha | Beta = Field(..., discriminator="behavior")


@dataclass_pydantic(kw_only=True, slots=True)
class ThreeWay:
    tag: str = ""
    data: Alpha | Beta | Gamma = Field(..., discriminator="behavior")


@dataclass_pydantic(kw_only=True, slots=True)
class OverlapA:
    """Shares 'shared' field with OverlapB."""

    behavior: Literal["oa"] = "oa"
    shared: float = 0.0
    only_a: float = 0.0


@dataclass_pydantic(kw_only=True, slots=True)
class OverlapB:
    behavior: Literal["ob"] = "ob"
    shared: float = 0.0
    only_b: float = 0.0


@dataclass_pydantic(kw_only=True, slots=True)
class OverlapParent:
    tag: str = ""
    data: OverlapA | OverlapB = Field(..., discriminator="behavior")


@dataclass_pydantic(kw_only=True, slots=True)
class NoDiscrimField:
    """A class with no 'behavior' field -- used to test skipping."""

    value: int = 0


@dataclass_pydantic(kw_only=True, slots=True)
class MixedParent:
    """Union where one variant lacks the discriminator field."""

    data: Alpha | NoDiscrimField


# ---------------------------------------------------------------------------
# nest_fields edge cases
# ---------------------------------------------------------------------------


class TestNestFieldsEdgeCases:
    def test_all_keys_match(self) -> None:
        """Every key in data is moved into the nested dict."""
        data: dict[str, object] = {"a": 1, "b": 2, "c": 3}
        result = nest_fields(data, "nested", {"a", "b", "c"})
        assert result == {"nested": {"a": 1, "b": 2, "c": 3}}

    def test_empty_data_dict(self) -> None:
        data: dict[str, object] = {}
        result = nest_fields(data, "nested", {"x", "y"})
        assert result == {"nested": {}}

    def test_field_name_overwrites_existing_key(self) -> None:
        """If field_name already exists in data it gets overwritten."""
        data: dict[str, object] = {"nested": "old_value", "a": 1}
        result = nest_fields(data, "nested", {"a"})
        assert result["nested"] == {"a": 1}

    def test_large_number_of_keys(self) -> None:
        n = 5000
        data: dict[str, object] = {f"k{i}": i for i in range(n)}
        keys = {f"k{i}" for i in range(n)}
        result = nest_fields(data, "bulk", keys)
        assert len(result["bulk"]) == n  # type: ignore[arg-type,unused-ignore]  # dict value type is Any at runtime
        # Only 'bulk' remains at top level
        assert set(result.keys()) == {"bulk"}

    def test_keys_with_empty_string(self) -> None:
        data: dict[str, object] = {"": "empty_key", "normal": 1}
        result = nest_fields(data, "nested", {""})
        assert result == {"normal": 1, "nested": {"": "empty_key"}}

    def test_keys_with_special_characters(self) -> None:
        data: dict[str, object] = {"a.b": 1, "c d": 2, "e\nf": 3, "ok": 4}
        result = nest_fields(data, "nested", {"a.b", "c d", "e\nf"})
        assert result == {"ok": 4, "nested": {"a.b": 1, "c d": 2, "e\nf": 3}}

    def test_mutates_original_dict(self) -> None:
        """nest_fields pops from the original dict -- verify mutation."""
        data: dict[str, object] = {"a": 1, "b": 2}
        nest_fields(data, "nested", {"a"})
        assert "a" not in data
        assert "nested" in data


# ---------------------------------------------------------------------------
# discriminator_map edge cases
# ---------------------------------------------------------------------------


class TestDiscriminatorMapEdgeCases:
    def test_single_variant_union(self) -> None:
        """Union with only one type arg still produces a mapping."""
        # OnlyAlpha.data is just Alpha (not a real union), get_args returns ()
        # so the result should be empty.
        mapping = discriminator_map(OnlyAlpha, "data", "behavior")
        # Single type -> get_args gives empty tuple -> empty mapping
        assert mapping == {}

    def test_field_not_on_parent_raises_key_error(self) -> None:
        with pytest.raises(KeyError):
            discriminator_map(TwoWay, "nonexistent_field", "behavior")

    def test_subclass_without_discriminator_is_skipped(self) -> None:
        """NoDiscrimField has no 'behavior' -- it should be silently skipped."""
        mapping = discriminator_map(MixedParent, "data", "behavior")
        assert mapping == {"alpha": Alpha}

    def test_three_way_mapping(self) -> None:
        mapping = discriminator_map(ThreeWay, "data", "behavior")
        assert mapping == {"alpha": Alpha, "beta": Beta, "gamma": Gamma}


# ---------------------------------------------------------------------------
# format_discriminated edge cases
# ---------------------------------------------------------------------------


class TestFormatDiscriminatedEdgeCases:
    def test_unknown_discriminator_value_raises_key_error(self) -> None:
        data: dict[str, object] = {"tag": "t", "behavior": "unknown"}
        with pytest.raises(KeyError):
            format_discriminated(data, TwoWay, field_name="data", discriminator="behavior")

    def test_empty_data_dict_raises_key_error(self) -> None:
        """Missing discriminator key causes KeyError."""
        data: dict[str, object] = {}
        with pytest.raises(KeyError):
            format_discriminated(data, TwoWay, field_name="data", discriminator="behavior")

    def test_three_way_discriminated_alpha(self) -> None:
        data: dict[str, object] = {"tag": "t", "behavior": "alpha", "x": 1.0, "y": 2.0, "z": 3.0}
        result = format_discriminated(data, ThreeWay, field_name="data", discriminator="behavior")
        assert result == {"tag": "t", "data": {"behavior": "alpha", "x": 1.0}}

    def test_three_way_discriminated_gamma(self) -> None:
        data: dict[str, object] = {"tag": "t", "behavior": "gamma", "x": 1.0, "y": 2.0, "z": 3.0}
        result = format_discriminated(data, ThreeWay, field_name="data", discriminator="behavior")
        assert result == {"tag": "t", "data": {"behavior": "gamma", "z": 3.0}}

    def test_three_way_removes_other_subtypes(self) -> None:
        """When selecting beta, alpha's x and gamma's z should be removed."""
        data: dict[str, object] = {"tag": "t", "behavior": "beta", "x": 1.0, "y": 2.0, "z": 3.0}
        result = format_discriminated(data, ThreeWay, field_name="data", discriminator="behavior")
        assert "x" not in result
        assert "z" not in result
        assert result["data"] == {"behavior": "beta", "y": 2.0}

    def test_extra_fields_not_in_any_subtype(self) -> None:
        """Extra keys that belong to neither subtype stay at top level."""
        data: dict[str, object] = {
            "tag": "t",
            "behavior": "alpha",
            "x": 1.0,
            "y": 2.0,
            "extra_col": "surprise",
        }
        result = format_discriminated(data, TwoWay, field_name="data", discriminator="behavior")
        assert result["extra_col"] == "surprise"
        assert result["data"] == {"behavior": "alpha", "x": 1.0}

    def test_overlapping_shared_field_goes_to_active(self) -> None:
        """When subtypes share a field, the active subtype claims it."""
        data: dict[str, object] = {
            "tag": "t",
            "behavior": "oa",
            "shared": 99.0,
            "only_a": 1.0,
            "only_b": 2.0,
        }
        result = format_discriminated(data, OverlapParent, field_name="data", discriminator="behavior")
        nested = result["data"]
        assert nested == {"behavior": "oa", "shared": 99.0, "only_a": 1.0}
        # only_b should be removed from top level
        assert "only_b" not in result

    def test_overlapping_other_side(self) -> None:
        """Selecting OverlapB: shared goes to nested, only_a removed."""
        data: dict[str, object] = {
            "tag": "t",
            "behavior": "ob",
            "shared": 42.0,
            "only_a": 1.0,
            "only_b": 2.0,
        }
        result = format_discriminated(data, OverlapParent, field_name="data", discriminator="behavior")
        assert result["data"] == {"behavior": "ob", "shared": 42.0, "only_b": 2.0}
        assert "only_a" not in result


# ---------------------------------------------------------------------------
# _data_fields edge cases
# ---------------------------------------------------------------------------


class TestDataFieldsEdgeCases:
    def test_regular_class_raises_type_error(self) -> None:
        class NotADataclass:
            x: int = 0

        with pytest.raises(TypeError, match="Expected a dataclass type"):
            _data_fields(NotADataclass)

    def test_stdlib_dataclass(self) -> None:
        @dataclasses.dataclass
        class StdLib:
            a: int = 0
            b: str = ""

        fields = _data_fields(StdLib)
        assert fields == {"a", "b"}

    def test_pydantic_dataclass(self) -> None:
        @dataclass_pydantic(kw_only=True, slots=True)
        class PydanticDC:
            foo: int = 0
            bar: str = ""

        fields = _data_fields(PydanticDC)
        assert fields == {"foo", "bar"}
