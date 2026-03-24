"""Edge-case unit tests for the SQLDataclass model module."""

from datetime import time
from typing import Literal

import pytest
from pydantic import ValidationError
from sqlalchemy import LargeBinary, String, Time

from sqldataclass import Field, Relationship, SQLDataclass
from sqldataclass.model import _default_tablename, _get_pk_columns, _python_type_to_sa
from sqldataclass.write import flatten_for_table

# ---------------------------------------------------------------------------
# Type mapping gaps
# ---------------------------------------------------------------------------


class TestTypeMappingEdgeCases:
    """Tests for Python -> SQLAlchemy type mapping edge cases."""

    def test_bytes_maps_to_large_binary(self) -> None:
        assert _python_type_to_sa(bytes) is LargeBinary

    def test_time_maps_to_time(self) -> None:
        assert _python_type_to_sa(time) is Time

    def test_literal_maps_to_string(self) -> None:
        assert _python_type_to_sa(Literal["a", "b"]) is String

    def test_unmappable_union_raises_type_error(self) -> None:
        with pytest.raises(TypeError, match="Cannot map Python type"):
            _python_type_to_sa(int | str)


# ---------------------------------------------------------------------------
# Field parameter gaps
# ---------------------------------------------------------------------------


class ServerDefaultModel(SQLDataclass, table=True):
    __tablename__ = "server_default_model"
    id: int | None = Field(default=None, primary_key=True)
    created: str = Field(default="", server_default="now()")


class ColumnKwargsModel(SQLDataclass, table=True):
    __tablename__ = "column_kwargs_model"
    id: int | None = Field(default=None, primary_key=True)
    note: str = Field(default="", sa_column_kwargs={"comment": "test"})


class NullableOverrideModel(SQLDataclass, table=True):
    __tablename__ = "nullable_override_model"
    id: int | None = Field(default=None, primary_key=True)
    force_nullable: int = Field(default=0, nullable=True)
    force_not_null: int | None = Field(default=None, nullable=False)


class TestFieldParameterGaps:
    """Tests for Field() parameter edge cases."""

    def test_server_default(self) -> None:
        cols = {c.name: c for c in ServerDefaultModel.__table__.columns}
        assert cols["created"].server_default is not None
        assert str(cols["created"].server_default.arg) == "now()"  # type: ignore[attr-defined]

    def test_sa_column_kwargs_comment(self) -> None:
        cols = {c.name: c for c in ColumnKwargsModel.__table__.columns}
        assert cols["note"].comment == "test"

    def test_nullable_true_on_non_optional(self) -> None:
        cols = {c.name: c for c in NullableOverrideModel.__table__.columns}
        assert cols["force_nullable"].nullable is True

    def test_nullable_false_on_optional(self) -> None:
        cols = {c.name: c for c in NullableOverrideModel.__table__.columns}
        assert cols["force_not_null"].nullable is False

    def test_default_factory(self) -> None:
        class FactoryModel(SQLDataclass):
            tags: list[str] = Field(default_factory=list)

        obj = FactoryModel()
        assert obj.tags == []
        # Ensure independent instances
        obj2 = FactoryModel()
        assert obj.tags is not obj2.tags

    def test_alias(self) -> None:
        class AliasModel(SQLDataclass):
            real_name: str = Field(default="x", alias="alias_name")

        obj = AliasModel(alias_name="hello")
        assert obj.real_name == "hello"

    def test_pattern_rejects_invalid(self) -> None:
        class PatternModel(SQLDataclass):
            code: str = Field(default="abc", pattern=r"^[a-z]+$")

        with pytest.raises(ValidationError):
            PatternModel(code="ABC123")

    def test_pattern_accepts_valid(self) -> None:
        class PatternModel2(SQLDataclass):
            code: str = Field(default="abc", pattern=r"^[a-z]+$")

        obj = PatternModel2(code="hello")
        assert obj.code == "hello"

    def test_combined_ge_le_validators_reject(self) -> None:
        class RangeModel(SQLDataclass):
            score: int = Field(default=50, ge=0, le=100)

        with pytest.raises(ValidationError):
            RangeModel(score=101)
        with pytest.raises(ValidationError):
            RangeModel(score=-1)

    def test_combined_ge_le_validators_accept(self) -> None:
        class RangeModel2(SQLDataclass):
            score: int = Field(default=50, ge=0, le=100)

        obj = RangeModel2(score=0)
        assert obj.score == 0
        obj2 = RangeModel2(score=100)
        assert obj2.score == 100


# ---------------------------------------------------------------------------
# Relationship parameter gaps
# ---------------------------------------------------------------------------


class TestRelationshipParameterGaps:
    """Tests for Relationship() parameter edge cases."""

    def test_relationship_explicit_default_list(self) -> None:
        class ChildStub(SQLDataclass, table=True):
            __tablename__ = "rel_child_stub"
            id: int | None = Field(default=None, primary_key=True)
            name: str = ""

        class ParentWithDefault(SQLDataclass):
            children: list[ChildStub] = Relationship(default=[])

        obj = ParentWithDefault()
        assert obj.children == []

    def test_relationship_discriminator_three_variants(self) -> None:
        class VariantA(SQLDataclass, table=True):
            __tablename__ = "variant_a"
            id: int | None = Field(default=None, primary_key=True)
            kind: Literal["a"] = "a"

        class VariantB(SQLDataclass, table=True):
            __tablename__ = "variant_b"
            id: int | None = Field(default=None, primary_key=True)
            kind: Literal["b"] = "b"

        class VariantC(SQLDataclass, table=True):
            __tablename__ = "variant_c"
            id: int | None = Field(default=None, primary_key=True)
            kind: Literal["c"] = "c"

        class HostModel(SQLDataclass, table=True):
            __tablename__ = "host_model_3v"
            id: int | None = Field(default=None, primary_key=True)
            kind: str = ""
            data: VariantA | VariantB | VariantC = Relationship(discriminator="kind")

        rels = HostModel.__relationships__
        assert "data" in rels
        assert rels["data"].kind == "discriminated"
        assert len(rels["data"].target_types) == 3


# ---------------------------------------------------------------------------
# _default_tablename edge cases
# ---------------------------------------------------------------------------


class TestDefaultTablenameEdgeCases:
    """Tests for _default_tablename with unusual inputs."""

    def test_acronym_followed_by_word(self) -> None:
        assert _default_tablename("HTTPServer") == "http_server"

    def test_digit_in_name(self) -> None:
        assert _default_tablename("Model2Factory") == "model2_factory"

    def test_single_letter(self) -> None:
        assert _default_tablename("X") == "x"

    def test_already_lowercase(self) -> None:
        assert _default_tablename("hero") == "hero"


# ---------------------------------------------------------------------------
# Composite PK raises error for collection relationships
# ---------------------------------------------------------------------------


class TestCompositePKError:
    """Test that composite PKs work with _get_pk_columns."""

    def test_composite_pk_returns_multiple_columns(self) -> None:
        class CompositePK(SQLDataclass, table=True):
            __tablename__ = "composite_pk_model"
            a: int = Field(default=0, primary_key=True)
            b: int = Field(default=0, primary_key=True)

        pk_cols = _get_pk_columns(CompositePK)
        assert len(pk_cols) == 2
        assert {c.name for c in pk_cols} == {"a", "b"}


# ---------------------------------------------------------------------------
# flatten_for_table edge cases
# ---------------------------------------------------------------------------


class TestFlattenForTable:
    """Tests for flatten_for_table edge cases."""

    def test_excludes_relationship_fields(self) -> None:
        class RelTarget(SQLDataclass, table=True):
            __tablename__ = "flatten_rel_target"
            id: int | None = Field(default=None, primary_key=True)

        class WithRel(SQLDataclass, table=True):
            __tablename__ = "flatten_with_rel"
            id: int | None = Field(default=None, primary_key=True)
            target_id: int = Field(default=0, foreign_key="flatten_rel_target.id")
            target: RelTarget | None = Relationship()

        obj = WithRel(id=1, target_id=5, target=None)
        flat = flatten_for_table(obj)
        assert "target" not in flat
        assert flat["id"] == 1
        assert flat["target_id"] == 5

    def test_excludes_list_values(self) -> None:
        class Parent(SQLDataclass):
            id: int = 1
            tags: list[str] = Field(default_factory=list)

        obj = Parent(id=1, tags=["a", "b"])
        flat = flatten_for_table(obj)
        assert "tags" not in flat
        assert flat["id"] == 1


# ---------------------------------------------------------------------------
# __init__.py exports are importable
# ---------------------------------------------------------------------------


class TestAllExportsImportable:
    """Verify every name in __all__ is importable."""

    def test_all_exports(self) -> None:
        import sqldataclass as _sqldataclass  # noqa: PLC0415

        for name in _sqldataclass.__all__:
            assert hasattr(_sqldataclass, name), f"{name} listed in __all__ but not importable"
