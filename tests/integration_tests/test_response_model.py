"""Integration tests for response models (table=False child of table=True parent).

Verifies that response models coexist with their parent's table operations
and can be constructed from loaded DB data.
"""

from typing import Any

import pytest
from sqlalchemy import create_engine

import sqldataclass.model as _model
from sqldataclass import Field, SQLDataclass


class IntParent(SQLDataclass, table=True):
    __tablename__ = "int_response_parent"
    id: int | None = Field(default=None, primary_key=True)
    name: str = ""
    length: float = 0.0


class IntResponse(IntParent, table=False, exclude={"id"}):
    pass


class IntResponseUS(IntResponse):
    """Chained response model that overrides a field."""

    length: int = 0


@pytest.fixture
def bound_engine() -> Any:  # type: ignore[misc]  # fixture typing
    engine = create_engine("sqlite:///:memory:")
    SQLDataclass.metadata.create_all(engine)
    SQLDataclass.bind(engine)
    yield engine
    _model._BOUND_ENGINE = None  # type: ignore[attr-defined]  # test cleanup of private state


@pytest.mark.integration
def test_parent_table_ops_unaffected_by_response_model(bound_engine: Any) -> None:
    """
    Defining a response model child must not interfere with the parent's
    table operations — insert and load should work normally.
    """
    IntParent(name="line-1", length=3.5).insert()
    IntParent(name="line-2", length=7.0).insert()

    loaded = IntParent.load_all()
    assert len(loaded) == 2
    assert {row.name for row in loaded} == {"line-1", "line-2"}


@pytest.mark.integration
def test_construct_response_from_loaded_data(bound_engine: Any) -> None:
    """
    The typical use case: load rows from the parent table, then construct
    response model instances from the loaded data (simulating an API endpoint).
    """
    IntParent(name="cable-a", length=12.5).insert()

    parent = IntParent.load_one(where=IntParent.c.name == "cable-a")
    assert parent is not None

    response = IntResponse(name=parent.name, length=parent.length)
    assert response.name == "cable-a"
    assert response.length == 12.5
    assert not hasattr(response, "id")


@pytest.mark.integration
def test_construct_chained_response_with_conversion(bound_engine: Any) -> None:
    """
    A chained response model (US variant) can convert values during construction
    from parent data.
    """
    IntParent(name="cable-b", length=10.0).insert()

    parent = IntParent.load_one(where=IntParent.c.name == "cable-b")
    assert parent is not None

    response = IntResponseUS(name=parent.name, length=int(parent.length))
    assert response.length == 10
    assert isinstance(response.length, int)
