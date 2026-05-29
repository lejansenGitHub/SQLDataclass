"""Tests for B4: overriding ``dump()`` on a SQLDataclass subclass.

Pydantic dataclasses with ``slots=True`` are incompatible with ``super()``
(see CPython issue #96249). The recommended pattern is to call the base
method explicitly: ``SQLDataclass.dump(self)``. This module pins the
working pattern so a future regression is caught immediately.
"""

from __future__ import annotations

from typing import Any

from sqldataclass import Field, SQLDataclass


def test_override_dump_with_explicit_base_call_works() -> None:
    """Calling ``SQLDataclass.dump(self)`` from an override succeeds and
    returns the augmented dict. This is the documented pattern; users
    who try ``super().dump()`` instead will hit a CPython slots/super
    interaction error at class-construction time."""

    class Foo(SQLDataclass, table=True):
        __tablename__ = "override_dump_explicit"
        id: int = Field(primary_key=True)
        name: str

        def dump(self) -> dict[str, Any]:
            data = SQLDataclass.dump(self)
            data["augmented"] = True
            return data

    inst = Foo(id=1, name="hello")
    dumped = inst.dump()

    # --- Assert ---
    assert dumped["id"] == 1
    assert dumped["name"] == "hello"
    assert dumped["augmented"] is True
