"""Unit tests for remove_unexpected_kwargs and migrate_legacy_version_strings."""

from __future__ import annotations

from sqldataclass import SQLDataclass, SQLModel, remove_unexpected_kwargs
from sqldataclass.utils import migrate_legacy_version_strings


class TestRemoveUnexpectedKwargs:
    def test_removes_extra_keys_dataclass(self) -> None:
        class Hero(SQLDataclass):
            name: str
            age: int = 0

        data = {"name": "Alice", "age": 25, "extra": "remove_me"}
        remove_unexpected_kwargs(data, Hero)
        assert "extra" not in data
        assert data["name"] == "Alice"

    def test_removes_extra_keys_sqlmodel(self) -> None:
        class Player(SQLModel):
            name: str
            score: float = 0.0

        data = {"name": "Bob", "score": 9.5, "junk": True}
        remove_unexpected_kwargs(data, Player)
        assert "junk" not in data
        assert data["name"] == "Bob"

    def test_keeps_valid_keys(self) -> None:
        class Hero(SQLDataclass):
            name: str
            age: int = 0

        data = {"name": "Alice", "age": 25}
        remove_unexpected_kwargs(data, Hero)
        assert data == {"name": "Alice", "age": 25}


class TestMigrateLegacyVersionStrings:
    def test_renames_legacy_field(self) -> None:
        class Address(SQLDataclass, versioned=True):
            ADDRESS_VERSION: int = 1
            street: str = ""

        data = {"ADDRESSSCHEMA_VERSION": 1, "street": "Main St"}
        migrate_legacy_version_strings(data, Address)
        assert "ADDRESS_VERSION" in data
        assert "ADDRESSSCHEMA_VERSION" not in data
        assert data["ADDRESS_VERSION"] == 1

    def test_no_op_without_legacy_key(self) -> None:
        class Address(SQLDataclass, versioned=True):
            ADDRESS_VERSION: int = 1
            street: str = ""

        data = {"ADDRESS_VERSION": 1, "street": "Main St"}
        migrate_legacy_version_strings(data, Address)
        assert data == {"ADDRESS_VERSION": 1, "street": "Main St"}
