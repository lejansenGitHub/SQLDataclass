"""Unit tests for remove_unexpected_kwargs."""

from __future__ import annotations

from sqldataclass import SQLDataclass, SQLModel, remove_unexpected_kwargs


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
