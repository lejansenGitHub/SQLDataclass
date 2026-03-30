"""Unit tests for FillValueIfNone validator."""

from __future__ import annotations

from typing import Annotated

import pytest

from sqldataclass import FillValueIfNone, SQLDataclass


class TestFillValueIfNone:
    def test_with_default(self) -> None:
        class Config(SQLDataclass):
            name: Annotated[str, FillValueIfNone(default="unnamed")]

        c = Config(name=None)  # type: ignore[arg-type]
        assert c.name == "unnamed"

    def test_preserves_non_none(self) -> None:
        class Config(SQLDataclass):
            name: Annotated[str, FillValueIfNone(default="unnamed")]

        c = Config(name="Alice")
        assert c.name == "Alice"

    def test_with_default_factory(self) -> None:
        class Config(SQLDataclass):
            tags: Annotated[list[str], FillValueIfNone(default_factory=list)]

        c = Config(tags=None)  # type: ignore[arg-type]
        assert c.tags == []

    def test_raises_without_default_or_factory(self) -> None:
        with pytest.raises(NotImplementedError, match="Either default or default_factory"):
            FillValueIfNone()
