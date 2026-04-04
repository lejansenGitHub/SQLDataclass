"""Tests for load_all(limit=, offset=) pagination — v0.0.7 feature."""

from typing import Any

import pytest
from sqlalchemy import create_engine

import sqldataclass.model as _model
from sqldataclass import Field, SQLDataclass


class PagHero(SQLDataclass, table=True):
    __tablename__ = "pag_hero"
    id: int | None = Field(default=None, primary_key=True)
    name: str
    rank: int = 0


@pytest.fixture
def bound_engine() -> Any:
    engine = create_engine("sqlite:///:memory:")
    SQLDataclass.metadata.create_all(engine)
    SQLDataclass.bind(engine)
    # Seed 10 heroes
    for i in range(1, 11):
        PagHero(name=f"Hero_{i:02d}", rank=i).insert()
    yield engine
    _model._BOUND_ENGINE = None


class TestPagination:
    def test_limit_returns_n_rows(self, bound_engine: Any) -> None:
        heroes = PagHero.load_all(limit=3)
        assert len(heroes) == 3

    def test_offset_skips_rows(self, bound_engine: Any) -> None:
        all_heroes = PagHero.load_all(order_by=PagHero.c.rank)
        offset_heroes = PagHero.load_all(order_by=PagHero.c.rank, offset=5)
        assert len(offset_heroes) == 5
        assert offset_heroes[0].rank == all_heroes[5].rank

    def test_limit_and_offset_together(self, bound_engine: Any) -> None:
        heroes = PagHero.load_all(order_by=PagHero.c.rank, limit=3, offset=2)
        assert len(heroes) == 3
        assert heroes[0].rank == 3
        assert heroes[2].rank == 5

    def test_limit_larger_than_total(self, bound_engine: Any) -> None:
        heroes = PagHero.load_all(limit=100)
        assert len(heroes) == 10

    def test_offset_beyond_total(self, bound_engine: Any) -> None:
        heroes = PagHero.load_all(offset=100)
        assert len(heroes) == 0

    def test_limit_with_where(self, bound_engine: Any) -> None:
        heroes = PagHero.load_all(where=PagHero.c.rank > 5, limit=2, order_by=PagHero.c.rank)
        assert len(heroes) == 2
        assert heroes[0].rank == 6
        assert heroes[1].rank == 7

    def test_limit_zero(self, bound_engine: Any) -> None:
        heroes = PagHero.load_all(limit=0)
        assert len(heroes) == 0

    def test_no_limit_no_offset_returns_all(self, bound_engine: Any) -> None:
        heroes = PagHero.load_all()
        assert len(heroes) == 10

    def test_pagination_with_explicit_conn(self, bound_engine: Any) -> None:
        with bound_engine.connect() as conn:
            heroes = PagHero.load_all(conn, limit=2)
            assert len(heroes) == 2
