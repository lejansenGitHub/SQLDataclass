"""Tests for per-model engine binding — v0.0.9 feature."""

from typing import Any

import pytest
from sqlalchemy import create_engine

import sqldataclass.model as _model
from sqldataclass import Field, SQLDataclass


class PmbHero(SQLDataclass, table=True):
    __tablename__ = "pmb_hero"
    id: int | None = Field(default=None, primary_key=True)
    name: str


class PmbVillain(SQLDataclass, table=True):
    __tablename__ = "pmb_villain"
    id: int | None = Field(default=None, primary_key=True)
    name: str


@pytest.fixture(autouse=True)
def _cleanup() -> Any:
    yield
    _model._BOUND_ENGINE = None
    # Remove per-model engines
    for attr in ("__sqldataclass_engine__",):
        for cls in (PmbHero, PmbVillain):
            if hasattr(cls, attr):
                delattr(cls, attr)


class TestPerModelBind:
    def test_per_model_bind_uses_separate_engines(self) -> None:
        """Two models bound to different SQLite DBs get separate data."""
        engine_a = create_engine("sqlite:///:memory:")
        engine_b = create_engine("sqlite:///:memory:")

        SQLDataclass.metadata.create_all(engine_a)
        SQLDataclass.metadata.create_all(engine_b)

        PmbHero.bind(engine_a)
        PmbVillain.bind(engine_b)

        PmbHero(name="Spider-Man").insert()
        PmbVillain(name="Thanos").insert()

        heroes = PmbHero.load_all()
        villains = PmbVillain.load_all()

        assert len(heroes) == 1
        assert heroes[0].name == "Spider-Man"
        assert len(villains) == 1
        assert villains[0].name == "Thanos"

        # Cross-check: heroes DB has no villains table data
        # (they're in different in-memory DBs)

    def test_per_model_overrides_global(self) -> None:
        """Per-model engine takes priority over global."""
        global_engine = create_engine("sqlite:///:memory:")
        model_engine = create_engine("sqlite:///:memory:")

        SQLDataclass.metadata.create_all(global_engine)
        SQLDataclass.metadata.create_all(model_engine)

        SQLDataclass.bind(global_engine)
        PmbHero.bind(model_engine)

        # Insert into model-specific engine
        PmbHero(name="ModelHero").insert()

        # Hero reads from its own engine
        heroes = PmbHero.load_all()
        assert len(heroes) == 1
        assert heroes[0].name == "ModelHero"

        # Villain uses global engine (no per-model bind)
        PmbVillain(name="GlobalVillain").insert()
        villains = PmbVillain.load_all()
        assert len(villains) == 1

    def test_global_bind_still_works(self) -> None:
        """Global bind works for models without per-model bind."""
        engine = create_engine("sqlite:///:memory:")
        SQLDataclass.metadata.create_all(engine)
        SQLDataclass.bind(engine)

        PmbHero(name="A").insert()
        assert len(PmbHero.load_all()) == 1

    def test_per_model_bind_with_update_delete(self) -> None:
        """update() and delete() use per-model engine."""
        engine = create_engine("sqlite:///:memory:")
        SQLDataclass.metadata.create_all(engine)
        PmbHero.bind(engine)

        PmbHero(name="A").insert()
        PmbHero.update({"name": "B"}, where=PmbHero.c.name == "A")
        hero = PmbHero.load_one(where=PmbHero.c.name == "B")
        assert hero is not None

        PmbHero.delete(where=PmbHero.c.name == "B")
        assert len(PmbHero.load_all()) == 0
