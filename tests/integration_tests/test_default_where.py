"""Tests for #1: ``__default_where__`` class attribute applied by convenience methods.

A class that declares ``__default_where__`` gets it AND-combined into the
``where`` passed to ``load_all`` / ``load_one`` / ``update`` / ``delete`` /
``select``. The canonical use case is the soft-delete pattern:

    class Post(SQLDataclass, table=True):
        __default_where__ = lambda: Post.c.deprecated.is_(False)
        ...

Reads automatically exclude deprecated rows. Pass ``apply_default_where=False``
to override per call.

Uses sqlite-in-memory and SQLDataclass.bind so the tests are fast and don't
need Postgres.
"""

from __future__ import annotations

from typing import Any

import pytest
from sqlalchemy import create_engine

import sqldataclass.model as _model
from sqldataclass import Field, SQLDataclass


class Post(SQLDataclass, table=True):
    __tablename__ = "dw_posts"
    __default_where__ = lambda: Post.c.deprecated.is_(False)  # noqa: E731  # lambda needed for forward-reference
    id: int | None = Field(default=None, primary_key=True)
    title: str
    deprecated: bool = False


class Order(SQLDataclass, table=True):
    """Same shape as Post but with __default_where__ as a static expression."""

    __tablename__ = "dw_orders"
    id: int | None = Field(default=None, primary_key=True)
    label: str
    archived: bool = False


# Static expression — assigned after class body so the reference resolves.
Order.__default_where__ = Order.c.archived.is_(False)


@pytest.fixture
def bound_engine() -> Any:
    engine = create_engine("sqlite:///:memory:")
    SQLDataclass.metadata.create_all(engine)
    SQLDataclass.bind(engine)
    yield engine
    _model._BOUND_ENGINE = None


def _seed_posts() -> None:
    Post(title="active-a", deprecated=False).insert()
    Post(title="active-b", deprecated=False).insert()
    Post(title="old-c", deprecated=True).insert()


def test_load_all_excludes_deprecated_by_default(bound_engine: Any) -> None:
    """A class with __default_where__ filtering on deprecated=False excludes
    rows where deprecated is True."""
    _seed_posts()
    titles = sorted(p.title for p in Post.load_all())

    # --- Assert ---
    assert titles == ["active-a", "active-b"]


def test_load_all_user_where_combines_with_default(bound_engine: Any) -> None:
    """A caller-supplied where AND-combines with __default_where__ — both
    constraints must be satisfied."""
    _seed_posts()
    posts = Post.load_all(where=Post.c.title == "active-a")

    # --- Assert ---
    assert [p.title for p in posts] == ["active-a"]


def test_apply_default_where_false_returns_everything(bound_engine: Any) -> None:
    """Setting apply_default_where=False bypasses the filter — load_all sees
    all rows including the deprecated one."""
    _seed_posts()
    titles = sorted(p.title for p in Post.load_all(apply_default_where=False))

    # --- Assert ---
    assert titles == ["active-a", "active-b", "old-c"]


def test_load_one_applies_default_filter(bound_engine: Any) -> None:
    """load_one returns None when the only matching row is filtered out."""
    Post(id=1, title="hidden", deprecated=True).insert()
    found = Post.load_one(where=Post.c.id == 1)
    bypassed = Post.load_one(where=Post.c.id == 1, apply_default_where=False)

    # --- Assert ---
    assert found is None
    assert bypassed is not None
    assert bypassed.title == "hidden"


def test_update_respects_default_filter(bound_engine: Any) -> None:
    """update() with __default_where__ only touches non-deprecated rows."""
    _seed_posts()
    count = Post.update({"title": "renamed"})

    # --- Assert ---
    assert count == 2  # the deprecated row was untouched
    titles = sorted(p.title for p in Post.load_all(apply_default_where=False))
    assert titles == ["old-c", "renamed", "renamed"]


def test_update_apply_default_where_false_touches_everything(bound_engine: Any) -> None:
    """apply_default_where=False on update reaches deprecated rows too."""
    _seed_posts()
    count = Post.update({"title": "renamed-all"}, apply_default_where=False)

    # --- Assert ---
    assert count == 3


def test_delete_respects_default_filter(bound_engine: Any) -> None:
    """delete() honors __default_where__ — deprecated rows survive a
    default-filtered delete."""
    _seed_posts()
    count = Post.delete()

    # --- Assert ---
    assert count == 2
    remaining = Post.load_all(apply_default_where=False)
    assert [p.title for p in remaining] == ["old-c"]


def test_select_includes_default_where_in_compiled_sql(bound_engine: Any) -> None:
    """Post.select() returns a SELECT pre-filtered by __default_where__."""
    sql = str(Post.select().compile(compile_kwargs={"literal_binds": True}))

    # --- Assert ---
    assert "deprecated" in sql
    assert "WHERE" in sql.upper()


def test_static_default_where_expression_works(bound_engine: Any) -> None:
    """__default_where__ can also be a static SA expression (not a callable)."""
    Order(label="active").insert()
    Order(label="archived", archived=True).insert()
    labels = sorted(o.label for o in Order.load_all())

    # --- Assert ---
    assert labels == ["active"]


def test_no_default_where_means_no_filtering(bound_engine: Any) -> None:
    """A class without __default_where__ behaves exactly as before."""

    class Plain(SQLDataclass, table=True):
        __tablename__ = "dw_plain"
        id: int | None = Field(default=None, primary_key=True)
        name: str

    SQLDataclass.metadata.create_all(bound_engine)
    Plain(name="a").insert()
    Plain(name="b").insert()
    rows = Plain.load_all()

    # --- Assert ---
    assert len(rows) == 2
