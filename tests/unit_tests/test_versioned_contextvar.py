"""Tests for the ``__migration_contextvar__`` class attribute.

Versioned models default to SD's built-in migration contextvar. Hosts that
already have their own contextvar (set by their own ``LegacyParent.load()``)
override SD's default by declaring ``__migration_contextvar__`` at class-body
scope:

    class Order(SQLDataclass, versioned=True):
        __migration_contextvar__ = HOST_DO_MIGRATION
        ORDER_VERSION: int = Field(default=2)

SD's validator on ``Order`` reads ``HOST_DO_MIGRATION``; when the host sets
that contextvar (e.g. from inside its own ``load()``), nested ``Order``
instances are migrated correctly.
"""

from __future__ import annotations

from contextvars import ContextVar
from typing import Any

import pytest

from sqldataclass import Field, SQLDataclass

HOST_DO_MIGRATION: ContextVar[bool] = ContextVar("HOST_DO_MIGRATION", default=False)


class Order(SQLDataclass, versioned=True):
    __migration_contextvar__ = HOST_DO_MIGRATION
    ORDER_VERSION: int = Field(default=2)
    order_id: int
    label: str = ""

    @classmethod
    def migrate(cls, obj: dict[str, Any]) -> dict[str, Any]:
        version = obj.get("ORDER_VERSION", 1)
        if version < 2:
            obj["label"] = obj.pop("legacy_label", "")
            obj["ORDER_VERSION"] = 2
        return obj


def test_class_attribute_overrides_builtin_contextvar() -> None:
    """A class that declares __migration_contextvar__ binds to that one
    instead of SD's __DO_MIGRATION__."""
    # --- Assert ---
    assert Order.__migration_contextvar__ is HOST_DO_MIGRATION


def test_load_via_external_contextvar_triggers_migration() -> None:
    """cls.load(data) toggles the bound ContextVar and runs migrate()."""
    obj = Order.load({"order_id": 7, "legacy_label": "old", "ORDER_VERSION": 1})

    # --- Assert ---
    # legacy_label was rewritten to label by migrate(), version bumped to 2.
    assert obj.order_id == 7
    assert obj.label == "old"
    assert obj.ORDER_VERSION == 2


def test_direct_construction_without_contextvar_skips_migration() -> None:
    """Direct cls(**data) without toggling the contextvar does NOT migrate.

    The data must already be in current shape for direct construction;
    legacy keys are extras and pydantic's extra='forbid' rejects them.
    """
    obj = Order(order_id=8, label="fresh", ORDER_VERSION=2)

    # --- Assert ---
    assert obj.order_id == 8
    assert obj.label == "fresh"
    assert obj.ORDER_VERSION == 2


def test_host_can_drive_migration_directly() -> None:
    """Setting the host contextvar manually causes nested cls(**data) to migrate.

    This is the bridging scenario: a host LegacyParent.load() sets the
    contextvar before pydantic constructs the nested SQLDataclass field.
    """
    token = HOST_DO_MIGRATION.set(True)
    try:
        # Construction with legacy key works because the bound contextvar is set.
        obj = Order(order_id=9, legacy_label="x", ORDER_VERSION=1)

        # --- Assert ---
        assert obj.label == "x"
        assert obj.ORDER_VERSION == 2
    finally:
        HOST_DO_MIGRATION.reset(token)


def test_versioned_true_without_override_uses_sd_internal_contextvar() -> None:
    """versioned=True with no __migration_contextvar__ declared defaults
    to SD's built-in contextvar — the backward-compatible path."""
    from sqldataclass.versioning import __DO_MIGRATION__

    class Hero(SQLDataclass, versioned=True):
        HERO_VERSION: int = Field(default=1)
        name: str = ""

    # --- Assert ---
    assert Hero.__migration_contextvar__ is __DO_MIGRATION__


def test_two_classes_with_distinct_contextvars_dont_interfere() -> None:
    """Each versioned model honors its own contextvar; setting one
    doesn't trigger the other."""
    cv_a: ContextVar[bool] = ContextVar("cv_a", default=False)
    cv_b: ContextVar[bool] = ContextVar("cv_b", default=False)

    class AmpA(SQLDataclass, versioned=True):
        __migration_contextvar__ = cv_a
        AMP_A_VERSION: int = Field(default=1)
        value: int

        @classmethod
        def migrate(cls, obj: dict[str, Any]) -> dict[str, Any]:
            obj["value"] = obj.get("value", -1) + 1000  # marker for migration
            return obj

    class AmpB(SQLDataclass, versioned=True):
        __migration_contextvar__ = cv_b
        AMP_B_VERSION: int = Field(default=1)
        value: int

        @classmethod
        def migrate(cls, obj: dict[str, Any]) -> dict[str, Any]:
            obj["value"] = obj.get("value", -1) + 2000
            return obj

    # Set only cv_a → only AmpA.migrate runs when AmpA constructs; AmpB unaffected.
    token_a = cv_a.set(True)
    try:
        a = AmpA(value=5)
        b = AmpB(value=5)
    finally:
        cv_a.reset(token_a)

    # --- Assert ---
    assert a.value == 1005  # migrate ran for AmpA
    assert b.value == 5  # AmpB's contextvar was not set, no migration


def test_subclass_can_override_parent_contextvar() -> None:
    """A subclass declaring its own __migration_contextvar__ uses that one
    instead of the parent's, even if both are versioned. The metaclass runs
    once per class, so each gets its own bound validator."""
    parent_cv: ContextVar[bool] = ContextVar("subclass_parent_cv", default=False)
    child_cv: ContextVar[bool] = ContextVar("subclass_child_cv", default=False)

    class Parent(SQLDataclass, versioned=True):
        __migration_contextvar__ = parent_cv
        PARENT_VERSION: int = Field(default=1)
        name: str

        @classmethod
        def migrate(cls, obj: dict[str, Any]) -> dict[str, Any]:
            obj["name"] = "PARENT-migrated"
            return obj

    class Child(Parent, versioned=True):
        __migration_contextvar__ = child_cv
        CHILD_VERSION: int = Field(default=1)

        @classmethod
        def migrate(cls, obj: dict[str, Any]) -> dict[str, Any]:
            obj["name"] = "CHILD-migrated"
            return obj

    # Set only child_cv — Child.migrate runs.
    token = child_cv.set(True)
    try:
        c = Child(name="orig")
    finally:
        child_cv.reset(token)

    # Set only parent_cv — Child must NOT be migrated (it listens to child_cv).
    token = parent_cv.set(True)
    try:
        c2 = Child(name="other")
    finally:
        parent_cv.reset(token)

    # --- Assert ---
    assert Parent.__migration_contextvar__ is parent_cv
    assert Child.__migration_contextvar__ is child_cv
    assert c.name == "CHILD-migrated"
    assert c2.name == "other"  # parent_cv set, but Child ignores it


def test_non_contextvar_override_is_rejected() -> None:
    """Setting __migration_contextvar__ to something that isn't a ContextVar
    raises a clear TypeError at class construction."""
    # --- Assert ---
    with pytest.raises(TypeError, match="must be a ContextVar"):

        class Broken(SQLDataclass, versioned=True):  # ad-hoc test class
            __migration_contextvar__ = "not a contextvar"  # type: ignore[assignment]  # deliberate misuse
            BROKEN_VERSION: int = Field(default=1)
            name: str = ""
