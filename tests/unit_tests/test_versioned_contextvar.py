"""Tests for B1: versioned= accepts a user-supplied ContextVar[bool].

Use case: a host system has its own migration contextvar (set by its own
``LegacyParent.load()``). When the host parent constructs a nested
SQLDataclass child via pydantic validation, the child's ``migrate()`` must
fire only when the host's contextvar is set. With ``versioned=True``, SD's
built-in contextvar is used; passing the host's ``ContextVar[bool]``
directly as ``versioned=`` bridges the two systems.
"""

from __future__ import annotations

from contextvars import ContextVar
from typing import Any

from sqldataclass import Field, SQLDataclass

HOST_DO_MIGRATION: ContextVar[bool] = ContextVar("HOST_DO_MIGRATION", default=False)


class Order(SQLDataclass, versioned=HOST_DO_MIGRATION):
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


def test_external_contextvar_bound_at_class_construction() -> None:
    """The class records the user-supplied ContextVar; SD's built-in is NOT used."""
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


def test_versioned_true_still_uses_sd_internal_contextvar() -> None:
    """versioned=True keeps the built-in contextvar for backward compat."""
    from sqldataclass.versioning import __DO_MIGRATION__

    class Hero(SQLDataclass, versioned=True):
        HERO_VERSION: int = Field(default=1)
        name: str = ""

    # --- Assert ---
    assert Hero.__migration_contextvar__ is __DO_MIGRATION__


def test_two_classes_with_distinct_contextvars_dont_interfere() -> None:
    """Each versioned model honors its own contextvar; setting one doesn't trigger the other."""
    cv_a: ContextVar[bool] = ContextVar("cv_a", default=False)
    cv_b: ContextVar[bool] = ContextVar("cv_b", default=False)

    class AmpA(SQLDataclass, versioned=cv_a):
        AMP_A_VERSION: int = Field(default=1)
        value: int

        @classmethod
        def migrate(cls, obj: dict[str, Any]) -> dict[str, Any]:
            obj["value"] = obj.get("value", -1) + 1000  # marker for migration
            return obj

    class AmpB(SQLDataclass, versioned=cv_b):
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
