"""Tests for B2: dump() preserves the version field on versioned models.

Previously, ``dump()`` excluded all ``column=False`` fields — including the
version field on versioned models declared as ``Field(default=N, column=False)``.
The resulting dict had no version key. When loaded back via ``cls.load(data)``,
``do_migration`` defaulted the missing version to ``1`` (oldest) and ran the
full migration chain — typically crashing on legacy fields that no longer
exist in the current shape.

Fix: ``dump()`` keeps the version field for versioned models regardless of
``column=False``. ``do_migration``'s "missing = oldest" semantics is preserved
(forces explicit version declaration for legacy data).
"""

from __future__ import annotations

from typing import Any

from sqldataclass import Field, SQLDataclass


class VersionedRecord(SQLDataclass, versioned=True):
    VERSIONED_RECORD_VERSION: int = Field(default=3, column=False)
    record_id: int
    name: str

    @classmethod
    def migrate(cls, obj: dict[str, Any]) -> dict[str, Any]:
        # Trivial migration just to make round-trips observable; never expected
        # to fire on a fresh dump.
        return obj


def test_dump_preserves_version_field() -> None:
    """A versioned model's dump() output includes the version key even though
    the field is column=False — so load() can read it back and skip migration."""
    instance = VersionedRecord(record_id=1, name="alpha")
    dumped = instance.dump()

    # --- Assert ---
    assert "VERSIONED_RECORD_VERSION" in dumped
    assert dumped["VERSIONED_RECORD_VERSION"] == 3
    assert dumped["record_id"] == 1
    assert dumped["name"] == "alpha"


def test_dump_load_round_trip_skips_migration() -> None:
    """dump() → load() round-trips without triggering migration —
    the version is carried through, so do_migration sees current schema and
    short-circuits."""
    original = VersionedRecord(record_id=42, name="bravo")
    dumped = original.dump()
    reloaded = VersionedRecord.load(dumped)

    # --- Assert ---
    assert reloaded.record_id == original.record_id
    assert reloaded.name == original.name
    assert reloaded.VERSIONED_RECORD_VERSION == 3


def test_dump_on_non_versioned_still_excludes_column_false() -> None:
    """The version-field carve-out applies only to versioned models;
    column=False on a non-versioned model still drops fields from dump()."""

    class Plain(SQLDataclass):
        record_id: int
        cached: str = Field(default="", column=False)

    instance = Plain(record_id=5, cached="ignored")
    dumped = instance.dump()

    # --- Assert ---
    assert dumped == {"record_id": 5}
    assert "cached" not in dumped


def test_missing_version_in_input_treated_as_oldest() -> None:
    """If the caller load()s a dict with no version key, do_migration defaults
    to oldest and runs migrate(). This is the intentional "explicit version"
    contract — fresh dumps already carry the version, so only legacy callers
    can land here, and they want the full migration chain."""

    class Tracked(SQLDataclass, versioned=True):
        TRACKED_VERSION: int = Field(default=2, column=False)
        record_id: int
        legacy_name: str | None = None
        name: str = ""

        @classmethod
        def migrate(cls, obj: dict[str, Any]) -> dict[str, Any]:
            version = obj.get("TRACKED_VERSION", 1)
            if version < 2:
                obj["name"] = obj.pop("legacy_name", "")
                obj["TRACKED_VERSION"] = 2
            return obj

    # load() with no version key → migrate runs from v1.
    reloaded = Tracked.load({"record_id": 10, "legacy_name": "old"})

    # --- Assert ---
    assert reloaded.name == "old"
    assert reloaded.TRACKED_VERSION == 2
