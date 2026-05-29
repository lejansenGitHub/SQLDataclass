"""Targeted unit tests for Phase B branch coverage edge cases.

Some branches in the metaclass / convenience helpers aren't naturally
exercised by the feature tests (e.g. a field declared with pydantic's
native ``Field`` instead of SD's). These tests pin the edge-case branches
so refactors don't silently drop the safety net.
"""

from __future__ import annotations

from pydantic import Field as PydanticField

from sqldataclass import Field, SQLDataclass


def test_pydantic_native_field_without_sa_info_is_tolerated() -> None:
    """A class that mixes SD ``Field`` with pydantic's native ``Field`` (no
    SAColumnInfo metadata) still builds; the native-pydantic field is treated
    as a plain dataclass field. Exercises the ``sa_info is None: continue``
    branch in the field-detection loop.
    """

    class WithNativeField(SQLDataclass, table=True):
        __tablename__ = "branches_with_native_field"
        id: int = Field(primary_key=True)
        # No SD metadata on this one — pydantic's Field is used directly.
        nickname: str = PydanticField(default="anon")

    column_names = [col.name for col in WithNativeField.__table__.columns]

    # --- Assert ---
    # The native pydantic field is still a column (it doesn't carry column=False),
    # but the SAColumnInfo metadata is absent so the field-detection branch hits
    # the `continue` path during non-column / server-managed classification.
    assert "id" in column_names
    assert "nickname" in column_names
