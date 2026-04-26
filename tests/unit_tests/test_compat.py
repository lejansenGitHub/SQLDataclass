"""Tests for sqldataclass.compat — from_psycopg bridge."""

from __future__ import annotations

import sys
from types import ModuleType

import pytest

from sqldataclass.compat import _extract_connection, from_psycopg

# ---------------------------------------------------------------------------
# Fake psycopg types for isinstance checks without real psycopg
# ---------------------------------------------------------------------------


class _FakePsycopgConnection:
    pass


class _FakePsycopgCursor:
    def __init__(self, connection: _FakePsycopgConnection) -> None:
        self.connection = connection


def _install_fake_psycopg() -> ModuleType:
    """Register a fake psycopg module so isinstance checks work."""
    mock_module = ModuleType("psycopg")
    mock_module.Cursor = _FakePsycopgCursor  # type: ignore[attr-defined]  # dynamic test module setup
    mock_module.Connection = _FakePsycopgConnection  # type: ignore[attr-defined]  # dynamic test module setup
    sys.modules["psycopg"] = mock_module
    return mock_module


def _uninstall_fake_psycopg(original: ModuleType | None) -> None:
    if original is not None:
        sys.modules["psycopg"] = original
    else:
        sys.modules.pop("psycopg", None)


# ---------------------------------------------------------------------------
# _extract_connection
# ---------------------------------------------------------------------------


def test_extract_connection_from_cursor() -> None:
    """Cursor's .connection attribute is returned when a cursor is passed."""
    original = sys.modules.get("psycopg")
    _install_fake_psycopg()
    try:
        conn = _FakePsycopgConnection()
        cursor = _FakePsycopgCursor(conn)

        # --- Assert ---
        result = _extract_connection(cursor)
        assert result is conn
    finally:
        _uninstall_fake_psycopg(original)


def test_extract_connection_from_connection() -> None:
    """A psycopg Connection is returned as-is."""
    original = sys.modules.get("psycopg")
    _install_fake_psycopg()
    try:
        conn = _FakePsycopgConnection()

        # --- Assert ---
        result = _extract_connection(conn)
        assert result is conn
    finally:
        _uninstall_fake_psycopg(original)


def test_extract_connection_rejects_invalid_type() -> None:
    """Non-psycopg objects raise TypeError with a descriptive message."""
    original = sys.modules.get("psycopg")
    _install_fake_psycopg()
    try:
        # --- Assert ---
        with pytest.raises(TypeError, match="Expected a psycopg Connection or Cursor"):
            _extract_connection("not a cursor")
    finally:
        _uninstall_fake_psycopg(original)


def test_import_error_when_psycopg_missing() -> None:
    """from_psycopg raises ImportError with install instructions when psycopg is absent."""
    original = sys.modules.get("psycopg")
    sys.modules["psycopg"] = None  # type: ignore[assignment]  # simulate missing module for ImportError test
    try:
        # --- Assert ---
        with pytest.raises(ImportError, match="pip install sqldataclass"):
            from_psycopg(object())
    finally:
        _uninstall_fake_psycopg(original)
