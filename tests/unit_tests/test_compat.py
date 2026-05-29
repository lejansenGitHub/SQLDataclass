"""Tests for sqldataclass.compat — from_psycopg bridge.

Covers:
- psycopg (v3) dispatch.
- psycopg2 dispatch (B6).
- ImportError when neither driver is available.
- TypeError for unrelated objects.
- B7: from_psycopg patches engine.dialect.do_rollback during first_connect
  and configures pool_reset_on_return=None.
"""

from __future__ import annotations

import sys
from types import ModuleType
from typing import Any

import pytest
from pytest_mock import MockerFixture

from sqldataclass.compat import (
    _detect_driver_and_extract_connection,
    _extract_connection,
    from_psycopg,
)

# ---------------------------------------------------------------------------
# Fake psycopg (v3) and psycopg2 modules registered in sys.modules so
# isinstance checks succeed without the real packages.
# ---------------------------------------------------------------------------


class _FakePsycopg3Connection:
    pass


class _FakePsycopg3Cursor:
    def __init__(self, connection: _FakePsycopg3Connection) -> None:
        self.connection = connection


class _FakePsycopg2Connection:
    pass


class _FakePsycopg2Cursor:
    def __init__(self, connection: _FakePsycopg2Connection) -> None:
        self.connection = connection


def _install_fake_psycopg3() -> ModuleType:
    """Register a fake psycopg (v3) module."""
    mock_module = ModuleType("psycopg")
    mock_module.Cursor = _FakePsycopg3Cursor  # type: ignore[attr-defined]  # dynamic test module setup
    mock_module.Connection = _FakePsycopg3Connection  # type: ignore[attr-defined]  # dynamic test module setup
    sys.modules["psycopg"] = mock_module
    return mock_module


def _install_fake_psycopg2() -> ModuleType:
    """Register a fake psycopg2 module with the extensions submodule."""
    psycopg2_mod = ModuleType("psycopg2")
    ext_mod = ModuleType("psycopg2.extensions")
    ext_mod.cursor = _FakePsycopg2Cursor  # type: ignore[attr-defined]  # dynamic test module setup
    ext_mod.connection = _FakePsycopg2Connection  # type: ignore[attr-defined]  # dynamic test module setup
    psycopg2_mod.extensions = ext_mod  # type: ignore[attr-defined]  # dynamic test module setup
    sys.modules["psycopg2"] = psycopg2_mod
    sys.modules["psycopg2.extensions"] = ext_mod
    return psycopg2_mod


def _uninstall_fakes(
    keys: tuple[str, ...],
    saved: dict[str, ModuleType | None],
) -> None:
    for key in keys:
        if saved[key] is not None:
            sys.modules[key] = saved[key]  # type: ignore[assignment]  # restoring saved module
        else:
            sys.modules.pop(key, None)


# ---------------------------------------------------------------------------
# _detect_driver_and_extract_connection — psycopg (v3)
# ---------------------------------------------------------------------------


def test_psycopg3_cursor_returns_psycopg_driver_and_connection() -> None:
    """A psycopg (v3) cursor is detected as 'psycopg' and its .connection returned."""
    saved = {"psycopg": sys.modules.get("psycopg")}
    _install_fake_psycopg3()
    try:
        conn = _FakePsycopg3Connection()
        cursor = _FakePsycopg3Cursor(conn)

        # --- Assert ---
        driver, result = _detect_driver_and_extract_connection(cursor)
        assert driver == "psycopg"
        assert result is conn
    finally:
        _uninstall_fakes(("psycopg",), saved)


def test_psycopg3_connection_returns_psycopg_driver_unchanged() -> None:
    """A psycopg (v3) connection is detected as 'psycopg' and returned as-is."""
    saved = {"psycopg": sys.modules.get("psycopg")}
    _install_fake_psycopg3()
    try:
        conn = _FakePsycopg3Connection()

        # --- Assert ---
        driver, result = _detect_driver_and_extract_connection(conn)
        assert driver == "psycopg"
        assert result is conn
    finally:
        _uninstall_fakes(("psycopg",), saved)


# ---------------------------------------------------------------------------
# _detect_driver_and_extract_connection — psycopg2 (B6)
# ---------------------------------------------------------------------------


def test_psycopg2_cursor_returns_psycopg2_driver_and_connection() -> None:
    """A psycopg2 cursor is detected as 'psycopg2' and its .connection returned."""
    saved = {
        "psycopg2": sys.modules.get("psycopg2"),
        "psycopg2.extensions": sys.modules.get("psycopg2.extensions"),
    }
    _install_fake_psycopg2()
    try:
        conn = _FakePsycopg2Connection()
        cursor = _FakePsycopg2Cursor(conn)

        # --- Assert ---
        driver, result = _detect_driver_and_extract_connection(cursor)
        assert driver == "psycopg2"
        assert result is conn
    finally:
        _uninstall_fakes(("psycopg2", "psycopg2.extensions"), saved)


def test_psycopg2_connection_returns_psycopg2_driver_unchanged() -> None:
    """A psycopg2 connection is detected as 'psycopg2' and returned as-is."""
    saved = {
        "psycopg2": sys.modules.get("psycopg2"),
        "psycopg2.extensions": sys.modules.get("psycopg2.extensions"),
    }
    _install_fake_psycopg2()
    try:
        conn = _FakePsycopg2Connection()

        # --- Assert ---
        driver, result = _detect_driver_and_extract_connection(conn)
        assert driver == "psycopg2"
        assert result is conn
    finally:
        _uninstall_fakes(("psycopg2", "psycopg2.extensions"), saved)


def test_psycopg2_cursor_subclass_is_accepted() -> None:
    """A subclass of the psycopg2 cursor base type (e.g. DictCursor) is dispatched correctly."""
    saved = {
        "psycopg2": sys.modules.get("psycopg2"),
        "psycopg2.extensions": sys.modules.get("psycopg2.extensions"),
    }
    _install_fake_psycopg2()
    try:

        class _SubclassedCursor(_FakePsycopg2Cursor):
            pass

        conn = _FakePsycopg2Connection()
        cursor = _SubclassedCursor(conn)

        # --- Assert ---
        driver, result = _detect_driver_and_extract_connection(cursor)
        assert driver == "psycopg2"
        assert result is conn
    finally:
        _uninstall_fakes(("psycopg2", "psycopg2.extensions"), saved)


# ---------------------------------------------------------------------------
# Both drivers installed — selection by object type
# ---------------------------------------------------------------------------


def test_driver_selected_by_object_type_when_both_installed() -> None:
    """When both psycopg and psycopg2 are present, the object's type chooses the driver."""
    saved = {
        "psycopg": sys.modules.get("psycopg"),
        "psycopg2": sys.modules.get("psycopg2"),
        "psycopg2.extensions": sys.modules.get("psycopg2.extensions"),
    }
    _install_fake_psycopg3()
    _install_fake_psycopg2()
    try:
        # --- Assert ---
        v3_driver, _ = _detect_driver_and_extract_connection(_FakePsycopg3Connection())
        v2_driver, _ = _detect_driver_and_extract_connection(_FakePsycopg2Connection())
        assert v3_driver == "psycopg"
        assert v2_driver == "psycopg2"
    finally:
        _uninstall_fakes(("psycopg", "psycopg2", "psycopg2.extensions"), saved)


# ---------------------------------------------------------------------------
# Error paths
# ---------------------------------------------------------------------------


def test_no_drivers_installed_raises_importerror() -> None:
    """When neither psycopg nor psycopg2 can be imported, raise ImportError."""
    saved = {
        "psycopg": sys.modules.get("psycopg"),
        "psycopg2": sys.modules.get("psycopg2"),
    }
    sys.modules["psycopg"] = None  # type: ignore[assignment]  # simulate missing
    sys.modules["psycopg2"] = None  # type: ignore[assignment]  # simulate missing
    try:
        # --- Assert ---
        with pytest.raises(ImportError, match="psycopg"):
            _detect_driver_and_extract_connection(object())
    finally:
        _uninstall_fakes(("psycopg", "psycopg2"), saved)


def test_unrelated_object_raises_typeerror_with_driver_list() -> None:
    """An object matching neither driver raises TypeError listing installed drivers."""
    saved = {
        "psycopg": sys.modules.get("psycopg"),
        "psycopg2": sys.modules.get("psycopg2"),
        "psycopg2.extensions": sys.modules.get("psycopg2.extensions"),
    }
    _install_fake_psycopg3()
    _install_fake_psycopg2()
    try:
        # --- Assert ---
        with pytest.raises(TypeError, match="Expected a psycopg or psycopg2"):
            _detect_driver_and_extract_connection("not a cursor")
    finally:
        _uninstall_fakes(("psycopg", "psycopg2", "psycopg2.extensions"), saved)


# ---------------------------------------------------------------------------
# Backwards-compatibility shim
# ---------------------------------------------------------------------------


def test_extract_connection_backcompat_returns_connection_only() -> None:
    """_extract_connection (deprecated) returns just the connection, no driver."""
    saved = {"psycopg": sys.modules.get("psycopg")}
    _install_fake_psycopg3()
    try:
        conn = _FakePsycopg3Connection()
        cursor = _FakePsycopg3Cursor(conn)

        # --- Assert ---
        result = _extract_connection(cursor)
        assert result is conn
    finally:
        _uninstall_fakes(("psycopg",), saved)


# ---------------------------------------------------------------------------
# B7 — from_psycopg patches do_rollback and disables pool reset
# ---------------------------------------------------------------------------


class _FakeSAConnection:
    pass


class _FakeDialect:
    def __init__(self) -> None:
        self.do_rollback_calls: list[Any] = []

    def do_rollback(self, conn: Any) -> None:
        self.do_rollback_calls.append(conn)


class _FakeEngine:
    """Records create_engine kwargs and exposes a controllable .connect()."""

    last_kwargs: dict[str, Any] | None = None

    def __init__(self, *_args: Any, **kwargs: Any) -> None:
        type(self).last_kwargs = kwargs
        self.dialect = _FakeDialect()
        # Capture the original __func__ at construction so the test can later
        # assert restoration by underlying function identity (bound methods
        # are recreated on each attribute access, so `is` would always fail).
        self.captured_original_func = self.dialect.do_rollback.__func__

    def connect(self) -> _FakeSAConnection:
        # While connect runs, do_rollback should already be patched.
        # Calling it must be a no-op (the lambda the SUT installed).
        self.do_rollback_during_connect_returns = self.dialect.do_rollback(None)
        return _FakeSAConnection()


def test_from_psycopg_sets_pool_reset_on_return_none(mocker: MockerFixture) -> None:
    """from_psycopg builds the engine with pool_reset_on_return=None (B7)."""
    saved = {"psycopg": sys.modules.get("psycopg")}
    _install_fake_psycopg3()
    try:
        conn = _FakePsycopg3Connection()
        mocker.patch("sqldataclass.compat.create_engine", _FakeEngine)
        from_psycopg(conn)

        # --- Assert ---
        assert _FakeEngine.last_kwargs is not None
        assert _FakeEngine.last_kwargs["pool_reset_on_return"] is None
    finally:
        _FakeEngine.last_kwargs = None
        _uninstall_fakes(("psycopg",), saved)


def test_from_psycopg_uses_psycopg3_url_for_psycopg3_object(
    mocker: MockerFixture,
) -> None:
    """from_psycopg picks the postgresql+psycopg URL when given a v3 object."""
    saved = {"psycopg": sys.modules.get("psycopg")}
    _install_fake_psycopg3()
    try:
        mocked = mocker.patch("sqldataclass.compat.create_engine")
        mocked.return_value = _FakeEngine()
        from_psycopg(_FakePsycopg3Connection())

        # --- Assert ---
        url = mocked.call_args.args[0]
        assert url == "postgresql+psycopg://"
    finally:
        _uninstall_fakes(("psycopg",), saved)


def test_from_psycopg_uses_psycopg2_url_for_psycopg2_object(
    mocker: MockerFixture,
) -> None:
    """from_psycopg picks the postgresql+psycopg2 URL when given a v2 object."""
    saved = {
        "psycopg2": sys.modules.get("psycopg2"),
        "psycopg2.extensions": sys.modules.get("psycopg2.extensions"),
    }
    _install_fake_psycopg2()
    try:
        mocked = mocker.patch("sqldataclass.compat.create_engine")
        mocked.return_value = _FakeEngine()
        from_psycopg(_FakePsycopg2Connection())

        # --- Assert ---
        url = mocked.call_args.args[0]
        assert url == "postgresql+psycopg2://"
    finally:
        _uninstall_fakes(("psycopg2", "psycopg2.extensions"), saved)


def test_from_psycopg_restores_do_rollback_after_connect(
    mocker: MockerFixture,
) -> None:
    """After from_psycopg returns, engine.dialect.do_rollback is restored to the original."""
    saved = {"psycopg": sys.modules.get("psycopg")}
    _install_fake_psycopg3()
    try:
        engine_holder: dict[str, _FakeEngine] = {}

        def _make_engine(*_args: Any, **_kwargs: Any) -> _FakeEngine:
            engine = _FakeEngine()
            engine_holder["engine"] = engine
            return engine

        mocker.patch("sqldataclass.compat.create_engine", _make_engine)
        from_psycopg(_FakePsycopg3Connection())

        engine = engine_holder["engine"]

        # --- Assert ---
        # Original method object restored (identity check).
        assert engine.dialect.do_rollback.__func__ is engine.captured_original_func
        # During connect, the patched no-op was called and returned None.
        assert engine.do_rollback_during_connect_returns is None
    finally:
        _uninstall_fakes(("psycopg",), saved)


def test_from_psycopg_restores_do_rollback_even_if_connect_raises(
    mocker: MockerFixture,
) -> None:
    """If engine.connect() raises, do_rollback is still restored."""
    saved = {"psycopg": sys.modules.get("psycopg")}
    _install_fake_psycopg3()
    try:

        class _ExplodingEngine(_FakeEngine):
            def connect(self) -> _FakeSAConnection:
                raise RuntimeError("simulated failure")

        engine_holder: dict[str, _ExplodingEngine] = {}

        def _make_engine(*_args: Any, **_kwargs: Any) -> _ExplodingEngine:
            engine = _ExplodingEngine()
            engine_holder["engine"] = engine
            return engine

        mocker.patch("sqldataclass.compat.create_engine", _make_engine)
        with pytest.raises(RuntimeError, match="simulated failure"):
            from_psycopg(_FakePsycopg3Connection())

        engine = engine_holder["engine"]

        # --- Assert ---
        assert engine.dialect.do_rollback.__func__ is engine.captured_original_func
    finally:
        _uninstall_fakes(("psycopg",), saved)
