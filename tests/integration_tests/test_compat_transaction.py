"""Integration tests for B7: ``from_psycopg`` preserves the caller's transaction.

These tests require a running PostgreSQL instance and either psycopg (v3),
psycopg2, or both. The DSN is read from the ``SQLDATACLASS_TEST_PG_DSN``
environment variable; tests are skipped if the variable is unset or the
driver isn't installed.

The bug this guards against: SA's dialect initialization and pool reset
both call ``do_rollback()`` on the wrapped connection by default. That
wipes any uncommitted writes the caller made via the original cursor.
"""

from __future__ import annotations

import os
from typing import Any

import pytest
from sqlalchemy import text

from sqldataclass import from_psycopg

_DSN_ENV_VAR = "SQLDATACLASS_TEST_PG_DSN"
_DSN = os.environ.get(_DSN_ENV_VAR)

_PROBE_TABLE = "sqldc_compat_probe"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _psycopg3_module() -> Any:
    try:
        import psycopg

        return psycopg
    except ImportError:
        return None


def _psycopg2_module() -> Any:
    try:
        import psycopg2

        return psycopg2
    except ImportError:
        return None


def _setup_probe_table(connect_factory: Any) -> None:
    """Create a clean probe table and commit."""
    conn = connect_factory()
    try:
        cur = conn.cursor()
        cur.execute(f"DROP TABLE IF EXISTS {_PROBE_TABLE}")
        cur.execute(f"CREATE TABLE {_PROBE_TABLE} (id int)")
        conn.commit()
    finally:
        conn.close()


def _drop_probe_table(connect_factory: Any) -> None:
    conn = connect_factory()
    try:
        cur = conn.cursor()
        cur.execute(f"DROP TABLE IF EXISTS {_PROBE_TABLE}")
        conn.commit()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# psycopg (v3) — full transaction-sharing scenario
# ---------------------------------------------------------------------------


@pytest.mark.skipif(_DSN is None, reason=f"{_DSN_ENV_VAR} not set")
@pytest.mark.skipif(_psycopg3_module() is None, reason="psycopg (v3) not installed")
def test_psycopg3_uncommitted_writes_visible_via_sa() -> None:
    """SA-wrapped psycopg3 conn sees rows the original cursor inserted in the same transaction."""
    psycopg = _psycopg3_module()
    _setup_probe_table(lambda: psycopg.connect(_DSN))

    conn = psycopg.connect(_DSN)
    try:
        cur = conn.cursor()
        cur.execute(f"INSERT INTO {_PROBE_TABLE} VALUES (1), (2), (3)")

        sa_conn = from_psycopg(cur)
        result = sa_conn.execute(text(f"SELECT count(*) FROM {_PROBE_TABLE}")).scalar()

        # --- Assert ---
        assert result == 3, "SA should see the uncommitted writes from the same transaction"

        # And the original cursor still sees them after the SA round trip.
        cur.execute(f"SELECT count(*) FROM {_PROBE_TABLE}")
        assert cur.fetchone()[0] == 3
    finally:
        conn.rollback()
        conn.close()
        _drop_probe_table(lambda: psycopg.connect(_DSN))


@pytest.mark.skipif(_DSN is None, reason=f"{_DSN_ENV_VAR} not set")
@pytest.mark.skipif(_psycopg3_module() is None, reason="psycopg (v3) not installed")
def test_psycopg3_sa_close_does_not_rollback_caller_tx() -> None:
    """Garbage-collecting the SAConnection does NOT roll back the caller's transaction."""
    psycopg = _psycopg3_module()
    _setup_probe_table(lambda: psycopg.connect(_DSN))

    conn = psycopg.connect(_DSN)
    try:
        cur = conn.cursor()
        cur.execute(f"INSERT INTO {_PROBE_TABLE} VALUES (10), (20)")

        sa_conn = from_psycopg(cur)
        sa_conn.execute(text(f"SELECT count(*) FROM {_PROBE_TABLE}")).scalar()
        sa_conn.close()  # would trigger pool reset; we disabled that

        cur.execute(f"SELECT count(*) FROM {_PROBE_TABLE}")

        # --- Assert ---
        assert cur.fetchone()[0] == 2
    finally:
        conn.rollback()
        conn.close()
        _drop_probe_table(lambda: psycopg.connect(_DSN))


# ---------------------------------------------------------------------------
# psycopg2 — same scenarios for the v2 driver
# ---------------------------------------------------------------------------


@pytest.mark.skipif(_DSN is None, reason=f"{_DSN_ENV_VAR} not set")
@pytest.mark.skipif(_psycopg2_module() is None, reason="psycopg2 not installed")
def test_psycopg2_uncommitted_writes_visible_via_sa() -> None:
    """SA-wrapped psycopg2 conn sees rows the original cursor inserted in the same transaction."""
    psycopg2 = _psycopg2_module()
    _setup_probe_table(lambda: psycopg2.connect(_DSN))

    conn = psycopg2.connect(_DSN)
    try:
        cur = conn.cursor()
        cur.execute(f"INSERT INTO {_PROBE_TABLE} VALUES (1), (2), (3)")

        sa_conn = from_psycopg(cur)
        result = sa_conn.execute(text(f"SELECT count(*) FROM {_PROBE_TABLE}")).scalar()

        # --- Assert ---
        assert result == 3

        cur.execute(f"SELECT count(*) FROM {_PROBE_TABLE}")
        assert cur.fetchone()[0] == 3
    finally:
        conn.rollback()
        conn.close()
        _drop_probe_table(lambda: psycopg2.connect(_DSN))


@pytest.mark.skipif(_DSN is None, reason=f"{_DSN_ENV_VAR} not set")
@pytest.mark.skipif(_psycopg2_module() is None, reason="psycopg2 not installed")
def test_psycopg2_two_consecutive_from_psycopg_calls_share_state() -> None:
    """Calling from_psycopg twice on the same conn — both see the same transaction state."""
    psycopg2 = _psycopg2_module()
    _setup_probe_table(lambda: psycopg2.connect(_DSN))

    conn = psycopg2.connect(_DSN)
    try:
        cur = conn.cursor()
        cur.execute(f"INSERT INTO {_PROBE_TABLE} VALUES (100)")

        sa1 = from_psycopg(cur)
        sa2 = from_psycopg(cur)

        # --- Assert ---
        assert sa1.execute(text(f"SELECT count(*) FROM {_PROBE_TABLE}")).scalar() == 1
        assert sa2.execute(text(f"SELECT count(*) FROM {_PROBE_TABLE}")).scalar() == 1
    finally:
        conn.rollback()
        conn.close()
        _drop_probe_table(lambda: psycopg2.connect(_DSN))
