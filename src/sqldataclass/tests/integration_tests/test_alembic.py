"""Tests for Alembic migration compatibility with SQLDataclass.metadata."""

import os
import shutil
import tempfile
from typing import Any

import pytest
from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from sqlalchemy import create_engine, inspect

import sqldataclass.model as _model
from sqldataclass import Field, SQLDataclass

# ---------------------------------------------------------------------------
# Models for migration tests (unique tablenames)
# ---------------------------------------------------------------------------


class AlcHero(SQLDataclass, table=True):
    __tablename__ = "alc_hero"
    id: int | None = Field(default=None, primary_key=True)
    name: str
    power: str = ""


class AlcTeam(SQLDataclass, table=True):
    __tablename__ = "alc_team"
    id: int | None = Field(default=None, primary_key=True)
    name: str


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def alembic_env() -> Any:
    """Create a temp directory with a minimal Alembic environment."""
    tmpdir = tempfile.mkdtemp()
    db_path = os.path.join(tmpdir, "test.db")
    db_url = f"sqlite:///{db_path}"
    engine = create_engine(db_url)
    migrations_dir = os.path.join(tmpdir, "migrations")
    versions_dir = os.path.join(migrations_dir, "versions")
    os.makedirs(versions_dir)

    # Write alembic.ini
    ini_path = os.path.join(tmpdir, "alembic.ini")
    with open(ini_path, "w") as f:
        f.write(f"""[alembic]
script_location = {migrations_dir}
sqlalchemy.url = {db_url}
""")

    # Write env.py
    env_py = os.path.join(migrations_dir, "env.py")
    with open(env_py, "w") as f:
        f.write("""
from alembic import context
from sqldataclass import SQLDataclass

target_metadata = SQLDataclass.metadata

def run_migrations_online():
    from sqlalchemy import engine_from_config, pool
    connectable = engine_from_config(
        context.config.get_section(context.config.config_ini_section),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)
        with context.begin_transaction():
            context.run_migrations()

run_migrations_online()
""")

    # Write script.py.mako
    mako_path = os.path.join(migrations_dir, "script.py.mako")
    with open(mako_path, "w") as f:
        f.write('''"""${message}

Revision ID: ${up_revision}
"""
from alembic import op
import sqlalchemy as sa

revision = ${repr(up_revision)}
down_revision = ${repr(down_revision)}

def upgrade():
    ${upgrades if upgrades else "pass"}

def downgrade():
    ${downgrades if downgrades else "pass"}
''')

    cfg = Config(ini_path)

    yield {
        "config": cfg,
        "engine": engine,
        "db_url": db_url,
        "tmpdir": tmpdir,
        "migrations_dir": migrations_dir,
    }

    engine.dispose()
    shutil.rmtree(tmpdir)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestAlembicAutogenerate:
    def test_autogenerate_detects_tables(self, alembic_env: Any) -> None:
        """Alembic --autogenerate should detect SQLDataclass tables."""
        cfg = alembic_env["config"]

        # Generate initial migration
        command.revision(cfg, message="init", autogenerate=True)

        # Verify a migration script was created
        script_dir = ScriptDirectory.from_config(cfg)
        revisions = list(script_dir.walk_revisions())
        assert len(revisions) == 1
        assert "init" in revisions[0].doc

    def test_upgrade_creates_tables(self, alembic_env: Any) -> None:
        """Running upgrade head should create the tables in the DB."""
        cfg = alembic_env["config"]
        engine = alembic_env["engine"]

        command.revision(cfg, message="create tables", autogenerate=True)
        command.upgrade(cfg, "head")

        table_names = set(inspect(engine).get_table_names())
        assert "alc_hero" in table_names
        assert "alc_team" in table_names

    def test_tables_have_correct_columns(self, alembic_env: Any) -> None:
        """Migrated tables should have the correct columns."""
        cfg = alembic_env["config"]
        engine = alembic_env["engine"]

        command.revision(cfg, message="create tables", autogenerate=True)
        command.upgrade(cfg, "head")

        columns = {c["name"] for c in inspect(engine).get_columns("alc_hero")}
        assert columns == {"id", "name", "power"}

    def test_downgrade_removes_tables(self, alembic_env: Any) -> None:
        """Running downgrade base should remove the tables."""
        cfg = alembic_env["config"]
        engine = alembic_env["engine"]

        command.revision(cfg, message="create tables", autogenerate=True)
        command.upgrade(cfg, "head")
        assert "alc_hero" in inspect(engine).get_table_names()

        command.downgrade(cfg, "base")
        assert "alc_hero" not in inspect(engine).get_table_names()


class TestAlembicWithData:
    def test_insert_after_migration(self, alembic_env: Any) -> None:
        """After migration, SQLDataclass CRUD should work."""
        cfg = alembic_env["config"]
        engine = alembic_env["engine"]

        command.revision(cfg, message="init", autogenerate=True)
        command.upgrade(cfg, "head")

        SQLDataclass.bind(engine)
        AlcHero(name="Spider-Man", power="web").insert()
        heroes = AlcHero.load_all()
        assert len(heroes) == 1
        assert heroes[0].name == "Spider-Man"

        _model._BOUND_ENGINE = None

    def test_multiple_revisions(self, alembic_env: Any) -> None:
        """Multiple migrations can be generated and applied sequentially."""
        cfg = alembic_env["config"]
        engine = alembic_env["engine"]

        # First revision: creates tables
        command.revision(cfg, message="create tables", autogenerate=True)
        command.upgrade(cfg, "head")
        assert "alc_hero" in inspect(engine).get_table_names()

        # Second revision: no changes detected (should be empty/noop)
        command.revision(cfg, message="no changes", autogenerate=True)

        script_dir = ScriptDirectory.from_config(cfg)
        revisions = list(script_dir.walk_revisions())
        assert len(revisions) == 2
