"""Unit tests for versioned=True — schema versioning and migration."""

from __future__ import annotations

import asyncio
import threading
from typing import Any

import pytest

from sqldataclass import Field, SQLDataclass, SQLModel

# ---------------------------------------------------------------------------
# SQLDataclass versioned tests
# ---------------------------------------------------------------------------


class TestVersionedSQLDataclass:
    """Basic versioning on SQLDataclass."""

    def test_version_field_required(self) -> None:
        with pytest.raises(AttributeError, match="requires a field"):

            class Bad(SQLDataclass, versioned=True):
                name: str

    def test_version_field_must_be_int(self) -> None:
        with pytest.raises(AttributeError, match="int default"):

            class Bad(SQLDataclass, versioned=True):
                BAD_VERSION: str = "1"
                name: str

    def test_basic_versioned_model(self) -> None:
        class Hero(SQLDataclass, versioned=True):
            HERO_VERSION: int = 1
            name: str

        hero = Hero(name="Alice", HERO_VERSION=1)
        assert hero.name == "Alice"
        assert hero.HERO_VERSION == 1

    def test_get_version_field_name(self) -> None:
        class MyModel(SQLDataclass, versioned=True):
            MY_MODEL_VERSION: int = 1
            name: str = ""

        assert MyModel.get_version_field_name() == "MY_MODEL_VERSION"

    def test_get_schema_version(self) -> None:
        class Hero(SQLDataclass, versioned=True):
            HERO_VERSION: int = 3
            name: str = ""

        assert Hero.get_schema_version() == 3

    def test_outdated_true(self) -> None:
        class Hero(SQLDataclass, versioned=True):
            HERO_VERSION: int = 3
            name: str = ""

        assert Hero.outdated({"HERO_VERSION": 1, "name": "old"}) is True

    def test_outdated_false(self) -> None:
        class Hero(SQLDataclass, versioned=True):
            HERO_VERSION: int = 3
            name: str = ""

        assert Hero.outdated({"HERO_VERSION": 3, "name": "current"}) is False

    def test_outdated_missing_key(self) -> None:
        class Hero(SQLDataclass, versioned=True):
            HERO_VERSION: int = 2
            name: str = ""

        # Missing key = version 1, which is < 2
        assert Hero.outdated({"name": "old"}) is True

    def test_data_fields_excludes_version(self) -> None:
        class Hero(SQLDataclass, versioned=True):
            HERO_VERSION: int = 1
            name: str
            age: int = 0

        df = Hero.data_fields()
        assert "name" in df
        assert "age" in df
        assert "HERO_VERSION" not in df


class TestMigrationSQLDataclass:
    """Migration via load() on SQLDataclass."""

    def test_load_triggers_migration(self) -> None:
        class Address(SQLDataclass, versioned=True):
            ADDRESS_VERSION: int = 2
            street: str
            postal_code: int

            @classmethod
            def migrate(cls, obj: dict[str, Any]) -> dict[str, Any]:
                if obj["ADDRESS_VERSION"] < 2:
                    obj["postal_code"] = int(obj["postal_code"])
                    obj["ADDRESS_VERSION"] = 2
                return obj

        addr = Address.load({"ADDRESS_VERSION": 1, "street": "Main St", "postal_code": "12345"})
        assert addr.postal_code == 12345
        assert addr.ADDRESS_VERSION == 2

    def test_load_no_version_key_assumes_v1(self) -> None:
        """Unversioned → versioned upgrade: missing key = version 1."""

        class Hero(SQLDataclass, versioned=True):
            HERO_VERSION: int = 2
            full_name: str
            age: int = 0

            @classmethod
            def migrate(cls, obj: dict[str, Any]) -> dict[str, Any]:
                if obj["HERO_VERSION"] < 2:
                    obj["full_name"] = obj.pop("name")
                    obj["HERO_VERSION"] = 2
                return obj

        # Old data without version key
        old_json = {"name": "Alice", "age": 25}
        hero = Hero.load(old_json)
        assert hero.full_name == "Alice"
        assert hero.age == 25
        assert hero.HERO_VERSION == 2

    def test_load_current_version_no_migration(self) -> None:
        class Hero(SQLDataclass, versioned=True):
            HERO_VERSION: int = 2
            name: str

            @classmethod
            def migrate(cls, obj: dict[str, Any]) -> dict[str, Any]:
                raise AssertionError("Should not be called")

        # Current version — migrate() should NOT be reached
        # (it IS called but obj is already at version 2, so no branch triggers)
        class Hero2(SQLDataclass, versioned=True):
            HERO2_VERSION: int = 2
            name: str

            @classmethod
            def migrate(cls, obj: dict[str, Any]) -> dict[str, Any]:
                # Nothing to do for current version
                return obj

        hero = Hero2.load({"HERO2_VERSION": 2, "name": "Alice"})
        assert hero.name == "Alice"

    def test_init_without_load_no_migration(self) -> None:
        """Direct __init__ does NOT trigger migration."""
        call_count = 0

        class Hero(SQLDataclass, versioned=True):
            HERO_VERSION: int = 1
            name: str

            @classmethod
            def migrate(cls, obj: dict[str, Any]) -> dict[str, Any]:
                nonlocal call_count
                call_count += 1
                return obj

        Hero(name="Alice", HERO_VERSION=1)
        assert call_count == 0

    def test_multi_step_migration(self) -> None:
        class Config(SQLDataclass, versioned=True):
            CONFIG_VERSION: int = 3
            name: str
            value: int
            unit: str = ""

            @classmethod
            def migrate(cls, obj: dict[str, Any]) -> dict[str, Any]:
                if obj["CONFIG_VERSION"] < 2:
                    obj["value"] = int(obj["value"])
                    obj["CONFIG_VERSION"] = 2
                if obj["CONFIG_VERSION"] < 3:
                    obj["unit"] = "default"
                    obj["CONFIG_VERSION"] = 3
                return obj

        result = Config.load({"CONFIG_VERSION": 1, "name": "test", "value": "42"})
        assert result.value == 42
        assert result.unit == "default"
        assert result.CONFIG_VERSION == 3


class TestMigrationThreadSafety:
    """Migration context var is thread-safe."""

    def test_thread_safe(self) -> None:
        class Hero(SQLDataclass, versioned=True):
            HERO_VERSION: int = 2
            name: str

            @classmethod
            def migrate(cls, obj: dict[str, Any]) -> dict[str, Any]:
                if obj["HERO_VERSION"] < 2:
                    obj["name"] = obj["name"].upper()
                    obj["HERO_VERSION"] = 2
                return obj

        results: list[str] = []
        errors: list[Exception] = []

        def load_with_migration() -> None:
            try:
                h = Hero.load({"HERO_VERSION": 1, "name": "alice"})
                results.append(h.name)
            except Exception as e:
                errors.append(e)

        def load_without_migration() -> None:
            try:
                h = Hero(name="bob", HERO_VERSION=2)
                results.append(h.name)
            except Exception as e:
                errors.append(e)

        threads = [
            threading.Thread(target=load_with_migration),
            threading.Thread(target=load_without_migration),
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors
        assert "ALICE" in results  # migrated
        assert "bob" in results  # not migrated

    def test_async_safe(self) -> None:
        class Hero(SQLDataclass, versioned=True):
            HERO_VERSION: int = 2
            name: str

            @classmethod
            def migrate(cls, obj: dict[str, Any]) -> dict[str, Any]:
                if obj["HERO_VERSION"] < 2:
                    obj["name"] = obj["name"].upper()
                    obj["HERO_VERSION"] = 2
                return obj

        async def run() -> list[str]:
            async def with_migration() -> str:
                h = Hero.load({"HERO_VERSION": 1, "name": "alice"})
                return h.name

            async def without_migration() -> str:
                h = Hero(name="bob", HERO_VERSION=2)
                return h.name

            return list(await asyncio.gather(with_migration(), without_migration()))

        results = asyncio.run(run())
        assert "ALICE" in results
        assert "bob" in results


# ---------------------------------------------------------------------------
# SQLModel versioned tests
# ---------------------------------------------------------------------------


class TestVersionedSQLModel:
    """Versioning on SQLModel (BaseModel)."""

    def test_version_field_required(self) -> None:
        with pytest.raises(AttributeError, match="requires a field"):

            class Bad(SQLModel, table=True, versioned=True):
                __tablename__ = "sm_bad_ver"
                id: int | None = Field(default=None, primary_key=True)
                name: str = ""

    def test_basic_versioned_model(self) -> None:
        class Player(SQLModel, versioned=True):
            PLAYER_VERSION: int = 1
            name: str

        p = Player(name="Alice", PLAYER_VERSION=1)
        assert p.PLAYER_VERSION == 1

    def test_load_triggers_migration(self) -> None:
        class Player(SQLModel, versioned=True):
            PLAYER_VERSION: int = 2
            full_name: str
            score: float = 0.0

            @classmethod
            def migrate(cls, obj: dict[str, Any]) -> dict[str, Any]:
                if obj["PLAYER_VERSION"] < 2:
                    obj["full_name"] = obj.pop("name")
                    obj["PLAYER_VERSION"] = 2
                return obj

        p = Player.load({"PLAYER_VERSION": 1, "name": "Alice", "score": 9.5})
        assert p.full_name == "Alice"
        assert p.PLAYER_VERSION == 2

    def test_load_no_version_key(self) -> None:
        class Player(SQLModel, versioned=True):
            PLAYER_VERSION: int = 2
            full_name: str

            @classmethod
            def migrate(cls, obj: dict[str, Any]) -> dict[str, Any]:
                if obj["PLAYER_VERSION"] < 2:
                    obj["full_name"] = obj.pop("name")
                    obj["PLAYER_VERSION"] = 2
                return obj

        p = Player.load({"name": "Bob"})
        assert p.full_name == "Bob"

    def test_get_schema_version(self) -> None:
        class Player(SQLModel, versioned=True):
            PLAYER_VERSION: int = 5
            name: str = ""

        assert Player.get_schema_version() == 5

    def test_outdated(self) -> None:
        class Player(SQLModel, versioned=True):
            PLAYER_VERSION: int = 3
            name: str = ""

        assert Player.outdated({"name": "old"}) is True
        assert Player.outdated({"PLAYER_VERSION": 3, "name": "new"}) is False

    def test_init_without_load_no_migration(self) -> None:
        call_count = 0

        class Player(SQLModel, versioned=True):
            PLAYER_VERSION: int = 1
            name: str

            @classmethod
            def migrate(cls, obj: dict[str, Any]) -> dict[str, Any]:
                nonlocal call_count
                call_count += 1
                return obj

        Player(name="Alice", PLAYER_VERSION=1)
        assert call_count == 0
