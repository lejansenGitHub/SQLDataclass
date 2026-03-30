"""Unit tests for load(), dump(), clone() and Field(json=False)."""

from __future__ import annotations

from sqldataclass import Field, SQLDataclass, SQLModel

# ---------------------------------------------------------------------------
# SQLDataclass tests
# ---------------------------------------------------------------------------


class TestSQLDataclassDump:
    """dump() on SQLDataclass pydantic dataclasses."""

    def test_basic_dump(self) -> None:
        class Hero(SQLDataclass):
            name: str
            age: int = 0

        hero = Hero(name="Alice", age=25)
        d = hero.dump()
        assert d == {"name": "Alice", "age": 25}

    def test_dump_excludes_json_false(self) -> None:
        class Secret(SQLDataclass):
            name: str
            password_hash: str = Field(default="", json=False)

        s = Secret(name="Alice", password_hash="abc123")
        d = s.dump()
        assert "name" in d
        assert "password_hash" not in d

    def test_dump_includes_column_false(self) -> None:
        class WithDisplay(SQLDataclass):
            name: str
            display_name: str = Field(default="", column=False)

        w = WithDisplay(name="alice", display_name="Alice")
        d = w.dump()
        assert d["display_name"] == "Alice"

    def test_dump_three_way_scoping(self) -> None:
        class ThreeWay(SQLDataclass, table=True):
            __tablename__ = "tw_dump_test"
            id: int | None = Field(default=None, primary_key=True)
            name: str = ""
            secret: str = Field(default="", json=False)
            computed: str = Field(default="", column=False)

        obj = ThreeWay(id=1, name="test", secret="hidden", computed="visible")
        d = obj.dump()
        assert d["name"] == "test"
        assert d["computed"] == "visible"
        assert "secret" not in d


class TestSQLDataclassLoad:
    """load() on SQLDataclass pydantic dataclasses."""

    def test_basic_load(self) -> None:
        class Hero(SQLDataclass):
            name: str
            age: int = 0

        hero = Hero.load({"name": "Alice", "age": 25})
        assert hero.name == "Alice"
        assert hero.age == 25

    def test_load_with_defaults(self) -> None:
        class Hero(SQLDataclass):
            name: str
            age: int = 0

        hero = Hero.load({"name": "Bob"})
        assert hero.name == "Bob"
        assert hero.age == 0


class TestSQLDataclassClone:
    """clone() on SQLDataclass pydantic dataclasses."""

    def test_shallow_clone(self) -> None:
        class Hero(SQLDataclass):
            name: str
            age: int = 0

        hero = Hero(name="Alice", age=25)
        cloned = hero.clone()
        assert cloned.name == "Alice"
        assert cloned.age == 25

    def test_deep_clone(self) -> None:
        class Hero(SQLDataclass):
            name: str
            tags: list[str] = Field(default_factory=list)

        hero = Hero(name="Alice", tags=["a", "b"])
        cloned = hero.clone(deep=True)
        assert cloned.tags == ["a", "b"]
        cloned.tags.append("c")
        assert hero.tags == ["a", "b"]  # original unchanged


class TestSQLDataclassFieldNames:
    """model_field_names() and data_fields()."""

    def test_model_field_names(self) -> None:
        class Hero(SQLDataclass):
            name: str
            age: int = 0

        names = Hero.model_field_names()
        assert "name" in names
        assert "age" in names

    def test_data_fields_same_as_model_field_names(self) -> None:
        class Hero(SQLDataclass):
            name: str
            age: int = 0

        assert Hero.data_fields() == Hero.model_field_names()


class TestSQLDataclassValidatePrivateField:
    """validate_private_field() helper."""

    def test_validate_int(self) -> None:
        class Hero(SQLDataclass):
            name: str

        hero = Hero(name="Alice")
        result = hero.validate_private_field(int, "42")
        assert result == 42


# ---------------------------------------------------------------------------
# SQLModel tests
# ---------------------------------------------------------------------------


class TestSQLModelDump:
    """dump() on SQLModel (Pydantic BaseModel)."""

    def test_basic_dump(self) -> None:
        class Player(SQLModel):
            name: str
            score: float = 0.0

        p = Player(name="Alice", score=9.5)
        d = p.dump()
        assert d == {"name": "Alice", "score": 9.5}

    def test_dump_excludes_json_false(self) -> None:
        class SecretModel(SQLModel, table=True):
            __tablename__ = "sm_secret_dump"
            id: int | None = Field(default=None, primary_key=True)
            name: str = ""
            token: str = Field(default="", json=False)

        s = SecretModel(id=1, name="Alice", token="secret123")
        d = s.dump()
        assert "name" in d
        assert "token" not in d

    def test_dump_includes_column_false(self) -> None:
        class Display(SQLModel, table=True):
            __tablename__ = "sm_display_dump"
            id: int | None = Field(default=None, primary_key=True)
            name: str = ""
            display: str = Field(default="", column=False)

        obj = Display(id=1, name="test", display="shown")
        d = obj.dump()
        assert d["display"] == "shown"


class TestSQLModelLoad:
    """load() on SQLModel."""

    def test_basic_load(self) -> None:
        class Player(SQLModel):
            name: str
            score: float = 0.0

        p = Player.load({"name": "Alice", "score": 9.5})
        assert p.name == "Alice"
        assert p.score == 9.5


class TestSQLModelClone:
    """clone() on SQLModel."""

    def test_shallow_clone(self) -> None:
        class Player(SQLModel):
            name: str
            score: float = 0.0

        p = Player(name="Alice", score=9.5)
        c = p.clone()
        assert c.name == "Alice"
        assert c.score == 9.5

    def test_deep_clone(self) -> None:
        class Player(SQLModel):
            name: str
            tags: list[str] = Field(default_factory=list)

        p = Player(name="Alice", tags=["a", "b"])
        c = p.clone(deep=True)
        c.tags.append("c")
        assert p.tags == ["a", "b"]
