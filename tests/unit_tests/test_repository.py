"""Tests for sqldataclass.repository — ReadRepository, WriteRepository, TransactionHandle."""

from __future__ import annotations

from collections.abc import Generator

import pytest
from sqlalchemy import Column, Integer, String, create_engine
from sqlalchemy.engine import Connection
from sqlalchemy.orm import DeclarativeBase

from sqldataclass.repository import ReadRepository, TransactionHandle, WriteRepository

# ---------------------------------------------------------------------------
# Isolated DeclarativeBase for this test module
# ---------------------------------------------------------------------------


class _TestBase(DeclarativeBase):
    pass


class _User(_TestBase):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)
    email = Column(String, nullable=True)


# ---------------------------------------------------------------------------
# Concrete test repositories
# ---------------------------------------------------------------------------


class UserReadRepo(ReadRepository):
    def get_by_id(self, user_id: int) -> dict[str, object] | None:
        row = self._fetch_one("SELECT * FROM users WHERE id = :user_id", {"user_id": user_id})
        return dict(row) if row else None

    def list_all(self) -> list[dict[str, object]]:
        return [dict(row) for row in self._fetch_all("SELECT * FROM users ORDER BY id")]

    def count(self) -> int:
        value = self._fetch_value("SELECT COUNT(*) FROM users")
        return int(value) if value is not None else 0


class UserWriteRepo(WriteRepository):
    def create(self, name: str, email: str | None = None) -> None:
        self._execute("INSERT INTO users (name, email) VALUES (:name, :email)", {"name": name, "email": email})


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def connection() -> Generator[Connection]:
    engine = create_engine("sqlite:///:memory:")
    _TestBase.metadata.create_all(engine)
    with engine.connect() as conn:
        yield conn


@pytest.fixture
def read_repo(connection: Connection) -> UserReadRepo:
    return UserReadRepo(connection)


@pytest.fixture
def write_repo(connection: Connection) -> UserWriteRepo:
    return UserWriteRepo(connection)


# ---------------------------------------------------------------------------
# ReadRepository
# ---------------------------------------------------------------------------


def test_fetch_one_returns_row(write_repo: UserWriteRepo, read_repo: UserReadRepo) -> None:
    """Inserted row is retrievable by its auto-generated PK."""
    write_repo.create("Alice", "alice@example.com")
    write_repo.commit()

    # --- Assert ---
    result = read_repo.get_by_id(1)
    assert result is not None
    assert result["name"] == "Alice"
    assert result["email"] == "alice@example.com"


def test_fetch_one_returns_none_when_missing(read_repo: UserReadRepo) -> None:
    """Non-existent PK yields None, not an exception."""
    # --- Assert ---
    assert read_repo.get_by_id(999) is None


def test_fetch_all_returns_list(write_repo: UserWriteRepo, read_repo: UserReadRepo) -> None:
    """Multiple rows are returned in insertion order."""
    write_repo.create("Alice")
    write_repo.create("Bob")
    write_repo.commit()

    # --- Assert ---
    users = read_repo.list_all()
    assert len(users) == 2
    assert users[0]["name"] == "Alice"
    assert users[1]["name"] == "Bob"


def test_fetch_all_returns_empty_list(read_repo: UserReadRepo) -> None:
    """Empty table yields an empty list, not None."""
    # --- Assert ---
    assert read_repo.list_all() == []


def test_fetch_value_returns_scalar(write_repo: UserWriteRepo, read_repo: UserReadRepo) -> None:
    """COUNT(*) returns the number of rows as a scalar."""
    write_repo.create("Alice")
    write_repo.create("Bob")
    write_repo.commit()

    # --- Assert ---
    assert read_repo.count() == 2


def test_fetch_value_returns_none_for_empty_table(read_repo: UserReadRepo) -> None:
    """Scalar query on non-existent row yields None."""
    # --- Assert ---
    value = read_repo._fetch_value("SELECT name FROM users WHERE id = :id", {"id": 999})
    assert value is None


# ---------------------------------------------------------------------------
# WriteRepository
# ---------------------------------------------------------------------------


def test_execute_inserts_row(write_repo: UserWriteRepo, read_repo: UserReadRepo) -> None:
    """_execute runs the INSERT and commit makes it visible."""
    write_repo.create("Charlie")
    write_repo.commit()

    # --- Assert ---
    assert read_repo.count() == 1


def test_commit_persists_data(connection: Connection) -> None:
    """Data is readable after commit on the same connection."""
    write_repo = UserWriteRepo(connection)
    write_repo.create("Dave")
    write_repo.commit()

    # --- Assert ---
    read_repo = UserReadRepo(connection)
    assert read_repo.count() == 1


# ---------------------------------------------------------------------------
# TransactionHandle
# ---------------------------------------------------------------------------


def _insert_and_raise(write_repo: UserWriteRepo, transaction: TransactionHandle) -> None:
    with transaction.savepoint():
        write_repo.create("Bob")
        raise Exception("deliberate")  # intentional bare raise for savepoint test


def test_savepoint_rolls_back_on_error(connection: Connection) -> None:
    """Only the savepoint block rolls back; Alice (outside) survives, Bob (inside) does not."""
    write_repo = UserWriteRepo(connection)
    transaction = TransactionHandle(connection)

    write_repo.create("Alice")

    with pytest.raises(Exception, match="deliberate"):
        _insert_and_raise(write_repo, transaction)

    transaction.commit()

    # --- Assert ---
    read_repo = UserReadRepo(connection)
    users = read_repo.list_all()
    assert len(users) == 1
    assert users[0]["name"] == "Alice"


def test_savepoint_commits_on_success(connection: Connection) -> None:
    """Both rows survive when the savepoint block succeeds."""
    write_repo = UserWriteRepo(connection)
    transaction = TransactionHandle(connection)

    write_repo.create("Alice")
    with transaction.savepoint():
        write_repo.create("Bob")
    transaction.commit()

    # --- Assert ---
    read_repo = UserReadRepo(connection)
    assert read_repo.count() == 2


def test_transaction_commit(connection: Connection) -> None:
    """TransactionHandle.commit() delegates to the underlying connection."""
    write_repo = UserWriteRepo(connection)
    transaction = TransactionHandle(connection)

    write_repo.create("Eve")
    transaction.commit()

    # --- Assert ---
    read_repo = UserReadRepo(connection)
    assert read_repo.count() == 1
