import pytest


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    """Mark all tests in performance_tests/ with the performance marker."""
    for item in items:
        if "/performance_tests/" in str(item.fspath):
            item.add_marker(pytest.mark.performance)
