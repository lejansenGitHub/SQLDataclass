import pytest


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    """Mark all tests in integration_tests/ with the integration marker."""
    for item in items:
        if "/integration_tests/" in str(item.fspath):
            item.add_marker(pytest.mark.integration)
