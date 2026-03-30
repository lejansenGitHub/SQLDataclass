"""Unit tests for CyFunctionDetector."""

from __future__ import annotations

from sqldataclass import CyFunctionDetector


class TestCyFunctionDetector:
    """CyFunctionDetector matches Cython functions by class name."""

    def test_normal_function_not_detected(self) -> None:
        def my_func() -> None:
            pass

        assert not isinstance(my_func, CyFunctionDetector)

    def test_regular_object_not_detected(self) -> None:
        assert not isinstance(42, CyFunctionDetector)
        assert not isinstance("hello", CyFunctionDetector)

    def test_fake_cython_function_detected(self) -> None:
        """Simulate a Cython function by faking the class name."""

        class cython_function_or_method:
            pass

        obj = cython_function_or_method()
        assert isinstance(obj, CyFunctionDetector)
