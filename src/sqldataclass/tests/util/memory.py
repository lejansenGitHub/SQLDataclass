"""Memory measurement utilities for benchmarking."""

from __future__ import annotations

import gc
import tracemalloc
from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass


@dataclass(slots=True)
class MemoryMeasurement:
    """Result of a memory measurement block."""

    retained: int = 0
    peak: int = 0


@contextmanager
def measure_memory() -> Generator[MemoryMeasurement]:
    """Context manager that tracks retained and peak memory allocation.

    Usage::

        with measure_memory() as measurement:
            objects = [SomeClass(**row) for row in rows]
        print(f"Peak: {measurement.peak} bytes")
    """
    result = MemoryMeasurement()
    tracemalloc.start()
    tracemalloc.reset_peak()
    yield result
    gc.collect()
    result.retained, result.peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
