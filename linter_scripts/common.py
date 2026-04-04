"""
Shared utilities for custom linter scripts.
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path


def check_files(check_function: object, description: str) -> int:
    """
    Standard entry point for linter scripts.
    Accepts file paths from argv, runs check_function on each,
    prints errors, returns exit code.
    """
    errors: list[str] = []
    for filepath_str in sys.argv[1:]:
        filepath = Path(filepath_str)
        if not filepath.exists() or filepath.suffix != ".py":
            continue
        try:
            source = filepath.read_text(encoding="utf-8")
            tree = ast.parse(source, filename=str(filepath))
        except SyntaxError:
            continue
        file_errors = check_function(filepath, tree)  # type: ignore[operator]  # callable check_function is typed as object for flexibility
        errors.extend(file_errors)
    for error in errors:
        print(error)  # noqa: T201  # linter output, not application print
    return 1 if errors else 0
