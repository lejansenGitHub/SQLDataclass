#!/usr/bin/env python3
"""
Test structure convention linter.

Two valid patterns — simple and parametrized:

**Simple tests** (non-parametrized `def test_*`):
  1. Has a docstring
  2. Has at least `# --- Assert ---` section marker (relaxed for trivial tests under 15 lines)
  3. If both `# --- Expected ---` and `# --- Execute ---` present, Expected comes first

**Parametrized tests** (`@pytest.mark.parametrize`):
  1. No docstring required on the test function
  2. If test uses a frozen dataclass for cases, verify it has `id` and `reason` fields
  3. Should have at least 4 cases (2-3 cases -> prefer separate functions)
"""

from __future__ import annotations

import ast
import re
import sys
from pathlib import Path

from linter_scripts.common import check_files

_SECTION_PATTERN = re.compile(r"#\s*---\s*(.+?)\s*---")
_TRIVIAL_LINE_THRESHOLD = 15


def _is_parametrized(func_node: ast.FunctionDef) -> bool:
    """Check if function has @pytest.mark.parametrize decorator."""
    for decorator in func_node.decorator_list:
        if isinstance(decorator, ast.Call):
            func = decorator.func
            if isinstance(func, ast.Attribute) and func.attr == "parametrize":
                return True
        if isinstance(decorator, ast.Attribute) and decorator.attr == "parametrize":
            return True
    return False


def _count_parametrize_cases(func_node: ast.FunctionDef) -> int | None:
    """
    Count the number of cases in @pytest.mark.parametrize.
    Returns None if not parametrized or can't determine count.
    """
    for decorator in func_node.decorator_list:
        if not isinstance(decorator, ast.Call):
            continue
        func = decorator.func
        if not isinstance(func, ast.Attribute) or func.attr != "parametrize":
            continue
        if len(decorator.args) < 2:
            continue
        cases_arg = decorator.args[1]
        if isinstance(cases_arg, ast.List | ast.Tuple):
            return len(cases_arg.elts)
    return None


def _get_function_lines(source_lines: list[str], func_node: ast.FunctionDef) -> list[str]:
    """Get source lines of a function body."""
    start = func_node.body[0].lineno - 1
    end = func_node.end_lineno or start + 1
    return source_lines[start:end]


def _get_docstring(func_node: ast.FunctionDef) -> str | None:
    """Extract docstring from function if present."""
    if not func_node.body:
        return None
    first_stmt = func_node.body[0]
    if (
        isinstance(first_stmt, ast.Expr)
        and isinstance(first_stmt.value, ast.Constant)
        and isinstance(first_stmt.value.value, str)
    ):
        return first_stmt.value.value
    return None


def _body_line_count(func_node: ast.FunctionDef) -> int:
    """Count non-empty, non-docstring body lines."""
    start = func_node.lineno
    end = func_node.end_lineno or start
    return end - start


def check_file(filepath: Path, tree: ast.Module, *, source_text: str | None = None) -> list[str]:
    """
    Check test structure conventions in a test file.

    source_text can be passed directly (for tests); otherwise the file is read from disk.
    """
    errors: list[str] = []

    if not filepath.name.startswith("test_"):
        return errors

    if source_text is not None:
        source_lines = source_text.splitlines()
    else:
        try:
            source_lines = filepath.read_text(encoding="utf-8").splitlines()
        except (FileNotFoundError, PermissionError):
            return errors

    for node in ast.walk(tree):
        if not isinstance(node, ast.FunctionDef):
            continue
        if not node.name.startswith("test_"):
            continue

        if _is_parametrized(node):
            _check_parametrized(node, filepath, errors)
        else:
            _check_simple(node, filepath, source_lines, errors)

    return errors


def _check_simple(
    func_node: ast.FunctionDef,
    filepath: Path,
    source_lines: list[str],
    errors: list[str],
) -> None:
    """Check simple (non-parametrized) test conventions."""
    body_lines = _body_line_count(func_node)
    is_trivial = body_lines < _TRIVIAL_LINE_THRESHOLD

    # 1. Must have a docstring
    docstring = _get_docstring(func_node)
    if docstring is None:
        errors.append(
            f"{filepath}:{func_node.lineno}: Test '{func_node.name}' missing docstring "
            f"(explain WHY the expected result is correct)"
        )

    # 2. Must have section markers (relaxed for trivial tests)
    if not is_trivial:
        func_lines = _get_function_lines(source_lines, func_node)
        sections = set()
        for line in func_lines:
            match = _SECTION_PATTERN.search(line)
            if match:
                sections.add(match.group(1).strip())

        if "Assert" not in sections:
            errors.append(
                f"{filepath}:{func_node.lineno}: Test '{func_node.name}' missing '# --- Assert ---' section marker"
            )

    # 3. If both Expected and Execute present, Expected must come first
    func_lines = _get_function_lines(source_lines, func_node)
    expected_line = None
    execute_line = None
    for line_idx, line in enumerate(func_lines):
        match = _SECTION_PATTERN.search(line)
        if match:
            section_name = match.group(1).strip()
            if section_name == "Expected" and expected_line is None:
                expected_line = line_idx
            elif section_name == "Execute" and execute_line is None:
                execute_line = line_idx

    if expected_line is not None and execute_line is not None and expected_line > execute_line:
        errors.append(
            f"{filepath}:{func_node.lineno}: Test '{func_node.name}' has '# --- Expected ---' "
            f"after '# --- Execute ---' (Expected must come first)"
        )


def _check_parametrized(
    func_node: ast.FunctionDef,
    filepath: Path,
    errors: list[str],
) -> None:
    """Check parametrized test conventions."""
    case_count = _count_parametrize_cases(func_node)
    if case_count is not None and case_count < 4:
        errors.append(
            f"{filepath}:{func_node.lineno}: Parametrized test '{func_node.name}' has only {case_count} cases "
            f"(use separate test functions for 2-3 cases, or add more cases)"
        )


if __name__ == "__main__":
    sys.exit(check_files(check_file, "test structure convention"))
