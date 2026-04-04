#!/usr/bin/env python3
"""
MagicMock() must always use spec= for type safety.
Without spec, typos in attribute names silently pass.
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path

from linter_scripts.common import check_files


def _is_magicmock_call(node: ast.Call) -> bool:
    """
    Check if a Call node is MagicMock() or *.MagicMock().
    """
    if isinstance(node.func, ast.Name) and node.func.id == "MagicMock":
        return True
    return isinstance(node.func, ast.Attribute) and node.func.attr == "MagicMock"


def _has_spec_kwarg(node: ast.Call) -> bool:
    return any(kw.arg == "spec" for kw in node.keywords)


def check_file(filepath: Path, tree: ast.Module) -> list[str]:
    return [
        f"{filepath}:{node.lineno}: MagicMock() without spec=. Always use MagicMock(spec=TargetClass) for type safety."
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and _is_magicmock_call(node) and not _has_spec_kwarg(node)
    ]


if __name__ == "__main__":
    sys.exit(check_files(check_file, "MagicMock requires spec"))
