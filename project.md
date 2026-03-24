# Project Conventions

These conventions apply across all my projects. SQLDataclass is the first one.

## Folder Structure

Every package folder under `src/` has its own colocated `tests/` folder with the following subfolders:

```
src/
└── <package>/
    ├── __init__.py
    ├── ...
    └── tests/
        ├── util/             # test helpers, shared assertions
        ├── factories/        # test data factories
        ├── cases/            # reusable test case definitions
        ├── unit_tests/       # fast, isolated unit tests
        └── integration_tests/ # tests hitting real DB / Redis
```

There is no top-level `tests/` folder.

## Tooling

- **Package manager**: uv
- **Linting & formatting**: ruff
- **Type checking**: mypy (strict mode)
- **Testing**: pytest
- **Python**: 3.13+

## Docker Sandbox

The Docker container includes Node.js solely as a runtime dependency for Claude Code (`@anthropic-ai/claude-code`). Node.js is not used by the project itself — it is a pure Python backend.
