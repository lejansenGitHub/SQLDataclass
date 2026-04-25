# Forking this project

This repo includes some internal dev tooling that external contributors don't need. Here's how to get a clean setup after forking.

## What to remove

1. **`vendor/`** — contains a private linter wheel. Delete the directory.

2. **`pyproject.toml`** — remove these lines:
   ```toml
   [tool.uv]
   find-links = ["vendor/"]
   ```
   And remove `"blueprint-linters>=0.10.0"` from `[project.optional-dependencies] dev`.

3. **`.pre-commit-config.yaml`** — remove the `blueprint-linters` hook:
   ```yaml
   - id: blueprint-linters
     name: "blueprint linters (lib)"
     entry: .venv/bin/python -m blueprint_linters --preset lib
     language: system
     types: [python]
   ```

4. **`.github/workflows/ci.yml`** — replace `vendor/blueprint_linters-*.whl` in install commands:
   ```bash
   # before
   pip install -e ".[dev]" vendor/blueprint_linters-*.whl
   # after
   pip install -e ".[dev]"
   ```

5. **Docker/sandbox files** (optional) — `Dockerfile`, `docker-compose.yml`, `entrypoint.sh`, `supervisord.conf` are for local sandbox development and can be removed.

## Setup after cleanup

```bash
uv sync --group dev
uv run pytest tests/ -m "not integration and not performance" -x
```
